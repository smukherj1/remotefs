use std::fmt;
use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};
use uuid::Uuid;

use crate::digest::{Digest, DigestError};
use crate::error_context::ResultContext as _;
use crate::reapi::bytestream::{ReadRequest, WriteRequest, byte_stream_client::ByteStreamClient};
use crate::reapi::remote_execution::{
    BatchReadBlobsRequest, BatchUpdateBlobsRequest, FindMissingBlobsRequest,
    batch_update_blobs_request, compressor,
    content_addressable_storage_client::ContentAddressableStorageClient,
};

const DEFAULT_DOWNLOAD_BLOB_STREAM_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_BATCH_UPDATE_BUDGET_BYTES: usize = 3_500 * 1024;
const FIND_MISSING_TIMEOUT_SECONDS: u64 = 10;
const BATCH_READ_TIMEOUT_SECONDS: u64 = 30;
const BATCH_UPDATE_TIMEOUT_SECONDS: u64 = 30;
const BYTESTREAM_IDLE_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_ATTEMPTS: usize = 5;
const MAX_RETRY_ATTEMPTS: usize = 5;
const DEFAULT_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const RETRY_BACKOFFS: [Duration; MAX_RETRY_ATTEMPTS - 1] = [
    DEFAULT_RETRY_INITIAL_BACKOFF,
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
];

/// Defines a configuration to connect to a CAS server.
#[derive(Debug, Clone)]
pub struct CasConfig {
    /// URL of the CAS server to connect to.
    pub cas_url: String,
    /// Instance name to use when connecting to the CAS server which namespaces the
    /// blobs stored by the CAS server.
    pub instance_name: String,
    /// Size in bytes of blobs that when exceeded will result in streaming the blob instead
    /// of downloading the entire blob in one shot.
    pub download_blob_stream_threshold_bytes: usize,
    /// Total size of blobs that will be included in a single batch update request.
    pub batch_update_budget_bytes: usize,
    /// RPC timeout for a FindMissingBlobs request.
    pub find_missing_timeout: Duration,
    /// RPC timeout for a BatchReadBlobs request.
    pub batch_read_timeout: Duration,
    /// RPC timeout for a BatchUpdateBlobs request.
    pub batch_update_timeout: Duration,
    /// Timeout for how long a Bytestream (Read or Write) is allowed to be
    /// idle between bytes transferred.
    pub bytestream_idle_timeout: Duration,
    /// Number of times an RPC is retried for retryable errors before giving
    /// up.
    pub max_attempts: usize,
}

impl CasConfig {
    /// Create a new CAS connection configuration for the given CAS Server URL and instance
    /// name.
    pub fn new(
        cas_url: impl Into<String>,
        instance_name: impl Into<String>,
    ) -> Result<Self, CasError> {
        let config = Self {
            cas_url: cas_url.into(),
            instance_name: instance_name.into(),
            download_blob_stream_threshold_bytes: DEFAULT_DOWNLOAD_BLOB_STREAM_THRESHOLD_BYTES,
            batch_update_budget_bytes: DEFAULT_BATCH_UPDATE_BUDGET_BYTES,
            find_missing_timeout: Duration::from_secs(FIND_MISSING_TIMEOUT_SECONDS),
            batch_read_timeout: Duration::from_secs(BATCH_READ_TIMEOUT_SECONDS),
            batch_update_timeout: Duration::from_secs(BATCH_UPDATE_TIMEOUT_SECONDS),
            bytestream_idle_timeout: Duration::from_secs(BYTESTREAM_IDLE_TIMEOUT_SECONDS),
            max_attempts: DEFAULT_ATTEMPTS,
        };
        validate_instance_name(&config.instance_name)
            .context("validate CAS instance name while constructing CasConfig")?;
        validate_retry_attempts(config.max_attempts)
            .context("validate retry attempts while constructing CasConfig")?;
        Ok(config)
    }
}

/// CAS operation identity used in errors and retry/timeout policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasOperation {
    Connect,
    FindMissingBlobs,
    BatchReadBlobs,
    BatchUpdateBlobs,
    ByteStreamRead,
    ByteStreamWrite,
}

impl CasOperation {
    fn timeout(self, config: &CasConfig) -> Duration {
        match self {
            Self::FindMissingBlobs => config.find_missing_timeout,
            Self::BatchReadBlobs => config.batch_read_timeout,
            Self::BatchUpdateBlobs => config.batch_update_timeout,
            Self::ByteStreamRead | Self::ByteStreamWrite => config.bytestream_idle_timeout,
            Self::Connect => Duration::from_secs(30),
        }
    }
}

impl fmt::Display for CasOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Connect => "connect",
            Self::FindMissingBlobs => "FindMissingBlobs",
            Self::BatchReadBlobs => "BatchReadBlobs",
            Self::BatchUpdateBlobs => "BatchUpdateBlobs",
            Self::ByteStreamRead => "ByteStream.Read",
            Self::ByteStreamWrite => "ByteStream.Write",
        })
    }
}

#[derive(Error, Debug)]
pub enum CasError {
    #[error("Invalid CAS instance name `{0}`: {1}")]
    InvalidInstanceName(String, String),
    #[error("Invalid CAS retry attempts `{0}`: {1}")]
    InvalidRetryAttempts(usize, String),
    #[error("Unsupported CAS URL `{0}`. MVP supports grpc:// endpoints only")]
    UnsupportedUrl(String),
    #[error(
        "CAS transport error for {operation} against {cas_url} instance `{instance_name}`: {source}"
    )]
    Transport {
        operation: CasOperation,
        cas_url: String,
        instance_name: String,
        #[source]
        source: tonic::transport::Error,
    },
    #[error(
        "CAS operation {operation} failed after {attempts} attempt(s) against {cas_url} instance `{instance_name}`: {status}"
    )]
    Rpc {
        operation: CasOperation,
        attempts: usize,
        cas_url: String,
        instance_name: String,
        status: Box<Status>,
    },
    #[error("CAS operation {operation} failed for {digest}: {message}")]
    BlobStatus {
        operation: CasOperation,
        digest: Digest,
        message: String,
    },
    #[error("CAS response for {operation} omitted digest {digest}")]
    MissingResponse {
        operation: CasOperation,
        digest: Digest,
    },
    #[error("Downloaded blob {digest} failed verification: {source}")]
    Verification {
        digest: Digest,
        #[source]
        source: DigestError,
    },
    #[error("{operation}: {source}")]
    Context {
        operation: String,
        #[source]
        source: Box<CasError>,
    },
}

impl crate::error_context::ResultContextError for CasError {
    fn with_context(self, operation: String) -> Self {
        CasError::Context {
            operation,
            source: Box::new(self),
        }
    }
}

/// Represents a blob of bytes along with its digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Blob {
    pub digest: Digest,
    pub data: Vec<u8>,
}

impl Blob {
    /// Creates a blob from raw bytes and computes its SHA-256 digest.
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            digest: Digest::for_bytes(&data),
            data,
        }
    }
}

/// Minimal blob storage surface used by filesystem and upload code.
///
/// Implementations check blob existence, upload blobs by digest, and return
/// verified bytes for downloads. Transport-specific resource names and response
/// validation remain encapsulated by the implementation.
#[async_trait]
pub trait BlobStore {
    /// Returns the subset of `digests` missing from storage.
    ///
    /// Implementations may return transport, authorization, validation, or
    /// digest-conversion errors when the remote service cannot answer the
    /// existence check.
    async fn find_missing_blobs(&mut self, digests: &[Digest]) -> Result<Vec<Digest>, CasError>;

    /// Uploads blobs that are not already present in storage.
    ///
    /// Each blob's embedded digest is used as its content identity. The method
    /// may fail on transport errors or if the remote service rejects an upload.
    async fn upload_blobs(&mut self, blobs: Vec<Blob>) -> Result<(), CasError>;

    /// Downloads and verifies the blob identified by `digest`.
    ///
    /// Returns verified bytes. A digest hash or size mismatch is reported as a
    /// verification error and is not retried as a semantic success.
    async fn download_blob(&mut self, digest: &Digest) -> Result<Bytes, CasError>;
}

/// Client to the CAS server allowing the transfer of blobs.
#[derive(Clone)]
pub struct CasClient {
    config: CasConfig,
    cas: ContentAddressableStorageClient<Channel>,
    bytestream: ByteStreamClient<Channel>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PackedBatch {
    pub batches: Vec<Vec<Blob>>,
    pub bytestream: Vec<Blob>,
}

impl CasClient {
    /// Connects to a CAS server with the given configuration.
    ///
    /// The configuration must contain a non-empty valid REAPI instance name, a
    /// supported `grpc://` CAS URL, and a retry attempt count in the supported
    /// range. Returns a client ready to issue CAS and ByteStream RPCs or a
    /// transport/configuration error if setup fails.
    pub async fn connect(config: CasConfig) -> Result<Self, CasError> {
        validate_instance_name(&config.instance_name).with_context(|| {
            format!(
                "validate CAS instance name before connecting to {}",
                config.cas_url
            )
        })?;
        validate_retry_attempts(config.max_attempts).with_context(|| {
            format!(
                "validate retry attempts before connecting to {}",
                config.cas_url
            )
        })?;
        let endpoint = endpoint_from_grpc_url(&config.cas_url)
            .with_context(|| format!("parse grpc CAS endpoint from {}", config.cas_url))?;
        let channel = Endpoint::from_shared(endpoint.clone())
            .map_err(|source| CasError::Transport {
                operation: CasOperation::Connect,
                cas_url: config.cas_url.clone(),
                instance_name: config.instance_name.clone(),
                source,
            })?
            .connect()
            .await
            .map_err(|source| CasError::Transport {
                operation: CasOperation::Connect,
                cas_url: config.cas_url.clone(),
                instance_name: config.instance_name.clone(),
                source,
            })?;

        Ok(Self {
            config,
            cas: ContentAddressableStorageClient::new(channel.clone()),
            bytestream: ByteStreamClient::new(channel),
        })
    }

    /// Returns the immutable connection and transfer configuration used by this client.
    pub fn config(&self) -> &CasConfig {
        &self.config
    }

    /// Finds blobs that do not exist in the CAS.
    ///
    /// Accepts digest identities to check and returns the subset reported
    /// missing by the remote CAS. Transport failures and malformed response
    /// digests are returned as `CasError`.
    pub async fn find_missing_blobs(
        &mut self,
        digests: &[Digest],
    ) -> Result<Vec<Digest>, CasError> {
        let request = FindMissingBlobsRequest {
            instance_name: self.config.instance_name.clone(),
            blob_digests: digests.iter().map(Digest::to_reapi).collect(),
        };
        let response = self
            .retry_rpc(CasOperation::FindMissingBlobs, || {
                let mut cas = self.cas.clone();
                let request = request.clone();
                async move { cas.find_missing_blobs(Request::new(request)).await }
            })
            .await;
        let response = response
            .with_context(|| {
                format!(
                    "check which {} blob digest(s) are missing from CAS",
                    digests.len()
                )
            })?
            .into_inner();

        response
            .missing_blob_digests
            .iter()
            .map(Digest::from_reapi)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| CasError::Verification {
                digest: Digest::for_bytes(&[]),
                source,
            })
    }

    /// Uploads blobs that are missing from the CAS.
    ///
    /// The client first performs an existence check and then uploads only the
    /// missing blobs. Small blobs are packed into `BatchUpdateBlobs`; blobs that
    /// exceed the configured batch budget are uploaded through ByteStream.
    pub async fn upload_blobs(&mut self, blobs: Vec<Blob>) -> Result<(), CasError> {
        let digests = blobs
            .iter()
            .map(|blob| blob.digest.clone())
            .collect::<Vec<_>>();
        let missing = self.find_missing_blobs(&digests).await.with_context(|| {
            format!(
                "check missing blobs before upload for {} blob(s)",
                digests.len()
            )
        })?;
        let missing_blobs = blobs
            .into_iter()
            .filter(|blob| missing.contains(&blob.digest))
            .collect::<Vec<_>>();
        let missing_count = missing_blobs.len();
        self.batch_update_blobs(missing_blobs)
            .await
            .with_context(|| format!("upload {missing_count} missing blob(s) through CAS"))
    }

    /// Downloads a blob from the CAS and verifies its digest.
    ///
    /// Blobs at or below the configured download threshold use
    /// `BatchReadBlobs`; larger blobs use ByteStream. The returned bytes are
    /// verified against `digest`, and mismatches return `CasError::Verification`.
    pub async fn download_blob(&mut self, digest: &Digest) -> Result<Bytes, CasError> {
        if digest.size_bytes() as usize <= self.config.download_blob_stream_threshold_bytes {
            self.batch_read_blob(digest)
                .await
                .with_context(|| format!("download blob {digest} through BatchReadBlobs"))
        } else {
            self.bytestream_read(digest)
                .await
                .with_context(|| format!("download blob {digest} through ByteStream.Read"))
        }
    }

    async fn batch_read_blob(&mut self, digest: &Digest) -> Result<Bytes, CasError> {
        let request = BatchReadBlobsRequest {
            instance_name: self.config.instance_name.clone(),
            digests: vec![digest.to_reapi()],
            acceptable_compressors: vec![compressor::Value::Identity as i32],
        };
        let response = self
            .retry_rpc(CasOperation::BatchReadBlobs, || {
                let mut cas = self.cas.clone();
                let request = request.clone();
                async move { cas.batch_read_blobs(Request::new(request)).await }
            })
            .await;
        let response = response
            .with_context(|| format!("read blob {digest} with BatchReadBlobs"))?
            .into_inner();
        let response = response
            .responses
            .first()
            .ok_or_else(|| CasError::MissingResponse {
                operation: CasOperation::BatchReadBlobs,
                digest: digest.clone(),
            })?;
        if let Some(status) = &response.status
            && status.code != 0
        {
            return Err(CasError::BlobStatus {
                operation: CasOperation::BatchReadBlobs,
                digest: digest.clone(),
                message: status.message.clone(),
            });
        }
        verify_download(digest, &response.data)
            .with_context(|| format!("verify BatchReadBlobs response for {digest}"))?;
        Ok(Bytes::from(response.data.clone()))
    }

    async fn batch_update_blobs(&mut self, blobs: Vec<Blob>) -> Result<(), CasError> {
        let packed = pack_batch_update_blobs(blobs, self.config.batch_update_budget_bytes);

        let batch_count = packed.batches.len();
        let bytestream_count = packed.bytestream.len();
        self.upload_packed_batches(packed.batches)
            .await
            .with_context(|| format!("upload {batch_count} packed BatchUpdateBlobs request(s)"))?;
        self.upload_bytestream_blobs(packed.bytestream)
            .await
            .with_context(|| format!("upload {bytestream_count} blob(s) through ByteStream.Write"))
    }

    async fn upload_packed_batches(&mut self, batches: Vec<Vec<Blob>>) -> Result<(), CasError> {
        for (index, batch) in batches.into_iter().enumerate() {
            let blob_count = batch.len();
            self.upload_batch(batch).await.with_context(|| {
                format!("upload BatchUpdateBlobs batch {index} containing {blob_count} blob(s)")
            })?;
        }
        Ok(())
    }

    async fn upload_batch(&mut self, batch: Vec<Blob>) -> Result<(), CasError> {
        let request = BatchUpdateBlobsRequest {
            instance_name: self.config.instance_name.clone(),
            requests: batch
                .iter()
                .map(|blob| batch_update_blobs_request::Request {
                    digest: Some(blob.digest.to_reapi()),
                    data: blob.data.clone(),
                    compressor: compressor::Value::Identity as i32,
                })
                .collect(),
        };
        let response = self
            .retry_rpc(CasOperation::BatchUpdateBlobs, || {
                let mut cas = self.cas.clone();
                let request = request.clone();
                async move { cas.batch_update_blobs(Request::new(request)).await }
            })
            .await;
        let response = response
            .with_context(|| {
                format!(
                    "send BatchUpdateBlobs request containing {} blob(s)",
                    batch.len()
                )
            })?
            .into_inner();

        for blob in &batch {
            let response = response
                .responses
                .iter()
                .find(|response| response.digest.as_ref() == Some(&blob.digest.to_reapi()))
                .ok_or_else(|| CasError::MissingResponse {
                    operation: CasOperation::BatchUpdateBlobs,
                    digest: blob.digest.clone(),
                })?;
            if let Some(status) = &response.status
                && status.code != 0
            {
                return Err(CasError::BlobStatus {
                    operation: CasOperation::BatchUpdateBlobs,
                    digest: blob.digest.clone(),
                    message: status.message.clone(),
                });
            }
        }
        Ok(())
    }

    async fn upload_bytestream_blobs(&mut self, blobs: Vec<Blob>) -> Result<(), CasError> {
        for blob in blobs {
            self.bytestream_write(&blob)
                .await
                .with_context(|| format!("upload blob {} with ByteStream.Write", blob.digest))?;
        }
        Ok(())
    }

    async fn bytestream_write(&mut self, blob: &Blob) -> Result<(), CasError> {
        let resource_name =
            bytestream_write_resource_name(&self.config.instance_name, &blob.digest);
        let data = blob.data.clone();
        let digest = blob.digest.clone();

        let write = self
            .retry_rpc(CasOperation::ByteStreamWrite, || {
                let mut bytestream = self.bytestream.clone();
                let resource_name = resource_name.clone();
                let data = data.clone();
                let digest = digest.clone();
                async move {
                    let (tx, rx) = tokio::sync::mpsc::channel(2);
                    let request_resource_name = resource_name.clone();
                    tx.send(WriteRequest {
                        resource_name: request_resource_name,
                        write_offset: 0,
                        finish_write: true,
                        data,
                    })
                    .await
                    .map_err(|_| Status::internal("failed to queue ByteStream write request"))?;
                    drop(tx);
                    let response = bytestream
                    .write(Request::new(ReceiverStream::new(rx)))
                    .await
                    .map_err(|status| {
                        Status::new(
                            status.code(),
                            format!(
                                "send ByteStream write request for resource {resource_name}: {}",
                                status.message()
                            ),
                        )
                    })?;
                    if response.get_ref().committed_size != digest.size_bytes() {
                        return Err(Status::data_loss(format!(
                            "committed {} bytes for {}",
                            response.get_ref().committed_size,
                            digest
                        )));
                    }
                    Ok(response)
                }
            })
            .await;
        write
            .with_context(|| {
                format!(
                    "write blob {} to ByteStream resource {resource_name}",
                    blob.digest
                )
            })
            .map(|_| ())
    }

    async fn bytestream_read(&mut self, digest: &Digest) -> Result<Bytes, CasError> {
        let resource_name = bytestream_read_resource_name(&self.config.instance_name, digest);
        let response = self
            .retry_rpc(CasOperation::ByteStreamRead, || {
                let mut bytestream = self.bytestream.clone();
                let resource_name = resource_name.clone();
                async move {
                    bytestream
                        .read(Request::new(ReadRequest {
                            resource_name,
                            read_offset: 0,
                            read_limit: 0,
                        }))
                        .await
                }
            })
            .await;
        let mut response = response
            .with_context(|| {
                format!("start ByteStream.Read for {digest} resource {resource_name}")
            })?
            .into_inner();

        let mut data = Vec::new();
        while let Some(chunk) =
            tokio::time::timeout(self.config.bytestream_idle_timeout, response.next())
                .await
                .map_err(|_| CasError::Rpc {
                    operation: CasOperation::ByteStreamRead,
                    attempts: 1,
                    cas_url: self.config.cas_url.clone(),
                    instance_name: self.config.instance_name.clone(),
                    status: Box::new(Status::deadline_exceeded(format!(
                        "idle timeout reading {digest}"
                    ))),
                })?
        {
            data.extend_from_slice(
                &chunk
                    .map_err(|status| CasError::Rpc {
                        operation: CasOperation::ByteStreamRead,
                        attempts: 1,
                        cas_url: self.config.cas_url.clone(),
                        instance_name: self.config.instance_name.clone(),
                        status: Box::new(Status::new(
                            status.code(),
                            format!(
                                "read next ByteStream chunk for {digest} resource {resource_name}: {}",
                                status.message()
                            ),
                        )),
                    })?
                    .data,
            );
        }
        verify_download(digest, &data).with_context(|| {
            format!("verify ByteStream.Read response for {digest} resource {resource_name}")
        })?;
        Ok(Bytes::from(data))
    }

    async fn retry_rpc<F, Fut, T>(
        &self,
        operation: CasOperation,
        mut op: F,
    ) -> Result<tonic::Response<T>, CasError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, Status>>,
    {
        let mut attempt = 1;
        loop {
            let result = tokio::time::timeout(operation.timeout(&self.config), op()).await;
            match result {
                // Success.
                Ok(Ok(response)) => return Ok(response),
                // Failed with retryable error and we still have retry attempts left.
                Ok(Err(status))
                    if attempt < self.config.max_attempts && is_retryable_status(&status) =>
                {
                    sleep_before_retry(attempt).await;
                    attempt += 1;
                }
                // Request failed.
                Ok(Err(status)) => {
                    return Err(CasError::Rpc {
                        operation,
                        attempts: attempt,
                        cas_url: self.config.cas_url.clone(),
                        instance_name: self.config.instance_name.clone(),
                        status: Box::new(status),
                    });
                }
                // Operation timeout exceeded but we still have retry attempts left.
                Err(_) if attempt < self.config.max_attempts => {
                    sleep_before_retry(attempt).await;
                    attempt += 1;
                }
                // Operation timed out.
                Err(_) => {
                    return Err(CasError::Rpc {
                        operation,
                        attempts: attempt,
                        cas_url: self.config.cas_url.clone(),
                        instance_name: self.config.instance_name.clone(),
                        status: Box::new(Status::deadline_exceeded("operation attempt timed out")),
                    });
                }
            }
        }
    }
}

fn endpoint_from_grpc_url(cas_url: &str) -> Result<String, CasError> {
    cas_url
        .strip_prefix("grpc://")
        .filter(|rest| !rest.is_empty())
        .map(|rest| format!("http://{rest}"))
        .ok_or_else(|| CasError::UnsupportedUrl(cas_url.to_string()))
}

fn validate_instance_name(instance_name: &str) -> Result<(), CasError> {
    if instance_name.is_empty() {
        return Err(CasError::InvalidInstanceName(
            instance_name.to_string(),
            "must not be empty".to_string(),
        ));
    }
    for segment in instance_name.split('/') {
        if segment.is_empty() {
            return Err(CasError::InvalidInstanceName(
                instance_name.to_string(),
                "must not contain empty path segments".to_string(),
            ));
        }
        if matches!(segment, "blobs" | "uploads" | "compressed-blobs") {
            return Err(CasError::InvalidInstanceName(
                instance_name.to_string(),
                format!("reserved REAPI resource segment `{segment}` is not allowed"),
            ));
        }
    }
    Ok(())
}

fn validate_retry_attempts(max_attempts: usize) -> Result<(), CasError> {
    if max_attempts == 0 {
        return Err(CasError::InvalidRetryAttempts(
            max_attempts,
            "must be at least 1".to_string(),
        ));
    }
    if max_attempts > MAX_RETRY_ATTEMPTS {
        return Err(CasError::InvalidRetryAttempts(
            max_attempts,
            format!("must not exceed {MAX_RETRY_ATTEMPTS}"),
        ));
    }
    Ok(())
}

fn bytestream_read_resource_name(instance_name: &str, digest: &Digest) -> String {
    format!(
        "{}/blobs/{}/{}",
        instance_name,
        digest.hash(),
        digest.size_bytes()
    )
}

fn bytestream_write_resource_name(instance_name: &str, digest: &Digest) -> String {
    format!(
        "{}/uploads/{}/blobs/{}/{}",
        instance_name,
        Uuid::new_v4(),
        digest.hash(),
        digest.size_bytes()
    )
}

fn verify_download(digest: &Digest, bytes: &[u8]) -> Result<(), CasError> {
    digest
        .verify_bytes(bytes)
        .map_err(|source| CasError::Verification {
            digest: digest.clone(),
            source,
        })
}

fn pack_batch_update_blobs(blobs: Vec<Blob>, budget_bytes: usize) -> PackedBatch {
    let mut batches: Vec<Vec<Blob>> = Vec::new();
    let mut current: Vec<Blob> = Vec::new();
    let mut current_bytes = 0usize;
    let mut bytestream = Vec::new();

    for blob in blobs {
        let blob_size = blob.data.len();
        if blob_size > budget_bytes {
            bytestream.push(blob);
            continue;
        }

        if !current.is_empty() && current_bytes + blob_size > budget_bytes {
            batches.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += blob_size;
        current.push(blob);
    }

    if !current.is_empty() {
        batches.push(current);
    }

    PackedBatch {
        batches,
        bytestream,
    }
}

fn is_retryable_status(status: &Status) -> bool {
    if matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted
    ) {
        return true;
    }

    status.code() == Code::Internal && status.message().to_ascii_lowercase().contains("reset")
}

async fn sleep_before_retry(attempt: usize) {
    let duration = retry_backoff(attempt);
    tokio::time::sleep(duration).await;
}

fn retry_backoff(attempt: usize) -> Duration {
    RETRY_BACKOFFS
        .get(attempt.saturating_sub(1))
        .copied()
        .unwrap_or_else(|| {
            *RETRY_BACKOFFS
                .last()
                .expect("retry backoff table is non-empty")
        })
}

#[async_trait]
impl BlobStore for CasClient {
    async fn find_missing_blobs(&mut self, digests: &[Digest]) -> Result<Vec<Digest>, CasError> {
        CasClient::find_missing_blobs(self, digests).await
    }

    async fn upload_blobs(&mut self, blobs: Vec<Blob>) -> Result<(), CasError> {
        CasClient::upload_blobs(self, blobs).await
    }

    async fn download_blob(&mut self, digest: &Digest) -> Result<Bytes, CasError> {
        CasClient::download_blob(self, digest).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_digest(size: i64) -> Digest {
        Digest::new(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
            size,
        )
        .unwrap()
    }

    fn rendered_chain(error: CasError) -> String {
        anyhow::Error::new(error)
            .chain()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn instance_name_rejects_empty_and_reserved_segments() {
        assert!(validate_instance_name("").is_err());
        assert!(validate_instance_name("project/blobs").is_err());
        assert!(validate_instance_name("uploads/project").is_err());
        assert!(validate_instance_name("project/compressed-blobs/cache").is_err());
        assert!(validate_instance_name("project//cache").is_err());
        assert!(validate_instance_name("project/cache").is_ok());
    }

    #[test]
    fn retry_attempt_validation_rejects_zero_and_above_supported_max() {
        assert!(validate_retry_attempts(1).is_ok());
        assert!(validate_retry_attempts(MAX_RETRY_ATTEMPTS).is_ok());
        assert!(matches!(
            validate_retry_attempts(0),
            Err(CasError::InvalidRetryAttempts(0, _))
        ));
        assert!(matches!(
            validate_retry_attempts(MAX_RETRY_ATTEMPTS + 1),
            Err(CasError::InvalidRetryAttempts(_, _))
        ));
    }

    #[test]
    fn operation_timeout_uses_typed_operations() {
        let config = CasConfig::new("grpc://127.0.0.1:9092", "project").unwrap();

        assert_eq!(
            CasOperation::FindMissingBlobs.timeout(&config),
            config.find_missing_timeout
        );
        assert_eq!(
            CasOperation::BatchReadBlobs.timeout(&config),
            config.batch_read_timeout
        );
        assert_eq!(
            CasOperation::BatchUpdateBlobs.timeout(&config),
            config.batch_update_timeout
        );
        assert_eq!(
            CasOperation::ByteStreamRead.timeout(&config),
            config.bytestream_idle_timeout
        );
        assert_eq!(
            CasOperation::ByteStreamWrite.timeout(&config),
            config.bytestream_idle_timeout
        );
    }

    #[test]
    fn retry_backoff_uses_supported_schedule() {
        assert_eq!(retry_backoff(1), Duration::from_millis(100));
        assert_eq!(retry_backoff(2), Duration::from_millis(250));
        assert_eq!(retry_backoff(3), Duration::from_millis(500));
        assert_eq!(retry_backoff(4), Duration::from_secs(1));
        assert_eq!(retry_backoff(99), Duration::from_secs(1));
    }

    #[test]
    fn bytestream_resource_names_include_instance_name() {
        let digest = valid_digest(12);
        assert_eq!(
            bytestream_read_resource_name("project/cache", &digest),
            format!("project/cache/blobs/{}/12", digest.hash())
        );

        let write_name = bytestream_write_resource_name("project/cache", &digest);
        assert!(write_name.starts_with("project/cache/uploads/"));
        assert!(write_name.ends_with(&format!("/blobs/{}/12", digest.hash())));
    }

    #[test]
    fn verifier_rejects_hash_or_size_mismatch() {
        let digest = Digest::for_bytes(b"hello");
        assert!(verify_download(&digest, b"hello").is_ok());
        assert!(matches!(
            verify_download(&digest, b"hell"),
            Err(CasError::Verification { .. })
        ));
        assert!(matches!(
            verify_download(&digest, b"jello"),
            Err(CasError::Verification { .. })
        ));
    }

    #[test]
    fn batch_packing_respects_budget_and_moves_oversized_entries() {
        let small_a = Blob::new(vec![b'a'; 8]);
        let small_b = Blob::new(vec![b'b'; 8]);
        let too_large = Blob::new(vec![b'c'; 64]);

        let packed = pack_batch_update_blobs(
            vec![small_a.clone(), small_b.clone(), too_large.clone()],
            12,
        );

        assert_eq!(packed.batches.len(), 2);
        assert_eq!(packed.batches[0], vec![small_a]);
        assert_eq!(packed.batches[1], vec![small_b]);
        assert_eq!(packed.bytestream, vec![too_large]);
    }

    #[test]
    fn retry_classifier_separates_transient_and_semantic_statuses() {
        for code in [
            Code::Unavailable,
            Code::DeadlineExceeded,
            Code::ResourceExhausted,
            Code::Aborted,
        ] {
            assert!(is_retryable_status(&Status::new(code, "transient")));
        }
        assert!(is_retryable_status(&Status::internal("transport reset")));

        for code in [
            Code::NotFound,
            Code::InvalidArgument,
            Code::PermissionDenied,
            Code::Unauthenticated,
            Code::FailedPrecondition,
        ] {
            assert!(!is_retryable_status(&Status::new(code, "semantic")));
        }
        assert!(!is_retryable_status(&Status::internal("digest mismatch")));
    }

    #[test]
    fn cas_context_chain_preserves_rpc_operation_url_and_instance() {
        let error = Err::<(), CasError>(CasError::Rpc {
            operation: CasOperation::BatchReadBlobs,
            attempts: 3,
            cas_url: "grpc://127.0.0.1:9092".to_string(),
            instance_name: "remotefs/tests".to_string(),
            status: Box::new(Status::unavailable("connection reset by peer")),
        })
        .with_context(|| format!("download blob {} through BatchReadBlobs", valid_digest(12)))
        .unwrap_err();

        let rendered = rendered_chain(error);
        assert!(rendered.contains("download blob sha256:"));
        assert!(rendered.contains("BatchReadBlobs"));
        assert!(rendered.contains("failed after 3 attempt(s)"));
        assert!(rendered.contains("grpc://127.0.0.1:9092"));
        assert!(rendered.contains("remotefs/tests"));
    }
}
