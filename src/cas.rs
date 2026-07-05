use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};
use uuid::Uuid;

use crate::digest::{Digest, DigestError};
use crate::error_context::ResultContext as _;
use crate::reapi::bytestream::{
    ReadRequest, WriteRequest, WriteResponse, byte_stream_client::ByteStreamClient,
};
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
const BYTESTREAM_UPLOAD_CHUNK_BYTES: usize = 1024 * 1024;
const DEFAULT_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const RETRY_BACKOFFS: [Duration; MAX_RETRY_ATTEMPTS - 1] = [
    DEFAULT_RETRY_INITIAL_BACKOFF,
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
];

// Tracks the in-progress BatchUpdateBlobs request without exposing batching
// policy outside the CAS client.
struct BatchUpdateState {
    blobs: Vec<Blob>,
    bytes: usize,
    next_index: usize,
}

impl BatchUpdateState {
    fn new() -> Self {
        Self {
            blobs: Vec::new(),
            bytes: 0,
            next_index: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.blobs.is_empty()
    }

    fn would_exceed_budget(&self, blob_size: usize, budget: usize) -> bool {
        !self.is_empty() && self.bytes + blob_size > budget
    }

    fn push(&mut self, blob: Blob, blob_size: usize) {
        self.bytes += blob_size;
        self.blobs.push(blob);
    }

    fn take(&mut self) -> Vec<Blob> {
        self.bytes = 0;
        self.next_index += 1;
        std::mem::take(&mut self.blobs)
    }
}

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
    #[error("CAS filesystem error during {operation} at {path}: {source}")]
    Io {
        operation: CasOperation,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Error converting between RE API and native object: {operation}: {source}")]
    ReApiConversion {
        operation: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
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

/// A blob that can be stored on CAS.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Blob {
    pub digest: Digest,
    pub contents: BlobContents,
}

/// Backing data for a Blob.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobContents {
    // The blob contents are available directly.
    Bytes(Bytes),
    // The blob contents are available at this
    // file path. Typically used for large blobs.
    FilePath(PathBuf),
}

impl Blob {
    /// Creates an in-memory blob from raw bytes and computes its SHA-256 digest.
    pub fn from_bytes(data: impl Into<Bytes>) -> Self {
        let data = data.into();
        Self {
            digest: Digest::for_bytes(data.as_ref()),
            contents: BlobContents::Bytes(data),
        }
    }

    /// Creates a path-backed blob with a caller-provided digest.
    pub fn from_file_path(digest: Digest, path: impl Into<PathBuf>) -> Self {
        Self {
            digest,
            contents: BlobContents::FilePath(path.into()),
        }
    }
}

/// Statistics for blobs uploaded by a `BlobStore`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UploadStats {
    pub uploaded_blobs: usize,
    pub bytes_uploaded: u64,
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

    /// Uploads the given blobs to the BlobStore.
    async fn upload_blobs(&mut self, blobs: Vec<Blob>) -> Result<UploadStats, CasError>;

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

    /// Uploads caller-selected blobs to the CAS.
    ///
    /// The caller owns existence checks. Small byte-backed and path-backed
    /// blobs are packed into `BatchUpdateBlobs`; larger blobs are uploaded
    /// through ByteStream, with path-backed blobs streamed from disk.
    pub async fn upload_blobs(&mut self, blobs: Vec<Blob>) -> Result<UploadStats, CasError> {
        let uploaded_blobs = blobs.len();
        let bytes_uploaded = blobs.iter().try_fold(0u64, |total, blob| {
            let size = digest_size_u64(&blob.digest)?;
            total.checked_add(size).ok_or_else(|| CasError::BlobStatus {
                operation: CasOperation::BatchUpdateBlobs,
                digest: blob.digest.clone(),
                message: "uploaded byte counter overflowed u64".to_string(),
            })
        })?;
        self.upload_blobs_to_cas(blobs)
            .await
            .with_context(|| format!("upload {uploaded_blobs} caller-selected blob(s)"))?;
        Ok(UploadStats {
            uploaded_blobs,
            bytes_uploaded,
        })
    }

    /// Downloads a blob from the CAS and verifies its digest.
    ///
    /// Blobs at or below the configured download threshold use
    /// `BatchReadBlobs`; larger blobs use ByteStream. The returned bytes are
    /// verified against `digest`, and mismatches return `CasError::Verification`.
    pub async fn download_blob(&mut self, digest: &Digest) -> Result<Bytes, CasError> {
        if digest_size_usize(digest)? <= self.config.download_blob_stream_threshold_bytes {
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

    // Uploads caller-selected blobs after existence checks have already
    // happened in the upload pipeline. BatchUpdateBlobs is used only for blobs
    // that fit the configured request budget; larger blobs use ByteStream.
    async fn upload_blobs_to_cas(&mut self, blobs: Vec<Blob>) -> Result<(), CasError> {
        let mut batch = BatchUpdateState::new();
        for blob in blobs {
            let blob_size = digest_size_usize(&blob.digest)?;
            if blob_size > self.config.batch_update_budget_bytes {
                self.flush_batch_update(&mut batch).await?;
                self.upload_oversized_blob(blob).await?;
                continue;
            }

            let blob = load_blob_as_bytes(blob, blob_size).await?;
            if batch.would_exceed_budget(blob_size, self.config.batch_update_budget_bytes) {
                self.flush_batch_update(&mut batch).await?;
            }
            batch.push(blob, blob_size);
        }

        self.flush_batch_update(&mut batch).await
    }

    async fn flush_batch_update(&mut self, batch: &mut BatchUpdateState) -> Result<(), CasError> {
        if batch.is_empty() {
            return Ok(());
        }

        let batch_index = batch.next_index;
        self.upload_batch_to_cas(batch.take())
            .await
            .with_context(|| format!("upload BatchUpdateBlobs batch {batch_index}"))
    }

    async fn upload_oversized_blob(&mut self, blob: Blob) -> Result<(), CasError> {
        let digest = blob.digest.clone();
        self.bytestream_write_blob(blob)
            .await
            .with_context(|| format!("upload oversized blob {digest} through ByteStream.Write"))
    }

    // Uploads the given batch of blobs to the CAS. Assumes the batch of blobs fits within the applicable
    // limits configured for the rfs as well as that supported by the CAS server.
    async fn upload_batch_to_cas(&mut self, batch: Vec<Blob>) -> Result<(), CasError> {
        let request = BatchUpdateBlobsRequest {
            instance_name: self.config.instance_name.clone(),
            requests: batch
                .iter()
                .map(|blob| batch_update_blobs_request::Request {
                    digest: Some(blob.digest.to_reapi()),
                    data: match &blob.contents {
                        BlobContents::Bytes(data) => data.to_vec(),
                        BlobContents::FilePath(_) => {
                            unreachable!("BatchUpdateBlobs batches contain byte-backed blobs")
                        }
                    },
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

    // Uploads the given blob using ByteStream.Write. Path-backed blobs are
    // streamed from disk on each retry so large files are not copied into
    // memory; byte-backed sources are already resident and are sent directly.
    async fn bytestream_write_blob(&mut self, blob: Blob) -> Result<(), CasError> {
        let data = match blob.contents {
            BlobContents::Bytes(data) => data,
            BlobContents::FilePath(path) => {
                // Stream the blob from the file path.
                return self.bytestream_write_file(blob.digest, path).await;
            }
        };
        // Stream the inlined blob.
        let resource_name =
            bytestream_write_resource_name(&self.config.instance_name, &blob.digest);
        let digest = blob.digest.clone();

        let write = self
            .retry_rpc(CasOperation::ByteStreamWrite, || {
                let bytestream = self.bytestream.clone();
                let resource_name = resource_name.clone();
                let data = data.clone();
                let digest = digest.clone();
                async move {
                    send_bytestream_bytes_write(bytestream, resource_name, digest, data).await
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

    async fn bytestream_write_file(
        &mut self,
        digest: Digest,
        path: PathBuf,
    ) -> Result<(), CasError> {
        let resource_name = bytestream_write_resource_name(&self.config.instance_name, &digest);

        let write = self
            .retry_rpc(CasOperation::ByteStreamWrite, || {
                let bytestream = self.bytestream.clone();
                let resource_name = resource_name.clone();
                let digest = digest.clone();
                let path = path.clone();
                async move { send_bytestream_file_write(bytestream, resource_name, digest, path).await }
            })
            .await;
        write
            .with_context(|| {
                format!(
                    "write file {} to ByteStream resource {resource_name}",
                    path.display()
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

// Materializes a blob for BatchUpdateBlobs and verifies that its current size
// still matches the digest chosen by the caller.
async fn load_blob_as_bytes(blob: Blob, expected_size: usize) -> Result<Blob, CasError> {
    match blob.contents {
        BlobContents::Bytes(data) => {
            if data.len() != expected_size {
                return Err(CasError::BlobStatus {
                    operation: CasOperation::BatchUpdateBlobs,
                    digest: blob.digest,
                    message: format!(
                        "byte-backed blob size does not match digest: expected {expected_size} bytes, got {} bytes",
                        data.len()
                    ),
                });
            }

            Ok(Blob {
                digest: blob.digest,
                contents: BlobContents::Bytes(data),
            })
        }
        BlobContents::FilePath(path) => {
            // Small path-backed blobs are read into memory so they can share the
            // BatchUpdateBlobs path with directory nodes. The size check catches
            // local file races between hashing and upload before sending bytes
            // under a stale digest.
            let data = tokio::fs::read(&path).await.map_err(|io| CasError::Io {
                operation: CasOperation::BatchUpdateBlobs,
                path: path.clone(),
                source: io,
            })?;
            if data.len() != expected_size {
                return Err(CasError::BlobStatus {
                    operation: CasOperation::BatchUpdateBlobs,
                    digest: blob.digest,
                    message: format!(
                        "path-backed blob size changed before upload: expected {expected_size} bytes, read {} bytes",
                        data.len()
                    ),
                });
            }

            Ok(Blob {
                digest: blob.digest,
                contents: BlobContents::Bytes(Bytes::from(data)),
            })
        }
    }
}

async fn send_bytestream_bytes_write(
    mut bytestream: ByteStreamClient<Channel>,
    resource_name: String,
    digest: Digest,
    data: Bytes,
) -> Result<tonic::Response<WriteResponse>, Status> {
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    tx.send(WriteRequest {
        resource_name: resource_name.clone(),
        write_offset: 0,
        finish_write: true,
        data: data.to_vec(),
    })
    .await
    .map_err(|_| Status::internal("failed to queue ByteStream write request"))?;
    drop(tx);

    let response = bytestream
        .write(Request::new(ReceiverStream::new(rx)))
        .await
        .map_err(|status| bytestream_write_status(&resource_name, status))?;
    if let Some(status) = bytestream_committed_size_error(&response, &digest) {
        return Err(status);
    }
    Ok(response)
}

async fn send_bytestream_file_write(
    mut bytestream: ByteStreamClient<Channel>,
    resource_name: String,
    digest: Digest,
    path: PathBuf,
) -> Result<tonic::Response<WriteResponse>, Status> {
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let producer_resource_name = resource_name.clone();
    let producer_path = path.clone();

    // ByteStream.Write consumes an async stream while the file is read. The
    // producer task keeps those two sides progressing concurrently, and every
    // retry recreates the task so the file is reopened from offset zero.
    let producer = tokio::spawn(async move {
        send_bytestream_file_requests(tx, producer_resource_name, producer_path).await
    });

    let response = bytestream
        .write(Request::new(ReceiverStream::new(rx)))
        .await;
    let producer_result = producer.await.map_err(|source| {
        Status::internal(format!("ByteStream file producer task failed: {source}"))
    })?;
    producer_result?;

    let response = response.map_err(|status| bytestream_write_status(&resource_name, status))?;
    if let Some(status) = bytestream_committed_size_error(&response, &digest) {
        return Err(status);
    }
    Ok(response)
}

async fn send_bytestream_file_requests(
    tx: tokio::sync::mpsc::Sender<WriteRequest>,
    resource_name: String,
    path: PathBuf,
) -> Result<(), Status> {
    let mut file = tokio::fs::File::open(&path)
        .await
        .map_err(|source| Status::internal(format!("open {}: {source}", path.display())))?;
    let mut offset = 0i64;

    loop {
        let mut buffer = vec![0u8; BYTESTREAM_UPLOAD_CHUNK_BYTES];
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|source| Status::internal(format!("read {}: {source}", path.display())))?;
        buffer.truncate(read);

        // REAPI marks stream completion with a final request carrying
        // finish_write=true. For exact chunk boundaries that means an empty
        // final message at the committed offset.
        let finish_write = read == 0;
        tx.send(WriteRequest {
            resource_name: resource_name.clone(),
            write_offset: offset,
            finish_write,
            data: buffer,
        })
        .await
        .map_err(|_| Status::internal("failed to queue ByteStream file write request"))?;

        if finish_write {
            return Ok(());
        }
        offset += read as i64;
    }
}

fn bytestream_write_status(resource_name: &str, status: Status) -> Status {
    Status::new(
        status.code(),
        format!(
            "send ByteStream write request for resource {resource_name}: {}",
            status.message()
        ),
    )
}

fn bytestream_committed_size_error(
    response: &tonic::Response<WriteResponse>,
    digest: &Digest,
) -> Option<Status> {
    if response.get_ref().committed_size != digest.size_bytes() {
        return Some(Status::data_loss(format!(
            "committed {} bytes for {}",
            response.get_ref().committed_size,
            digest
        )));
    }
    None
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

fn digest_size_u64(digest: &Digest) -> Result<u64, CasError> {
    digest
        .size_bytes()
        .try_into()
        .map_err(|source| CasError::ReApiConversion {
            operation: format!("convert digest size for {digest} into u64"),
            source: Box::new(source),
        })
}

fn digest_size_usize(digest: &Digest) -> Result<usize, CasError> {
    digest
        .size_bytes()
        .try_into()
        .map_err(|source| CasError::ReApiConversion {
            operation: format!("convert digest size for {digest} into usize"),
            source: Box::new(source),
        })
}

fn verify_download(digest: &Digest, bytes: &[u8]) -> Result<(), CasError> {
    digest
        .verify_bytes(bytes)
        .map_err(|source| CasError::Verification {
            digest: digest.clone(),
            source,
        })
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

    async fn upload_blobs(&mut self, blobs: Vec<Blob>) -> Result<UploadStats, CasError> {
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
