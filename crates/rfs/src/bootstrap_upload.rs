//! Narrow bootstrap upload capability used before a daemon session exists.

use std::path::{Path, PathBuf};

use rfs_common::cas::{CasClient, CasConfig};
use rfs_common::digest::Digest;
use rfs_common::upload::{UploadOptions, upload_local_directory};
use thiserror::Error;

/// Configuration for the bootstrap uploader.
#[derive(Debug, Clone)]
pub struct BootstrapUploadConfig {
    /// Remote Execution API endpoint.
    pub cas_url: String,
    /// Non-empty REAPI instance name.
    pub instance_name: String,
}

/// Bootstrap upload failures with implementation details kept private.
#[derive(Debug, Error)]
pub enum BootstrapUploadError {
    #[error("invalid bootstrap upload configuration: {message}")]
    InvalidConfig { message: String },
    #[error("upload root `{path}` must be a directory")]
    InvalidRoot { path: PathBuf },
    #[error("connect to bootstrap CAS: {message}")]
    Connect { message: String },
    #[error("upload bootstrap directory `{path}`: {message}")]
    Upload { path: PathBuf, message: String },
}

/// Configured one-operation bootstrap upload façade.
pub struct BootstrapUploader {
    client: CasClient,
}

impl BootstrapUploader {
    /// Validates configuration and connects to the configured CAS.
    pub async fn connect(config: BootstrapUploadConfig) -> Result<Self, BootstrapUploadError> {
        let cas_config = CasConfig::new(config.cas_url, config.instance_name).map_err(|error| {
            BootstrapUploadError::InvalidConfig {
                message: error.to_string(),
            }
        })?;
        let client = CasClient::connect(cas_config).await.map_err(|error| {
            BootstrapUploadError::Connect {
                message: error.to_string(),
            }
        })?;
        Ok(Self { client })
    }

    /// Uploads a local directory and returns only its canonical root digest.
    pub async fn upload(&mut self, path: &Path) -> Result<Digest, BootstrapUploadError> {
        if !path.is_dir() {
            return Err(BootstrapUploadError::InvalidRoot {
                path: path.to_path_buf(),
            });
        }
        let summary = upload_local_directory(&mut self.client, path, UploadOptions::default())
            .await
            .map_err(|error| BootstrapUploadError::Upload {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;
        tracing::info!(
            operation = "bootstrap_upload",
            path = %path.display(),
            digest = %summary.root_digest,
            files = summary.files,
            directories = summary.directories,
            symlinks = summary.symlinks,
            uploaded_blobs = summary.uploaded_blobs,
            reused_blobs = summary.reused_blobs,
            bytes_uploaded = summary.bytes_uploaded,
            "bootstrap upload completed"
        );
        Ok(summary.root_digest)
    }
}
