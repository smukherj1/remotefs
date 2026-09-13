use std::fs;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rfs_common::cas::{Blob, BlobStore, CasClient, CasConfig, CasError, UploadStats};
use rfs_common::config::Config;
use rfs_common::session::{InodeId, Session};
use rfs_common::tree::DirectoryBuilder;
use rfs_common::upload::{UploadOptions, upload_local_directory};
use rfsd::filesystem::{FilesystemError, FilesystemService};

const LOCAL_CAS_ADDR: &str = "127.0.0.1:9092";

fn verify_prerequisite() {
    let addr: SocketAddr = LOCAL_CAS_ADDR.parse().unwrap();
    TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap_or_else(|error| {
        panic!(
            "PREREQUISITE FAILED: local bazel-remote CAS is not reachable at \
             grpc://{LOCAL_CAS_ADDR}: {error}. Run `task cas:up` before \
             `task test:integration:readonly`."
        )
    });
}

/// Returns whether a contextual filesystem failure retains an internal error.
fn is_internal_error(error: &FilesystemError) -> bool {
    match error {
        FilesystemError::InternalError { .. } => true,
        FilesystemError::Context { source, .. } => is_internal_error(source),
        FilesystemError::FailedPreconditionError { .. }
        | FilesystemError::NotFound { .. }
        | FilesystemError::NotDirectory { .. }
        | FilesystemError::IsDirectory { .. }
        | FilesystemError::InvalidArgument { .. }
        | FilesystemError::InvalidInode { .. } => false,
    }
}

/// Returns whether a contextual filesystem failure retains a missing-entry error.
fn is_not_found(error: &FilesystemError) -> bool {
    match error {
        FilesystemError::NotFound { .. } => true,
        FilesystemError::Context { source, .. } => is_not_found(source),
        FilesystemError::InternalError { .. }
        | FilesystemError::FailedPreconditionError { .. }
        | FilesystemError::NotDirectory { .. }
        | FilesystemError::IsDirectory { .. }
        | FilesystemError::InvalidArgument { .. }
        | FilesystemError::InvalidInode { .. } => false,
    }
}

/// Returns whether a contextual filesystem failure retains a non-directory error.
fn is_not_directory(error: &FilesystemError) -> bool {
    match error {
        FilesystemError::NotDirectory { .. } => true,
        FilesystemError::Context { source, .. } => is_not_directory(source),
        FilesystemError::InternalError { .. }
        | FilesystemError::FailedPreconditionError { .. }
        | FilesystemError::NotFound { .. }
        | FilesystemError::IsDirectory { .. }
        | FilesystemError::InvalidArgument { .. }
        | FilesystemError::InvalidInode { .. } => false,
    }
}

/// Returns whether a contextual filesystem failure retains a lifecycle precondition.
fn is_failed_precondition(error: &FilesystemError) -> bool {
    match error {
        FilesystemError::FailedPreconditionError { .. } => true,
        FilesystemError::Context { source, .. } => is_failed_precondition(source),
        FilesystemError::InternalError { .. }
        | FilesystemError::NotFound { .. }
        | FilesystemError::NotDirectory { .. }
        | FilesystemError::IsDirectory { .. }
        | FilesystemError::InvalidArgument { .. }
        | FilesystemError::InvalidInode { .. } => false,
    }
}

/// Blob store that fails every read to exercise Session's remote-read failure boundary.
struct FailingBlobStore;

#[async_trait]
impl BlobStore for FailingBlobStore {
    async fn find_missing_blobs(
        &self,
        _digests: &[rfs_common::digest::Digest],
    ) -> Result<Vec<rfs_common::digest::Digest>, CasError> {
        Err(CasError::InvalidInstanceName(
            "fake".to_owned(),
            "find_missing_blobs failure from FailingBlobStore".to_owned(),
        ))
    }

    async fn upload_blobs(&self, _blobs: Vec<Blob>) -> Result<UploadStats, CasError> {
        Err(CasError::InvalidInstanceName(
            "fake".to_owned(),
            "upload_blobs failure from FailingBlobStore".to_owned(),
        ))
    }

    async fn stream_blob(
        &self,
        _digest: &rfs_common::digest::Digest,
        _destination: &mut (dyn Write + Send),
    ) -> Result<(), CasError> {
        Err(CasError::InvalidInstanceName(
            "fake".to_owned(),
            "stream_blob failure from FailingBlobStore".to_owned(),
        ))
    }
}

#[tokio::test]
async fn uploaded_fixture_is_read_lazily_through_verified_cache() -> Result<()> {
    verify_prerequisite();
    let temp = tempfile::tempdir().context("create read-only integration root")?;
    let source = temp.path().join("source");
    let nested = source.join("nested");
    fs::create_dir_all(&nested).context("create integration fixture directories")?;
    fs::write(source.join("root.txt"), b"root contents").context("write root fixture file")?;
    fs::write(nested.join("child.txt"), b"child contents").context("write nested fixture file")?;

    let instance = format!("remotefs/readonly-integration/{}", std::process::id());
    let cas_config = CasConfig::new(format!("grpc://{LOCAL_CAS_ADDR}"), instance)?;
    let uploader = CasClient::connect(cas_config.clone()).await?;
    let summary = upload_local_directory(&uploader, &source, UploadOptions::default()).await?;

    let mountpoint = temp.path().join("mount");
    fs::create_dir(&mountpoint).context("create integration mountpoint")?;
    let reader = CasClient::connect(cas_config).await?;
    let session = std::sync::Arc::new(Session::open(
        Config {
            rfs_home: temp.path().join("rfs-home"),
        },
        summary.root_digest.clone(),
        mountpoint,
        Box::new(reader),
        tokio::runtime::Handle::current(),
    )?);
    let workflow_session = std::sync::Arc::clone(&session);
    let counters = tokio::task::spawn_blocking(move || -> Result<_> {
        let filesystem = FilesystemService::new(std::sync::Arc::clone(&workflow_session))?;
        let nested = filesystem.lookup_dir_child(InodeId::ROOT, "nested")?;
        let child = filesystem.lookup_dir_child(nested.inode, "child.txt")?;
        assert_eq!(
            filesystem.read(child.inode, 0, 64)?.as_ref(),
            b"child contents"
        );

        // A missing entry and a directory operation on a file retain their Session categories.
        let missing_error = filesystem
            .lookup_dir_child(InodeId::ROOT, "missing")
            .expect_err("reject a missing directory entry");
        assert!(is_not_found(&missing_error));
        let directory_error = filesystem
            .readdir(child.inode)
            .expect_err("reject listing a regular file");
        assert!(is_not_directory(&directory_error));

        let counters = filesystem.counters();
        workflow_session.close()?;
        let closed_error = filesystem
            .getattr(InodeId::ROOT)
            .expect_err("reject attributes after session close");
        // A service read after close returns the lifecycle precondition category.
        assert!(is_failed_precondition(&closed_error));
        Ok(counters)
    })
    .await??;
    assert_eq!(counters.directory_downloads, 2);
    assert_eq!(counters.file_downloads, 1);

    Ok(())
}

#[tokio::test]
async fn service_returns_internal_error_when_cas_read_fails() -> Result<()> {
    // An unloaded root and failing store cause the public constructor's root CAS read to fail.
    let temporary = tempfile::tempdir().context("create fake CAS integration root")?;
    let mountpoint = temporary.path().join("mount");
    fs::create_dir(&mountpoint).context("create fake CAS mountpoint")?;
    let root = DirectoryBuilder::new()
        .encode()
        .context("encode fake CAS root")?;
    let session = std::sync::Arc::new(Session::open(
        Config {
            rfs_home: temporary.path().join("rfs-home"),
        },
        root.digest,
        mountpoint,
        Box::new(FailingBlobStore),
        tokio::runtime::Handle::current(),
    )?);

    let service_session = std::sync::Arc::clone(&session);
    let error =
        match tokio::task::spawn_blocking(move || FilesystemService::new(service_session)).await? {
            Err(error) => error,
            Ok(_) => return Err(anyhow::anyhow!("accepted fake CAS root read")),
        };
    // A failed root-directory CAS read produces an internal filesystem error.
    assert!(is_internal_error(&error));
    Ok(())
}
