//! Synchronous remote-aware orchestration for the read-only filesystem.
//!
//! SQLite-backed `Session` state is the authoritative namespace. This service
//! retains only its CAS dependency, counters, and per-digest locks that
//! deduplicate in-progress downloads.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use rfs_common::cas::{BlobStore, CasError};
use rfs_common::digest::{Digest, DigestError, EMPTY_DIGEST};
use rfs_common::error_context::{ResultContext, ResultContextError};
use rfs_common::reapi::remote_execution::{Directory, NodeProperties};
use rfs_common::session::{
    InodeId, LocalRead, Lookup, Node, NodeKind, NodeTime, RemoteChild, RemoteContent, Session,
    SessionError,
};
use rfs_common::tree::{TreeError, decode_directory};
use thiserror::Error;
use tokio::runtime::Handle;

/// Observable lazy-fetch and cache behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadOnlyCounters {
    /// Directory objects streamed from CAS.
    pub directory_downloads: u64,
    /// Directory objects decoded from admitted local cache entries.
    pub directory_cache_hits: u64,
    /// File objects streamed from CAS.
    pub blob_downloads: u64,
    /// File ranges served from admitted local cache entries.
    pub blob_cache_hits: u64,
}

#[derive(Default)]
struct AtomicCounters {
    directory_downloads: AtomicU64,
    directory_cache_hits: AtomicU64,
    blob_downloads: AtomicU64,
    blob_cache_hits: AtomicU64,
    cached_blobs: AtomicU64,
}

/// Errors from remote directory orchestration and verified immutable reads.
#[derive(Debug, Error)]
pub enum FilesystemError {
    /// A digest-bearing REAPI node omitted its digest.
    #[error("remote {kind} entry `{name}` has no digest")]
    MissingDigest { kind: &'static str, name: String },
    /// A REAPI digest could not be converted to the domain digest.
    #[error("invalid digest for remote entry `{name}`: {source}")]
    InvalidDigest {
        name: String,
        #[source]
        source: DigestError,
    },
    /// A REAPI node timestamp is outside the supported range.
    #[error("remote entry `{name}` has an invalid modification time")]
    InvalidTimestamp { name: String },
    /// Remote blob transfer failed.
    #[error("download {object} {digest} from CAS failed: {source}")]
    Cas {
        object: &'static str,
        digest: Digest,
        #[source]
        source: Box<CasError>,
    },
    /// A complete directory object failed semantic decoding.
    #[error("validate remote directory {digest} failed: {source}")]
    Directory {
        digest: Digest,
        #[source]
        source: TreeError,
    },
    /// A local session operation failed.
    #[error("local session operation for inode {inode} failed: {source}")]
    Session {
        inode: InodeId,
        #[source]
        source: SessionError,
    },
    /// An inode has an invalid state.
    #[error("inode {inode} has an invalid state: {reason}")]
    InvalidInode { inode: InodeId, reason: String },
    /// The mutex serializing the CAS client was poisoned.
    #[error("CAS client lock is poisoned while downloading {digest}")]
    CasLock { digest: Digest },
    /// A per-digest download-coordination mutex was poisoned.
    #[error("download lock is poisoned for {digest}: {details}")]
    DownloadLock { digest: Digest, details: String },
    /// Additional owning-operation context for another filesystem error.
    #[error("{operation}: {source}")]
    Context {
        operation: String,
        #[source]
        source: Box<FilesystemError>,
    },
}

impl ResultContextError for FilesystemError {
    fn with_context(self, operation: String) -> Self {
        Self::Context {
            operation,
            source: Box::new(self),
        }
    }
}

/// Remote-aware synchronous filesystem service.
pub struct FilesystemService<S> {
    // TODO: Remove mutex. Clone client before using.
    store: Mutex<S>,
    session: Arc<Session>,
    runtime: Handle,
    download_locks: Mutex<HashMap<Digest, Arc<Mutex<()>>>>,
    counters: AtomicCounters,
}

impl<S: BlobStore + Send> FilesystemService<S> {
    /// Validates the root directory while leaving descendants and file data lazy.
    ///
    /// The caller must run construction on a blocking thread. Async CAS
    /// transport is bridged internally through `runtime`.
    pub fn mount(
        store: S,
        session: Arc<Session>,
        runtime: Handle,
    ) -> Result<Self, FilesystemError> {
        let cached_blobs = session
            .cached_blob_count()
            .map_err(|source| session_error(InodeId::ROOT, source))?;
        let filesystem = Self {
            store: Mutex::new(store),
            session,
            runtime,
            download_locks: Mutex::new(HashMap::new()),
            counters: AtomicCounters {
                cached_blobs: AtomicU64::new(cached_blobs),
                ..AtomicCounters::default()
            },
        };
        filesystem.ensure_directory(InodeId::ROOT)?;
        Ok(filesystem)
    }

    /// Looks up one direct child without fetching child directory metadata.
    pub fn lookup(&self, parent: InodeId, name: &str) -> Result<Node, FilesystemError> {
        let mut materialized = false;
        let mut last_digest = EMPTY_DIGEST.clone();

        loop {
            match self
                .session
                .lookup(parent, name)
                .map_err(|source| session_error(parent, source))?
            {
                Lookup::Ready(node) => return Ok(node),
                Lookup::NeedsMaterialization { digest } if !materialized => {
                    tracing::info!(
                        "Materializing directory inode {} with digest {}",
                        parent,
                        digest
                    );
                    self.materialize_remote_directory(parent, &digest).with_context(|| format!("failed to materialize node named {} with digest {} in parent inode {}", name, digest, parent))?;
                    materialized = true;
                    last_digest = digest;
                }
                Lookup::NeedsMaterialization { digest } => {
                    return Err(FilesystemError::InvalidInode {
                        inode: parent,
                        reason: format!(
                            "child node {} in inode {} needs materialization as digest {} despite just being materialized as digest {}",
                            name, parent, digest, last_digest
                        ),
                    });
                }
            }
        }
    }

    /// Lists one directory in stable namespace order.
    pub fn readdir(&self, inode: InodeId) -> Result<Vec<Node>, FilesystemError> {
        self.ensure_directory(inode)
    }

    /// Returns current visible metadata for one inode.
    pub fn getattr(&self, inode: InodeId) -> Result<Node, FilesystemError> {
        self.session
            .node(inode)
            .map_err(|source| session_error(inode, source))
    }

    /// Returns the exact target for a visible symlink.
    pub fn readlink(&self, inode: InodeId) -> Result<String, FilesystemError> {
        let node = self.getattr(inode)?;
        if node.kind != NodeKind::Symlink {
            return Err(session_error(
                inode,
                SessionError::WrongKind {
                    inode,
                    expected: NodeKind::Symlink,
                    actual: node.kind,
                },
            ));
        }
        node.symlink_target
            .ok_or_else(|| FilesystemError::InvalidInode {
                inode,
                reason: format!("symlink {} is missing a target", node.name),
            })
    }

    /// Reads a byte range after ensuring complete immutable content is admitted.
    pub fn read(&self, inode: InodeId, offset: u64, size: usize) -> Result<Bytes, FilesystemError> {
        let read_result = self
            .session
            .read_range(inode, offset, size)
            .map_err(|source| session_error(inode, source))?;
        if let LocalRead::Ready(bytes) = read_result {
            self.counters
                .blob_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(bytes);
        }
        let LocalRead::NeedsDownload { digest } = read_result else {
            return Err(FilesystemError::InvalidInode {
                inode,
                reason: format!(
                    "inode read returned unhandled local result: {}",
                    read_result
                ),
            });
        };
        if self.ensure_blob(&digest, "file blob")? {
            self.counters.blob_downloads.fetch_add(1, Ordering::Relaxed);
            self.counters.cached_blobs.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters
                .blob_cache_hits
                .fetch_add(1, Ordering::Relaxed);
        }
        let LocalRead::Ready(bytes) = self
            .session
            .read_range(inode, offset, size)
            .map_err(|source| session_error(inode, source))
            .with_context(|| format!("reading blob {} after downloading", digest))?
        else {
            return Err(FilesystemError::InvalidInode {
                inode,
                reason: format!(
                    "inode backed by blob {} failed to register in blob cache despite downloading it",
                    digest
                ),
            });
        };
        Ok(bytes)
    }

    /// Returns a point-in-time snapshot of read-only fetch counters.
    pub fn counters(&self) -> ReadOnlyCounters {
        ReadOnlyCounters {
            directory_downloads: self.counters.directory_downloads.load(Ordering::Relaxed),
            directory_cache_hits: self.counters.directory_cache_hits.load(Ordering::Relaxed),
            blob_downloads: self.counters.blob_downloads.load(Ordering::Relaxed),
            blob_cache_hits: self.counters.blob_cache_hits.load(Ordering::Relaxed),
        }
    }

    /// Returns the current number of admitted objects observed by this daemon.
    pub fn cached_blobs(&self) -> u64 {
        self.counters.cached_blobs.load(Ordering::Relaxed)
    }

    fn ensure_directory(&self, inode: InodeId) -> Result<Vec<Node>, FilesystemError> {
        // TODO: wth is this loop.
        loop {
            match self
                .session
                .list_directory(inode)
                .map_err(|source| session_error(inode, source))?
            {
                Lookup::Ready(nodes) => return Ok(nodes),
                Lookup::NeedsMaterialization { digest } => {
                    self.materialize_remote_directory(inode, &digest)?;
                }
            }
        }
    }

    fn materialize_remote_directory(
        &self,
        inode: InodeId,
        digest: &Digest,
    ) -> Result<(), FilesystemError> {
        let bytes = match self
            .session
            .read_blob(digest)
            .map_err(|source| session_error(inode, source))?
        {
            Some(bytes) => {
                self.counters
                    .directory_cache_hits
                    .fetch_add(1, Ordering::Relaxed);
                bytes
            }
            None => {
                if self.ensure_blob(digest, "directory")? {
                    self.counters
                        .directory_downloads
                        .fetch_add(1, Ordering::Relaxed);
                    self.counters.cached_blobs.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.counters
                        .directory_cache_hits
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.session
                    .read_blob(digest)
                    .map_err(|source| session_error(inode, source))?
                    .ok_or_else(|| FilesystemError::InvalidInode {
                        inode,
                        reason: format!("directory node {digest} remained missing after admission"),
                    })?
            }
        };
        let directory = decode_directory(digest.clone(), bytes).map_err(|source| {
            FilesystemError::Directory {
                digest: digest.clone(),
                source,
            }
        })?;
        let children = remote_children(&directory)?;
        self.session
            .materialize_directory(inode, digest, children)
            .map_err(|source| session_error(inode, source))?;
        Ok(())
    }

    // TODO: Ensure blob should first check if blob exists in cache before locking and return
    // cache hit (i.e. false) if it does. Otherwise, it should acquire lock and download blob.
    // Then callers don't need to do the unlocked check.
    fn ensure_blob(&self, digest: &Digest, object: &'static str) -> Result<bool, FilesystemError> {
        let lock = self.download_lock(digest)?;
        let _guard = lock.lock().map_err(|_| FilesystemError::DownloadLock {
            digest: digest.clone(),
            details: "while locking the digest specific lock".to_string(),
        })?;
        if self.session.exists(digest) {
            return Ok(false);
        }
        let mut writer = self
            .session
            .start_blob_download(digest)
            .map_err(|source| session_error(InodeId::ROOT, source))
            .with_context(|| {
                format!(
                    "starting download for blob {} that missed the cache",
                    digest
                )
            })?;
        let mut store = self.store.lock().map_err(|_| FilesystemError::CasLock {
            digest: digest.clone(),
        })?;
        self.runtime
            .block_on(store.stream_blob(digest, &mut writer))
            .map_err(|source| FilesystemError::Cas {
                object,
                digest: digest.clone(),
                source: Box::new(source),
            })
            .with_context(|| "streaming blob contents from CAS".to_string())?;
        self.session
            .finalize_blob(writer)
            .map_err(|source| session_error(InodeId::ROOT, source))
            .with_context(|| format!("finalizing downloaded blob {}", digest))?;
        Ok(true)
    }

    fn download_lock(&self, digest: &Digest) -> Result<Arc<Mutex<()>>, FilesystemError> {
        Ok(self
            .download_locks
            .lock()
            .map_err(|_| FilesystemError::DownloadLock {
                digest: digest.clone(),
                details: "acquiring lock on the lookup containing digest download locks"
                    .to_string(),
            })?
            .entry(digest.clone())
            .or_default()
            .clone())
    }
}

fn remote_children(directory: &Directory) -> Result<Vec<RemoteChild>, FilesystemError> {
    let mut children = Vec::with_capacity(
        directory.files.len() + directory.directories.len() + directory.symlinks.len(),
    );
    for file in &directory.files {
        let (mode, mtime) = properties(&file.name, file.node_properties.as_ref())?;
        children.push(RemoteChild {
            name: file.name.clone(),
            content: RemoteContent::File(required_digest(
                "file",
                &file.name,
                file.digest.as_ref(),
            )?),
            mode,
            mtime,
        });
    }
    for directory in &directory.directories {
        children.push(RemoteChild {
            name: directory.name.clone(),
            content: RemoteContent::Directory(required_digest(
                "directory",
                &directory.name,
                directory.digest.as_ref(),
            )?),
            mode: None,
            mtime: None,
        });
    }
    for symlink in &directory.symlinks {
        let (mode, mtime) = properties(&symlink.name, symlink.node_properties.as_ref())?;
        children.push(RemoteChild {
            name: symlink.name.clone(),
            content: RemoteContent::Symlink(symlink.target.clone()),
            mode,
            mtime,
        });
    }
    Ok(children)
}

fn properties(
    name: &str,
    properties: Option<&NodeProperties>,
) -> Result<(Option<u32>, Option<NodeTime>), FilesystemError> {
    let Some(properties) = properties else {
        return Ok((None, None));
    };
    let mtime = properties
        .mtime
        .as_ref()
        .map(|timestamp| {
            u32::try_from(timestamp.nanos)
                .ok()
                .and_then(|nanos| NodeTime::new(timestamp.seconds, nanos))
                .ok_or_else(|| FilesystemError::InvalidTimestamp {
                    name: name.to_owned(),
                })
        })
        .transpose()?;
    Ok((properties.unix_mode, mtime))
}

fn required_digest(
    kind: &'static str,
    name: &str,
    digest: Option<&rfs_common::reapi::remote_execution::Digest>,
) -> Result<Digest, FilesystemError> {
    let digest = digest.ok_or_else(|| FilesystemError::MissingDigest {
        kind,
        name: name.to_owned(),
    })?;
    Digest::from_reapi(digest).map_err(|source| FilesystemError::InvalidDigest {
        name: name.to_owned(),
        source,
    })
}

fn session_error(inode: InodeId, source: SessionError) -> FilesystemError {
    FilesystemError::Session { inode, source }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;

    use async_trait::async_trait;
    use googletest::prelude::*;
    use rfs_common::cas::{Blob, CasOperation, UploadStats};
    use rfs_common::config::Config;
    use rfs_common::logging;
    use rfs_common::tree::{DirectoryBuilder, FileEntry, NodeKind as TreeNodeKind, NodeMetadata};

    use super::*;

    struct FakeStore {
        blobs: HashMap<Digest, Bytes>,
        downloads: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BlobStore for FakeStore {
        async fn find_missing_blobs(
            &mut self,
            _digests: &[Digest],
        ) -> Result<Vec<Digest>, CasError> {
            unreachable!("read-only service does not check upload existence")
        }

        async fn upload_blobs(&mut self, _blobs: Vec<Blob>) -> Result<UploadStats, CasError> {
            unreachable!("read-only service does not upload")
        }

        async fn stream_blob(
            &mut self,
            digest: &Digest,
            destination: &mut (dyn Write + Send),
        ) -> Result<(), CasError> {
            self.downloads.fetch_add(1, Ordering::SeqCst);
            let bytes = self.blobs.get(digest).ok_or_else(|| CasError::BlobStatus {
                operation: CasOperation::BatchReadBlobs,
                digest: digest.clone(),
                message: "not found".into(),
            })?;
            destination
                .write_all(bytes)
                .map_err(|source| CasError::StreamWrite {
                    digest: digest.clone(),
                    source,
                })
        }
    }

    struct Fixture {
        root: Digest,
        file: Digest,
        blobs: HashMap<Digest, Bytes>,
    }

    fn fixture() -> Fixture {
        let file_bytes = Bytes::from_static(b"hello from remote");
        let file = Digest::for_bytes(&file_bytes);
        let mut root = DirectoryBuilder::new();
        root.add_file(FileEntry {
            name: "hello.txt".into(),
            digest: file.clone(),
            metadata: NodeMetadata::new(TreeNodeKind::File, Some(0o640), None),
        })
        .unwrap();
        let root = root.encode().unwrap();
        Fixture {
            root: root.digest.clone(),
            file: file.clone(),
            blobs: HashMap::from([(root.digest, root.bytes), (file, file_bytes)]),
        }
    }

    fn session(temp: &tempfile::TempDir, root: &Digest) -> Arc<Session> {
        let mountpoint = temp.path().join("mount");
        if !mountpoint.exists() {
            std::fs::create_dir(&mountpoint).unwrap();
        }
        Arc::new(
            Session::open(
                Config {
                    rfs_home: temp.path().join("home"),
                },
                root.clone(),
                mountpoint,
            )
            .unwrap(),
        )
    }

    #[test]
    fn concurrent_missing_reads_use_one_remote_stream() {
        let fixture = fixture();
        let temp = tempfile::tempdir().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let downloads = Arc::new(AtomicUsize::new(0));
        let session = session(&temp, &fixture.root);
        let filesystem = Arc::new(
            FilesystemService::mount(
                FakeStore {
                    blobs: fixture.blobs,
                    downloads: Arc::clone(&downloads),
                },
                Arc::clone(&session),
                runtime.handle().clone(),
            )
            .unwrap(),
        );
        let file = filesystem.lookup(InodeId::ROOT, "hello.txt").unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let left = {
            let filesystem = Arc::clone(&filesystem);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                filesystem.read(file.inode, 0, 5).unwrap()
            })
        };
        let right = {
            let filesystem = Arc::clone(&filesystem);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                filesystem.read(file.inode, 6, 4).unwrap()
            })
        };
        barrier.wait();
        assert_eq!(left.join().unwrap(), Bytes::from_static(b"hello"));
        assert_eq!(right.join().unwrap(), Bytes::from_static(b"from"));
        assert_eq!(
            downloads.load(Ordering::SeqCst),
            2,
            "one root directory plus one coalesced file stream"
        );
        assert_eq!(filesystem.counters().blob_downloads, 1);
        drop(filesystem);
        session.close().unwrap();
    }

    #[test]
    fn unified_cache_is_reused_for_directory_and_file_across_sessions() {
        logging::init_test();
        let fixture = fixture();
        let temp = tempfile::tempdir().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let first_downloads = Arc::new(AtomicUsize::new(0));
        let first_session = session(&temp, &fixture.root);
        let first = FilesystemService::mount(
            FakeStore {
                blobs: fixture.blobs.clone(),
                downloads: Arc::clone(&first_downloads),
            },
            Arc::clone(&first_session),
            runtime.handle().clone(),
        )
        .unwrap();
        let file = first.lookup(InodeId::ROOT, "hello.txt").unwrap();
        assert_eq!(
            first.read(file.inode, 0, usize::MAX).unwrap(),
            Bytes::from_static(b"hello from remote")
        );
        assert_eq!(first_downloads.load(Ordering::SeqCst), 2);
        drop(first);
        first_session.close().unwrap();

        let second_downloads = Arc::new(AtomicUsize::new(0));
        let second_session = session(&temp, &fixture.root);
        let second = FilesystemService::mount(
            FakeStore {
                blobs: fixture.blobs,
                downloads: Arc::clone(&second_downloads),
            },
            Arc::clone(&second_session),
            runtime.handle().clone(),
        )
        .unwrap();
        let file = second.lookup(InodeId::ROOT, "hello.txt").unwrap();
        assert_eq!(
            second.read(file.inode, 0, usize::MAX).unwrap(),
            Bytes::from_static(b"hello from remote")
        );
        assert_eq!(second_downloads.load(Ordering::SeqCst), 0);
        assert_eq!(second.counters().directory_cache_hits, 1);
        assert_eq!(second.counters().blob_cache_hits, 1);
        assert_eq!(second.cached_blobs(), 2);
        drop(second);
        second_session.close().unwrap();
    }

    #[test]
    fn invalid_remote_bytes_are_rejected_before_admission() {
        let mut fixture = fixture();
        fixture
            .blobs
            .insert(fixture.file.clone(), Bytes::from_static(b"wrong"));
        let temp = tempfile::tempdir().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let session = session(&temp, &fixture.root);
        let filesystem = FilesystemService::mount(
            FakeStore {
                blobs: fixture.blobs,
                downloads: Arc::new(AtomicUsize::new(0)),
            },
            Arc::clone(&session),
            runtime.handle().clone(),
        )
        .unwrap();
        let file = filesystem.lookup(InodeId::ROOT, "hello.txt").unwrap();
        let Err(read_err) = filesystem.read(file.inode, 0, 1) else {
            panic!("Lookup of corrupted blob succeeded, expected error.");
        };
        read_err.to_string().contains("blob verification failed");
        assert_that!(
            read_err.to_string(),
            contains_substring("blob verification failed")
        );
        drop(filesystem);
        session.close().unwrap();
    }
}
