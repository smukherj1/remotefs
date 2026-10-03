//! Synchronous filesystem policy over a mounted session.

use std::sync::Arc;

use bytes::Bytes;
use rfs_common::error_context::{ResultContext, ResultContextError};
use rfs_common::session::{Inode, InodeId, IoCounters, NodeKind, Session, SessionError};
use thiserror::Error;

/// Errors from filesystem policy and session operations.
#[derive(Debug, Error)]
pub enum FilesystemError {
    /// An unexpected failure in the session hierarchy.
    #[error("filesystem internal error: {reason}")]
    InternalError {
        /// Description of the unexpected failure.
        reason: String,
    },
    /// The session lifecycle prevents the requested operation.
    #[error("current filesystem state does not permit this operation: {reason}")]
    FailedPreconditionError {
        /// State that prevented the operation.
        reason: String,
    },
    /// A requested inode or directory entry does not exist.
    #[error("not found: {reason}")]
    NotFound { reason: String },
    /// A directory operation received another node kind.
    #[error("not a directory: {reason}")]
    NotDirectory { reason: String },
    /// A regular-file operation received a directory.
    #[error("is a directory: {reason}")]
    IsDirectory { reason: String },
    /// A filesystem request has invalid arguments.
    #[error("invalid filesystem argument: {reason}")]
    InvalidArgument { reason: String },
    /// A visible inode has an impossible filesystem representation.
    #[error("inode {inode} has an invalid state: {reason}")]
    InvalidInode {
        /// Inode with the invalid state.
        inode: InodeId,
        /// Description of the violated invariant.
        reason: String,
    },
    /// Adds the owning filesystem operation to an error.
    #[error("{operation}: {source}")]
    Context {
        /// Operation that failed.
        operation: String,
        /// Original failure.
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

impl From<SessionError> for FilesystemError {
    /// Preserves an existing session failure category and recursively maps context.
    fn from(source: SessionError) -> Self {
        match source {
            SessionError::InternalError { reason } => Self::InternalError { reason },
            SessionError::FailedPreconditionError { reason } => {
                Self::FailedPreconditionError { reason }
            }
            SessionError::NotFound { reason } => Self::NotFound { reason },
            SessionError::NotDirectory { reason } => Self::NotDirectory { reason },
            SessionError::IsDirectory { reason } => Self::IsDirectory { reason },
            source @ SessionError::Database { .. } => Self::InternalError {
                reason: source.to_string(),
            },
            SessionError::Context { operation, source } => Self::Context {
                operation,
                source: Box::new(Self::from(*source)),
            },
        }
    }
}

/// Synchronous daemon filesystem policy over one mounted `Session`.
pub struct FilesystemService {
    /// Mounted-workspace facade that executes every namespace and content read.
    session: Arc<Session>,
}

impl FilesystemService {
    /// Creates a service after loading and validating the root directory.
    pub fn new(session: Arc<Session>) -> Result<Self, FilesystemError> {
        let filesystem = Self { session };
        filesystem
            .readdir(InodeId::ROOT)
            .with_context(|| "load mounted root directory".to_owned())?;
        Ok(filesystem)
    }

    /// Looks up one direct child of a directory.
    pub fn lookup_dir_child(
        &self,
        dir_inode: InodeId,
        child_name: &str,
    ) -> Result<Inode, FilesystemError> {
        self.session
            .lookup_child(dir_inode, child_name)
            .map_err(FilesystemError::from)
            .with_context(|| format!("look up `{child_name}` in directory inode {dir_inode}"))
    }

    /// Lists visible direct children of a directory in basename order.
    pub fn readdir(&self, inode: InodeId) -> Result<Vec<Inode>, FilesystemError> {
        self.session
            .list_directory(inode)
            .map_err(FilesystemError::from)
            .with_context(|| format!("read directory inode {inode}"))
    }

    /// Returns visible metadata for an inode without loading a directory.
    pub fn getattr(&self, inode: InodeId) -> Result<Inode, FilesystemError> {
        self.session
            .get_inode(inode)
            .map_err(FilesystemError::from)
            .with_context(|| format!("read attributes for inode {inode}"))
    }

    /// Returns the exact target for a symbolic-link inode.
    pub fn readlink(&self, inode: InodeId) -> Result<String, FilesystemError> {
        let node = self.getattr(inode)?;
        if node.kind != NodeKind::Symlink {
            return Err(FilesystemError::InvalidArgument {
                reason: format!("read symlink inode {inode}: actual kind is {:?}", node.kind),
            });
        }
        node.symlink_target
            .ok_or_else(|| FilesystemError::InvalidInode {
                inode,
                reason: format!("symlink `{}` has no target", node.name),
            })
    }

    /// Reads at most `size` bytes from a file starting at `offset`.
    pub fn read(&self, inode: InodeId, offset: u64, size: usize) -> Result<Bytes, FilesystemError> {
        self.session
            .read_range(inode, offset, size)
            .map_err(FilesystemError::from)
            .with_context(|| format!("read {size} bytes at offset {offset} from inode {inode}"))
    }

    /// Returns the session's point-in-time cache and download metrics.
    pub fn counters(&self) -> IoCounters {
        self.session.io_counters()
    }
}

#[cfg(test)]
mod tests {
    use rfs_common::session::SessionError;
    use rfs_common::testing::BlobResult;

    use super::test_support::{child, fixture_store, root_cause, service};
    use super::*;

    /// Every non-database `SessionError` keeps its category when converted.
    #[test]
    fn session_errors_keep_their_filesystem_category() {
        // Each row builds one session error and names the category it must map to.
        let reason = || "reason".to_owned();
        let rows: Vec<(SessionError, fn(&FilesystemError) -> bool)> = vec![
            (SessionError::InternalError { reason: reason() }, |e| {
                matches!(e, FilesystemError::InternalError { .. })
            }),
            (
                SessionError::FailedPreconditionError { reason: reason() },
                |e| matches!(e, FilesystemError::FailedPreconditionError { .. }),
            ),
            (SessionError::NotFound { reason: reason() }, |e| {
                matches!(e, FilesystemError::NotFound { .. })
            }),
            (SessionError::NotDirectory { reason: reason() }, |e| {
                matches!(e, FilesystemError::NotDirectory { .. })
            }),
            (SessionError::IsDirectory { reason: reason() }, |e| {
                matches!(e, FilesystemError::IsDirectory { .. })
            }),
        ];

        // All conversions succeed and land in the matching category.
        for (session_error, is_expected) in rows {
            let label = session_error.to_string();
            let converted = FilesystemError::from(session_error);
            assert!(is_expected(&converted), "wrong category for {label}");
        }
    }

    /// Session context becomes filesystem context around the original category and reason.
    #[test]
    fn session_context_is_preserved() {
        // A NotFound wrapped in one operation context.
        let converted = FilesystemError::from(SessionError::Context {
            operation: "load child".to_owned(),
            source: Box::new(SessionError::NotFound {
                reason: "no such entry".to_owned(),
            }),
        });

        // The context keeps its operation and the inner error keeps its reason.
        let FilesystemError::Context { operation, source } = converted else {
            panic!("expected context");
        };
        assert_eq!(operation, "load child");
        assert!(matches!(
            *source,
            FilesystemError::NotFound { ref reason } if reason == "no such entry"
        ));
    }

    /// Each failing service call keeps its category and names the operation that failed.
    #[test]
    fn service_failures_keep_category_and_name_the_operation() {
        // Open a service over the fixture: `file.txt`, `dir`, and `link` under the root.
        let (store, root) = fixture_store();
        let (_temp, _runtime, session, service) = service(store, root);
        let file = child(&service, InodeId::ROOT, "file.txt");
        let dir = child(&service, InodeId::ROOT, "dir");

        // Each call fails with the expected category and an operation-naming message.
        let cases: Vec<(FilesystemError, fn(&FilesystemError) -> bool, &str)> = vec![
            (
                service
                    .lookup_dir_child(InodeId::ROOT, "missing")
                    .unwrap_err(),
                |e| matches!(e, FilesystemError::NotFound { .. }),
                "look up `missing`",
            ),
            (
                service.readdir(file).unwrap_err(),
                |e| matches!(e, FilesystemError::NotDirectory { .. }),
                "read directory inode",
            ),
            (
                service.read(dir, 0, 1).unwrap_err(),
                |e| matches!(e, FilesystemError::IsDirectory { .. }),
                "read 1 bytes",
            ),
            (
                service.readlink(file).unwrap_err(),
                |e| matches!(e, FilesystemError::InvalidArgument { .. }),
                "read symlink inode",
            ),
        ];
        for (error, is_expected, operation) in &cases {
            assert!(is_expected(root_cause(error)), "wrong category: {error}");
            assert!(error.to_string().contains(operation), "{error}");
        }

        // A closed session reports a lifecycle failure for a read.
        session.close().unwrap();
        let closed = service.getattr(InodeId::ROOT).unwrap_err();
        assert!(matches!(
            root_cause(&closed),
            FilesystemError::FailedPreconditionError { .. }
        ));
        assert!(closed.to_string().contains("read attributes"));
    }

    /// A remote store failure while loading the root surfaces as an internal error.
    #[test]
    fn service_creation_fails_internally_when_the_root_cannot_be_loaded() {
        // The root digest's stream fails, so the constructor cannot load the root directory.
        let (store, root) = fixture_store();
        store.set_result(root.clone(), BlobResult::Fail);
        let (_temp, _runtime, session) = test_support::open_session(store, root);

        // Construction fails with an internal error that names the failed step.
        let error = match FilesystemService::new(session) {
            Err(error) => error,
            Ok(_) => panic!("service accepted an unreadable root"),
        };
        assert!(matches!(
            root_cause(&error),
            FilesystemError::InternalError { .. }
        ));
        assert!(error.to_string().contains("load mounted root directory"));
    }
}

/// Shared in-memory session fixture for filesystem and FUSE adapter unit tests.
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;

    use bytes::Bytes;
    use prost_types::Timestamp;
    use rfs_common::config::Config;
    use rfs_common::digest::Digest;
    use rfs_common::session::{InodeId, NodeKind, Session};
    use rfs_common::testing::InMemoryBlobStore;
    use rfs_common::tree::{
        DirectoryBuilder, DirectoryEntry, FileEntry, NodeMetadata, SymlinkEntry,
    };
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    use super::{FilesystemError, FilesystemService};

    /// Contents of `file.txt` in the fixture root.
    pub(crate) const FILE_CONTENTS: &[u8] = b"hello filesystem";

    /// Target of the fixture root's `link` symlink.
    pub(crate) const LINK_TARGET: &str = "file.txt";

    /// Builds a store holding a root with `file.txt`, an empty `dir`, and a symlink `link`.
    ///
    /// Returns the store and the root directory digest.
    pub(crate) fn fixture_store() -> (InMemoryBlobStore, Digest) {
        let file_digest = Digest::for_bytes(FILE_CONTENTS);
        let empty_dir = DirectoryBuilder::new().encode().expect("encode empty dir");
        let mut root = DirectoryBuilder::new();
        root.add_file(FileEntry {
            name: "file.txt".to_owned(),
            digest: file_digest.clone(),
            metadata: NodeMetadata::new(NodeKind::File, Some(0o644), Some(timestamp())),
        })
        .expect("add file");
        root.add_directory(DirectoryEntry {
            name: "dir".to_owned(),
            digest: empty_dir.digest.clone(),
            metadata: NodeMetadata::new(NodeKind::Directory, Some(0o755), Some(timestamp())),
        })
        .expect("add directory");
        root.add_symlink(SymlinkEntry {
            name: "link".to_owned(),
            target: LINK_TARGET.to_owned(),
            metadata: NodeMetadata::new(NodeKind::Symlink, None, Some(timestamp())),
        })
        .expect("add symlink");
        let root = root.encode().expect("encode root");
        let store = InMemoryBlobStore::new([
            (root.digest.clone(), root.bytes),
            (empty_dir.digest, empty_dir.bytes),
            (file_digest, Bytes::from_static(FILE_CONTENTS)),
        ]);
        (store, root.digest)
    }

    /// Opens a session rooted at `root` over `store` without building a service on top of it.
    pub(crate) fn open_session(
        store: InMemoryBlobStore,
        root: Digest,
    ) -> (TempDir, Runtime, Arc<Session>) {
        let temp = TempDir::new().expect("temporary home and mountpoint");
        let mountpoint = temp.path().join("mount");
        std::fs::create_dir(&mountpoint).expect("create mountpoint");
        let runtime = Runtime::new().expect("test runtime");
        let session = Session::open(
            Config {
                rfs_home: temp.path().join("home"),
            },
            root,
            mountpoint,
            Box::new(store),
            runtime.handle().clone(),
        )
        .expect("open session");
        (temp, runtime, Arc::new(session))
    }

    /// Opens a session and a service over `store` and `root`, which must be able to load the root.
    pub(crate) fn service(
        store: InMemoryBlobStore,
        root: Digest,
    ) -> (TempDir, Runtime, Arc<Session>, FilesystemService) {
        let (temp, runtime, session) = open_session(store, root);
        let service = FilesystemService::new(Arc::clone(&session)).expect("open service");
        (temp, runtime, session, service)
    }

    /// Returns the inode of `name` directly under `dir`.
    pub(crate) fn child(service: &FilesystemService, dir: InodeId, name: &str) -> InodeId {
        service
            .lookup_dir_child(dir, name)
            .expect("fixture child exists")
            .inode
    }

    /// Unwraps `Context` layers so assertions ignore how deeply an error is nested.
    pub(crate) fn root_cause(error: &FilesystemError) -> &FilesystemError {
        match error {
            FilesystemError::Context { source, .. } => root_cause(source),
            other => other,
        }
    }

    /// Fixed modification time for fixture entries.
    fn timestamp() -> Timestamp {
        Timestamp {
            seconds: 42,
            nanos: 0,
        }
    }
}
