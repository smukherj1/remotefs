//! Synchronous filesystem policy over a mounted session.

use std::sync::Arc;

use bytes::Bytes;
use rfs_common::error_context::{ResultContext, ResultContextError};
use rfs_common::session::{Inode, InodeId, IoCounters, NodeKind, Session, SessionError};
use thiserror::Error;

/// Errors from filesystem policy and session operations.
#[derive(Debug, Error)]
pub enum FilesystemError {
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
    /// A session operation for an inode failed.
    #[error("filesystem operation for inode {inode} failed: {source}")]
    Session {
        /// Inode that was the subject of the operation.
        inode: InodeId,
        /// Preserved session failure.
        #[source]
        source: SessionError,
    },
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
            .map_err(|source| filesystem_error(dir_inode, source))
            .with_context(|| format!("look up `{child_name}` in directory inode {dir_inode}"))
    }

    /// Lists visible direct children of a directory in basename order.
    pub fn readdir(&self, inode: InodeId) -> Result<Vec<Inode>, FilesystemError> {
        self.session
            .list_directory(inode)
            .map_err(|source| filesystem_error(inode, source))
            .with_context(|| format!("read directory inode {inode}"))
    }

    /// Returns visible metadata for an inode without loading a directory.
    pub fn getattr(&self, inode: InodeId) -> Result<Inode, FilesystemError> {
        self.session
            .get_inode(inode)
            .map_err(|source| filesystem_error(inode, source))
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
            .map_err(|source| filesystem_error(inode, source))
            .with_context(|| format!("read {size} bytes at offset {offset} from inode {inode}"))
    }

    /// Returns the session's point-in-time cache and download metrics.
    pub fn counters(&self) -> IoCounters {
        self.session.io_counters()
    }
}

fn filesystem_error(inode: InodeId, source: SessionError) -> FilesystemError {
    match source {
        SessionError::Context { operation, source } => FilesystemError::Context {
            operation,
            source: Box::new(filesystem_error(inode, *source)),
        },
        SessionError::NotFound { reason } => FilesystemError::NotFound { reason },
        SessionError::NotDirectory { reason } => FilesystemError::NotDirectory { reason },
        SessionError::IsDirectory { reason } => FilesystemError::IsDirectory { reason },
        source => FilesystemError::Session { inode, source },
    }
}
