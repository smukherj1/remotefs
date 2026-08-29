//! Concrete local-session facade for durable namespace state and immutable blobs.

mod cache;
mod overlay;
mod store;

use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::runtime::Handle;
use uuid::Uuid;

use crate::cas::BlobStore;
use crate::config::Config;
use crate::digest::Digest;
use crate::error_context::{ResultContext, ResultContextError};
use crate::tree::decode_directory;

pub use crate::tree::NodeKind;
use cache::CachedBlobStore;
use overlay::OverlayStore;
use store::{Inode as StoreInode, SessionStore};

/// Session-stable inode identity that is positive and representable by SQLite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InodeId(i64);

impl InodeId {
    /// Default inode ID.
    pub const INVALID: Self = Self(0);
    /// Root inode shared by every mounted workspace.
    pub const ROOT: Self = Self(1);

    /// Validates a raw inode supplied by an external adapter.
    pub fn new(value: u64) -> Result<Self, SessionError> {
        let value = i64::try_from(value).map_err(|_| {
            internal_error(format!(
                "validate external inode value {value}: outside the supported range"
            ))
        })?;
        Ok(Self(value))
    }

    /// Returns the raw inode value used by FUSE.
    pub fn get(self) -> u64 {
        u64::try_from(self.0).expect("validated inode is positive")
    }
    pub(super) fn sqlite(self) -> i64 {
        self.0
    }
    pub(super) fn from_sqlite(value: i64) -> Result<Self, SessionError> {
        if value <= 0 {
            return Err(internal_error(format!(
                "decode stored inode {value}: value is not positive"
            )));
        }
        Ok(Self(value))
    }
}

impl fmt::Display for InodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(f)
    }
}

/// Transport-independent normalized node modification time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeTime {
    seconds: i64,
    nanos: u32,
}

impl NodeTime {
    /// Unix epoch, used as the effective value for absent remote mtimes.
    pub const UNIX_EPOCH: Self = Self {
        seconds: 0,
        nanos: 0,
    };
    /// Constructs a time in the range supported by REAPI timestamps.
    pub fn new(seconds: i64, nanos: u32) -> Option<Self> {
        ((MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS).contains(&seconds)
            && nanos < 1_000_000_000)
            .then_some(Self { seconds, nanos })
    }
    /// Whole seconds relative to the Unix epoch.
    pub fn seconds(self) -> i64 {
        self.seconds
    }
    /// Normalized nanosecond fraction.
    pub fn nanos(self) -> u32 {
        self.nanos
    }
}

/// Visible, validated inode metadata exposed by the session facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inode {
    /// Session-stable inode identity.
    pub inode: InodeId,
    /// Parent identity; root refers to itself.
    pub parent: InodeId,
    /// UTF-8 basename, empty only for root.
    pub name: String,
    /// Visible filesystem node kind.
    pub kind: NodeKind,
    /// Effective content or symlink-target size.
    pub size: u64,
    /// Effective supported Unix mode bits.
    pub mode: u32,
    /// Effective modification time.
    pub mtime: NodeTime,
    /// Exact target for symlinks and `None` for other kinds.
    pub symlink_target: Option<String>,
}

/// Durable session lifecycle stored in SQLite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionLifecycle {
    Initializing,
    Active,
    Closed,
}
impl fmt::Display for SessionLifecycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Initializing => "initializing",
            Self::Active => "active",
            Self::Closed => "closed",
        })
    }
}

/// Immutable facts established while opening a writable session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionInfo {
    /// Immutable root directory digest.
    pub root_digest: Digest,
    /// Path where the active fuse session is mounted.
    pub mountpoint: PathBuf,
    /// Daemon process ID.
    pub daemon_pid: u32,
    /// Path to the Unix control socket.
    pub control_endpoint: PathBuf,
    /// Path to the retained daemon log file.
    pub log_path: PathBuf,
}

/// Metrics for remote blob downloads and verified-cache hits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoCounters {
    /// Directory objects streamed from remote storage.
    pub directory_downloads: u64,
    /// Directory objects served from the verified cache.
    pub directory_cache_hits: u64,
    /// File objects streamed from remote storage.
    pub file_downloads: u64,
    /// File objects served from the verified cache.
    pub file_cache_hits: u64,
}

/// Failures owned by the local session hierarchy.
#[derive(Debug, Error)]
pub enum SessionError {
    /// Unexpected local-session failure.
    #[error("session internal error: {reason}")]
    InternalError { reason: String },
    /// Current state blocks the operation.
    #[error("current session state does not permit this operation: {reason}")]
    FailedPreconditionError { reason: String },
    /// Requested inode or entry is not visible.
    #[error("not found: {reason}")]
    NotFound { reason: String },
    /// Directory operation received another node kind.
    #[error("not a directory: {reason}")]
    NotDirectory { reason: String },
    /// Regular-file operation received a directory.
    #[error("is a directory: {reason}")]
    IsDirectory { reason: String },
    /// A SQLite API operation failed.
    #[error("SQLite {operation} failed on db {dbpath}: {source}")]
    Database {
        operation: String,
        dbpath: PathBuf,
        #[source]
        source: rusqlite::Error,
    },
    #[error("{operation}: {source}")]
    Context {
        operation: String,
        #[source]
        source: Box<SessionError>,
    },
}

/// Builds an unexpected session failure with complete diagnostic context.
pub(super) fn internal_error(reason: impl Into<String>) -> SessionError {
    SessionError::InternalError {
        reason: reason.into(),
    }
}

/// Builds an error for an operation blocked by current session state.
pub(super) fn failed_precondition(reason: impl Into<String>) -> SessionError {
    SessionError::FailedPreconditionError {
        reason: reason.into(),
    }
}

/// Builds an error for a missing visible inode or directory entry.
pub(super) fn not_found(reason: impl Into<String>) -> SessionError {
    SessionError::NotFound {
        reason: reason.into(),
    }
}

/// Builds an error for a directory operation on another node kind.
pub(super) fn not_directory(reason: impl Into<String>) -> SessionError {
    SessionError::NotDirectory {
        reason: reason.into(),
    }
}

/// Builds an error for a regular-file operation on a directory.
pub(super) fn is_directory(reason: impl Into<String>) -> SessionError {
    SessionError::IsDirectory {
        reason: reason.into(),
    }
}
impl ResultContextError for SessionError {
    fn with_context(self, operation: String) -> Self {
        Self::Context {
            operation,
            source: Box::new(self),
        }
    }
}

/// Session-owned internal download and cache metrics.
struct InternalIoCounters {
    directory_downloads: AtomicU64,
    directory_cache_hits: AtomicU64,
    file_downloads: AtomicU64,
    file_cache_hits: AtomicU64,
}

/// Concrete synchronous facade for one mounted workspace.
pub struct Session {
    /// Immutable mounted root directory digest.
    root_digest: Digest,
    /// Path where the root of this session is mounted.
    mountpoint: PathBuf,
    /// Fixed paths beneath the configured RemoteFS home.
    layout: SessionLayout,
    /// Durable session lifecycle and merged namespace repository.
    store: SessionStore,
    /// Synchronous remote read-through cache.
    cached_blob_store: CachedBlobStore,
    /// Local storage for editable file content.
    overlay: OverlayStore,
    /// Stable lock retained until the session is dropped.
    _session_lock: SessionLock,
    /// Strong references to per-inode operation locks.
    inode_locks: Mutex<HashMap<InodeId, Arc<Mutex<()>>>>,
    /// Atomic download and cache-hit metrics.
    counters: InternalIoCounters,
}

impl Session {
    /// Opens a fresh writable session and acquires exclusive `RFS_HOME` ownership.
    pub fn open(
        config: Config,
        root_digest: Digest,
        mountpoint: impl AsRef<Path>,
        blob_store: Box<dyn BlobStore>,
        runtime: Handle,
    ) -> Result<Self, SessionError> {
        let mountpoint = canonicalize_mountpoint(mountpoint.as_ref()).with_context(|| {
            format!(
                "canonicalize session mountpoint {}",
                mountpoint.as_ref().display()
            )
        })?;
        ensure_writable_home(&config).with_context(|| "set up writable session home".to_owned())?;
        let layout = SessionLayout::new(&config.rfs_home);
        let session_id = Uuid::new_v4().to_string();
        let mut session_lock = SessionLock::acquire(&layout.session_lock)
            .with_context(|| format!("acquire session lock {}", layout.session_lock.display()))?;
        session_lock
            .write_record(
                &layout.session_lock,
                &LockRecord {
                    record_version: LOCK_RECORD_VERSION,
                    session_id: session_id.clone(),
                    pid: std::process::id(),
                },
            )
            .with_context(|| {
                format!(
                    "write session lock record {}",
                    layout.session_lock.display()
                )
            })?;
        remove_if_present(&layout.session)
            .with_context(|| format!("replace session directory {}", layout.session.display()))?;
        create_dir_if_absent(&layout.session)
            .with_context(|| format!("create session directory {}", layout.session.display()))?;
        create_empty_file(&layout.log_path())
            .with_context(|| format!("create session log {}", layout.log_path().display()))?;
        create_empty_file(&layout.database_path()).with_context(|| {
            format!(
                "create session database {}",
                layout.database_path().display()
            )
        })?;
        let cached_blob_store = CachedBlobStore::open(layout.cache.clone(), blob_store, runtime)
            .with_context(|| format!("open verified cache {}", layout.cache.display()))?;
        let overlay = OverlayStore::open(layout.overlay_path())
            .with_context(|| format!("open session overlay {}", layout.overlay_path().display()))?;
        let store = SessionStore::create(
            layout.database_path(),
            session_id,
            std::process::id(),
            root_digest.clone(),
            mountpoint.clone(),
        )
        .with_context(|| format!("create session store {}", layout.database_path().display()))?;
        Ok(Self {
            root_digest,
            mountpoint,
            layout,
            store,
            cached_blob_store,
            overlay,
            _session_lock: session_lock,
            inode_locks: Mutex::new(HashMap::new()),
            counters: InternalIoCounters {
                directory_downloads: AtomicU64::new(0),
                directory_cache_hits: AtomicU64::new(0),
                file_downloads: AtomicU64::new(0),
                file_cache_hits: AtomicU64::new(0),
            },
        })
    }

    /// Returns immutable startup facts without I/O.
    pub fn info(&self) -> SessionInfo {
        SessionInfo {
            root_digest: self.root_digest.clone(),
            mountpoint: self.mountpoint.clone(),
            daemon_pid: std::process::id(),
            control_endpoint: self.layout.control_endpoint.clone(),
            log_path: self.layout.log_path(),
        }
    }

    /// Derives the fixed control endpoint without opening or modifying session state.
    pub fn control_endpoint(config: &Config) -> Result<PathBuf, SessionError> {
        ensure_reader_home(config)?;
        Ok(SessionLayout::new(&config.rfs_home).control_endpoint)
    }

    /// Inspects retained session metadata without creating, locking, or modifying state.
    pub fn inspect(config: &Config) -> Result<Option<SessionInfo>, SessionError> {
        ensure_reader_home(config)?;
        let layout = SessionLayout::new(&config.rfs_home);
        if !layout.session.exists() {
            return Ok(None);
        }
        let stored = SessionStore::inspect(&layout.database_path())?;
        Ok(Some(SessionInfo {
            root_digest: stored.root_digest,
            mountpoint: stored.mountpoint,
            daemon_pid: stored.daemon_pid,
            control_endpoint: layout.control_endpoint.clone(),
            log_path: layout.log_path(),
        }))
    }

    /// Returns one effective visible inode without remote I/O.
    pub fn get_inode(&self, inode: InodeId) -> Result<Inode, SessionError> {
        project_inode(self.get_visible_inode(inode)?)
    }

    /// Resolves one visible direct child, loading its parent directory if needed.
    pub fn lookup_child(&self, parent: InodeId, name: &str) -> Result<Inode, SessionError> {
        self.ensure_directory_loaded(parent)?;
        match self.store.child(parent, name)? {
            Some(child) if !child.tombstone => project_inode(child),
            Some(_) => Err(not_found(format!(
                "look up child `{name}` in directory inode {parent}: entry is not visible"
            ))),
            None => Err(not_found(format!(
                "look up child `{name}` in directory inode {parent}: entry does not exist"
            ))),
        }
    }

    /// Lists all visible direct children in basename order, loading the directory if needed.
    pub fn list_directory(&self, inode: InodeId) -> Result<Vec<Inode>, SessionError> {
        self.ensure_directory_loaded(inode)?;
        self.store
            .get_directory_children(inode)?
            .into_iter()
            .filter(|child| !child.tombstone)
            .map(project_inode)
            .collect()
    }

    /// Reads a file range, preferring overlay data over remote-backed cache data.
    pub fn read_range(
        &self,
        inode: InodeId,
        offset: u64,
        size: usize,
    ) -> Result<Bytes, SessionError> {
        let lock = self.inode_lock(inode)?;
        let _guard = lock
            .lock()
            .map_err(|_| internal_error(format!("read inode {inode}: inode lock is poisoned")))?;
        let stored = self.get_visible_inode(inode)?;
        if stored.kind == NodeKind::Directory {
            return Err(is_directory(format!(
                "read inode {inode}: actual kind is directory"
            )));
        }
        if stored.kind != NodeKind::File {
            return Err(internal_error(format!(
                "read inode {inode}: actual kind is {:?}",
                stored.kind
            )));
        }
        if let Some(path) = stored.file_overlay_path {
            return self.overlay.read_range(&path, offset, size);
        }
        let digest = stored.file_remote_digest.ok_or_else(|| {
            internal_error(format!("read inode {inode}: file has no backing digest"))
        })?;
        let (bytes, downloaded) = self.cached_blob_store.read_range(&digest, offset, size)?;
        self.record_file_read(downloaded);
        Ok(bytes)
    }

    /// Returns a snapshot of the current cache and download counters.
    pub fn io_counters(&self) -> IoCounters {
        IoCounters {
            directory_downloads: self.counters.directory_downloads.load(Ordering::Relaxed),
            directory_cache_hits: self.counters.directory_cache_hits.load(Ordering::Relaxed),
            file_downloads: self.counters.file_downloads.load(Ordering::Relaxed),
            file_cache_hits: self.counters.file_cache_hits.load(Ordering::Relaxed),
        }
    }

    /// Commits the one-shot active-to-closed lifecycle transition.
    pub fn close(&self) -> Result<(), SessionError> {
        self.store.close()
    }

    fn ensure_directory_loaded(&self, inode: InodeId) -> Result<(), SessionError> {
        let lock = self.inode_lock(inode)?;
        let _guard = lock.lock().map_err(|_| {
            internal_error(format!(
                "load directory inode {inode}: inode lock is poisoned"
            ))
        })?;
        let directory = self.get_visible_inode(inode)?;
        if directory.kind != NodeKind::Directory {
            return Err(not_directory(format!(
                "load directory inode {inode}: actual kind is {:?}",
                directory.kind
            )));
        }
        if directory.directory_loaded == Some(true) {
            return Ok(());
        }
        let digest = directory.directory_remote_digest.ok_or_else(|| {
            internal_error(format!(
                "directory inode {inode} is not loaded but has no backing remote digest to fetch from the blob store"
            ))
        })?;
        let (bytes, downloaded) = self.cached_blob_store.read_blob(&digest)?;
        let decoded = decode_directory(&digest, bytes).map_err(|source| {
            internal_error(format!("decode directory blob {digest}: {source}"))
        })?;
        let children = decoded_directory_children(decoded)?;
        self.store.get_or_create_dir_children(inode, &children)?;
        self.record_directory_read(downloaded);
        Ok(())
    }

    fn inode_lock(&self, inode: InodeId) -> Result<Arc<Mutex<()>>, SessionError> {
        self.inode_locks
            .lock()
            .map_err(|_| {
                internal_error(format!(
                    "coordinate inode lock {inode}: lock map is poisoned"
                ))
            })
            .map(|mut locks| {
                locks
                    .entry(inode)
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone()
            })
    }
    /// Returns an existing, non-tombstoned inode without projecting it for callers.
    fn get_visible_inode(&self, inode: InodeId) -> Result<StoreInode, SessionError> {
        match self.store.inode(inode)? {
            Some(stored) if !stored.tombstone => Ok(stored),
            Some(_) => Err(not_found(format!("inode {inode} is not visible"))),
            None => Err(not_found(format!("inode {inode} doesn't exist"))),
        }
    }
    fn record_directory_read(&self, downloaded: bool) {
        if downloaded {
            self.counters
                .directory_downloads
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters
                .directory_cache_hits
                .fetch_add(1, Ordering::Relaxed);
        }
    }
    fn record_file_read(&self, downloaded: bool) {
        if downloaded {
            self.counters.file_downloads.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters
                .file_cache_hits
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Resolves a mountpoint to a canonical existing directory.
/// TODO: Refactor the mountpoint validation here and in cli.rs into a
/// common location.
pub(crate) fn canonicalize_mountpoint(path: &Path) -> Result<PathBuf, SessionError> {
    let canonical = fs::canonicalize(path).map_err(|source| {
        internal_error(format!(
            "canonicalize mountpoint {}: {source}",
            path.display()
        ))
    })?;
    if !fs::metadata(&canonical)
        .map_err(|source| {
            internal_error(format!(
                "inspect mountpoint {} canonicalized to {}: {source}",
                path.display(),
                canonical.display()
            ))
        })?
        .is_dir()
    {
        return Err(internal_error(format!(
            "validate mountpoint {} canonicalized to {}: result is not a directory",
            path.display(),
            canonical.display()
        )));
    }
    Ok(canonical)
}

const LOCK_RECORD_VERSION: u32 = 1;
const MIN_TIMESTAMP_SECONDS: i64 = -62_135_596_800;
const MAX_TIMESTAMP_SECONDS: i64 = 253_402_300_799;

fn project_inode(stored: StoreInode) -> Result<Inode, SessionError> {
    let inode = stored.id;
    let parent = stored.parent.unwrap_or(inode);
    let mode = stored.mode.unwrap_or(match stored.kind {
        NodeKind::File => 0o444,
        NodeKind::Directory => 0o555,
        NodeKind::Symlink => 0o777,
    });
    let size = match stored.kind {
        NodeKind::File => stored
            .file_remote_digest
            .as_ref()
            .map(Digest::size_bytes)
            .map(|size| {
                u64::try_from(size).map_err(|_| {
                    internal_error(format!(
                        "project file inode {inode}: size {size} is negative"
                    ))
                })
            })
            .transpose()?
            .unwrap_or(0),
        NodeKind::Directory => 0,
        NodeKind::Symlink => u64::try_from(stored.symlink_target.as_ref().map_or(0, String::len))
            .map_err(|_| {
            internal_error(format!(
                "project symlink inode {inode}: target is oversized"
            ))
        })?,
    };
    Ok(Inode {
        inode,
        parent,
        name: stored.name,
        kind: stored.kind,
        size,
        mode,
        mtime: stored.mtime.unwrap_or(NodeTime::UNIX_EPOCH),
        symlink_target: stored.symlink_target,
    })
}

fn decoded_directory_children(
    directory: crate::reapi::remote_execution::Directory,
) -> Result<Vec<StoreInode>, SessionError> {
    let mut children = Vec::with_capacity(
        directory.files.len() + directory.directories.len() + directory.symlinks.len(),
    );
    for node in directory.files {
        let name = node.name;
        children.push(remote_file(
            name.clone(),
            node.digest.as_ref().ok_or_else(|| {
                internal_error(format!("decode file node `{name}`: missing digest"))
            })?,
            node.node_properties.as_ref(),
        )?);
    }
    for node in directory.directories {
        let name = node.name;
        children.push(remote_directory(
            name.clone(),
            node.digest.as_ref().ok_or_else(|| {
                internal_error(format!("decode directory node `{name}`: missing digest"))
            })?,
            node.node_properties.as_ref(),
        )?);
    }
    for node in directory.symlinks {
        children.push(remote_symlink(
            node.name,
            node.target,
            node.node_properties.as_ref(),
        )?);
    }
    Ok(children)
}

fn remote_file(
    name: String,
    digest: &crate::reapi::remote_execution::Digest,
    properties: Option<&crate::reapi::remote_execution::NodeProperties>,
) -> Result<StoreInode, SessionError> {
    let (mode, mtime) = node_metadata(properties)?;
    Ok(StoreInode {
        id: InodeId::INVALID,
        parent: None,
        name: name.clone(),
        kind: NodeKind::File,
        mode,
        mtime,
        tombstone: false,
        file_remote_digest: Some(Digest::from_reapi(digest).map_err(|error| {
            internal_error(format!(
                "error translating digest of file node {name} from REAPI digest: {error}"
            ))
        })?),
        file_overlay_path: None,
        file_content_dirty: Some(false),
        symlink_target: None,
        directory_remote_digest: None,
        directory_loaded: None,
    })
}
fn remote_directory(
    name: String,
    digest: &crate::reapi::remote_execution::Digest,
    properties: Option<&crate::reapi::remote_execution::NodeProperties>,
) -> Result<StoreInode, SessionError> {
    let (mode, mtime) = node_metadata(properties)?;
    Ok(StoreInode {
        id: InodeId::INVALID,
        parent: None,
        name: name.clone(),
        kind: NodeKind::Directory,
        mode,
        mtime,
        tombstone: false,
        file_remote_digest: None,
        file_overlay_path: None,
        file_content_dirty: None,
        symlink_target: None,
        directory_remote_digest: Some(Digest::from_reapi(digest).map_err(|error| {
            internal_error(format!("decode child `{name}` directory digest: {error}"))
        })?),
        directory_loaded: Some(false),
    })
}
fn remote_symlink(
    name: String,
    target: String,
    properties: Option<&crate::reapi::remote_execution::NodeProperties>,
) -> Result<StoreInode, SessionError> {
    let (mode, mtime) = node_metadata(properties)?;
    Ok(StoreInode {
        id: InodeId::INVALID,
        parent: None,
        name,
        kind: NodeKind::Symlink,
        mode,
        mtime,
        tombstone: false,
        file_remote_digest: None,
        file_overlay_path: None,
        file_content_dirty: None,
        symlink_target: Some(target),
        directory_remote_digest: None,
        directory_loaded: None,
    })
}
fn node_metadata(
    properties: Option<&crate::reapi::remote_execution::NodeProperties>,
) -> Result<(Option<u32>, Option<NodeTime>), SessionError> {
    let Some(properties) = properties else {
        return Ok((None, None));
    };
    let mtime = properties
        .mtime
        .as_ref()
        .map(|time| {
            let nanos = u32::try_from(time.nanos).map_err(|_| {
                internal_error(format!(
                    "decode node mtime seconds {} nanos {}: unable to cast nanos to a 32-bit unsigned integer",
                    time.seconds, time.nanos
                ))
            })?;
            NodeTime::new(time.seconds, nanos).ok_or_else(|| {
                internal_error(format!(
                    "decode node mtime seconds {} nanos {}: invalid timestamp",
                    time.seconds, time.nanos
                ))
            })
        })
        .transpose()?;
    Ok((properties.unix_mode, mtime))
}

#[derive(Debug, Serialize, Deserialize)]
struct LockRecord {
    record_version: u32,
    session_id: String,
    pid: u32,
}
struct SessionLock {
    file: File,
}
impl SessionLock {
    fn acquire(path: &Path) -> Result<Self, SessionError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)
            .map_err(|source| fs_error("open session lock", path, source))?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|source| fs_error("secure session lock", path, source))?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let owner = fs::read_to_string(path)
                .ok()
                .and_then(|value| serde_json::from_str::<LockRecord>(&value).ok())
                .map(|value| format!("(pid {}, session {})", value.pid, value.session_id))
                .unwrap_or_else(|| "(owner diagnostics unavailable)".into());
            return Err(failed_precondition(format!(
                "acquire session lock {}: another RemoteFS session owns it {}",
                path.display(),
                owner
            )));
        }
        Ok(Self { file })
    }
    fn write_record(&mut self, path: &Path, record: &LockRecord) -> Result<(), SessionError> {
        self.file
            .set_len(0)
            .map_err(|source| fs_error("truncate session lock", path, source))?;
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|source| fs_error("rewind session lock", path, source))?;
        serde_json::to_writer(&mut self.file, record).map_err(|source| {
            internal_error(format!(
                "encode session lock record {}: {source}",
                path.display()
            ))
        })?;
        self.file
            .write_all(b"\n")
            .map_err(|source| fs_error("write session lock", path, source))
    }
}
impl Drop for SessionLock {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

fn ensure_writable_home(config: &Config) -> Result<(), SessionError> {
    if !config.rfs_home.exists() {
        fs::create_dir_all(&config.rfs_home)
            .map_err(|source| fs_error("create state root", &config.rfs_home, source))?;
        fs::set_permissions(&config.rfs_home, fs::Permissions::from_mode(0o700))
            .map_err(|source| fs_error("secure state root", &config.rfs_home, source))?;
    }
    Ok(())
}
fn ensure_reader_home(config: &Config) -> Result<(), SessionError> {
    if config.rfs_home.exists() {
        Ok(())
    } else {
        Err(failed_precondition(format!(
            "inspect session home {}: path does not exist",
            config.rfs_home.display()
        )))
    }
}
fn create_dir_if_absent(path: &Path) -> Result<(), SessionError> {
    match fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(source) => Err(fs_error("create directory", path, source)),
    }
}
fn create_empty_file(path: &Path) -> Result<(), SessionError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| fs_error("create file", path, source))?;
    Ok(())
}
fn remove_if_present(path: &Path) -> Result<(), SessionError> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == ErrorKind::NotFound => Ok(()),
        Err(source) => Err(fs_error("local state cleanup", path, source)),
    }
}

pub(super) fn fs_error(
    operation: &'static str,
    path: &Path,
    source: std::io::Error,
) -> SessionError {
    internal_error(format!("{operation} {}: {source}", path.display()))
}

/// Fixed local filesystem paths owned by one RemoteFS home.
struct SessionLayout {
    cache: PathBuf,
    session: PathBuf,
    session_lock: PathBuf,
    control_endpoint: PathBuf,
}
impl SessionLayout {
    fn new(home: &Path) -> Self {
        let session = home.join("session");
        Self {
            cache: home.join("cache"),
            session: session.clone(),
            session_lock: home.join("session.lock"),
            control_endpoint: session.join("control.sock"),
        }
    }
    fn database_path(&self) -> PathBuf {
        self.session.join("session.db")
    }
    fn overlay_path(&self) -> PathBuf {
        self.session.join("overlay")
    }
    fn log_path(&self) -> PathBuf {
        self.session.join("session.log")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::io::Write;
    use std::sync::{Arc, Barrier, Mutex};

    use async_trait::async_trait;
    use prost_types::Timestamp;
    use tempfile::TempDir;

    use super::*;
    use crate::cas::{Blob, CasError, UploadStats};
    use crate::logging::init_test;
    use crate::tree::{DirectoryBuilder, DirectoryEntry, FileEntry, NodeMetadata};

    /// In-memory backing store used to exercise the public Session boundary.
    struct FakeBlobStore {
        blobs: HashMap<Digest, Bytes>,
        streams: Mutex<u64>,
    }

    #[async_trait]
    impl BlobStore for FakeBlobStore {
        async fn find_missing_blobs(&self, digests: &[Digest]) -> Result<Vec<Digest>, CasError> {
            Ok(digests
                .iter()
                .filter(|digest| !self.blobs.contains_key(*digest))
                .cloned()
                .collect())
        }

        async fn upload_blobs(&self, _blobs: Vec<Blob>) -> Result<UploadStats, CasError> {
            Ok(UploadStats::default())
        }

        async fn stream_blob(
            &self,
            digest: &Digest,
            destination: &mut (dyn Write + Send),
        ) -> Result<(), CasError> {
            let bytes = self.blobs.get(digest).ok_or_else(|| {
                CasError::InvalidInstanceName("fake".to_owned(), "missing blob".to_owned())
            })?;
            *self.streams.lock().expect("test stream mutex") += 1;
            destination
                .write_all(bytes)
                .expect("test destination is writable");
            Ok(())
        }
    }

    /// Opens a session with one remote file exposed by its root directory.
    fn session() -> (TempDir, tokio::runtime::Runtime, Session, InodeId) {
        let home = TempDir::new().expect("temporary home");
        let mountpoint = TempDir::new().expect("temporary mountpoint");
        let file_bytes = Bytes::from_static(b"hello remote filesystem");
        let file_digest = Digest::for_bytes(&file_bytes);
        let mut builder = DirectoryBuilder::new();
        builder
            .add_file(FileEntry {
                name: "readme".to_owned(),
                digest: file_digest.clone(),
                metadata: NodeMetadata::new(
                    NodeKind::File,
                    Some(0o644),
                    Some(Timestamp {
                        seconds: 42,
                        nanos: 7,
                    }),
                ),
            })
            .expect("valid test file entry");
        let root = builder.encode().expect("encode test root");
        let blobs = HashMap::from([(root.digest.clone(), root.bytes), (file_digest, file_bytes)]);
        let runtime = tokio::runtime::Runtime::new().expect("test runtime");
        let session = Session::open(
            Config {
                rfs_home: home.path().join("home"),
            },
            root.digest,
            mountpoint.path(),
            Box::new(FakeBlobStore {
                blobs,
                streams: Mutex::new(0),
            }),
            runtime.handle().clone(),
        )
        .expect("open test session");
        (home, runtime, session, InodeId::ROOT)
    }

    /// A directory load creates visible children and remote range reads reuse the cache.
    #[test]
    fn directory_loading_and_file_reads_use_the_session_facade() {
        init_test();
        let (_home, _runtime, session, root) = session();

        let child = session.lookup_child(root, "readme").expect("load child");
        assert_eq!(child.kind, NodeKind::File);
        assert_eq!(child.mode, 0o644);
        assert_eq!(
            child.mtime,
            NodeTime::new(42, 7).expect("valid metadata time")
        );
        assert_eq!(
            session.list_directory(root).expect("list root"),
            vec![child.clone()]
        );
        assert_eq!(
            session.read_range(child.inode, 6, 6).expect("read range"),
            "remote"
        );
        assert_eq!(
            session
                .read_range(child.inode, 0, 5)
                .expect("read cached range"),
            "hello"
        );
        assert!(matches!(
            session.lookup_child(root, "missing"),
            Err(SessionError::NotFound { .. })
        ));
        assert!(matches!(
            session.get_inode(InodeId::new(99).expect("valid inode")),
            Err(SessionError::NotFound { .. })
        ));
        assert_eq!(
            session.io_counters(),
            IoCounters {
                directory_downloads: 1,
                directory_cache_hits: 0,
                file_downloads: 1,
                file_cache_hits: 1
            }
        );
    }

    /// A child directory retains the mode and modification time encoded in its parent.
    #[test]
    fn directory_child_metadata_is_visible_through_the_session_facade() {
        init_test();
        let home = TempDir::new().expect("temporary home");
        let mountpoint = TempDir::new().expect("temporary mountpoint");
        let child_directory = DirectoryBuilder::new()
            .encode()
            .expect("encode child directory");
        let mut root_builder = DirectoryBuilder::new();
        root_builder
            .add_directory(DirectoryEntry {
                name: "nested".to_owned(),
                digest: child_directory.digest.clone(),
                metadata: NodeMetadata::new(
                    NodeKind::Directory,
                    Some(0o750),
                    Some(Timestamp {
                        seconds: 123,
                        nanos: 456,
                    }),
                ),
            })
            .expect("add child directory");
        let root = root_builder.encode().expect("encode root directory");
        let runtime = tokio::runtime::Runtime::new().expect("test runtime");
        let session = Session::open(
            Config {
                rfs_home: home.path().join("home"),
            },
            root.digest.clone(),
            mountpoint.path(),
            Box::new(FakeBlobStore {
                blobs: HashMap::from([
                    (root.digest, root.bytes),
                    (child_directory.digest, child_directory.bytes),
                ]),
                streams: Mutex::new(0),
            }),
            runtime.handle().clone(),
        )
        .expect("open session");

        let child = session
            .lookup_child(InodeId::ROOT, "nested")
            .expect("load child directory");

        assert_eq!(child.kind, NodeKind::Directory);
        assert_eq!(child.mode, 0o750);
        assert_eq!(
            child.mtime,
            NodeTime::new(123, 456).expect("valid directory metadata time")
        );
    }

    /// Closing a session is a one-shot durable transition that retained inspection can read.
    #[test]
    fn close_is_one_shot_and_retained_metadata_is_inspectable() {
        init_test();
        let (home, _runtime, session, _) = session();
        let config = Config {
            rfs_home: home.path().join("home"),
        };

        session.close().expect("close active session");
        assert!(session.close().is_err());
        drop(session);
        assert!(
            Session::inspect(&config)
                .expect("inspect retained state")
                .is_some()
        );
    }

    /// An empty directory can be loaded concurrently without allocating inconsistent state.
    #[test]
    fn empty_directory_loading_is_stable_under_concurrent_calls() {
        init_test();
        let home = TempDir::new().expect("temporary home");
        let mountpoint = TempDir::new().expect("temporary mountpoint");
        let root = DirectoryBuilder::new().encode().expect("encode empty root");
        let runtime = tokio::runtime::Runtime::new().expect("test runtime");
        let session = Arc::new(
            Session::open(
                Config {
                    rfs_home: home.path().join("home"),
                },
                root.digest.clone(),
                mountpoint.path(),
                Box::new(FakeBlobStore {
                    blobs: HashMap::from([(root.digest, root.bytes)]),
                    streams: Mutex::new(0),
                }),
                runtime.handle().clone(),
            )
            .expect("open empty session"),
        );
        let barrier = Arc::new(Barrier::new(5));
        let threads = (0..4)
            .map(|_| {
                let session = Arc::clone(&session);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    session
                        .list_directory(InodeId::ROOT)
                        .expect("list empty root")
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        for thread in threads {
            assert!(thread.join().expect("join listing thread").is_empty());
        }
        assert_eq!(session.io_counters().directory_downloads, 1);
    }

    /// A malformed directory never exposes partial children through the public namespace API.
    #[test]
    fn malformed_directory_does_not_make_children_visible() {
        init_test();
        let home = TempDir::new().expect("temporary home");
        let mountpoint = TempDir::new().expect("temporary mountpoint");
        let malformed = Bytes::from_static(b"not a directory proto");
        let root = Digest::for_bytes(&malformed);
        let runtime = tokio::runtime::Runtime::new().expect("test runtime");
        let session = Session::open(
            Config {
                rfs_home: home.path().join("home"),
            },
            root.clone(),
            mountpoint.path(),
            Box::new(FakeBlobStore {
                blobs: HashMap::from([(root, malformed)]),
                streams: Mutex::new(0),
            }),
            runtime.handle().clone(),
        )
        .expect("open malformed session");
        assert!(matches!(
            session.list_directory(InodeId::ROOT),
            Err(SessionError::InternalError { .. })
        ));
        assert!(matches!(
            session.lookup_child(InodeId::ROOT, "partial"),
            Err(SessionError::InternalError { .. })
        ));
    }

    /// A mismatched directory blob fails cache verification before namespace rows are committed.
    #[test]
    fn digest_mismatched_directory_does_not_make_children_visible() {
        init_test();
        let home = TempDir::new().expect("temporary home");
        let mountpoint = TempDir::new().expect("temporary mountpoint");
        let root = Digest::for_bytes(b"expected directory");
        let runtime = tokio::runtime::Runtime::new().expect("test runtime");
        let session = Session::open(
            Config {
                rfs_home: home.path().join("home"),
            },
            root.clone(),
            mountpoint.path(),
            Box::new(FakeBlobStore {
                blobs: HashMap::from([(root, Bytes::from_static(b"wrong directory"))]),
                streams: Mutex::new(0),
            }),
            runtime.handle().clone(),
        )
        .expect("open mismatched session");
        assert!(matches!(
            session.list_directory(InodeId::ROOT),
            Err(SessionError::InternalError { .. })
        ));
        assert!(matches!(
            session.lookup_child(InodeId::ROOT, "partial"),
            Err(SessionError::InternalError { .. })
        ));
    }

    /// Overlay data wins over remote bytes, and both backing kinds return EOF as empty bytes.
    #[test]
    fn overlay_precedence_and_eof_are_visible_through_read_range() {
        init_test();
        let (_home, _runtime, session, root) = session();
        let child = session.lookup_child(root, "readme").expect("load child");
        assert!(
            session
                .read_range(child.inode, 99, 1)
                .expect("read remote EOF")
                .is_empty(),
            "reading file at offset past its size did not return an empty bytea array"
        );
        let overlay_path = session.layout.overlay_path().join("data/replacement");
        fs::write(&overlay_path, b"overlay").expect("write test overlay");
        let database = session.layout.database_path();
        // TODO: Use method from store module that'll likely be needed in the future
        // anyways.
        rusqlite::Connection::open(database).expect("open test database")
            .execute("UPDATE inodes SET file_overlay_path = 'replacement', file_content_dirty = 1 WHERE id = ?1", [child.inode.sqlite()])
            .expect("set test overlay backing");
        assert_eq!(
            session
                .read_range(child.inode, 0, 32)
                .expect("read overlay"),
            "overlay"
        );
        assert!(
            session
                .read_range(child.inode, 99, 1)
                .expect("read overlay EOF")
                .is_empty()
        );
    }

    /// Two inodes sharing one digest classify the cache fill and follower as separate outcomes.
    #[test]
    fn shared_digest_reads_report_one_download_and_one_cache_hit() {
        init_test();
        let home = TempDir::new().expect("temporary home");
        let mountpoint = TempDir::new().expect("temporary mountpoint");
        let bytes = Bytes::from_static(b"shared bytes");
        let digest = Digest::for_bytes(&bytes);
        let mut builder = DirectoryBuilder::new();
        for name in ["first", "second"] {
            builder
                .add_file(FileEntry {
                    name: name.to_owned(),
                    digest: digest.clone(),
                    metadata: NodeMetadata::new(NodeKind::File, None, None),
                })
                .expect("add shared file");
        }
        let root = builder.encode().expect("encode shared root");
        let runtime = tokio::runtime::Runtime::new().expect("test runtime");
        let session = Arc::new(
            Session::open(
                Config {
                    rfs_home: home.path().join("home"),
                },
                root.digest.clone(),
                mountpoint.path(),
                Box::new(FakeBlobStore {
                    blobs: HashMap::from([(root.digest, root.bytes), (digest, bytes)]),
                    streams: Mutex::new(0),
                }),
                runtime.handle().clone(),
            )
            .expect("open shared session"),
        );
        let first = session
            .lookup_child(InodeId::ROOT, "first")
            .expect("load first");
        let second = session
            .lookup_child(InodeId::ROOT, "second")
            .expect("load second");
        let barrier = Arc::new(Barrier::new(3));
        let threads = [first, second].map(|inode| {
            let session = Arc::clone(&session);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                session
                    .read_range(inode.inode, 0, 32)
                    .expect("read shared blob")
            })
        });
        barrier.wait();
        for thread in threads {
            assert_eq!(thread.join().expect("join read thread"), "shared bytes");
        }
        assert_eq!(session.io_counters().file_downloads, 1);
        assert_eq!(session.io_counters().file_cache_hits, 1);
    }

    /// Info and retained inspection are read-only, while control endpoint discovery creates no state.
    #[test]
    fn info_control_endpoint_and_inspect_are_read_only() {
        init_test();
        let missing = TempDir::new().expect("temporary missing home");
        let missing_config = Config {
            rfs_home: missing.path().join("missing"),
        };
        assert!(Session::control_endpoint(&missing_config).is_err());
        assert!(Session::inspect(&missing_config).is_err());
        assert!(!missing_config.rfs_home.exists());

        let (home, _runtime, session, _) = session();
        let config = Config {
            rfs_home: home.path().join("home"),
        };
        assert_eq!(
            Session::control_endpoint(&config).expect("control endpoint"),
            session.info().control_endpoint
        );
        assert_eq!(
            Session::inspect(&config)
                .expect("inspect active")
                .expect("retained info")
                .root_digest,
            session.info().root_digest
        );
    }
}
