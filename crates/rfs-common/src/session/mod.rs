//! Concrete local-session facade for durable namespace state and immutable blobs.
//!
//! `Session` is the only local-storage type exposed to higher layers. It owns a
//! shared content-addressed cache and one exclusive active session. SQLite is
//! authoritative for the visible namespace; cache paths, database connections,
//! overlay paths, and advisory-lock handles remain private.

mod cache;
mod overlay;
mod store;

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::config::Config;
use crate::digest::Digest;
use crate::error_context::ResultContextError;

pub use crate::tree::NodeKind;
pub use cache::{BlobDownloader, BlobWriter};

use cache::BlobCache;
use overlay::OverlayStore;
use store::{ReadSource, SessionMetadata, SessionStore};

const LOCK_RECORD_VERSION: u32 = 1;
const MIN_TIMESTAMP_SECONDS: i64 = -62_135_596_800;
const MAX_TIMESTAMP_SECONDS: i64 = 253_402_300_799;

/// Session-stable inode identity that is positive and representable by SQLite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InodeId(i64);

impl InodeId {
    /// Root inode shared by every mounted workspace.
    pub const ROOT: Self = Self(1);

    /// Validates a raw inode supplied by an external adapter.
    pub fn new(value: u64) -> Result<Self, SessionError> {
        let value = i64::try_from(value).map_err(|_| SessionError::InvalidInode { value })?;
        if value == 0 {
            return Err(SessionError::InvalidInode { value: 0 });
        }
        Ok(Self(value))
    }

    /// Returns the raw inode value used by FUSE.
    pub fn get(self) -> u64 {
        u64::try_from(self.0).expect("validated inode is positive")
    }

    pub(super) fn sqlite(self) -> i64 {
        self.0
    }

    pub(super) fn from_sqlite(value: i64, path: &Path) -> Result<Self, SessionError> {
        if value <= 0 {
            return Err(stale_path(
                path,
                format!("stored inode {value} is not positive"),
            ));
        }
        Ok(Self(value))
    }
}

impl fmt::Display for InodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
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
pub struct Node {
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

/// Kind-safe immutable identity for a decoded remote child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteContent {
    /// Regular-file content digest.
    File(Digest),
    /// Child-directory message digest.
    Directory(Digest),
    /// Exact symbolic-link target.
    Symlink(String),
}

/// Transport-independent child descriptor used for atomic materialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteChild {
    /// UTF-8 basename relative to the materialized parent.
    pub name: String,
    /// Kind-safe immutable content identity.
    pub content: RemoteContent,
    /// Preserved remote mode; absence is distinct from zero.
    pub mode: Option<u32>,
    /// Preserved remote mtime; absence is distinct from the epoch.
    pub mtime: Option<NodeTime>,
}

/// Result of a lazy namespace operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup<T> {
    /// SQLite contains a definitive visible result.
    Ready(T),
    /// The caller must fetch and materialize this complete directory object.
    NeedsMaterialization { digest: Digest },
}

/// Result of an inode-based immutable range read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalRead {
    /// The requested range was served from admitted local content.
    Ready(Bytes),
    /// The caller must fetch this immutable object and retry by inode.
    NeedsDownload { digest: Digest },
}

/// Durable session lifecycle stored in SQLite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionLifecycle {
    /// Durable creation started but did not activate.
    Initializing,
    /// The daemon currently owns the session.
    Active,
    /// Clean close committed successfully.
    Closed,
}

impl fmt::Display for SessionLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
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
    /// Canonical mounted workspace path.
    pub mountpoint: PathBuf,
    /// Process identifier recorded at construction.
    pub daemon_pid: u32,
    /// Fixed Unix control-socket path.
    pub control_endpoint: PathBuf,
    /// Shared cache root used for diagnostics.
    pub cache_root: PathBuf,
    /// Active-session root used for diagnostics.
    pub active_root: PathBuf,
    /// Daemon log path used during logging initialization.
    pub log_path: PathBuf,
}

/// Durable fields used by fallback status and mountpoint validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedSession {
    /// Process identifier recorded in durable metadata.
    pub daemon_pid: u32,
    /// Durable lifecycle observed during inspection.
    pub state: SessionLifecycle,
    /// Immutable mounted root digest.
    pub root_digest: Digest,
    /// Canonical mountpoint recorded at creation.
    pub mountpoint: PathBuf,
}

/// Failures owned by the local session hierarchy.
#[derive(Debug, Error)]
pub enum SessionError {
    /// A path violates type, ownership, permission, or inventory policy.
    #[error("local session path `{path}` is unsafe: {reason}")]
    UnsafePath { path: PathBuf, reason: String },
    /// Another process holds the stable advisory lock.
    #[error("another RemoteFS session owns `{path}`{owner}")]
    ActiveSession { path: PathBuf, owner: String },
    /// Retained durable state is malformed, partial, or unsupported.
    #[error(
        "stale or malformed session state at `{path}`; delete `RFS_HOME` and start again: {reason}"
    )]
    StaleSession { path: PathBuf, reason: String },
    /// A named local filesystem operation failed.
    #[error("filesystem operation on `{path}` failed: {source}")]
    Filesystem {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// A named SQLite operation failed.
    #[error("SQLite {operation} on `{path}` failed: {source}")]
    Database {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },
    /// A mutex protecting session-owned state was poisoned.
    #[error("session synchronization failed while {operation}")]
    Synchronization { operation: &'static str },
    /// The supplied mountpoint is not a canonicalizable directory.
    #[error("mountpoint `{path}` must be an existing directory")]
    InvalidMountpoint { path: PathBuf },
    /// A raw inode is zero or exceeds SQLite's signed range.
    #[error("inode value {value} is outside the supported range")]
    InvalidInode { value: u64 },
    /// No visible row exists for an inode.
    #[error("inode {inode} is not visible")]
    UnknownInode { inode: InodeId },
    /// A fully materialized directory has no visible child with this name.
    #[error("entry `{name}` was not found in directory inode {parent}")]
    NotFound { parent: InodeId, name: String },
    /// An operation was applied to the wrong node kind.
    #[error("inode {inode} is a {actual:?}, expected {expected:?}")]
    WrongKind {
        inode: InodeId,
        expected: NodeKind,
        actual: NodeKind,
    },
    /// Completed pending bytes do not match the expected digest.
    #[error("blob verification failed: expected {expected}, got {actual}")]
    BlobIntegrity { expected: Digest, actual: Digest },
    /// Current wall-clock time cannot be stored safely.
    #[error("system time is outside the supported timestamp range")]
    InvalidSystemTime,
    /// An operation requiring active resources ran after clean close.
    #[error("session is already closed")]
    Closed,
    /// Additional owning-operation context for another session error.
    #[error("{operation}: {source}")]
    Context {
        operation: String,
        #[source]
        source: Box<SessionError>,
    },
}

impl ResultContextError for SessionError {
    fn with_context(self, operation: String) -> Self {
        Self::Context {
            operation,
            source: Box::new(self),
        }
    }
}

/// Concrete synchronous facade for all local storage used by one workspace.
pub struct Session {
    info: SessionInfo,
    cache: BlobCache,
    active: ActiveSession,
}

impl Session {
    /// Opens a fresh writable session and acquires exclusive `RFS_HOME` ownership.
    pub fn open(
        config: Config,
        root_digest: Digest,
        mountpoint: impl AsRef<Path>,
    ) -> Result<Self, SessionError> {
        let mountpoint = canonicalize_mountpoint(mountpoint.as_ref())?;
        let home = writable_home(&config)?;
        validate_top_level(&home)?;
        let cache = BlobCache::open(home.join("cache/blobs"))?;
        let active = ActiveSession::open(
            home.join("active"),
            home.join("active.lock"),
            root_digest.clone(),
            mountpoint.clone(),
        )?;
        let info = SessionInfo {
            root_digest,
            mountpoint,
            daemon_pid: std::process::id(),
            control_endpoint: active.root.join("control.sock"),
            cache_root: home.join("cache"),
            active_root: active.root.clone(),
            log_path: active.root.join("rfsd.log"),
        };
        Ok(Self {
            info,
            cache,
            active,
        })
    }

    /// Returns immutable startup and live-status facts without rereading SQLite.
    pub fn info(&self) -> SessionInfo {
        self.info.clone()
    }

    /// Derives the fixed control endpoint without opening SQLite or mutating state.
    pub fn control_endpoint(config: &Config) -> Result<PathBuf, SessionError> {
        Ok(reader_home(config)?.join("active/control.sock"))
    }

    /// Performs one-shot read-only retained-session inspection.
    pub fn inspect(config: &Config) -> Result<Option<RetainedSession>, SessionError> {
        let home = reader_home(config)?;
        ActiveSession::inspect(&home.join("active"), &home.join("active.lock"))
    }

    /// Returns one visible inode from authoritative SQLite state.
    pub fn node(&self, inode: InodeId) -> Result<Node, SessionError> {
        self.active.with_store(|store| store.node(inode))
    }

    /// Looks up a child or reports the remote directory that still needs loading.
    pub fn lookup(&self, parent: InodeId, name: &str) -> Result<Lookup<Node>, SessionError> {
        self.active.with_store(|store| store.lookup(parent, name))
    }

    /// Lists a directory or reports the remote directory that still needs loading.
    pub fn list_directory(&self, inode: InodeId) -> Result<Lookup<Vec<Node>>, SessionError> {
        self.active.with_store(|store| store.list_directory(inode))
    }

    /// Atomically records a complete decoded remote child set.
    pub fn materialize_directory(
        &self,
        parent: InodeId,
        remote_digest: &Digest,
        remote_children: Vec<RemoteChild>,
    ) -> Result<Vec<Node>, SessionError> {
        self.active.with_store(|store| {
            store.materialize_directory(parent, remote_digest, &remote_children)
        })
    }

    /// Reads a complete admitted object, returning `None` on a cache miss.
    pub fn read_blob(&self, digest: &Digest) -> Result<Option<Bytes>, SessionError> {
        self.cache.read_blob(digest)
    }

    /// Resolves current inode backing and serves a range or requests a remote fill.
    pub fn read_range(
        &self,
        inode: InodeId,
        offset: u64,
        size: usize,
    ) -> Result<LocalRead, SessionError> {
        match self.active.with_store(|store| store.read_source(inode))? {
            ReadSource::Remote(digest) => match self.cache.read_range(&digest, offset, size)? {
                Some(bytes) => Ok(LocalRead::Ready(bytes)),
                None => Ok(LocalRead::NeedsDownload { digest }),
            },
            ReadSource::Overlay(path) => self
                .active
                .read_overlay_range(&path, offset, size)
                .map(LocalRead::Ready),
        }
    }

    /// Rechecks cache presence and creates an opaque streaming writer on a miss.
    pub fn start_blob_download(&self, digest: &Digest) -> Result<BlobDownloader, SessionError> {
        self.cache.start_download(digest)
    }

    /// Verifies, syncs, and atomically admits a completed pending blob.
    pub fn finalize_blob(&self, writer: BlobWriter) -> Result<(), SessionError> {
        self.cache.finalize(writer)
    }

    /// Counts admitted cache files for live status composition.
    pub fn cached_blob_count(&self) -> Result<u64, SessionError> {
        self.cache.entry_count()
    }

    /// Commits clean close and releases active resources. Repeated calls succeed.
    pub fn close(&self) -> Result<(), SessionError> {
        self.active.close()
    }
}

/// Resolves a mountpoint to a canonical existing directory.
pub fn canonicalize_mountpoint(path: &Path) -> Result<PathBuf, SessionError> {
    let canonical = fs::canonicalize(path).map_err(|_| SessionError::InvalidMountpoint {
        path: path.to_path_buf(),
    })?;
    if !fs::metadata(&canonical)
        .map_err(|_| SessionError::InvalidMountpoint {
            path: path.to_path_buf(),
        })?
        .is_dir()
    {
        return Err(SessionError::InvalidMountpoint {
            path: path.to_path_buf(),
        });
    }
    Ok(canonical)
}

struct ActiveSession {
    root: PathBuf,
    resources: Mutex<Option<ActiveResources>>,
}

struct ActiveResources {
    store: SessionStore,
    overlay: OverlayStore,
    _lock: SessionLock,
}

impl ActiveSession {
    fn open(
        root: PathBuf,
        lock_path: PathBuf,
        root_digest: Digest,
        mountpoint: PathBuf,
    ) -> Result<Self, SessionError> {
        let session_id = Uuid::new_v4().to_string();
        let mut lock = SessionLock::acquire(&lock_path)?;
        if root.exists() {
            validate_closed_session(&root, &lock_path)?;
            remove_if_present(&root)?;
        }
        lock.write_record(
            &lock_path,
            &LockRecord {
                record_version: LOCK_RECORD_VERSION,
                session_id: session_id.clone(),
                pid: std::process::id(),
            },
        )?;
        create_dir_private(&root)?;
        let log = root.join("rfsd.log");
        let database = root.join("session.db");
        create_file_private(&log)?;
        create_file_private(&database)?;
        let overlay = OverlayStore::open(root.join("overlay"))?;
        let store = SessionStore::create(
            database,
            session_id,
            std::process::id(),
            root_digest,
            mountpoint,
        )?;
        Ok(Self {
            root,
            resources: Mutex::new(Some(ActiveResources {
                store,
                overlay,
                _lock: lock,
            })),
        })
    }

    fn inspect(root: &Path, lock_path: &Path) -> Result<Option<RetainedSession>, SessionError> {
        if !root.exists() {
            return Ok(None);
        }
        validate_closed_layout(root)?;
        let stored = SessionStore::inspect(&root.join("session.db"))?;
        validate_closed_identity(lock_path, &stored)?;
        Ok(Some(stored.metadata.into()))
    }

    fn with_store<T>(
        &self,
        operation: impl FnOnce(&SessionStore) -> Result<T, SessionError>,
    ) -> Result<T, SessionError> {
        let guard = self
            .resources
            .lock()
            .map_err(|_| SessionError::Synchronization {
                operation: "access active session resources",
            })?;
        let resources = guard.as_ref().ok_or(SessionError::Closed)?;
        operation(&resources.store)
    }

    fn read_overlay_range(
        &self,
        relative: &Path,
        offset: u64,
        size: usize,
    ) -> Result<Bytes, SessionError> {
        let guard = self
            .resources
            .lock()
            .map_err(|_| SessionError::Synchronization {
                operation: "read active overlay",
            })?;
        let resources = guard.as_ref().ok_or(SessionError::Closed)?;
        resources.overlay.read_range(relative, offset, size)
    }

    fn close(&self) -> Result<(), SessionError> {
        let mut guard = self
            .resources
            .lock()
            .map_err(|_| SessionError::Synchronization {
                operation: "close active session",
            })?;
        let Some(resources) = guard.take() else {
            return Ok(());
        };
        if let Err(error) = resources.store.close() {
            *guard = Some(resources);
            return Err(error);
        }
        drop(resources);
        Ok(())
    }
}

impl From<SessionMetadata> for RetainedSession {
    fn from(value: SessionMetadata) -> Self {
        Self {
            daemon_pid: value.daemon_pid,
            state: value.state,
            root_digest: value.root_digest,
            mountpoint: value.mountpoint,
        }
    }
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
                .map(|value| format!(" (pid {}, session {})", value.pid, value.session_id))
                .unwrap_or_else(|| " (owner diagnostics unavailable)".into());
            return Err(SessionError::ActiveSession {
                path: path.to_path_buf(),
                owner,
            });
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
            SessionError::UnsafePath {
                path: path.to_path_buf(),
                reason: format!("cannot encode lock record: {source}"),
            }
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

fn validate_closed_session(root: &Path, lock_path: &Path) -> Result<(), SessionError> {
    validate_closed_layout(root)?;
    let stored = SessionStore::inspect(&root.join("session.db"))?;
    validate_closed_identity(lock_path, &stored)
}

fn validate_closed_identity(
    lock_path: &Path,
    stored: &store::StoredSession,
) -> Result<(), SessionError> {
    let record: LockRecord = fs::read_to_string(lock_path)
        .ok()
        .and_then(|value| serde_json::from_str(&value).ok())
        .ok_or_else(|| stale_path(lock_path, "invalid lock record".into()))?;
    if record.record_version != LOCK_RECORD_VERSION {
        return Err(stale_path(
            lock_path,
            "unsupported lock record version".into(),
        ));
    }
    if stored.session_id != record.session_id || stored.metadata.daemon_pid != record.pid {
        return Err(stale_path(
            lock_path,
            "lock and database session identity do not match".into(),
        ));
    }
    if stored.metadata.state != SessionLifecycle::Closed
        || stored.closed_at_seconds.is_none()
        || stored.closed_at_nanos.is_none()
    {
        return Err(stale_path(
            lock_path,
            "session was not closed cleanly".into(),
        ));
    }
    Ok(())
}

fn validate_closed_layout(root: &Path) -> Result<(), SessionError> {
    validate_directory_entries(
        root,
        &[
            ("session.db", false),
            ("rfsd.log", false),
            ("overlay", true),
        ],
    )
    .map_err(|error| stale_path(root, error.to_string()))?;
    validate_directory_entries(&root.join("overlay"), &[("data", true), ("tmp", true)])
        .map_err(|error| stale_path(root, error.to_string()))
}

fn writable_home(config: &Config) -> Result<PathBuf, SessionError> {
    if !config.rfs_home.exists() {
        fs::create_dir_all(&config.rfs_home)
            .map_err(|source| fs_error("create state root", &config.rfs_home, source))?;
        fs::set_permissions(&config.rfs_home, fs::Permissions::from_mode(0o700))
            .map_err(|source| fs_error("secure state root", &config.rfs_home, source))?;
    }
    canonical_private_home(&config.rfs_home)
}

fn reader_home(config: &Config) -> Result<PathBuf, SessionError> {
    if config.rfs_home.exists() {
        canonical_private_home(&config.rfs_home)
    } else {
        Ok(config.rfs_home.clone())
    }
}

fn canonical_private_home(path: &Path) -> Result<PathBuf, SessionError> {
    let home = fs::canonicalize(path)
        .map_err(|source| fs_error("canonicalize state root", path, source))?;
    let metadata = fs::symlink_metadata(&home)
        .map_err(|source| fs_error("inspect state root", &home, source))?;
    if !metadata.file_type().is_dir() {
        return Err(SessionError::UnsafePath {
            path: home,
            reason: "RFS_HOME is not a directory".into(),
        });
    }
    validate_existing_permissions(&home, &metadata)?;
    Ok(home)
}

fn validate_top_level(home: &Path) -> Result<(), SessionError> {
    for entry in fs::read_dir(home).map_err(|source| fs_error("read state root", home, source))? {
        let entry = entry.map_err(|source| fs_error("read state-root entry", home, source))?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)
            .map_err(|source| fs_error("inspect state-root entry", &path, source))?;
        let valid = match entry.file_name().to_str() {
            Some("active.lock") => metadata.file_type().is_file(),
            Some("cache" | "active") => metadata.file_type().is_dir(),
            _ => false,
        };
        if !valid {
            return Err(SessionError::UnsafePath {
                path,
                reason: "unknown entry, symlink, or wrong entry type; inspect it manually".into(),
            });
        }
        validate_existing_permissions(&path, &metadata)?;
    }
    Ok(())
}

pub(super) fn create_dir_private(path: &Path) -> Result<(), SessionError> {
    match fs::create_dir(path) {
        Ok(()) => fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|source| fs_error("secure session directory", path, source)),
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path)
                .map_err(|source| fs_error("inspect session directory", path, source))?;
            if !metadata.file_type().is_dir() {
                return Err(SessionError::UnsafePath {
                    path: path.to_path_buf(),
                    reason: "expected a directory and will not follow a symlink".into(),
                });
            }
            validate_existing_permissions(path, &metadata)
        }
        Err(source) => Err(fs_error("create session directory", path, source)),
    }
}

fn create_file_private(path: &Path) -> Result<(), SessionError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|source| fs_error("create session file", path, source))?;
    Ok(())
}

pub(super) fn validate_existing_permissions(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<(), SessionError> {
    if metadata.uid() != unsafe { libc::geteuid() } {
        return Err(SessionError::UnsafePath {
            path: path.to_path_buf(),
            reason: "entry is not owned by the effective user".into(),
        });
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(SessionError::UnsafePath {
            path: path.to_path_buf(),
            reason: "entry is accessible by group or other users".into(),
        });
    }
    Ok(())
}

fn validate_directory_entries(
    directory: &Path,
    expected: &[(&str, bool)],
) -> Result<(), SessionError> {
    let mut found = Vec::new();
    for entry in fs::read_dir(directory)
        .map_err(|source| fs_error("read session directory", directory, source))?
    {
        let entry =
            entry.map_err(|source| fs_error("read session-directory entry", directory, source))?;
        let path = entry.path();
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| SessionError::UnsafePath {
                path: path.clone(),
                reason: "entry name is not UTF-8".into(),
            })?;
        let Some((_, should_be_directory)) =
            expected.iter().find(|(candidate, _)| *candidate == name)
        else {
            return Err(SessionError::UnsafePath {
                path,
                reason: "unknown entry; inspect it manually".into(),
            });
        };
        let metadata = fs::symlink_metadata(&path)
            .map_err(|source| fs_error("inspect session-directory entry", &path, source))?;
        let correct_type = if *should_be_directory {
            metadata.file_type().is_dir()
        } else {
            metadata.file_type().is_file()
        };
        if !correct_type {
            return Err(SessionError::UnsafePath {
                path,
                reason: "entry has the wrong type".into(),
            });
        }
        validate_existing_permissions(&entry.path(), &metadata)?;
        found.push(name);
    }
    if let Some((missing, _)) = expected
        .iter()
        .find(|(name, _)| !found.iter().any(|item| item == name))
    {
        return Err(SessionError::UnsafePath {
            path: directory.join(missing),
            reason: "required entry is missing".into(),
        });
    }
    Ok(())
}

fn remove_if_present(path: &Path) -> Result<(), SessionError> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(fs_error("remove closed session", path, source)),
    }
}

pub(super) fn now_parts() -> Result<(i64, i64), SessionError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SessionError::InvalidSystemTime)?;
    Ok((
        i64::try_from(duration.as_secs()).map_err(|_| SessionError::InvalidSystemTime)?,
        i64::from(duration.subsec_nanos()),
    ))
}

pub(super) fn stale_path(path: &Path, reason: String) -> SessionError {
    SessionError::StaleSession {
        path: path.to_path_buf(),
        reason,
    }
}

pub(super) fn fs_error(
    operation: &'static str,
    path: &Path,
    source: std::io::Error,
) -> SessionError {
    let kind = source.kind();
    SessionError::Filesystem {
        path: path.to_path_buf(),
        source: std::io::Error::new(kind, format!("{operation} failed: {source}")),
    }
}

pub(super) fn db_error(
    operation: &'static str,
    path: &Path,
    source: rusqlite::Error,
) -> SessionError {
    SessionError::Database {
        operation,
        path: path.to_path_buf(),
        source,
    }
}
