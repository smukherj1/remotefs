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
use std::io::{ErrorKind, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::config::Config;
use crate::digest::Digest;
use crate::error_context::{ResultContext, ResultContextError};

pub use crate::tree::NodeKind;
pub use cache::BlobWriter;

use cache::BlobCache;
use overlay::OverlayStore;
use store::{ROOT_INODE_ID, ReadSource, SessionStore};

const LOCK_RECORD_VERSION: u32 = 1;
const MIN_TIMESTAMP_SECONDS: i64 = -62_135_596_800;
const MAX_TIMESTAMP_SECONDS: i64 = 253_402_300_799;

/// Session-stable inode identity that is positive and representable by SQLite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InodeId(i64);

impl InodeId {
    /// Root inode shared by every mounted workspace.
    pub const ROOT: Self = Self(ROOT_INODE_ID);

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
    /// Daemon process ID.
    pub daemon_pid: u32,
    /// Path to the Unix socket serving the daemon's control endpoint.
    pub control_endpoint: PathBuf,
    /// Path to the file containing the daemon logs for a session.
    pub log_path: PathBuf,
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
    /// Session state does not exist or is malformed.
    #[error(
        "missing or malformed session state at `{path}`; delete the session directory (if it exists) and start again: {reason}"
    )]
    InvalidSession { path: PathBuf, reason: String },
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
    /// A remote-backed file was read before its blob was admitted to the cache.
    #[error("blob {digest} is missing from the local cache")]
    MissingBlob { digest: Digest },
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
    root_digest: Digest,
    mountpoint: PathBuf,
    layout: SessionLayout,
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
        tracing::info!(
            "Session::open(home={}, root_digest={}, mountpoint={}",
            config.rfs_home.display(),
            root_digest,
            mountpoint.as_ref().display()
        );
        let mountpoint = canonicalize_mountpoint(mountpoint.as_ref()).with_context(|| {
            format!(
                "unable to canonizalize session mountpoint {}",
                mountpoint.as_ref().display()
            )
        })?;
        ensure_writable_home(&config)
            .with_context(|| "unable to set up the session writable home directory".to_string())?;
        let layout = SessionLayout::new(&config.rfs_home);
        let cache = BlobCache::open(layout.cache.clone()).with_context(|| {
            format!(
                "while initializing the blob cache in {}",
                layout.cache.display()
            )
        })?;

        let active = ActiveSession::open(
            layout.session.clone(),
            layout.session_lock.clone(),
            root_digest.clone(),
            mountpoint.clone(),
        )
        .with_context(|| {
            format!(
                "while initializing active session in {} with lock {}",
                layout.session.display(),
                layout.session_lock.display()
            )
        })?;

        Ok(Self {
            root_digest,
            mountpoint,
            layout,
            cache,
            active,
        })
    }

    /// Returns immutable startup and live-status facts without rereading SQLite.
    pub fn info(&self) -> SessionInfo {
        SessionInfo {
            root_digest: self.root_digest.clone(),
            mountpoint: self.mountpoint.clone(),
            daemon_pid: std::process::id(),
            control_endpoint: self.layout.control_endpoint.clone(),
            log_path: self.active.layout.log_path.clone(),
        }
    }

    /// Derives the fixed control endpoint without opening SQLite or mutating state.
    pub fn control_endpoint(config: &Config) -> Result<PathBuf, SessionError> {
        ensure_reader_home(config)
            .with_context(|| "unable to open home directory for inspection".to_string())?;
        let layout = SessionLayout::new(&config.rfs_home);
        Ok(layout.control_endpoint.clone())
    }

    /// Performs one-shot read-only retained-session inspection.
    pub fn inspect(config: &Config) -> Result<Option<SessionInfo>, SessionError> {
        ensure_reader_home(config)
            .with_context(|| "unable to open home directory for inspection".to_string())?;
        let layout = SessionLayout::new(&config.rfs_home);
        ActiveSession::inspect(&layout.session)
    }

    /// Returns one visible inode from authoritative SQLite state.
    pub fn node(&self, inode: InodeId) -> Result<Inode, SessionError> {
        self.active.with_store(|store| store.node(inode))
    }

    /// Looks up a child or reports the remote directory that still needs loading.
    pub fn lookup(&self, parent: InodeId, name: &str) -> Result<Lookup<Inode>, SessionError> {
        self.active.with_store(|store| store.lookup(parent, name))
    }

    /// Lists a directory or reports the remote directory that still needs loading.
    pub fn list_directory(&self, inode: InodeId) -> Result<Lookup<Vec<Inode>>, SessionError> {
        self.active.with_store(|store| store.list_directory(inode))
    }

    /// Atomically records a complete decoded remote child set.
    pub fn materialize_directory(
        &self,
        parent: InodeId,
        remote_digest: &Digest,
        remote_children: Vec<RemoteChild>,
    ) -> Result<Vec<Inode>, SessionError> {
        self.active.with_store(|store| {
            store.materialize_directory(parent, remote_digest, &remote_children)
        })
    }

    /// Checks if the given digest exists in the local blob cache.
    pub fn exists(&self, digest: &Digest) -> bool {
        self.cache.exists(digest)
    }

    /// Reads a complete admitted object, returning `None` on a cache miss.
    pub fn read_blob(&self, digest: &Digest) -> Result<Option<Bytes>, SessionError> {
        self.cache.read_blob(digest)
    }

    /// Returns the immutable digest for a remote-backed file.
    ///
    /// Overlay-backed files return `None`. The inode must identify a visible
    /// regular file.
    pub fn remote_file_digest(&self, inode: InodeId) -> Result<Option<Digest>, SessionError> {
        match self
            .active
            .with_store(|store| store.get_file_source(inode))?
        {
            ReadSource::Remote(digest) => Ok(Some(digest)),
            ReadSource::Overlay(_) => Ok(None),
        }
    }

    /// Resolves current inode backing and serves a range from local storage.
    ///
    /// A remote-backed file's verified blob must already be admitted to the
    /// cache. Overlay-backed files are read directly from the active session.
    pub fn read_range(
        &self,
        inode: InodeId,
        offset: u64,
        size: usize,
    ) -> Result<Bytes, SessionError> {
        match self
            .active
            .with_store(|store| store.get_file_source(inode))?
        {
            ReadSource::Remote(digest) => self
                .cache
                .read_range(&digest, offset, size)?
                .ok_or(SessionError::MissingBlob { digest }),
            ReadSource::Overlay(path) => self.active.read_overlay_range(&path, offset, size),
        }
    }

    /// Rechecks cache presence and creates an opaque streaming writer on a miss.
    pub fn start_blob_download(&self, digest: &Digest) -> Result<BlobWriter, SessionError> {
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

struct ActiveSessionLayout {
    log_path: PathBuf,
    db_path: PathBuf,
    overlay_dir: PathBuf,
}

impl ActiveSessionLayout {
    fn new(session_dir: PathBuf) -> Self {
        Self {
            log_path: session_dir.join("session.log"),
            db_path: session_dir.join("session.db"),
            overlay_dir: session_dir.join("overlay"),
        }
    }
}

struct ActiveSession {
    layout: ActiveSessionLayout,
    resources: Mutex<Option<ActiveResources>>,
}

struct ActiveResources {
    store: SessionStore,
    overlay: OverlayStore,
    _lock: SessionLock,
}

impl ActiveSession {
    fn open(
        session_dir: PathBuf,
        lock_path: PathBuf,
        root_digest: Digest,
        mountpoint: PathBuf,
    ) -> Result<Self, SessionError> {
        let session_id = Uuid::new_v4().to_string();
        tracing::info!(
            "ActiveSession(session_dir={}, lock_path={}, root_digest={}, mountpoint={}), session_id={}",
            session_dir.display(),
            lock_path.display(),
            root_digest,
            mountpoint.display(),
            session_id
        );
        let mut lock = SessionLock::acquire(&lock_path).with_context(|| {
            format!(
                "unable to acquire lock {} when creating a new active session",
                lock_path.display()
            )
        })?;
        lock.write_record(
            &lock_path,
            &LockRecord {
                record_version: LOCK_RECORD_VERSION,
                session_id: session_id.clone(),
                pid: std::process::id(),
            },
        )?;

        tracing::info!("Creating new session directory: {}", session_dir.display());
        remove_if_present(&session_dir)
            .with_context(|| "removing old session directory".to_string())?;
        create_dir_if_absent(&session_dir)
            .with_context(|| "creating session directory".to_string())?;

        let layout = ActiveSessionLayout::new(session_dir);
        tracing::info!("Creating logs file: {}", layout.log_path.display());
        create_empty_file(&layout.log_path)
            .with_context(|| "creating file for daemon session logs".to_string())?;
        tracing::info!("Creating session db: {}", layout.db_path.display());
        create_empty_file(&layout.db_path)
            .with_context(|| "creating file for session db".to_string())?;
        let overlay = OverlayStore::open(layout.overlay_dir.clone())?;
        let store = SessionStore::create(
            layout.db_path.clone(),
            session_id,
            std::process::id(),
            root_digest,
            mountpoint,
        )?;
        Ok(Self {
            layout,
            resources: Mutex::new(Some(ActiveResources {
                store,
                overlay,
                _lock: lock,
            })),
        })
    }

    fn inspect(root: &Path) -> Result<Option<SessionInfo>, SessionError> {
        if !root.exists() {
            return Ok(None);
        }
        let layout = ActiveSessionLayout::new(root.to_path_buf());
        let stored = SessionStore::inspect(&layout.db_path)
            .with_context(|| "inspecting session status from session db".to_string())?;
        Ok(Some(SessionInfo {
            root_digest: stored.root_digest,
            mountpoint: stored.mountpoint,
            daemon_pid: stored.daemon_pid,
            control_endpoint: PathBuf::from(""),
            log_path: layout.log_path,
        }))
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
        // TODO: Use the native file like API provided by the standard fs crate instead of doing
        // usafe calls.
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
        return Ok(());
    }

    Err(SessionError::InvalidSession {
        path: config.rfs_home.clone(),
        reason: format!(
            "unable to query session status because session home directory {} does not exist",
            config.rfs_home.display()
        ),
    })
}

// Wraps the filesystem directory creation method to return a SessionError.
fn create_dir_if_absent(path: &Path) -> Result<(), SessionError> {
    match fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(source) => Err(fs_error("create directory", path, source)),
    }
}

// Wraps the filesystem file creation method to return a SessionError.
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
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(fs_error("local state cleanup", path, source)),
    }
}

fn now_parts() -> Result<(i64, i64), SessionError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SessionError::InvalidSystemTime)?;
    Ok((
        i64::try_from(duration.as_secs()).map_err(|_| SessionError::InvalidSystemTime)?,
        i64::from(duration.subsec_nanos()),
    ))
}

fn stale_path(path: &Path, reason: String) -> SessionError {
    SessionError::InvalidSession {
        path: path.to_path_buf(),
        reason,
    }
}

fn fs_error(operation: &'static str, path: &Path, source: std::io::Error) -> SessionError {
    let kind = source.kind();
    SessionError::Filesystem {
        path: path.to_path_buf(),
        source: std::io::Error::new(kind, format!("{operation} failed: {source}")),
    }
}

/// Layout of files under the home directory maintain by the rfs daemon. This
/// includes both the blob cache that can live across sessions as well as
/// files created for a specific daemon session.
struct SessionLayout {
    /// Directory containing cached blobs downloaded from the CAS server.
    cache: PathBuf,
    /// Directory containing files and directories for the active session.
    session: PathBuf,
    /// Lock file for the active session.
    session_lock: PathBuf,
    /// The UDS socket serving the daemon's control endpoint.
    control_endpoint: PathBuf,
}

impl SessionLayout {
    // Initialize a new session layout under the given rfsd home directory.
    fn new(home: &Path) -> Self {
        let session = home.join("session");
        Self {
            cache: home.join("cache"),
            session: session.clone(),
            session_lock: home.join("session.lock"),
            control_endpoint: session.join("control.sock"),
        }
    }
}
