//! Durable local state and exclusive foreground-session ownership.
//!
//! Public capabilities are synchronous because their callers include FUSE
//! callbacks. All writable database work is serialized on one daemon-owned
//! thread and one `rusqlite` connection. SQL defines persistence shape and
//! relational constraints; this module validates domain values and translates
//! rows to and from Rust types.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error_context::{ResultContext as _, ResultContextError};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::config::Config;
use crate::digest::Digest;

/// Fixed inode number for the root of every mount session.
pub const ROOT_INODE: u64 = 1;

/// Failures while opening, inspecting, or mutating durable RemoteFS state.
#[derive(Debug, Error)]
pub enum StateError {
    /// A local state entry violates the ownership, permission, or layout policy.
    #[error("local state path `{path}` is unsafe: {reason}")]
    UnsafePath {
        /// Unsafe state path.
        path: PathBuf,
        /// Violated safety rule.
        reason: String,
    },
    /// Another process holds the exclusive session lock.
    #[error("another RemoteFS session owns `{path}`{owner}")]
    ActiveSession {
        /// Stable session lock path.
        path: PathBuf,
        /// Best-effort owner diagnostics, including leading punctuation.
        owner: String,
    },
    /// Retained state is incomplete, inconsistent, or unsupported.
    #[error(
        "stale or malformed session state at `{path}`; delete `RFS_HOME` and start again: {reason}"
    )]
    StaleSession {
        /// Active-session or database path containing stale state.
        path: PathBuf,
        /// Failed durable invariant.
        reason: String,
    },
    /// A named filesystem operation failed for a state path.
    #[error("filesystem operation on `{path}` failed: {source}")]
    Filesystem {
        /// State path involved in the operation.
        path: PathBuf,
        /// Underlying filesystem failure.
        #[source]
        source: std::io::Error,
    },
    /// A named SQLite operation failed for the session database.
    #[error("SQLite {operation} on `{path}` failed: {source}")]
    Database {
        /// Database operation that failed.
        operation: &'static str,
        /// Session database path.
        path: PathBuf,
        /// Underlying SQLite failure.
        #[source]
        source: rusqlite::Error,
    },
    /// The state worker could not execute or return a command.
    #[error("state worker operation `{operation}` for `{path}` failed: {reason}")]
    Worker {
        /// Worker command or lifecycle operation.
        operation: &'static str,
        /// Session database path owned by the worker.
        path: PathBuf,
        /// Channel or thread failure detail.
        reason: String,
    },
    /// The configured mountpoint cannot be canonicalized to a directory.
    #[error("mountpoint `{path}` must be an existing directory")]
    InvalidMountpoint {
        /// Original mountpoint path supplied by the caller.
        path: PathBuf,
    },
    /// Current system time cannot be represented by the durable timestamp fields.
    #[error("system time is outside the supported timestamp range")]
    InvalidSystemTime,
    /// Enables wrapping a StateError with additional context.
    #[error("{operation}: {source}")]
    Context {
        operation: String,
        #[source]
        source: Box<StateError>,
    },
}

impl ResultContextError for StateError {
    fn with_context(self, operation: String) -> Self {
        return StateError::Context {
            operation,
            source: Box::new(self),
        };
    }
}

/// Closed set of durable session lifecycle states stored in SQLite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionLifecycle {
    /// Session metadata exists but startup has not completed.
    Initializing,
    /// The daemon owns the session and accepts control requests.
    Active,
    /// The daemon completed a clean, inspectable shutdown.
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

/// Metadata needed to create a daemon session.
#[derive(Debug, Clone)]
pub struct SessionStartup {
    /// Root digest fixed for the lifetime of the daemon session.
    pub root_digest: Digest,
    /// Existing mountpoint directory; canonicalized during state creation.
    pub mountpoint: PathBuf,
    /// Process identifier recorded in session and lock metadata.
    pub daemon_pid: u32,
}

impl SessionStartup {
    /// Creates startup metadata for the current process.
    pub fn new(root_digest: Digest, mountpoint: PathBuf) -> Self {
        Self {
            root_digest,
            mountpoint,
            daemon_pid: std::process::id(),
        }
    }
}

/// State information exposed to clients without persistence types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionView {
    /// Process identifier recorded when the session was created.
    pub daemon_pid: u32,
    /// Current durable lifecycle state.
    pub state: SessionLifecycle,
    /// Root digest mounted by the session.
    pub root_digest: Digest,
    /// Canonical mountpoint owned by the session.
    pub mountpoint: PathBuf,
    /// Unix control endpoint for a live session.
    pub control_endpoint: PathBuf,
    /// Shared cache path used for status presentation.
    pub cache_path: PathBuf,
    /// Active session path used for status presentation.
    pub session_path: PathBuf,
}

/// Read-only retained-session capability.
pub trait SessionStateReader: Send + Sync {
    /// Returns retained session metadata, or `None` when no session exists.
    fn session(&self) -> Result<Option<SessionView>, StateError>;
    /// Returns the fixed endpoint to probe for a live daemon.
    fn control_endpoint(&self) -> PathBuf;
}

/// Daemon-only state lifecycle capability.
pub trait DaemonState: SessionStateReader {
    /// Returns durable inode and directory-materialization state.
    fn filesystem_state(&self) -> Arc<dyn FilesystemState>;
    /// Marks the session cleanly closed and releases its ownership lock.
    fn close(self: Box<Self>) -> Result<(), StateError>;
}

/// Kind of an immutable remote node recorded in the inode table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteNodeKind {
    /// Regular file backed by a content digest.
    File,
    /// Directory backed by another REAPI directory digest.
    Directory,
    /// Symbolic link backed by its exact target.
    Symlink,
}

/// Stable identity of one child in an immutable remote directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteNodeIdentity {
    /// UTF-8 name relative to the materialized parent.
    pub name: String,
    /// Immutable remote node kind.
    pub kind: RemoteNodeKind,
    /// File/directory digest or exact symlink target.
    pub content_identity: String,
}

/// Session-stable inode allocated for a materialized remote child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedInode {
    /// Synthetic inode stable for this mount session.
    pub inode: u64,
    /// Child name associated with this allocation.
    pub name: String,
}

/// Daemon filesystem persistence without exposing SQLite types.
pub trait FilesystemState: Send + Sync {
    /// Atomically records a fetched directory and allocates stable child inodes.
    ///
    /// Re-materializing the same immutable directory is idempotent. A changed
    /// digest, child identity, or child set is rejected as stale session state.
    fn materialize_remote_directory(
        &self,
        inode: u64,
        digest: &Digest,
        children: &[RemoteNodeIdentity],
    ) -> Result<Vec<MaterializedInode>, StateError>;
}

/// Opens retained session state without creating, migrating, locking, or writing.
///
/// Returns a reader whose [`SessionStateReader::session`] method yields `None`
/// when no active state directory exists. Existing malformed or unsupported
/// state is reported as [`StateError::StaleSession`].
pub fn open_reader(config: Config) -> Result<Box<dyn SessionStateReader>, StateError> {
    let paths = StatePaths::for_reader(&config)?;
    Ok(Box::new(StateReader { paths }))
}

/// Opens writable daemon state and establishes a fresh active session.
///
/// This creates the private state layout when needed, replaces only a cleanly
/// closed session, acquires the exclusive session lock, and starts the SQLite
/// worker. Unsafe or stale retained state is preserved and returned as an error.
pub fn open_daemon(
    config: Config,
    startup: SessionStartup,
) -> Result<Box<dyn DaemonState>, StateError> {
    let paths = StatePaths::from_config(&config)?;
    let store = SessionStore::create(paths, startup)?;
    Ok(Box::new(store))
}

/// Resolves `path` to a canonical existing directory.
///
/// Symlinks are resolved. Any canonicalization or metadata failure, and any
/// non-directory result, is returned as [`StateError::InvalidMountpoint`] with
/// the original path.
pub fn canonicalize_mountpoint(path: &Path) -> Result<PathBuf, StateError> {
    let canonical = fs::canonicalize(path).map_err(|_| StateError::InvalidMountpoint {
        path: path.to_path_buf(),
    })?;
    if !fs::metadata(&canonical)
        .map_err(|_| StateError::InvalidMountpoint {
            path: path.to_path_buf(),
        })?
        .is_dir()
    {
        return Err(StateError::InvalidMountpoint {
            path: path.to_path_buf(),
        });
    }
    Ok(canonical)
}

const LOCK_RECORD_VERSION: u32 = 1;
const SCHEMA_VERSION: i64 = 1;
const SCHEMA_SQL: &str = include_str!("state/schema.sql");

/// Operation context stored inside an `io::Error` without changing the public error shape.
#[derive(Debug, Error)]
#[error("{operation} failed: {source}")]
struct FilesystemOperationError {
    /// Filesystem operation attempted.
    operation: &'static str,
    /// Original I/O failure.
    #[source]
    source: std::io::Error,
}

/// Canonical `RFS_HOME` and its fixed descendants.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StatePaths {
    /// Backing storage for the canonical state root.
    stored_home: PathBuf,
    /// Backing storage for the shared cache directory.
    stored_cache: PathBuf,
    /// Backing storage for the blob cache directory.
    stored_blob_cache: PathBuf,
    /// Backing storage for the directory-blob cache directory.
    stored_directory_cache: PathBuf,
    /// Backing storage for the active-session directory.
    stored_active: PathBuf,
    /// Backing storage for the stable lock file outside the active-session tree.
    stored_lock: PathBuf,
    /// Backing storage for the active-session database.
    stored_database: PathBuf,
    /// Backing storage for the daemon log.
    stored_log: PathBuf,
    /// Backing storage for the daemon control socket.
    stored_control_socket: PathBuf,
    /// Backing storage for the overlay directory.
    stored_overlay: PathBuf,
    /// Backing storage for durable overlay file data.
    stored_overlay_data: PathBuf,
    /// Backing storage for overlay temporary files.
    stored_overlay_tmp: PathBuf,
}

impl StatePaths {
    /// Creates the daemon path set, including a missing home, and validates it.
    fn from_config(config: &Config) -> Result<Self, StateError> {
        if !config.rfs_home.exists() {
            create_home_private(&config.rfs_home)?;
        }
        let home = canonical_private_home(&config.rfs_home).with_context(|| {
            format!(
                "unable to cannonicalize home path {}",
                config.rfs_home.display()
            )
        })?;
        let paths = Self::init(home);
        paths.validate_top_level().with_context(|| {
            format!(
                "validating structure of home directory {}",
                paths.home().display()
            )
        })?;
        Ok(paths)
    }

    /// Creates a read-only path set without creating a missing home.
    fn for_reader(config: &Config) -> Result<Self, StateError> {
        if !config.rfs_home.exists() {
            return Ok(Self::init(config.rfs_home.clone()));
        }
        let home = canonical_private_home(&config.rfs_home).with_context(|| {
            format!(
                "unable to cannonicalize home path {}",
                config.rfs_home.display()
            )
        })?;
        Ok(Self::init(home))
    }

    /// Derives and stores every fixed path under the daemon home directory.
    fn init(home: PathBuf) -> Self {
        let cache = home.join("cache");
        let active = home.join("active");
        let overlay = active.join("overlay");
        Self {
            stored_lock: home.join("active.lock"),
            stored_blob_cache: cache.join("blobs"),
            stored_directory_cache: cache.join("dirs"),
            stored_database: active.join("session.db"),
            stored_log: active.join("rfsd.log"),
            stored_control_socket: active.join("control.sock"),
            stored_overlay_data: overlay.join("data"),
            stored_overlay_tmp: overlay.join("tmp"),
            stored_home: home,
            stored_cache: cache,
            stored_active: active,
            stored_overlay: overlay,
        }
    }

    /// Returns the canonical state root, or configured missing reader root.
    fn home(&self) -> &Path {
        &self.stored_home
    }

    /// Returns the shared cache directory.
    fn cache(&self) -> &Path {
        &self.stored_cache
    }

    /// Returns the blob cache directory.
    fn blob_cache(&self) -> &Path {
        &self.stored_blob_cache
    }

    /// Returns the directory-blob cache directory.
    fn directory_cache(&self) -> &Path {
        &self.stored_directory_cache
    }

    /// Returns the active-session directory.
    fn active(&self) -> &Path {
        &self.stored_active
    }

    /// Returns the stable lock file outside the active-session tree.
    fn lock(&self) -> &Path {
        &self.stored_lock
    }

    /// Returns the active-session database.
    fn database(&self) -> &Path {
        &self.stored_database
    }

    /// Returns the daemon log.
    fn log(&self) -> &Path {
        &self.stored_log
    }

    /// Returns the daemon control socket.
    fn control_socket(&self) -> &Path {
        &self.stored_control_socket
    }

    /// Returns the overlay directory.
    fn overlay(&self) -> &Path {
        &self.stored_overlay
    }

    /// Returns the durable overlay-data directory.
    fn overlay_data(&self) -> &Path {
        &self.stored_overlay_data
    }

    /// Returns the overlay temporary-file directory.
    fn overlay_tmp(&self) -> &Path {
        &self.stored_overlay_tmp
    }

    /// Derives the sharded blob-cache path for a test digest.
    #[cfg(test)]
    fn blob_cache_path(&self, digest: &Digest) -> PathBuf {
        cache_path(self.blob_cache(), digest)
    }

    /// Derives the sharded directory-cache path for a test digest.
    #[cfg(test)]
    fn directory_cache_path(&self, digest: &Digest) -> PathBuf {
        cache_path(self.directory_cache(), digest)
    }

    /// Rejects unknown, symlinked, wrongly typed, or overly permissive entries.
    fn validate_top_level(&self) -> Result<(), StateError> {
        for entry in fs::read_dir(self.home())
            .map_err(|source| fs_error("read state root", self.home(), source))?
        {
            let entry =
                entry.map_err(|source| fs_error("read state-root entry", self.home(), source))?;
            let name = entry.file_name();
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)
                .map_err(|source| fs_error("inspect state-root entry", &path, source))?;
            let valid = match name.to_str() {
                Some("active.lock") => metadata.file_type().is_file(),
                Some("cache" | "active") => metadata.file_type().is_dir(),
                _ => false,
            };
            if !valid {
                return Err(StateError::UnsafePath {
                    path,
                    reason: "unknown entry, symlink, or wrong entry type; inspect it manually"
                        .into(),
                });
            }
            validate_existing_permissions(&path, &metadata)?;
        }
        Ok(())
    }
}

/// JSON diagnostics stored in the stable session lock file.
#[derive(Debug, Serialize, Deserialize)]
struct LockRecord {
    /// Lock record format version.
    record_version: u32,
    /// UUID shared with the session database row.
    session_id: String,
    /// Process identifier of the owning daemon.
    pid: u32,
}

/// Validated session fields used outside the SQLite repository.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionMetadata {
    /// Process identifier recorded at session creation.
    daemon_pid: u32,
    /// Validated durable lifecycle.
    state: SessionLifecycle,
    /// Root digest fixed for the session.
    root_digest: Digest,
    /// Canonical session mountpoint.
    mountpoint: PathBuf,
}

/// Advisory lock held for the full writable session lifetime.
struct SessionLock {
    /// Open lock file whose descriptor owns the advisory lock.
    file: File,
}

impl SessionLock {
    /// Opens, secures, and non-blockingly acquires the stable session lock.
    fn acquire(paths: &StatePaths) -> Result<Self, StateError> {
        let path = paths.lock();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&path)
            .map_err(|source| fs_error("open session lock", &path, source))?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .map_err(|source| fs_error("secure session lock", &path, source))?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let owner = fs::read_to_string(&path)
                .ok()
                .and_then(|value| serde_json::from_str::<LockRecord>(&value).ok())
                .map(|value| format!(" (pid {}, session {})", value.pid, value.session_id))
                .unwrap_or_else(|| " (owner diagnostics unavailable)".into());
            return Err(StateError::ActiveSession {
                path: path.to_path_buf(),
                owner,
            });
        }
        Ok(Self { file })
    }

    /// Replaces lock diagnostics after exclusive ownership is established.
    fn write_record(&mut self, path: &Path, record: &LockRecord) -> Result<(), StateError> {
        self.file
            .set_len(0)
            .map_err(|source| fs_error("truncate session lock", path, source))?;
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|source| fs_error("rewind session lock", path, source))?;
        serde_json::to_writer(&mut self.file, record).map_err(|source| StateError::UnsafePath {
            path: path.to_path_buf(),
            reason: format!("cannot encode lock record: {source}"),
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

/// Typed commands serialized through the daemon-owned database connection.
enum WorkerCommand {
    /// Reads the active session metadata.
    Metadata(mpsc::SyncSender<Result<SessionMetadata, StateError>>),
    /// Materializes one immutable remote directory transactionally.
    Materialize {
        /// Parent directory inode.
        inode: u64,
        /// Expected immutable directory digest.
        digest: Digest,
        /// Complete remote child set.
        children: Vec<RemoteNodeIdentity>,
        /// Command result channel.
        reply: mpsc::SyncSender<Result<Vec<MaterializedInode>, StateError>>,
    },
    /// Records a clean close and terminates the worker.
    Close {
        /// Close timestamp seconds since the Unix epoch.
        seconds: i64,
        /// Close timestamp nanosecond fraction.
        nanos: i64,
        /// Command result channel.
        reply: mpsc::SyncSender<Result<(), StateError>>,
    },
}

/// Cloneable synchronous command client for the state worker.
#[derive(Clone)]
struct WorkerClient {
    /// Database path used in worker transport errors.
    path: PathBuf,
    /// Command sender owned by daemon capabilities.
    sender: mpsc::Sender<WorkerCommand>,
}

impl WorkerClient {
    /// Sends one typed command and waits for its typed result.
    fn request<T>(
        &self,
        operation: &'static str,
        make: impl FnOnce(mpsc::SyncSender<Result<T, StateError>>) -> WorkerCommand,
    ) -> Result<T, StateError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.sender
            .send(make(reply))
            .map_err(|_| worker_error(&self.path, operation, "worker stopped"))?;
        response
            .recv()
            .map_err(|_| worker_error(&self.path, operation, "worker dropped its response"))?
    }
}

/// Writable session capability that keeps the worker and lock alive.
struct SessionStore {
    /// Canonical paths used to construct public views.
    paths: StatePaths,
    /// Serialized database command client.
    worker: WorkerClient,
    /// Exclusive lock released only after all other fields are dropped.
    _lock: SessionLock,
}

/// Filesystem-scoped view of the state worker.
struct FilesystemStore {
    /// Serialized database command client.
    worker: WorkerClient,
}

impl SessionStore {
    /// Establishes one new session, replacing only validated closed state.
    fn create(paths: StatePaths, mut startup: SessionStartup) -> Result<Self, StateError> {
        startup.mountpoint = canonicalize_mountpoint(&startup.mountpoint)?;
        let session_id = Uuid::new_v4().to_string();
        let record = LockRecord {
            record_version: LOCK_RECORD_VERSION,
            session_id: session_id.clone(),
            pid: startup.daemon_pid,
        };
        let mut lock = SessionLock::acquire(&paths)?;
        if paths.active().exists() {
            validate_closed_session(&paths)?;
            remove_if_present(paths.active())?;
        }
        lock.write_record(paths.lock(), &record)?;
        create_layout(&paths)?;
        create_file_private(paths.log())?;
        create_file_private(paths.database())?;
        let worker = start_worker(paths.database().to_path_buf(), session_id, startup)?;
        Ok(Self {
            paths,
            worker,
            _lock: lock,
        })
    }
}

/// Read-only retained-state implementation.
struct StateReader {
    /// Existing or configured state paths; never created by this reader.
    paths: StatePaths,
}

impl SessionStateReader for StateReader {
    fn session(&self) -> Result<Option<SessionView>, StateError> {
        retained_session_result(&self.paths)
    }

    fn control_endpoint(&self) -> PathBuf {
        self.paths.control_socket().to_path_buf()
    }
}

impl SessionStateReader for SessionStore {
    fn session(&self) -> Result<Option<SessionView>, StateError> {
        self.worker
            .request("read active session", WorkerCommand::Metadata)
            .map(|metadata| Some(session_view(&self.paths, metadata)))
    }

    fn control_endpoint(&self) -> PathBuf {
        self.paths.control_socket().to_path_buf()
    }
}

impl DaemonState for SessionStore {
    fn filesystem_state(&self) -> Arc<dyn FilesystemState> {
        Arc::new(FilesystemStore {
            worker: self.worker.clone(),
        })
    }

    fn close(self: Box<Self>) -> Result<(), StateError> {
        let (seconds, nanos) = now_parts()?;
        self.worker
            .request("close session", |reply| WorkerCommand::Close {
                seconds,
                nanos,
                reply,
            })
    }
}

impl FilesystemState for FilesystemStore {
    fn materialize_remote_directory(
        &self,
        inode: u64,
        digest: &Digest,
        children: &[RemoteNodeIdentity],
    ) -> Result<Vec<MaterializedInode>, StateError> {
        self.worker
            .request("materialize remote directory", |reply| {
                WorkerCommand::Materialize {
                    inode,
                    digest: digest.clone(),
                    children: children.to_vec(),
                    reply,
                }
            })
    }
}

/// Starts the database worker and waits until initialization succeeds.
fn start_worker(
    path: PathBuf,
    session_id: String,
    startup: SessionStartup,
) -> Result<WorkerClient, StateError> {
    let (sender, receiver) = mpsc::channel();
    let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
    let worker_path = path.clone();
    thread::Builder::new()
        .name("rfs-state".into())
        .spawn(move || {
            let result = run_worker(worker_path, session_id, startup, receiver, ready_sender);
            if let Err(error) = result {
                // Startup failures are returned through `ready_sender`; later
                // failures make pending command channels disconnect.
                tracing::error!(error = %error, "state worker stopped");
            }
        })
        .map_err(|error| worker_error(&path, "spawn worker", error.to_string()))?;
    let startup_result = ready_receiver
        .recv()
        .map_err(|_| worker_error(&path, "start worker", "worker stopped during startup"))?;
    startup_result.map(|()| WorkerClient { path, sender })
}

/// Owns the writable connection and serially dispatches commands until close.
fn run_worker(
    path: PathBuf,
    session_id: String,
    startup: SessionStartup,
    receiver: mpsc::Receiver<WorkerCommand>,
    ready: mpsc::SyncSender<Result<(), StateError>>,
) -> Result<(), StateError> {
    let mut connection = match open_database(&path, false) {
        Ok(connection) => connection,
        Err(error) => {
            let _ = ready.send(Err(error));
            return Ok(());
        }
    };
    if let Err(error) = initialize_database(&mut connection, &path, &session_id, &startup) {
        let _ = ready.send(Err(error));
        return Ok(());
    }
    let _ = ready.send(Ok(()));

    while let Ok(command) = receiver.recv() {
        match command {
            WorkerCommand::Metadata(reply) => {
                let _ = reply.send(read_session_metadata(&connection, &path));
            }
            WorkerCommand::Materialize {
                inode,
                digest,
                children,
                reply,
            } => {
                let result =
                    materialize_remote_directory(&mut connection, &path, inode, &digest, &children);
                let _ = reply.send(result);
            }
            WorkerCommand::Close {
                seconds,
                nanos,
                reply,
            } => {
                let result = close_session(&mut connection, &path, seconds, nanos);
                let _ = reply.send(result);
                return Ok(());
            }
        }
    }
    Ok(())
}

/// Creates the baseline schema and atomically inserts an active session root.
fn initialize_database(
    connection: &mut Connection,
    path: &Path,
    session_id: &str,
    startup: &SessionStartup,
) -> Result<(), StateError> {
    prepare_schema(connection, path)?;
    let (seconds, nanos) = now_parts()?;
    let mountpoint = startup
        .mountpoint
        .to_str()
        .ok_or_else(|| stale_path(path, "mountpoint is not UTF-8".into()))?;
    validate_session_row(
        path,
        &RawSession {
            singleton: 1,
            session_id: session_id.to_owned(),
            daemon_pid: i64::from(startup.daemon_pid),
            lifecycle: "initializing".into(),
            root_digest_hash: startup.root_digest.hash().to_owned(),
            root_digest_size: startup.root_digest.size_bytes(),
            mountpoint: mountpoint.to_owned(),
            created_at_seconds: seconds,
            created_at_nanos: nanos,
            closed_at_seconds: None,
            closed_at_nanos: None,
            log_level: "info".into(),
            log_format: "text".into(),
        },
    )?;
    let transaction = connection
        .transaction()
        .map_err(|source| db_error("begin session initialization", path, source))?;
    transaction
        .execute(
            "INSERT INTO session_metadata (
                singleton, session_id, daemon_pid, lifecycle,
                root_digest_hash, root_digest_size, mountpoint,
                created_at_seconds, created_at_nanos,
                closed_at_seconds, closed_at_nanos, log_level, log_format
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, NULL, ?10, ?11)",
            params![
                1_i64,
                session_id,
                i64::from(startup.daemon_pid),
                "initializing",
                startup.root_digest.hash(),
                startup.root_digest.size_bytes(),
                mountpoint,
                seconds,
                nanos,
                "info",
                "text",
            ],
        )
        .map_err(|source| db_error("insert session metadata", path, source))?;
    transaction
        .execute(
            "INSERT INTO inodes (
                inode, parent_inode, name, kind, remote_digest, symlink_target,
                overlay_file, mode, mtime_seconds, mtime_nanos, tombstone,
                content_dirty, tree_dirty
             ) VALUES (?1, NULL, '', 'directory', ?2, NULL, NULL, 0, 0, 0, 0, 0, 0)",
            params![
                i64::try_from(ROOT_INODE).expect("root inode fits SQLite"),
                startup.root_digest.to_string()
            ],
        )
        .map_err(|source| db_error("insert root inode", path, source))?;
    transaction
        .execute(
            "UPDATE session_metadata SET lifecycle = 'active' WHERE singleton = 1",
            [],
        )
        .map_err(|source| db_error("activate session", path, source))?;
    transaction
        .commit()
        .map_err(|source| db_error("commit session initialization", path, source))?;
    Ok(())
}

/// Validated session row plus identity and clean-close fields used internally.
#[derive(Debug)]
struct StoredSession {
    /// UUID shared with lock diagnostics.
    session_id: String,
    /// Validated fields exposed to state consumers.
    metadata: SessionMetadata,
    /// Clean-close seconds, present only for a closed session.
    closed_at_seconds: Option<i64>,
    /// Clean-close nanoseconds, present only for a closed session.
    closed_at_nanos: Option<i64>,
}

/// Unvalidated SQLite representation of the singleton session row.
struct RawSession {
    /// Singleton primary key, required to equal one.
    singleton: i64,
    /// Session UUID text.
    session_id: String,
    /// Daemon process identifier stored in SQLite's integer domain.
    daemon_pid: i64,
    /// Persisted lifecycle text.
    lifecycle: String,
    /// SHA-256 root digest hash.
    root_digest_hash: String,
    /// Root digest size.
    root_digest_size: i64,
    /// Canonical mountpoint encoded as UTF-8.
    mountpoint: String,
    /// Creation timestamp seconds.
    created_at_seconds: i64,
    /// Creation timestamp nanosecond fraction.
    created_at_nanos: i64,
    /// Optional clean-close timestamp seconds.
    closed_at_seconds: Option<i64>,
    /// Optional clean-close timestamp nanosecond fraction.
    closed_at_nanos: Option<i64>,
    /// Non-empty configured logging level.
    log_level: String,
    /// Supported logging format.
    log_format: String,
}

/// Validates every domain invariant in an untrusted session row.
fn validate_session_row(path: &Path, row: &RawSession) -> Result<StoredSession, StateError> {
    if row.singleton != 1 {
        return Err(stale_path(path, "session singleton is not 1".into()));
    }
    if Uuid::parse_str(&row.session_id).is_err() {
        return Err(stale_path(path, "session id is not a UUID".into()));
    }
    let daemon_pid =
        u32::try_from(row.daemon_pid).map_err(|_| stale_path(path, "invalid daemon pid".into()))?;
    if daemon_pid == 0 {
        return Err(stale_path(path, "daemon pid is zero".into()));
    }
    let state = parse_lifecycle(path, &row.lifecycle)?;
    let _created_at_seconds = row.created_at_seconds;
    validate_timestamp(path, "creation time", row.created_at_nanos)?;
    match (state, row.closed_at_seconds, row.closed_at_nanos) {
        (SessionLifecycle::Closed, Some(_), Some(nanos)) => {
            validate_timestamp(path, "closed time", nanos)?;
        }
        (SessionLifecycle::Closed, _, _) => {
            return Err(stale_path(path, "closed session has no closed time".into()));
        }
        (_, None, None) => {}
        (_, _, _) => {
            return Err(stale_path(
                path,
                "non-closed session has a closed time".into(),
            ));
        }
    }
    let root_digest = Digest::new(row.root_digest_hash.clone(), row.root_digest_size)
        .map_err(|error| stale_path(path, format!("invalid root digest: {error}")))?;
    let mountpoint = PathBuf::from(&row.mountpoint);
    if !mountpoint.is_absolute() {
        return Err(stale_path(path, "mountpoint is not absolute".into()));
    }
    if row.log_level.is_empty() {
        return Err(stale_path(path, "log level is empty".into()));
    }
    if !matches!(row.log_format.as_str(), "text" | "json") {
        return Err(stale_path(
            path,
            format!("unsupported log format `{}`", row.log_format),
        ));
    }
    Ok(StoredSession {
        session_id: row.session_id.clone(),
        metadata: SessionMetadata {
            daemon_pid,
            state,
            root_digest,
            mountpoint,
        },
        closed_at_seconds: row.closed_at_seconds,
        closed_at_nanos: row.closed_at_nanos,
    })
}

/// Reads and validates the required singleton session row.
fn read_stored_session(connection: &Connection, path: &Path) -> Result<StoredSession, StateError> {
    let row: Option<RawSession> = connection
        .query_row(
            "SELECT singleton, session_id, daemon_pid, lifecycle,
                    root_digest_hash, root_digest_size, mountpoint,
                    created_at_seconds, created_at_nanos,
                    closed_at_seconds, closed_at_nanos, log_level, log_format
             FROM session_metadata WHERE singleton = 1",
            [],
            |row| {
                Ok(RawSession {
                    singleton: row.get(0)?,
                    session_id: row.get(1)?,
                    daemon_pid: row.get(2)?,
                    lifecycle: row.get(3)?,
                    root_digest_hash: row.get(4)?,
                    root_digest_size: row.get(5)?,
                    mountpoint: row.get(6)?,
                    created_at_seconds: row.get(7)?,
                    created_at_nanos: row.get(8)?,
                    closed_at_seconds: row.get(9)?,
                    closed_at_nanos: row.get(10)?,
                    log_level: row.get(11)?,
                    log_format: row.get(12)?,
                })
            },
        )
        .optional()
        .map_err(|source| db_error("read session metadata", path, source))?;
    let Some(row) = row else {
        return Err(stale_path(path, "missing session metadata".into()));
    };
    validate_session_row(path, &row)
}

/// Reads only the validated session fields exposed to consumers.
fn read_session_metadata(
    connection: &Connection,
    path: &Path,
) -> Result<SessionMetadata, StateError> {
    Ok(read_stored_session(connection, path)?.metadata)
}

/// Atomically transitions an active session to cleanly closed.
fn close_session(
    connection: &mut Connection,
    path: &Path,
    seconds: i64,
    nanos: i64,
) -> Result<(), StateError> {
    validate_timestamp(path, "closed time", nanos)?;
    let transaction = connection
        .transaction()
        .map_err(|source| db_error("begin clean close", path, source))?;
    let stored = read_stored_session(&transaction, path)?;
    if stored.metadata.state != SessionLifecycle::Active {
        return Err(stale_path(
            path,
            format!(
                "cannot close session while lifecycle is {}",
                stored.metadata.state
            ),
        ));
    }
    transaction
        .execute(
            "UPDATE session_metadata
             SET lifecycle = 'closed', closed_at_seconds = ?1, closed_at_nanos = ?2
             WHERE singleton = 1",
            params![seconds, nanos],
        )
        .map_err(|source| db_error("mark session closed", path, source))?;
    transaction
        .commit()
        .map_err(|source| db_error("commit clean close", path, source))
}

/// Validated inode fields required by read-only remote materialization.
#[derive(Debug)]
struct InodeRow {
    /// Positive SQLite inode identity.
    inode: i64,
    /// Validated remote node kind.
    kind: RemoteNodeKind,
    /// File or directory digest when applicable.
    remote_digest: Option<String>,
    /// Exact symlink target when applicable.
    symlink_target: Option<String>,
}

/// Raw column tuple decoded from the inode table before domain validation.
type InodeTuple = (
    i64,
    Option<i64>,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
);

/// Validates and transactionally records a complete immutable directory.
fn materialize_remote_directory(
    connection: &mut Connection,
    path: &Path,
    parent: u64,
    digest: &Digest,
    children: &[RemoteNodeIdentity],
) -> Result<Vec<MaterializedInode>, StateError> {
    validate_remote_children(path, children)?;
    let parent =
        i64::try_from(parent).map_err(|_| stale_path(path, "inode exceeds SQLite range".into()))?;
    let transaction = connection
        .transaction()
        .map_err(|source| db_error("begin directory materialization", path, source))?;
    let prior = validate_materialization_parent(&transaction, path, parent, digest)?;
    let materialized = materialize_children(&transaction, path, parent, children)?;
    if prior.is_none() {
        transaction
            .execute(
                "INSERT INTO directory_materializations (inode, directory_digest)
                 VALUES (?1, ?2)",
                params![parent, digest.to_string()],
            )
            .map_err(|source| db_error("record directory materialization", path, source))?;
    }
    transaction
        .commit()
        .map_err(|source| db_error("commit directory materialization", path, source))?;
    Ok(materialized)
}

/// Validates the active parent identity and any prior materialization record.
fn validate_materialization_parent(
    transaction: &Transaction<'_>,
    path: &Path,
    parent: i64,
    digest: &Digest,
) -> Result<Option<String>, StateError> {
    ensure_active(transaction, path)?;
    let stored_parent = read_inode_by_id(transaction, path, parent, "read parent inode")?
        .ok_or_else(|| stale_path(path, format!("directory inode {parent} is missing")))?;
    if stored_parent.kind != RemoteNodeKind::Directory
        || stored_parent.remote_digest.as_deref() != Some(&digest.to_string())
    {
        return Err(stale_path(
            path,
            format!("inode {parent} is not directory {digest}"),
        ));
    }

    let prior: Option<String> = transaction
        .query_row(
            "SELECT directory_digest FROM directory_materializations WHERE inode = ?1",
            [parent],
            |row| row.get(0),
        )
        .optional()
        .map_err(|source| db_error("read directory materialization", path, source))?;
    if let Some(value) = &prior {
        validate_digest(path, "directory materialization digest", value)?;
        if value != &digest.to_string() {
            return Err(stale_path(
                path,
                format!("directory inode {parent} was materialized with another digest"),
            ));
        }
    }
    Ok(prior)
}

/// Reconciles the complete child set and returns stable inode allocations.
fn materialize_children(
    transaction: &Transaction<'_>,
    path: &Path,
    parent: i64,
    children: &[RemoteNodeIdentity],
) -> Result<Vec<MaterializedInode>, StateError> {
    let mut materialized = Vec::with_capacity(children.len());
    for child in children {
        let model = match read_child(transaction, path, parent, &child.name)? {
            Some(model) => model,
            None => insert_remote_child(transaction, path, parent, child)?,
        };
        if !remote_identity_matches(&model, child) {
            return Err(stale_path(
                path,
                format!("remote identity changed for inode {parent}/{}", child.name),
            ));
        }
        materialized.push(MaterializedInode {
            inode: u64::try_from(model.inode)
                .map_err(|_| stale_path(path, "stored inode is negative".into()))?,
            name: child.name.clone(),
        });
    }
    let stored_children: i64 = transaction
        .query_row(
            "SELECT count(*) FROM inodes WHERE parent_inode = ?1",
            [parent],
            |row| row.get(0),
        )
        .map_err(|source| db_error("count materialized children", path, source))?;
    if usize::try_from(stored_children).ok() != Some(children.len()) {
        return Err(stale_path(
            path,
            format!("remote child set changed for directory inode {parent}"),
        ));
    }
    Ok(materialized)
}

/// Requires an active lifecycle inside a materialization transaction.
fn ensure_active(transaction: &Transaction<'_>, path: &Path) -> Result<(), StateError> {
    let metadata = read_stored_session(transaction, path)?;
    if metadata.metadata.state != SessionLifecycle::Active {
        return Err(stale_path(
            path,
            format!(
                "cannot materialize inodes while session is {}",
                metadata.metadata.state
            ),
        ));
    }
    Ok(())
}

/// Inserts one previously unseen remote child and returns its allocated inode.
fn insert_remote_child(
    transaction: &Transaction<'_>,
    path: &Path,
    parent: i64,
    child: &RemoteNodeIdentity,
) -> Result<InodeRow, StateError> {
    let (remote_digest, symlink_target) = match child.kind {
        RemoteNodeKind::File | RemoteNodeKind::Directory => {
            (Some(child.content_identity.clone()), None)
        }
        RemoteNodeKind::Symlink => (None, Some(child.content_identity.clone())),
    };
    transaction
        .execute(
            "INSERT INTO inodes (
                parent_inode, name, kind, remote_digest, symlink_target,
                overlay_file, mode, mtime_seconds, mtime_nanos, tombstone,
                content_dirty, tree_dirty
             ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, 0, 0, 0, 0, 0, 0)",
            params![
                parent,
                child.name,
                node_kind_text(child.kind),
                remote_digest,
                symlink_target,
            ],
        )
        .map_err(|source| db_error("insert materialized child", path, source))?;
    Ok(InodeRow {
        inode: transaction.last_insert_rowid(),
        kind: child.kind,
        remote_digest,
        symlink_target,
    })
}

/// Compares the immutable identity fields relevant to a remote child kind.
fn remote_identity_matches(model: &InodeRow, child: &RemoteNodeIdentity) -> bool {
    if model.kind != child.kind {
        return false;
    }
    match child.kind {
        RemoteNodeKind::File | RemoteNodeKind::Directory => {
            model.remote_digest.as_deref() == Some(&child.content_identity)
                && model.symlink_target.is_none()
        }
        RemoteNodeKind::Symlink => {
            model.remote_digest.is_none()
                && model.symlink_target.as_deref() == Some(&child.content_identity)
        }
    }
}

/// Reads and validates an inode by its stable identity.
fn read_inode_by_id(
    connection: &Connection,
    path: &Path,
    inode: i64,
    operation: &'static str,
) -> Result<Option<InodeRow>, StateError> {
    query_inode(
        connection,
        path,
        operation,
        "SELECT inode, parent_inode, name, kind, remote_digest, symlink_target,
                overlay_file, mode, mtime_seconds, mtime_nanos, tombstone,
                content_dirty, tree_dirty
         FROM inodes WHERE inode = ?1",
        params![inode],
    )
}

/// Reads and validates a child by its parent/name identity.
fn read_child(
    connection: &Connection,
    path: &Path,
    parent: i64,
    name: &str,
) -> Result<Option<InodeRow>, StateError> {
    query_inode(
        connection,
        path,
        "find materialized child",
        "SELECT inode, parent_inode, name, kind, remote_digest, symlink_target,
                overlay_file, mode, mtime_seconds, mtime_nanos, tombstone,
                content_dirty, tree_dirty
         FROM inodes WHERE parent_inode = ?1 AND name = ?2",
        params![parent, name],
    )
}

/// Executes one optional inode query and validates any returned row.
fn query_inode(
    connection: &Connection,
    path: &Path,
    operation: &'static str,
    sql: &str,
    parameters: impl rusqlite::Params,
) -> Result<Option<InodeRow>, StateError> {
    let row: Option<InodeTuple> = connection
        .query_row(sql, parameters, |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
                row.get(9)?,
                row.get(10)?,
                row.get(11)?,
                row.get(12)?,
            ))
        })
        .optional()
        .map_err(|source| db_error(operation, path, source))?;
    row.map(|row| validate_inode_row(path, row)).transpose()
}

/// Validates identity, booleans, timestamps, paths, and kind-specific shape.
fn validate_inode_row(path: &Path, row: InodeTuple) -> Result<InodeRow, StateError> {
    let (
        inode,
        parent_inode,
        name,
        kind,
        remote_digest,
        symlink_target,
        overlay_file,
        mode,
        _mtime_seconds,
        mtime_nanos,
        tombstone,
        content_dirty,
        tree_dirty,
    ) = row;
    if inode == i64::try_from(ROOT_INODE).expect("root inode fits SQLite") {
        if parent_inode.is_some() || !name.is_empty() {
            return Err(stale_path(
                path,
                "root inode has an invalid identity".into(),
            ));
        }
    } else if inode <= 1 || parent_inode.is_none() || name.is_empty() || name.contains('/') {
        return Err(stale_path(
            path,
            format!("inode {inode} has an invalid identity"),
        ));
    }
    let kind = parse_node_kind(path, &kind)?;
    validate_timestamp(path, "inode modification time", mtime_nanos)?;
    for (field, value) in [
        ("tombstone", tombstone),
        ("content_dirty", content_dirty),
        ("tree_dirty", tree_dirty),
    ] {
        if !matches!(value, 0 | 1) {
            return Err(stale_path(
                path,
                format!("inode {inode} has invalid {field}"),
            ));
        }
    }
    if mode < 0 {
        return Err(stale_path(
            path,
            format!("inode {inode} has a negative mode"),
        ));
    }
    if let Some(value) = &remote_digest {
        validate_digest(path, "inode remote digest", value)?;
    }
    if let Some(value) = &overlay_file
        && (value.is_empty() || Path::new(value).is_absolute())
    {
        return Err(stale_path(
            path,
            format!("inode {inode} has an invalid overlay file"),
        ));
    }
    let valid_shape = match kind {
        RemoteNodeKind::Symlink => {
            symlink_target.is_some() && remote_digest.is_none() && overlay_file.is_none()
        }
        RemoteNodeKind::Directory => symlink_target.is_none() && overlay_file.is_none(),
        RemoteNodeKind::File => symlink_target.is_none(),
    };
    if !valid_shape {
        return Err(stale_path(
            path,
            format!("inode {inode} fields do not match its kind"),
        ));
    }
    Ok(InodeRow {
        inode,
        kind,
        remote_digest,
        symlink_target,
    })
}

/// Validates unique child names and digest-bearing child identities.
fn validate_remote_children(
    path: &Path,
    children: &[RemoteNodeIdentity],
) -> Result<(), StateError> {
    let mut names = std::collections::HashSet::with_capacity(children.len());
    for child in children {
        if child.name.is_empty() || child.name.contains('/') || !names.insert(&child.name) {
            return Err(stale_path(
                path,
                format!("invalid or duplicate remote child name `{}`", child.name),
            ));
        }
        if matches!(child.kind, RemoteNodeKind::File | RemoteNodeKind::Directory) {
            validate_digest(path, "remote child digest", &child.content_identity)?;
        }
    }
    Ok(())
}

/// Parses a persisted digest and attaches its field and database path.
fn validate_digest(path: &Path, field: &str, value: &str) -> Result<Digest, StateError> {
    value
        .parse()
        .map_err(|error| stale_path(path, format!("invalid {field}: {error}")))
}

/// Validates a persisted timestamp nanosecond fraction.
fn validate_timestamp(path: &Path, field: &str, nanos: i64) -> Result<(), StateError> {
    if !(0..=999_999_999).contains(&nanos) {
        return Err(stale_path(path, format!("{field} nanoseconds are invalid")));
    }
    Ok(())
}

/// Parses the closed set of persisted session lifecycle values.
fn parse_lifecycle(path: &Path, value: &str) -> Result<SessionLifecycle, StateError> {
    match value {
        "initializing" => Ok(SessionLifecycle::Initializing),
        "active" => Ok(SessionLifecycle::Active),
        "closed" => Ok(SessionLifecycle::Closed),
        _ => Err(stale_path(
            path,
            format!("unsupported session lifecycle `{value}`"),
        )),
    }
}

/// Returns the stable SQLite representation of a remote node kind.
fn node_kind_text(kind: RemoteNodeKind) -> &'static str {
    match kind {
        RemoteNodeKind::File => "file",
        RemoteNodeKind::Directory => "directory",
        RemoteNodeKind::Symlink => "symlink",
    }
}

/// Parses the closed set of persisted remote node kinds.
fn parse_node_kind(path: &Path, value: &str) -> Result<RemoteNodeKind, StateError> {
    match value {
        "file" => Ok(RemoteNodeKind::File),
        "directory" => Ok(RemoteNodeKind::Directory),
        "symlink" => Ok(RemoteNodeKind::Symlink),
        _ => Err(stale_path(
            path,
            format!("unsupported inode kind `{value}`"),
        )),
    }
}

/// Combines validated metadata with fixed state paths for presentation.
fn session_view(paths: &StatePaths, metadata: SessionMetadata) -> SessionView {
    SessionView {
        daemon_pid: metadata.daemon_pid,
        state: metadata.state,
        root_digest: metadata.root_digest,
        mountpoint: metadata.mountpoint,
        control_endpoint: paths.control_socket().to_path_buf(),
        cache_path: paths.cache().to_path_buf(),
        session_path: paths.active().to_path_buf(),
    }
}

/// Opens and validates retained state without mutating it.
fn retained_session_result(paths: &StatePaths) -> Result<Option<SessionView>, StateError> {
    if !paths.active().exists() {
        return Ok(None);
    }
    validate_closed_layout(paths)?;
    let connection = open_database(paths.database(), true)?;
    validate_schema_version(&connection, paths.database())?;
    let metadata = read_session_metadata(&connection, paths.database())?;
    validate_closed_identity(paths, &metadata, &connection)?;
    Ok(Some(session_view(paths, metadata)))
}

/// Proves that an existing active tree is safe to replace.
fn validate_closed_session(paths: &StatePaths) -> Result<(), StateError> {
    validate_closed_layout(paths)?;
    let connection = open_database(paths.database(), true)?;
    validate_schema_version(&connection, paths.database())?;
    let metadata = read_session_metadata(&connection, paths.database())?;
    validate_closed_identity(paths, &metadata, &connection)
}

/// Validates the exact retained-session directory and file inventory.
fn validate_closed_layout(paths: &StatePaths) -> Result<(), StateError> {
    validate_directory_entries(
        paths.active(),
        &[
            ("session.db", false),
            ("rfsd.log", false),
            ("overlay", true),
        ],
    )
    .map_err(|error| stale(paths, error.to_string()))?;
    validate_directory_entries(paths.overlay(), &[("data", true), ("tmp", true)])
        .map_err(|error| stale(paths, error.to_string()))
}

/// Cross-checks clean-close lifecycle and lock/database session identity.
fn validate_closed_identity(
    paths: &StatePaths,
    metadata: &SessionMetadata,
    connection: &Connection,
) -> Result<(), StateError> {
    let previous: LockRecord = fs::read_to_string(paths.lock())
        .ok()
        .and_then(|value| serde_json::from_str(&value).ok())
        .ok_or_else(|| stale(paths, "invalid lock record".into()))?;
    if previous.record_version != LOCK_RECORD_VERSION {
        return Err(stale(paths, "unsupported lock record version".into()));
    }
    let stored = read_stored_session(connection, paths.database())?;
    if stored.session_id != previous.session_id || metadata.daemon_pid != previous.pid {
        return Err(stale(
            paths,
            "lock and database session identity do not match".into(),
        ));
    }
    if metadata.state != SessionLifecycle::Closed
        || stored.closed_at_seconds.is_none()
        || stored.closed_at_nanos.is_none()
    {
        return Err(stale(paths, "session was not closed cleanly".into()));
    }
    Ok(())
}

/// Opens one SQLite connection with reader- or worker-appropriate settings.
fn open_database(path: &Path, read_only: bool) -> Result<Connection, StateError> {
    let flags = if read_only {
        OpenFlags::SQLITE_OPEN_READ_ONLY
    } else {
        OpenFlags::SQLITE_OPEN_READ_WRITE
    };
    let connection = Connection::open_with_flags(path, flags)
        .map_err(|source| db_error("open connection", path, source))?;
    connection
        .busy_timeout(Duration::from_secs(2))
        .map_err(|source| db_error("set busy timeout", path, source))?;
    if !read_only {
        connection
            .pragma_update(None, "journal_mode", "DELETE")
            .map_err(|source| db_error("set journal mode", path, source))?;
        connection
            .pragma_update(None, "foreign_keys", true)
            .map_err(|source| db_error("enable foreign keys", path, source))?;
    }
    Ok(connection)
}

/// Creates the idempotent baseline schema or validates an existing version.
fn prepare_schema(connection: &Connection, path: &Path) -> Result<(), StateError> {
    let version = schema_version(connection, path)?;
    if version != 0 {
        validate_schema_version(connection, path)?;
    }
    connection
        .execute_batch(SCHEMA_SQL)
        .map_err(|source| db_error("create schema", path, source))?;
    if version == 0 {
        connection
            .pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(|source| db_error("record schema version", path, source))?;
    }
    Ok(())
}

/// Reads SQLite's durable schema version.
fn schema_version(connection: &Connection, path: &Path) -> Result<i64, StateError> {
    connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|source| db_error("read schema version", path, source))
}

/// Requires the exact schema version supported by this binary.
fn validate_schema_version(connection: &Connection, path: &Path) -> Result<(), StateError> {
    let version = schema_version(connection, path)?;
    if version != SCHEMA_VERSION {
        return Err(stale_path(
            path,
            format!("unsupported state schema version {version}; expected {SCHEMA_VERSION}"),
        ));
    }
    Ok(())
}

/// Canonicalizes and validates the private state root.
fn canonical_private_home(path: &Path) -> Result<PathBuf, StateError> {
    let home = fs::canonicalize(path)
        .map_err(|source| fs_error("canonicalize state root", path, source))?;
    let metadata = fs::symlink_metadata(&home)
        .map_err(|source| fs_error("inspect state root", &home, source))?;
    if !metadata.file_type().is_dir() {
        return Err(StateError::UnsafePath {
            path: home,
            reason: "RFS_HOME is not a directory".into(),
        });
    }
    validate_existing_permissions(&home, &metadata)?;
    Ok(home)
}

/// Derives a sharded cache path for tests.
#[cfg(test)]
fn cache_path(base: &Path, digest: &Digest) -> PathBuf {
    base.join(&digest.hash()[..2])
        .join(format!("{}-{}", digest.hash(), digest.size_bytes()))
}

/// Creates every fixed cache and active-session directory with private modes.
fn create_layout(paths: &StatePaths) -> Result<(), StateError> {
    for path in [
        paths.cache(),
        paths.blob_cache(),
        paths.directory_cache(),
        paths.active(),
        paths.overlay(),
        paths.overlay_data(),
        paths.overlay_tmp(),
    ] {
        create_dir_private(path)?;
    }
    Ok(())
}

/// Returns the current Unix timestamp split into SQLite integer fields.
fn now_parts() -> Result<(i64, i64), StateError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StateError::InvalidSystemTime)?;
    Ok((
        <i64 as TryFrom<u64>>::try_from(duration.as_secs())
            .map_err(|_| StateError::InvalidSystemTime)?,
        i64::from(duration.subsec_nanos()),
    ))
}

/// Creates one directory without following an existing symlink.
fn create_dir_private(path: &Path) -> Result<(), StateError> {
    match fs::create_dir(path) {
        Ok(()) => fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|source| fs_error("secure state directory", path, source)),
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path)
                .map_err(|source| fs_error("inspect state directory", path, source))?;
            if !metadata.file_type().is_dir() {
                return Err(StateError::UnsafePath {
                    path: path.to_path_buf(),
                    reason: "expected a directory and will not follow a symlink".into(),
                });
            }
            validate_existing_permissions(path, &metadata)
        }
        Err(source) => Err(fs_error("create state directory", path, source)),
    }
}

/// Creates a missing state-root ancestry and applies the private root mode.
fn create_home_private(path: &Path) -> Result<(), StateError> {
    fs::create_dir_all(path).map_err(|source| fs_error("create state root", path, source))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|source| fs_error("secure state root", path, source))
}

/// Exclusively creates an empty private state file.
fn create_file_private(path: &Path) -> Result<(), StateError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|source| fs_error("create state file", path, source))?;
    Ok(())
}

/// Validates effective-user ownership and rejects group/other access.
fn validate_existing_permissions(path: &Path, metadata: &fs::Metadata) -> Result<(), StateError> {
    if metadata.uid() != unsafe { libc::geteuid() } {
        return Err(StateError::UnsafePath {
            path: path.to_path_buf(),
            reason: "entry is not owned by the effective user".into(),
        });
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(StateError::UnsafePath {
            path: path.to_path_buf(),
            reason: "entry is accessible by group or other users".into(),
        });
    }
    Ok(())
}

/// Validates an exact directory inventory without following symlinks.
fn validate_directory_entries(
    directory: &Path,
    expected: &[(&str, bool)],
) -> Result<(), StateError> {
    let mut found = Vec::new();
    for entry in fs::read_dir(directory)
        .map_err(|source| fs_error("read state directory", directory, source))?
    {
        let entry =
            entry.map_err(|source| fs_error("read state-directory entry", directory, source))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(StateError::UnsafePath {
                path: entry.path(),
                reason: "entry name is not UTF-8".into(),
            });
        };
        let Some((_, should_be_directory)) =
            expected.iter().find(|(candidate, _)| *candidate == name)
        else {
            return Err(StateError::UnsafePath {
                path: entry.path(),
                reason: "unknown entry; inspect it manually".into(),
            });
        };
        let metadata = fs::symlink_metadata(entry.path())
            .map_err(|source| fs_error("inspect state-directory entry", &entry.path(), source))?;
        let correct_type = if *should_be_directory {
            metadata.file_type().is_dir()
        } else {
            metadata.file_type().is_file()
        };
        if !correct_type {
            return Err(StateError::UnsafePath {
                path: entry.path(),
                reason: "entry has the wrong type".into(),
            });
        }
        validate_existing_permissions(&entry.path(), &metadata)?;
        found.push(name.to_owned());
    }
    if let Some((missing, _)) = expected
        .iter()
        .find(|(name, _)| !found.iter().any(|item| item == name))
    {
        return Err(StateError::UnsafePath {
            path: directory.join(missing),
            reason: "required entry is missing".into(),
        });
    }
    Ok(())
}

/// Removes an existing active-session tree while tolerating absence.
fn remove_if_present(path: &Path) -> Result<(), StateError> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(fs_error("remove closed session", path, source)),
    }
}

/// Builds a stale-session error rooted at the active directory.
fn stale(paths: &StatePaths, reason: String) -> StateError {
    StateError::StaleSession {
        path: paths.active().to_path_buf(),
        reason,
    }
}

/// Builds a stale-session error for a specific durable state path.
fn stale_path(path: &Path, reason: String) -> StateError {
    StateError::StaleSession {
        path: path.to_path_buf(),
        reason,
    }
}

/// Attaches a stable operation and path to an I/O failure.
fn fs_error(operation: &'static str, path: &Path, source: std::io::Error) -> StateError {
    let kind = source.kind();
    StateError::Filesystem {
        path: path.to_path_buf(),
        source: std::io::Error::new(kind, FilesystemOperationError { operation, source }),
    }
}

/// Attaches a stable operation and database path to an SQLite failure.
fn db_error(operation: &'static str, path: &Path, source: rusqlite::Error) -> StateError {
    StateError::Database {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

/// Attaches a stable command and database path to a worker failure.
fn worker_error(path: &Path, operation: &'static str, reason: impl Into<String>) -> StateError {
    StateError::Worker {
        operation,
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(temp: &tempfile::TempDir) -> StatePaths {
        StatePaths::from_config(&Config {
            rfs_home: temp.path().join("home"),
        })
        .unwrap()
    }

    #[test]
    fn fixed_paths_are_derived_once_with_stable_lock_outside_active() {
        let home = PathBuf::from("/state/home");
        let paths = StatePaths::init(home.clone());

        assert_eq!(paths.home(), home);
        assert_eq!(paths.cache(), home.join("cache"));
        assert_eq!(paths.blob_cache(), home.join("cache/blobs"));
        assert_eq!(paths.directory_cache(), home.join("cache/dirs"));
        assert_eq!(paths.active(), home.join("active"));
        assert_eq!(paths.lock(), home.join("active.lock"));
        assert_eq!(paths.database(), home.join("active/session.db"));
        assert_eq!(paths.log(), home.join("active/rfsd.log"));
        assert_eq!(paths.control_socket(), home.join("active/control.sock"));
        assert_eq!(paths.overlay(), home.join("active/overlay"));
        assert_eq!(paths.overlay_data(), home.join("active/overlay/data"));
        assert_eq!(paths.overlay_tmp(), home.join("active/overlay/tmp"));
        assert!(!paths.lock().starts_with(paths.active()));
    }

    #[test]
    fn cache_paths_are_sharded_and_include_size() {
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp);
        let digest = Digest::for_bytes(b"hello");
        assert!(paths.blob_cache_path(&digest).ends_with(format!(
            "{}/{}-5",
            &digest.hash()[..2],
            digest.hash()
        )));
        assert!(
            paths
                .directory_cache_path(&digest)
                .starts_with(paths.directory_cache())
        );
    }

    #[test]
    fn session_can_close_and_be_replaced_without_removing_cache() {
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp);
        let mountpoint = temp.path().join("mount");
        fs::create_dir(&mountpoint).unwrap();
        let startup = SessionStartup::new(Digest::for_bytes(b"root"), mountpoint.clone());
        Box::new(SessionStore::create(paths.clone(), startup.clone()).unwrap())
            .close()
            .unwrap();
        fs::write(paths.cache().join("keep"), b"cache").unwrap();
        Box::new(SessionStore::create(paths.clone(), startup).unwrap())
            .close()
            .unwrap();
        assert!(paths.cache().join("keep").exists());
    }

    #[test]
    fn reader_does_not_create_a_missing_home() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("missing");
        let reader = open_reader(Config {
            rfs_home: home.clone(),
        })
        .unwrap();
        assert!(reader.session().unwrap().is_none());
        assert!(!home.exists());
    }

    #[test]
    fn existing_home_rejects_permissions_for_other_users() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        fs::create_dir(&home).unwrap();
        fs::set_permissions(&home, fs::Permissions::from_mode(0o755)).unwrap();
        let error = StatePaths::from_config(&Config { rfs_home: home })
            .expect_err("group-readable state home must be rejected");
        assert!(matches!(
            error,
            StateError::Context { source, .. }
                if matches!(*source, StateError::UnsafePath { .. })
        ));
    }

    #[test]
    fn filesystem_errors_identify_operation_and_path() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        create_file_private(&path).unwrap();

        let error = create_file_private(&path).expect_err("exclusive creation must fail");
        assert!(matches!(
            &error,
            StateError::Filesystem {
                path: error_path,
                ..
            } if error_path == &path
        ));
        assert!(error.to_string().contains("create state file failed"));
    }

    #[test]
    fn closed_session_with_unknown_active_entry_is_not_replaced() {
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp);
        let mountpoint = temp.path().join("mount");
        fs::create_dir(&mountpoint).unwrap();
        let startup = SessionStartup::new(Digest::for_bytes(b"root"), mountpoint);
        Box::new(SessionStore::create(paths.clone(), startup.clone()).unwrap())
            .close()
            .unwrap();
        fs::write(paths.active().join("unexpected"), b"preserve me").unwrap();
        assert!(matches!(
            SessionStore::create(paths, startup),
            Err(StateError::StaleSession { .. })
        ));
    }

    #[test]
    fn remote_materialization_is_stable_and_rejects_identity_changes() {
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp);
        let mountpoint = temp.path().join("mount");
        fs::create_dir(&mountpoint).unwrap();
        let root = Digest::for_bytes(b"root directory");
        let store =
            SessionStore::create(paths, SessionStartup::new(root.clone(), mountpoint)).unwrap();
        let filesystem = store.filesystem_state();
        let children = vec![
            RemoteNodeIdentity {
                name: "child".into(),
                kind: RemoteNodeKind::Directory,
                content_identity: Digest::for_bytes(b"child directory").to_string(),
            },
            RemoteNodeIdentity {
                name: "file".into(),
                kind: RemoteNodeKind::File,
                content_identity: Digest::for_bytes(b"file").to_string(),
            },
        ];
        let first = filesystem
            .materialize_remote_directory(ROOT_INODE, &root, &children)
            .unwrap();
        let second = filesystem
            .materialize_remote_directory(ROOT_INODE, &root, &children)
            .unwrap();
        assert_eq!(first, second);
        assert!(first.iter().all(|entry| entry.inode > ROOT_INODE));
        assert!(matches!(
            filesystem.materialize_remote_directory(ROOT_INODE, &root, &children[..1]),
            Err(StateError::StaleSession { .. })
        ));
        drop(filesystem);
        Box::new(store).close().unwrap();
    }

    #[test]
    fn embedded_schema_is_idempotent_and_rejects_newer_versions() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        create_file_private(&path).unwrap();
        let connection = open_database(&path, false).unwrap();
        prepare_schema(&connection, &path).unwrap();
        prepare_schema(&connection, &path).unwrap();
        connection
            .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();
        let error = validate_schema_version(&connection, &path)
            .expect_err("future schema must be rejected");
        assert!(
            error
                .to_string()
                .contains(&(SCHEMA_VERSION + 1).to_string())
        );
    }

    #[test]
    fn invalid_lifecycle_is_rejected_by_rust_with_database_context() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        create_file_private(&path).unwrap();
        let connection = open_database(&path, false).unwrap();
        prepare_schema(&connection, &path).unwrap();
        connection
            .execute(
                "INSERT INTO session_metadata (
                    singleton, session_id, daemon_pid, lifecycle,
                    root_digest_hash, root_digest_size, mountpoint,
                    created_at_seconds, created_at_nanos,
                    closed_at_seconds, closed_at_nanos, log_level, log_format
                 ) VALUES (1, ?1, 1, 'closed', ?2, 0, '/mount', 0, 0,
                           NULL, NULL, 'info', 'text')",
                params![Uuid::new_v4().to_string(), "0".repeat(64)],
            )
            .expect("SQL schema intentionally permits domain-invalid rows");
        let error = read_stored_session(&connection, &path)
            .expect_err("Rust decoding must reject a closed lifecycle without a timestamp");
        let message = error.to_string();
        assert!(message.contains("closed session has no closed time"));
        assert!(message.contains(path.to_string_lossy().as_ref()));
    }

    #[test]
    fn embedded_schema_owns_tables_indexes_and_foreign_keys_without_checks() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(SCHEMA_SQL).unwrap();
        let columns = |table: &str| {
            let mut statement = connection
                .prepare("SELECT name FROM pragma_table_info(?1) ORDER BY cid")
                .unwrap();
            statement
                .query_map([table], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        assert_eq!(
            columns("session_metadata"),
            [
                "singleton",
                "session_id",
                "daemon_pid",
                "lifecycle",
                "root_digest_hash",
                "root_digest_size",
                "mountpoint",
                "created_at_seconds",
                "created_at_nanos",
                "closed_at_seconds",
                "closed_at_nanos",
                "log_level",
                "log_format",
            ]
        );
        assert_eq!(
            columns("inodes"),
            [
                "inode",
                "parent_inode",
                "name",
                "kind",
                "remote_digest",
                "symlink_target",
                "overlay_file",
                "mode",
                "mtime_seconds",
                "mtime_nanos",
                "tombstone",
                "content_dirty",
                "tree_dirty",
            ]
        );
        assert_eq!(
            columns("directory_materializations"),
            ["inode", "directory_digest"]
        );
        let index_count: i64 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_schema
                 WHERE type = 'index'
                   AND name IN ('uq_inodes_parent_name', 'ix_inodes_parent')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(index_count, 2);
        let foreign_key_count: i64 = connection
            .query_row(
                "SELECT
                    (SELECT count(*) FROM pragma_foreign_key_list('inodes')) +
                    (SELECT count(*) FROM pragma_foreign_key_list('directory_materializations'))",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(foreign_key_count, 2);
        let normalized_schema = SCHEMA_SQL.to_ascii_uppercase();
        assert!(!normalized_schema.contains("CHECK"));
        assert!(!normalized_schema.contains("NOT NULL"));
    }
}
