//! Durable local state and exclusive foreground-session ownership.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::shared::config::Config;
use crate::shared::digest::Digest;

const SCHEMA_VERSION: i64 = 1;
const LOCK_RECORD_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum StateError {
    #[error("local state path `{path}` is unsafe: {reason}")]
    UnsafePath { path: PathBuf, reason: String },
    #[error("another RemoteFS session owns `{path}`{owner}")]
    ActiveSession { path: PathBuf, owner: String },
    #[error("stale or malformed session state at `{path}`; run `rfs cleanup`: {reason}")]
    StaleSession { path: PathBuf, reason: String },
    #[error("filesystem operation on `{path}` failed: {source}")]
    Filesystem {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("SQLite operation on `{path}` failed: {source}")]
    Database {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },
    #[error("mountpoint `{path}` must be an existing directory")]
    InvalidMountpoint { path: PathBuf },
    #[error("system time is outside the supported timestamp range")]
    InvalidSystemTime,
}

/// Canonical `RFS_HOME` and its fixed descendants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatePaths {
    home: PathBuf,
}

impl StatePaths {
    /// Creates a missing home privately, canonicalizes it, and validates its
    /// direct children without traversing cache or overlay contents.
    pub fn from_config(config: &Config) -> Result<Self, StateError> {
        if !config.rfs_home.exists() {
            create_home_private(&config.rfs_home)?;
        }
        let home = fs::canonicalize(&config.rfs_home)
            .map_err(|source| fs_error(&config.rfs_home, source))?;
        let metadata = fs::symlink_metadata(&home).map_err(|source| fs_error(&home, source))?;
        if !metadata.file_type().is_dir() {
            return Err(StateError::UnsafePath {
                path: home,
                reason: "RFS_HOME is not a directory".into(),
            });
        }
        validate_existing_permissions(&home, &metadata)?;
        let paths = Self { home };
        paths.validate_top_level()?;
        Ok(paths)
    }

    pub fn home(&self) -> &Path {
        &self.home
    }
    pub fn cache(&self) -> PathBuf {
        self.home.join("cache")
    }
    pub fn blob_cache(&self) -> PathBuf {
        self.cache().join("blobs")
    }
    pub fn directory_cache(&self) -> PathBuf {
        self.cache().join("dirs")
    }
    pub fn active(&self) -> PathBuf {
        self.home.join("active")
    }
    pub fn lock(&self) -> PathBuf {
        self.home.join("active.lock")
    }
    pub fn database(&self) -> PathBuf {
        self.active().join("session.db")
    }
    pub fn log(&self) -> PathBuf {
        self.active().join("rfsd.log")
    }
    /// Returns the fixed Unix control-socket path for the active session.
    pub fn control_socket(&self) -> PathBuf {
        self.active().join("control.sock")
    }
    pub fn overlay_data(&self) -> PathBuf {
        self.active().join("overlay/data")
    }
    pub fn overlay_tmp(&self) -> PathBuf {
        self.active().join("overlay/tmp")
    }

    pub fn blob_cache_path(&self, digest: &Digest) -> PathBuf {
        cache_path(self.blob_cache(), digest)
    }

    pub fn directory_cache_path(&self, digest: &Digest) -> PathBuf {
        cache_path(self.directory_cache(), digest)
    }

    /// Removes all recognized local state while holding the stable lock.
    pub fn cleanup(&self) -> Result<(), StateError> {
        self.validate_top_level()?;
        let lock = SessionLock::acquire(self)?;
        remove_if_present(&self.active())?;
        remove_if_present(&self.cache())?;
        fs::remove_file(self.lock()).map_err(|source| fs_error(&self.lock(), source))?;
        drop(lock);
        Ok(())
    }

    fn validate_top_level(&self) -> Result<(), StateError> {
        for entry in fs::read_dir(&self.home).map_err(|source| fs_error(&self.home, source))? {
            let entry = entry.map_err(|source| fs_error(&self.home, source))?;
            let name = entry.file_name();
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(|source| fs_error(&path, source))?;
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

#[derive(Debug, Clone)]
pub struct SessionStartup {
    pub root_digest: Digest,
    pub mountpoint: PathBuf,
    pub daemon_pid: u32,
}

impl SessionStartup {
    pub fn new(root_digest: Digest, mountpoint: PathBuf) -> Self {
        Self {
            root_digest,
            mountpoint,
            daemon_pid: std::process::id(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct LockRecord {
    record_version: u32,
    session_id: String,
    pid: u32,
}

struct ClosedSessionMetadata {
    session_id: String,
    daemon_pid: u32,
    state: String,
    closed_at_seconds: Option<i64>,
    closed_at_nanos: Option<i64>,
}

/// Session metadata exposed by the daemon control API and retained-state inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMetadata {
    /// Process identifier recorded when the session was created.
    pub daemon_pid: u32,
    /// Current durable session lifecycle state.
    pub state: String,
    /// Root digest mounted by the session.
    pub root_digest: Digest,
    /// Canonical mountpoint owned by the session.
    pub mountpoint: PathBuf,
}

/// Result of inspecting session state when no live daemon can be reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetainedSession {
    /// No active or retained session directory exists.
    None,
    /// A valid, cleanly closed session remains available for inspection.
    Closed(SessionMetadata),
    /// Session state exists but cannot be trusted or safely reused.
    Stale(String),
}

/// An exclusively held stable ownership lock.
pub struct SessionLock {
    file: File,
}

impl SessionLock {
    fn acquire(paths: &StatePaths) -> Result<Self, StateError> {
        let path = paths.lock();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&path)
            .map_err(|source| fs_error(&path, source))?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .map_err(|source| fs_error(&path, source))?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let owner = fs::read_to_string(&path)
                .ok()
                .and_then(|value| serde_json::from_str::<LockRecord>(&value).ok())
                .map(|value| format!(" (pid {}, session {})", value.pid, value.session_id))
                .unwrap_or_else(|| " (owner diagnostics unavailable)".into());
            return Err(StateError::ActiveSession { path, owner });
        }
        Ok(Self { file })
    }

    fn write_record(&mut self, path: &Path, record: &LockRecord) -> Result<(), StateError> {
        self.file
            .set_len(0)
            .map_err(|source| fs_error(path, source))?;
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|source| fs_error(path, source))?;
        serde_json::to_writer(&mut self.file, record).map_err(|source| StateError::UnsafePath {
            path: path.to_path_buf(),
            reason: format!("cannot encode lock record: {source}"),
        })?;
        self.file
            .write_all(b"\n")
            .map_err(|source| fs_error(path, source))
    }
}

impl Drop for SessionLock {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

/// Sole owner of a writable session database and its ownership lock.
pub struct SessionStore {
    paths: StatePaths,
    connection: Connection,
    _lock: SessionLock,
}

impl SessionStore {
    /// Creates a fresh active session, replacing only confidently closed state.
    pub fn create(paths: StatePaths, mut startup: SessionStartup) -> Result<Self, StateError> {
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
            remove_if_present(&paths.active())?;
        }
        lock.write_record(&paths.lock(), &record)?;
        create_layout(&paths)?;
        let connection = open_database(&paths.database())?;
        migrate(&connection, &paths.database())?;
        insert_metadata(&connection, &paths.database(), &session_id, &startup)?;
        create_file_private(&paths.log())?;
        connection
            .execute(
                "UPDATE session_metadata SET state = 'active' WHERE singleton = 1",
                [],
            )
            .map_err(|source| db_error(&paths.database(), source))?;
        append_log(&paths.log(), "session active\n")?;
        Ok(Self {
            paths,
            connection,
            _lock: lock,
        })
    }

    pub fn paths(&self) -> &StatePaths {
        &self.paths
    }

    /// Returns metadata for the active session owned by this store.
    pub fn metadata(&self) -> Result<SessionMetadata, StateError> {
        read_session_metadata(&self.connection, &self.paths.database())
    }

    /// Marks the session closed transactionally before releasing ownership.
    pub fn close_cleanly(self) -> Result<(), StateError> {
        append_log(&self.paths.log(), "session closing cleanly\n")?;
        let (seconds, nanos) = now_parts()?;
        self.connection.execute(
            "UPDATE session_metadata SET state = 'closed', closed_at_seconds = ?1, closed_at_nanos = ?2 WHERE singleton = 1",
            params![seconds, nanos],
        ).map_err(|source| db_error(&self.paths.database(), source))?;
        Ok(())
    }
}

/// Inspects retained state without modifying or replacing it.
pub fn inspect_retained_session(paths: &StatePaths) -> RetainedSession {
    if !paths.active().exists() {
        return RetainedSession::None;
    }
    if let Err(error) = validate_closed_session(paths) {
        return RetainedSession::Stale(error.to_string());
    }
    let connection =
        match Connection::open_with_flags(paths.database(), OpenFlags::SQLITE_OPEN_READ_ONLY) {
            Ok(connection) => connection,
            Err(error) => return RetainedSession::Stale(error.to_string()),
        };
    match read_session_metadata(&connection, &paths.database()) {
        Ok(metadata) => RetainedSession::Closed(metadata),
        Err(error) => RetainedSession::Stale(error.to_string()),
    }
}

fn read_session_metadata(
    connection: &Connection,
    path: &Path,
) -> Result<SessionMetadata, StateError> {
    let (pid, state, hash, size, mountpoint): (i64, String, String, i64, String) = connection
        .query_row(
            "SELECT daemon_pid, state, root_digest_hash, root_digest_size, mountpoint FROM session_metadata WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .map_err(|source| db_error(path, source))?;
    let daemon_pid =
        u32::try_from(pid).map_err(|_| stale_path(path, "invalid daemon pid".into()))?;
    let digest_text = format!("sha256:{hash}/{size}");
    let root_digest = digest_text
        .parse()
        .map_err(|error| stale_path(path, format!("invalid root digest: {error}")))?;
    Ok(SessionMetadata {
        daemon_pid,
        state,
        root_digest,
        mountpoint: PathBuf::from(mountpoint),
    })
}

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

fn cache_path(base: PathBuf, digest: &Digest) -> PathBuf {
    base.join(&digest.hash()[..2])
        .join(format!("{}-{}", digest.hash(), digest.size_bytes()))
}

fn create_layout(paths: &StatePaths) -> Result<(), StateError> {
    for path in [
        paths.cache(),
        paths.blob_cache(),
        paths.directory_cache(),
        paths.active(),
        paths.active().join("overlay"),
        paths.overlay_data(),
        paths.overlay_tmp(),
    ] {
        create_dir_private(&path)?;
    }
    Ok(())
}

fn validate_closed_session(paths: &StatePaths) -> Result<(), StateError> {
    validate_directory_entries(
        &paths.active(),
        &[
            ("session.db", false),
            ("rfsd.log", false),
            ("overlay", true),
        ],
    )
    .map_err(|error| stale(paths, error.to_string()))?;
    validate_directory_entries(
        &paths.active().join("overlay"),
        &[("data", true), ("tmp", true)],
    )
    .map_err(|error| stale(paths, error.to_string()))?;
    let previous: LockRecord = fs::read_to_string(paths.lock())
        .ok()
        .and_then(|value| serde_json::from_str(&value).ok())
        .ok_or_else(|| stale(paths, "invalid lock record".into()))?;
    if previous.record_version != LOCK_RECORD_VERSION {
        return Err(stale(paths, "unsupported lock record version".into()));
    }
    let connection =
        Connection::open_with_flags(paths.database(), OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|source| stale(paths, source.to_string()))?;
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|source| stale(paths, source.to_string()))?;
    if version != SCHEMA_VERSION {
        return Err(stale(
            paths,
            format!("unsupported schema version {version}"),
        ));
    }
    let row: Option<ClosedSessionMetadata> = connection.query_row(
        "SELECT session_id, daemon_pid, state, closed_at_seconds, closed_at_nanos FROM session_metadata WHERE singleton = 1",
        [], |row| Ok(ClosedSessionMetadata {
            session_id: row.get(0)?,
            daemon_pid: row.get(1)?,
            state: row.get(2)?,
            closed_at_seconds: row.get(3)?,
            closed_at_nanos: row.get(4)?,
        }),
    ).optional().map_err(|source| stale(paths, source.to_string()))?;
    let Some(metadata) = row else {
        return Err(stale(paths, "missing metadata".into()));
    };
    if metadata.session_id != previous.session_id || metadata.daemon_pid != previous.pid {
        return Err(stale(
            paths,
            "lock and database session identity do not match".into(),
        ));
    }
    if metadata.state != "closed"
        || metadata.closed_at_seconds.is_none()
        || metadata.closed_at_nanos.is_none()
    {
        return Err(stale(paths, "session was not closed cleanly".into()));
    }
    Ok(())
}

fn open_database(path: &Path) -> Result<Connection, StateError> {
    let connection = Connection::open(path).map_err(|source| db_error(path, source))?;
    connection
        .busy_timeout(Duration::from_secs(2))
        .map_err(|source| db_error(path, source))?;
    connection
        .execute_batch("PRAGMA journal_mode = DELETE; PRAGMA foreign_keys = ON;")
        .map_err(|source| db_error(path, source))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .map_err(|source| fs_error(path, source))?;
    Ok(connection)
}

fn migrate(connection: &Connection, path: &Path) -> Result<(), StateError> {
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|source| db_error(path, source))?;
    if version > SCHEMA_VERSION {
        return Err(stale_path(
            path,
            format!("unsupported schema version {version}"),
        ));
    }
    if version == 0 {
        connection.execute_batch("BEGIN;
            CREATE TABLE session_metadata (
              singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
              session_id TEXT NOT NULL CHECK (length(session_id) > 0),
              daemon_pid INTEGER NOT NULL CHECK (daemon_pid > 0),
              state TEXT NOT NULL CHECK (state IN ('initializing','active','closed')),
              root_digest_hash TEXT NOT NULL CHECK (length(root_digest_hash) = 64 AND root_digest_hash NOT GLOB '*[^0-9a-f]*'),
              root_digest_size INTEGER NOT NULL CHECK (root_digest_size >= 0),
              mountpoint TEXT NOT NULL CHECK (length(mountpoint) > 0),
              created_at_seconds INTEGER NOT NULL,
              created_at_nanos INTEGER NOT NULL CHECK (created_at_nanos BETWEEN 0 AND 999999999),
              closed_at_seconds INTEGER,
              closed_at_nanos INTEGER CHECK (closed_at_nanos IS NULL OR closed_at_nanos BETWEEN 0 AND 999999999),
              CHECK ((state = 'closed' AND closed_at_seconds IS NOT NULL AND closed_at_nanos IS NOT NULL) OR
                     (state != 'closed' AND closed_at_seconds IS NULL AND closed_at_nanos IS NULL))
            );
            PRAGMA user_version = 1; COMMIT;").map_err(|source| db_error(path, source))?;
    }
    Ok(())
}

fn insert_metadata(
    connection: &Connection,
    path: &Path,
    session_id: &str,
    startup: &SessionStartup,
) -> Result<(), StateError> {
    let (seconds, nanos) = now_parts()?;
    connection.execute("INSERT INTO session_metadata VALUES (1, ?1, ?2, 'initializing', ?3, ?4, ?5, ?6, ?7, NULL, NULL)", params![session_id, i64::from(startup.daemon_pid), startup.root_digest.hash(), startup.root_digest.size_bytes(), startup.mountpoint.to_string_lossy(), seconds, nanos])
        .map_err(|source| db_error(path, source))?;
    Ok(())
}

fn now_parts() -> Result<(i64, i64), StateError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StateError::InvalidSystemTime)?;
    Ok((
        i64::try_from(duration.as_secs()).map_err(|_| StateError::InvalidSystemTime)?,
        i64::from(duration.subsec_nanos()),
    ))
}

fn create_dir_private(path: &Path) -> Result<(), StateError> {
    match fs::create_dir(path) {
        Ok(()) => fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|source| fs_error(path, source)),
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path).map_err(|source| fs_error(path, source))?;
            if !metadata.file_type().is_dir() {
                return Err(StateError::UnsafePath {
                    path: path.to_path_buf(),
                    reason: "expected a directory and will not follow a symlink".into(),
                });
            }
            validate_existing_permissions(path, &metadata)
        }
        Err(source) => Err(fs_error(path, source)),
    }
}

fn create_home_private(path: &Path) -> Result<(), StateError> {
    fs::create_dir_all(path).map_err(|source| fs_error(path, source))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|source| fs_error(path, source))
}

fn create_file_private(path: &Path) -> Result<(), StateError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|source| fs_error(path, source))?;
    Ok(())
}

fn append_log(path: &Path, message: &str) -> Result<(), StateError> {
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(|source| fs_error(path, source))?;
    file.write_all(message.as_bytes())
        .map_err(|source| fs_error(path, source))
}

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

fn validate_directory_entries(
    directory: &Path,
    expected: &[(&str, bool)],
) -> Result<(), StateError> {
    let mut found = Vec::new();
    for entry in fs::read_dir(directory).map_err(|source| fs_error(directory, source))? {
        let entry = entry.map_err(|source| fs_error(directory, source))?;
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
        let metadata =
            fs::symlink_metadata(entry.path()).map_err(|source| fs_error(&entry.path(), source))?;
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

fn remove_if_present(path: &Path) -> Result<(), StateError> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(fs_error(path, source)),
    }
}

fn stale(paths: &StatePaths, reason: String) -> StateError {
    StateError::StaleSession {
        path: paths.active(),
        reason,
    }
}
fn stale_path(path: &Path, reason: String) -> StateError {
    StateError::StaleSession {
        path: path.to_path_buf(),
        reason,
    }
}
fn fs_error(path: &Path, source: std::io::Error) -> StateError {
    StateError::Filesystem {
        path: path.to_path_buf(),
        source,
    }
}
fn db_error(path: &Path, source: rusqlite::Error) -> StateError {
    StateError::Database {
        path: path.to_path_buf(),
        source,
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
        SessionStore::create(paths.clone(), startup.clone())
            .unwrap()
            .close_cleanly()
            .unwrap();
        fs::write(paths.cache().join("keep"), b"cache").unwrap();
        SessionStore::create(paths.clone(), startup)
            .unwrap()
            .close_cleanly()
            .unwrap();
        assert!(paths.cache().join("keep").exists());
    }

    #[test]
    fn cleanup_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp);
        paths.cleanup().unwrap();
        paths.cleanup().unwrap();
        assert_eq!(fs::read_dir(paths.home()).unwrap().count(), 0);
    }

    #[test]
    fn existing_home_rejects_permissions_for_other_users() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        fs::create_dir(&home).unwrap();
        fs::set_permissions(&home, fs::Permissions::from_mode(0o755)).unwrap();

        let result = StatePaths::from_config(&Config { rfs_home: home });

        assert!(matches!(result, Err(StateError::UnsafePath { .. })));
    }

    #[test]
    fn closed_session_with_unknown_active_entry_is_not_replaced() {
        let temp = tempfile::tempdir().unwrap();
        let paths = paths(&temp);
        let mountpoint = temp.path().join("mount");
        fs::create_dir(&mountpoint).unwrap();
        let startup = SessionStartup::new(Digest::for_bytes(b"root"), mountpoint);
        SessionStore::create(paths.clone(), startup.clone())
            .unwrap()
            .close_cleanly()
            .unwrap();
        fs::write(paths.active().join("unexpected"), b"preserve me").unwrap();

        let result = SessionStore::create(paths, startup);

        assert!(matches!(result, Err(StateError::StaleSession { .. })));
    }
}
