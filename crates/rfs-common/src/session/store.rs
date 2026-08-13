//! SQLite repository for session lifecycle and the merged inode namespace.
//!
//! [`SessionStore`] serializes access to one writable connection. The embedded
//! schema defines relational shape, while this module validates domain values
//! and converts database rows into session types.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use uuid::Uuid;

use crate::digest::Digest;

use super::{InodeId, NodeTime, SessionError, SessionLifecycle, now_parts, stale_path};

/// Fixed inode ID of the root directory in every session database.
pub const ROOT_INODE_ID: i64 = 1;

/// Repository owning the durable state for one active mount session.
///
/// All operations lock the single SQLite connection, so callers observe
/// transactionally complete namespace changes in call order.
pub struct SessionStore {
    /// Stable path included in validation and SQLite error diagnostics.
    database_path: PathBuf,
    /// Writable connection serialized across synchronous session operations.
    connection: Mutex<Connection>,
}

/// Validated session metadata returned by live and retained-state inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSession {
    /// Positive process ID recorded for the daemon that created the session.
    pub daemon_pid: u32,
    /// Current durable lifecycle state.
    pub state: SessionLifecycle,
    /// Immutable root directory digest mounted by the session.
    pub root_digest: Digest,
    /// Canonical absolute path of the mounted workspace.
    pub mountpoint: PathBuf,
}

/// Backing location from which a regular file's bytes can be read.
pub(super) enum ReadSource {
    /// Immutable content in the shared cache or remote CAS, identified by digest.
    Remote(String),
    /// Session-local content identified by a relative overlay-data path.
    Overlay(String),
}

/// Closed set of node kinds accepted from SQLite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StoreNodeKind {
    /// Persisted regular file.
    File,
    /// Persisted directory.
    Directory,
    /// Persisted symbolic link.
    Symlink,
}

/// Faithful representation of one row in the `inodes` table.
///
/// Decoded rows always carry an `inode`; insert rows leave it `None` to request
/// SQLite's `AUTOINCREMENT` allocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Inode {
    /// Raw SQLite primary key; `None` only when building an auto-allocated row.
    pub inode: Option<i64>,
    /// Raw nullable parent key; only root has no parent.
    pub parent_inode: Option<i64>,
    /// Persisted UTF-8 basename.
    pub name: String,
    /// Decoded persisted kind.
    pub kind: StoreNodeKind,
    /// Canonical digest text when the inode has remote backing.
    pub remote_digest: Option<String>,
    /// Exact persisted symlink target.
    pub symlink_target: Option<String>,
    /// Relative overlay-data filename when the inode has local backing.
    pub overlay_file: Option<String>,
    /// Raw nullable Unix mode.
    pub mode: Option<i64>,
    /// Raw nullable mtime seconds.
    pub mtime_seconds: Option<i64>,
    /// Raw nullable mtime nanoseconds paired with `mtime_seconds`.
    pub mtime_nanos: Option<i64>,
    /// Decoded namespace-visibility flag.
    pub tombstone: bool,
    /// Decoded content-dirty flag.
    pub content_dirty: bool,
    /// Decoded tree-dirty flag.
    pub tree_dirty: bool,
}

/// Store-native lazy lookup result.
pub(super) enum Lookup<T> {
    /// SQLite has a definitive result.
    Ready(T),
    /// The named remote directory still needs materialization.
    NeedsMaterialization { digest: String },
}

/// Schema version supported by this repository implementation.
const SCHEMA_VERSION: i64 = 1;
/// Idempotent baseline schema embedded in the `rfs-common` binary.
const SCHEMA_SQL: &str = include_str!("schema.sql");

/// Unvalidated `session_metadata` row decoded directly from SQLite.
struct RawSession {
    /// Singleton primary key, required to equal one.
    singleton: i64,
    /// UUID string uniquely identifying this mount session.
    session_id: String,
    /// Daemon process ID in SQLite's signed integer representation.
    daemon_pid: i64,
    /// Text representation of [`SessionLifecycle`].
    lifecycle: String,
    /// SHA-256 hash component of the immutable root digest.
    root_digest_hash: String,
    /// Byte-size component of the immutable root digest.
    root_digest_size: i64,
    /// UTF-8 absolute path of the mounted workspace.
    mountpoint: String,
    /// Whole seconds of the creation time relative to the Unix epoch.
    created_at_seconds: i64,
    /// Normalized nanosecond fraction of the creation time.
    created_at_nanos: i64,
    /// Whole seconds of the clean-close time, present only when closed.
    closed_at_seconds: Option<i64>,
    /// Nanosecond fraction of the clean-close time, present only when closed.
    closed_at_nanos: Option<i64>,
    /// Effective daemon logging level recorded for status reporting.
    log_level: String,
    /// Effective daemon log encoding, either `text` or `json`.
    log_format: String,
}

impl SessionStore {
    /// Opens writable state and atomically initializes one active session.
    ///
    /// The database file must already be openable for writing. This installs the
    /// supported schema, records session metadata, and creates root inode `1`.
    /// SQLite, timestamp, path-encoding, and existing-row failures are returned
    /// as [`SessionError`].
    pub fn create(
        database_path: PathBuf,
        session_id: String,
        daemon_pid: u32,
        root_digest: Digest,
        mountpoint: PathBuf,
        root_inode: Inode,
    ) -> Result<Self, SessionError> {
        tracing::info!(
            "SessionStore::create(db_path={}, session_id={}, daemon_pid={}, root_digest={}, mountpoint={})",
            database_path.display(),
            session_id,
            daemon_pid,
            root_digest,
            mountpoint.display()
        );
        let mut connection = open_database(&database_path, false)?;
        initialize_database(
            &mut connection,
            &database_path,
            &session_id,
            daemon_pid,
            &root_digest,
            &mountpoint,
            &root_inode,
        )?;
        Ok(Self {
            database_path,
            connection: Mutex::new(connection),
        })
    }

    /// Reads and validates retained session metadata without modifying the file.
    ///
    /// The database is opened read-only and must have exactly the supported
    /// schema version and one valid singleton metadata row.
    pub fn inspect(path: &Path) -> Result<StoredSession, SessionError> {
        let connection = open_database(path, true)?;
        validate_schema_version(&connection, path)?;
        read_stored_session(&connection, path)
    }

    /// Returns one visible inode's stored row.
    ///
    /// Missing and tombstoned rows return [`SessionError::UnknownInode`];
    /// malformed persisted fields return an invalid-session error.
    pub fn node(&self, inode: InodeId) -> Result<Inode, SessionError> {
        let connection = self.connection("read visible inode")?;
        let stored = read_inode_by_id(&connection, &self.database_path, inode)?
            .filter(|node| !node.tombstone)
            .ok_or(SessionError::UnknownInode { inode })?;
        Ok(stored)
    }

    /// Looks up a visible child by UTF-8 basename.
    ///
    /// Returns the child when already known, the parent's remote directory
    /// digest when its complete child set still needs materialization, or
    /// [`SessionError::NotFound`] after materialization proves absence. The
    /// parent must be a visible remote-backed directory and `name` must be a
    /// single non-special path component.
    pub fn lookup(&self, parent: InodeId, name: &str) -> Result<Lookup<Inode>, SessionError> {
        validate_child_name(&self.database_path, name)?;
        let connection = self.connection("look up visible child")?;
        let parent_node = required_directory(&connection, &self.database_path, parent)?;
        if let Some(child) = read_child(&connection, &self.database_path, parent, name)?
            && !child.tombstone
        {
            return Ok(Lookup::Ready(child));
        }
        match materialized_digest(&connection, &self.database_path, parent)? {
            Some(_) => Err(SessionError::NotFound {
                parent,
                name: name.to_owned(),
            }),
            None => Ok(Lookup::NeedsMaterialization {
                digest: required_remote_digest(&self.database_path, &parent_node)?.to_owned(),
            }),
        }
    }

    /// Lists a directory's visible children in ascending basename order.
    ///
    /// Returns the remote directory digest instead when the complete child set
    /// has not been materialized. The inode must identify a visible,
    /// remote-backed directory.
    pub fn list_directory(&self, inode: InodeId) -> Result<Lookup<Vec<Inode>>, SessionError> {
        let connection = self.connection("list visible directory")?;
        let directory = required_directory(&connection, &self.database_path, inode)?;
        if materialized_digest(&connection, &self.database_path, inode)?.is_none() {
            return Ok(Lookup::NeedsMaterialization {
                digest: required_remote_digest(&self.database_path, &directory)?.to_owned(),
            });
        }
        Ok(Lookup::Ready(read_visible_children(
            &connection,
            &self.database_path,
            inode,
        )?))
    }

    /// Atomically records and returns the complete visible child set of a directory.
    ///
    /// `children` must have valid unique basenames and must describe `digest`,
    /// the immutable remote identity currently stored for `parent`. Repeating
    /// the same materialization is allowed; conflicting remote identity or child
    /// sets are rejected. Existing tombstones and overlay-backed children take
    /// precedence. The session must be active, and no partial inserts are
    /// committed on failure.
    pub fn materialize_directory(
        &self,
        parent: InodeId,
        digest: &str,
        children: &[Inode],
    ) -> Result<Vec<Inode>, SessionError> {
        validate_remote_children(&self.database_path, children)?;
        let mut connection = self.connection("materialize remote directory")?;
        let transaction = connection.transaction().map_err(|source| {
            db_error(
                "begin directory materialization",
                &self.database_path,
                source,
            )
        })?;
        ensure_active(&transaction, &self.database_path)?;
        let parent_node = required_directory(&transaction, &self.database_path, parent)?;
        if required_remote_digest(&self.database_path, &parent_node)? != digest {
            return Err(stale_path(
                &self.database_path,
                format!("inode {parent} is not remote directory {digest}"),
            ));
        }
        let prior = materialized_digest(&transaction, &self.database_path, parent)?;
        if prior.as_deref().is_some_and(|prior| prior != digest) {
            return Err(stale_path(
                &self.database_path,
                format!("directory inode {parent} was materialized with another digest"),
            ));
        }
        reconcile_children(&transaction, &self.database_path, parent, children)?;
        if prior.is_none() {
            transaction
                .execute(
                    "INSERT INTO directory_materializations (inode, directory_digest)
                     VALUES (?1, ?2)",
                    params![parent.sqlite(), digest],
                )
                .map_err(|source| {
                    db_error(
                        "record directory materialization",
                        &self.database_path,
                        source,
                    )
                })?;
        }
        let visible = read_visible_children(&transaction, &self.database_path, parent)?;
        transaction.commit().map_err(|source| {
            db_error(
                "commit directory materialization",
                &self.database_path,
                source,
            )
        })?;
        Ok(visible)
    }

    /// Resolves the byte backing for a visible regular file.
    ///
    /// Overlay content takes precedence over a remote digest. Missing,
    /// tombstoned, wrong-kind, and unbacked file rows return [`SessionError`].
    pub fn get_file_source(&self, inode: InodeId) -> Result<ReadSource, SessionError> {
        let connection = self.connection("resolve inode read source")?;
        let stored = read_inode_by_id(&connection, &self.database_path, inode)?
            .filter(|node| !node.tombstone)
            .ok_or(SessionError::UnknownInode { inode })?;
        if stored.kind != StoreNodeKind::File {
            return Err(SessionError::WrongKind {
                inode,
                expected: super::NodeKind::File,
                actual: session_node_kind(stored.kind),
            });
        }
        if let Some(path) = stored.overlay_file {
            return Ok(ReadSource::Overlay(path));
        }
        stored.remote_digest.map(ReadSource::Remote).ok_or_else(|| {
            stale_path(
                &self.database_path,
                format!("regular file inode {inode} has no content backing"),
            )
        })
    }

    /// Atomically marks an active session as cleanly closed at the current time.
    ///
    /// This store-level transition requires the current lifecycle to be
    /// [`SessionLifecycle::Active`]; repeated close calls are rejected. The
    /// session facade provides the externally visible idempotent close behavior.
    pub fn close(&self) -> Result<(), SessionError> {
        let (seconds, nanos) = now_parts()?;
        let mut connection = self.connection("close session")?;
        let transaction = connection
            .transaction()
            .map_err(|source| db_error("begin clean close", &self.database_path, source))?;
        let stored = read_stored_session(&transaction, &self.database_path)?;
        if stored.state != SessionLifecycle::Active {
            return Err(stale_path(
                &self.database_path,
                format!("cannot close session while lifecycle is {}", stored.state),
            ));
        }
        transaction
            .execute(
                "UPDATE session_metadata
                 SET lifecycle = 'closed', closed_at_seconds = ?1, closed_at_nanos = ?2
                 WHERE singleton = 1",
                params![seconds, nanos],
            )
            .map_err(|source| db_error("mark session closed", &self.database_path, source))?;
        transaction
            .commit()
            .map_err(|source| db_error("commit clean close", &self.database_path, source))
    }

    /// Locks the session-owned connection for one synchronous repository operation.
    ///
    /// `operation` identifies the caller in synchronization diagnostics and must
    /// describe the operation that will use the returned guard. The caller must
    /// not already hold this store's non-reentrant mutex. Returns exclusive
    /// access to the writable connection until the guard is dropped. Returns
    /// [`SessionError::Synchronization`] if another thread poisoned the mutex;
    /// acquiring the guard otherwise has no database side effects.
    fn connection(
        &self,
        operation: &'static str,
    ) -> Result<MutexGuard<'_, Connection>, SessionError> {
        self.connection
            .lock()
            .map_err(|_| SessionError::Synchronization { operation })
    }
}

/// Installs the schema and creates the singleton metadata and root-inode rows.
///
/// `connection` is the writable connection for `path`; `session_id`,
/// `daemon_pid`, `root_digest`, and `mountpoint` become the immutable identity of
/// the new session. The database must not already contain session metadata or a
/// root inode, `mountpoint` must be UTF-8, and the supplied connection must allow
/// schema and row writes. Returns `()` after both rows commit atomically. Returns
/// [`SessionError`] for schema/version, path-encoding, clock, SQLite, or
/// constraint failures; a failed transaction commits neither initialization row.
fn initialize_database(
    connection: &mut Connection,
    path: &Path,
    session_id: &str,
    daemon_pid: u32,
    root_digest: &Digest,
    mountpoint: &Path,
    root_inode: &Inode,
) -> Result<(), SessionError> {
    prepare_schema(connection, path)?;
    let mountpoint = mountpoint
        .to_str()
        .ok_or_else(|| stale_path(path, "mountpoint is not UTF-8".into()))?;
    let (seconds, nanos) = now_parts()?;
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
             ) VALUES (1, ?1, ?2, 'active', ?3, ?4, ?5, ?6, ?7, NULL, NULL, 'info', 'text')",
            params![
                session_id,
                i64::from(daemon_pid),
                root_digest.hash(),
                root_digest.size_bytes(),
                mountpoint,
                seconds,
                nanos,
            ],
        )
        .map_err(|source| db_error("insert session metadata", path, source))?;
    insert_inode(&transaction, path, "insert root inode", root_inode)?;
    transaction
        .commit()
        .map_err(|source| db_error("commit session initialization", path, source))
}

/// Merges one authoritative remote child set into a materialized directory.
///
/// `transaction` supplies the atomic write boundary, `path` identifies its
/// database in errors, `parent` is the directory inode, and `children` is the
/// already-validated remote child set. The parent must exist and each child name
/// must be unique and valid. Existing tombstones and overlay-backed rows win over
/// remote data; equivalent remote rows are retained and missing rows are
/// inserted. Returns `()` when the stored remote identities exactly reconcile.
/// Returns [`SessionError`] on reads, inserts, malformed stored rows, or any
/// conflicting/stale remote identity. The caller decides whether to commit or
/// roll back changes made before an error.
fn reconcile_children(
    transaction: &Transaction<'_>,
    path: &Path,
    parent: InodeId,
    children: &[Inode],
) -> Result<(), SessionError> {
    let mut remote_names = HashSet::with_capacity(children.len());
    for child in children {
        remote_names.insert(child.name.as_str());
        match read_child(transaction, path, parent, &child.name)? {
            Some(stored) if stored.tombstone || stored.overlay_file.is_some() => {}
            Some(stored) if remote_identity_matches(&stored, child) => {}
            Some(_) => {
                return Err(stale_path(
                    path,
                    format!("remote identity changed for inode {parent}/{}", child.name),
                ));
            }
            None => insert_remote_child(transaction, path, parent, child)?,
        }
    }
    for stored in read_children(transaction, path, parent)? {
        if (stored.remote_digest.is_some() || stored.kind == StoreNodeKind::Symlink)
            && stored.overlay_file.is_none()
            && !stored.tombstone
            && !remote_names.contains(stored.name.as_str())
        {
            return Err(stale_path(
                path,
                format!("remote child set changed for directory inode {parent}"),
            ));
        }
    }
    Ok(())
}

/// Inserts a single immutable remote child as a clean inode row.
///
/// `transaction` is the caller-owned materialization transaction, `path`
/// identifies the database in errors, `parent` is the owning directory, and
/// `child` provides the validated name, content identity, mode, and mtime. The
/// parent row must exist, and no row with the same `(parent, name)` may exist.
/// Returns `()` after the insert is staged in the transaction. Returns
/// [`SessionError`] if SQLite rejects the row; the insert remains uncommitted
/// until the caller commits the transaction.
fn insert_remote_child(
    transaction: &Transaction<'_>,
    path: &Path,
    parent: InodeId,
    child: &Inode,
) -> Result<(), SessionError> {
    debug_assert_eq!(child.parent_inode, Some(parent.sqlite()));
    insert_inode(transaction, path, "insert materialized child", child)
}

fn insert_inode(
    transaction: &Transaction<'_>,
    path: &Path,
    operation: &'static str,
    inode: &Inode,
) -> Result<(), SessionError> {
    transaction
        .execute(
            "INSERT INTO inodes (
                inode, parent_inode, name, kind, remote_digest, symlink_target,
                overlay_file, mode, mtime_seconds, mtime_nanos, tombstone,
                content_dirty, tree_dirty
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                inode.inode,
                inode.parent_inode,
                inode.name,
                node_kind_text(inode.kind),
                inode.remote_digest,
                inode.symlink_target,
                inode.overlay_file,
                inode.mode,
                inode.mtime_seconds,
                inode.mtime_nanos,
                i64::from(inode.tombstone),
                i64::from(inode.content_dirty),
                i64::from(inode.tree_dirty),
            ],
        )
        .map_err(|source| db_error(operation, path, source))?;
    Ok(())
}

/// Tests whether a stored inode has exactly the remote identity and metadata supplied by a child.
///
/// `stored` is a validated database row and `child` is a validated remote entry.
/// No additional preconditions apply. Returns `true` only when kind, content
/// digest or symlink target, optional mode, and optional mtime all match; overlay
/// and tombstone precedence is handled by the caller. This pure comparison does
/// not fail and has no side effects.
fn remote_identity_matches(stored: &Inode, child: &Inode) -> bool {
    stored.kind == child.kind
        && stored.remote_digest == child.remote_digest
        && stored.symlink_target == child.symlink_target
        && stored.mode == child.mode
        && stored.mtime_seconds == child.mtime_seconds
        && stored.mtime_nanos == child.mtime_nanos
}

/// Loads a visible inode and requires it to be a directory.
///
/// `connection` is any connection containing the session schema, `path`
/// identifies that database in diagnostics, and `inode` is the requested ID.
/// Returns the validated stored row, including its persistence-only fields.
/// Returns [`SessionError::UnknownInode`] for a missing or tombstoned row,
/// [`SessionError::WrongKind`] for another node kind, or [`SessionError`] for
/// SQLite and persisted-row validation failures. The database is not modified.
fn required_directory(
    connection: &Connection,
    path: &Path,
    inode: InodeId,
) -> Result<Inode, SessionError> {
    let node = read_inode_by_id(connection, path, inode)?
        .filter(|node| !node.tombstone)
        .ok_or(SessionError::UnknownInode { inode })?;
    if node.kind != StoreNodeKind::Directory {
        return Err(SessionError::WrongKind {
            inode,
            expected: super::NodeKind::Directory,
            actual: session_node_kind(node.kind),
        });
    }
    Ok(node)
}

/// Extracts the remote digest required to materialize a directory row.
///
/// `path` identifies the database in corruption diagnostics and `node` is a
/// validated stored row that the caller must already have established is a
/// directory. Returns a clone of its remote digest. Returns [`SessionError`] if
/// the directory has no remote backing, which indicates stale or malformed
/// persisted state. The row and database are unchanged.
fn required_remote_digest<'a>(path: &Path, node: &'a Inode) -> Result<&'a str, SessionError> {
    node.remote_digest.as_deref().ok_or_else(|| {
        let inode = node
            .inode
            .expect("store inode rows always carry a decoded identity");
        stale_path(
            path,
            format!("directory inode {inode} has no remote digest"),
        )
    })
}

/// Reads the remote digest previously recorded for a directory materialization.
///
/// `connection` is a session database connection, `path` identifies it in
/// diagnostics, and `inode` is the directory whose marker is queried. The caller
/// need not first prove that the inode exists. Returns `None` when no marker is
/// present or `Some(digest)` after parsing the stored value. Returns
/// [`SessionError`] for SQLite failures or an invalid persisted digest. This is a
/// read-only operation.
fn materialized_digest(
    connection: &Connection,
    path: &Path,
    inode: InodeId,
) -> Result<Option<String>, SessionError> {
    let value: Option<String> = connection
        .query_row(
            "SELECT directory_digest FROM directory_materializations WHERE inode = ?1",
            [inode.sqlite()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|source| db_error("read directory materialization", path, source))?;
    Ok(value)
}

/// Looks up and validates one inode by its durable ID.
///
/// `connection` is a session database connection, `path` identifies it in
/// diagnostics, and `inode` is the exact primary key to query. The session schema
/// must be present. Returns `None` when no row exists or `Some(Inode)` for a
/// decoded row, including tombstones. Returns [`SessionError`] for SQLite or
/// store-representation failures. The database is not modified.
fn read_inode_by_id(
    connection: &Connection,
    path: &Path,
    inode: InodeId,
) -> Result<Option<Inode>, SessionError> {
    query_inode(
        connection,
        path,
        "read inode",
        "SELECT inode, parent_inode, name, kind, remote_digest, symlink_target,
                overlay_file, mode, mtime_seconds, mtime_nanos, tombstone,
                content_dirty, tree_dirty
         FROM inodes WHERE inode = ?1",
        params![inode.sqlite()],
    )
}

/// Looks up and validates one child by its parent inode and basename.
///
/// `connection` is a session database connection, `path` identifies it in
/// diagnostics, `parent` is the parent inode ID, and `name` is the exact stored
/// basename. Callers that accept untrusted names must validate `name` first.
/// Returns `None` when the pair has no row or `Some(Inode)` for a decoded row,
/// including tombstones. Returns [`SessionError`] for SQLite or
/// store-representation failures. The database is not modified.
fn read_child(
    connection: &Connection,
    path: &Path,
    parent: InodeId,
    name: &str,
) -> Result<Option<Inode>, SessionError> {
    query_inode(
        connection,
        path,
        "read child",
        "SELECT inode, parent_inode, name, kind, remote_digest, symlink_target,
                overlay_file, mode, mtime_seconds, mtime_nanos, tombstone,
                content_dirty, tree_dirty
         FROM inodes WHERE parent_inode = ?1 AND name = ?2",
        params![parent.sqlite(), name],
    )
}

/// Reads and validates every stored child of a parent in basename order.
///
/// `connection` is a session database connection, `path` identifies it in
/// diagnostics, and `parent` selects the rows; the parent need not itself exist
/// for this query. Returns all matching rows, including tombstones, sorted by
/// `name`. Returns [`SessionError`] if statement preparation, querying, row
/// decoding, or domain validation fails. No database state changes.
fn read_children(
    connection: &Connection,
    path: &Path,
    parent: InodeId,
) -> Result<Vec<Inode>, SessionError> {
    let mut statement = connection
        .prepare(
            "SELECT inode, parent_inode, name, kind, remote_digest, symlink_target,
                    overlay_file, mode, mtime_seconds, mtime_nanos, tombstone,
                    content_dirty, tree_dirty
             FROM inodes WHERE parent_inode = ?1 ORDER BY name",
        )
        .map_err(|source| db_error("prepare child listing", path, source))?;
    let mut rows = statement
        .query([parent.sqlite()])
        .map_err(|source| db_error("list children", path, source))?;
    let mut children = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|source| db_error("read child row", path, source))?
    {
        children.push(decode_inode_row(path, row)?);
    }
    Ok(children)
}

/// Reads a parent's visible children in basename order.
///
/// `connection`, `path`, and `parent` have the same requirements as
/// [`read_children`]. Returns each complete store [`Inode`] for non-tombstoned
/// rows. Returns [`SessionError`] for failures inherited from `read_children`.
/// This operation is read-only.
fn read_visible_children(
    connection: &Connection,
    path: &Path,
    parent: InodeId,
) -> Result<Vec<Inode>, SessionError> {
    Ok(read_children(connection, path, parent)?
        .into_iter()
        .filter(|stored| !stored.tombstone)
        .collect())
}

/// Executes a single-row inode query and validates its optional result.
///
/// `connection` is a session database connection, `path` identifies it in
/// diagnostics, `operation` names the query for SQLite errors, `sql` must select
/// the thirteen inode columns in schema order, and `parameters` must bind that
/// statement. Returns `None` for no row or a decoded [`Inode`] for one row.
/// Returns [`SessionError`] for SQLite, column-decoding, or store-representation
/// failures. The SQL must identify at most one logical row. The supplied SQL is
/// expected to be read-only and this helper adds no writes.
fn query_inode(
    connection: &Connection,
    path: &Path,
    operation: &'static str,
    sql: &str,
    parameters: impl rusqlite::Params,
) -> Result<Option<Inode>, SessionError> {
    let mut statement = connection
        .prepare(sql)
        .map_err(|source| db_error(operation, path, source))?;
    let mut rows = statement
        .query(parameters)
        .map_err(|source| db_error(operation, path, source))?;
    let Some(row) = rows
        .next()
        .map_err(|source| db_error(operation, path, source))?
    else {
        return Ok(None);
    };
    Ok(Some(decode_inode_row(path, row)?))
}

/// Decodes all inode columns without applying facade defaults or projections.
fn decode_inode_row(path: &Path, row: &rusqlite::Row<'_>) -> Result<Inode, SessionError> {
    let inode = row
        .get(0)
        .map_err(|source| db_error("decode inode identity", path, source))?;
    let kind: String = row
        .get(3)
        .map_err(|source| db_error("decode inode kind", path, source))?;
    let kind = parse_node_kind(path, inode, &kind)?;
    let mtime_seconds: Option<i64> = row
        .get(8)
        .map_err(|source| db_error("decode inode mtime seconds", path, source))?;
    let mtime_nanos: Option<i64> = row
        .get(9)
        .map_err(|source| db_error("decode inode mtime nanos", path, source))?;
    if mtime_seconds.is_some() != mtime_nanos.is_some() {
        return Err(stale_path(
            path,
            format!("inode {inode} has a partial modification time"),
        ));
    }
    Ok(Inode {
        inode: Some(inode),
        parent_inode: row
            .get(1)
            .map_err(|source| db_error("decode inode parent", path, source))?,
        name: row
            .get(2)
            .map_err(|source| db_error("decode inode name", path, source))?,
        kind,
        remote_digest: row
            .get(4)
            .map_err(|source| db_error("decode inode remote digest", path, source))?,
        symlink_target: row
            .get(5)
            .map_err(|source| db_error("decode inode symlink target", path, source))?,
        overlay_file: row
            .get(6)
            .map_err(|source| db_error("decode inode overlay file", path, source))?,
        mode: row
            .get(7)
            .map_err(|source| db_error("decode inode mode", path, source))?,
        mtime_seconds,
        mtime_nanos,
        tombstone: parse_boolean(
            path,
            inode,
            "tombstone",
            row.get(10)
                .map_err(|source| db_error("decode inode tombstone", path, source))?,
        )?,
        content_dirty: parse_boolean(
            path,
            inode,
            "content_dirty",
            row.get(11)
                .map_err(|source| db_error("decode inode content dirty", path, source))?,
        )?,
        tree_dirty: parse_boolean(
            path,
            inode,
            "tree_dirty",
            row.get(12)
                .map_err(|source| db_error("decode inode tree dirty", path, source))?,
        )?,
    })
}

/// Validates the names and uniqueness of a complete remote child set.
///
/// `path` identifies the session database in validation errors and `children`
/// contains the entries proposed for one directory. No database access is
/// required. Returns `()` when every basename is valid and appears exactly once.
/// Returns [`SessionError`] for an empty, special, slash-containing, or duplicate
/// name. The slice is not modified and validation has no side effects.
fn validate_remote_children(path: &Path, children: &[Inode]) -> Result<(), SessionError> {
    let mut names = HashSet::with_capacity(children.len());
    for child in children {
        validate_child_name(path, &child.name)?;
        if !names.insert(&child.name) {
            return Err(stale_path(
                path,
                format!("duplicate remote child name `{}`", child.name),
            ));
        }
    }
    Ok(())
}

/// Validates one basename accepted by the merged inode namespace.
///
/// `path` identifies the session database in diagnostics and `name` is the
/// candidate UTF-8 child name. Returns `()` when `name` is non-empty, contains no
/// slash, and is neither `.` nor `..`. Returns [`SessionError`] otherwise. The
/// function performs no I/O and does not normalize or mutate the name.
fn validate_child_name(path: &Path, name: &str) -> Result<(), SessionError> {
    if name.is_empty() || name.contains('/') || matches!(name, "." | "..") {
        return Err(stale_path(
            path,
            format!("invalid remote child name `{name}`"),
        ));
    }
    Ok(())
}

/// Requires the singleton session metadata row to have the active lifecycle.
///
/// `connection` must contain a readable session schema and `path` identifies the
/// database in diagnostics. Returns `()` for an active session. Returns
/// [`SessionError`] when metadata cannot be read or validated, or when the
/// lifecycle is not active. The check is read-only; callers must perform it
/// inside the same transaction as any guarded mutation to preserve the
/// precondition through commit.
fn ensure_active(connection: &Connection, path: &Path) -> Result<(), SessionError> {
    let stored = read_stored_session(connection, path)?;
    if stored.state != SessionLifecycle::Active {
        return Err(stale_path(
            path,
            format!(
                "cannot materialize inodes while session is {}",
                stored.state
            ),
        ));
    }
    Ok(())
}

/// Reads and validates the singleton row used for session inspection.
///
/// `connection` must contain the supported `session_metadata` table and `path`
/// identifies its database in diagnostics. Returns validated daemon PID,
/// lifecycle, root digest, and mountpoint metadata. Returns [`SessionError`] when
/// the row is missing, SQLite decoding fails, or any stored session invariant is
/// invalid. The database is not modified.
fn read_stored_session(
    connection: &Connection,
    path: &Path,
) -> Result<StoredSession, SessionError> {
    let row = connection
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
        .map_err(|source| db_error("read session metadata", path, source))?
        .ok_or_else(|| stale_path(path, "missing session metadata".into()))?;
    validate_session_row(path, row)
}

/// Converts raw session metadata into the externally useful validated subset.
///
/// `path` identifies the database in corruption diagnostics and `row` is the
/// untrusted SQLite representation of the singleton metadata. The row must
/// describe a UUID session, positive daemon PID, supported lifecycle, coherent
/// timestamps, valid digest, absolute mountpoint, and supported logging values.
/// Returns `StoredSession` when all invariants hold. Returns [`SessionError`] at
/// the first invalid field or cross-field condition. This pure validation does
/// not modify the database.
fn validate_session_row(path: &Path, row: RawSession) -> Result<StoredSession, SessionError> {
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
    validate_timestamp(
        path,
        "creation time",
        row.created_at_seconds,
        row.created_at_nanos,
    )?;
    match (state, row.closed_at_seconds, row.closed_at_nanos) {
        (SessionLifecycle::Closed, Some(seconds), Some(nanos)) => {
            validate_timestamp(path, "closed time", seconds, nanos)?;
        }
        (SessionLifecycle::Closed, _, _) => {
            return Err(stale_path(path, "closed session has no closed time".into()));
        }
        (_, None, None) => {}
        _ => {
            return Err(stale_path(
                path,
                "non-closed session has a closed time".into(),
            ));
        }
    }
    let root_digest = Digest::new(row.root_digest_hash, row.root_digest_size)
        .map_err(|error| stale_path(path, format!("invalid root digest: {error}")))?;
    let mountpoint = PathBuf::from(row.mountpoint);
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
        daemon_pid,
        state,
        root_digest,
        mountpoint,
    })
}

/// Opens and configures an existing SQLite session database.
///
/// `path` is the exact database file and `read_only` selects read-only inspection
/// or writable daemon access. The file must already exist and be accessible with
/// the requested mode. Returns a connection with a two-second busy timeout; a
/// writable connection also uses rollback-journal mode and enables foreign keys.
/// Returns [`SessionError`] if opening or any pragma configuration fails. The
/// writable path may update SQLite's journal-mode metadata, while read-only mode
/// does not intentionally mutate the file.
fn open_database(path: &Path, read_only: bool) -> Result<Connection, SessionError> {
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

/// Creates the baseline schema or verifies compatibility before reapplying it.
///
/// `connection` must be writable and refer to `path`. A zero `user_version`
/// denotes an unversioned database eligible for initialization; any nonzero
/// version must equal [`SCHEMA_VERSION`]. Returns `()` after idempotently applying
/// [`SCHEMA_SQL`] and recording the supported version for a new database. Returns
/// [`SessionError`] for version mismatches or SQLite failures. SQLite may retain
/// statements completed before a later error because this helper does not create
/// an explicit transaction.
fn prepare_schema(connection: &Connection, path: &Path) -> Result<(), SessionError> {
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

/// Reads SQLite's `user_version` value for a session database.
///
/// `connection` is the database to query and `path` identifies it in errors. The
/// connection must support pragma reads. Returns the signed schema-version value
/// exactly as stored. Returns [`SessionError`] if SQLite cannot read or decode
/// the pragma. The database is not modified.
fn schema_version(connection: &Connection, path: &Path) -> Result<i64, SessionError> {
    connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|source| db_error("read schema version", path, source))
}

/// Requires a database's schema version to match this repository implementation.
///
/// `connection` is the database to inspect and `path` identifies it in errors.
/// Returns `()` only when `user_version` equals [`SCHEMA_VERSION`]. Returns
/// [`SessionError`] when the pragma cannot be read or the version is unsupported.
/// The check is read-only and performs no migration or repair.
fn validate_schema_version(connection: &Connection, path: &Path) -> Result<(), SessionError> {
    let version = schema_version(connection, path)?;
    if version != SCHEMA_VERSION {
        return Err(stale_path(
            path,
            format!("unsupported state schema version {version}; expected {SCHEMA_VERSION}"),
        ));
    }
    Ok(())
}

/// Parses the persisted text representation of a session lifecycle.
///
/// `path` identifies the database in validation diagnostics and `value` is the
/// exact SQLite text to decode. Returns the matching [`SessionLifecycle`] for
/// `initializing`, `active`, or `closed`. Returns [`SessionError`] for any other
/// spelling. Parsing is case-sensitive, allocates only on error, and has no side
/// effects.
fn parse_lifecycle(path: &Path, value: &str) -> Result<SessionLifecycle, SessionError> {
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

/// Parses the persisted text representation of an inode kind.
///
/// `path` identifies the database in validation diagnostics and `value` is the
/// exact SQLite text to decode. Returns the matching [`NodeKind`] for `file`,
/// `directory`, or `symlink`. Returns [`SessionError`] for any other spelling.
/// Parsing is case-sensitive, allocates only on error, and has no side effects.
fn parse_node_kind(path: &Path, inode: i64, value: &str) -> Result<StoreNodeKind, SessionError> {
    match value {
        "file" => Ok(StoreNodeKind::File),
        "directory" => Ok(StoreNodeKind::Directory),
        "symlink" => Ok(StoreNodeKind::Symlink),
        _ => Err(stale_path(
            path,
            format!("inode {inode} has unsupported kind `{value}`"),
        )),
    }
}

/// Returns the canonical SQLite text representation of an inode kind.
///
/// `kind` is any [`NodeKind`] value; there are no additional preconditions.
/// Returns one of the static strings `file`, `directory`, or `symlink`. This
/// total conversion cannot fail, allocate, or modify state.
fn node_kind_text(kind: StoreNodeKind) -> &'static str {
    match kind {
        StoreNodeKind::File => "file",
        StoreNodeKind::Directory => "directory",
        StoreNodeKind::Symlink => "symlink",
    }
}

fn session_node_kind(kind: StoreNodeKind) -> super::NodeKind {
    match kind {
        StoreNodeKind::File => super::NodeKind::File,
        StoreNodeKind::Directory => super::NodeKind::Directory,
        StoreNodeKind::Symlink => super::NodeKind::Symlink,
    }
}

/// Decodes a SQLite integer constrained to the repository's boolean encoding.
///
/// `path` identifies the database, while `inode` and `field` identify the owning
/// value in diagnostics; `value` is the raw integer. Returns `false` for zero and
/// `true` for one. Returns [`SessionError`] for every other integer. This pure
/// validation does not modify database state.
fn parse_boolean(path: &Path, inode: i64, field: &str, value: i64) -> Result<bool, SessionError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(stale_path(
            path,
            format!("inode {inode} has invalid {field}"),
        )),
    }
}

/// Validates a persisted seconds-and-nanoseconds timestamp pair.
///
/// `path` identifies the database, `field` names the timestamp in diagnostics,
/// and `seconds`/`nanos` are the raw SQLite values. Returns `()` when the pair can
/// construct a [`NodeTime`], including a nanosecond fraction in its valid range.
/// Returns [`SessionError`] when seconds fall outside the supported REAPI range
/// or nanoseconds are negative, overflow `u32`, or are not normalized. No state
/// is changed.
fn validate_timestamp(
    path: &Path,
    field: &str,
    seconds: i64,
    nanos: i64,
) -> Result<(), SessionError> {
    let nanos = u32::try_from(nanos).ok();
    if nanos
        .and_then(|nanos| NodeTime::new(seconds, nanos))
        .is_none()
    {
        return Err(stale_path(path, format!("{field} is invalid")));
    }
    Ok(())
}

/// Wraps a SQLite failure with its repository operation and database identity.
///
/// `operation` names the failed database action, `path` is the exact database
/// path, and `source` is the original [`rusqlite::Error`]. Callers should provide
/// stable, action-specific operation text. Returns [`SessionError::Database`]
/// while preserving `source`; this conversion cannot itself fail and has no side
/// effects beyond cloning the path into the error.
fn db_error(operation: &'static str, path: &Path, source: rusqlite::Error) -> SessionError {
    SessionError::Database {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Verifies that the embedded schema can be applied twice and creates each required table once.
    ///
    /// This test takes no arguments and requires only an in-memory SQLite
    /// connection. It returns `()` after checking the schema and panics if schema
    /// execution fails, a required relation is absent or duplicated, or SQL
    /// domain checks have been introduced. All database effects remain confined
    /// to the temporary in-memory connection.
    #[test]
    fn embedded_schema_is_idempotent_and_has_expected_relations() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(SCHEMA_SQL).unwrap();
        connection.execute_batch(SCHEMA_SQL).unwrap();
        let tables: HashMap<String, i64> =
            ["session_metadata", "inodes", "directory_materializations"]
                .into_iter()
                .map(|name| {
                    let count = connection
                        .query_row(
                            "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ?1",
                            [name],
                            |row| row.get(0),
                        )
                        .unwrap();
                    (name.to_owned(), count)
                })
                .collect();
        assert!(tables.values().all(|count| *count == 1));
        assert!(!SCHEMA_SQL.to_ascii_uppercase().contains("CHECK"));
    }

    /// Builds a `SessionStore` over an in-memory connection seeded with one raw inode row.
    fn store_with_inode(row_values: &str) -> SessionStore {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(SCHEMA_SQL).unwrap();
        connection
            .execute(
                "INSERT INTO inodes (inode, parent_inode, name, kind, remote_digest, symlink_target,
                 overlay_file, mode, mtime_seconds, mtime_nanos, tombstone, content_dirty, tree_dirty)
                 VALUES (1, NULL, '', 'directory', NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0)",
                [],
            )
            .unwrap();
        connection
            .execute(
                &format!(
                    "INSERT INTO inodes (inode, parent_inode, name, kind, remote_digest, symlink_target,
                     overlay_file, mode, mtime_seconds, mtime_nanos, tombstone, content_dirty, tree_dirty)
                     VALUES ({row_values})"
                ),
                [],
            )
            .unwrap();
        SessionStore {
            database_path: PathBuf::from("/state/session.db"),
            connection: Mutex::new(connection),
        }
    }

    /// `node` returns every decoded column without applying facade defaults.
    #[test]
    fn node_preserves_every_database_column() {
        let store = store_with_inode(
            "7, 1, 'entry', 'file', 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/12',
             NULL, 'overlay-7', 292, -1, 42, 0, 1, 0",
        );
        let decoded = store.node(InodeId::new(7).unwrap()).unwrap();
        assert_eq!(decoded.inode, Some(7));
        assert_eq!(decoded.parent_inode, Some(1));
        assert_eq!(decoded.name, "entry");
        assert_eq!(decoded.kind, StoreNodeKind::File);
        assert_eq!(
            decoded.remote_digest.as_deref(),
            Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/12")
        );
        assert_eq!(decoded.symlink_target, None);
        assert_eq!(decoded.overlay_file.as_deref(), Some("overlay-7"));
        assert_eq!(decoded.mode, Some(0o444));
        assert_eq!(decoded.mtime_seconds, Some(-1));
        assert_eq!(decoded.mtime_nanos, Some(42));
        assert!(!decoded.tombstone);
        assert!(decoded.content_dirty);
        assert!(!decoded.tree_dirty);
    }

    /// `node` distinguishes absent metadata from an explicit default value.
    #[test]
    fn node_preserves_null_and_explicit_default_metadata() {
        let absent = store_with_inode(
            "8, 1, 'absent', 'directory', NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 1",
        );
        let decoded = absent.node(InodeId::new(8).unwrap()).unwrap();
        assert_eq!(decoded.mode, None);
        assert_eq!(decoded.mtime_seconds, None);
        assert_eq!(decoded.mtime_nanos, None);
        assert!(decoded.tree_dirty);

        let explicit =
            store_with_inode("9, 1, 'explicit', 'directory', NULL, NULL, NULL, 365, 0, 0, 0, 0, 0");
        let decoded = explicit.node(InodeId::new(9).unwrap()).unwrap();
        assert_eq!(decoded.mode, Some(0o555));
        assert_eq!(decoded.mtime_seconds, Some(0));
        assert_eq!(decoded.mtime_nanos, Some(0));
    }

    /// `node` rejects malformed store encodings with database and inode context.
    #[test]
    fn node_rejects_store_encodings_with_inode_context() {
        for (row_values, expected) in [
            (
                "7, 1, 'entry', 'device', NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0",
                "inode 7 has unsupported kind",
            ),
            (
                "7, 1, 'entry', 'file', NULL, NULL, NULL, NULL, NULL, NULL, 2, 0, 0",
                "inode 7 has invalid tombstone",
            ),
            (
                "7, 1, 'entry', 'file', NULL, NULL, NULL, NULL, NULL, NULL, 0, -1, 0",
                "inode 7 has invalid content_dirty",
            ),
            (
                "7, 1, 'entry', 'file', NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 3",
                "inode 7 has invalid tree_dirty",
            ),
            (
                "7, 1, 'entry', 'file', NULL, NULL, NULL, NULL, 0, NULL, 0, 0, 0",
                "inode 7 has a partial modification time",
            ),
        ] {
            let store = store_with_inode(row_values);
            let message = store
                .node(InodeId::new(7).unwrap())
                .unwrap_err()
                .to_string();
            assert!(message.contains("/state/session.db"), "{message}");
            assert!(message.contains(expected), "{message}");
        }
    }
}
