//! SQLite repository for one session's lifecycle and lossless inode namespace.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use uuid::Uuid;

use crate::digest::Digest;
use crate::error_context::ResultContext;

use super::{
    InodeId, NodeKind, NodeTime, SessionError, SessionLifecycle, failed_precondition,
    internal_error, not_directory,
};

const SCHEMA_VERSION: i64 = 1;
const SCHEMA_SQL: &str = include_str!("schema.sql");

/// Repository for the durable lifecycle and merged namespace of one session.
pub(super) struct SessionStore {
    /// Path to the SQLite database backing this session store, used for debug logging.
    database_path: PathBuf,
    /// Connection to the SQLite database backing this session store.
    connection: Mutex<Connection>,
}

/// Validated retained session metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SessionMetadata {
    /// Positive daemon process ID.
    pub(super) daemon_pid: u32,
    /// Durable lifecycle state.
    pub(super) state: SessionLifecycle,
    /// Immutable mounted root directory digest.
    pub(super) root_digest: Digest,
    /// Canonical absolute mountpoint.
    pub(super) mountpoint: PathBuf,
}

/// Lossless inode values read from or supplied to the session store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Inode {
    /// ID for this inode.
    pub(super) id: InodeId,
    /// ID of this inode's parent inode; absent for the root inode.
    pub(super) parent: Option<InodeId>,
    /// UTF-8 basename, empty only for the root.
    pub(super) name: String,
    /// Filesystem node kind.
    pub(super) kind: NodeKind,
    /// Preserved mode, when explicitly supplied.
    pub(super) mode: Option<u32>,
    /// Preserved modification time, when explicitly supplied.
    pub(super) mtime: Option<NodeTime>,
    /// Whether this inode was deleted.
    pub(super) tombstone: bool,
    /// Digest backing this file inode; provided only for files in the root directory tree.
    pub(super) file_remote_digest: Option<Digest>,
    /// Overlay file backing this file's contents, for modified or newly created root-tree files.
    pub(super) file_overlay_path: Option<PathBuf>,
    /// Whether this file changed since the daemon last snapshotted and uploaded it to CAS.
    pub(super) file_content_dirty: Option<bool>,
    /// Exact symbolic-link target.
    pub(super) symlink_target: Option<String>,
    /// Immutable serialized directory digest.
    pub(super) directory_remote_digest: Option<Digest>,
    /// Whether child inodes were created after loading this directory's backing CAS blob.
    pub(super) directory_loaded: Option<bool>,
}

impl SessionStore {
    /// Creates a SQLite database for a session mounting `root_digest` at `mountpoint`.
    ///
    /// Seeds the database with session metadata and an unloaded root inode.
    pub(super) fn create(
        database_path: PathBuf,
        session_id: String,
        daemon_pid: u32,
        root_digest: Digest,
        mountpoint: PathBuf,
    ) -> Result<Self, SessionError> {
        let connection = open_database(&database_path, false)?;
        prepare_schema(&connection, &database_path)?;
        let store = Self {
            database_path,
            connection: Mutex::new(connection),
        };
        store.initialize_database(&session_id, daemon_pid, &root_digest, &mountpoint)?;
        Ok(store)
    }

    /// Reads retained metadata through a separate read-only connection.
    pub(super) fn inspect(path: &Path) -> Result<SessionMetadata, SessionError> {
        let connection = open_database(path, true)?;
        validate_schema_version(&connection, path)?;
        read_stored_session(&connection, path)
    }

    /// Fetches one inode row without applying visibility policy.
    pub(super) fn inode(&self, inode: InodeId) -> Result<Option<Inode>, SessionError> {
        let connection = self.connection("read inode")?;
        self.get_inode_by_id(&connection, inode)
    }

    /// Fetches one named child row without applying visibility policy.
    pub(super) fn child(&self, parent: InodeId, name: &str) -> Result<Option<Inode>, SessionError> {
        validate_inode_name(name).with_context(|| {
            format!(
                "invalid name {} given to lookup in parent inode {}",
                name, parent
            )
        })?;
        let connection = self.connection("create sql db connection to read child")?;
        self.lookup_child_in_dir_inode(&connection, parent, name)
    }

    /// Fetches every direct child row in basename order, including tombstones.
    pub(super) fn get_directory_children(
        &self,
        parent: InodeId,
    ) -> Result<Vec<Inode>, SessionError> {
        let connection = self.connection("list children")?;
        self.get_directory_children_on(&connection, parent)
    }

    /// Returns an already-loaded child set or atomically materializes an unloaded one.
    ///
    /// Inputs: `parent`, the stored directory inode, and `children`, its complete
    /// decoded remote child set. Returns stored children in basename order with
    /// allocated inode IDs. Errors: missing or malformed parents, non-directory
    /// parents, inconsistent unloaded state, invalid child sets, and SQLite
    /// failures. A successful materialization inserts all children and marks the
    /// parent loaded in one transaction.
    pub(super) fn get_or_create_remote_dir_children(
        &self,
        parent: InodeId,
        children: &[Inode],
    ) -> Result<Vec<Inode>, SessionError> {
        self.with_transaction("get or create directory children", |transaction| {
            let parent_inode = self
                .get_inode_by_id(transaction, parent)
                .with_context(|| format!("read parent inode {parent} to create its children"))?
                .ok_or_else(|| {
                    internal_error(format!(
                        "can't create children because parent inode {parent} is missing"
                    ))
                })?;
            ensure_inode_is_dir(&parent_inode).with_context(|| {
                format!("can't create children for non-directory inode {parent}")
            })?;
            let stored_children = self
                .get_directory_children_on(transaction, parent)
                .with_context(|| format!("read stored children of directory inode {parent}"))?;
            if parent_inode.directory_loaded == Some(true) {
                return Ok(stored_children);
            }
            if !stored_children.is_empty() {
                return Err(internal_error(format!(
                    "unloaded directory inode {parent} already has stored children"
                )));
            }
            let children = validated_remote_dir_child_inodes_for_creation(parent, children)
                .with_context(|| {
                    format!("validate children to create in directory inode {parent}")
                })?;
            for child in children {
                self.insert_inode(transaction, "insert directory child", &child)
                    .with_context(|| {
                        format!("insert child `{}` in directory inode {parent}", child.name)
                    })?;
            }
            transaction
                .execute(
                    "UPDATE inodes SET directory_loaded = 1 WHERE id = ?1",
                    [parent.sqlite()],
                )
                .map_err(|source| self.db_error("mark directory loaded", source))?;
            self.get_directory_children_on(transaction, parent)
                .with_context(|| {
                    format!("read children of directory inode {parent} after creation")
                })
        })
    }

    /// Transitions active metadata to closed exactly once.
    ///
    /// Errors: `FailedPreconditionError` when the stored session is not active, so a
    /// second close attempt reports the mismatch instead of rewriting the close time.
    pub(super) fn close(&self) -> Result<(), SessionError> {
        let seconds = now_seconds()
            .context("getting current time to set session close time in session state db")?;
        self.with_transaction("close session", |transaction| {
            self.ensure_active(transaction)
                .context("can't close already closed session")?;
            transaction.execute("UPDATE session_metadata SET lifecycle = 'closed', closed_at_seconds = ?1 WHERE singleton = 1", [seconds])
                .map_err(|source| self.db_error("mark session closed", source))?;
            Ok(())
        })
    }

    fn connection(
        &self,
        operation: &'static str,
    ) -> Result<MutexGuard<'_, Connection>, SessionError> {
        self.connection
            .lock()
            .map_err(|_| internal_error(format!("{operation}: connection lock is poisoned")))
    }

    /// Runs `body` inside a newly opened transaction and commits its result.
    ///
    /// The operation labels lock, begin, and commit failures. The body receives
    /// the transaction and may return any result value. A body or commit error is
    /// returned as a session error; a body error rolls the transaction back when
    /// it is dropped.
    fn with_transaction<T>(
        &self,
        operation: &'static str,
        body: impl FnOnce(&Transaction<'_>) -> Result<T, SessionError>,
    ) -> Result<T, SessionError> {
        let mut connection = self.connection(operation)?;
        let transaction = connection
            .transaction()
            .map_err(|source| self.db_error(format!("begin {operation}"), source))?;
        let result = body(&transaction).with_context(|| format!("run {operation} transaction"))?;
        transaction
            .commit()
            .map_err(|source| self.db_error(format!("commit {operation}"), source))?;
        Ok(result)
    }

    /// Seeds active session metadata and the unloaded root inode atomically.
    fn initialize_database(
        &self,
        session_id: &str,
        daemon_pid: u32,
        root_digest: &Digest,
        mountpoint: &Path,
    ) -> Result<(), SessionError> {
        let mountpoint = mountpoint
            .to_str()
            .ok_or_else(|| internal_error("mountpoint is not UTF-8"))?;
        if Uuid::parse_str(session_id).is_err() || daemon_pid == 0 {
            return Err(internal_error("invalid session identity"));
        }
        let now = now_seconds().context("determine current time for session creation")?;
        self.with_transaction("initialize database", |transaction| {
            self.insert_session(
                transaction,
                session_id,
                daemon_pid,
                root_digest,
                mountpoint,
                now,
            )?;
            self.insert_root_inode(transaction, root_digest)
                .with_context(|| "inserting root inode".to_owned())
        })
    }

    /// Inserts the initial active session metadata row.
    fn insert_session(
        &self,
        transaction: &Transaction<'_>,
        session_id: &str,
        daemon_pid: u32,
        root_digest: &Digest,
        mountpoint: &str,
        created_at_seconds: i64,
    ) -> Result<(), SessionError> {
        transaction
            .execute(
                "INSERT INTO session_metadata (
                    singleton,
                    session_id,
                    daemon_pid,
                    lifecycle,
                    root_digest_hash,
                    root_digest_size,
                    mountpoint,
                    created_at_seconds,
                    closed_at_seconds,
                    log_level,
                    log_format
                ) VALUES (
                    1,
                    ?1,
                    ?2,
                    'active',
                    ?3,
                    ?4,
                    ?5,
                    ?6,
                    NULL,
                    'info',
                    'text'
                )",
                params![
                    session_id,
                    i64::from(daemon_pid),
                    root_digest.hash(),
                    root_digest.size_bytes(),
                    mountpoint,
                    created_at_seconds,
                ],
            )
            .map_err(|source| self.db_error("insert session metadata", source))?;
        Ok(())
    }

    /// Constructs and inserts the unloaded remote root inode for a new session.
    fn insert_root_inode(
        &self,
        transaction: &Transaction<'_>,
        root_digest: &Digest,
    ) -> Result<(), SessionError> {
        let root = Inode {
            id: InodeId::ROOT,
            parent: None,
            name: String::new(),
            kind: NodeKind::Directory,
            mode: None,
            mtime: None,
            tombstone: false,
            file_remote_digest: None,
            file_overlay_path: None,
            file_content_dirty: None,
            symlink_target: None,
            directory_remote_digest: Some(root_digest.clone()),
            directory_loaded: Some(false),
        };
        self.insert_inode(transaction, "insert root inode", &root)
    }

    /// Fetches one inode row by identity without applying visibility policy.
    ///
    /// Inputs: `connection` borrowed from the caller's transaction or lock
    /// scope. Returns the row when present, `None` otherwise. Errors: database
    /// failures, or decode failures for malformed stored rows.
    fn get_inode_by_id(
        &self,
        connection: &Connection,
        inode_id: InodeId,
    ) -> Result<Option<Inode>, SessionError> {
        connection
            .query_row(
                &format!(
                    "SELECT {} FROM inodes WHERE id = ?1",
                    InodeRow::column_list()
                ),
                params![inode_id.sqlite()],
                InodeRow::from_rusqlite_row,
            )
            .optional()
            .map_err(|source| self.db_error("get inode by id", source))?
            .map(validate_inode_row)
            .transpose()
            .with_context(|| format!("validate stored inode {inode_id}"))
    }

    /// Fetches one named direct child row without applying visibility policy.
    ///
    /// Inputs: `connection` borrowed from the caller's transaction or lock
    /// scope, and the parent directory identity with the child basename.
    /// Returns the row when present, `None` otherwise. Errors: database
    /// failures, or decode failures for malformed stored rows.
    fn lookup_child_in_dir_inode(
        &self,
        connection: &Connection,
        parent_id: InodeId,
        name: &str,
    ) -> Result<Option<Inode>, SessionError> {
        connection
            .query_row(
                &format!(
                    "SELECT {} FROM inodes WHERE parent_id = ?1 AND name = ?2",
                    InodeRow::column_list()
                ),
                params![parent_id.sqlite(), name],
                InodeRow::from_rusqlite_row,
            )
            .optional()
            .map_err(|source| self.db_error("lookup child by name in dir inode", source))?
            .map(validate_inode_row)
            .transpose()
            .with_context(|| {
                format!("validate stored child `{name}` of directory inode {parent_id}")
            })
    }

    /// Reads and validates every direct child in basename order on a borrowed connection.
    fn get_directory_children_on(
        &self,
        connection: &Connection,
        parent: InodeId,
    ) -> Result<Vec<Inode>, SessionError> {
        let mut statement = connection
            .prepare(&format!(
                "SELECT {} FROM inodes WHERE parent_id = ?1 ORDER BY name",
                InodeRow::column_list()
            ))
            .map_err(|source| self.db_error("prepare children", source))?;
        let rows = statement
            .query_map([parent.sqlite()], InodeRow::from_rusqlite_row)
            .map_err(|source| self.db_error("list children", source))?;
        let rows = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| self.db_error("decode child", source))?;
        rows.into_iter()
            .map(validate_inode_row)
            .collect::<Result<Vec<_>, _>>()
            .with_context(|| format!("validate stored children of directory inode {parent}"))
    }

    /// Validates and inserts one root or unallocated child inode in a transaction.
    fn insert_inode(
        &self,
        transaction: &Transaction<'_>,
        operation: &'static str,
        inode: &Inode,
    ) -> Result<(), SessionError> {
        validate_inode_for_create(inode)
            .with_context(|| "inode failed validation for insertion".to_owned())?;
        let overlay_path = inode
            .file_overlay_path
            .as_ref()
            .map(|path| {
                path.to_str()
                    .ok_or_else(|| internal_error("overlay path is not UTF-8"))
            })
            .transpose()?;
        let query = format!(
            "INSERT INTO inodes ({}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            InodeRow::column_list()
        );
        let id = (inode.id != InodeId::INVALID).then(|| inode.id.sqlite());
        transaction
            .execute(
                &query,
                params![
                    id,
                    inode.parent.map(InodeId::sqlite),
                    inode.name,
                    kind_text(inode.kind),
                    inode.mode.map(i64::from),
                    inode.mtime.map(NodeTime::seconds),
                    inode.mtime.map(|time| i64::from(time.nanos())),
                    i64::from(inode.tombstone),
                    inode.file_remote_digest.as_ref().map(ToString::to_string),
                    overlay_path,
                    inode.file_content_dirty.map(i64::from),
                    inode.symlink_target,
                    inode
                        .directory_remote_digest
                        .as_ref()
                        .map(ToString::to_string),
                    inode.directory_loaded.map(i64::from),
                ],
            )
            .map_err(|source| self.db_error(operation, source))?;
        Ok(())
    }

    /// Maps a SQLite failure from this store's owned connection to its database path.
    fn db_error(&self, operation: impl Into<String>, source: rusqlite::Error) -> SessionError {
        database_error(operation, &self.database_path, source)
    }

    /// Requires the stored session metadata to report the active lifecycle.
    ///
    /// Inputs: `connection` used to read session metadata within the caller's
    /// transaction scope. Returns `Ok(())` only when the session is active.
    /// Errors: `FailedPreconditionError` naming the actual lifecycle when it
    /// differs, or database failures from reading the metadata row.
    fn ensure_active(&self, connection: &Connection) -> Result<(), SessionError> {
        let state = read_stored_session(connection, &self.database_path)
            .context("read session lifecycle state")?
            .state;
        if state != SessionLifecycle::Active {
            return Err(failed_precondition(format!(
                "session lifecycle is {state}, expected active"
            )));
        }
        Ok(())
    }
}

/// Unvalidated inode values decoded directly from one SQLite row.
///
/// The query projection must contain every field named by this decoder.
struct InodeRow {
    /// Stored inode identity before positive-value validation.
    id: i64,
    /// Stored parent identity before positive-value validation.
    parent_id: Option<i64>,
    /// Stored UTF-8 basename.
    name: String,
    /// Stored node-kind label.
    kind: String,
    /// Stored Unix mode before range validation.
    mode: Option<i64>,
    /// Whole seconds of the optional modification time.
    mtime_seconds: Option<i64>,
    /// Nanosecond fraction of the optional modification time.
    mtime_nanos: Option<i64>,
    /// Stored tombstone boolean before value validation.
    tombstone: i64,
    /// Stored remote file digest text.
    file_remote_digest: Option<String>,
    /// Stored relative overlay path text.
    file_overlay_path: Option<String>,
    /// Stored dirty-content boolean before value validation.
    file_content_dirty: Option<i64>,
    /// Stored symbolic-link target.
    symlink_target: Option<String>,
    /// Stored remote directory digest text.
    directory_remote_digest: Option<String>,
    /// Stored directory-loaded boolean before value validation.
    directory_loaded: Option<i64>,
}

impl InodeRow {
    /// Returns the ordered inode columns shared by read and insert statements.
    fn column_list() -> &'static str {
        "id, parent_id, name, kind, mode, mtime_seconds, mtime_nanos,
        tombstone, file_remote_digest, file_overlay_path, file_content_dirty,
        symlink_target, directory_remote_digest, directory_loaded"
    }

    /// Decodes one inode projection without applying domain validation.
    ///
    /// Inputs: `row` whose projection contains the named inode columns. Returns
    /// the SQLite-shaped values. Errors: SQLite type or column-read failures.
    fn from_rusqlite_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get("id")?,
            parent_id: row.get("parent_id")?,
            name: row.get("name")?,
            kind: row.get("kind")?,
            mode: row.get("mode")?,
            mtime_seconds: row.get("mtime_seconds")?,
            mtime_nanos: row.get("mtime_nanos")?,
            tombstone: row.get("tombstone")?,
            file_remote_digest: row.get("file_remote_digest")?,
            file_overlay_path: row.get("file_overlay_path")?,
            file_content_dirty: row.get("file_content_dirty")?,
            symlink_target: row.get("symlink_target")?,
            directory_remote_digest: row.get("directory_remote_digest")?,
            directory_loaded: row.get("directory_loaded")?,
        })
    }
}

/// Validates proposed remote children before they are materialized in SQLite
/// and transforms it for insertion into the db.
///
/// Inputs: `children`, the complete remote child set for one unloaded
/// directory. Returns the children inodes with their parent set to the given
/// parent inode.
/// state. Errors: the first invalid child, identified by name. Side effects:
/// none.
fn validated_remote_dir_child_inodes_for_creation(
    parent: InodeId,
    children: &[Inode],
) -> Result<Vec<Inode>, SessionError> {
    let mut names = HashSet::with_capacity(children.len());
    let mut result: Vec<Inode> = Vec::with_capacity(children.len());
    for child in children {
        if child.id != InodeId::INVALID {
            return Err(internal_error(format!(
                "new child inode `{}` supplies an inode ID {} instead of invalid / default Inode ID {}",
                child.name,
                child.id,
                InodeId::INVALID
            )));
        }
        if child.parent.is_some() {
            return Err(internal_error(format!(
                "new child  inode `{}` supplies a parent",
                child.name
            )));
        }
        // We're loading the 'parent' directory so any child directory inode
        // can't also be loaded right now.
        if child.directory_loaded.is_some_and(|loaded| loaded) {
            return Err(internal_error(format!(
                "new child directory inode `{}` is set to loaded",
                child.name
            )));
        }
        validate_inode_name(&child.name)?;
        if !names.insert(child.name.as_str()) {
            return Err(internal_error(format!(
                "duplicate child name `{}`",
                child.name
            )));
        }
        let mut child = child.clone();
        child.parent = Some(parent);
        validate_inode_for_create(&child)?;
        result.push(child);
    }
    Ok(result)
}

/// Converts one SQLite-shaped inode row into a validated domain inode.
///
/// Inputs: `row` decoded from [`InodeRow::column_list`]. Returns a lossless
/// [`Inode`] after validating scalar conversions, stored identity, and
/// node-kind field combinations. Errors: invalid persisted values.
fn validate_inode_row(row: InodeRow) -> Result<Inode, SessionError> {
    let inode = InodeId::from_sqlite(row.id)
        .with_context(|| format!("decode stored inode identity {}", row.id))?;
    let parent = row
        .parent_id
        .map(InodeId::from_sqlite)
        .transpose()
        .with_context(|| format!("decode parent identity of stored inode {inode}"))?;
    let kind = match row.kind.as_str() {
        "file" => NodeKind::File,
        "directory" => NodeKind::Directory,
        "symlink" => NodeKind::Symlink,
        value => {
            return Err(internal_error(format!(
                "stored inode {inode} has invalid kind `{value}`"
            )));
        }
    };
    let mode = row
        .mode
        .map(u32::try_from)
        .transpose()
        .map_err(|_| internal_error(format!("stored inode {inode} has invalid mode")))?;
    let mtime = validate_inode_mtime(inode, row.mtime_seconds, row.mtime_nanos)
        .with_context(|| format!("validate modification time of stored inode {inode}"))?;
    let result = Inode {
        id: inode,
        parent,
        name: row.name,
        kind,
        mode,
        mtime,
        tombstone: validate_stored_bool(inode, "tombstone", row.tombstone)
            .with_context(|| format!("validate tombstone of stored inode {inode}"))?,
        file_remote_digest: validate_stored_digest(
            inode,
            "file remote digest",
            row.file_remote_digest,
        )
        .with_context(|| format!("validate remote file digest of stored inode {inode}"))?,
        file_overlay_path: row.file_overlay_path.map(PathBuf::from),
        file_content_dirty: row
            .file_content_dirty
            .map(|value| validate_stored_bool(inode, "file content dirty", value))
            .transpose()
            .with_context(|| format!("validate dirty state of stored inode {inode}"))?,
        symlink_target: row.symlink_target,
        directory_remote_digest: validate_stored_digest(
            inode,
            "directory remote digest",
            row.directory_remote_digest,
        )
        .with_context(|| format!("validate remote directory digest of stored inode {inode}"))?,
        directory_loaded: row
            .directory_loaded
            .map(|value| validate_stored_bool(inode, "directory loaded", value))
            .transpose()
            .with_context(|| format!("validate loaded state of stored inode {inode}"))?,
    };
    validate_inode(&result).with_context(|| format!("validate stored inode {inode}"))?;
    Ok(result)
}

/// Validates the paired persisted modification-time columns of one inode.
///
/// Returns `None` when both columns are null and a normalized [`NodeTime`]
/// when both are valid. Errors: a partial or out-of-range stored time.
fn validate_inode_mtime(
    inode: InodeId,
    seconds: Option<i64>,
    nanos: Option<i64>,
) -> Result<Option<NodeTime>, SessionError> {
    match (seconds, nanos) {
        (None, None) => Ok(None),
        (Some(seconds), Some(nanos)) => {
            let nanos = u32::try_from(nanos)
                .map_err(|_| internal_error(format!("stored inode {inode} has invalid mtime")))?;
            NodeTime::new(seconds, nanos)
                .map(Some)
                .ok_or_else(|| internal_error(format!("stored inode {inode} has invalid mtime")))
        }
        _ => Err(internal_error(format!(
            "stored inode {inode} has partial mtime"
        ))),
    }
}

/// Validates one integer-backed SQLite boolean.
///
/// Returns the corresponding boolean. Errors: any stored integer other than
/// zero or one.
fn validate_stored_bool(
    inode: InodeId,
    field: &'static str,
    value: i64,
) -> Result<bool, SessionError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(internal_error(format!(
            "stored inode {inode} has {field} with invalid boolean, got {}, want 0 or 1",
            value
        ))),
    }
}

/// Parses one nullable persisted digest belonging to an inode.
///
/// Returns the validated digest while preserving null. Errors: malformed
/// digest text, including unsupported hash or size representations.
fn validate_stored_digest(
    inode: InodeId,
    field: &'static str,
    value: Option<String>,
) -> Result<Option<Digest>, SessionError> {
    value
        .map(|value| {
            value.parse().map_err(|error| {
                internal_error(format!("stored inode {inode} has invalid {field}: {error}"))
            })
        })
        .transpose()
}

/// Validates an inode.
///
/// Inputs: `inode` with an allocated identity. Returns `Ok(())` when its
/// identity, mode, and kind-specific fields are valid. Errors: invalid stored
/// identity or invalid fields for the inode kind.
fn validate_inode(inode: &Inode) -> Result<(), SessionError> {
    if inode.id == InodeId::INVALID {
        return Err(internal_error(format!("inode had invalid id {}", inode.id)));
    }
    validate_inode_common(inode)
}

/// Validates a new inode that's being created and doesn't have an allocated
/// id yet.
///
/// Inputs: `inode` created for insertion. Returns `Ok(())` when its mode and
/// kind-specific fields are valid. Errors: an unsupported mode or fields that
/// violate the inode kind's nullability and exclusivity rules.
fn validate_inode_for_create(inode: &Inode) -> Result<(), SessionError> {
    if inode.id != InodeId::ROOT && inode.id != InodeId::INVALID {
        return Err(internal_error(format!(
            "insert inode specified id {}, must be root id {} or invalid id {}",
            inode.id,
            InodeId::ROOT,
            InodeId::INVALID
        )));
    }
    if inode.tombstone {
        return Err(internal_error(format!(
            "new inode `{}` can't be set to tombstoned",
            inode.name
        )));
    }
    validate_inode_common(inode)
}

/// Validates one inode's kind-specific fields.
///
/// Inputs: `inode` and whether it is about to be materialized from a remote
/// directory. Returns `Ok(())` when the kind's state is valid. Errors: field
/// combinations incompatible with the inode kind. Side effects: none.
fn validate_inode_kind(inode: &Inode) -> Result<(), SessionError> {
    match inode.kind {
        NodeKind::File => validate_file_inode(inode),
        NodeKind::Symlink => validate_symlink_inode(inode),
        NodeKind::Directory => validate_directory_inode(inode),
    }
}

/// Validates a file inode's fields and, when materializing one, its remote state.
fn validate_file_inode(inode: &Inode) -> Result<(), SessionError> {
    if inode.symlink_target.is_some()
        || inode.directory_remote_digest.is_some()
        || inode.directory_loaded.is_some()
        || inode.file_content_dirty.is_none()
    {
        return Err(internal_error(format!(
            "file inode `{}` has invalid fields",
            inode.name
        )));
    }
    if let Some(overlay_path) = &inode.file_overlay_path
        && (overlay_path.as_os_str().is_empty()
            || overlay_path.is_absolute()
            || overlay_path
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir)))
    {
        return Err(internal_error(format!(
            "file inode `{}` has an invalid overlay path",
            inode.name
        )));
    }
    if inode.file_overlay_path.is_none() && inode.file_content_dirty.is_some_and(|v| v) {
        return Err(internal_error(format!(
            "file inode `{}` is dirty but has no overlay path",
            inode.name
        )));
    }
    if inode.file_remote_digest.is_none() && inode.file_overlay_path.is_none() {
        return Err(internal_error(format!(
            "file inode `{}` neither has a remote digest nor an overlay path for the file contents",
            inode.name
        )));
    }
    match (&inode.file_remote_digest, &inode.file_overlay_path) {
        (Some(digest), Some(overlay_path)) => {
            return Err(internal_error(format!(
                "file inode `{}` specified both remote digest {} and an overlay path {}, only one of these is permitted at a given time",
                inode.name,
                digest,
                overlay_path.display()
            )));
        }
        (_, _) => {}
    };
    Ok(())
}

/// Validates that a new symbolic-link inode has only clean remote link state.
fn validate_symlink_inode(inode: &Inode) -> Result<(), SessionError> {
    if inode.symlink_target.is_none()
        || inode.file_remote_digest.is_some()
        || inode.file_overlay_path.is_some()
        || inode.file_content_dirty.is_some()
        || inode.directory_remote_digest.is_some()
        || inode.directory_loaded.is_some()
    {
        return Err(internal_error(format!(
            "symlink inode `{}` has invalid fields",
            inode.name
        )));
    }
    Ok(())
}

/// Validates that a directory inode has remote backing and a valid load state.
///
/// A directory materialized from a remote child set must be unloaded. A
/// persisted directory may already be loaded after its child set is stored.
fn validate_directory_inode(inode: &Inode) -> Result<(), SessionError> {
    if inode.file_remote_digest.is_some()
        || inode.file_overlay_path.is_some()
        || inode.file_content_dirty.is_some()
        || inode.symlink_target.is_some()
    {
        return Err(internal_error(format!(
            "directory inode `{}` has invalid fields",
            inode.name
        )));
    }
    if inode.directory_loaded.is_none() {
        return Err(internal_error(format!(
            "directory inode `{}` did not specify a boolean for loaded",
            inode.name
        )));
    }
    Ok(())
}

/// Validate an inode with checks that's common between:
/// - Existing stored inode.
/// - A new inode being created.
/// - Remote CAS blob backed inode.
/// - Overlay file backed inode.
fn validate_inode_common(inode: &Inode) -> Result<(), SessionError> {
    if let Some(mode) = inode.mode
        && mode > 0o7777
    {
        return Err(internal_error(format!(
            "inode `{}` has invalid mode {}",
            inode.name, mode
        )));
    }
    validate_inode_hierarchy(inode)?;
    validate_inode_kind(inode)
}

/// Validates the given inode is either a valid root or a descendent of the root.
fn validate_inode_hierarchy(inode: &Inode) -> Result<(), SessionError> {
    match (inode.id, inode.parent) {
        (InodeId::ROOT, None) => {
            if !inode.name.is_empty() {
                return Err(internal_error(format!(
                    "root inode unexpected had name {}, expected empty name",
                    inode.name
                )));
            }
            if inode.kind != NodeKind::Directory {
                return Err(internal_error(format!(
                    "root inode kind was {}, expected directory",
                    inode.kind
                )));
            }
            if inode.directory_remote_digest.is_none() {
                return Err(internal_error("root inode remote digest is missing"));
            }
            if inode.directory_loaded.is_none() {
                return Err(internal_error("root inode was not loaded"));
            }
            Ok(())
        }
        (InodeId::ROOT, Some(parent)) => Err(internal_error(format!(
            "root inode unexpectedly had a parent inode with id {}",
            parent
        ))),
        (_, Some(_)) => validate_inode_name(&inode.name)
            .with_context(|| format!("inode {} name validation failed", inode.id)),
        (_, None) => Err(internal_error(format!(
            "inode {} is not root but has no parent",
            inode.id
        ))),
    }
}

/// Requires an inode to be a directory.
///
/// Inputs: `inode`, any stored inode row. Returns `Ok(())` when its kind is
/// [`NodeKind::Directory`] and NotDirectory error otherwise.
fn ensure_inode_is_dir(inode: &Inode) -> Result<(), SessionError> {
    if inode.kind != NodeKind::Directory {
        return Err(not_directory(format!(
            "inode {} is not a directory; actual kind is {:?}",
            inode.id, inode.kind
        )));
    }
    Ok(())
}

fn validate_inode_name(name: &str) -> Result<(), SessionError> {
    if name.is_empty() || name.contains('/') || matches!(name, "." | "..") {
        Err(internal_error(format!("invalid inode name `{name}`")))
    } else {
        Ok(())
    }
}

/// Unvalidated session metadata decoded from the single `session_metadata` row.
///
/// The query projection must contain every field named by this decoder.
struct SessionMetadataRow {
    /// Singleton row marker seeded by `initialize_database`; must equal 1.
    singleton: i64,
    /// UUID text identifying the session.
    session_id: String,
    /// Stored daemon process ID before range validation.
    daemon_pid: i64,
    /// Lifecycle state text before conversion to [`SessionLifecycle`].
    lifecycle: String,
    /// Root directory digest hash text.
    root_digest_hash: String,
    /// Root directory digest size in bytes.
    root_digest_size: i64,
    /// Mountpoint text before path conversion and absoluteness validation.
    mountpoint: String,
    /// Whole seconds since the Unix epoch when the session was created.
    created_at_seconds: i64,
    /// Whole seconds since the Unix epoch when the session was closed, if closed.
    closed_at_seconds: Option<i64>,
    /// Retained log level label.
    log_level: String,
    /// Retained log format label.
    log_format: String,
}

impl SessionMetadataRow {
    /// Returns the ordered columns used to read retained session metadata.
    fn column_list() -> &'static str {
        "singleton,
        session_id,
        daemon_pid,
        lifecycle,
        root_digest_hash,
        root_digest_size,
        mountpoint,
        created_at_seconds,
        closed_at_seconds,
        log_level,
        log_format"
    }

    /// Decodes one session-metadata projection without applying domain validation.
    ///
    /// Inputs: `row` whose projection contains the named session-metadata
    /// columns. Returns the SQLite-shaped values. Errors: SQLite type or
    /// column-read failures.
    fn from_rusqlite_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            singleton: row.get("singleton")?,
            session_id: row.get("session_id")?,
            daemon_pid: row.get("daemon_pid")?,
            lifecycle: row.get("lifecycle")?,
            root_digest_hash: row.get("root_digest_hash")?,
            root_digest_size: row.get("root_digest_size")?,
            mountpoint: row.get("mountpoint")?,
            created_at_seconds: row.get("created_at_seconds")?,
            closed_at_seconds: row.get("closed_at_seconds")?,
            log_level: row.get("log_level")?,
            log_format: row.get("log_format")?,
        })
    }
}

/// Reads and validates retained metadata from an open database connection.
///
/// Inputs: `connection` borrowed from the caller's read-only connection or
/// transaction scope, and the database path used in error identifiers.
/// Returns the validated [`SessionMetadata`]. Errors: database failures, a
/// missing metadata row, or precondition failures from invalid stored values.
fn read_stored_session(
    connection: &Connection,
    path: &Path,
) -> Result<SessionMetadata, SessionError> {
    let row = connection
        .query_row(
            &format!(
                "SELECT {} FROM session_metadata WHERE singleton = 1",
                SessionMetadataRow::column_list()
            ),
            [],
            SessionMetadataRow::from_rusqlite_row,
        )
        .optional()
        .map_err(|source| database_error("read session metadata", path, source))?
        .ok_or_else(|| {
            failed_precondition(
                "db session is missing session metadata; either it wasn't seeded during session \
                 initialization or deleted accidentally",
            )
        })?;
    validate_session(row)
}

/// Validates one decoded metadata row and converts it to domain metadata.
///
/// Inputs: `row` freshly decoded by [`SessionMetadataRow::from_rusqlite_row`].
/// Returns the [`SessionMetadata`] when every field passes its checks. Errors:
/// `FailedPreconditionError` naming the first violated invariant (identity,
/// lifecycle, close-time pairing, or mountpoint shape).
fn validate_session(row: SessionMetadataRow) -> Result<SessionMetadata, SessionError> {
    let SessionMetadataRow {
        singleton,
        session_id,
        daemon_pid: pid,
        lifecycle,
        root_digest_hash: hash,
        root_digest_size: size,
        mountpoint,
        created_at_seconds: created_seconds,
        closed_at_seconds: closed_seconds,
        log_level: level,
        log_format: format,
    } = row;
    if singleton != 1
        || Uuid::parse_str(&session_id).is_err()
        || pid <= 0
        || created_seconds < 0
        || level.is_empty()
        || !matches!(format.as_str(), "text" | "json")
    {
        return Err(failed_precondition(
            "inspect session metadata: invalid metadata",
        ));
    }
    let state = match lifecycle.as_str() {
        "initializing" => SessionLifecycle::Initializing,
        "active" => SessionLifecycle::Active,
        "closed" => SessionLifecycle::Closed,
        _ => {
            return Err(failed_precondition(
                "inspect session metadata: invalid lifecycle",
            ));
        }
    };
    match (state, closed_seconds) {
        (SessionLifecycle::Closed, Some(seconds)) if seconds >= 0 => {}
        (SessionLifecycle::Closed, _) => {
            return Err(failed_precondition(
                "inspect session metadata: closed session has invalid close time",
            ));
        }
        (_, None) => {}
        _ => {
            return Err(failed_precondition(
                "inspect session metadata: non-closed session has close time",
            ));
        }
    };
    let mountpoint = PathBuf::from(mountpoint);
    if !mountpoint.is_absolute() {
        return Err(failed_precondition(
            "inspect session metadata: mountpoint is not absolute",
        ));
    }
    Ok(SessionMetadata {
        daemon_pid: u32::try_from(pid)
            .map_err(|_| failed_precondition("inspect session metadata: invalid daemon pid"))?,
        state,
        root_digest: Digest::new(hash, size).map_err(|error| {
            failed_precondition(format!(
                "inspect session metadata: invalid root digest: {error}"
            ))
        })?,
        mountpoint,
    })
}

fn open_database(path: &Path, read_only: bool) -> Result<Connection, SessionError> {
    let flags = if read_only {
        OpenFlags::SQLITE_OPEN_READ_ONLY
    } else {
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
    };
    let connection = Connection::open_with_flags(path, flags)
        .map_err(|source| database_error("open connection", path, source))?;
    connection
        .busy_timeout(Duration::from_secs(2))
        .map_err(|source| database_error("set busy timeout", path, source))?;
    if !read_only {
        connection
            .pragma_update(None, "journal_mode", "DELETE")
            .map_err(|source| database_error("set journal mode", path, source))?;
        connection
            .pragma_update(None, "foreign_keys", true)
            .map_err(|source| database_error("enable foreign keys", path, source))?;
    }
    Ok(connection)
}
fn prepare_schema(connection: &Connection, path: &Path) -> Result<(), SessionError> {
    let version = schema_version(connection, path)?;
    if version != 0 {
        validate_schema_version(connection, path)?;
    }
    connection
        .execute_batch(SCHEMA_SQL)
        .map_err(|source| database_error("create schema", path, source))?;
    if version == 0 {
        connection
            .pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(|source| database_error("record schema version", path, source))?;
    }
    Ok(())
}
fn schema_version(connection: &Connection, path: &Path) -> Result<i64, SessionError> {
    connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|source| database_error("read schema version", path, source))
}
fn validate_schema_version(connection: &Connection, path: &Path) -> Result<(), SessionError> {
    let version = schema_version(connection, path)?;
    if version == SCHEMA_VERSION {
        Ok(())
    } else {
        Err(failed_precondition(format!(
            "unsupported state schema version {version}; expected {SCHEMA_VERSION}"
        )))
    }
}
fn kind_text(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::File => "file",
        NodeKind::Directory => "directory",
        NodeKind::Symlink => "symlink",
    }
}
/// Builds a database error with an owned operation label and stable database path.
fn database_error(
    operation: impl Into<String>,
    path: &Path,
    source: rusqlite::Error,
) -> SessionError {
    SessionError::Database {
        operation: operation.into(),
        dbpath: path.to_path_buf(),
        source,
    }
}

/// Returns the current whole second since the Unix epoch for session metadata.
fn now_seconds() -> Result<i64, SessionError> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        internal_error("read current session time: value is outside the supported range")
    })?;
    i64::try_from(duration.as_secs()).map_err(|_| {
        internal_error("read current session time: value is outside the supported range")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging::init_test;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Creates an isolated active store whose root digest is stable for each test.
    fn store() -> (TempDir, SessionStore, Digest) {
        let directory = TempDir::new().unwrap();
        let root_digest = Digest::for_bytes(b"root directory");
        let store = SessionStore::create(
            directory.path().join("session.db"),
            Uuid::new_v4().to_string(),
            1,
            root_digest.clone(),
            PathBuf::from("/mounted/workspace"),
        )
        .unwrap();
        (directory, store, root_digest)
    }

    /// Builds one valid remote file child input for directory reconciliation tests.
    fn remote_file(name: &str, digest: Digest) -> Inode {
        Inode {
            id: InodeId::INVALID,
            parent: None,
            name: name.to_owned(),
            kind: NodeKind::File,
            mode: None,
            mtime: None,
            tombstone: false,
            file_remote_digest: Some(digest),
            file_overlay_path: None,
            file_content_dirty: Some(false),
            symlink_target: None,
            directory_remote_digest: None,
            directory_loaded: None,
        }
    }

    /// Builds one valid unloaded remote directory child input.
    fn remote_directory(name: &str, digest: Digest) -> Inode {
        Inode {
            id: InodeId::INVALID,
            parent: None,
            name: name.to_owned(),
            kind: NodeKind::Directory,
            mode: None,
            mtime: None,
            tombstone: false,
            file_remote_digest: None,
            file_overlay_path: None,
            file_content_dirty: None,
            symlink_target: None,
            directory_remote_digest: Some(digest),
            directory_loaded: Some(false),
        }
    }

    /// Builds one valid remote symbolic-link child input.
    fn remote_symlink(name: &str, target: &str) -> Inode {
        Inode {
            id: InodeId::INVALID,
            parent: None,
            name: name.to_owned(),
            kind: NodeKind::Symlink,
            mode: None,
            mtime: None,
            tombstone: false,
            file_remote_digest: None,
            file_overlay_path: None,
            file_content_dirty: None,
            symlink_target: Some(target.to_owned()),
            directory_remote_digest: None,
            directory_loaded: None,
        }
    }

    /// Returns an unsorted complete remote child fixture covering every node kind.
    fn remote_children() -> Vec<Inode> {
        let mut file = remote_file("b-file", Digest::for_bytes(b"file"));
        file.mode = Some(0o640);
        file.mtime = NodeTime::new(-1, 42);
        vec![
            file,
            remote_symlink("c-link", "target"),
            remote_directory("a-directory", Digest::for_bytes(b"directory")),
        ]
    }

    /// Seeds children directly so tests can construct loaded and inconsistent states.
    fn seed_directory_state(
        store: &SessionStore,
        directory: InodeId,
        loaded: bool,
        children: &[Inode],
    ) -> Vec<Inode> {
        let mut connection = store.connection.lock().unwrap();
        let transaction = connection.transaction().unwrap();
        for child in children {
            let mut child = child.clone();
            child.parent = Some(directory);
            store
                .insert_inode(&transaction, "seed directory child", &child)
                .unwrap();
        }
        transaction
            .execute(
                "UPDATE inodes SET directory_loaded = ?1 WHERE id = ?2",
                params![i64::from(loaded), directory.sqlite()],
            )
            .unwrap();
        transaction.commit().unwrap();
        drop(connection);
        store.get_directory_children(directory).unwrap()
    }

    /// Verifies proposed children were stored losslessly with allocated identities.
    fn assert_created_children(parent: InodeId, proposed: &[Inode], actual: &[Inode]) {
        let mut expected = proposed.to_vec();
        expected.sort_by(|left, right| left.name.cmp(&right.name));
        assert_eq!(expected.len(), actual.len());
        let mut ids = HashSet::new();
        for (mut expected, actual) in expected.into_iter().zip(actual) {
            assert_ne!(actual.id, InodeId::INVALID);
            assert_ne!(actual.id, InodeId::ROOT);
            assert!(ids.insert(actual.id));
            expected.id = actual.id;
            expected.parent = Some(parent);
            assert_eq!(expected, *actual);
        }
    }

    /// Verifies the exact persisted loaded flag and complete child vector.
    fn assert_directory_state(
        store: &SessionStore,
        directory: InodeId,
        loaded: bool,
        expected_children: &[Inode],
    ) {
        assert_eq!(
            store.inode(directory).unwrap().unwrap().directory_loaded,
            Some(loaded)
        );
        assert_eq!(
            store.get_directory_children(directory).unwrap(),
            expected_children
        );
    }

    /// A new store exposes an unloaded remote root and retained active metadata.
    #[test]
    fn create_persists_unloaded_root() {
        init_test();
        let (directory, store, root_digest) = store();

        let root = store.inode(InodeId::ROOT).unwrap().unwrap();
        let retained = SessionStore::inspect(&directory.path().join("session.db")).unwrap();

        assert_eq!(root.directory_remote_digest, Some(root_digest.clone()));
        assert_eq!(root.directory_loaded, Some(false));
        assert_eq!(retained.root_digest, root_digest);
        assert_eq!(retained.state, SessionLifecycle::Active);
    }

    /// An unloaded directory atomically stores its complete lossless remote child set.
    #[test]
    fn get_or_create_dir_children_materializes_unloaded_directory() {
        init_test();
        let (_directory, store, _) = store();
        let proposed = remote_children();

        let actual = store
            .get_or_create_remote_dir_children(InodeId::ROOT, &proposed)
            .unwrap();

        assert_created_children(InodeId::ROOT, &proposed, &actual);
        assert_directory_state(&store, InodeId::ROOT, true, &actual);
    }

    /// Directly corrupts stored root values to verify that raw SQLite decoding
    /// succeeds but the store API rejects every invalid domain representation.
    #[test]
    fn inode_reads_validate_persisted_values() {
        init_test();
        let corruptions = [
            ("kind = 'socket'", "invalid kind"),
            ("tombstone = 2", "tombstone with invalid boolean"),
            ("mtime_seconds = 0, mtime_nanos = NULL", "partial mtime"),
            (
                "directory_remote_digest = 'not-a-digest'",
                "invalid directory remote digest",
            ),
            ("mode = 32768", "invalid mode"),
            (
                "parent_id = 1",
                "root inode unexpectedly had a parent inode with id 1",
            ),
            ("file_content_dirty = 0", "has invalid fields"),
        ];

        for (assignment, expected_error) in corruptions {
            // Each case gets a fresh store so one malformed value cannot hide
            // another. Direct SQL is limited to test setup because corruption
            // cannot be created through the validated store API.
            let (_directory, store, _) = store();
            let connection = store.connection.lock().unwrap();
            connection
                .execute(
                    &format!("UPDATE inodes SET {assignment} WHERE id = ?1"),
                    [InodeId::ROOT.sqlite()],
                )
                .unwrap();
            drop(connection);

            let error = store.inode(InodeId::ROOT).unwrap_err();
            assert!(
                error.to_string().contains(expected_error),
                "expected `{expected_error}` in `{error}` after `{assignment}`"
            );
        }
    }

    /// An empty remote directory becomes distinguishably loaded with no children.
    #[test]
    fn get_or_create_dir_children_materializes_empty_directory() {
        init_test();
        let (_directory, store, _) = store();

        assert!(
            store
                .get_or_create_remote_dir_children(InodeId::ROOT, &[])
                .unwrap()
                .is_empty()
        );
        assert_directory_state(&store, InodeId::ROOT, true, &[]);
    }

    /// A loaded directory ignores a different proposed child set and retains its identities.
    #[test]
    fn get_or_create_dir_children_returns_existing_when_already_loaded() {
        init_test();
        let (_directory, store, _) = store();
        let existing = seed_directory_state(&store, InodeId::ROOT, true, &remote_children());
        let mut invalid_proposal = remote_file("ignored", Digest::for_bytes(b"ignored"));
        invalid_proposal.id = InodeId::ROOT;

        let actual = store
            .get_or_create_remote_dir_children(InodeId::ROOT, &[invalid_proposal])
            .unwrap();

        assert_eq!(actual, existing);
        assert_directory_state(&store, InodeId::ROOT, true, &existing);
    }

    /// Concurrent updates accept one complete proposed set without mixing or corruption.
    #[test]
    fn concurrent_directory_child_creation_succeeds() {
        init_test();
        let (_directory, store, _) = store();
        let first_proposed = remote_children();
        let second_proposed = vec![remote_file("other-file", Digest::for_bytes(b"other-file"))];
        let store = Arc::new(store);
        let first_store = Arc::clone(&store);
        let second_store = Arc::clone(&store);
        let first_children = first_proposed.clone();
        let second_children = second_proposed.clone();
        let first = std::thread::spawn(move || {
            first_store.get_or_create_remote_dir_children(InodeId::ROOT, &first_children)
        });
        let second = std::thread::spawn(move || {
            second_store.get_or_create_remote_dir_children(InodeId::ROOT, &second_children)
        });

        let first = first.join().unwrap().unwrap();
        let second = second.join().unwrap().unwrap();
        assert_eq!(first, second);
        let accepted = if first.len() == first_proposed.len() {
            &first_proposed
        } else {
            &second_proposed
        };
        assert_created_children(InodeId::ROOT, accepted, &first);
        assert_directory_state(&store, InodeId::ROOT, true, &first);
    }

    /// An unloaded directory with children is rejected without repairing inconsistent state.
    #[test]
    fn get_or_create_dir_children_rejects_unloaded_directory_with_children() {
        init_test();
        let (_directory, store, _) = store();
        let existing = seed_directory_state(&store, InodeId::ROOT, false, &remote_children());

        let error = store
            .get_or_create_remote_dir_children(InodeId::ROOT, &[])
            .unwrap_err();

        assert!(error.to_string().contains("already has stored children"));
        assert_directory_state(&store, InodeId::ROOT, false, &existing);
    }

    /// Missing and non-directory parents retain their distinct domain errors.
    #[test]
    fn get_or_create_dir_children_rejects_invalid_parent() {
        init_test();
        let (_directory, store, _) = store();
        let file = seed_directory_state(
            &store,
            InodeId::ROOT,
            true,
            &[remote_file("file", Digest::for_bytes(b"file"))],
        )[0]
        .id;

        assert!(matches!(
            store
                .get_or_create_remote_dir_children(InodeId::INVALID, &[])
                .unwrap_err(),
            SessionError::InternalError { .. } | SessionError::Context { .. }
        ));
        assert!(
            store
                .get_or_create_remote_dir_children(file, &[])
                .unwrap_err()
                .to_string()
                .contains("not a directory")
        );
    }

    /// A directory with a NULL loaded flag is rejected as malformed stored data.
    #[test]
    fn get_or_create_dir_children_rejects_parent_without_loaded_flag() {
        init_test();
        let (_directory, store, _) = store();
        store
            .connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE inodes SET directory_loaded = NULL WHERE id = ?1",
                [InodeId::ROOT.sqlite()],
            )
            .unwrap();

        let error = store
            .get_or_create_remote_dir_children(InodeId::ROOT, &[])
            .unwrap_err();
        assert!(error.to_string().contains("root inode was not loaded"));
        assert!(
            store
                .get_directory_children(InodeId::ROOT)
                .unwrap()
                .is_empty()
        );
    }

    /// Every invalid child set fails before any prefix or loaded flag is committed.
    #[test]
    fn get_or_create_dir_children_rejects_invalid_child_sets_atomically() {
        init_test();
        let mut supplied_id = remote_file("id", Digest::for_bytes(b"id"));
        supplied_id.id = InodeId::ROOT;
        let mut supplied_parent = remote_file("parent", Digest::for_bytes(b"parent"));
        supplied_parent.parent = Some(InodeId::ROOT);
        let mut invalid_mode = remote_file("mode", Digest::for_bytes(b"mode"));
        invalid_mode.mode = Some(0o100000);
        let mut wrong_fields = remote_file("fields", Digest::for_bytes(b"fields"));
        wrong_fields.symlink_target = Some("target".to_owned());
        let mut tombstone = remote_file("tombstone", Digest::for_bytes(b"tombstone"));
        tombstone.tombstone = true;
        let mut overlay = remote_file("overlay", Digest::for_bytes(b"overlay"));
        overlay.file_overlay_path = Some(PathBuf::from("overlay"));
        let mut dirty = remote_file("dirty", Digest::for_bytes(b"dirty"));
        dirty.file_content_dirty = Some(true);
        let mut file_no_contents = remote_file("file-no-contents", Digest::for_bytes(b"digest"));
        file_no_contents.file_remote_digest = None;
        let mut loaded_dir = remote_directory("loaded", Digest::for_bytes(b"loaded"));
        loaded_dir.directory_loaded = Some(true);
        let duplicate = remote_file("duplicate", Digest::for_bytes(b"one"));
        let cases = vec![
            ("supplies an inode ID", vec![supplied_id]),
            ("supplies a parent", vec![supplied_parent]),
            (
                "invalid inode name",
                vec![remote_file("", Digest::for_bytes(b"empty"))],
            ),
            (
                "invalid inode name",
                vec![remote_file(".", Digest::for_bytes(b"dot"))],
            ),
            (
                "invalid inode name",
                vec![remote_file("..", Digest::for_bytes(b"dotdot"))],
            ),
            (
                "invalid inode name",
                vec![remote_file("a/b", Digest::for_bytes(b"slash"))],
            ),
            ("duplicate child name", vec![duplicate.clone(), duplicate]),
            ("has invalid mode", vec![invalid_mode]),
            ("has invalid fields", vec![wrong_fields]),
            (
                "new inode `tombstone` can't be set to tombstoned",
                vec![tombstone],
            ),
            (
                "file inode `overlay` specified both remote digest",
                vec![overlay],
            ),
            (
                "file inode `dirty` is dirty but has no overlay path",
                vec![dirty],
            ),
            (
                "file inode `file-no-contents` neither has a remote digest nor an overlay path for the file contents",
                vec![file_no_contents],
            ),
            (
                "new child directory inode `loaded` is set to loaded",
                vec![loaded_dir],
            ),
        ];

        for (expected_error, children) in cases {
            let (_directory, store, _) = store();
            let result = store.get_or_create_remote_dir_children(InodeId::ROOT, &children);
            let error = match result {
                Ok(children) => panic!(
                    "Got Ok result instead of error containing `{}`, result: {:#?}",
                    expected_error, children
                ),
                Err(err) => err,
            };
            assert!(
                error.to_string().contains(expected_error),
                "expected `{expected_error}` in `{error}`"
            );
            assert_directory_state(&store, InodeId::ROOT, false, &[]);
        }
    }

    /// Closing changes lifecycle once and retained inspection exposes the completed transition.
    #[test]
    fn close_transitions_once_and_inspect_reports_closed() {
        init_test();
        let (directory, store, _) = store();

        store.close().unwrap();
        assert!(store.close().is_err());

        assert_eq!(
            SessionStore::inspect(&directory.path().join("session.db"))
                .unwrap()
                .state,
            SessionLifecycle::Closed
        );
    }
}
