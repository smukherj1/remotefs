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
        let mut connection = open_database(&database_path, false)?;
        prepare_schema(&connection, &database_path)?;
        initialize_database(
            &mut connection,
            &database_path,
            &session_id,
            daemon_pid,
            &root_digest,
            &mountpoint,
        )?;
        Ok(Self {
            database_path,
            connection: Mutex::new(connection),
        })
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
        validate_child_name(name)?;
        let connection = self.connection("create sql db connection to read child")?;
        self.lookup_child_in_dir_inode(&connection, parent, name)
    }

    /// Fetches every direct child row in basename order, including tombstones.
    pub(super) fn get_directory_children(
        &self,
        parent: InodeId,
    ) -> Result<Vec<Inode>, SessionError> {
        let connection = self.connection("list children")?;
        read_children(&connection, &self.database_path, parent)
    }

    /// Atomically creates an unloaded directory's complete remote child set.
    pub(super) fn create_directory_children(
        &self,
        parent: InodeId,
        children: &[Inode],
    ) -> Result<Vec<Inode>, SessionError> {
        // TODO: Simplify logic.
        // Case 1: Directory is loaded, then just return. Nothing to do.
        // Case 2: Directory is not loaded.
        //   Case 2a: Directory has children, return internal error as this is
        //            unexpected. We should be atomically creating children and
        //            set loaded to true.
        //   Case 2b: Directory doesn't have children, create them and atomically
        //            set loaded to true.
        validate_child_inodes_for_creation(children).with_context(|| {
            format!(
                "validating child inodes to be created in directory inode {}",
                parent
            )
        })?;
        let mut connection = self.connection("create directory children")?;
        let transaction = connection
            .transaction()
            .map_err(|source| db_error("begin child creation", &self.database_path, source))?;
        let parent_inode = self
            .get_inode_by_id(&transaction, parent)
            .with_context(|| format!("read parent inode {parent} to create its children"))?
            .ok_or_else(|| {
                internal_error(format!(
                    "can't create children because parent inode {parent} is missing"
                ))
            })?;
        ensure_inode_is_dir(&parent_inode)
            .with_context(|| format!("can't create children for non-directory inode {parent}"))?;
        if parent_inode.directory_loaded == Some(true) {
            let result =
                read_children(&transaction, &self.database_path, parent).with_context(|| {
                    format!("reading child inodes of directory inode {} from db", parent)
                })?;
            transaction.commit().map_err(|source| {
                db_error(
                    "commit repeated child creation",
                    &self.database_path,
                    source,
                )
            })?;
            return Ok(result);
        }
        for child in children {
            match self
                .lookup_child_in_dir_inode(&transaction, parent, &child.name)
                .with_context(|| format!("read stored child `{}` of inode {parent}", child.name))?
            {
                Some(existing) if authoritative_overlay(&existing) => {}
                Some(existing) if remote_inode_matches(&existing, child) => {}
                Some(_) => {
                    return Err(internal_error(format!(
                        "directory inode {parent} has conflicting child `{}`",
                        child.name
                    )));
                }
                None => insert_inode(
                    &transaction,
                    &self.database_path,
                    "insert directory child",
                    &child_for_parent(parent, child),
                )
                .with_context(|| {
                    format!(
                        "inserting child inode {} in directory inode {} to db",
                        child.name, parent
                    )
                })?,
            }
        }
        transaction
            .execute(
                "UPDATE inodes SET directory_loaded = 1 WHERE id = ?1",
                [parent.sqlite()],
            )
            .map_err(|source| db_error("mark directory loaded", &self.database_path, source))?;
        let result =
            read_children(&transaction, &self.database_path, parent).with_context(|| {
                format!(
                    "loading children of directory inode {} after creating children inodes",
                    parent
                )
            })?;
        transaction
            .commit()
            .map_err(|source| db_error("commit child creation", &self.database_path, source))?;
        Ok(result)
    }

    /// Transitions active metadata to closed exactly once.
    ///
    /// Errors: `FailedPreconditionError` when the stored session is not active, so a
    /// second close attempt reports the mismatch instead of rewriting the close time.
    pub(super) fn close(&self) -> Result<(), SessionError> {
        let seconds = now_seconds()
            .context("getting current time to set session close time in session state db")?;
        let mut connection = self.connection("close session")?;
        let transaction = connection
            .transaction()
            .map_err(|source| db_error("begin close", &self.database_path, source))?;
        self.ensure_active(&transaction)
            .context("can't close already closed session")?;
        transaction.execute("UPDATE session_metadata SET lifecycle = 'closed', closed_at_seconds = ?1 WHERE singleton = 1", [seconds])
            .map_err(|source| db_error("mark session closed", &self.database_path, source))?;
        transaction
            .commit()
            .map_err(|source| db_error("commit close", &self.database_path, source))
    }

    fn connection(
        &self,
        operation: &'static str,
    ) -> Result<MutexGuard<'_, Connection>, SessionError> {
        self.connection
            .lock()
            .map_err(|_| internal_error(format!("{operation}: connection lock is poisoned")))
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
            .map_err(|source| db_error("get inode by id", &self.database_path, source))?
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
            .map_err(|source| {
                db_error(
                    "lookup child by name in dir inode",
                    &self.database_path,
                    source,
                )
            })?
            .map(validate_inode_row)
            .transpose()
            .with_context(|| {
                format!("validate stored child `{name}` of directory inode {parent_id}")
            })
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
/// Field order in [`Self::from_rusqlite_row`] must match [`Self::column_list`].
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
    /// Inputs: `row` produced by a query using [`Self::column_list`]. Returns
    /// the SQLite-shaped values. Errors: SQLite type or column-read failures.
    fn from_rusqlite_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get(0)?,
            parent_id: row.get(1)?,
            name: row.get(2)?,
            kind: row.get(3)?,
            mode: row.get(4)?,
            mtime_seconds: row.get(5)?,
            mtime_nanos: row.get(6)?,
            tombstone: row.get(7)?,
            file_remote_digest: row.get(8)?,
            file_overlay_path: row.get(9)?,
            file_content_dirty: row.get(10)?,
            symlink_target: row.get(11)?,
            directory_remote_digest: row.get(12)?,
            directory_loaded: row.get(13)?,
        })
    }
}

fn initialize_database(
    connection: &mut Connection,
    path: &Path,
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
    let now = now_seconds()
        .with_context(|| "unable to determine current time to set session open time".to_string())?;
    let transaction = connection
        .transaction()
        .map_err(|source| db_error("begin initialization", path, source))?;
    transaction.execute("INSERT INTO session_metadata (singleton, session_id, daemon_pid, lifecycle, root_digest_hash, root_digest_size, mountpoint, created_at_seconds, closed_at_seconds, log_level, log_format) VALUES (1, ?1, ?2, 'active', ?3, ?4, ?5, ?6, NULL, 'info', 'text')", params![session_id, i64::from(daemon_pid), root_digest.hash(), root_digest.size_bytes(), mountpoint, now])
        .map_err(|source| db_error("insert session metadata", path, source))?;
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
    insert_inode(&transaction, path, "insert root inode", &root)?;
    transaction
        .commit()
        .map_err(|source| db_error("commit initialization", path, source))
}

fn child_for_parent(parent: InodeId, child: &Inode) -> Inode {
    let mut child = child.clone();
    child.id = InodeId::INVALID;
    child.parent = Some(parent);
    child
}
fn authoritative_overlay(inode: &Inode) -> bool {
    inode.tombstone
        || inode.file_overlay_path.is_some()
        || (inode.kind == NodeKind::Directory && inode.directory_remote_digest.is_none())
}
fn remote_inode_matches(stored: &Inode, proposed: &Inode) -> bool {
    stored.kind == proposed.kind
        && stored.mode == proposed.mode
        && stored.mtime == proposed.mtime
        && stored.tombstone == proposed.tombstone
        && stored.file_remote_digest == proposed.file_remote_digest
        && stored.symlink_target == proposed.symlink_target
        && stored.directory_remote_digest == proposed.directory_remote_digest
}

fn validate_child_inodes_for_creation(children: &[Inode]) -> Result<(), SessionError> {
    let mut names = HashSet::with_capacity(children.len());
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
        validate_child_name(&child.name)?;
        if !names.insert(child.name.as_str()) {
            return Err(internal_error(format!(
                "duplicate child name `{}`",
                child.name
            )));
        }
        validate_inode(child, false)?;
        // TODO: Lots of different checks combined into one here that need distinct error
        // messages.
        if child.tombstone
            || child.file_overlay_path.is_some()
            || child.file_content_dirty == Some(true)
            || matches!(child.kind, NodeKind::File) && child.file_remote_digest.is_none()
            || matches!(child.kind, NodeKind::Directory)
                && (child.directory_remote_digest.is_none()
                    || child.directory_loaded != Some(false))
        {
            return Err(internal_error(format!(
                "child `{}` is not a remote inode",
                child.name
            )));
        }
    }
    Ok(())
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
    validate_inode(&result, true).with_context(|| format!("validate stored inode {inode}"))?;
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

fn validate_inode(inode: &Inode, is_stored: bool) -> Result<(), SessionError> {
    if let Some(mode) = inode.mode
        && mode > 0o7777
    {
        return Err(internal_error(format!(
            "inode `{}` has invalid mode",
            inode.name
        )));
    }
    if is_stored {
        validate_stored_identity(inode)?;
    }
    let file_values = (
        &inode.file_remote_digest,
        &inode.file_overlay_path,
        inode.file_content_dirty,
    );
    match inode.kind {
        NodeKind::File => {
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
        }
        NodeKind::Symlink => {
            if inode.symlink_target.is_none()
                || file_values.0.is_some()
                || file_values.1.is_some()
                || file_values.2.is_some()
                || inode.directory_remote_digest.is_some()
                || inode.directory_loaded.is_some()
            {
                return Err(internal_error(format!(
                    "symlink inode `{}` has invalid fields",
                    inode.name
                )));
            }
        }
        NodeKind::Directory => {
            if file_values.0.is_some()
                || file_values.1.is_some()
                || file_values.2.is_some()
                || inode.symlink_target.is_some()
                || inode.directory_loaded.is_none()
            {
                return Err(internal_error(format!(
                    "directory inode `{}` has invalid fields",
                    inode.name
                )));
            }
            if inode.directory_remote_digest.is_none() && inode.directory_loaded != Some(true) {
                return Err(internal_error(format!(
                    "local directory inode `{}` is not loaded",
                    inode.name
                )));
            }
        }
    }
    Ok(())
}

fn validate_stored_identity(inode: &Inode) -> Result<(), SessionError> {
    let identity = inode.id;
    match (identity, inode.parent) {
        (InodeId::ROOT, None)
            if inode.name.is_empty()
                && inode.kind == NodeKind::Directory
                && inode.directory_remote_digest.is_some()
                && inode.directory_loaded.is_some() =>
        {
            Ok(())
        }
        (InodeId::ROOT, _) => Err(internal_error("root inode shape is invalid")),
        (_, Some(_)) => validate_child_name(&inode.name),
        (_, None) => Err(internal_error(format!("inode {identity} has no parent"))),
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

fn validate_child_name(name: &str) -> Result<(), SessionError> {
    if name.is_empty() || name.contains('/') || matches!(name, "." | "..") {
        Err(internal_error(format!("invalid child name `{name}`")))
    } else {
        Ok(())
    }
}

fn insert_inode(
    transaction: &Transaction<'_>,
    path: &Path,
    operation: &'static str,
    inode: &Inode,
) -> Result<(), SessionError> {
    if inode.id != InodeId::ROOT && inode.id != InodeId::INVALID {
        return Err(internal_error(format!(
            "insert inode specified id {}, must be either root id {} or set to invalid id {} for auto-assignment of next available inode id",
            inode.id,
            InodeId::ROOT,
            InodeId::INVALID
        )));
    }
    validate_inode(inode, false)?;
    let overlay_path = inode
        .file_overlay_path
        .as_ref()
        .map(|overlay_path| {
            overlay_path
                .to_str()
                .ok_or_else(|| internal_error("overlay path is not UTF-8"))
        })
        .transpose()?;
    let query = format!(
        "INSERT INTO inodes ({})
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        InodeRow::column_list()
    );

    let id = if inode.id == InodeId::INVALID {
        // Setting id to None / NULL makes Sqlite auto-assign the next id.
        None
    } else {
        Some(inode.id.sqlite())
    };

    let params = params![
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
        inode.directory_loaded.map(i64::from)
    ];
    transaction
        .execute(&query, params)
        .map_err(|source| db_error(operation, path, source))?;
    Ok(())
}

fn read_children(
    connection: &Connection,
    path: &Path,
    parent: InodeId,
) -> Result<Vec<Inode>, SessionError> {
    let mut statement = connection
        .prepare(&format!(
            "SELECT {} FROM inodes WHERE parent_id = ?1 ORDER BY name",
            InodeRow::column_list()
        ))
        .map_err(|source| db_error("prepare children", path, source))?;
    let rows = statement
        .query_map([parent.sqlite()], InodeRow::from_rusqlite_row)
        .map_err(|source| db_error("list children", path, source))?;
    let rows = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| db_error("decode child", path, source))?;
    rows.into_iter()
        .map(validate_inode_row)
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("validate stored children of directory inode {parent}"))
}

/// Unvalidated session metadata decoded from the single `session_metadata` row.
///
/// Field order in [`Self::from_rusqlite_row`] must match [`Self::column_list`].
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
    /// Inputs: `row` produced by a query using [`Self::column_list`]. Returns
    /// the SQLite-shaped values. Errors: SQLite type or column-read failures.
    fn from_rusqlite_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            singleton: row.get(0)?,
            session_id: row.get(1)?,
            daemon_pid: row.get(2)?,
            lifecycle: row.get(3)?,
            root_digest_hash: row.get(4)?,
            root_digest_size: row.get(5)?,
            mountpoint: row.get(6)?,
            created_at_seconds: row.get(7)?,
            closed_at_seconds: row.get(8)?,
            log_level: row.get(9)?,
            log_format: row.get(10)?,
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
        .map_err(|source| db_error("read session metadata", path, source))?
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
fn schema_version(connection: &Connection, path: &Path) -> Result<i64, SessionError> {
    connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|source| db_error("read schema version", path, source))
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
fn db_error(operation: &'static str, path: &Path, source: rusqlite::Error) -> SessionError {
    SessionError::Database {
        operation,
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

    /// Lossless child reads retain nullable metadata, include every node kind,
    /// and return rows sorted by basename.
    #[test]
    fn child_and_children_are_lossless_and_sorted() {
        init_test();
        let (_directory, store, _) = store();
        let mut file = remote_file("b-file", Digest::for_bytes(b"file"));
        file.mode = Some(0o640);
        file.mtime = NodeTime::new(-1, 42);
        let directory = remote_directory("a-directory", Digest::for_bytes(b"directory"));
        let symlink = remote_symlink("c-link", "target");

        let children = store
            .create_directory_children(InodeId::ROOT, &[file, directory, symlink])
            .unwrap();
        let file = store.child(InodeId::ROOT, "b-file").unwrap().unwrap();

        assert_eq!(
            children
                .iter()
                .map(|child| child.name.as_str())
                .collect::<Vec<_>>(),
            ["a-directory", "b-file", "c-link"]
        );
        assert_eq!(file.mode, Some(0o640));
        assert_eq!(file.mtime, NodeTime::new(-1, 42));
        assert_eq!(file.file_remote_digest, Some(Digest::for_bytes(b"file")));
        assert_eq!(children[0].directory_loaded, Some(false));
        assert_eq!(children[2].symlink_target.as_deref(), Some("target"));
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
            ("parent_id = 1", "root inode shape is invalid"),
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

    /// Empty and repeated loads persist one complete child set and stable allocated identities.
    #[test]
    fn directory_child_creation_handles_empty_and_repeated_loads() {
        init_test();
        let (_directory, store, _) = store();
        let first = store
            .create_directory_children(
                InodeId::ROOT,
                &[remote_directory(
                    "empty",
                    Digest::for_bytes(b"empty directory"),
                )],
            )
            .unwrap();
        let second = store.create_directory_children(InodeId::ROOT, &[]).unwrap();

        let empty = first[0].id;
        assert_eq!(first, second);
        assert_ne!(first[0].id, InodeId::ROOT);
        assert!(
            store
                .create_directory_children(empty, &[])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store.inode(empty).unwrap().unwrap().directory_loaded,
            Some(true)
        );
        assert_eq!(
            store
                .inode(InodeId::ROOT)
                .unwrap()
                .unwrap()
                .directory_loaded,
            Some(true)
        );
    }

    /// Concurrent callers observe the first complete remote child set without allocating another identity.
    #[test]
    fn concurrent_directory_child_creation_reuses_allocations() {
        init_test();
        let (_directory, store, _) = store();
        let store = Arc::new(store);
        let first_store = Arc::clone(&store);
        let second_store = Arc::clone(&store);
        let first = std::thread::spawn(move || {
            first_store.create_directory_children(
                InodeId::ROOT,
                &[remote_file("entry", Digest::for_bytes(b"entry"))],
            )
        });
        let second = std::thread::spawn(move || {
            second_store.create_directory_children(
                InodeId::ROOT,
                &[remote_file("entry", Digest::for_bytes(b"entry"))],
            )
        });

        assert_eq!(
            first.join().unwrap().unwrap(),
            second.join().unwrap().unwrap()
        );
    }

    /// Child creation rejects a supplied inode identity before its write transaction.
    #[test]
    fn supplied_child_inode_is_rejected_without_changes() {
        init_test();
        let (_directory, store, _) = store();
        let mut child = remote_file("entry", Digest::for_bytes(b"entry"));
        child.id = InodeId::ROOT;

        assert!(
            store
                .create_directory_children(InodeId::ROOT, &[child])
                .is_err()
        );
        assert!(
            store
                .get_directory_children(InodeId::ROOT)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .inode(InodeId::ROOT)
                .unwrap()
                .unwrap()
                .directory_loaded,
            Some(false)
        );
    }

    /// Invalid kind fields and duplicate names fail before a transaction can expose partial children.
    #[test]
    fn invalid_children_do_not_partially_commit() {
        init_test();
        let (_directory, store, _) = store();
        let invalid = remote_file("duplicate", Digest::for_bytes(b"first"));
        let duplicate = remote_file("duplicate", Digest::for_bytes(b"second"));

        assert!(
            store
                .create_directory_children(InodeId::ROOT, &[invalid, duplicate])
                .is_err()
        );
        assert!(
            store
                .get_directory_children(InodeId::ROOT)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .inode(InodeId::ROOT)
                .unwrap()
                .unwrap()
                .directory_loaded,
            Some(false)
        );

        let mut invalid_kind = remote_file("invalid", Digest::for_bytes(b"invalid"));
        invalid_kind.directory_loaded = Some(false);
        assert!(
            store
                .create_directory_children(InodeId::ROOT, &[invalid_kind])
                .is_err()
        );
        assert!(
            store
                .get_directory_children(InodeId::ROOT)
                .unwrap()
                .is_empty()
        );
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
