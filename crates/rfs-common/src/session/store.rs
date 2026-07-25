use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use uuid::Uuid;

use crate::digest::Digest;

use super::{
    InodeId, Lookup, Node, NodeKind, NodeTime, RemoteChild, RemoteContent, SessionError,
    SessionLifecycle, db_error, now_parts, stale_path,
};

const SCHEMA_VERSION: i64 = 1;
const SCHEMA_SQL: &str = include_str!("schema.sql");

pub(super) struct SessionStore {
    database_path: PathBuf,
    connection: Mutex<Connection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SessionMetadata {
    pub(super) daemon_pid: u32,
    pub(super) state: SessionLifecycle,
    pub(super) root_digest: Digest,
    pub(super) mountpoint: PathBuf,
}

pub(super) struct StoredSession {
    pub(super) session_id: String,
    pub(super) metadata: SessionMetadata,
    pub(super) closed_at_seconds: Option<i64>,
    pub(super) closed_at_nanos: Option<i64>,
}

pub(super) enum ReadSource {
    Remote(Digest),
    Overlay(PathBuf),
}

#[derive(Debug, Clone)]
struct StoredNode {
    node: Node,
    remote_digest: Option<Digest>,
    overlay_file: Option<PathBuf>,
    stored_mode: Option<u32>,
    stored_mtime: Option<NodeTime>,
    tombstone: bool,
}

type InodeTuple = (
    i64,
    Option<i64>,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    i64,
    i64,
    i64,
);

struct RawSession {
    singleton: i64,
    session_id: String,
    daemon_pid: i64,
    lifecycle: String,
    root_digest_hash: String,
    root_digest_size: i64,
    mountpoint: String,
    created_at_seconds: i64,
    created_at_nanos: i64,
    closed_at_seconds: Option<i64>,
    closed_at_nanos: Option<i64>,
    log_level: String,
    log_format: String,
}

impl SessionStore {
    pub(super) fn create(
        database_path: PathBuf,
        session_id: String,
        daemon_pid: u32,
        root_digest: Digest,
        mountpoint: PathBuf,
    ) -> Result<Self, SessionError> {
        let mut connection = open_database(&database_path, false)?;
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

    pub(super) fn inspect(path: &Path) -> Result<StoredSession, SessionError> {
        let connection = open_database(path, true)?;
        validate_schema_version(&connection, path)?;
        read_stored_session(&connection, path)
    }

    pub(super) fn node(&self, inode: InodeId) -> Result<Node, SessionError> {
        let connection = self.connection("read visible inode")?;
        let stored = read_inode_by_id(&connection, &self.database_path, inode)?
            .filter(|node| !node.tombstone)
            .ok_or(SessionError::UnknownInode { inode })?;
        Ok(stored.node)
    }

    pub(super) fn lookup(&self, parent: InodeId, name: &str) -> Result<Lookup<Node>, SessionError> {
        validate_child_name(&self.database_path, name)?;
        let connection = self.connection("look up visible child")?;
        let parent_node = required_directory(&connection, &self.database_path, parent)?;
        if let Some(child) = read_child(&connection, &self.database_path, parent, name)?
            && !child.tombstone
        {
            return Ok(Lookup::Ready(child.node));
        }
        match materialized_digest(&connection, &self.database_path, parent)? {
            Some(_) => Err(SessionError::NotFound {
                parent,
                name: name.to_owned(),
            }),
            None => Ok(Lookup::NeedsMaterialization {
                digest: required_remote_digest(&self.database_path, &parent_node)?,
            }),
        }
    }

    pub(super) fn list_directory(&self, inode: InodeId) -> Result<Lookup<Vec<Node>>, SessionError> {
        let connection = self.connection("list visible directory")?;
        let directory = required_directory(&connection, &self.database_path, inode)?;
        if materialized_digest(&connection, &self.database_path, inode)?.is_none() {
            return Ok(Lookup::NeedsMaterialization {
                digest: required_remote_digest(&self.database_path, &directory)?,
            });
        }
        Ok(Lookup::Ready(read_visible_children(
            &connection,
            &self.database_path,
            inode,
        )?))
    }

    pub(super) fn materialize_directory(
        &self,
        parent: InodeId,
        digest: &Digest,
        children: &[RemoteChild],
    ) -> Result<Vec<Node>, SessionError> {
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
        if required_remote_digest(&self.database_path, &parent_node)? != *digest {
            return Err(stale_path(
                &self.database_path,
                format!("inode {parent} is not remote directory {digest}"),
            ));
        }
        let prior = materialized_digest(&transaction, &self.database_path, parent)?;
        if prior.as_ref().is_some_and(|prior| prior != digest) {
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
                    params![parent.sqlite(), digest.to_string()],
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

    pub(super) fn read_source(&self, inode: InodeId) -> Result<ReadSource, SessionError> {
        let connection = self.connection("resolve inode read source")?;
        let stored = read_inode_by_id(&connection, &self.database_path, inode)?
            .filter(|node| !node.tombstone)
            .ok_or(SessionError::UnknownInode { inode })?;
        if stored.node.kind != NodeKind::File {
            return Err(SessionError::WrongKind {
                inode,
                expected: NodeKind::File,
                actual: stored.node.kind,
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

    pub(super) fn close(&self) -> Result<(), SessionError> {
        let (seconds, nanos) = now_parts()?;
        let mut connection = self.connection("close session")?;
        let transaction = connection
            .transaction()
            .map_err(|source| db_error("begin clean close", &self.database_path, source))?;
        let stored = read_stored_session(&transaction, &self.database_path)?;
        if stored.metadata.state != SessionLifecycle::Active {
            return Err(stale_path(
                &self.database_path,
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
            .map_err(|source| db_error("mark session closed", &self.database_path, source))?;
        transaction
            .commit()
            .map_err(|source| db_error("commit clean close", &self.database_path, source))
    }

    fn connection(
        &self,
        operation: &'static str,
    ) -> Result<MutexGuard<'_, Connection>, SessionError> {
        self.connection
            .lock()
            .map_err(|_| SessionError::Synchronization { operation })
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
    transaction
        .execute(
            "INSERT INTO inodes (
                inode, parent_inode, name, kind, remote_digest, symlink_target,
                overlay_file, mode, mtime_seconds, mtime_nanos, tombstone,
                content_dirty, tree_dirty
             ) VALUES (1, NULL, '', 'directory', ?1, NULL, NULL, NULL, NULL, NULL, 0, 0, 0)",
            [root_digest.to_string()],
        )
        .map_err(|source| db_error("insert root inode", path, source))?;
    transaction
        .commit()
        .map_err(|source| db_error("commit session initialization", path, source))
}

fn reconcile_children(
    transaction: &Transaction<'_>,
    path: &Path,
    parent: InodeId,
    children: &[RemoteChild],
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
        if (stored.remote_digest.is_some() || stored.node.kind == NodeKind::Symlink)
            && stored.overlay_file.is_none()
            && !stored.tombstone
            && !remote_names.contains(stored.node.name.as_str())
        {
            return Err(stale_path(
                path,
                format!("remote child set changed for directory inode {parent}"),
            ));
        }
    }
    Ok(())
}

fn insert_remote_child(
    transaction: &Transaction<'_>,
    path: &Path,
    parent: InodeId,
    child: &RemoteChild,
) -> Result<(), SessionError> {
    let (kind, remote_digest, symlink_target) = match &child.content {
        RemoteContent::File(digest) => (NodeKind::File, Some(digest.to_string()), None),
        RemoteContent::Directory(digest) => (NodeKind::Directory, Some(digest.to_string()), None),
        RemoteContent::Symlink(target) => (NodeKind::Symlink, None, Some(target.clone())),
    };
    let (mtime_seconds, mtime_nanos) = child
        .mtime
        .map(|time| (Some(time.seconds()), Some(i64::from(time.nanos()))))
        .unwrap_or((None, None));
    transaction
        .execute(
            "INSERT INTO inodes (
                parent_inode, name, kind, remote_digest, symlink_target,
                overlay_file, mode, mtime_seconds, mtime_nanos, tombstone,
                content_dirty, tree_dirty
             ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8, 0, 0, 0)",
            params![
                parent.sqlite(),
                child.name,
                node_kind_text(kind),
                remote_digest,
                symlink_target,
                child.mode.map(i64::from),
                mtime_seconds,
                mtime_nanos,
            ],
        )
        .map_err(|source| db_error("insert materialized child", path, source))?;
    Ok(())
}

fn remote_identity_matches(stored: &StoredNode, child: &RemoteChild) -> bool {
    let content_matches = match &child.content {
        RemoteContent::File(digest) => {
            stored.node.kind == NodeKind::File
                && stored.remote_digest.as_ref() == Some(digest)
                && stored.node.symlink_target.is_none()
        }
        RemoteContent::Directory(digest) => {
            stored.node.kind == NodeKind::Directory
                && stored.remote_digest.as_ref() == Some(digest)
                && stored.node.symlink_target.is_none()
        }
        RemoteContent::Symlink(target) => {
            stored.node.kind == NodeKind::Symlink
                && stored.remote_digest.is_none()
                && stored.node.symlink_target.as_ref() == Some(target)
        }
    };
    content_matches && stored.stored_mode == child.mode && stored.stored_mtime == child.mtime
}

fn required_directory(
    connection: &Connection,
    path: &Path,
    inode: InodeId,
) -> Result<StoredNode, SessionError> {
    let node = read_inode_by_id(connection, path, inode)?
        .filter(|node| !node.tombstone)
        .ok_or(SessionError::UnknownInode { inode })?;
    if node.node.kind != NodeKind::Directory {
        return Err(SessionError::WrongKind {
            inode,
            expected: NodeKind::Directory,
            actual: node.node.kind,
        });
    }
    Ok(node)
}

fn required_remote_digest(path: &Path, node: &StoredNode) -> Result<Digest, SessionError> {
    node.remote_digest.clone().ok_or_else(|| {
        stale_path(
            path,
            format!("directory inode {} has no remote digest", node.node.inode),
        )
    })
}

fn materialized_digest(
    connection: &Connection,
    path: &Path,
    inode: InodeId,
) -> Result<Option<Digest>, SessionError> {
    let value: Option<String> = connection
        .query_row(
            "SELECT directory_digest FROM directory_materializations WHERE inode = ?1",
            [inode.sqlite()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|source| db_error("read directory materialization", path, source))?;
    value
        .map(|value| {
            value.parse().map_err(|error| {
                stale_path(
                    path,
                    format!("invalid directory materialization digest: {error}"),
                )
            })
        })
        .transpose()
}

fn read_inode_by_id(
    connection: &Connection,
    path: &Path,
    inode: InodeId,
) -> Result<Option<StoredNode>, SessionError> {
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

fn read_child(
    connection: &Connection,
    path: &Path,
    parent: InodeId,
    name: &str,
) -> Result<Option<StoredNode>, SessionError> {
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

fn read_children(
    connection: &Connection,
    path: &Path,
    parent: InodeId,
) -> Result<Vec<StoredNode>, SessionError> {
    let mut statement = connection
        .prepare(
            "SELECT inode, parent_inode, name, kind, remote_digest, symlink_target,
                    overlay_file, mode, mtime_seconds, mtime_nanos, tombstone,
                    content_dirty, tree_dirty
             FROM inodes WHERE parent_inode = ?1 ORDER BY name",
        )
        .map_err(|source| db_error("prepare child listing", path, source))?;
    let rows = statement
        .query_map([parent.sqlite()], inode_tuple)
        .map_err(|source| db_error("list children", path, source))?;
    rows.map(|row| {
        row.map_err(|source| db_error("decode child row", path, source))
            .and_then(|row| validate_inode_row(path, row))
    })
    .collect()
}

fn read_visible_children(
    connection: &Connection,
    path: &Path,
    parent: InodeId,
) -> Result<Vec<Node>, SessionError> {
    Ok(read_children(connection, path, parent)?
        .into_iter()
        .filter(|stored| !stored.tombstone)
        .map(|stored| stored.node)
        .collect())
}

fn query_inode(
    connection: &Connection,
    path: &Path,
    operation: &'static str,
    sql: &str,
    parameters: impl rusqlite::Params,
) -> Result<Option<StoredNode>, SessionError> {
    let row = connection
        .query_row(sql, parameters, inode_tuple)
        .optional()
        .map_err(|source| db_error(operation, path, source))?;
    row.map(|row| validate_inode_row(path, row)).transpose()
}

fn inode_tuple(row: &rusqlite::Row<'_>) -> rusqlite::Result<InodeTuple> {
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
}

fn validate_inode_row(path: &Path, row: InodeTuple) -> Result<StoredNode, SessionError> {
    let (
        inode,
        parent_inode,
        name,
        kind,
        remote_digest,
        symlink_target,
        overlay_file,
        mode,
        mtime_seconds,
        mtime_nanos,
        tombstone,
        content_dirty,
        tree_dirty,
    ) = row;
    let inode = InodeId::from_sqlite(inode, path)?;
    let parent = match (inode, parent_inode) {
        (InodeId::ROOT, None) if name.is_empty() => InodeId::ROOT,
        (InodeId::ROOT, _) => {
            return Err(stale_path(
                path,
                "root inode has an invalid identity".into(),
            ));
        }
        (_, Some(parent)) if !name.is_empty() && !name.contains('/') => {
            InodeId::from_sqlite(parent, path)?
        }
        _ => {
            return Err(stale_path(
                path,
                format!("inode {inode} has an invalid identity"),
            ));
        }
    };
    let kind = parse_node_kind(path, &kind)?;
    let tombstone = parse_boolean(path, inode, "tombstone", tombstone)?;
    parse_boolean(path, inode, "content_dirty", content_dirty)?;
    parse_boolean(path, inode, "tree_dirty", tree_dirty)?;
    let stored_mode = mode
        .map(|value| {
            u32::try_from(value)
                .map_err(|_| stale_path(path, format!("inode {inode} has an invalid mode")))
        })
        .transpose()?;
    let stored_mtime = match (mtime_seconds, mtime_nanos) {
        (None, None) => None,
        (Some(seconds), Some(nanos)) => {
            let nanos = u32::try_from(nanos).ok();
            nanos
                .and_then(|nanos| NodeTime::new(seconds, nanos))
                .map(Some)
                .ok_or_else(|| {
                    stale_path(
                        path,
                        format!("inode {inode} has an invalid modification time"),
                    )
                })?
        }
        _ => {
            return Err(stale_path(
                path,
                format!("inode {inode} has a partial modification time"),
            ));
        }
    };
    let remote_digest: Option<Digest> = remote_digest
        .map(|value| {
            value
                .parse()
                .map_err(|error| stale_path(path, format!("invalid inode remote digest: {error}")))
        })
        .transpose()?;
    let overlay_file = overlay_file
        .map(PathBuf::from)
        .map(|value| {
            if value.as_os_str().is_empty() || value.is_absolute() {
                Err(stale_path(
                    path,
                    format!("inode {inode} has an invalid overlay file"),
                ))
            } else {
                Ok(value)
            }
        })
        .transpose()?;
    let valid_shape = match kind {
        NodeKind::File => symlink_target.is_none(),
        NodeKind::Directory => symlink_target.is_none() && overlay_file.is_none(),
        NodeKind::Symlink => {
            symlink_target.is_some() && remote_digest.is_none() && overlay_file.is_none()
        }
    };
    if !valid_shape {
        return Err(stale_path(
            path,
            format!("inode {inode} fields do not match its kind"),
        ));
    }
    let size = match kind {
        NodeKind::File => remote_digest
            .as_ref()
            .map(|digest| u64::try_from(digest.size_bytes()).expect("digest size is non-negative"))
            .unwrap_or(0),
        NodeKind::Directory => 0,
        NodeKind::Symlink => symlink_target.as_ref().map_or(0, |target| {
            u64::try_from(target.len()).expect("string length fits u64")
        }),
    };
    let effective_mode = stored_mode.unwrap_or(match kind {
        NodeKind::File => 0o444,
        NodeKind::Directory => 0o555,
        NodeKind::Symlink => 0o777,
    });
    Ok(StoredNode {
        node: Node {
            inode,
            parent,
            name,
            kind,
            size,
            mode: effective_mode,
            mtime: stored_mtime.unwrap_or(NodeTime::UNIX_EPOCH),
            symlink_target,
        },
        remote_digest,
        overlay_file,
        stored_mode,
        stored_mtime,
        tombstone,
    })
}

fn validate_remote_children(path: &Path, children: &[RemoteChild]) -> Result<(), SessionError> {
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

fn validate_child_name(path: &Path, name: &str) -> Result<(), SessionError> {
    if name.is_empty() || name.contains('/') || matches!(name, "." | "..") {
        return Err(stale_path(
            path,
            format!("invalid remote child name `{name}`"),
        ));
    }
    Ok(())
}

fn ensure_active(connection: &Connection, path: &Path) -> Result<(), SessionError> {
    let stored = read_stored_session(connection, path)?;
    if stored.metadata.state != SessionLifecycle::Active {
        return Err(stale_path(
            path,
            format!(
                "cannot materialize inodes while session is {}",
                stored.metadata.state
            ),
        ));
    }
    Ok(())
}

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
        session_id: row.session_id,
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
    if version != SCHEMA_VERSION {
        return Err(stale_path(
            path,
            format!("unsupported state schema version {version}; expected {SCHEMA_VERSION}"),
        ));
    }
    Ok(())
}

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

fn parse_node_kind(path: &Path, value: &str) -> Result<NodeKind, SessionError> {
    match value {
        "file" => Ok(NodeKind::File),
        "directory" => Ok(NodeKind::Directory),
        "symlink" => Ok(NodeKind::Symlink),
        _ => Err(stale_path(
            path,
            format!("unsupported inode kind `{value}`"),
        )),
    }
}

fn node_kind_text(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::File => "file",
        NodeKind::Directory => "directory",
        NodeKind::Symlink => "symlink",
    }
}

fn parse_boolean(
    path: &Path,
    inode: InodeId,
    field: &str,
    value: i64,
) -> Result<bool, SessionError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(stale_path(
            path,
            format!("inode {inode} has invalid {field}"),
        )),
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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
}
