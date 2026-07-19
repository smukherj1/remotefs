-- Durable RemoteFS state schema.
--
-- Domain values and cross-column invariants are validated in the Rust storage
-- layer. SQL owns only persistence shape, keys, indexes, and foreign keys.

CREATE TABLE IF NOT EXISTS session_metadata (
    singleton INTEGER PRIMARY KEY,
    session_id TEXT,
    daemon_pid INTEGER,
    lifecycle TEXT,
    root_digest_hash TEXT,
    root_digest_size INTEGER,
    mountpoint TEXT,
    created_at_seconds INTEGER,
    created_at_nanos INTEGER,
    closed_at_seconds INTEGER,
    closed_at_nanos INTEGER,
    log_level TEXT,
    log_format TEXT
);

CREATE TABLE IF NOT EXISTS inodes (
    inode INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_inode INTEGER,
    name TEXT,
    kind TEXT,
    remote_digest TEXT,
    symlink_target TEXT,
    overlay_file TEXT,
    mode INTEGER,
    mtime_seconds INTEGER,
    mtime_nanos INTEGER,
    tombstone INTEGER,
    content_dirty INTEGER,
    tree_dirty INTEGER,
    FOREIGN KEY (parent_inode) REFERENCES inodes(inode) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS directory_materializations (
    inode INTEGER PRIMARY KEY,
    directory_digest TEXT,
    FOREIGN KEY (inode) REFERENCES inodes(inode) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_inodes_parent_name
    ON inodes(parent_inode, name);

CREATE INDEX IF NOT EXISTS ix_inodes_parent
    ON inodes(parent_inode);
