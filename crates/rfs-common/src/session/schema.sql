-- Durable RemoteFS state schema. Rust owns domain validation; SQL owns shape.

-- The single mount session and its durable lifecycle state.
CREATE TABLE IF NOT EXISTS session_metadata (
    -- Primary key fixed to 1 because a database describes one session.
    singleton INTEGER PRIMARY KEY,
    -- Non-empty UUID that identifies the mount session.
    session_id TEXT,
    -- Positive process ID of the daemon that created the session.
    daemon_pid INTEGER,
    -- Lifecycle value: initializing, active, or closed.
    lifecycle TEXT,
    -- Lowercase SHA-256 hash of the root Directory object.
    root_digest_hash TEXT,
    -- Size of the root Directory object.
    root_digest_size INTEGER,
    -- Canonical absolute UTF-8 path of the mounted workspace.
    mountpoint TEXT,
    -- Whole seconds since Unix Epoch when the session was created.
    created_at_seconds INTEGER,
    -- Whole seconds of clean close time, null until the session is closed.
    closed_at_seconds INTEGER,
    -- Effective daemon log level retained for inspection.
    log_level TEXT,
    -- Effective daemon log encoding: text or json.
    log_format TEXT
);

-- The durable merged namespace. Rust's `store::Inode` validates every row.
CREATE TABLE IF NOT EXISTS inodes (
    -- The inode ID.
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Owning parent inode ID; null only for root.
    parent_id            INTEGER,
    -- UTF-8 basename; empty only for root.
    name                    TEXT NOT NULL,
    -- Node type: file, directory, or symlink.
    kind                    TEXT NOT NULL,
    -- Explicit Unix mode; null means the kind's effective default.
    mode                    INTEGER,
    -- Whole seconds of explicit modification time.
    mtime_seconds           INTEGER,
    -- Normalized nanosecond fraction paired with mtime_seconds.
    mtime_nanos             INTEGER,
    -- Boolean indicating the inode was unlinked. However, any existing
    -- open handles to the inode may still read the inode. An enhancement
    -- would be to garbage collect the inode when the last open handle to
    -- the inode closes it after it's been tombstoned / unlinked.
    tombstone               INTEGER NOT NULL,
    -- Immutable regular-file content digest; null for other kinds.
    file_remote_digest      TEXT,
    -- Relative session-overlay path for regular-file content.
    file_overlay_path       TEXT,
    -- Boolean dirty-content state for regular files; null otherwise.
    file_content_dirty      INTEGER,
    -- Exact symbolic-link target; null for other kinds.
    symlink_target          TEXT,
    -- Immutable serialized Directory digest; null for other kinds.
    directory_remote_digest TEXT,
    -- Boolean complete-child-set state for directories; null otherwise.
    directory_loaded        INTEGER,
    -- Child rows cannot outlive or delete their parent row.
    FOREIGN KEY (parent_id) REFERENCES inodes(id) ON DELETE RESTRICT
);

-- One stored name may exist below a parent, including a tombstone.
CREATE UNIQUE INDEX IF NOT EXISTS uq_inodes_parent_name
    ON inodes(parent_id, name);

-- Supports basename lookup and ordered directory listing by parent.
CREATE INDEX IF NOT EXISTS ix_inodes_parent
    ON inodes(parent_id);
