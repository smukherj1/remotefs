-- Durable RemoteFS state schema. Rust owns domain validation; SQL owns shape.

-- The single mount session, its immutable identity, and lifecycle timestamps.
-- Rust requires exactly one row whose singleton value is 1.
CREATE TABLE IF NOT EXISTS session_metadata (
    -- Primary key fixed to 1 so the table represents one session.
    singleton INTEGER PRIMARY KEY,
    -- Non-empty UUID identifying this mount session.
    session_id TEXT,
    -- Positive process ID of the daemon that created the session.
    daemon_pid INTEGER,
    -- Lifecycle value: initializing, active, or closed.
    lifecycle TEXT,
    -- Lowercase SHA-256 hash of the immutable root Directory digest.
    root_digest_hash TEXT,
    -- Non-negative byte size of the root Directory message.
    root_digest_size INTEGER,
    -- Canonical absolute UTF-8 path of the mounted workspace.
    mountpoint TEXT,
    -- Whole seconds of session creation time relative to the Unix epoch.
    created_at_seconds INTEGER,
    -- Normalized nanosecond fraction of session creation time.
    created_at_nanos INTEGER,
    -- Whole seconds of clean-close time; null unless lifecycle is closed.
    closed_at_seconds INTEGER,
    -- Clean-close nanosecond fraction; present exactly with closed_at_seconds.
    closed_at_nanos INTEGER,
    -- Effective non-empty daemon log level used for status reporting.
    log_level TEXT,
    -- Effective daemon log encoding: text or json.
    log_format TEXT
);

-- Session-stable merged namespace containing remote, overlay, and hidden nodes.
CREATE TABLE IF NOT EXISTS inodes (
    -- Synthetic inode identity; the root is always 1.
    inode INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Owning directory inode; null only for the root.
    parent_inode INTEGER,
    -- UTF-8 basename; empty only for the root and never contains a slash.
    name TEXT,
    -- Node type: file, directory, or symlink.
    kind TEXT,
    -- Canonical digest of remote file bytes or a remote Directory; otherwise null.
    remote_digest TEXT,
    -- Exact UTF-8 symlink target; non-null only for symlinks.
    symlink_target TEXT,
    -- Relative overlay-data filename for local file bytes; never absolute.
    overlay_file TEXT,
    -- Preserved supported Unix mode bits; null means use the kind default.
    mode INTEGER,
    -- Whole seconds of preserved mtime relative to the Unix epoch.
    mtime_seconds INTEGER,
    -- Normalized mtime nanoseconds; present exactly with mtime_seconds.
    mtime_nanos INTEGER,
    -- Boolean indicating that this row hides its remote namespace entry.
    tombstone INTEGER,
    -- Boolean indicating that file bytes must be hashed for a snapshot.
    content_dirty INTEGER,
    -- Boolean indicating that this node or a descendant changes tree encoding.
    tree_dirty INTEGER,
    -- Namespace rows cannot outlive or delete their parent directory row.
    FOREIGN KEY (parent_inode) REFERENCES inodes(inode) ON DELETE RESTRICT
);

-- Remote directories whose complete immutable child set is present in inodes.
CREATE TABLE IF NOT EXISTS directory_materializations (
    -- Materialized directory inode and primary key owned by the inodes table.
    inode INTEGER PRIMARY KEY,
    -- Canonical digest of the remote Directory whose children were recorded.
    directory_digest TEXT,
    -- Removing an inode also removes its materialization marker.
    FOREIGN KEY (inode) REFERENCES inodes(inode) ON DELETE CASCADE
);

-- Enforces one namespace entry for each parent and basename pair.
CREATE UNIQUE INDEX IF NOT EXISTS uq_inodes_parent_name
    ON inodes(parent_inode, name);

-- Supports locating child rows by parent inode.
CREATE INDEX IF NOT EXISTS ix_inodes_parent
    ON inodes(parent_inode);
