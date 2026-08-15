# Session Refactor Program Design

## Purpose

Make `Session` the remote-aware facade for a mounted workspace. Callers request
filesystem results; `Session` owns namespace hydration, immutable-content
download, cache admission, and access to durable session state.

## Highlights

- **`Session`** — absorbs `ActiveSession` and the remote-read orchestration now
  implemented by `FilesystemService`; exposes complete filesystem operations.
- **`SessionStore`** — becomes a row-shaped SQLite repository with three reads
  and one atomic directory-child creation operation.
- **Inode schema** — separates file and directory digests, stores directory
  loading on the directory inode, and removes `directory_materializations`.
- **`FilesystemService`** — retains filesystem policy and error mapping; no
  longer owns CAS clients, decoding, cache admission, counters, or download
  locks.
- **`ActiveSession`** — is removed. `BlobCache` and `OverlayStore` retain their
  current behavior and become private implementation dependencies of `Session`.

## `Session`

### Changed behavior and dependencies

- `lookup`, `list_directory`, and `read_range` return final filesystem results.
  They never expose cache misses, directory-materialization requests, remote
  digests, or cache writers.
- Directory hydration downloads and decodes the inode's
  `directory_remote_digest`, then asks `SessionStore` to commit the complete
  child set and `directory_loaded = true` atomically.
- File reads prefer `file_overlay_path`; otherwise they download and admit
  `file_remote_digest` before reading the requested range from `BlobCache`.
- An inode lock serializes multi-step work for one inode. A digest lock
  coalesces downloads of the same object; independent `BlobStore` handles allow
  different digests to download concurrently. Weak lock-map entries prevent
  completed operations from accumulating.
- `close` is a one-shot durable transition. It does not wait for operations or
  release the ownership lock; the daemon quiesces FUSE before calling it and
  drops `Session` after the close attempt.
- Depends on `SessionStore`, `BlobCache`, `OverlayStore`, `BlobStore`, the REAPI
  directory decoder, a Tokio runtime handle, and the session ownership lock.
- `SessionError` gains source-preserving variants for remote reads, directory
  decoding, and invalid post-admission cache state. `CasError` and `TreeError`
  remain available through the error source chain.

### Target shape

`Inode`, `SessionInfo`, and their value types are unchanged and are not repeated.

```rust
/// Creates an independent remote-store handle for one download operation.
type BlobStoreFactory =
    Arc<dyn Fn() -> Box<dyn BlobStore + Send> + Send + Sync>;

/// Atomic counters for immutable-object reads performed by one session.
struct AtomicIoCounters {
    /// Directory objects downloaded from remote storage.
    directory_downloads: AtomicU64,
    /// Directory objects served from the shared cache, including coalesced hits.
    directory_cache_hits: AtomicU64,
    /// File objects downloaded from remote storage.
    file_downloads: AtomicU64,
    /// File objects served from the shared cache, including coalesced hits.
    file_cache_hits: AtomicU64,
    /// Current number of admitted objects observed in the shared cache.
    cached_blobs: AtomicU64,
}

/// Point-in-time immutable-object I/O counters for status reporting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoCounters {
    /// Directory objects downloaded from remote storage.
    pub directory_downloads: u64,
    /// Directory objects served from the shared cache, including coalesced hits.
    pub directory_cache_hits: u64,
    /// File objects downloaded from remote storage.
    pub file_downloads: u64,
    /// File objects served from the shared cache, including coalesced hits.
    pub file_cache_hits: u64,
    /// Current number of admitted objects observed in the shared cache.
    pub cached_blobs: u64,
}

/// Remote-aware synchronous facade for one mounted workspace.
///
/// The facade owns the session's durable namespace, overlay, shared immutable
/// cache, remote reads, and operation coordination. Its filesystem methods
/// return usable inode metadata or bytes rather than hydration protocol states.
pub struct Session {
    /// Immutable root `Directory` digest mounted by this session.
    root_digest: Digest,
    /// Canonical absolute mountpoint for this session.
    mountpoint: PathBuf,
    /// Fixed cache, database, overlay, log, socket, and lock paths.
    layout: SessionLayout,
    /// Authoritative SQLite namespace and lifecycle repository.
    store: SessionStore,
    /// Shared verified cache for file and serialized-directory objects.
    cache: BlobCache,
    /// Session-local storage for overlay-backed regular-file bytes.
    overlay: OverlayStore,
    /// Factory producing an independent clone of the configured remote client.
    blob_store_factory: BlobStoreFactory,
    /// Runtime used to bridge asynchronous remote reads into this synchronous API.
    runtime: Handle,
    /// Stable advisory lock held until this `Session` is dropped.
    session_lock: SessionLock,
    /// Per-inode coordination for multi-step hydration and read operations.
    inode_locks: Mutex<HashMap<InodeId, Weak<Mutex<()>>>>,
    /// Per-digest coordination that coalesces concurrent cache misses.
    download_locks: Mutex<HashMap<Digest, Weak<Mutex<()>>>>,
    /// Read and cache counters updated by the layer that determines hit type.
    counters: AtomicIoCounters,
}
```

### Public API

```rust
impl Session {
    /// Opens a fresh writable session for `root_digest` at `mountpoint`.
    ///
    /// `config` identifies `RFS_HOME`; `blob_store` is the cloneable remote
    /// client used for lazy reads; `runtime` executes its asynchronous calls.
    /// Returns the exclusively owned session. Returns `SessionError` when the
    /// mountpoint, home, lock, layout, cache, overlay, or database cannot be
    /// initialized. Acquires `session.lock`, replaces the prior session tree,
    /// creates fresh durable state, and retains the shared cache.
    pub fn open<S>(
        config: Config,
        root_digest: Digest,
        mountpoint: impl AsRef<Path>,
        blob_store: S,
        runtime: Handle,
    ) -> Result<Self, SessionError>
    where
        S: BlobStore + Clone + Send + Sync + 'static;

    /// Returns immutable identity and daemon paths established by `open`.
    ///
    /// Returns a new `SessionInfo` value. This operation has no I/O or side
    /// effects and does not fail.
    pub fn info(&self) -> SessionInfo;

    /// Derives the fixed control-socket path for `config` without opening SQLite.
    ///
    /// Returns the socket path. Returns `SessionError` if `RFS_HOME` cannot be
    /// inspected safely. Does not create or modify state.
    pub fn control_endpoint(config: &Config) -> Result<PathBuf, SessionError>;

    /// Inspects retained session metadata under the `RFS_HOME` in `config`.
    ///
    /// Returns `Some(SessionInfo)` for valid retained state and `None` when the
    /// home exists without a session. Returns `SessionError` for a missing home,
    /// unsupported schema, invalid metadata, or I/O failure. Opens SQLite
    /// read-only and does not lock, repair, create, or modify state.
    pub fn inspect(config: &Config) -> Result<Option<SessionInfo>, SessionError>;

    /// Returns the visible inode identified by `inode`.
    ///
    /// Returns effective `Inode` metadata. Returns `UnknownInode` for a missing
    /// or tombstoned row and `SessionError` for invalid durable state or storage
    /// failure. Performs no hydration and has no side effects.
    pub fn node(&self, inode: InodeId) -> Result<Inode, SessionError>;

    /// Resolves `name` as a direct visible child of directory `parent`.
    ///
    /// Returns the child's effective `Inode`. Returns `NotFound` after the
    /// complete child set is known, `WrongKind` for a non-directory parent, or
    /// `SessionError` for download, decode, integrity, synchronization, or
    /// storage failures. May download and cache one directory object, update
    /// counters, and atomically populate the parent's children in SQLite.
    pub fn lookup(
        &self,
        parent: InodeId,
        name: &str,
    ) -> Result<Inode, SessionError>;

    /// Returns every visible direct child of directory `inode` in basename order.
    ///
    /// Returns a complete `Vec<Inode>`. Returns `WrongKind` for a non-directory
    /// inode or `SessionError` for download, decode, integrity,
    /// synchronization, or storage failures. May perform the same directory
    /// hydration and durable namespace update as `lookup`.
    pub fn list_directory(
        &self,
        inode: InodeId,
    ) -> Result<Vec<Inode>, SessionError>;

    /// Reads at most `size` bytes from regular-file `inode` starting at `offset`.
    ///
    /// Returns available bytes and returns empty bytes when `offset` is at or
    /// beyond EOF. Returns `WrongKind`, `UnknownInode`, or `SessionError` for
    /// remote, integrity, synchronization, overlay, cache, or storage failures.
    /// Overlay bytes take precedence; a remote read may download and admit the
    /// complete object and update counters before serving the range.
    pub fn read_range(
        &self,
        inode: InodeId,
        offset: u64,
        size: usize,
    ) -> Result<Bytes, SessionError>;

    /// Returns a point-in-time snapshot of this session's immutable-read counters.
    ///
    /// The returned counters may become stale immediately under concurrent I/O.
    /// This operation does not fail and has no side effects.
    pub fn io_counters(&self) -> IoCounters;

    /// Durably changes the session lifecycle from active to closed.
    ///
    /// Returns `()` after the transaction commits. Returns `SessionError` for a
    /// database failure or when the lifecycle is not active, including a second
    /// call. Records the close time but retains the ownership lock until
    /// `Session` is dropped.
    pub fn close(&self) -> Result<(), SessionError>;
}
```

Removed from the public session boundary: `Lookup`, `RemoteChild`,
`RemoteContent`, `BlobWriter`, `materialize_directory`, `read_blob`, `exists`,
`start_blob_download`, `finalize_blob`, `remote_file_digest`, and
`cached_blob_count`.

### Unit tests

All tests construct a `Session` with a fake `BlobStore` and call only the API
above.

- `node` returns effective metadata and rejects a missing inode.
- `lookup` and `list_directory` hydrate an unloaded directory, handle an empty
  directory, preserve stable inode allocation, and return the same visible set
  under concurrent calls.
- A malformed or digest-mismatched directory fails without making partial
  children visible through `lookup` or `list_directory`.
- `read_range` covers an initial remote miss, cache hit, coalesced concurrent
  miss, failed stream, failed admission, retry after failure, and EOF.
- Concurrent reads of one digest issue one remote stream; reads of different
  digests can stream concurrently.
- `io_counters` distinguishes downloads from initial and coalesced cache hits
  and reports the resulting admitted-object count.
- `close` commits one active-to-closed transition, rejects a second call, and
  leaves retained metadata inspectable after `Session` is dropped.
- `info`, `control_endpoint`, and `inspect` retain their documented read-only
  behavior.

## `SessionStore`

### Changed behavior and dependencies

- The store returns lossless `StoredInode` rows. `Session` alone filters
  tombstones and projects stored rows into effective public `Inode` values.
- `inode`, `child`, and `children` are the only inode reads. `children` includes
  tombstones so `Session` can reconcile the authoritative merged namespace.
- `create_directory_children` is the only inode write added by this refactor.
  It validates the entire input before its transaction, rechecks the parent and
  digest inside the transaction, preserves authoritative overlay rows, creates
  the complete remote child set, and marks the parent loaded atomically.
- An empty child set still marks the parent loaded. A concurrent repeat for the
  same loaded digest returns the stored complete set without allocating new
  inode IDs. A digest mismatch or any failed child write commits nothing.
- No raw connection, transaction, generic CRUD, or closure-based transaction
  API is exposed.
- Depends on `rusqlite`, `Digest`, `InodeId`, `NodeKind`, `NodeTime`, and
  `SessionError`. It has no dependency on CAS, cache, overlay, REAPI, FUSE, or
  filesystem-service types.

### Target shape

`StoredSession` and its value types are unchanged and are not repeated.

```rust
/// Repository for the durable lifecycle and merged namespace of one session.
///
/// Each operation locks one writable SQLite connection for only its bounded
/// query or transaction. Returned inode rows preserve persisted values without
/// applying visibility rules or effective metadata defaults.
pub(super) struct SessionStore {
    /// Stable database path included in validation and SQLite diagnostics.
    database_path: PathBuf,
    /// Writable connection serialized across synchronous repository operations.
    connection: Mutex<Connection>,
}

/// Lossless, validated representation of one row in the `inodes` table.
pub(super) struct StoredInode {
    /// Session-stable positive inode identity; root is `InodeId::ROOT`.
    pub(super) inode: InodeId,
    /// Parent identity, absent only for root.
    pub(super) parent: Option<InodeId>,
    /// UTF-8 basename, empty only for root.
    pub(super) name: String,
    /// Filesystem node kind controlling the valid kind-specific fields.
    pub(super) kind: NodeKind,
    /// Preserved Unix mode, or `None` when the kind default applies.
    pub(super) mode: Option<u32>,
    /// Preserved modification time, or `None` when the epoch default applies.
    pub(super) mtime: Option<NodeTime>,
    /// Whether this row hides its namespace entry.
    pub(super) tombstone: bool,
    /// Immutable file-content digest; valid only for regular files.
    pub(super) file_remote_digest: Option<Digest>,
    /// Relative overlay-data path; valid only for regular files.
    pub(super) file_overlay_path: Option<PathBuf>,
    /// File-content dirty state; present only for regular files.
    pub(super) file_content_dirty: Option<bool>,
    /// Exact symbolic-link target; present only for symbolic links.
    pub(super) symlink_target: Option<String>,
    /// Immutable serialized-`Directory` digest; valid only for directories.
    pub(super) directory_remote_digest: Option<Digest>,
    /// Whether the complete remote child set is represented in SQLite;
    /// present only for directories.
    pub(super) directory_loaded: Option<bool>,
    /// Directory-tree dirty state; present only for directories.
    pub(super) directory_tree_dirty: Option<bool>,
}

/// Validated values for a child row whose inode ID is allocated by SQLite.
pub(super) struct NewStoredInode {
    /// UTF-8 child basename relative to the parent supplied to the batch call.
    pub(super) name: String,
    /// Filesystem node kind controlling the valid kind-specific fields.
    pub(super) kind: NodeKind,
    /// Preserved Unix mode, or `None` when the kind default applies.
    pub(super) mode: Option<u32>,
    /// Preserved modification time, or `None` when the epoch default applies.
    pub(super) mtime: Option<NodeTime>,
    /// Whether this row hides its namespace entry.
    pub(super) tombstone: bool,
    /// Immutable file-content digest; valid only for regular files.
    pub(super) file_remote_digest: Option<Digest>,
    /// Relative overlay-data path; valid only for regular files.
    pub(super) file_overlay_path: Option<PathBuf>,
    /// File-content dirty state; present only for regular files.
    pub(super) file_content_dirty: Option<bool>,
    /// Exact symbolic-link target; present only for symbolic links.
    pub(super) symlink_target: Option<String>,
    /// Immutable serialized-`Directory` digest; valid only for directories.
    pub(super) directory_remote_digest: Option<Digest>,
    /// Whether the complete remote child set is represented in SQLite;
    /// present only for directories.
    pub(super) directory_loaded: Option<bool>,
    /// Directory-tree dirty state; present only for directories.
    pub(super) directory_tree_dirty: Option<bool>,
}
```

### Component API

`SessionStore` is private to `rfs_common::session`; these are its complete
callable operations.

```rust
impl SessionStore {
    /// Creates the database at `database_path` for one active session.
    ///
    /// The identity arguments are persisted in `session_metadata`; `root_digest`
    /// also backs root inode `1`. Returns the initialized store. Returns
    /// `SessionError` for schema, existing-row, clock, encoding, constraint, or
    /// SQLite failures. Installs the schema and atomically inserts metadata and
    /// the unloaded root directory.
    pub(super) fn create(
        database_path: PathBuf,
        session_id: String,
        daemon_pid: u32,
        root_digest: Digest,
        mountpoint: PathBuf,
    ) -> Result<Self, SessionError>;

    /// Reads validated retained metadata from `path` using a read-only connection.
    ///
    /// Returns the singleton `StoredSession`. Returns `SessionError` for a
    /// missing database, unsupported schema, invalid row, or SQLite failure.
    /// Opens and closes a separate read-only connection without modifying state.
    pub(super) fn inspect(path: &Path) -> Result<StoredSession, SessionError>;

    /// Fetches the row identified by `inode`, including a tombstoned row.
    ///
    /// Returns `Some(StoredInode)` when present and `None` when absent. Returns
    /// `SessionError` for synchronization, decoding, validation, or SQLite
    /// failure. Holds the connection mutex only for the indexed query.
    pub(super) fn inode(
        &self,
        inode: InodeId,
    ) -> Result<Option<StoredInode>, SessionError>;

    /// Fetches the row named `name` directly below `parent`, including a tombstone.
    ///
    /// Returns `Some(StoredInode)` when present and `None` when absent. Returns
    /// `SessionError` when `name` is invalid or the query, decoding, or
    /// validation fails. Holds the connection mutex only for the indexed query.
    pub(super) fn child(
        &self,
        parent: InodeId,
        name: &str,
    ) -> Result<Option<StoredInode>, SessionError>;

    /// Fetches every row directly below `parent` in basename order.
    ///
    /// Returns lossless rows, including tombstones. Returns `SessionError` for
    /// synchronization, decoding, validation, or SQLite failure. Holds the
    /// connection mutex only for the query.
    pub(super) fn children(
        &self,
        parent: InodeId,
    ) -> Result<Vec<StoredInode>, SessionError>;

    /// Creates the complete remote child set for directory `parent`.
    ///
    /// `expected_digest` identifies the directory bytes decoded by the caller;
    /// `children` contains the complete validated child set without allocated
    /// IDs. Returns the resulting complete stored child set. Returns
    /// `SessionError` if the parent is absent, not a directory, has another
    /// digest, any child is invalid, reconciliation fails, or SQLite fails.
    /// Commits reconciliation and `directory_loaded = true` in one transaction;
    /// a repeated call for the already-loaded digest performs no inserts.
    pub(super) fn create_directory_children(
        &self,
        parent: InodeId,
        expected_digest: &Digest,
        children: &[NewStoredInode],
    ) -> Result<Vec<StoredInode>, SessionError>;

    /// Durably changes the singleton lifecycle from active to closed.
    ///
    /// Returns `()` after commit. Returns `SessionError` for a non-active
    /// lifecycle, invalid clock value, synchronization failure, or SQLite
    /// failure. Records the close timestamp in the same transaction.
    pub(super) fn close(&self) -> Result<(), SessionError>;
}
```

### Schema changes

`session_metadata` and `PRAGMA user_version` retain the definitions in
`technical-design.md`. Replace `inodes` and its indexes with:

```sql
CREATE TABLE IF NOT EXISTS inodes (
    inode                   INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_inode            INTEGER,
    name                    TEXT NOT NULL,
    kind                    TEXT NOT NULL,
    mode                    INTEGER,
    mtime_seconds           INTEGER,
    mtime_nanos             INTEGER,
    tombstone               INTEGER NOT NULL,
    file_remote_digest      TEXT,
    file_overlay_path       TEXT,
    file_content_dirty      INTEGER,
    symlink_target          TEXT,
    directory_remote_digest TEXT,
    directory_loaded        INTEGER,
    directory_tree_dirty    INTEGER,
    FOREIGN KEY (parent_inode) REFERENCES inodes(inode) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_inodes_parent_name
    ON inodes(parent_inode, name);

CREATE INDEX IF NOT EXISTS ix_inodes_parent
    ON inodes(parent_inode);
```

The private Rust decoder and write validator enforce:

- Root is inode `1`, has no parent, has an empty name, is a directory backed by
  the session root digest, and starts with `directory_loaded = false`.
- Non-root rows have a parent and a non-empty valid basename.
- File rows alone may set `file_*`; `file_content_dirty` is non-null for files.
  Overlay backing takes precedence when both file backing fields are present.
- Symlink rows alone set a non-null `symlink_target`.
- Directory rows alone may set `directory_*`; `directory_loaded` and
  `directory_tree_dirty` are non-null for directories. Remote directories start
  unloaded; local directories start loaded.
- Kind-specific columns are null for every other kind. Mtime seconds and nanos
  are either both null or both valid. SQLite integer booleans are exactly `0`
  or `1`.

Drop `directory_materializations`. Development databases are not migrated
because every mount creates a fresh session database.

### Unit tests

Tests call only the component API above; they do not open the connection,
execute SQL, invoke row decoders, or call transaction helpers directly.

- `create`, `inode`, and `inspect` expose a valid active session and unloaded
  remote root with inode `1`.
- `child` and `children` return lossless rows, distinguish absent mode and
  mtime, and order children by basename.
- `create_directory_children` covers file, directory, symlink, empty, repeated,
  and concurrent batches with stable allocated IDs.
- A wrong parent digest, invalid kind-specific field combination, duplicate
  basename, or invalid child causes no child or loaded-state change observable
  through `inode`, `child`, or `children`.
- `close` succeeds once, rejects a second transition, and makes `inspect`
  report the closed lifecycle.

## `FilesystemService`

### Changed behavior and dependencies

- Delegates namespace hydration and immutable reads directly to `Session`.
- Retains filesystem-policy methods, `SessionError` to `FilesystemError`
  mapping, and symlink-kind validation.
- Is concrete rather than generic. Its only stored dependency is `Arc<Session>`;
  it has no `BlobStore`, Tokio runtime, REAPI decoder, cache protocol, counters,
  or lock map.

### Target shape

```rust
/// Synchronous daemon filesystem policy over one mounted `Session`.
///
/// The service presents operations used by the FUSE adapter and adds
/// filesystem-specific validation and error context. Storage hydration and
/// operation coordination belong to `Session`.
pub struct FilesystemService {
    /// Mounted-workspace facade that executes every namespace and content read.
    session: Arc<Session>,
}
```

### Public API

```rust
impl FilesystemService {
    /// Creates a filesystem service and validates the mounted root directory.
    ///
    /// `session` is the sole mounted-workspace facade used by the service.
    /// Returns the ready service. Returns `FilesystemError` if root hydration or
    /// validation fails. May download, cache, decode, and persist the root child
    /// set through `Session`.
    pub fn mount(session: Arc<Session>) -> Result<Self, FilesystemError>;

    /// Looks up `child_name` directly below directory `dir_inode`.
    ///
    /// Returns the child's `Inode`; returns a contextual `FilesystemError` for
    /// absence, wrong kind, hydration, or storage failure. May hydrate the
    /// directory through `Session`.
    pub fn lookup_dir_child(
        &self,
        dir_inode: InodeId,
        child_name: &str,
    ) -> Result<Inode, FilesystemError>;

    /// Lists every visible child of directory `inode` in basename order.
    ///
    /// Returns the complete child vector or a contextual `FilesystemError`.
    /// May hydrate the directory through `Session`.
    pub fn readdir(&self, inode: InodeId) -> Result<Vec<Inode>, FilesystemError>;

    /// Returns effective visible metadata for `inode`.
    ///
    /// Returns `Inode` or a contextual `FilesystemError`. Has no hydration side
    /// effects.
    pub fn getattr(&self, inode: InodeId) -> Result<Inode, FilesystemError>;

    /// Returns the exact target of symbolic-link `inode`.
    ///
    /// Returns the target string. Returns `FilesystemError` when the inode is
    /// absent, not a symlink, malformed, or unreadable. Has no hydration side
    /// effects.
    pub fn readlink(&self, inode: InodeId) -> Result<String, FilesystemError>;

    /// Reads at most `size` bytes from file `inode` starting at `offset`.
    ///
    /// Returns available bytes or a contextual `FilesystemError`. May cause
    /// `Session` to download and admit the complete immutable file object.
    pub fn read(
        &self,
        inode: InodeId,
        offset: u64,
        size: usize,
    ) -> Result<Bytes, FilesystemError>;

    /// Returns the session's point-in-time immutable-read counters.
    ///
    /// Returns `IoCounters`; it does not fail or change state.
    pub fn counters(&self) -> IoCounters;
}
```

### Unit tests

Tests call only the public API above.

- `mount` validates and hydrates the root or returns a contextual error.
- `lookup_dir_child`, `readdir`, and `getattr` preserve existing visible
  read-only filesystem behavior.
- `read` serves remote and cached ranges and preserves session error context.
- `readlink` returns exact targets and rejects non-symlinks.
- `counters` reflects downloads and cache hits caused through public filesystem
  operations.

## Removed and unchanged components

### `ActiveSession`

- Remove `ActiveSession`, `ActiveResources`, their layout, the optional-resource
  mutex, forwarding methods, and idempotent close behavior.
- Move the store, overlay, ownership lock, and active-session paths directly
  into `Session`. No unit tests remain for the removed component.

### `BlobCache` and `OverlayStore`

- No behavior or API changes. Both remain private to the session module.
- Existing component tests remain scoped to their own callable APIs; new
  hydration behavior is tested through `Session` and `FilesystemService`, not
  through cache paths, temporary files, or overlay implementation helpers.
