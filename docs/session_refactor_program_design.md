# Session Refactor Program Design

## Purpose

Make `Session` own the inode state for a mounted workspace. It downloads and
caches the backing blobs from CAS and creates overlays for edited content.

## Highlights

- **`Session`** — owns inode state, filesystem reads, directory loading, and
  overlays for edited content. It delegates blob downloads and caching to
  `CachedBlobStore`.
- **`BlobStore`** — supports shared, concurrent use through immutable method
  receivers so callers can store it as a boxed trait object.
- **`CachedBlobStore`** — owns the backing `BlobStore`, synchronous runtime
  bridge, digest download locks, verified cache writes, and complete or ranged
  reads from cached blobs.
- **`SessionStore`** — provides three inode reads and one atomic operation for
  creating a directory's children.
- **Inode schema** — separates file and directory digests, stores directory
  loading on the directory inode, and removes `directory_materializations`.
- **`FilesystemService`** — owns filesystem policy and error mapping and
  delegates directory loading and blob reads to `Session`.
- **`ActiveSession`** — is removed. `CachedBlobStore` and `OverlayStore` become
  private implementation dependencies of `Session`.

## `Session`

### Changed behavior and dependencies

- `lookup_child`, `list_directory`, and `read_range` return filesystem results.
- Directory loading reads the inode's `directory_remote_digest` through
  `CachedBlobStore`, decodes it, then asks `SessionStore` to commit the complete
  child set and `directory_loaded = true` atomically.
- File reads prefer `file_overlay_path`; otherwise they ask `CachedBlobStore`
  for a range of `file_remote_digest`. The store handles any required download
  and caching before returning the bytes.
- An inode lock serializes directory loading and reads for one inode.
  Digest-level download coordination is encapsulated by `CachedBlobStore` and
  is not part of `Session`'s inode state.
- `Session` keeps a strong reference to each inode lock for its lifetime. An
  operation clones that reference and releases the lock-map mutex before
  waiting on the inode lock. Storing the mutex directly in the map would require
  holding the lock-map guard while waiting, which would block access to every
  inode lock.
- `close` is a one-shot durable transition. It does not wait for operations or
  release the ownership lock; the daemon quiesces FUSE before calling it and
  drops `Session` after the close attempt.
- Depends on `SessionStore`, `CachedBlobStore`, `OverlayStore`, the REAPI
  directory decoder, and the session ownership lock. It has no direct
  `BlobStore`, Tokio runtime, cache-writer, or digest-lock dependency.
- `SessionError` gains source-preserving variants for cached remote reads and
  directory decoding. `CasError` and `TreeError` remain available through the
  error source chain.

### Target shape

`Inode`, `SessionInfo`, and their value types are unchanged and are not repeated.

```rust
/// Internal counters for blob download and cache stats.
struct InternalIoCounters {
    /// Directory objects downloaded from remote storage.
    directory_downloads: AtomicU64,
    /// Directory objects served from the shared cache, including coalesced hits.
    directory_cache_hits: AtomicU64,
    /// File objects downloaded from remote storage.
    file_downloads: AtomicU64,
    /// File objects served from the shared cache, including coalesced hits.
    file_cache_hits: AtomicU64,
}

/// Metrics for blob download and cache stats queryable on Session.
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
}

/// Session owns the inode state for the filesystem. This also includes
/// downloading and caching the backing blobs from CAS and creating overlays for
/// edited content.
pub struct Session {
    /// Digest of the root directory mounted by this session.
    root_digest: Digest,
    /// Mountpoint for this session where the directory represented by
    /// 'root_digest' is mounted.
    mountpoint: PathBuf,
    /// Fixed cache, database, overlay, log, socket, and lock paths.
    layout: SessionLayout,
    /// Persistent storage for session and inode metadata state.
    store: SessionStore,
    /// Downloads backing blobs from CAS and stores verified blobs in the cache.
    cached_blob_store: CachedBlobStore,
    /// Storage for edited regular-file content.
    overlay: OverlayStore,
    /// Stable advisory lock held until this `Session` is dropped.
    session_lock: SessionLock,
    /// Per-inode locks for directory loading and read operations.
    inode_locks: Mutex<HashMap<InodeId, Arc<Mutex<()>>>>,
    /// File and directory counters updated from `CachedBlobStore` read outcomes.
    counters: InternalIoCounters,
}
```

### Public API

```rust
impl Session {
    /// Opens a fresh writable session for `root_digest` at `mountpoint`.
    ///
    /// `config` identifies `RFS_HOME`; `blob_store` is the remote
    /// client used by the private read-through cache; `runtime` executes that
    /// cache's asynchronous backing-store calls.
    ///
    /// Returns the exclusively owned session. Returns `SessionError` when the
    /// mountpoint, home, lock, layout, cache, overlay, or database cannot be
    /// initialized. Acquires `session.lock`, replaces the prior session tree,
    /// creates fresh durable state, and retains the shared cache.
    pub fn open(
        config: Config,
        root_digest: Digest,
        mountpoint: impl AsRef<Path>,
        blob_store: Box<dyn BlobStore>,
        runtime: Handle,
    ) -> Result<Self, SessionError>;

    /// Returns session identity and daemon paths established by `open`.
    ///
    /// Returns a new `SessionInfo` value. This operation has no I/O or side
    /// effects.
    pub fn info(&self) -> SessionInfo;

    /// Returns the UDS socket the daemon listens for the control API from the
    /// given daemon config.
    ///
    /// TODO: Determining the session layout should probably be refactored into
    /// `Config`.
    ///
    /// Returns the socket path. Returns `SessionError` if `RFS_HOME` cannot be
    /// inspected safely. Does not create or modify state.
    pub fn control_endpoint(config: &Config) -> Result<PathBuf, SessionError>;

    /// Inspects retained session metadata under the `RFS_HOME` in `config`.
    ///
    /// Returns `Some(SessionInfo)` for valid retained state and `None` when the
    /// home exists without a session. Returns `SessionError` for a missing home,
    /// unsupported schema, invalid metadata, or I/O failure. Reads session state
    /// without locking, repairing, creating, or modifying it.
    pub fn inspect(config: &Config) -> Result<Option<SessionInfo>, SessionError>;

    /// Returns the visible inode identified by `inode`.
    ///
    /// Returns effective `Inode` metadata. Returns `UnknownInode` for a missing
    /// or tombstoned row and `SessionError` for invalid durable state or storage
    /// failure. Performs no remote I/O and has no side effects.
    pub fn get_inode(&self, inode: InodeId) -> Result<Inode, SessionError>;

    /// Resolves `name` as a direct visible child of directory `parent`.
    ///
    /// Returns the child's effective `Inode`. Returns `NotFound` after the
    /// complete child set is known, `WrongKind` for a non-directory parent, or
    /// `SessionError` for download, decode, integrity, synchronization, or
    /// storage failures. May read through `CachedBlobStore`, update counters,
    /// and atomically store the parent's children.
    pub fn lookup_child(
        &self,
        parent: InodeId,
        name: &str,
    ) -> Result<Inode, SessionError>;

    /// Returns every visible direct child of directory `inode` in basename order.
    ///
    /// Returns a complete `Vec<Inode>`. Returns `WrongKind` for a non-directory
    /// inode or `SessionError` for download, decode, integrity,
    /// synchronization, or storage failures. May perform the same directory
    /// loading and durable child update as `lookup_child`.
    pub fn list_directory(
        &self,
        inode: InodeId,
    ) -> Result<Vec<Inode>, SessionError>;

    /// Reads at most `size` bytes from regular-file `inode` starting at `offset`.
    ///
    /// Returns available bytes and returns empty bytes when `offset` is at or
    /// beyond EOF. Returns `WrongKind`, `UnknownInode`, or `SessionError` for
    /// remote, integrity, synchronization, overlay, cache, or storage failures.
    /// Overlay bytes take precedence; a remote read through `CachedBlobStore`
    /// may download and cache the complete blob before serving the range.
    pub fn read_range(
        &self,
        inode: InodeId,
        offset: u64,
        size: usize,
    ) -> Result<Bytes, SessionError>;

    /// Returns the current blob download and cache metrics for this session.
    ///
    /// The returned counters may become stale immediately under concurrent I/O.
    /// Reading the counters has no side effects.
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
`start_blob_download`, `finalize_blob`, and `remote_file_digest`.

### Unit tests

All tests construct a `Session` with a fake backing `BlobStore` and call only
the API above.

- `get_inode` returns effective metadata and rejects a missing inode.
- `lookup_child` and `list_directory` load an unloaded directory, handle an empty
  directory, preserve stable inode allocation, and return the same visible set
  under concurrent calls.
- A malformed or digest-mismatched directory fails without making partial
  children visible through `lookup_child` or `list_directory`.
- `read_range` covers remote and cached content, overlay precedence, and EOF.
- `io_counters` distinguishes downloads from initial and coalesced cache hits.
- `close` commits one active-to-closed transition, rejects a second call, and
  leaves retained metadata inspectable after `Session` is dropped.
- `info`, `control_endpoint`, and `inspect` retain their documented read-only
  behavior.

## `BlobStore`

### Changed behavior and dependencies

- Every operation takes `&self`, allowing one boxed store to serve concurrent
  calls without a caller-owned factory or mutex.
- An implementation whose transport client requires mutable access clones that
  client inside the operation before awaiting transport work.
- The trait requires `Send + Sync` so `CachedBlobStore` can use the boxed store
  from concurrent filesystem calls.
- The trait remains independent of session, cache, inode, overlay, FUSE, and
  filesystem-service types.

### Target shape

`BlobStore` remains a trait. No supporting public structs are added.

### Public API

```rust
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Finds which requested digests are absent from remote storage.
    ///
    /// `digests` contains the objects to check. Returns the missing subset.
    /// Returns `CasError` for invalid digests, transport failures, authorization
    /// failures, or invalid responses. Performs remote I/O without changing
    /// stored objects.
    async fn find_missing_blobs(
        &self,
        digests: &[Digest],
    ) -> Result<Vec<Digest>, CasError>;

    /// Uploads objects to remote storage.
    ///
    /// `blobs` contains bytes paired with their expected digests. Returns upload
    /// statistics. Returns `CasError` for validation, transport, authorization,
    /// or response failures. May add missing objects to remote storage.
    async fn upload_blobs(&self, blobs: Vec<Blob>) -> Result<UploadStats, CasError>;

    /// Streams one verified remote object into `destination`.
    ///
    /// `digest` identifies the expected object and `destination` receives its
    /// bytes. Returns `()` after the complete verified object is written.
    /// Returns `CasError` for transport, authorization, absence, write, size,
    /// hash, or response failures. Performs remote I/O and writes incrementally
    /// to `destination`.
    async fn stream_blob(
        &self,
        digest: &Digest,
        destination: &mut (dyn Write + Send),
    ) -> Result<(), CasError>;

    /// Downloads one verified remote object into memory.
    ///
    /// `digest` identifies the expected object. Returns its verified bytes.
    /// Returns the same errors as `stream_blob`. Performs remote I/O and
    /// allocates enough memory to collect the complete object.
    async fn download_blob(&self, digest: &Digest) -> Result<Bytes, CasError>;
}
```

`download_blob` remains a collecting convenience implemented through
`stream_blob`.

### Unit tests

- A shared boxed fake store serves concurrent stream calls without serializing
  different digests.
- `download_blob` collects the bytes produced by `stream_blob` and preserves
  its errors.
- A transport-backed implementation can clone its internal client for each
  operation without requiring callers to clone the trait object.

## `CachedBlobStore`

### Changed behavior and dependencies

- Presents one interface for reading blobs from the shared verified cache or a
  remote `BlobStore`.
- A complete or ranged read first checks the local cache. On a miss,
  it acquires the digest-specific lock, rechecks the cache, streams the complete
  object from the shared backing store into a shard-local temporary,
  verifies its size and SHA-256 digest, syncs it, atomically adds it to the cache
  without overwriting an existing entry, and then serves the requested bytes
  locally.
- Each read returns `(Bytes, bool)`. The boolean is `true` only when that call
  streamed the object from the backing store; it is `false` for both an initial
  local hit and a coalesced hit observed after waiting for another call.
- The shared `BlobStore` supports concurrent calls, allowing different digests
  to download concurrently. The lock map owns a strong reference to each active
  download lock and removes the entry when the download finishes. A failed
  download or cache write releases the lock and leaves a later call free to
  retry.
- It owns the Tokio runtime bridge because asynchronous remote transfer is an
  implementation detail of satisfying its synchronous read API.
- `Session` uses the read boolean to classify file and directory downloads or
  cache hits.
- It knows only digest-addressed blob bytes. It has no dependency on
  inodes, directory decoding, `SessionStore`, overlays, FUSE, or whether an
  object contains file data or a serialized REAPI `Directory`.

### Target shape

```rust
/// Synchronous read-through store backed by the shared verified local cache.
pub(super) struct CachedBlobStore {
    /// Root of the digest-sharded shared cache.
    root: PathBuf,
    /// Shared remote store used to fill cache misses.
    blob_store: Box<dyn BlobStore>,
    /// Runtime used to execute asynchronous backing-store reads.
    runtime: Handle,
    /// Per-digest coordination that coalesces concurrent cache misses.
    download_locks: Mutex<HashMap<Digest, Arc<Mutex<()>>>>,
}
```

### Public API

```rust
impl CachedBlobStore {
    /// Opens the shared cache with `blob_store` as its remote backing store.
    ///
    /// Returns the ready cache. Returns `SessionError` when the cache root
    /// cannot be initialized.
    pub(super) fn open(
        root: PathBuf,
        blob_store: Box<dyn BlobStore>,
        runtime: Handle,
    ) -> Result<Self, SessionError>;

    /// Returns the complete verified object and whether this call downloaded it.
    ///
    /// A cache miss is handled internally and is never returned to the caller.
    pub(super) fn read_blob(
        &self,
        digest: &Digest,
    ) -> Result<(Bytes, bool), SessionError>;

    /// Returns a byte range and whether this call downloaded the complete object.
    ///
    /// Returns empty bytes when `offset` is at or beyond EOF. A cache miss is
    /// handled internally and is never returned to the caller.
    pub(super) fn read_range(
        &self,
        digest: &Digest,
        offset: u64,
        size: usize,
    ) -> Result<(Bytes, bool), SessionError>;

}
```

`exists`, the cache-miss `Option`, `BlobWriter`, temporary paths, download
start/finalize operations, and digest locks remain private implementation
details and are not part of the component API.

### Unit tests

- A first complete read downloads, caches, returns the expected bytes, and sets
  the boolean to `true`; the next read returns the same bytes and `false`.
- Ranged reads download the complete object on a miss, return only the requested
  bytes, handle EOF, and reuse the cached blob on later calls.
- Concurrent reads of one digest issue one remote stream; the downloader returns
  `true` and coalesced readers return `false`.
- Reads of different digests can stream concurrently.
- A malformed object, failed stream, or failed cache write exposes a
  source-preserving error, leaves no partial blob in the cache, and allows a
  later retry.
- Unfinished temporary files are removed, finalization is size- and
  hash-verifying, and the atomic cache write preserves an existing object.

## `SessionStore`

### Changed behavior and dependencies

- The store returns lossless `Inode` rows. `Session` alone filters
  tombstones and projects stored rows into effective public `Inode` values.
- `inode`, `child`, and `children` are the only inode reads. `children` includes
  tombstones so `Session` can reconcile the authoritative merged namespace.
- `create_directory_children` validates the entire input before its
  transaction, rechecks the parent inside the transaction, preserves
  authoritative overlay rows, creates the complete remote child set, and marks
  the parent loaded atomically. It ignores each input inode ID, rejects an input
  parent ID, and assigns the `parent` argument to inserted children.
- An empty child set still marks the parent loaded. A concurrent repeat for the
  loaded directory returns the stored complete set without allocating new
  inode IDs. A failed child write commits nothing.
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

/// Lossless inode values read from or supplied to the session store.
///
/// Rows read from storage have an inode identity. Directory-child inputs may
/// carry an inode identity, but the store ignores it and allocates a new one.
pub(super) struct Inode {
    /// Session-stable positive identity when read; ignored for child creation.
    pub(super) inode: Option<InodeId>,
    /// Parent identity when read; child creation requires this to be absent.
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
}
```

### Public API

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
    /// Returns `Some(Inode)` when present and `None` when absent. Returns
    /// `SessionError` for synchronization, decoding, validation, or SQLite
    /// failure. Holds the connection mutex only for the indexed query.
    pub(super) fn inode(
        &self,
        inode: InodeId,
    ) -> Result<Option<Inode>, SessionError>;

    /// Fetches the row named `name` directly below `parent`, including a tombstone.
    ///
    /// Returns `Some(Inode)` when present and `None` when absent. Returns
    /// `SessionError` when `name` is invalid or the query, decoding, or
    /// validation fails. Holds the connection mutex only for the indexed query.
    pub(super) fn child(
        &self,
        parent: InodeId,
        name: &str,
    ) -> Result<Option<Inode>, SessionError>;

    /// Fetches every row directly below `parent` in basename order.
    ///
    /// Returns lossless rows, including tombstones. Returns `SessionError` for
    /// synchronization, decoding, validation, or SQLite failure. Holds the
    /// connection mutex only for the query.
    pub(super) fn children(
        &self,
        parent: InodeId,
    ) -> Result<Vec<Inode>, SessionError>;

    /// Creates the complete remote child set for directory `parent`.
    ///
    /// `children` contains the complete decoded child set. The store ignores
    /// each child's `inode`, requires each child's `parent` to be `None`, and
    /// assigns `parent` to inserted rows. Returns the resulting complete stored
    /// child set. Returns `SessionError` if the parent is absent or not a
    /// directory, any child is invalid, reconciliation fails, or SQLite fails.
    /// Commits reconciliation and `directory_loaded = true` in one transaction;
    /// a repeated call for an already-loaded directory performs no inserts.
    pub(super) fn create_directory_children(
        &self,
        parent: InodeId,
        children: &[Inode],
    ) -> Result<Vec<Inode>, SessionError>;

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
-- See type Inode in crates/rfs-common/src/session/store.rs, which represents
-- each row in this table, for documentation on the table and its columns.
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
- Directory rows alone may set `directory_*`; `directory_loaded` is non-null
  for directories. Remote directories start unloaded; local directories start
  loaded.
- Kind-specific columns are null for every other kind. Mtime seconds and nanos
  are either both null or both valid. SQLite integer booleans are exactly `0`
  or `1`.

Drop `directory_materializations`. Development databases are not migrated
because every mount creates a fresh session database.

Directory tree-dirty tracking and traversal for snapshot upload are deferred to
the later snapshot program design.

### Unit tests

Tests call only the component API above; they do not open the connection,
execute SQL, invoke row decoders, or call transaction helpers directly.

- `create`, `inode`, and `inspect` expose a valid active session and unloaded
  remote root with inode `1`.
- `child` and `children` return lossless rows, distinguish absent mode and
  mtime, and order children by basename.
- `create_directory_children` covers file, directory, symlink, empty, repeated,
  and concurrent batches with stable allocated IDs.
- Child creation ignores supplied inode IDs and rejects supplied parent IDs.
- An invalid kind-specific field combination, duplicate basename, or invalid
  child causes no child or loaded-state change observable through `inode`,
  `child`, or `children`.
- `close` succeeds once, rejects a second transition, and makes `inspect`
  report the closed lifecycle.

## `FilesystemService`

### Changed behavior and dependencies

- Delegates directory loading and blob reads directly to `Session`.
- Owns filesystem-policy methods, `SessionError` to `FilesystemError` mapping,
  and symlink-kind validation.
- It is concrete. Its only stored dependency is `Arc<Session>`;
  it has no `BlobStore`, Tokio runtime, REAPI decoder, cache protocol, counters,
  or lock map.

### Target shape

```rust
/// Synchronous daemon filesystem policy over one mounted `Session`.
///
/// The service presents operations used by the FUSE adapter and adds
/// filesystem-specific validation and error context. Directory loading, blob
/// reads, and operation coordination belong to `Session`.
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
    /// Returns the ready service. Returns `FilesystemError` if loading or
    /// validating the root fails. May download, cache, decode, and store the
    /// root child set through `Session`.
    pub fn new(session: Arc<Session>) -> Result<Self, FilesystemError>;

    /// Looks up `child_name` directly below directory `dir_inode`.
    ///
    /// Returns the child's `Inode`; returns a contextual `FilesystemError` for
    /// absence, wrong kind, directory-loading, or storage failure.
    pub fn lookup_dir_child(
        &self,
        dir_inode: InodeId,
        child_name: &str,
    ) -> Result<Inode, FilesystemError>;

    /// Lists every visible child of directory `inode` in basename order.
    ///
    /// Returns the complete child vector or a contextual `FilesystemError`.
    pub fn readdir(&self, inode: InodeId) -> Result<Vec<Inode>, FilesystemError>;

    /// Returns effective visible metadata for `inode`.
    ///
    /// Returns `Inode` or a contextual `FilesystemError`. Does not load any
    /// directory.
    pub fn getattr(&self, inode: InodeId) -> Result<Inode, FilesystemError>;

    /// Returns the exact target of symbolic-link `inode`.
    ///
    /// Returns the target string. Returns `FilesystemError` when the inode is
    /// absent, not a symlink, malformed, or unreadable. Does not load any
    /// directory.
    pub fn readlink(&self, inode: InodeId) -> Result<String, FilesystemError>;

    /// Reads at most `size` bytes from file `inode` starting at `offset`.
    ///
    /// Returns available bytes or a contextual `FilesystemError`. May cause
    /// `Session`'s `CachedBlobStore` to download and cache the complete file
    /// blob.
    pub fn read(
        &self,
        inode: InodeId,
        offset: u64,
        size: usize,
    ) -> Result<Bytes, FilesystemError>;

    /// Returns the session's current blob download and cache metrics.
    ///
    /// Returns `IoCounters` without changing state.
    pub fn counters(&self) -> IoCounters;
}
```

### Unit tests

Tests call only the public API above.

- `mount` loads and validates the root or returns a contextual error.
- `lookup_dir_child`, `readdir`, and `getattr` provide visible read-only
  filesystem behavior.
- `read` serves remote and cached ranges and preserves session error context.
- `readlink` returns exact targets and rejects non-symlinks.
- `counters` reflects downloads and cache hits caused through public filesystem
  operations.

## Removed and retained components

### `ActiveSession`

- Remove `ActiveSession`, `ActiveResources`, their layout, the optional-resource
  mutex, forwarding methods, and idempotent close behavior.
- Move the store, overlay, ownership lock, and active-session paths directly
  into `Session`. No unit tests remain for the removed component.

### `OverlayStore`

- No behavior or API changes. It remains private to the session module.
- Existing component tests remain scoped to its callable API; new namespace
  directory-loading behavior is tested through `Session` and
  `FilesystemService`.
