# Step 6.1 Overlay Index and Merged View Program Design

## Purpose

Extend the session facade and its stores to represent namespace and metadata
mutations. File-content copy-up and FUSE mutation callbacks remain in steps 6.2
and 6.3.

## Highlights

- **`Session`** — adds create, remove, rename, and metadata-update operations
  that load affected remote directories before committing a mutation.
- **`SessionStore`** — adds atomic overlay mutations, tombstone precedence,
  dirty-directory propagation, and directory-digest invalidation.
- **`OverlayStore`** — atomically publishes empty local files and reports data
  files not referenced by the namespace.

## `Session`

### Changed behavior and dependencies

- Each mutation is synchronous and delegates its durable changes to one
  `SessionStore` transaction.
- Before a mutation inspects or changes a remote directory's metadata or child
  set, `Session` loads that directory through the existing path. CAS reads and
  decoding finish before the SQLite transaction starts.
- File creation publishes an empty overlay file before committing its inode.
  A failed commit can leave an unreferenced file, but cannot expose the name.
- Inode lookup and directory listing exclude tombstones. A local inode created
  at a tombstoned name is the visible inode.
- Metadata-only file changes retain `file_remote_digest`. Directory metadata
  changes clear that directory's digest after materialization. Child-set
  changes clear the changed directory and its dirty ancestors.
- Symlink replacement creates a new inode and invalidates the parent directory
  chain; symlinks have no standalone digest.
- Direct dependencies remain `SessionStore`, `OverlayStore`, `CachedBlobStore`,
  the REAPI directory decoder, and per-inode locks. No FUSE, snapshot, or
  writable-handle dependency is added.

### Target shape

Only new types and enum variants are shown. Existing `Session` fields and
public value types do not change.

```rust
/// Metadata values to apply to one visible inode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataUpdate {
    /// Replacement supported Unix mode, or `None` to retain the stored mode.
    pub mode: Option<u32>,
    /// Replacement modification time, or `None` to retain the stored mtime.
    pub mtime: Option<NodeTime>,
}

#[derive(Debug, Error)]
pub enum SessionError {
    // Existing variants are unchanged.

    /// A visible entry occupies the requested name.
    #[error("already exists: {reason}")]
    AlreadyExists {
        /// Parent and name identifying the conflict.
        reason: String,
    },
    /// Removal or replacement requires an empty directory.
    #[error("directory not empty: {reason}")]
    DirectoryNotEmpty {
        /// Directory that prevented the operation.
        reason: String,
    },
    /// A mutation argument violates namespace rules.
    #[error("invalid session argument: {reason}")]
    InvalidArgument {
        /// Invalid name, target, or move.
        reason: String,
    },
}
```

### Public API

Only new or changed methods are shown.

```rust
impl Session {
    // ... other methods unchanged ...

    /// Resolves visible `name` below `parent`.
    ///
    /// Returns the non-tombstoned child. Returns `NotFound` when no visible
    /// child exists, `NotDirectory` for the wrong parent kind, or a
    /// remote/decode/storage error. May materialize the parent's remote child
    /// set and update I/O counters.
    pub fn lookup_child(
        &self,
        parent: InodeId,
        name: &str,
    ) -> Result<Inode, SessionError>;

    /// Lists visible children of directory `inode` in basename order.
    ///
    /// Returns non-tombstoned children. Returns `NotDirectory` or a
    /// remote/decode/storage error. May materialize the directory and update
    /// I/O counters.
    pub fn list_directory(
        &self,
        inode: InodeId,
    ) -> Result<Vec<Inode>, SessionError>;

    /// Creates an empty local file named `name` below `parent`.
    ///
    /// `mode` and `mtime` initialize its metadata. Returns the new inode.
    /// Returns name, parent-kind, duplicate-name, overlay I/O, lifecycle,
    /// validation, or SQLite errors. Loads the parent, publishes an empty data
    /// file, then commits the inode and dirty-directory chain. A failed commit
    /// may leave an unreferenced data file.
    pub fn create_file(
        &self,
        parent: InodeId,
        name: &str,
        mode: u32,
        mtime: NodeTime,
    ) -> Result<Inode, SessionError>;

    /// Creates an empty local directory named `name` below `parent`.
    ///
    /// Returns a loaded, dirty directory inode. Returns name, parent-kind,
    /// duplicate-name, lifecycle, validation, or SQLite errors. Loads the
    /// parent, then commits the child and dirty-directory chain.
    pub fn create_directory(
        &self,
        parent: InodeId,
        name: &str,
        mode: u32,
        mtime: NodeTime,
    ) -> Result<Inode, SessionError>;

    /// Creates a symlink named `name` below `parent`.
    ///
    /// `target` is stored without being followed. Returns the new inode.
    /// Returns name, target, parent-kind, duplicate-name, lifecycle,
    /// validation, or SQLite errors. Loads the parent, then commits the target
    /// and dirty-directory chain. Creates no overlay file or digest.
    pub fn create_symlink(
        &self,
        parent: InodeId,
        name: &str,
        target: &str,
        mode: u32,
        mtime: NodeTime,
    ) -> Result<Inode, SessionError>;

    /// Applies `update` to visible `inode`.
    ///
    /// Returns the resulting inode. Returns visibility, validation, remote
    /// directory-loading, lifecycle, or SQLite errors. A directory is loaded
    /// before its digest is cleared. A file retains its content backing. An
    /// effective no-op writes nothing.
    pub fn set_metadata(
        &self,
        inode: InodeId,
        update: MetadataUpdate,
    ) -> Result<Inode, SessionError>;

    /// Removes non-directory `name` below `parent`.
    ///
    /// Returns the tombstoned inode. Returns `NotFound`, `IsDirectory`,
    /// parent-kind, lifecycle, validation, or SQLite errors. Loads the parent,
    /// then tombstones the child and invalidates the parent chain in one
    /// transaction. Does not remove CAS or overlay bytes.
    pub fn unlink(
        &self,
        parent: InodeId,
        name: &str,
    ) -> Result<Inode, SessionError>;

    /// Removes empty directory `name` below `parent`.
    ///
    /// Returns the tombstoned inode. Returns `NotFound`, `NotDirectory`,
    /// `DirectoryNotEmpty`, remote-loading, lifecycle, validation, or SQLite
    /// errors. Loads the parent and child, then tombstones the child and
    /// invalidates the parent chain in one transaction.
    pub fn remove_directory(
        &self,
        parent: InodeId,
        name: &str,
    ) -> Result<Inode, SessionError>;

    /// Moves a visible child while preserving its inode ID.
    ///
    /// Moves `source_name` below `source_parent` to `destination_name` below
    /// `destination_parent`. Returns the moved inode. Returns absence,
    /// incompatible replacement, non-empty destination, descendant-cycle,
    /// lifecycle, validation, or SQLite errors. Loads both parents and any
    /// directory replacement candidate before one transaction replaces the
    /// destination, moves the source, and invalidates both directory chains.
    /// The source content or subtree digest remains reusable.
    pub fn rename(
        &self,
        source_parent: InodeId,
        source_name: &str,
        destination_parent: InodeId,
        destination_name: &str,
    ) -> Result<Inode, SessionError>;
}
```

### Schema

`Session` owns no schema. `SessionStore` owns the changes below.

### Unit tests

Cases `S61-SES-001` through `S61-SES-019` in
[`step-6.1-test-cases.md`](step-6.1-test-cases.md) cover the public API. They do
not call stores or private helpers.

## `SessionStore`

### Changed behavior and dependencies

- Each namespace mutation validates the active lifecycle and commits all inode,
  tombstone, dirty-state, and digest changes in one transaction.
- `child` returns the non-tombstoned row for a parent/name pair.
  `inode` and `get_directory_children` remain lossless and may return
  tombstones.
- A partial unique index permits tombstoned history beside one visible inode at
  the same parent/name while rejecting two visible rows.
- Local files are overlay-backed and content-dirty; local directories are
  loaded and tree-dirty; symlinks store their target inline.
- Metadata updates compare effective values before writing. Directory changes
  invalidate self and ancestors; file and symlink changes invalidate the
  parent chain. File content backing is unchanged.
- Removal preserves the inode and backing data as a tombstone. Rename preserves
  the source inode ID, permits same-kind replacement, and tombstones the
  replaced inode. Kind, emptiness, and cycle checks precede writes in the same
  transaction.
- Dirty propagation walks parent directories to root, setting
  `directory_tree_dirty` and clearing `directory_remote_digest`. It rejects an
  unloaded directory in the dirty chain.
- Dependencies remain `rusqlite` and session domain types. No CAS, cache,
  REAPI, FUSE, or remote-download dependency is added.

### Target shape

Only new types and changed `Inode` fields are shown. `SessionStore` and
`SessionMetadata` fields do not change.

```rust
/// Description of a local inode before SQLite allocates its ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NewInode {
    /// UTF-8 basename below the supplied parent.
    pub(super) name: String,
    /// Supported Unix mode.
    pub(super) mode: u32,
    /// Modification time.
    pub(super) mtime: NodeTime,
    /// Kind-specific content representation.
    pub(super) content: NewInodeContent,
}

/// Kind-specific content for a local inode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum NewInodeContent {
    /// File bytes published in the overlay store.
    File {
        /// Overlay data-file identity.
        overlay_file: OverlayFileId,
    },
    /// Directory whose child set is empty and loaded.
    Directory,
    /// Symlink target stored inline.
    Symlink {
        /// UTF-8 target retained without resolution.
        target: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Inode {
    // Existing fields are unchanged except those below.

    /// Overlay data identity for local or copied-up files.
    pub(super) file_overlay_path: Option<OverlayFileId>,
    /// Whether snapshot must re-encode this directory; `None` for other kinds.
    pub(super) directory_tree_dirty: Option<bool>,
}
```

### Public API

Only new or changed methods are shown.

```rust
impl SessionStore {
    // ... other methods unchanged ...

    /// Creates a database for an active session with an unloaded remote root.
    ///
    /// Persists session identity, daemon PID, root digest, and mountpoint.
    /// Returns the store or a schema, clock, validation, constraint, or SQLite
    /// error. Installs schema version 2 and seeds metadata plus root atomically.
    pub(super) fn create(
        database_path: PathBuf,
        session_id: String,
        daemon_pid: u32,
        root_digest: Digest,
        mountpoint: PathBuf,
    ) -> Result<Self, SessionError>;

    /// Fetches visible `name` below `parent`.
    ///
    /// Returns the non-tombstoned inode or `None`. Returns name-validation,
    /// decode, invariant, synchronization, or SQLite errors. Performs no
    /// mutation.
    pub(super) fn child(
        &self,
        parent: InodeId,
        name: &str,
    ) -> Result<Option<Inode>, SessionError>;

    /// Fetches all child rows below `parent` in name and inode order.
    ///
    /// Returns visible and tombstoned rows. Returns synchronization, decode,
    /// validation, or SQLite errors. Performs no mutation.
    pub(super) fn get_directory_children(
        &self,
        parent: InodeId,
    ) -> Result<Vec<Inode>, SessionError>;

    /// Returns stored children or materializes one remote child set.
    ///
    /// `children` is the decoded child set of unloaded remote directory
    /// `parent`. Returns allocated rows in visible basename order. Returns
    /// parent, child-set, validation, or SQLite errors. Inserts all children
    /// and marks the parent loaded in one transaction; a repeated call returns
    /// stored rows.
    pub(super) fn get_or_create_remote_dir_children(
        &self,
        parent: InodeId,
        children: &[Inode],
    ) -> Result<Vec<Inode>, SessionError>;

    /// Creates `child` below a loaded directory.
    ///
    /// Returns the allocated inode. Returns parent, duplicate-name, lifecycle,
    /// input-validation, dirty-chain, or SQLite errors. Inserts kind-specific
    /// state and invalidates the parent chain in one transaction. A new
    /// directory is loaded and tree-dirty.
    pub(super) fn create_local_child(
        &self,
        parent: InodeId,
        child: NewInode,
    ) -> Result<Inode, SessionError>;

    /// Applies `update` to a visible inode.
    ///
    /// Returns the resulting inode. Returns visibility, lifecycle, validation,
    /// unloaded-directory, dirty-chain, or SQLite errors. A changed directory
    /// invalidates self and ancestors; a changed file or symlink invalidates
    /// the parent chain. File backing is preserved. A no-op writes nothing.
    pub(super) fn update_metadata(
        &self,
        inode: InodeId,
        update: MetadataUpdate,
    ) -> Result<Inode, SessionError>;

    /// Tombstones visible `name` after checking its kind.
    ///
    /// `expect_directory` selects rmdir when true and unlink when false.
    /// Returns the tombstoned inode. Returns absence, wrong-kind, non-empty,
    /// lifecycle, dirty-chain, or SQLite errors. The tombstone and parent-chain
    /// invalidation commit together.
    pub(super) fn tombstone_child(
        &self,
        parent: InodeId,
        name: &str,
        expect_directory: bool,
    ) -> Result<Inode, SessionError>;

    /// Moves a visible child and may replace a same-kind destination.
    ///
    /// Returns the moved inode with its ID unchanged. Returns
    /// source/destination absence, wrong-kind, non-empty destination,
    /// descendant-cycle, lifecycle, dirty-chain, or SQLite errors. A directory
    /// destination must be loaded. Destination tombstoning, source movement,
    /// and both dirty chains commit together. Rename alone retains the moved
    /// inode's content or directory digest.
    pub(super) fn rename_child(
        &self,
        source_parent: InodeId,
        source_name: &str,
        destination_parent: InodeId,
        destination_name: &str,
    ) -> Result<Inode, SessionError>;

    /// Returns overlay identities referenced by inode rows.
    ///
    /// Includes tombstoned rows because open handles may use their bytes.
    /// Returns synchronization, decode, validation, or SQLite errors. Performs
    /// no mutation.
    pub(super) fn referenced_overlay_files(
        &self,
    ) -> Result<HashSet<OverlayFileId>, SessionError>;
}
```

### Schema

Schema version becomes 2. Each mount creates a fresh database, so there is no
writable migration. Retained version-1 databases are unsupported diagnostic
state.

Only the changed baseline SQL is shown:

```sql
-- Added to CREATE TABLE inodes.
directory_tree_dirty INTEGER,

-- Replaces uq_inodes_parent_name.
CREATE UNIQUE INDEX IF NOT EXISTS uq_inodes_visible_parent_name
    ON inodes(parent_id, name)
    WHERE tombstone = 0;

-- Replaces the previous parent-only index definition.
CREATE INDEX IF NOT EXISTS ix_inodes_parent
    ON inodes(parent_id, name, id);
```

The embedded baseline drops `uq_inodes_parent_name`; no table or column is
renamed. The row decoder and write validator add these rules:

- `directory_tree_dirty` is `0` or `1` for directories and null for other
  kinds.
- An unloaded directory has a remote digest and is clean. A digest-free
  directory is loaded and dirty.
- Tombstones retain kind-specific backing. At most one non-tombstoned row has
  a parent/name pair.
- Dirty propagation accepts only loaded directories and clears each affected
  `directory_remote_digest` while setting `directory_tree_dirty = 1`.

Other schema and validation rules remain as specified in
[`technical-design.md`](technical-design.md).

### Unit tests

Cases `S61-STO-001` through `S61-STO-019` in
[`step-6.1-test-cases.md`](step-6.1-test-cases.md) cover the component API. They
do not use raw connections, SQL, decoders, or transaction helpers.

## `OverlayStore`

### Changed behavior and dependencies

- `create_empty_file` creates and syncs a temporary file, then publishes it as
  a UUID-named entry in `data/` without replacement.
- Reads accept only validated `OverlayFileId` values.
- `unreferenced_files` compares store references with regular files directly
  below `data/`. It ignores `tmp/` and deletes nothing.
- Dependencies add UUID generation. The component otherwise depends on local
  filesystem APIs, `Bytes`, and `SessionError`; it has no SQLite, CAS, cache,
  inode, or FUSE dependency.

### Target shape

`OverlayStore` fields do not change. This type is new:

```rust
/// Relative identity of a file beneath the overlay data directory.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct OverlayFileId {
    /// One validated UTF-8 path component stored in SQLite.
    relative_path: PathBuf,
}
```

### Public API

Only new or changed methods are shown.

```rust
impl OverlayFileId {
    /// Validates an identity decoded from durable state.
    ///
    /// Returns an opaque identity when `path` is one relative normal
    /// component. Returns `SessionError` for empty, absolute, multi-component,
    /// parent, or non-UTF-8 values. Performs no I/O.
    pub(super) fn from_stored(
        path: PathBuf,
    ) -> Result<Self, SessionError>;

    /// Returns the path component used for SQLite encoding.
    ///
    /// Performs no I/O or mutation.
    pub(super) fn as_path(&self) -> &Path;
}

impl OverlayStore {
    /// Opens the overlay rooted at `root`.
    ///
    /// Returns the store or a local filesystem error. Creates `root`, `data/`,
    /// and `tmp/` when absent. Narrows the existing method to module-private
    /// visibility.
    pub(super) fn open(root: PathBuf) -> Result<Self, SessionError>;

    // ... other methods unchanged ...

    /// Publishes an empty data file.
    ///
    /// Returns its `OverlayFileId`. Returns create, permission, sync, or rename
    /// errors. Creates and syncs a temporary, renames it into `data/` without
    /// replacement, and removes the temporary on failure when possible.
    pub(super) fn create_empty_file(
        &self,
    ) -> Result<OverlayFileId, SessionError>;

    /// Reads at most `size` bytes from `file` at `offset`.
    ///
    /// Returns available bytes, including empty bytes at or after EOF. Returns
    /// local open, seek, or read errors. Reads only beneath `data/` and changes
    /// no content.
    pub(super) fn read_range(
        &self,
        file: &OverlayFileId,
        offset: u64,
        size: usize,
    ) -> Result<Bytes, SessionError>;

    /// Finds published files absent from `referenced`.
    ///
    /// Returns validated identities in sorted order. Returns directory-read,
    /// entry-type, name-validation, or metadata errors. Reads direct `data/`
    /// entries and deletes nothing.
    pub(super) fn unreferenced_files(
        &self,
        referenced: &HashSet<OverlayFileId>,
    ) -> Result<Vec<OverlayFileId>, SessionError>;
}
```

### Schema

`OverlayStore` owns no schema. `OverlayFileId` is stored in
`SessionStore.file_overlay_path` as one UTF-8 path component.

### Unit tests

Cases `S61-OVL-001` through `S61-OVL-007` in
[`step-6.1-test-cases.md`](step-6.1-test-cases.md) cover the component API.

## Removed and unchanged components

### Removed components

No component is removed.

### `CachedBlobStore`

No API or behavior changes. `Session` uses it to materialize remote directories
and read unchanged remote file content.

### `FilesystemService` and FUSE adapter

No API or behavior changes. They remain read-only until step 6.3.

### File-content copy-up

Step 6.2 adds the remote-to-overlay content transition. Step 6.1 does not
modify existing file bytes.

### Snapshot traversal

No snapshot API is added. Step 7.1 consumes the merged rows and dirty state
defined here.
