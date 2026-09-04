# Step 6.1.4 Create an Empty File Program Design

## Purpose

Add `Session::create_file`, including the durable dirty-directory foundation
and atomic publication of an empty overlay data file. This slice creates files
only; directories and symlinks follow separately.

## Highlights

- **`Session`** — adds empty-file creation and the two creation error
  categories.
- **`SessionStore`** — installs schema version 2, creates a local file in one
  transaction, invalidates its loaded ancestor chain, and reports referenced
  overlay identities.
- **`OverlayStore`** — introduces validated overlay identities, atomic empty
  file publication, and non-destructive orphan discovery.
- **`FilesystemService`** — adds `AlreadyExists` and begins mapping the
  existing filesystem `InvalidArgument` variant from `SessionError`.
- **FUSE adapter** — adds the `EEXIST` mapping; its existing `EINVAL` mapping
  now also covers session-originated invalid arguments.

## `Session`

### Changed behavior and dependencies

- Load the parent directory before starting the mutation.
- Publish and sync an empty overlay file before committing namespace state.
- Expose the name only after one successful `SessionStore` transaction.
- A failed SQLite commit may leave an unreferenced data file but cannot expose
  a partial inode.
- Add `AlreadyExists` and `InvalidArgument` to `SessionError`; all prior error
  mappings remain unchanged.
- Dependencies remain the owned stores, cache, decoder, and per-inode locks.

### Target shape

Existing `Session` fields do not change. Add only these variants to the enum
established in 6.1.1:

```rust
pub enum SessionError {
    // ... variants from 6.1.1 unchanged ...
    /// A visible entry occupies the requested name.
    #[error("already exists: {reason}")]
    AlreadyExists {
        /// Parent and name identifying the conflict.
        reason: String,
    },
    /// A creation argument violates namespace or metadata rules.
    #[error("invalid session argument: {reason}")]
    InvalidArgument {
        /// Invalid name, mode, or modification time.
        reason: String,
    },
}
```

### Public API

```rust
impl Session {
    // ... other methods unchanged ...

    /// Creates an empty local file named `name` below `parent`.
    ///
    /// `mode` and `mtime` initialize supported metadata. Returns the new
    /// visible inode. Returns invalid-argument, wrong-parent-kind,
    /// already-exists, lifecycle, overlay I/O, validation, synchronization, or
    /// SQLite errors. It loads the parent, publishes an empty overlay file,
    /// then commits the inode and dirty ancestor chain. A failed commit may
    /// leave an unreferenced data file.
    pub fn create_file(
        &self,
        parent: InodeId,
        name: &str,
        mode: u32,
        mtime: NodeTime,
    ) -> Result<Inode, SessionError>;
}
```

### Schema

`Session` owns no schema. `SessionStore` owns the version-2 change below.

### Unit tests

- Create below an unloaded remote root; expect one parent materialization, a
  zero-size regular-file inode with exact metadata, and stable lookup/listing.
- Read the returned inode and receive empty bytes without a file CAS request.
- Race two creates for one name; expect one success, one `AlreadyExists`, and
  one visible inode.
- Close a session before creation; expect `FailedPreconditionError` and no
  visible name or published referenced file.

### Integration tests

- Inject a SQLite commit failure after overlay publication. Expect an error,
  an absent lookup/listing name, unchanged parent metadata, and the published
  identity reported by orphan discovery.

## `SessionStore`

### Changed behavior and dependencies

- Create a local file and invalidate every loaded directory from parent to root
  in one transaction.
- Reject an unloaded directory in the dirty chain without changing state.
- Permit tombstoned history beside one visible row through a partial unique
  index; a second visible row is rejected.
- Keep local files overlay-backed, remote-digest-free, and content-dirty.
- Include tombstoned rows in `referenced_overlay_files` for future open-handle
  semantics.
- Dependencies remain `rusqlite` and session domain types.

### Target shape

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
    /// File content representation.
    pub(super) content: NewInodeContent,
}

/// Kind-specific content accepted in this slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum NewInodeContent {
    /// File bytes published in the overlay store.
    File {
        /// Overlay data-file identity.
        overlay_file: OverlayFileId,
    },
}

pub(super) struct Inode {
    // ... existing fields unchanged ...
    /// Overlay data identity for local files.
    ///
    /// Changes (DO NOT INCLUDE IN IMPLEMENTATION): The raw path becomes a
    /// validated opaque identity.
    pub(super) file_overlay_path: Option<OverlayFileId>,
    /// Whether snapshot must re-encode this directory; `None` otherwise.
    ///
    /// Changes (DO NOT INCLUDE IN IMPLEMENTATION): This version-2 field now
    /// records ancestor invalidation.
    pub(super) directory_tree_dirty: Option<bool>,
}
```

### Public API

```rust
impl SessionStore {
    // ... other methods unchanged ...

    // Changes (DO NOT INCLUDE IN IMPLEMENTATION): New stores install schema
    // version 2 and seed a clean root dirty flag.
    /// Creates active session state with an unloaded remote root.
    ///
    /// Returns the store. Returns schema, clock, validation, constraint, or
    /// SQLite errors. Installs version 2 and seeds metadata and root atomically.
    pub(super) fn create(
        database_path: PathBuf,
        session_id: String,
        daemon_pid: u32,
        root_digest: Digest,
        mountpoint: PathBuf,
    ) -> Result<Self, SessionError>;

    /// Creates `child` below a loaded directory.
    ///
    /// Returns the allocated file inode. Returns wrong-parent-kind,
    /// already-exists, invalid-input, lifecycle, unloaded-dirty-chain,
    /// synchronization, or SQLite errors. The insert and complete ancestor
    /// invalidation commit together.
    pub(super) fn create_local_child(
        &self,
        parent: InodeId,
        child: NewInode,
    ) -> Result<Inode, SessionError>;

    /// Returns overlay identities referenced by all inode rows.
    ///
    /// Includes tombstones. Returns synchronization, row-validation, or SQLite
    /// errors and performs no mutation.
    pub(super) fn referenced_overlay_files(
        &self,
    ) -> Result<HashSet<OverlayFileId>, SessionError>;
}
```

### Schema

New writable sessions use version 2. Every mount creates a fresh database, so
there is no writable migration; retained version-1 state remains unsupported
diagnostic state.

```sql
-- Added to CREATE TABLE inodes.
-- Changes (DO NOT INCLUDE IN IMPLEMENTATION): Tracks whether snapshot must
-- re-encode a directory after it or a descendant changes.
directory_tree_dirty INTEGER,

-- Changes (DO NOT INCLUDE IN IMPLEMENTATION): Replaces the unique index that
-- also included tombstones.
CREATE UNIQUE INDEX IF NOT EXISTS uq_inodes_visible_parent_name
    ON inodes(parent_id, name)
    WHERE tombstone = 0;

-- Changes (DO NOT INCLUDE IN IMPLEMENTATION): Adds deterministic inode-ID
-- ordering for historical rows sharing a name.
CREATE INDEX IF NOT EXISTS ix_inodes_parent
    ON inodes(parent_id, name, id);
```

The baseline removes `uq_inodes_parent_name`. Rust row decoding and writes
enforce:

- `directory_tree_dirty` is `0` or `1` for directories and null otherwise.
- An unloaded directory has a remote digest and is clean.
- A digest-free directory is loaded and dirty.
- At most one non-tombstoned row uses a parent/name pair.
- Dirty propagation accepts only loaded directories, clears each affected
  directory digest, and sets each affected dirty flag.

### Unit tests

- Version-2 root decoding returns inode 1, an unloaded clean root, and null
  non-directory fields.
- Materializing one remote file, directory, and symlink initializes clean,
  kind-correct version-2 state, including a clean directory tree flag.
- Local file creation records the overlay identity, no remote digest, and dirty
  content state.
- A three-level create clears and dirties the complete ancestor chain but not a
  sibling branch.
- An unloaded dirty ancestor rejects creation atomically.
- Two visible rows are rejected without consuming observable inode state.
- `referenced_overlay_files` returns every referenced local file identity.
- Concurrent creates serialize complete transactions and allocate unique IDs.

### Integration tests

- Covered by the `Session` commit-failure case and successful create workflow
  above.

## `OverlayStore`

### Changed behavior and dependencies

- Publish a synced empty temporary file as a UUID-named `data/` entry without
  replacement.
- Accept only a validated, one-component `OverlayFileId` for reads.
- Find sorted unreferenced regular files directly below `data/`; ignore `tmp/`
  and delete nothing.
- Add UUID generation; do not add SQLite, CAS, inode, or FUSE dependencies.

### Target shape

```rust
/// Relative identity of a file beneath the overlay data directory.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct OverlayFileId {
    /// One validated UTF-8 path component stored in SQLite.
    relative_path: PathBuf,
}
```

`OverlayStore` fields do not change.

### Public API

```rust
impl OverlayFileId {
    /// Validates an identity decoded from durable state.
    ///
    /// Returns an opaque identity. Returns a validation `SessionError` for an
    /// empty, absolute, multi-component, parent, or non-UTF-8 value. Performs
    /// no I/O.
    pub(super) fn from_stored(path: PathBuf) -> Result<Self, SessionError>;

    /// Returns the validated component used for SQLite encoding.
    ///
    /// Performs no I/O or mutation.
    pub(super) fn as_path(&self) -> &Path;
}

impl OverlayStore {
    // ... other methods unchanged ...

    // Changes (DO NOT INCLUDE IN IMPLEMENTATION): Visibility narrows from
    // public to module-private; behavior is unchanged.
    /// Opens the overlay rooted at `root`.
    ///
    /// Returns the store or a contextual local filesystem error. Creates
    /// `root`, `data/`, and `tmp/` when absent.
    pub(super) fn open(root: PathBuf) -> Result<Self, SessionError>;

    /// Publishes a synced empty data file without replacement.
    ///
    /// Returns its identity. Returns contextual create, permission, sync, or
    /// rename errors. Best-effort removes its temporary after failure.
    pub(super) fn create_empty_file(&self) -> Result<OverlayFileId, SessionError>;

    // Changes (DO NOT INCLUDE IN IMPLEMENTATION): Accepts an opaque identity
    // instead of a raw path.
    /// Reads at most `size` bytes at `offset` from `file`.
    ///
    /// Returns available bytes, including empty bytes at or beyond EOF.
    /// Returns contextual local open, seek, or read errors and changes nothing.
    pub(super) fn read_range(
        &self,
        file: &OverlayFileId,
        offset: u64,
        size: usize,
    ) -> Result<Bytes, SessionError>;

    /// Finds published files absent from `referenced`.
    ///
    /// Returns sorted validated identities. Returns directory, entry-type,
    /// metadata, or name-validation errors. Deletes nothing.
    pub(super) fn unreferenced_files(
        &self,
        referenced: &HashSet<OverlayFileId>,
    ) -> Result<Vec<OverlayFileId>, SessionError>;
}
```

### Schema

The identity is encoded in the existing `inodes.file_overlay_path` column as
one UTF-8 path component.

### Unit tests

- Opening twice leaves usable empty `data/` and `tmp/` directories.
- Empty publication is readable at zero and beyond EOF and leaves no temporary.
- Sequential and concurrent publications never collide or replace data.
- Reject empty, absolute, parent, nested, and non-UTF-8 stored paths before I/O.
- Orphan discovery is sorted, non-destructive, and excludes temporary files.
- Inject create, sync, and rename failures; expect no visible partial data file
  and best-effort temporary cleanup.

### Integration tests

- Covered by the failed-create orphan integration case in the `Session`
  section.

## `FilesystemService`

### Changed behavior and dependencies

- Add `FilesystemError::AlreadyExists` for the new session category.
- Map the new `SessionError::InvalidArgument` to the already existing
  filesystem-owned `FilesystemError::InvalidArgument`.
- No service method or dependency is added.

### Target shape

```rust
pub enum FilesystemError {
    // ... variants from 6.1.1 unchanged ...
    /// A visible name conflict; maps to `EEXIST`.
    AlreadyExists {
        /// Parent and name identifying the conflict.
        reason: String,
    },
    // Existing filesystem-only `InvalidArgument` is unchanged in shape.
}
```

### Public API

`From<SessionError>` gains exhaustive arms for both new session variants. Other
service APIs are unchanged.

### Schema

No schema change.

### Unit tests

- Assert exact conversion for both new session variants, including when nested
  in `Context`.

### Integration tests

- The `Session` integration test proves both domain categories at the session
  boundary.

## FUSE adapter

### Changed behavior and dependencies

- Map `AlreadyExists` to `EEXIST`.
- Keep the existing `InvalidArgument` to `EINVAL` arm; it now also receives
  session-originated failures.
- No callback or dependency changes.

### Target shape

No struct changes.

### Public API

The private errno mapper keeps its signature and gains the `AlreadyExists`
arm.

### Schema

No schema change.

### Unit tests

- Assert `EEXIST` for direct and context-wrapped `AlreadyExists` and retain the
  existing `EINVAL` assertion for `InvalidArgument`.

### Integration tests

- No writable FUSE integration is added before Step 6.3.

## Removed and unchanged components

`CachedBlobStore` is unchanged. Directory creation, symlink creation, metadata
updates, unlink, directory removal, rename, and file-content mutation are not
implemented in this slice.
