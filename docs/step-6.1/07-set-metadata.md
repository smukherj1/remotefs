# Step 6.1.7 Update Inode Metadata Program Design

## Purpose

Add `Session::set_metadata` with kind-specific directory-digest invalidation
and no file-content copy-up.

## Highlights

- **`Session`** — adds a partial mode/mtime update and materializes a remote
  directory before clearing its reusable digest.
- **`SessionStore`** — applies effective metadata changes and dirty propagation
  atomically while making effective no-ops write nothing.

## `Session`

### Changed behavior and dependencies

- A directory is fully materialized before its metadata transaction.
- A file retains its remote digest or overlay identity because bytes do not
  change.
- A symlink retains its exact inline target.
- A changed directory invalidates itself and its ancestors; a changed file or
  symlink invalidates its parent and ancestors.
- An effective no-op performs no materialization beyond what was needed to
  identify the inode and no durable write.
- Reuse existing error categories and dependencies.

### Target shape

```rust
/// Metadata values to apply to one visible inode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataUpdate {
    /// Replacement supported Unix mode, or `None` to retain the stored mode.
    pub mode: Option<u32>,
    /// Replacement modification time, or `None` to retain the stored mtime.
    pub mtime: Option<NodeTime>,
}
```

### Public API

```rust
impl Session {
    // ... other methods unchanged ...

    /// Applies `update` to visible `inode`.
    ///
    /// Returns the resulting inode. Returns not-found, invalid-argument,
    /// remote-directory-loading, lifecycle, validation, synchronization, or
    /// SQLite errors. It materializes a changed directory before clearing its
    /// digest. It retains file content backing and the symlink target. An
    /// effective no-op writes nothing.
    pub fn set_metadata(
        &self,
        inode: InodeId,
        update: MetadataUpdate,
    ) -> Result<Inode, SessionError>;
}
```

### Schema

No schema change.

### Unit tests

- Update mode, mtime, and both values on an initially unloaded remote
  directory. Expect one directory read, complete children retained, and no
  second read for the next update.
- Update a remote file and expect its content digest and later readable bytes
  unchanged; metadata update itself makes no file-content request.
- Update a symlink and expect exact target and inode ID unchanged with no
  standalone blob request.
- Apply absent and already-effective values; expect the same inode and no
  observable dirtying.
- Closed-session and invalid-metadata failures leave state unchanged.

### Integration tests

- Update every node kind in a session containing remote and local entries,
  close it, record database and overlay bytes, and run retained inspection
  repeatedly. Expect exact metadata in the final merged reads and no bytes
  changed by inspection.

## `SessionStore`

### Changed behavior and dependencies

- Compare effective metadata before opening a write transaction.
- Preserve kind-specific backing fields.
- Invalidate self plus ancestors for a changed directory and parent plus
  ancestors for a changed file or symlink.
- Reject an unloaded directory in an invalidation chain atomically.
- Dependencies do not change.

### Target shape

No stored struct changes.

### Public API

```rust
impl SessionStore {
    // ... other methods unchanged ...

    /// Applies `update` to a visible inode.
    ///
    /// Returns the resulting lossless inode. Returns not-found,
    /// invalid-metadata, lifecycle, unloaded-directory, dirty-chain,
    /// synchronization, or SQLite errors. A changed directory invalidates
    /// itself and ancestors; another changed kind invalidates its ancestors.
    /// File and symlink backing is preserved. A no-op performs no write.
    pub(super) fn update_metadata(
        &self,
        inode: InodeId,
        update: MetadataUpdate,
    ) -> Result<Inode, SessionError>;
}
```

### Schema

No schema change.

### Unit tests

- Directory metadata clears its own digest and all ancestor digests while
  retaining loaded children.
- File metadata retains remote or overlay backing and clears only directories.
- Symlink metadata retains its target and null per-symlink digest fields.
- Absent or equal values preserve all digest and dirty fields.
- A failure produces byte-for-byte equivalent component-level inode results.

### Integration tests

- Covered by the retained-inspection `Session` workflow above.

## Removed and unchanged components

Errors, `OverlayStore`, `CachedBlobStore`, and creation methods are unchanged.
No content copy-up occurs.
