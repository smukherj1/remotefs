# Step 6.1.8 Unlink a Non-Directory Program Design

## Purpose

Add `Session::unlink` so a visible file or symlink becomes a retained tombstone
and disappears from merged lookup and listing.

## Highlights

- **`Session`** — adds non-directory removal and relies on the visible read
  semantics from 6.1.2 and 6.1.3.
- **`SessionStore`** — tombstones one visible child and invalidates its parent
  chain in one transaction.

## `Session`

### Changed behavior and dependencies

- Load the parent before the mutation.
- Tombstone files and symlinks; reject directories with the existing
  `IsDirectory` category.
- Do not remove CAS, cache, or overlay bytes.
- Lookup and listing immediately hide the tombstone.
- Reuse all existing errors and dependencies.

### Target shape

No struct or enum changes.

### Public API

```rust
impl Session {
    // ... other methods unchanged ...

    /// Removes non-directory `name` below `parent` from the visible namespace.
    ///
    /// Returns the retained tombstoned inode. Returns `NotFound`,
    /// `IsDirectory`, wrong-parent-kind, invalid-argument, lifecycle,
    /// validation, synchronization, or SQLite errors. It loads the parent and
    /// commits the tombstone plus dirty ancestor chain atomically. It removes
    /// no CAS, cache, or overlay bytes.
    pub fn unlink(
        &self,
        parent: InodeId,
        name: &str,
    ) -> Result<Inode, SessionError>;
}
```

### Schema

No schema change.

### Unit tests

- Unlink a remote file and, separately, a remote symlink; expect `NotFound`
  from lookup, omission from listing, and stable unrelated siblings.
- Unlink a local file and expect its overlay bytes still readable through its
  retained identity at the store/overlay layers.
- Reject missing names and directories without observable changes.
- Closed-session unlink returns `FailedPreconditionError`.

### Integration tests

- Unlink a remote file, create a new local file at the same name, and expect
  only the new inode in lookup and listing with a different inode ID. Close and
  inspect without mutating the retained files.

## `SessionStore`

### Changed behavior and dependencies

- Tombstone the selected visible file or symlink and invalidate the loaded
  parent-to-root chain in one transaction.
- Retain the inode ID and all kind-specific backing fields.
- `child` returns the replacement if one later occupies the same parent/name;
  `inode` and `get_directory_children` retain both identities.
- `referenced_overlay_files` includes tombstoned rows.
- Dependencies do not change.

### Target shape

No struct changes.

### Public API

```rust
impl SessionStore {
    // ... other methods unchanged ...

    /// Tombstones visible non-directory `name` below `parent`.
    ///
    /// Returns the retained inode. Returns not-found, wrong-parent-kind,
    /// is-directory, lifecycle, dirty-chain, synchronization, or SQLite
    /// errors. Tombstoning and ancestor invalidation commit together; backing
    /// fields remain unchanged.
    pub(super) fn tombstone_child(
        &self,
        parent: InodeId,
        name: &str,
    ) -> Result<Inode, SessionError>;
}
```

### Schema

No schema change. The partial unique index installed in 6.1.4 allows retained
tombstones beside one visible replacement.

### Unit tests

- Tombstone and dirty propagation become observable together, never partly.
- Missing name and directory target failures leave all rows and dirty state
  unchanged.
- Recreate a local file at the same name; `child` returns it while lossless
  APIs return both IDs in name/inode order.
- `referenced_overlay_files` includes both visible and tombstoned local files.

### Integration tests

- Covered by the unlink-and-recreate `Session` workflow above.

## Removed and unchanged components

No errors change. `OverlayStore` deletes nothing. Directory removal and rename
are not implemented in this slice.
