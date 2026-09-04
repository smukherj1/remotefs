# Step 6.1.9 Remove an Empty Directory Program Design

## Purpose

Add `Session::remove_directory`, including remote child materialization before
the atomic emptiness check and tombstone transaction.

## Highlights

- **`Session`** — adds empty-directory removal and the
  `DirectoryNotEmpty` error category.
- **`SessionStore`** — extends tombstoning with directory kind and emptiness
  checks.
- **`FilesystemService`** — mirrors only the new error.
- **FUSE adapter** — maps the new error to `ENOTEMPTY`.

## `Session`

### Changed behavior and dependencies

- Load the parent and target directory before the mutation transaction.
- Tombstone only an empty visible directory.
- Preserve the target inode and reusable directory digest on its tombstoned
  row; invalidate only the containing directory chain.
- Add `DirectoryNotEmpty` to `SessionError`; prior variants are unchanged.
- Dependencies do not change.

### Target shape

Add only this variant to the enum established by earlier designs:

```rust
pub enum SessionError {
    // ... existing variants unchanged ...
    /// Removal or replacement requires an empty directory.
    #[error("directory not empty: {reason}")]
    DirectoryNotEmpty {
        /// Directory that prevented the operation.
        reason: String,
    },
}
```

### Public API

```rust
impl Session {
    // ... other methods unchanged ...

    /// Removes empty directory `name` below `parent` from the visible namespace.
    ///
    /// Returns the retained tombstoned inode. Returns `NotFound`,
    /// `NotDirectory`, `DirectoryNotEmpty`, wrong-parent-kind,
    /// invalid-argument, remote-loading, lifecycle, validation,
    /// synchronization, or SQLite errors. It materializes the target before
    /// atomically tombstoning it and invalidating the parent chain. It removes
    /// no CAS or cache bytes.
    pub fn remove_directory(
        &self,
        parent: InodeId,
        name: &str,
    ) -> Result<Inode, SessionError>;
}
```

### Schema

No schema change.

### Unit tests

- Remove an initially unloaded empty remote directory; expect its directory
  blob read before success and immediate invisibility afterward.
- Attempt an initially unloaded non-empty remote directory; expect one read,
  `DirectoryNotEmpty`, and unchanged target and children.
- Reject a file with `NotDirectory`, a missing name with `NotFound`, and a
  closed session with `FailedPreconditionError`; every failure is atomic.

### Integration tests

- Mix successful empty-directory removal with failed non-empty removal, close
  the session, and verify retained inspection leaves the database and cache
  bytes unchanged.

## `SessionStore`

### Changed behavior and dependencies

- Generalize the tombstone transaction to select unlink or directory-removal
  kind checks explicitly.
- A directory removal requires the already-materialized target to have no
  visible children.
- The emptiness check, tombstone, and parent-chain invalidation share one
  transaction.
- Dependencies do not change.

### Target shape

No struct changes.

### Public API

```rust
impl SessionStore {
    // ... other methods unchanged ...

    // Changes (DO NOT INCLUDE IN IMPLEMENTATION): `expect_directory` extends
    // the unlink-only method from 6.1.8.
    /// Tombstones visible `name` after checking its expected kind.
    ///
    /// `expect_directory` selects directory removal when true and unlink when
    /// false. Returns the retained inode. Returns not-found, wrong-kind,
    /// non-empty, lifecycle, dirty-chain, synchronization, or SQLite errors.
    /// The checks, tombstone, and ancestor invalidation commit together.
    pub(super) fn tombstone_child(
        &self,
        parent: InodeId,
        name: &str,
        expect_directory: bool,
    ) -> Result<Inode, SessionError>;
}
```

### Schema

No schema change.

### Unit tests

- Empty-directory tombstone and dirty propagation commit together.
- Non-empty, wrong-kind, and missing-name failures return the specific category
  with byte-for-byte equivalent public component reads before and after.
- The tombstoned directory retains its inode ID, loaded child state, and
  reusable digest.

### Integration tests

- Covered by the `Session` removal workflow above.

## `FilesystemService`

### Changed behavior and dependencies

- Add the matching `FilesystemError::DirectoryNotEmpty` variant and exhaustive
  conversion arm.
- No service method or dependency is added.

### Target shape

```rust
pub enum FilesystemError {
    // ... existing variants unchanged ...
    /// A directory required by the operation is not empty; maps to `ENOTEMPTY`.
    DirectoryNotEmpty {
        /// Directory that prevented the operation.
        reason: String,
    },
}
```

### Public API

`From<SessionError>` gains one exhaustive arm. Other service APIs remain
unchanged.

### Schema

No schema change.

### Unit tests

- Convert direct and context-wrapped `DirectoryNotEmpty` and assert the exact
  filesystem variant.

### Integration tests

- The `Session` integration test exercises the domain error.

## FUSE adapter

### Changed behavior and dependencies

- Map `DirectoryNotEmpty` to `ENOTEMPTY`.
- No callback or dependency changes.

### Target shape

No struct changes.

### Public API

The private errno mapper keeps its signature and gains one exhaustive arm.

### Schema

No schema change.

### Unit tests

- Assert `ENOTEMPTY` for direct and context-wrapped
  `FilesystemError::DirectoryNotEmpty`.

### Integration tests

- No writable FUSE integration is added before Step 6.3.

## Removed and unchanged components

`OverlayStore`, `CachedBlobStore`, creation, metadata update, and unlink retain
their behavior. Rename is not implemented yet.
