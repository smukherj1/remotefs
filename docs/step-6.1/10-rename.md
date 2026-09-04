# Step 6.1.10 Rename an Entry Program Design

## Purpose

Add `Session::rename` as the final Step 6.1 namespace mutation, preserving the
source inode identity while atomically handling replacement and both dirty
ancestor chains.

## Highlights

- **`Session`** — adds same-directory and cross-directory rename with required
  remote materialization.
- **`SessionStore`** — moves one visible inode, optionally tombstones a
  compatible destination, rejects cycles, and dirties the union of both
  ancestor chains in one transaction.

## `Session`

### Changed behavior and dependencies

- Materialize both parents and any destination directory whose emptiness must
  be checked before opening the transaction.
- Preserve the source inode ID and its file-content or directory digest.
- Permit file-over-file, symlink-over-symlink, and empty-directory-over-empty-
  directory replacement; tombstone the replaced inode.
- Reject kind mismatch, non-empty directory replacement, and moving a
  directory into itself or a descendant.
- Invalidate the union of source and destination containing-directory chains.
- Reuse existing errors and dependencies; no enum change.

### Target shape

No struct or enum changes.

### Public API

```rust
impl Session {
    // ... other methods unchanged ...

    /// Moves a visible child while preserving its inode ID.
    ///
    /// Moves `source_name` below `source_parent` to `destination_name` below
    /// `destination_parent`. Returns the moved inode. Returns not-found,
    /// incompatible-kind, non-empty-destination, descendant-cycle,
    /// invalid-argument, lifecycle, remote-loading, validation,
    /// synchronization, or SQLite errors. It materializes required directories
    /// before one transaction tombstones a compatible destination, moves the
    /// source, and invalidates both ancestor chains. Source content or subtree
    /// backing remains reusable.
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

No schema change.

### Unit tests

- Rename a remote file, remote directory, local file, and symlink within one
  directory; expect old-name absence, new-name visibility, original inode ID,
  and reusable content.
- Move each supported kind across loaded directories; expect both listings to
  update atomically.
- Replace file with file, symlink with symlink, and empty directory with empty
  directory; expect the moved source visible and destination retained only as
  a tombstone.
- Reject file/directory mismatch and non-empty destination without changing
  either path.
- Reject moving a directory below itself or a descendant with
  `InvalidArgument` and an unchanged complete tree.
- Closed-session rename returns `FailedPreconditionError`.

### Integration tests

- Run the complete Step 6.1 workflow: materialize remote directories; create
  every local kind; update metadata; unlink; remove an empty directory; and
  rename across directories. Re-read every affected inode and directory and
  verify the final merged namespace, exact metadata, stable identities, and
  empty local-file bytes.
- Run materialization, creation, lookup, listing, metadata updates, unlink, and
  independent renames from several threads. Expect no duplicate visible names,
  partial listings, invalid backing combinations, or dirty directory retaining
  a stale digest.
- Close that session and open a new one under the same `RFS_HOME`. Expect the
  retained overlay namespace to be replaced, original remote names restored,
  and verified cache entries reusable.

## `SessionStore`

### Changed behavior and dependencies

- Validate source, destination, kind compatibility, destination emptiness, and
  directory ancestry inside the transaction before writes.
- Tombstone a compatible visible destination and move the source in the same
  transaction.
- Preserve the source inode ID and kind-specific backing.
- Dirty each unique directory in the union of source and destination ancestor
  chains once; reject unloaded chain members.
- Dependencies do not change.

### Target shape

No struct changes.

### Public API

```rust
impl SessionStore {
    // ... other methods unchanged ...

    /// Moves a visible child and may replace a compatible destination.
    ///
    /// Returns the moved inode with its ID unchanged. Returns source or
    /// destination absence, incompatible kind, non-empty destination,
    /// descendant cycle, lifecycle, dirty-chain, synchronization, or SQLite
    /// errors. All validation and the destination tombstone, source move, and
    /// union of dirty chains occur in one transaction. Rename alone retains
    /// source content and directory backing.
    pub(super) fn rename_child(
        &self,
        source_parent: InodeId,
        source_name: &str,
        destination_parent: InodeId,
        destination_name: &str,
    ) -> Result<Inode, SessionError>;
}
```

### Schema

No schema change.

### Unit tests

- Remote file and directory moves retain source inode ID and reusable digest.
- A cousin-to-cousin move dirties each unique ancestor once and leaves
  unrelated branches clean.
- Compatible replacement exposes destination tombstone, moved source, and both
  dirty chains together. Injected transaction failure exposes none of them.
- Kind mismatch, non-empty destination, self-cycle, and descendant-cycle
  failures make no observable change.
- Concurrent independent and competing mutations serialize complete
  transactions, allocate unique IDs, and report deterministic domain errors.

### Integration tests

- Covered by the complete and concurrent `Session` workflows above.

## Removed and unchanged components

No error, schema, `OverlayStore`, `CachedBlobStore`, or FUSE callback change is
needed. File-content copy-up remains Step 6.2, writable FUSE callbacks remain
Step 6.3, and snapshot traversal remains Step 7.1.
