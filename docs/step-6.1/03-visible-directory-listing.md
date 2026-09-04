# Step 6.1.3 Visible Directory Listing Program Design

## Purpose

Update `Session::list_directory` so a directory listing contains only visible
children in stable basename order.

## Highlights

- **`Session`** — filters lossless store rows into the visible merged listing.

## `Session`

### Changed behavior and dependencies

- Listing omits every tombstone returned by `SessionStore`.
- Visible children remain sorted by basename; inode ID remains the tie-breaker
  only inside the lossless store result.
- The store remains lossless so later unlink and rename work can retain
  historical rows.
- Existing lazy materialization and dependencies do not change.

### Target shape

No struct or enum changes.

### Public API

```rust
impl Session {
    // ... other methods unchanged ...

    // Changes (DO NOT INCLUDE IN IMPLEMENTATION): Tombstones are filtered
    // after the complete stored child set is obtained.
    /// Lists visible children of directory `inode` in basename order.
    ///
    /// Returns non-tombstoned children. Returns `NotDirectory` or a remote,
    /// decode, cache, validation, synchronization, or SQLite error. It may
    /// materialize the directory and update I/O counters.
    pub fn list_directory(
        &self,
        inode: InodeId,
    ) -> Result<Vec<Inode>, SessionError>;
}
```

### Schema

No schema change.

### Unit tests

- Listing an unloaded remote directory reads its `Directory` once and returns
  each child exactly once in basename order.
- Remote rows produce only visible public `Inode` values. In 6.1.8, extend this
  case through the new unlink API to prove tombstones are omitted without
  arranging private state directly.
- A second listing is stable and performs no second remote directory request.

### Integration tests

- Open the remote fixture and mix lookup with listing from several threads;
  verify one materialization, stable inode IDs, no duplicate names, and the
  same sorted result for every caller.

## Removed and unchanged components

`SessionStore::get_directory_children` intentionally remains lossless and
ordered by name then inode ID. Errors, schema, overlay storage, lookup, and all
mutation APIs are unchanged.
