# Step 6.1.2 Visible Child Lookup Program Design

## Purpose

Update `Session::lookup_child` so lookup returns the one visible child and
never exposes a tombstoned historical row.

## Highlights

- **`Session`** — keeps its lookup API while adopting visible-row semantics.
- **`SessionStore`** — makes `child` select only `tombstone = 0`.

## `Session`

### Changed behavior and dependencies

- A stored tombstone is treated as absent.
- If historical tombstones later coexist with a visible replacement, lookup
  returns only the replacement.
- Existing lazy parent materialization and direct dependencies do not change.

### Target shape

No struct or enum changes.

### Public API

```rust
impl Session {
    // ... other methods unchanged ...

    // Changes (DO NOT INCLUDE IN IMPLEMENTATION): The store lookup now
    // applies visibility policy and cannot return a tombstone.
    /// Resolves visible `name` below `parent`.
    ///
    /// Returns the visible inode. Returns `NotFound` when no visible child
    /// exists, `NotDirectory` for the wrong parent kind, or a remote, decode,
    /// cache, validation, synchronization, or SQLite error. It may materialize
    /// the parent's remote child set and update I/O counters.
    pub fn lookup_child(
        &self,
        parent: InodeId,
        name: &str,
    ) -> Result<Inode, SessionError>;
}
```

### Schema

`Session` owns no schema. This slice needs no schema change because current
version-1 state still permits at most one row per parent/name.

### Unit tests

- An unloaded remote root is read once; repeated lookup returns the same inode
  ID without a second directory request.
- Store lookup returns the expected visible row and `None` for an absent name.
  The tombstone branch receives its API-only regression case in 6.1.8, when
  `tombstone_child` first makes that state constructible.
- Wrong-parent-kind and missing-name errors keep their existing categories.

### Integration tests

- Open a session over the remote fixture, look up every node kind, close it,
  and verify the read-only workflow has stable inode IDs and unchanged remote
  download behavior.

## `SessionStore`

### Changed behavior and dependencies

- `child` applies visibility policy with `tombstone = 0`.
- `inode` and `get_directory_children` remain lossless store APIs.
- Dependencies remain `rusqlite` and session domain types.

### Target shape

No struct changes.

### Public API

```rust
impl SessionStore {
    // ... other methods unchanged ...

    // Changes (DO NOT INCLUDE IN IMPLEMENTATION): Tombstones are excluded.
    /// Fetches visible `name` below `parent`.
    ///
    /// Returns the visible inode or `None`. Returns name-validation, row
    /// decoding, invariant, synchronization, or SQLite errors. Performs no
    /// mutation.
    pub(super) fn child(
        &self,
        parent: InodeId,
        name: &str,
    ) -> Result<Option<Inode>, SessionError>;
}
```

### Schema

No schema change.

### Unit tests

- `child` returns a visible remote row and `None` for an absent name.
- In 6.1.8, extend this test through `tombstone_child`: `child` returns `None`
  while `inode` still retrieves the tombstone by ID. This proves visibility
  policy is confined to the named-child API without using raw SQL or helpers.

### Integration tests

- Covered by the `Session` integration test above.

## Removed and unchanged components

`SessionError`, `FilesystemError`, `OverlayStore`, listing behavior, and all
mutation APIs are unchanged.
