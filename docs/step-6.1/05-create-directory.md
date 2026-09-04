# Step 6.1.5 Create a Directory Program Design

## Purpose

Add `Session::create_directory` by extending the local-child transaction from
6.1.4 with an immediately loaded, empty directory representation.

## Highlights

- **`Session`** — adds local directory creation.
- **`SessionStore`** — accepts the directory form of `NewInodeContent`.

## `Session`

### Changed behavior and dependencies

- Load the parent before committing the child.
- Return a digest-free, loaded, tree-dirty directory.
- Invalidate the loaded parent-to-root chain in the same transaction.
- Reuse all error categories from earlier designs; no enum changes.
- Dependencies do not change.

### Target shape

No struct or enum changes.

### Public API

```rust
impl Session {
    // ... other methods unchanged ...

    /// Creates an empty local directory named `name` below `parent`.
    ///
    /// `mode` and `mtime` initialize supported metadata. Returns the new
    /// loaded directory inode. Returns invalid-argument, wrong-parent-kind,
    /// already-exists, lifecycle, validation, synchronization, or SQLite
    /// errors. It loads the parent and commits the child plus dirty ancestor
    /// chain in one transaction. It performs no overlay-file or CAS I/O.
    pub fn create_directory(
        &self,
        parent: InodeId,
        name: &str,
        mode: u32,
        mtime: NodeTime,
    ) -> Result<Inode, SessionError>;
}
```

### Schema

No schema change after 6.1.4.

### Unit tests

- Create below the remote root and list the returned inode; expect an empty
  result without a CAS request.
- Expect exact metadata and immediate visibility in the parent's sorted list.
- Duplicate, wrong-parent-kind, and closed-session failures change nothing.

### Integration tests

- Create nested local directories, interleave lookups and listings, close the
  session, and verify retained inspection succeeds without changing database
  or overlay bytes.

## `SessionStore`

### Changed behavior and dependencies

- `create_local_child` accepts a directory content form.
- The new directory is loaded, digest-free, and tree-dirty at insertion.
- The existing single transaction also invalidates its ancestor chain.
- Dependencies do not change.

### Target shape

```rust
pub(super) enum NewInodeContent {
    // ... `File` from 6.1.4 unchanged ...
    /// Empty local directory whose complete child set is already loaded.
    Directory,
}
```

### Public API

`SessionStore::create_local_child` keeps the signature and contract from
6.1.4; its accepted `NewInodeContent` set gains `Directory`.

### Schema

No schema change. Existing version-2 fields represent this state.

### Unit tests

- Create a local directory and expect `directory_loaded = true`, no remote
  digest, `directory_tree_dirty = true`, and null file/symlink fields.
- Create a child inside it and expect only that directory and its ancestors to
  become or remain dirty.
- Failure leaves inode allocation and ancestor state observably unchanged.

### Integration tests

- Covered by the nested-directory `Session` workflow above.

## Removed and unchanged components

Errors, `OverlayStore`, `CachedBlobStore`, and existing methods are unchanged.
No directory removal or rename behavior is added.
