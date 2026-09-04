# Step 6.1.6 Create a Symlink Program Design

## Purpose

Add `Session::create_symlink` while preserving the supplied target exactly and
without inventing a per-symlink CAS or overlay object.

## Highlights

- **`Session`** — adds symlink creation.
- **`SessionStore`** — accepts inline symlink content in the existing local
  child transaction.

## `Session`

### Changed behavior and dependencies

- Load the parent and validate the new name and target before the transaction.
- Preserve relative, absolute, broken, and parent-containing targets without
  following them.
- Invalidate the containing directory and loaded ancestors.
- Reuse existing `InvalidArgument` and `AlreadyExists`; no error changes.
- Add no CAS or overlay-file operation.

### Target shape

No struct or enum changes.

### Public API

```rust
impl Session {
    // ... other methods unchanged ...

    /// Creates a symlink named `name` below `parent`.
    ///
    /// `target` is retained exactly without resolution; `mode` and `mtime`
    /// initialize supported metadata. Returns the new inode. Returns
    /// invalid-name, invalid-target, wrong-parent-kind, already-exists,
    /// lifecycle, validation, synchronization, or SQLite errors. It loads the
    /// parent and commits the child plus dirty ancestor chain in one
    /// transaction. It performs no content I/O.
    pub fn create_symlink(
        &self,
        parent: InodeId,
        name: &str,
        target: &str,
        mode: u32,
        mtime: NodeTime,
    ) -> Result<Inode, SessionError>;
}
```

### Schema

No schema change.

### Unit tests

- Table-test relative, absolute, broken, and `..`-containing targets.
- Expect exact target, target byte length, metadata, and stable inode ID from
  create and later lookup.
- Expect no standalone CAS request or overlay identity.
- Invalid names, duplicate names, and closed sessions fail without a visible
  child.

### Integration tests

- Create all target forms among remote siblings, repeatedly look them up and
  list their parent through `Session`, and verify exact `symlink_target`
  values, stable sorted merged results, and no content downloads.

## `SessionStore`

### Changed behavior and dependencies

- `create_local_child` accepts an inline symlink target.
- The inserted row has only symlink kind fields plus common metadata.
- Existing dirty propagation invalidates the parent-to-root chain.
- Dependencies do not change.

### Target shape

```rust
pub(super) enum NewInodeContent {
    // ... `File` and `Directory` unchanged ...
    /// Symbolic link stored inline.
    Symlink {
        /// UTF-8 target retained without resolution.
        target: String,
    },
}
```

### Public API

`SessionStore::create_local_child` keeps its signature and gains the `Symlink`
content form. It performs no additional side effect outside its transaction.

### Schema

No schema change; the existing `symlink_target` column owns the target.

### Unit tests

- Create a symlink and expect exact inline target with null file and directory
  backing fields.
- Verify parent-chain digest clearing and atomic duplicate failure.

### Integration tests

- Covered by the `Session` symlink workflow above.

## Removed and unchanged components

Errors, `OverlayStore`, `CachedBlobStore`, and earlier creation methods remain
unchanged. Symlink metadata mutation and replacement arrive in later slices.
