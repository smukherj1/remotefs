# Step 6.1.1 Existing Session Error Mapping Program Design

## Purpose

Correct the boundary between the existing read-only `Session` API and
`FilesystemService` before adding writable operations. No `Session` method or
error category is added in this change.

## Highlights

- **`Session`** — documents the complete existing `SessionError` categories as
  the stable starting point for later incremental additions.
- **`FilesystemService`** — replaces `FilesystemError::Session` with one
  filesystem variant for each error already returned by `Session`.
- **FUSE adapter** — maps the newly explicit filesystem variants without
  changing any callback.

## `Session`

### Changed behavior and dependencies

- The existing variants keep their meaning and sources.
- Each existing `SessionError` has one specific `FilesystemError` mapping.
- `Context` remains recursive and preserves the owning operation.
- Dependencies do not change.

### Target shape

`Session` fields and public value types do not change. This is the complete
error enum at the end of this mini-design:

```rust
/// Failures owned by the local session hierarchy.
///
/// Changes (DO NOT INCLUDE IN IMPLEMENTATION): Each existing variant now has
/// one explicit `FilesystemError` mapping.
#[derive(Debug, Error)]
pub enum SessionError {
    /// Unexpected local-session failure.
    #[error("session internal error: {reason}")]
    InternalError {
        /// Description of the unexpected failure.
        reason: String,
    },
    /// Current state blocks the operation.
    #[error("current session state does not permit this operation: {reason}")]
    FailedPreconditionError {
        /// State that prevented the operation.
        reason: String,
    },
    /// Requested inode or entry is not visible.
    #[error("not found: {reason}")]
    NotFound {
        /// Inode or entry that was not visible.
        reason: String,
    },
    /// Directory operation received another node kind.
    #[error("not a directory: {reason}")]
    NotDirectory {
        /// Node whose kind did not satisfy the directory operation.
        reason: String,
    },
    /// Regular-file operation received a directory.
    #[error("is a directory: {reason}")]
    IsDirectory {
        /// Directory supplied to a regular-file operation.
        reason: String,
    },
    /// A SQLite API operation failed.
    #[error("SQLite {operation} failed on db {dbpath}: {source}")]
    Database {
        /// SQLite operation that failed.
        operation: String,
        /// Database on which the operation failed.
        dbpath: PathBuf,
        /// Original SQLite failure.
        #[source]
        source: rusqlite::Error,
    },
    /// Adds the owning session operation.
    #[error("{operation}: {source}")]
    Context {
        /// Session operation that failed.
        operation: String,
        /// Original session failure.
        #[source]
        source: Box<SessionError>,
    },
}
```

### Public API

All `Session` method signatures and behavior remain unchanged.

### Schema

No schema change.

### Unit tests

- Construct every existing variant and a nested `Context`; pass each through
  the filesystem conversion and assert its exact filesystem category.
- Exercise an existing closed-session call and assert
  `FailedPreconditionError`, not an opaque wrapper.

### Integration tests

- Through `FilesystemService`, trigger existing not-found, wrong-kind,
  closed-session, and fake-CAS/internal failures and verify that each category
  survives the real `Session` boundary.

## `FilesystemService`

### Changed behavior and dependencies

- `FilesystemError::Session` is removed.
- Existing session failures map exhaustively to identically named filesystem
  variants; `Context` maps its nested source recursively.
- `InvalidInode`, `InvalidArgument`, and filesystem `Context` remain
  filesystem-owned. `InvalidArgument` is not yet a session mapping.
- Dependencies remain `Session`, session domain types, `Bytes`, and error
  context support.

### Target shape

Only changed or replacement variants are shown; filesystem-only variants are
unchanged.

```rust
#[derive(Debug, Error)]
pub enum FilesystemError {
    /// Unexpected failure; maps to `EIO`.
    InternalError {
        /// Description of the unexpected failure.
        reason: String,
    },
    /// Current session state blocks the operation; maps to `EIO`.
    FailedPreconditionError {
        /// State that prevented the operation.
        reason: String,
    },
    /// Requested inode or entry is absent; maps to `ENOENT`.
    NotFound {
        /// Inode or entry that was not visible.
        reason: String,
    },
    /// A directory operation received another kind; maps to `ENOTDIR`.
    NotDirectory {
        /// Node whose kind did not satisfy the directory operation.
        reason: String,
    },
    /// A file operation received a directory; maps to `EISDIR`.
    IsDirectory {
        /// Directory supplied to a regular-file operation.
        reason: String,
    },
    /// A session database operation failed; maps to `EIO`.
    Database {
        /// Preserved `SessionError::Database` source.
        source: SessionError,
    },
    // ... filesystem-only variants unchanged ...
    /// Adds an operation and delegates errno selection to `source`.
    Context {
        /// Filesystem operation that failed.
        operation: String,
        /// Original filesystem failure.
        source: Box<FilesystemError>,
    },
}
```

### Public API

```rust
impl From<SessionError> for FilesystemError {
    /// Preserves the category and diagnostics of an existing session failure.
    ///
    /// Returns the matching filesystem error and recursively maps `Context`.
    /// Performs no I/O and has no side effects.
    fn from(source: SessionError) -> Self;
}

impl FilesystemService {
    // ... methods unchanged; every Session result uses `FilesystemError::from` ...
}
```

Removed from the public boundary: `FilesystemError::Session`.

### Schema

No schema change.

### Unit tests

- The exhaustive `From<SessionError>` test must require an explicit match-arm
  update whenever the session enum changes.
- Preserve the original SQLite source inside `FilesystemError::Database`.

### Integration tests

- Covered through the `FilesystemService` integration cases in the `Session`
  section above.

## FUSE adapter

### Changed behavior and dependencies

- Map `InternalError`, `FailedPreconditionError`, and `Database` to `EIO`.
- Continue mapping `NotFound`, `NotDirectory`, and `IsDirectory` to `ENOENT`,
  `ENOTDIR`, and `EISDIR`.
- `Context` delegates to its nested error. No callback changes.

### Target shape

No struct changes.

### Public API

The private errno mapper keeps its existing signature and gains explicit arms
for the replacement variants.

### Schema

No schema change.

### Unit tests

- Assert the documented errno for every filesystem variant available at this
  point, including nested `Context`.

### Integration tests

- The existing read-only FUSE error integration suite continues to pass with
  unchanged observed errno values.

## Removed and unchanged components

`SessionStore`, `OverlayStore`, `CachedBlobStore`, and all `Session` methods are
unchanged. No writable behavior is introduced.
