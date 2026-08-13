# Session Store Inode Boundary Refactoring Plan

## Goal

`session/store.rs` should return the row-shaped inode data it owns.
`session.rs` should validate and project that data into the smaller public
`session::Inode` used by daemon code.

This is an in-process boundary refactor. It does not change `schema.sql`, its
`user_version`, the public `Session` API, or persisted database semantics.

## Structs

`session::Inode` is unchanged:

```rust
pub struct Inode {
    pub inode: InodeId,
    pub parent: InodeId,
    pub name: String,
    pub kind: NodeKind,
    pub size: u64,
    pub mode: u32,
    pub mtime: NodeTime,
    pub symlink_target: Option<String>,
}
```

It remains the validated, effective view: mode and mtime defaults are applied,
size is derived, and persistence details are hidden.

Old storage struct:

```rust
struct StoredNode {
    node: session::Inode,
    remote_digest: Option<Digest>,
    overlay_file: Option<PathBuf>,
    stored_mode: Option<u32>,
    stored_mtime: Option<NodeTime>,
    tombstone: bool,
}
```

New storage struct:

```rust
pub(super) struct Inode {
    pub inode: Option<i64>,
    pub parent_inode: Option<i64>,
    pub name: String,
    pub kind: StoreNodeKind,
    pub remote_digest: Option<String>,
    pub symlink_target: Option<String>,
    pub overlay_file: Option<String>,
    pub mode: Option<i64>,
    pub mtime_seconds: Option<i64>,
    pub mtime_nanos: Option<i64>,
    pub tombstone: bool,
    pub content_dirty: bool,
    pub tree_dirty: bool,
}
```

The new type mirrors SQLite without applying defaults or discarding
storage-only fields. It serves both decoded rows (where `inode` is always
present) and insert rows (where `inode` is `None` to request SQLite's
`AUTOINCREMENT` allocation). Store-local closed types should represent persisted
kinds and decoded booleans where useful.

## Storage layer

Update the inode-returning methods to use storage-native values:

```rust
fn node(&self, inode: InodeId)
    -> Result<store::Inode, SessionError>;

fn lookup(&self, parent: InodeId, name: &str)
    -> Result<store::Lookup<store::Inode>, SessionError>;

fn list_directory(&self, inode: InodeId)
    -> Result<store::Lookup<Vec<store::Inode>>, SessionError>;

fn materialize_directory(
    &self,
    parent: InodeId,
    digest: &Digest,
    children: &[store::RemoteChild],
) -> Result<Vec<store::Inode>, SessionError>;
```

These methods continue to own visibility filtering, reconciliation, ordering,
and transaction boundaries, but no longer construct `session::Inode`.
Store-native ID, lookup, and child types should replace facade types if the
dependency audit finds those types are not genuinely shared domain concepts.

Replace tuple decoding and combined domain projection with a row decoder:

```rust
fn decode_inode_row(
    path: &Path,
    row: &rusqlite::Row<'_>,
) -> Result<store::Inode, SessionError>;
```

It owns SQLite column/type errors and minimal representation checks needed by
the store, including kind and boolean decoding and paired timestamp columns.
Errors retain the database path, operation, SQLite source, and inode when known.

Remove:

```rust
struct StoredNode;
type InodeTuple = (...);

fn validate_inode_row(...) -> Result<StoredNode, SessionError>;
```

## Session layer

Add one private, fallible translator:

```rust
fn store_inode_to_session(
    inode: StoreInode,
) -> Result<session::Inode, SessionError>;
```

It validates inode and parent identities, names, digests, modes, timestamps,
overlay paths, visibility, and kind-specific field combinations. It then
derives size, applies mode/mtime defaults, maps the node kind, and deliberately
drops backing, tombstone, and dirty fields.

The public methods keep their existing signatures and translate every returned
store inode through that helper:

```rust
pub fn node(&self, inode: InodeId)
    -> Result<Inode, SessionError>;

pub fn lookup(&self, parent: InodeId, name: &str)
    -> Result<Lookup<Inode>, SessionError>;

pub fn list_directory(&self, inode: InodeId)
    -> Result<Lookup<Vec<Inode>>, SessionError>;

pub fn materialize_directory(
    &self,
    parent: InodeId,
    remote_digest: &Digest,
    remote_children: Vec<RemoteChild>,
) -> Result<Vec<Inode>, SessionError>;
```

`Lookup::NeedsMaterialization` passes through unchanged. Any invalid inode in a
ready result fails the whole operation with inode context. The session layer
does not read the database path; the store keeps that path private and reports
it only in its own store-level errors.

Add operation-specific write builders; exact names may follow the implementation:

```rust
fn root_store_insert(root_digest: &Digest) -> StoreInode;

fn remote_child_store_insert(
    parent: InodeId,
    child: RemoteChild,
) -> StoreInode;
```

They preserve nullable mode/mtime and all backing and dirty fields. Do not add
`From<session::Inode> for store::Inode`: the public inode cannot recover fields
it discarded or distinguish `NULL` mode/mtime from explicit default values.

## Verification

Tests must show that:

- every database column survives store decoding, including `NULL` versus an
  explicit default value;
- malformed store encodings (kinds, booleans, paired timestamps) fail with
  database context, and malformed inode identities, digests, modes, timestamps,
  overlay paths, and field combinations fail with inode context;
- visible size, mode, mtime, and symlink target remain unchanged;
- tombstones stay hidden and every ready lookup result is translated;
- materialization remains atomic, idempotent, ordered, and preserves nullable
  mode/mtime values.

Run formatting, `rfs-common` unit tests, session integration tests, and the
read-only integration/e2e workflows from `docs/implementation-plan.md`.

## Done when

- `session/store.rs` neither imports nor constructs `session::Inode`;
- the store inode retains all persisted fields and serves reads and inserts;
- `session.rs` owns visible inode validation, defaults, and projection without
  reading the database path;
- no implicit lossy public-to-storage conversion exists;
- public behavior and SQLite bytes/schema remain compatible.
