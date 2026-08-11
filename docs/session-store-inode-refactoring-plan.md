# Session Store Inode Boundary Refactoring Plan

## Status and assessment

The proposed dependency direction is correct: `session/store.rs` should expose
the row-shaped data it owns, and `session.rs` should translate that data into
the smaller, validated `session::Inode` view exposed to daemon code.

Two details need to be resolved as part of the refactor:

1. `session::Inode` is intentionally lossy. It contains effective mode and
   mtime values after defaults have been applied, and it omits
   `remote_digest`, `overlay_file`, `tombstone`, `content_dirty`, and
   `tree_dirty`. A general `session::Inode -> store::Inode` conversion therefore
   cannot recreate the data that was read from or should be written to SQLite.
   In particular, it cannot distinguish a stored `NULL` mode or mtime from an
   explicitly stored value equal to the default.
2. The store uses kind, tombstone, backing, and original mode/mtime values while
   performing lookup and atomic materialization. It must not make these
   decisions using unchecked SQLite strings and integers. Storage-level decoding
   must still reject values that cannot be represented safely, while the session
   translator owns the higher-level inode invariants and projection.

The recommended resolution is to avoid a blanket bidirectional conversion.
Use a fallible storage-to-session translator for reads, and explicit
operation-specific builders for writes. A write builder may accept a
`session::Inode` only when it also receives the persistence fields that the
public type does not contain. Existing remote materialization should translate
`RemoteChild` plus its parent and write policy directly rather than attempting
to route through the lossy public inode.

## Goals

- Remove the dependency from `session/store.rs` on `session::Inode`.
- Give the store an inode type whose fields preserve SQLite semantics exactly,
  including nullable values and storage-only flags.
- Keep `session::Inode` as the validated, effective, storage-agnostic API used by
  callers of `Session`.
- Put domain validation and projection at the `Session` facade boundary.
- Preserve transaction boundaries, merged-namespace behavior, error context,
  ordering, and the existing public `Session` API.
- Make lossy or default-applying conversions explicit and testable.

## Non-goals

- Do not change `schema.sql` or increment `user_version`; this is an in-process
  boundary refactor, not a persisted-format migration.
- Do not expose SQLite, `rusqlite` rows, store paths, or storage-only inode fields
  outside `rfs_common::session`.
- Do not implement Phase 6 mutation behavior or redesign the merged namespace.
- Do not rename the public `session::Inode` or change daemon/FUSE callers unless
  compilation exposes an accidental store dependency.

## Target boundary

### Storage representation

Add a `pub(super) store::Inode` (imported in `session.rs` as `StoreInode` to
avoid ambiguity) that represents all columns in the `inodes` table without
applying visible-node defaults or discarding fields. It should contain the
storage equivalents of:

- inode and nullable parent inode;
- name and store-local node kind;
- nullable remote digest text, symlink target, and overlay filename;
- nullable mode, mtime seconds, and mtime nanoseconds;
- tombstone, content-dirty, and tree-dirty flags.

Use store-local closed types for persisted kind and decoded booleans where that
makes invalid values unrepresentable. Preserve nullable fields as nullable; do
not put effective mode, effective mtime, or derived size on this type. The
storage representation may use SQLite-width integers so overflow checks remain
visible in the session conversion.

Keep SQLite decoding in `store.rs`. Decoding owns SQL column/type errors and the
minimal representation checks needed for the store to operate safely, such as
recognizing kind/boolean encodings and requiring paired timestamp columns.
These failures must retain the database path, operation, row/inode identity
when available, and the SQLite source where applicable.

### Session representation

Keep `session::Inode` unchanged as the visible domain type. Add a private
translator in `session.rs`, preferably a named function or
`TryFromStoreInode`-style helper rather than a broad public trait
implementation:

```text
store_inode_to_session(path/context, StoreInode) -> Result<session::Inode, SessionError>
```

The translator must validate:

- positive, representable inode and parent IDs;
- the root inode's fixed ID, absent stored parent, and empty name;
- non-root parent presence and basename rules, including `.` and `..`;
- digest syntax and size validity;
- mode conversion and supported mode policy;
- timestamp range and normalized nanoseconds;
- relative, non-empty overlay paths;
- kind-specific nullable-field combinations;
- any visibility precondition required by the facade operation.

After validation it projects the storage row into `session::Inode` by:

- mapping the store kind to the shared visible `NodeKind`;
- deriving file or symlink size;
- applying kind-specific mode defaults only when stored mode is absent;
- applying the Unix epoch only when stored mtime is absent;
- retaining the symlink target only for symlinks;
- deliberately dropping digest, overlay, tombstone, and dirty fields.

The field filtering should be evident in this one conversion function so a
future schema change cannot accidentally leak storage details into the public
inode.

### Write translation

Do not implement `From<session::Inode> for store::Inode`: the conversion is
fallible and underspecified. Introduce named write mappings for each actual
write workflow:

- root creation maps root digest and fixed root policy to a storage insert;
- remote materialization maps `RemoteChild`, parent identity, and clean flags to
  a storage insert;
- later overlay mutations will map their complete mutation inputs, including
  backing and dirty state, to a storage insert/update.

If a workflow genuinely starts with `session::Inode`, define a companion
private `StoreInodePersistence`/write-input value containing the omitted
persistence fields and have a fallible builder consume both. That builder must
state whether effective default values are stored explicitly or restored to
`NULL`; it must never infer that choice silently.

## API and responsibility changes

1. Change `SessionStore::node`, `lookup`, `list_directory`, and
   `materialize_directory` to return storage-native inode values (and a
   storage-native lookup outcome if necessary). They must no longer mention
   `session::Inode` in their signatures or implementations.
2. Translate return values in the corresponding `Session` facade methods.
   Preserve `Lookup::NeedsMaterialization` without inode translation and
   traverse every inode in `Lookup::Ready` through the same fallible helper.
3. Keep visibility filtering and transaction-sensitive reconciliation in the
   store because they depend on persistence fields and atomic reads/writes.
   Define their behavior in storage terms rather than through the public inode.
4. Convert caller-owned IDs, remote children, and kinds into storage inputs at
   the facade boundary. If retaining `InodeId`, `RemoteChild`, or `Lookup` in a
   store signature creates an inverse dependency, add small store-native input
   or outcome types rather than importing more facade types.
5. Keep `SessionError` as the common private hierarchy for this refactor, but
   ensure session conversion errors identify the database path and inode. A
   separate storage error type is not required unless removing the remaining
   store-to-facade dependency makes error ownership materially clearer.

The fourth step is important: removing only the nested `node: session::Inode`
field improves the immediate design, but store methods currently also import
several types declared by the parent facade. The implementation should audit
those imports and either document genuinely shared domain types or replace
facade-only dependencies. The completed boundary should not merely hide the
same upward dependency behind another aggregate.

## Implementation sequence

1. Characterize current behavior with focused tests for row decoding, effective
   defaults, visibility, kind/backing combinations, and materialization
   idempotency before moving code.
2. Introduce the row-shaped `store::Inode` and store-local kind/flag types.
   Replace `InodeTuple`/`StoredNode` with one clearly named decode path that
   preserves every selected column.
3. Update store helpers (`required_directory`, remote identity comparison,
   visible-child filtering, and file-source resolution) to use storage fields
   directly. Do not derive a public inode inside the store.
4. Add and unit-test the storage-to-session translator in `session.rs`. Move the
   domain portions of `validate_inode_row` into it while leaving SQL decoding
   and minimal storage representation checks below the boundary.
5. Update each `Session` inode-returning method to translate scalar, optional,
   lookup, and vector results without losing operation context. Ensure one bad
   row fails the whole operation rather than being silently filtered.
6. Replace materialization/root write construction with explicit storage write
   builders. Do not add a general public-inode round trip.
7. Remove obsolete imports, `StoredNode`, and duplicate validation/defaulting
   code. Update module documentation in `session.rs` and `store.rs` to state the
   new ownership boundary.
8. Update `docs/technical-design.md` after implementation so its row-translation
   wording distinguishes storage decoding from session-domain validation. Mark
   the work in `docs/implementation-plan.md` only if this refactor becomes part
   of the active implementation sequence.

## Verification

Add or retain tests covering:

- every database column survives store decoding, including `NULL` versus an
  explicit value equal to a visible default;
- root and non-root identity validation;
- invalid kind, boolean, digest, mode, timestamp, overlay path, and
  kind-specific field combinations;
- files, directories, and symlinks derive the same visible size, mode, mtime,
  and target as before;
- tombstoned nodes never reach callers, while malformed visible rows fail with
  database path and inode context;
- scalar lookup, directory listing, and materialization translate all ready
  results and preserve `NeedsMaterialization` unchanged;
- materialization remains atomic, idempotent, ordered, and preserves nullable
  remote mode/mtime values;
- a read followed by a write does not convert absent mode/mtime to explicit
  defaults unless the operation explicitly requests that semantic change.

Run formatting, the `rfs-common` unit suite, session integration tests, and the
read-only integration/e2e workflows named in the main implementation plan.

## Definition of done

- `session/store.rs` does not import or construct `session::Inode`.
- The store-owned inode mirrors persisted semantics and retains every
  storage-only field.
- All visible inode construction and domain validation occurs at the
  `Session` boundary.
- No implicit lossy public-to-storage conversion exists.
- Existing public behavior and SQLite bytes/schema remain compatible.
- Tests demonstrate both the layer boundary and preservation of validation,
  visibility, defaulting, and transaction behavior.
