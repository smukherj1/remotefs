# State Module Simplification

Status: implemented by implementation-plan step 5.4. Writable APIs remain
intentionally deferred to Phase 6.

This document records the design discussion about simplifying
`rfs-common::state` after implementation-plan step 5.3. The resulting facade is
`rfs_common::session`; historical descriptions below document the architecture
that step 5.4 replaced.

## Goals

- Replace the current collection of capability traits, trait objects, forwarding
  stores, worker commands, partial row models, and path helpers with a small
  hierarchy of concrete components.
- Give each component one clear ownership boundary over its files, resources,
  invariants, and lifecycle.
- Expose each layer through one facade so callers do not depend on how that
  layer is internally decomposed.
- Separate remote filesystem orchestration from local state and cache
  mechanics. Local state must not know about CAS clients or REAPI transport.
- Split the current large module into files organized by responsibility without
  manufacturing new layers merely to make files smaller.
- Establish design principles that can later be extracted into a general skill
  for low-level class, type, and layer design.

## Alignment Scope

This alignment defines concrete facade behavior only for implemented read-only
filesystem, cache, retained-inspection, and lifecycle consumers. Exact APIs for
future writable operations are deferred to the Phase 6 slice that implements
them, when their FUSE and core consumers, transaction boundaries, and failure
behavior are concrete.

The document may preserve high-level writable invariants that constrain the
current ownership design, but it does not commit future Rust signatures or
result shapes. Phase 6 must begin with an API-and-transaction design task before
writable implementation starts.

## Pre-5.4 System

`crates/rfs-common/src/state.rs` is currently about 2,100 lines and combines:

- public session, daemon-state, and filesystem-state capabilities;
- fixed `RFS_HOME` path derivation;
- state-root ownership and permission validation;
- the active-session advisory lock and its diagnostic record;
- active-session creation, replacement, inspection, and clean close;
- SQLite opening and schema management;
- raw row decoding and domain validation;
- inode allocation and remote-directory materialization;
- a dedicated database worker thread and its typed channel protocol;
- tests for all of the above.

The cache implementation is split across modules:

- `state.rs` defines and creates `cache/blobs` and `cache/dirs`;
- `rfsd/src/fs.rs` performs cache lookup, miss coalescing, downloading,
  verification, and atomic admission;
- digest-to-cache-path logic exists independently in both modules.

The lazy filesystem core in `rfsd/src/fs.rs` already acts as a service above
CAS, cache, and SQLite state. Its local-state dependency is currently expressed
through `Arc<dyn FilesystemState>`.

### Current public capability chain

The main writable chain is:

```text
Box<dyn DaemonState>
  -> Arc<dyn FilesystemState>
    -> FilesystemStore
      -> WorkerClient
        -> WorkerCommand
          -> SessionStore worker thread
            -> rusqlite::Connection
```

Retained inspection separately uses:

```text
Box<dyn SessionStateReader>
  -> StateReader
    -> read-only rusqlite::Connection
```

These abstractions preserve useful behavior, but most do not represent
independent implementations or domain concepts. Much of the chain exists to
forward one operation or carry the database command channel.

## Problems to Address

### Responsibilities are grouped by history rather than ownership

`StatePaths` owns paths for shared cache data, active-session data, the lock,
logs, control socket, database, and overlay. Shared cache data and active
session data have different lifetimes and different owners, so one path object
obscures rather than explains the design.

### The local cache has split ownership

The state module creates and partly models the cache layout, while the daemon
filesystem module implements cache behavior. This duplicates path rules and
makes neither component the complete owner of the cache.

### Capability traits are being used as partitions

`SessionStateReader`, `DaemonState`, and `FilesystemState` divide access, but
the implementation has only one meaningful local-state backend. The resulting
trait objects require wrapper types, allocation, forwarding methods, and worker
transport types. Cheap temporary SQLite state can be used directly by most
tests instead.

### The worker protocol duplicates the repository API

`WorkerCommand`, `WorkerClient`, response channels, the startup handshake, and
`FilesystemStore` reproduce every database operation around one
`rusqlite::Connection`. The resolved `rusqlite` version makes `Connection`
`Send` but not `Sync`; a mutex can provide the required serialization without a
dedicated command language and thread.

### Persistence types are inconsistent

The module currently uses a mixture of a 13-element `InodeTuple`, partial
`InodeRow`, `RawSession`, `StoredSession`, `SessionMetadata`,
`RemoteNodeIdentity`, and `MaterializedInode`. Some distinctions mark real
validation or shape boundaries, while others reflect incremental
implementation. The result is difficult to keep in working memory.

### Identical CAS objects are classified unnecessarily

File contents and serialized REAPI `Directory` messages are both immutable CAS
blobs addressed by the same digest definition. Cache lookup, verification,
admission, and concurrency behavior do not differ. Separate `cache/blobs` and
`cache/dirs` namespaces force a semantic distinction into a layer that does
not use it.

### Local-state internals leak into higher-level composition

The filesystem core receives a state capability and a cache root separately.
The target design should let it depend on one local-state facade without
knowing whether an operation uses a cache, SQLite, an overlay directory, or a
combination of them.

## Agreed Target Hierarchy

The highest local-state type is `Session`:

```text
FilesystemService
├── CAS
└── Session
    ├── BlobCache
    └── ActiveSession
        ├── SessionStore
        └── OverlayStore
```

Dependency and interaction rules are stricter than the ownership diagram:

```text
FilesystemService -> Session
Session           -> BlobCache, ActiveSession
ActiveSession     -> SessionStore, OverlayStore
```

- `FilesystemService` must not receive or expose `BlobCache`, `ActiveSession`,
  `SessionStore`, or `OverlayStore`.
- `Session` must not expose accessors that let callers bypass it and operate on
  its children.
- `Session` asks `ActiveSession` to perform session-scoped work; it does not
  reach through `ActiveSession` directly to `SessionStore` or `OverlayStore`.
- `ActiveSession` is the only consumer of `SessionStore` and `OverlayStore`
  result types.
- Child component types should remain private unless a concrete consumer
  demonstrates a need for visibility.

`FilesystemService` is the remote-aware orchestration layer. It owns or uses a
CAS dependency and understands REAPI directory semantics. `Session` and
everything below it have no knowledge of `BlobStore`, CAS clients, CAS URLs,
REAPI RPCs, or remote transport.

`FilesystemService` does not retain an authoritative namespace view or decoded
directory state. It may retain dependencies, immutable configuration,
telemetry, and a synchronous keyed map used only to deduplicate in-progress
remote downloads. Each operation queries `Session`, performs any required
remote work, and commits the result through `Session`. Remote-download
coordination belongs to `FilesystemService`; local cache integrity and atomic
admission belong to `BlobCache`; SQLite serialization and materialization
idempotence belong below `Session`; and FUSE file-handle tracking belongs to
the adapter or daemon.

The keyed download map is a narrow exception to the service's otherwise
stateless operation model. A digest's coordination guard spans the locked cache
recheck, pending-blob creation, remote stream, and successful finalization or
abandonment. It exists only to avoid duplicate network transfers. Cache
correctness does not rely on it: `BlobCache` still verifies content and admits
with atomic no-clobber semantics, so an orchestration defect cannot overwrite a
valid entry. The service does not add inode maps, decoded-directory caches, or
another namespace view.

The type is `FilesystemService` in the daemon-owned `rfsd::filesystem` module.
The current `rfsd::fs::ReadOnlyFilesystem` performs much of this role before
the refactor.

## Agreed Component Responsibilities

### `Session`

`Session` is the concrete facade for all local storage used by one mounted
workspace. It is not merely a bag of child objects.

It:

- resolves and validates the overall `RFS_HOME` root;
- establishes the top-level ownership split between shared cache and active
  session data;
- owns one `BlobCache` and one `ActiveSession`;
- exposes local operations needed by `FilesystemService`;
- is the authoritative lookup boundary for visible inode, child, and directory
  state;
- coordinates operations that genuinely cross the cache/active-session
  boundary;
- hides which child component performs an operation;
- exposes one-shot retained inspection through `Session::inspect`;
- exposes explicit, idempotent clean close through `Session::close(&self)`.

Writable construction returns only the concrete `Session`:

```rust
Session::open(config, root_digest, mountpoint) -> Result<Session, SessionError>
Session::info(&self) -> SessionInfo
```

`Session::open` derives the current daemon process ID, canonicalizes the
mountpoint, establishes the layout and exclusive lock, creates the durable
session, and retains its validated immutable information. `SessionInfo`
contains the root digest, canonical mountpoint, daemon PID, control endpoint,
cache root, active-session root, and daemon log path. There is no separate
startup-result wrapper and no second SQLite metadata query after construction.
The method returns one low-frequency value rather than partitioning immutable
facts across identity and diagnostic accessors.

`SessionInfo` does not contain cache/download counters, dirty counts or dirty
state, snapshot blockers, protocol version, or mounted health. The daemon owns
those runtime and protocol facts and combines them with `SessionInfo` when it
constructs the live `rfs status` response.

It does not:

- download from CAS itself;
- decode or encode REAPI messages;
- expose its child components;
- add forwarding methods that have no consumer or invariant to enforce.

`FilesystemService` must not maintain a second authoritative namespace view.
SQLite remains the source of truth for visible nodes, including local
overrides, tombstones, renames, and metadata changes.

#### Agreed facade behavior

The following read-only behavior and synchronous boundary are agreed.

Inode lookup returns the current visible node or a contextual `SessionError`.
Child lookup and directory listing must distinguish a definitive result from a
remote directory that has not been materialized. Conceptually:

```rust
enum Lookup<T> {
    Ready(T),
    NeedsMaterialization { digest: Digest },
}

Session::lookup(parent, name) -> Result<Lookup<Node>, SessionError>
Session::list_directory(parent) -> Result<Lookup<Vec<Node>>, SessionError>
```

`NeedsMaterialization` is ordinary lazy-loading control flow, not a
`SessionError`. A definitively absent child after complete materialization is a
contextual `SessionError::NotFound`. The facade does not expose a separate
`is_materialized` check, which would leak persistence mechanics and introduce a
check/use race.

Directory materialization accepts the complete decoded remote directory after
`FilesystemService` translates it into transport-independent child
descriptors. Each descriptor contains its name, kind-specific content identity,
mode, and mtime. Conceptually:

```rust
Session::materialize_directory(
    parent: InodeId,
    remote_digest: Digest,
    remote_children: Vec<RemoteChild>,
) -> Result<Vec<Node>, SessionError>
```

`ActiveSession` and `SessionStore` atomically reconcile the remote child set and
allocate stable inode identities. Existing local overrides win, tombstones
suppress matching names, and the result is the complete visible child set in
stable namespace order. Tombstones, persistence rows, REAPI types, and cache
paths do not escape. Repeating the same materialization is idempotent; a
conflicting digest or immutable remote child identity is an error.

For complete immutable objects that `FilesystemService` must decode, the
facade exposes one generic full-blob operation:

```rust
Session::read_blob(digest: &Digest) -> Result<Option<Bytes>, SessionError>
```

`Some(bytes)` contains the complete admitted object and `None` means it is not
cached. `FilesystemService` then coordinates and commits the missing download
and retries. No separate `contains_blob` facade method is needed: `read_blob`
is the fast path for callers that need complete bytes, while
`start_blob_download` performs the authoritative recheck under the service's
download lock. Full reads are used for objects that require complete decoding,
such as serialized REAPI directories; regular file data uses range reads so a
large file is not accumulated in memory. The cache does not otherwise
distinguish file content from directory objects.

#### Deferred writable facade design

Do not define `SessionFile`, open options, creation results, write, truncate,
synchronization, metadata mutation, unlink, directory removal, or rename APIs
as part of the read-only state simplification. Design them in the Phase 6 slice
that introduces their concrete consumers.

The following are non-binding constraints for that later design:

- raw write-capable `std::fs::File` values and backing paths must not let a
  caller bypass `Session` invariants;
- opening remote content should remain lazy unless the concrete writable
  operation demonstrates a correctness reason to fetch;
- all handles for one inode must observe one logical content backing, with
  first-write copy-up coordinated per inode rather than independently per
  handle;
- rename, unlink, and replacement must preserve Unix open-handle identity;
- remote loading and large file copying should remain outside SQLite
  transactions;
- a new regular file must receive physical overlay backing before its inode
  becomes visible; a failed SQLite commit may leave only an invisible orphan;
- write and truncate operations must not permit bypassing copy-up, dirty-state,
  or ancestor-marking rules;
- synchronization of copied-up content must wait behind prior inode mutations
  and flush the current overlay backing without itself causing copy-up.

Earlier interview sketches involving `SessionFile`, `OpenOptions`,
`CreatedFile`, and `SyncMode` are intentionally not API commitments.

#### Clean close

`Session` may be shared through `Arc`, so clean close does not consume it.
Successful `rfs unmount` remains completion-based: the daemon must not report
success until `Session::close(&self)` has committed the durable `closed`
lifecycle.

The close contract is deliberately simple:

1. The daemon first stops operation sources, unmounts FUSE, and joins the FUSE
   thread. Quiescing and draining daemon operations is an orchestration-layer
   responsibility and a precondition of `Session::close`; `Session` does not
   maintain its own in-flight-operation protocol.
2. `Session::close` makes the SQLite clean-close transition.
3. After a successful commit, it drops active-session resources and releases
   the advisory lock as the final ownership step before returning success.
4. Repeated calls after a successful close return success without changing
   SQLite again.

A failed clean-close attempt returns its error and retains the advisory lock
while the `Session` remains alive. There is no automatic retry state machine or
ambiguous-commit recovery protocol. Failure recovery is external: the daemon
may be stopped, at which point the operating system releases the advisory
lock. The stable `active.lock` file itself is not manually removed. Any
non-closed retained session continues to block automatic replacement under the
existing conservative stale-state policy.

### `BlobCache`

`BlobCache` owns one unified content-addressed cache rooted at:

```text
RFS_HOME/cache/blobs/<2-hex-prefix>/<hash>-<size>
```

It:

- derives sharded paths;
- detects hits;
- creates an opaque, non-cloneable streaming writer backed by a unique
  temporary file for an expected digest;
- incrementally tracks the written size and hash;
- verifies the completed size and hash during finalization;
- uses a temporary file in the target filesystem;
- atomically admits verified bytes;
- opens or reads admitted blobs;
- trusts already-admitted entries during ordinary reads, matching the current
  design.

It does not distinguish file contents from serialized directory objects.
Semantic directory decoding and validation remain above the cache.

`BlobCache` does not load remote data and does not coordinate callers across
the start/finalize boundary. `FilesystemService` may perform an unlocked cache
check as a fast path, acquires its per-digest download lock, and then asks
`Session` to atomically recheck local presence and create a temporary writer
when needed. Conceptually:

```rust
let _download = filesystem_service.download_lock(digest);
match session.start_blob_download(digest)? {
    BlobDownloader::Exists => { /* cache hit */ }
    BlobDownloader::Writer(mut writer) => {
        remote_blobs.download_into(digest, &mut writer)?;
        session.finalize_blob(writer)?;
    }
}
```

The first unlocked cache check is optional. `start_blob_download` under the
per-digest service lock is authoritative for download deduplication and returns
`BlobDownloader::Exists` or `BlobDownloader::Writer(BlobWriter)`. It does not
reserve the digest inside `Session`; unsynchronized callers could each receive
a writer, while atomic no-clobber finalization still protects correctness.

The pending-blob value exposes a streaming write interface but no temporary or
final path. Finalization consumes it, verifies the digest, syncs it, and
atomically moves it to the final cache location. Dropping it before successful
finalization cleans up its temporary file. Remote download failures remain
`FilesystemService` errors and require no callback-error wrapping.

`BlobWriter` implements `std::io::Write` but not `Read`, `Seek`, cloning, or a
path accessor. Each successful sequential write updates an incremental SHA-256
state and byte count, and writing beyond the expected size fails immediately.
Finalization checks the completed size and hash using that state, so admitting
a large blob requires neither buffering it in memory nor rereading it solely
for verification.

`Session::finalize_blob(writer)` consumes the opaque writer and returns only
`Result<(), SessionError>`. It validates the completed size and hash, flushes
and syncs the temporary, and atomically admits it without overwriting an
existing final entry. If another valid writer has already admitted the same
digest, finalization discards its temporary and succeeds. Hit/download
telemetry remains derivable by `FilesystemService` from whether
`start_blob_download` returned `Exists` or `Writer`.

The existing remote `BlobStore` trait gains `stream_blob`, which streams a
verified remote object into a caller-provided `std::io::Write` destination.
`FilesystemService` bridges the trait's async tonic implementation through the
daemon runtime while writing into the synchronous `BlobWriter`. The existing
`download_blob` operation remains as a default collecting convenience built on
`stream_blob`, so small complete-object callers and tests do not require a
second transport implementation. No additional remote-source trait is added.

Each pending file is created beside its final sharded cache entry, with a
unique name such as `.<hash>-<size>-<uuid>.tmp`. Keeping temporary and final
files in the same directory guarantees same-filesystem atomic admission and
keeps cache-fill lifetime under `BlobCache`; `active/overlay/tmp` is not used
for immutable cache objects. A process-crash orphan is reconstructible cache
debris handled by the retained-layout and cleanup policy.

The current `cache/dirs` tree becomes obsolete. Because cache data is
reconstructible, an encountered old directory-cache tree may remain untouched
and ignored. RemoteFS is still in development, so the refactor does not promise
migration of existing local development state; developers may reset
`RFS_HOME`. The technical design, implementation plan, and cache-layout tests
must be updated with the implementation.

Where callers need different access patterns over the same cached object, use
standard library/data types rather than semantic cache wrappers where possible:

- `bytes::Bytes` for complete-object decoding.

The current read-only filesystem performs immutable range reads by inode rather
than by a digest selected above the facade. `Session` resolves the inode's
current content identity, validates that it is a regular file, and selects the
backing source. It returns `LocalRead::Ready(bytes)` when that source is local
or cached, or `LocalRead::NeedsDownload { digest }` when immutable remote
content must be fetched. `FilesystemService` deduplicates and commits that
download, then retries the inode-based operation so `Session` re-resolves the
current backing rather than assuming it remained unchanged during remote I/O.
This keeps SQLite authoritative while exposing the digest only as immediate
remote-work control flow. Handle-based access and any restricted handle type
are deferred to Phase 6.

Cache paths used for storage operations do not cross the `Session` boundary.
The live-status contract is a deliberate narrow exception: `Session` may
report the cache root and active-session root as read-only diagnostic values,
and it may provide the active log path needed to initialize daemon logging.
It never exposes digest-specific cache paths, overlay backing paths, or child
component access, and callers do not use diagnostic paths to perform local
storage operations. `FilesystemService` derives cache-hit and download
telemetry from `BlobDownloader::{Exists, Writer}` and the lazy read outcomes.

Do not introduce `CachedBlob`, `FileBlobCache`, or `DirectoryBlobCache` unless
a later requirement gives such a type its own invariant or lifecycle.

`Session` hides local-versus-remote read-source selection and exposes the local
cache-fill protocol without exposing an admitted path. `FilesystemService`
owns remote transport and download deduplication. Phase 6 will decide how local
and copied-up content through `OverlayStore` participates in the same facade.
No overlay path, cache path, raw write-capable file, or backing-kind
implementation detail may escape.

### `ActiveSession`

`ActiveSession` owns:

- `RFS_HOME/active.lock` and its diagnostic record;
- the entire `RFS_HOME/active` tree;
- startup validation and exclusive ownership;
- safe replacement of a valid cleanly closed active tree;
- the session log and control-socket paths;
- session lifecycle;
- one `SessionStore`;
- one `OverlayStore`;
- coordination of operations that span SQLite and overlay files.

The lock must outlive the writable store and overlay resources and must only be
released according to the agreed clean-close contract.

`ActiveSession` hides `SessionStore` and `OverlayStore` from `Session`.

### `SessionStore`

`SessionStore` owns:

- the writable `rusqlite::Connection`;
- schema creation and version validation;
- SQL statements and row extraction;
- row validation and representation conversion;
- transaction boundaries;
- session metadata, inode, and directory-materialization persistence.

It uses a mutex-protected connection rather than a worker:

```rust
struct SessionStore {
    database_path: PathBuf,
    connection: Mutex<rusqlite::Connection>,
}
```

This replaces `WorkerCommand`, `WorkerClient`, the dedicated worker thread,
startup/reply channels, `FilesystemStore`, and worker-transport errors.

The store exposes focused, transactionally complete repository operations, not
a generic CRUD framework. Likely operations include:

```text
session_metadata
inode
child
materialize_directory
create_file
create_directory
create_symlink
close
```

Private `insert_*` and `update_*` helpers are acceptable. `ActiveSession` must
not assemble a multi-table atomic operation by calling public low-level CRUD
methods one at a time.

Every `Session` facade operation is synchronous, including namespace and
SQLite operations, cache inspection and fill, complete-blob access, immutable
range reads, retained inspection, and clean close. The FUSE API is synchronous
and waits for remote content on a miss regardless; propagating the current
async filesystem-core shape into local state would not make the kernel callback
asynchronous.

Any adaptation required by the async tonic CAS client occurs once in the
remote-aware daemon layer. `FilesystemService` holds its per-digest download
coordination while it streams remote chunks into the opaque pending-blob
writer, but no SQLite transaction or connection mutex is held during remote
I/O.

#### Retained inspection

A constructed `SessionStore` is writable-only; it has no reader/writer mode
flag. Read-only inspection is a one-shot associated operation:

```rust
SessionStore::inspect(database_path)
```

It opens SQLite with `SQLITE_OPEN_READ_ONLY`, validates the schema and row,
returns metadata, and closes immediately. It never initializes the schema or
mutates the database.

The facade call chain is:

```text
Session::inspect
  -> ActiveSession::inspect
    -> SessionStore::inspect
```

This keeps SQL out of the upper layers without creating a reader trait or a
long-lived read-only store type.

Control-endpoint discovery remains separate from retained inspection. The CLI
must be able to derive and probe the fixed active-session socket without first
opening SQLite; a live daemon therefore remains reachable even if retained
database inspection would fail. Conceptually, the read-only facade behavior is:

```rust
Session::control_endpoint(config) -> Result<PathBuf, SessionError>
Session::inspect(config) -> Result<Option<RetainedSession>, SessionError>
```

The CLI probes the endpoint first and calls `inspect` only when no live daemon
answers. Endpoint discovery and inspection create and mutate nothing.

`RetainedSession` contains only the fields consumed by fallback status and
mountpoint validation: durable lifecycle, canonical mountpoint, root digest,
and recorded daemon PID. It does not expose cache paths, active-session paths,
SQLite representations, or child components.

### `OverlayStore`

`OverlayStore` owns:

- `RFS_HOME/active/overlay`;
- its `data` and `tmp` descendants;
- temporary-file creation;
- local file reads and writes;
- atomic file admission;
- copying cached remote bytes into writable overlay data;
- identifying orphaned overlay files if cleanup is implemented.

It does not decide which file is visible in the workspace. SQLite remains the
source of truth for visibility, inode identity, dirty state, and overlay-file
references. `ActiveSession` owns the ordering and failure contract for
operations spanning `OverlayStore` and `SessionStore`.

## Agreed Path-Ownership Design

Do not replace `StatePaths` with another large hierarchy of path-holder types.
Each component stores its root and derives its own descendants:

```rust
struct Session {
    home: PathBuf,
    cache: BlobCache,
    active: ActiveSession,
}

struct BlobCache {
    root: PathBuf, // RFS_HOME/cache/blobs
}

struct ActiveSession {
    root: PathBuf, // RFS_HOME/active
    // lock, store, overlay, ...
}

struct OverlayStore {
    root: PathBuf, // RFS_HOME/active/overlay
}
```

At startup, `Session` resolves `RFS_HOME` and supplies only owned roots:

```text
BlobCache <- RFS_HOME/cache/blobs
ActiveSession <- RFS_HOME/active and RFS_HOME/active.lock
```

`BlobCache` owns cache subtree creation and validation. `ActiveSession` owns
active subtree creation and validation. `Session` retains any top-level
validation needed to enforce the known `RFS_HOME` inventory, but it does not
derive every descendant path.

## Agreed Type and Representation Policy

All local-state layers use concrete types. Remove:

- `SessionStateReader`;
- `DaemonState`;
- `FilesystemState`;
- their `Box<dyn ...>` and `Arc<dyn ...>` uses;
- wrapper implementations that exist solely to implement those traits.

Tests should use temporary real local state and SQLite where practical. SQLite
is cheap enough that a persistence trait solely for in-memory fakes is not
justified. Pure transformations and policy decisions should still be tested as
ordinary unit tests.

Use the minimum number of persistence representations:

- If SQLite extraction and the value consumed by `ActiveSession` have the same
  fields and Rust member types, use one `<Type>`.
- Introduce a private `<Type>Row` only when the persisted shape or member types
  differ from the validated value.
- `<Type>Row` is untrusted storage representation.
- `<Type>` is the validated value returned to `ActiveSession`.
- Conversion and validation happen inside `SessionStore`.
- Store result types are not exposed beyond `ActiveSession`.
- A further `Session`-level type is justified only if the facade exposes a
  genuinely different shape or concept.

For example, SQLite integers and strings usually require both `InodeRow` and
`Inode` when the validated value uses an inode ID, enum node kind, `Digest`,
and booleans. Do not add both forms mechanically when no representation or
trust-boundary difference exists.

## General Design Philosophy

This section is intentionally component-neutral so it can seed a future skill.

### Start with ownership and dependency direction

Before designing methods or files, draw the ownership tree and the permitted
interaction edges. Ownership describes lifetime; dependency edges describe who
may know whom. They are related but not interchangeable.

A parent facade should hide its internal decomposition. A caller depends on the
facade, not on a selection of its children. A child should not reach upward or
sideways around its owner.

### Add a layer only when it owns something

A useful layer owns at least one of:

- a resource or lifetime;
- an invariant;
- a transaction or atomicity boundary;
- a representation/trust conversion;
- a policy decision;
- an external-system boundary.

A type that only forwards every method unchanged is evidence that the boundary
may not exist. File size alone is not a reason to add a service layer.

### Prefer concrete types by default

Use a trait or interface when there are independent implementations, an
external boundary needs substitution, or callers genuinely require behavioral
polymorphism. Do not introduce a trait solely to:

- divide one implementation into capabilities;
- hide a concrete type already private to a module;
- make a cheap local dependency fakeable;
- mirror every method of one concrete type.

Test cheap infrastructure directly. Reserve fakes for expensive,
nondeterministic, remote, or independently implemented boundaries.

### Use standard types until a wrapper owns an invariant

Prefer standard handles, paths, byte buffers, and collections when no additional
invariant is enforced. Introduce a wrapper when construction proves something,
drop has meaning, operations must be restricted, or representation must remain
opaque.

### Concurrency machinery must earn its protocol

Use the smallest primitive that enforces the required serialization and
lifetime. A mutex is preferable to a worker thread and command enum when work
does not require thread affinity, background progress, isolation, batching, or
independent scheduling.

Keep blocking boundaries at the orchestration layer that knows its execution
environment. Do not force asynchronous or worker abstractions into a low-level
component merely because one caller is async.

### Errors should identify the owning operation and entity

Each layer should add context for the operation and stable entity it owns while
preserving structured lower-level failures. Avoid separate error variants for
abstraction machinery that the design can remove, such as channel failures for
a worker that is unnecessary.

### Organize files after responsibilities converge

First settle ownership, dependencies, invariants, and operations. Then split
files along those boundaries. Do not infer architecture from a desired
directory tree.

## Tentative Source Layout

The facade module is `rfs_common::session`. A likely layout is:

```text
crates/rfs-common/src/session/
  mod.rs              # Session facade, public session API, top-level errors
  cache.rs            # BlobCache
  active/
    mod.rs            # ActiveSession and retained inspection
    store.rs          # SessionStore and SQL operations
    model.rs          # only if row/validated types justify a separate file
    overlay.rs        # OverlayStore
    schema.sql
```

This is a working consequence of the ownership design, not an agreed file list.
Types should remain next to their owner when a separate `model.rs` would merely
force readers to jump between files.

The higher `FilesystemService` remains daemon-owned under `rfsd`; the FUSE
adapter remains a separate neighboring module.

## Recently Resolved Alignment Decisions

### Clean-close ownership and concurrency

- Use explicit, idempotent `Session::close(&self)` rather than consuming
  `Session` or relying on final `Arc` destruction for normal clean close.
- Preserve the guarantee that a successful unmount response means the durable
  session is already marked `closed`.
- Make the daemon quiesce and drain FUSE and other operation sources before
  calling close; do not add an in-flight-operation state machine to `Session`.
- Commit the SQLite lifecycle first and release the active-session advisory
  lock last.
- On failure, return the close error and retain the lock until the session or
  daemon is dropped. Do not add automatic retries or ambiguous-commit
  detection. Process exit releases advisory ownership without removing the
  stable lock file.

### Settled portions of the `Session` facade

- `Session` is authoritative for visible inode, child, and directory lookup.
  `FilesystemService` does not retain a second namespace view.
- `FilesystemService` retains no authoritative namespace or decoded-directory
  state. Its only operation-coordination state is a keyed map that deduplicates
  in-progress remote downloads.
- Child lookup and directory listing return an ordinary
  `NeedsMaterialization` outcome when remote metadata must be loaded. A
  definitive missing entry is a contextual `SessionError::NotFound`.
- Directory materialization accepts one complete transport-independent remote
  child set and returns the complete merged visible child set. It is atomic and
  idempotent, applies local overrides and tombstones, and allocates stable
  inodes internally.
- Cache paths are private. `Session::read_blob` returns `Option<bytes::Bytes>`
  for complete-object decoding; cache misses are filled explicitly through the
  service-owned download flow.
- One unified `BlobCache` serves both file content and serialized directory
  objects.
- `Session` selects local overlay versus remote cached backing and is the only
  entry point for content writes, truncation, and copy-up. `FilesystemService`
  explicitly checks local cache state, streams missing remote content into an
  opaque pending-blob writer, and asks `Session` to finalize it. It receives no
  backing paths or child-component access.
- Inode allocation is internal to logical create operations. A new regular
  file receives overlay backing before its SQLite inode becomes visible; a
  failed commit may leave only an invisible orphan file.
- Exact writable facade and handle APIs are deferred to Phase 6. The current
  alignment retains only ownership and correctness constraints that affect the
  present hierarchy.
- Control-endpoint discovery does not open SQLite and remains separate from
  one-shot retained inspection. Retained inspection returns only lifecycle,
  mountpoint, root digest, and daemon PID.
- Live status continues to report the cache root and active-session root as
  read-only diagnostics. This does not permit operational cache or overlay
  paths, or access to child components, to cross the facade.
- Writable startup returns one concrete `Session`. Its single `info()` method
  supplies the immutable local-session facts needed by daemon startup and live
  status without rereading SQLite or introducing separate identity,
  diagnostics, or startup-result types. Runtime status remains daemon-owned.
- The existing `tree::NodeKind`, `state::RemoteNodeKind`, and
  `rfsd::fs::NodeKind` collapse into one neutral `rfs-common` `NodeKind` with
  file, directory, and symlink variants. SQLite text and generated REAPI enums
  are translated at their owning boundaries and do not become domain types.
- Session-stable inode identity uses a copyable `InodeId` newtype rather than a
  raw `u64`. Construction proves that the value is positive and fits SQLite's
  signed integer range; `InodeId::ROOT` represents inode one, and the FUSE
  adapter can obtain the underlying `u64`.
- The facade's visible inode type is simply `Node`. It exposes inode, parent,
  name, shared `NodeKind`, explicit size, mode, mtime, and symlink target, but
  not a remote digest or local backing identity. Missing remote identities
  cross only through `NeedsMaterialization` and `NeedsDownload` control flow.
- `FilesystemService` translates a completely decoded REAPI `Directory` into
  `Vec<RemoteChild>` before calling `Session::materialize_directory`. Each
  child contains its name, mode, mtime, and one kind-safe
  `RemoteContent::{File(Digest), Directory(Digest), Symlink(String)}` value.
  This occurs on the first access to each remote directory; `Session` stores
  the complete set atomically and idempotently and returns the visible `Node`
  set. Protocol messages and stringly typed content identities do not cross
  the facade.
- Node modification times use a transport-independent `NodeTime { seconds:
  i64, nanos: u32 }`. Construction enforces normalized nanoseconds and the
  supported range. REAPI protobuf timestamps, SQLite integer columns, and FUSE
  `SystemTime` values are converted only at their owning boundaries.
- `RemoteChild` and SQLite preserve absent mode and mtime distinctly from
  explicit zero/epoch values so snapshot re-encoding can retain the original
  REAPI representation and digest. The facade's `Node` instead exposes
  non-optional effective values, applying defaults once during conversion:
  file `0o444`, directory `0o555`, symlink `0o777`, and Unix-epoch mtime. FUSE
  does not duplicate defaulting policy.
- The local-state hierarchy uses one public, contextual `SessionError` rather
  than private `CacheError`, `StoreError`, and `ActiveSessionError` enums.
  Private helpers attach the owning operation and stable path, inode, or digest
  while preserving `io` and `rusqlite` sources. The removed worker contributes
  no channel/thread variants. A distinct overlay error is deferred until Phase
  6 demonstrates a concrete independent boundary.
- The facade module is renamed from `rfs_common::state` to
  `rfs_common::session`, matching the concrete lifecycle and API callers use.
- `FilesystemService` remains in `rfsd`, where its only production consumer is
  the daemon's FUSE adapter. FUSE independence is used for ordinary unit and
  integration testing and does not by itself justify a shared-crate API.
- Writable startup preserves the exact top-level `RFS_HOME` inventory:
  `active.lock` is a private regular file, while `cache` and an optional
  `active` are private directories. Unknown entries, symlinks, wrong types,
  unsafe ownership, or permissive modes at this boundary block startup.
  `Session` owns this validation; it does not imply recursive cache scanning.
- Cache validation is access-driven rather than recursive. Unrelated shard
  names and stale temporary files are ignored. On access, `BlobCache` validates
  only the expected shard and exact digest path: an owned private regular file
  is a trusted hit, absence is a miss, and a symlink, wrong type, unsafe owner,
  or unsafe permissions is an error that is neither followed nor deleted.
  Obsolete `cache/dirs` content remains untouched and ignored.
- The refactor adds no separate filesystem-layout version. SQLite
  `user_version` remains the explicit durable schema version; exact active-tree
  structure and access-driven cache validation are sufficient to recognize the
  changed ownership layout. A separate version is deferred until a future
  layout cannot be recognized safely from structure and database version.

## Alignment Result and Implementation Handoff

The major read-only facade is settled. Exact Rust borrowing details may be
adjusted during implementation without reopening the component boundaries.
The required `Session` surface is:

```text
open                    Session coordinates top-level validation, BlobCache,
                        and ActiveSession construction
info                    immutable startup/status facts owned by Session
control_endpoint        read-only fixed endpoint discovery; opens no SQLite
inspect                 one-shot retained inspection through ActiveSession
node                    visible inode lookup through ActiveSession
lookup                  child lookup through ActiveSession
list_directory          visible listing through ActiveSession
materialize_directory   atomic reconciliation through ActiveSession
read_blob               complete admitted-object read through BlobCache
read_range              inode resolution through ActiveSession, then local
                        range access through BlobCache as required
start_blob_download     local cache recheck and BlobWriter creation
finalize_blob            verified, synced, atomic cache admission
close                   idempotent clean close and final lock release
```

No facade child accessor is exposed. `FilesystemService` owns CAS,
REAPI decoding, retrying `NeedsMaterialization` and `NeedsDownload` outcomes,
per-digest remote-download deduplication, and remote error context. `Session`
and its children remain synchronous and transport-independent.

The implementation should proceed in buildable slices:

1. characterize existing lifecycle, materialization, cache admission, and
   read-only behavior with tests;
2. introduce `InodeId`, `NodeKind`, `NodeTime`, `Node`, `RemoteChild`, and the
   minimal row/validated persistence representations;
3. replace the worker protocol with one mutex-protected `rusqlite::Connection`;
4. extract `BlobCache`, `OverlayStore`, and `ActiveSession`;
5. introduce `rfs_common::session::Session` and convert retained inspection and
   daemon lifecycle consumers;
6. add streaming `BlobStore::stream_blob`, `BlobWriter`, and service-owned
   per-digest download coordination;
7. convert `FilesystemService`, remove its inode and decoded-directory maps,
   and remove the old state traits, forwarding stores, worker commands, and
   duplicated node types;
8. update the technical design, implementation plan, state/cache tests, and
   control/status composition;
9. run formatting, lint, workspace unit, state integration, read-only
   integration, and read-only end-to-end workflows.

There is no compatibility-migration requirement for existing development
state. Fresh tests use fresh `RFS_HOME` values, and developers may reset old
local state. Runtime safety rules for any retained state that is encountered
remain conservative.

The following work is deliberately deferred rather than unresolved:

- Phase 6 designs writable handles and mutations from their concrete FUSE
  consumers, I/O ordering, SQLite transactions, partial-failure visibility,
  copy-up behavior, and concurrency.
- Counter storage and exact status-counter semantics are implementation-plan
  details. The daemon telemetry sink and `FilesystemService` own cache-hit,
  download, remote-error, and semantic-operation observations; `SessionInfo`
  remains free of runtime telemetry.
- Source files may be combined when a proposed private module would only
  forward calls or separate a type from its sole owner.

## Constraints That Must Remain True

The simplification must preserve or deliberately replace these existing
contracts:

- only one active daemon session per `RFS_HOME`;
- the stable lock remains outside the removable active tree;
- unsafe, malformed, partial, corrupt, or unsupported retained state is
  preserved and blocks startup;
- only a valid cleanly closed active tree is automatically replaced;
- retained inspection creates and mutates nothing;
- SQLite remains the source of truth for visible workspace state;
- schema shape remains in embedded SQL and domain validation remains in Rust;
- writable database access uses one serialized connection;
- directory materialization and inode allocation remain atomic and idempotent;
- cached bytes are admitted only after digest verification and atomic rename;
- `FilesystemService` coalesces in-process concurrent remote downloads for one
  digest;
- cache contents remain reusable across sequential sessions;
- FUSE-specific types remain outside the local-state hierarchy.

Any intentional contract change, including the unified cache layout, must be
called out explicitly in the technical design and tests rather than hidden
inside a structural refactor.
