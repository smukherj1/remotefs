# RemoteFS Technical Design

## Status

This document captures implementation and architecture decisions. Product scope and user-visible requirements live in `PRD.md`.

## Major Decisions

- Use the Remote Execution API CAS, ByteStream, and `Directory` model as the storage protocol and snapshot encoding.
- Use `bazel-remote` as the default MVP CAS target.
- Use Buildbarn storage as the secondary compatibility target.
- Do not implement a custom RemoteFS storage service in the MVP.
- Build the client and daemon in Rust.
- Use `fuser` for the initial Linux FUSE implementation.
- Ship two binaries:
  - `rfs`: user-facing CLI.
  - `rfsd`: long-running mount daemon.
- Use SQLite through `rusqlite` for durable session and overlay state.
- Allow at most one active mount session per `RFS_HOME`, coordinated by a stable `RFS_HOME/session.lock` outside the removable session tree.
- Use one Unix control socket per active `RFS_HOME` session for CLI-to-daemon commands.
- Use whole-file copy-on-write for remote-backed file mutations.
- Use full-workspace snapshotting for the earliest writable MVP.

## REAPI Profile

RemoteFS stores snapshot trees as Remote Execution API `Directory` objects.

The MVP profile supports:

- SHA-256 digests only.
- Structured digests containing hash and size.
- Root digest string format: `sha256:<64-lowercase-hex>/<decimal-size-bytes>`.
- Regular files via `FileNode`.
- Directories via `DirectoryNode`.
- Symlinks via `SymlinkNode`.
- File and directory mtimes via `NodeProperties`.
- Basic Unix mode bits via `NodeProperties.unix_mode`.
- `FileNode.is_executable` as compatibility data derived from Unix mode.

The MVP profile excludes:

- Hard-link identity.
- Device files.
- FIFOs.
- Sockets.
- Extended attributes.
- ACLs.
- Sparse-file preservation.
- UID/GID ownership preservation.

Entries must use REAPI canonical ordering. Upload and snapshot must share the same upload-free encoder so bootstrap uploads and mounted workspace snapshots produce compatible trees.

The encoder owns canonical directory encoding, deterministic digest calculation, decode validation, and metadata warnings. CAS existence checks, upload orchestration, counters, and batch/ByteStream policy are handled outside the encoder by upload code over the `BlobStore` abstraction. Input traversal remains separate: `rfs upload` walks a local directory, while daemon snapshot walks the merged overlay graph after the snapshot barrier.

## CAS Target

The default development and evaluation target is `bazel-remote`.

RemoteFS uses the Remote Execution API `instance_name` field as the CAS namespace selector. The MVP requires a non-empty `--instance-name`; there is no RemoteFS-level default empty instance. RemoteFS should not introduce a separate storage namespace concept such as "project"; CI/CD systems remain responsible for mapping their own projects, repositories, commits, cache keys, and builds to root digests and CAS instance configuration.

ByteStream resource names use the standard uncompressed REAPI forms:

- Read: `{instance_name}/blobs/{hash}/{size}`
- Write: `{instance_name}/uploads/{uuid}/blobs/{hash}/{size}`

RemoteFS generates a fresh UUID v4 per ByteStream upload attempt. The MVP does not use compressed ByteStream resource names, digest-function prefixes, or optional metadata. `instance_name` validation rejects empty values and path segments that equal reserved REAPI resource keywords such as `blobs`, `uploads`, and `compressed-blobs`.

MVP deployments must run the remote CAS with eviction disabled or enough capacity to keep all objects reachable from snapshots they intend to reuse. REAPI CAS does not provide snapshot retention roots or safe reachability garbage collection by itself.

Root digests are therefore capability references. They are valid only while the CAS still contains the root object and all reachable descendants.

CAS client calls use conservative retry and timeout defaults for CI network resilience:

- Per-attempt timeouts default to 10 seconds for `FindMissingBlobs` and 30 seconds for `BatchReadBlobs` and `BatchUpdateBlobs`.
- ByteStream read/write uses a 30 second idle timeout rather than one whole-stream timeout in the MVP.
- Retries are limited to transient transport or server failures: unavailable, deadline exceeded, resource exhausted, aborted, and internal errors that indicate a transport reset.
- Semantic and authorization failures are not retried: not found after mount validation, invalid argument, permission denied, unauthenticated, digest mismatch, and failed precondition.
- Exponential backoff with jitter starts at 100 ms, then 250 ms, 500 ms, and 1 second, with at most 5 attempts by default.
- CAS configuration validates retry limits at construction time. The configured attempt count may not be zero and may not exceed the package maximum used by the backoff schedule.
- ByteStream retries restart from byte 0 in the MVP. Partial stream resume is deferred.
- Final errors include the operation name, digest or resource name when relevant, attempt count, CAS URL, and `instance_name`.
- CAS operation identity is represented internally by a typed enum or equivalent closed set, not ad hoc string matching. That operation type owns timeout selection, display names, and retry metadata.

Small blob and directory uploads use `BatchUpdateBlobs` packing to reduce RPC overhead:

- The default batch/ByteStream split is 4 MiB.
- The default batch request payload budget is 4 MiB minus a small reserved overhead allowance and is configurable for compatibility testing.
- Small blob and directory-node entries are packed by a running payload-size total. When the next entry would exceed the request budget, the current request is sent and a new request starts.
- A near-threshold entry that cannot fit in the batch budget by itself is uploaded through ByteStream.
- Entry order within a `BatchUpdateBlobs` request is not semantically significant and is not part of RemoteFS determinism guarantees. Determinism is required for tree encoding, root digests, and user-visible summaries.

The CAS client exposes only RemoteFS-level operations to callers: existence
checks, blob upload, and verified blob download. `BlobStore::stream_blob`
writes a download incrementally to a caller-provided `Write` destination;
`download_blob` is a collecting convenience over the same implementation.
ByteStream resource-name construction, response verification, batch packing,
retry classification, and transport-specific helper functions remain private
implementation details unless another module has a concrete need for them.
Public methods are documented with expected arguments, return values, and
error behavior.

## Process Model

`rfs` is the user-facing CLI. Its target MVP command surface is:

```sh
rfs upload <local-dir>
rfs mount <root-digest> <mountpoint>
rfs snapshot
rfs unmount
rfs status
```

`rfsd` owns one immutable mount session. Its root digest and mountpoint are fixed
at startup. The daemon owns:

- The FUSE mount.
- The mounted root digest.
- The CAS client.
- The shared local cache handles.
- The active session SQLite database.
- The active session overlay data directory.
- The Unix control socket.
- Snapshot upload for the mounted workspace.
- Graceful session close before a successful unmount response.

The CLI owns parsing, presentation, daemon process startup, read-only retained
session inspection, and calls through the command-oriented daemon client. The
only domain-work exception is bootstrap `rfs upload`, exposed through a narrow
configured capability whose operation accepts a local path and returns a root
digest. CAS clients, tree traversal, batching, and encoding are not CLI APIs.

When implemented, `rfs mount` will start `rfsd` in the background by default and return only after the root directory is validated, the FUSE mount is active, and the control socket is reachable. Direct `rfsd` invocation will run in the foreground unless supervised externally.

Current CLI behavior implements the read-only milestone: `upload`, `mount`,
`status`, and `unmount` are functional, while `snapshot` remains unimplemented.
`mount` starts `rfsd` in the background and returns after root validation, FUSE
initialization, and control-socket readiness. Direct `rfsd` invocation keeps the
same mount workflow in the foreground for tests and manual debugging.

## Rust Source Layout

RemoteFS is a Cargo workspace whose manifests enforce process ownership:

```text
crates/rfs/
  src/main.rs            # rfs binary entrypoint
  src/cli.rs             # command parsing, coordination, and presentation
  src/daemon_client.rs   # command-oriented control client and private transport
  src/bootstrap_upload.rs # narrow local-directory upload workflow
crates/rfsd/
  src/main.rs            # rfsd binary entrypoint
  src/control_service.rs # daemon-side control protocol and shutdown
  src/filesystem.rs      # remote-aware filesystem orchestration
  src/fuse.rs            # synchronous FUSE adapter
crates/rfs-common/
  src/session.rs         # concrete local-session facade and public API
  src/session/           # private cache, overlay, store, and schema components
  src/                   # shared domain, storage, protocol, and logging modules
```

Direct internal dependency edges are:

```text
rfs  -> rfs-common
rfsd -> rfs-common
```

Cargo prevents either binary package from importing the other. Finer ownership
boundaries are modules: daemon-client and bootstrap-upload orchestration stay in
`rfs`; the control service and filesystem stay in `rfsd`; shared protocol,
session, CAS, tree, and lower-level upload mechanics stay in `rfs-common`.
Generated protocol messages and tonic clients are confined to the client and
service adapter modules and are not CLI command/result types. `rusqlite` and
SQLite row types do not escape the common session module.

`rfs_common::session::Session` is the one concrete facade for local state used
by the daemon filesystem. It owns a unified `BlobCache` and one
`ActiveSession`; `ActiveSession` privately owns `SessionStore` and
`OverlayStore`. Callers cannot access those children. `Session::open` owns
locking, unconditional replacement of the previous session tree, fresh active
session creation, and explicit idempotent clean close. It does not validate,
reuse, migrate, or repair a previous session. `Session::control_endpoint`
discovers the fixed socket without opening SQLite, and `Session::inspect`
performs one-shot best-effort retained inspection without creating, locking,
initializing, repairing, or mutating state.

The session module uses `rusqlite`. A private embedded `schema.sql` file is the
source of truth for table shape, primary keys, indexes, and foreign keys. It
uses `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` and is
executed when the daemon opens writable state. SQLite's `user_version` records
the supported schema version.

SQL does not encode domain checks. The session module owns private Rust row
decoders and validates lifecycle values, digests, timestamps, booleans, inode
shape, node-kind field combinations, paths, and other domain invariants when
writing or reading a row. Adding or changing a persisted field requires
updating `schema.sql`, the corresponding Rust translation and validation, and
tests in the same change.

FUSE callbacks, `FilesystemService`, and all `Session` operations are
synchronous. `SessionStore` owns one mutex-protected
`rusqlite::Connection`; focused repository operations serialize through that
mutex without a worker thread or mirrored command protocol. The daemon
connection enables foreign keys and retains SQLite rollback-journal mode.
Retained inspection opens a separate read-only connection and closes it after
one validated result.

The local hierarchy uses one contextual `SessionError`; private helpers attach
the owning operation and stable entity while preserving I/O and SQLite sources.
`FilesystemService` separately owns CAS, REAPI, and orchestration errors. The
daemon maps failures to safe control responses; the client exposes stable error
codes without tonic types; only the CLI renders command errors.

`rfsd` constructs exactly one `FilesystemService` for the lifetime of its mount,
and every FUSE filesystem operation is routed through that instance. The
service is the synchronization and remote-orchestration layer around `Session`;
constructing multiple services over the same session or cache is outside the
daemon's supported concurrency model. SQLite remains authoritative for the
namespace.

The service's only mutable operation-coordination state is a keyed map that
deduplicates in-progress remote blob downloads. A digest-specific lock
serializes the cache-presence check, remote stream, verification, and admission
for that digest. This makes a separate cache check followed by writer creation
safe within the single-service daemon invariant; atomic no-clobber admission
still protects the cache against an already-existing destination. `Session`
exposes an opaque write-only `BlobWriter`; the service streams through
`BlobStore::stream_blob` and then asks `Session` to verify, sync, and atomically
admit the completed object. The async tonic implementation is bridged inside
the remote-aware daemon layer and does not make the FUSE or local-session APIs
asynchronous.

The MVP permits only one active mount session per `RFS_HOME`. Writable daemon
session startup acquires `RFS_HOME/session.lock`; if another process holds that
advisory lock, the mount fails and reports its diagnostic lock metadata when
readable. The lock is outside `RFS_HOME/session/` so replacement never removes
the inode that coordinates ownership. This simplifies daemon discovery and
prevents two cooperating daemons from sharing the same writable session state
root. Concurrent mounts require distinct `RFS_HOME` values.

`rfs snapshot`, `rfs status`, and `rfs unmount` discover the sole session through
`RFS_HOME` and do not accept a mountpoint. `Session` validates the mountpoint
supplied to `rfs mount` as an existing directory before creating session state,
then stores and reports its original spelling without canonicalizing it.

`rfs status` reports only session state:

- If a live active session exists, it talks to the daemon through the session's
  Unix control socket and reports mountpoint, root digest, daemon PID/socket,
  counters, dirty state, and snapshot blockers. Cache and session paths remain
  private implementation details and are not part of the control response or
  CLI JSON.
- If no live session exists but `RFS_HOME/session/` remains, it attempts a
  read-only inspection and reports readable metadata as `inactive`. Retained
  inspection is diagnostic only: it does not classify stale state, authorize
  reuse, or affect whether the next mount may start. Malformed, corrupt, or
  unsupported retained data may produce an inspection error.
- If `RFS_HOME` exists but contains neither live nor retained session state, it
  reports a clean no-session state. A missing `RFS_HOME` is an inspection error
  rather than an implicit no-session result.

RemoteFS does not include `rfs doctor` in the MVP. Configuration, CAS, path, FUSE, and root-digest validation happen in the commands that need them: `upload`, `mount`, `snapshot`, and `unmount`.

## Local State Layout

Default state root:

```text
$HOME/.rfs/
  session.lock
  cache/
    <2-hex-prefix>/
      <sha256-hex>-<size>
  session/
    session.db
    session.log
    overlay/
      data/
      tmp/
    control.sock
```

`RFS_HOME` is the only local-state path setting. It defaults to `$HOME/.rfs`
and is used as configured; session setup does not canonicalize it. It may
itself resolve through a symlink. Cache and session paths are fixed beneath it,
so users who need another filesystem set or symlink `RFS_HOME` rather than
overriding individual subdirectories.

`RFS_HOME` is an exclusively RemoteFS-managed state root. The supported
workflow assumes its contents were created and maintained through the daemon
session lifecycle and are not changed out of band. RemoteFS provides no
safety, integrity, compatibility, or recovery guarantees for a home that was
manually doctored or concurrently modified by a process that bypasses the
ownership-lock protocol. In particular, existing fixed children are trusted
as daemon-managed entries rather than treated as a hostile filesystem
boundary: startup does not inventory them or defend against substituted
symlinks, entry types, ownership, or permissions. Best-effort retained
inspection does not expand this contract.

The shared cache is content-addressed and may be reused across sequential mount sessions on the same runner.

One unified blob cache stores both regular-file contents and raw serialized
REAPI `Directory` messages. Paths are sharded by hash prefix to avoid very
large flat directories, and size remains in the filename so the path preserves
the complete structured digest identity. A large download streams into an
opaque temporary file beside its final shard entry; finalization verifies the
digest, syncs the file, and atomically admits it without overwriting an existing
entry. Decoded directories and inode maps are not retained as a second
authoritative namespace view.

Active session state is isolated under `RFS_HOME/session/`:

- SQLite overlay/session database.
- Local files for copied-up and newly created file contents.
- Control socket.
- Session logs and SQLite-backed session metadata.

Only one active session may exist for an `RFS_HOME` at a time. Clean unmount
transactionally marks the session `closed`, then leaves `RFS_HOME/session/` in
place for best-effort read-only inspection. Every new mount, while holding
`session.lock`, removes any previous `session/` tree and creates a fresh one.
This applies equally to cleanly closed, unclean, partial, malformed, corrupt,
and unsupported session state. A new mount never reuses, migrates, validates,
or repairs retained session data. The stable lock and shared cache are outside
the removed tree and remain in place, so verified blobs may still be reused
across mounts. Consequently, starting a new mount after an unclean exit
discards the previous unsnapshotted overlay and its session log.

Local cache eviction is deferred. The earliest MVP may provide manual pruning only. Remote CAS eviction is a deployment concern and must be disabled or capacity-provisioned during MVP evaluation.

## State Database Schema

`session/schema.sql` documents the durable tables and columns. Private
Rust row translation in the session module documents and enforces their domain
meaning.
The durable domain tables are:

| Table | Purpose |
| --- | --- |
| `session_metadata` | The single mount session and its lifecycle. |
| `inodes` | The session-stable merged namespace, including remote, local, copied-up, and tombstoned entries. |

Fields are intentionally compact:

| Table | Field | Meaning |
| --- | --- | --- |
| `session_metadata` | `singleton` | Primary key fixed to `1`. |
|  | `session_id` | Non-empty UUID identifying this mount session. |
|  | `daemon_pid` | Positive daemon process ID. |
|  | `lifecycle` | `initializing`, `active`, or `closed`. |
|  | `root_digest_hash`, `root_digest_size` | Validated SHA-256 root digest components. |
|  | `mountpoint` | Canonical absolute mountpoint. |
|  | `created_at_seconds` | Session creation time in whole seconds. |
|  | `closed_at_seconds` | Clean-close time in whole seconds; present only when closed. |
|  | `log_level`, `log_format` | Effective daemon logging settings. |
| `inodes` | `id` | Synthetic primary key; root is always `1`. |
|  | `parent_id`, `name` | Parent and UTF-8 basename. At most one visible row uses a pair; tombstoned history may retain the same pair. Root alone has no parent and an empty name. |
|  | `kind` | `file`, `directory`, or `symlink`. |
|  | `file_remote_digest` | Immutable file-content digest while bytes remain remote-backed; null after copy-up and for other kinds. |
|  | `directory_remote_digest` | Immutable serialized `Directory` digest while that representation remains reusable; null after the directory changes and for other kinds. |
|  | `symlink_target` | Exact target for a symlink; null for other kinds. |
|  | `file_overlay_path` | Relative overlay data filename for local file content; never an absolute path. |
|  | `mode` | Preserved Unix permission and supported special bits. |
|  | `mtime_seconds`, `mtime_nanos` | Signed mtime seconds and normalized nanoseconds. |
|  | `tombstone` | Whether this entry hides the remote name. |
|  | `file_content_dirty` | Whether file bytes must be hashed at snapshot; null for other kinds. |
|  | `directory_loaded` | Whether a directory's complete child set is present in `inodes`; null for other kinds. |
|  | `directory_tree_dirty` | Whether a directory must be re-encoded because it or a descendant changed; null for other kinds. |
Boolean and closed-set fields use typed Rust enums or booleans at the state
module boundary, not ad hoc strings in state operations. Rust validation
enforces digest, timestamp, lifecycle, root-inode, kind-specific nullable-field,
and clean-close invariants on reads and writes. SQL indexes enforce visible
inode name uniqueness and lookup performance; foreign keys enforce the inode
tree and parent ownership.

## FUSE Model

The MVP is Linux-only and FUSE-only.

Use `fuser` for the initial implementation. The filesystem core should be separated from the FUSE adapter enough to unit test lookup, overlay mutation, and snapshot behavior without mounting FUSE.

Initial FUSE behavior should prefer conservative correctness:

- Disable FUSE writeback cache for the MVP.
- Use short entry and attribute TTLs initially. Expiry only causes the kernel to ask `rfsd` for lookup or metadata again; it does not evict RemoteFS blob or directory caches.
- Support read-only mmap and test it explicitly.
- Do not support writable mmap in the MVP. Reject it clearly where the FUSE layer exposes enough signal; otherwise document writable mmap as outside MVP snapshot correctness guarantees.
- Add aggressive caching only after benchmark workloads identify the need.

## Inodes

RemoteFS manages synthetic session-stable inode numbers.

Rules:

- Root inode is fixed.
- Remote entries get inode rows when materialized.
- Local entries get inode rows when created.
- Rename preserves inode identity within one mounted workspace.
- Snapshot encoding does not include inode numbers.
- Inode stability is only guaranteed for the lifetime of a mount session.

## Lazy Metadata Fetch

The lazy metadata unit is one REAPI `Directory`.

Rules:

- Mount validates only the root `Directory`.
- `lookup(parent, name)` fetches the parent directory metadata if absent.
- `readdir(parent)` fetches that directory metadata if absent.
- Child directories remain as digests until accessed.
- File contents remain unfetched until read or copied up.
- Missing descendant directories fail lazily on the operation that needs them.

## Blob Read Path

Opening a remote-backed file does not fetch file contents.

The first read:

1. Checks the shared local blob cache.
2. If missing, downloads the full blob from CAS to a temp file in the target cache filesystem.
3. Hashes and size-checks the downloaded bytes.
4. Atomically admits the verified blob into the cache.
5. Serves reads from the local cache.

Partial/range serving before full verification is out of scope for MVP.

Verified cache entries are trusted by default after admission; the client does not re-hash every cached blob on read or daemon startup. A future explicit cache verification command may re-check cached content. Concurrent cache fills use per-digest in-process locks and atomic rename; duplicate downloads across separate daemon processes are tolerated in the MVP. Cached blob files are opened per FUSE `open` and the file descriptor is kept only for that FUSE file handle lifetime. A global cached-file-handle pool is deferred.

## Overlay Model

The mount daemon maintains an explicit durable overlay index in the SQLite
`inodes` table.

The overlay index tracks:

- Copied-up remote files.
- New files, directories, and symlinks.
- Deletes as tombstones.
- Renames.
- Mode changes.
- Mtime updates.
- Truncates.
- Dirty ancestors.
- Remote subtree references that remain unchanged.

The physical overlay data directory stores local file contents. It is not the only source of truth for the merged workspace.

Writes only mark file content dirty. Dirty and new files are hashed at snapshot time.

SQLite remains the source of truth for visible overlay state. Each logical filesystem mutation uses one SQLite transaction, but large file IO and remote fetches stay outside SQLite transactions where possible:

- Metadata-only operations such as `chmod`, `utimens`, `mkdir`, unlink/tombstone, and rename metadata commit as one SQLite transaction.
- Local file create, truncate, and write update file data first, then commit metadata and dirty-state in one SQLite transaction after the file operation succeeds.
- First-write copy-up ensures the verified blob cache, copies the remote blob to a temporary overlay file, applies the requested write or truncate, atomically renames the temp file into the overlay data directory, then commits one SQLite transaction recording copied-up state, dirty content, and dirty ancestors.
- If the SQLite commit fails after overlay file rename, the orphan overlay file is not visible without database state. A cleanup path may remove unreferenced overlay files later.
- Rename and delete directory mutations update all affected overlay rows and dirty ancestors in one SQLite transaction.
- Snapshot opens a short read transaction only after the snapshot barrier passes, so it sees a consistent overlay graph.

A remote digest remains present only while it still identifies the inode data
that snapshot may reuse. A file-content mutation atomically replaces
`file_remote_digest` with `file_overlay_path`. A directory metadata or child-set
mutation first materializes the complete directory, then clears its
`directory_remote_digest`; every dirty directory ancestor is likewise fully
materialized and loses its stale digest. File rename and metadata-only changes
retain `file_remote_digest` because the referenced bytes did not change, while
the affected parent directory digests are cleared. REAPI symlinks have no
standalone CAS object or digest: their target and metadata live in the parent
`Directory`, so symlink replacement or metadata change clears the affected
parent and ancestor directory digests.

## Copy-on-Write

Remote snapshots are immutable.

The first content mutation of a remote-backed file performs whole-file
copy-on-write:

1. Ensure the remote blob is present in the verified local cache.
2. Copy the full blob into the session overlay data directory.
3. Apply the write or truncate locally.
4. Mark the file and affected ancestors dirty in SQLite.

Metadata-only file mutations do not copy file bytes and retain the reusable
file-content digest. They update the inode metadata and invalidate the affected
directory encodings in one SQLite transaction.

Whole-file COW is the only MVP write strategy. Large-file COW emits structured warnings by default and proceeds with copy-up; a configurable hard size guardrail can be added later as an explicit opt-in.

## Delete and Rename

Deletes are tombstone-based and session-local. They never remove objects from remote CAS or the shared local cache.

Rename semantics are scoped to one mounted workspace:

- File rename within the same mount is supported.
- Empty-directory rename within the same mount is supported.
- Directory rename within the same mount is supported when the destination does not exist.
- Rename over existing files follows normal Unix replacement behavior.
- Rename over non-empty directories is rejected.
- File-over-directory and directory-over-file renames are rejected with normal Unix-style errors.
- Moving a directory into itself or one of its descendants is rejected.
- Cross-mount atomic rename is unsupported and should return a clear cross-device error if encountered.
- Rename preserves inode identity for the renamed source within one mounted workspace.
- If the destination existed, its old path becomes tombstoned/replaced in overlay state.
- Snapshot reflects final path state, not rename history.
- Open file handles follow Unix semantics: an already-open file handle remains usable for that file object, while path lookup observes the new name or replacement.

## Symlinks and Hard Links

Symlinks are preserved exactly by default:

- Upload and snapshot store symlinks as `SymlinkNode`.
- Symlink targets are not followed.
- Absolute, relative, escaping, and broken symlinks are preserved.
- Upload/snapshot should warn or count absolute and workspace-escaping symlinks.
- A strict mode may fail on absolute or escaping symlinks.

Hard-link identity is not preserved:

- Hard-linked regular files are uploaded as ordinary files.
- Identical file contents deduplicate by blob digest.
- Mutating one path after mount does not mutate another formerly hard-linked path.
- Upload/snapshot should warn or count detected hard links.

## Timestamps and Modes

Mtime preservation is mandatory for supported files and directories.

Implementation rules:

- Store timestamps internally as signed seconds plus normalized nanoseconds, where nanoseconds are always `0..999_999_999`.
- Store SQLite timestamp fields as integer seconds and integer nanoseconds, not text.
- Preserve valid pre-1970 mtimes; negative timestamp seconds represent times before the Unix epoch.
- Encode mtimes into REAPI `NodeProperties` using protobuf `Timestamp`.
- Reject timestamps outside the protobuf `Timestamp` range or with invalid nanoseconds using structured unsupported-metadata errors.
- Do not silently clamp or truncate timestamp values.
- Preserve source filesystem mtimes on upload.
- Report mtimes through FUSE `getattr`.
- Support explicit timestamp updates through FUSE operations.
- Preserve exactly the precision reported by the local OS/filesystem, including nanosecond precision when available.

Unix mode is represented primarily by `NodeProperties.unix_mode`. `FileNode.is_executable` is derived for compatibility.

Mode rules:

- File type is represented by the REAPI node kind, not by the stored mode contract.
- Preserve permission bits `0o777` for regular files, directories, and symlinks where the platform reports symlink mode.
- Preserve sticky bit `0o1000` on directories.
- Do not preserve setuid `0o4000` or setgid `0o2000` in the MVP. Mask them out and emit a warning/count on upload or snapshot.
- Ignore UID/GID ownership in the MVP.
- Derive `FileNode.is_executable` from any executable bit in the final stored regular-file mode: `(mode & 0o111) != 0`.
- Expose stored modes through FUSE `getattr`, subject to normal FUSE/kernel behavior.
- `chmod` through the mounted workspace updates only supported mode bits. Attempts to set setuid or setgid are masked with a warning or clear unsupported-metadata response.

## Snapshot

Mounted workspace snapshotting goes through the live `rfsd` process.

`rfs snapshot`:

1. Discovers the active session from `RFS_HOME` and sends a snapshot request over the Unix control socket.
2. Daemon enters a short snapshot barrier.
3. If writable handles or in-flight mutations are active, snapshot fails.
4. Daemon walks the overlay graph and dirty ancestors.
5. Unchanged remote-backed blobs and subtrees reuse existing digests without rechecking CAS.
6. Dirty/new files are streamed, hashed, checked with `FindMissingBlobs`, and uploaded if missing.
7. New or changed `Directory` nodes are encoded canonically and uploaded if missing.
8. Daemon returns the new root digest.

`rfs snapshot` is daemon-session-only in the MVP. If no active RemoteFS session exists, it fails clearly and points users to `rfs upload <local-dir>` for ordinary local directories. `rfs snapshot <local-dir>` is not an alias for upload.

The daemon tracks FUSE file handles in memory. Handles opened with write capability block snapshot until release, even if no write has happened. Read-only handles and read-only mmap do not block snapshot. Each mutation call commits its own SQLite dirty-state before returning; RemoteFS does not defer overlay metadata commits to `flush` or `fsync`. `flush` and `fsync` only synchronize already-written local overlay file bytes and surface local IO errors. Snapshot blocking is based on writable handles and currently executing mutation calls, not flush/fsync state.

Earliest writable MVP snapshots the full workspace only. Path selection and include/exclude filters are deferred.

## Upload

`rfs upload <local-dir>` is the bootstrap ingestion path.

Upload walks the local filesystem directly and uses the same canonical tree encoder as mounted snapshot. It does not interpret Git metadata, `.gitignore`, or source-control state.

Upload does not follow symlinks by default.

The implemented upload pipeline is:

1. A recursive scanner records all directory metadata and entries without following symlinks. It sorts entries by raw filename bytes, rejects non-UTF-8 entry names and symlink targets, and fails on unsupported nodes by default.
2. The scanner records each regular-file path even when it shares a Unix `(dev, ino)` identity with an earlier path. Such additional paths increment the hard-link warning count; hard-link identity is not encoded.
3. Regular files are hashed by blocking tasks with at most `min(available_parallelism, 8)` concurrent workers and a minimum of 2. Each task streams with a reusable buffer of at most 1 MiB; the 64 MiB default in-flight setting is divided across workers to size those buffers. This is a per-worker buffer bound, not yet a queue-wide byte-accounting or backpressure mechanism.
4. The upload fails if a file's byte count changes between scan metadata and hashing. Completed digests are sorted by relative path before bottom-up directory encoding.
5. Directories are encoded bottom-up with the shared canonical encoder. File blobs and serialized `Directory` objects are deduplicated by digest, checked once with `FindMissingBlobs`, then submitted through `BlobStore`.
6. The CAS client owns the current batch-versus-ByteStream choice and batching policy. Upload does not yet impose a separate ByteStream concurrency cap or an upload worker pool.

Canonical directory encoding and the root digest are independent of filesystem traversal and hashing completion order. The current object submission order is not a user-visible determinism guarantee.

`rfs upload` writes the root digest alone to stdout in text mode. In JSON mode it writes one versioned success envelope containing the root digest. Entry, object, byte, warning, and timing summaries are operation-level logs on stderr rather than stable command-result fields. JSON failures are emitted as one versioned envelope on stderr.

## Observability

Structured logs and command summaries are the first observability layer.

The completed architecture-boundary slice supports:

- Four standard levels: error, warn, info, and debug.
- Text or JSON Lines process logging configured once at startup.
- CLI logs on stderr and command results on stdout.
- Daemon logs in `RFS_HOME/session/session.log` after state layout establishment.
- Lifecycle and upload-summary events with structured operation fields.
- No routine per-file lookup, traversal, cache-probe, read, or write event stream.

The later MVP observability work should extend this with:

- CLI logs go to stderr only. Command results and JSON summaries go to stdout.
- `rfsd` logs remain inspectable with the active or retained session until the
  next mount replaces that session tree or the user manually removes it.
- Human-readable compact text logs by default.
- JSON Lines logs and JSON command summaries via `--output-format json`; text logs and human command summaries via `--output-format text`.
- `--log-level` and `--output-format` apply to both `rfs` and any `rfsd` process spawned by `rfs mount`.
- Effective daemon log level and format are written into the SQLite
  `session_metadata` table so `rfs status` can report them.
- No log rotation in the MVP.
- `rfs status`.
- `rfs status --output-format json`.
- `--output-format json` command summaries for `status`, `upload`, and `snapshot`.
- JSON command summaries use a stable envelope with `schema_version`, `command`, `ok`, `warnings`, `error`, and command-specific `data`.
- JSON field names and types are stable within a schema version. Later versions may add optional fields, but removing or changing existing fields requires bumping `schema_version`.
- On `--output-format json` failure, commands print one JSON object for machine consumers; logs remain separate.
- Per-session counters for:
  - Directory nodes fetched.
  - Blobs fetched.
  - Bytes fetched.
  - Cache hits and misses.
  - Blobs uploaded.
  - Bytes uploaded.
  - Upload deduplication ratio.
  - Snapshot duration.
  - Remote errors.
  - Digest verification failures.

A metrics endpoint is secondary.

Daemon log events should include at least timestamp, level, target/module, session id, operation when available, path or digest when relevant, and message.

## Milestones

### Milestone 1: Read-Only Lazy Mount

End-to-end path:

```text
local dir -> rfs upload -> REAPI CAS -> rfs mount -> lazy read -> remount same root
```

Scope:

- Regular files.
- Directories.
- REAPI root digest format.
- `bazel-remote` CAS.
- Lazy directory fetch.
- Whole-blob fetch and verification on first read.
- Read-only FUSE mount.

### Milestone 2: Writable COW Snapshot

Scope:

- `rusqlite`-backed SQLite overlay index.
- Synthetic inode stability.
- Create/write/delete/rename.
- Whole-file COW.
- Full-workspace snapshot through `rfsd`.
- Snapshot/remount equivalence tests.

## Deferred Design Topics

- Safe remote CAS retention and garbage collection.
- Local cache eviction policy.
- Object-store-backed CAS deployment.
- Authentication and TLS.
- Metrics endpoint.
- Path-selected snapshots.
- Include/exclude filters.
- Block-level or chunked COW.
- Strong crash recovery for in-flight writable sessions.
- Minimum supported Linux kernel and FUSE versions.
- Benchmark workload selection.
