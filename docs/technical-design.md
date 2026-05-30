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
- Use SQLite for durable session and overlay state.
- Allow at most one active mount session per `RFS_HOME`.
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

Entries must use REAPI canonical ordering. Upload and snapshot must share the same encoder so bootstrap uploads and mounted workspace snapshots produce compatible trees.

## CAS Target

The default development and evaluation target is `bazel-remote`.

RemoteFS uses the Remote Execution API `instance_name` field as the CAS namespace selector. The MVP supports a configurable `--instance-name`, defaulting to the empty instance name for simple `bazel-remote` development setups. RemoteFS should not introduce a separate storage namespace concept such as "project"; CI/CD systems remain responsible for mapping their own projects, repositories, commits, cache keys, and builds to root digests and CAS instance configuration.

MVP deployments must run the remote CAS with eviction disabled or enough capacity to keep all objects reachable from snapshots they intend to reuse. REAPI CAS does not provide snapshot retention roots or safe reachability garbage collection by itself.

Root digests are therefore capability references. They are valid only while the CAS still contains the root object and all reachable descendants.

## Process Model

`rfs` is the user-facing CLI. Expected MVP commands:

```sh
rfs upload <local-dir>
rfs mount <root-digest> <mountpoint>
rfs snapshot [mountpoint]
rfs unmount [mountpoint]
rfs status [mountpoint]
rfs cleanup
rfs doctor
```

`rfsd` owns:

- The FUSE mount.
- The mounted root digest.
- The CAS client.
- The shared local cache handles.
- The active session SQLite database.
- The active session overlay data directory.
- The Unix control socket.

`rfs mount` starts `rfsd` in the background by default and returns only after the root directory is validated, the FUSE mount is active, and the control socket is reachable. Direct `rfsd` invocation runs in the foreground unless supervised externally.

The MVP permits only one active mount session per `RFS_HOME`. `rfs mount` acquires an active-session lock under `RFS_HOME` before starting `rfsd`; if another live session owns that lock, the mount fails and reports the active session metadata. This simplifies daemon discovery and prevents two writable overlays from sharing the same active state root. Concurrent mounts can still be run by using distinct `RFS_HOME` values.

`rfs snapshot`, `rfs status`, and `rfs unmount` talk to the live daemon through the active session's Unix control socket discovered from `RFS_HOME`. Their mountpoint argument is optional in the MVP. If supplied, the CLI validates that it matches the active session mountpoint before sending the request.

## Local State Layout

Default state root:

```text
$HOME/.rfs/
  cache/
    blobs/
      <2-hex-prefix>/
        <sha256-hex>-<size>
    dirs/
      <2-hex-prefix>/
        <sha256-hex>-<size>
  active/
    session.db
    overlay/
    control.sock
    metadata.json
  logs/
```

Config overrides:

- `RFS_HOME`
- `RFS_CACHE_DIR`
- `RFS_SESSION_DIR`

The shared cache is content-addressed and may be reused across sequential mount sessions on the same runner.

Blob and directory cache paths are sharded by hash prefix to avoid very large flat directories. The size is included in the filename so the cache path preserves the full structured digest identity. Directory cache entries store raw serialized REAPI `Directory` bytes keyed by digest; decoded forms may be cached in memory but are not persisted as an internal representation.

Active session state is isolated under `RFS_HOME/active/`:

- SQLite overlay/session database.
- Local files for copied-up and newly created file contents.
- Control socket.
- Session logs and metadata.

Only one active session may exist for an `RFS_HOME` at a time. Clean unmount leaves `RFS_HOME/active/` in place for inspection, including the session database, logs, and overlay data. A later `rfs mount` fails if stale active state exists without a live lock and tells the operator to run `rfs cleanup`. `rfs cleanup` refuses to run while a live active-session lock exists, and removes stale `RFS_HOME/active/` when no session is active.

Local cache eviction is deferred. The earliest MVP may provide manual pruning only. Remote CAS eviction is a deployment concern and must be disabled or capacity-provisioned during MVP evaluation.

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

The mount daemon maintains an explicit durable overlay index in SQLite.

The overlay index tracks:

- Copied-up remote files.
- New files and directories.
- Deletes as tombstones.
- Renames.
- Mode changes.
- Mtime updates.
- Truncates.
- Dirty ancestors.
- Remote subtree references that remain unchanged.

The physical overlay data directory stores local file contents. It is not the only source of truth for the merged workspace.

Writes only mark file content dirty. Dirty and new files are hashed at snapshot time.

## Copy-on-Write

Remote snapshots are immutable.

The first mutation of a remote-backed file performs whole-file copy-on-write:

1. Ensure the remote blob is present in the verified local cache.
2. Copy the full blob into the session overlay data directory.
3. Apply the write, truncate, or metadata mutation locally.
4. Mark the file and affected ancestors dirty in SQLite.

Whole-file COW is the only MVP write strategy. Large-file COW should emit structured diagnostics and may later support a configurable size guardrail.

## Delete and Rename

Deletes are tombstone-based and session-local. They never remove objects from remote CAS or the shared local cache.

Rename semantics are scoped to one mounted workspace:

- File rename within the same mount is supported.
- Directory rename within the same mount is supported.
- Rename over existing files follows normal Unix replacement behavior.
- Rename over non-empty directories is rejected.
- Cross-mount atomic rename is unsupported and should return a clear cross-device error if encountered.
- Snapshot reflects final path state, not rename history.

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

- Store timestamps as seconds and nanoseconds internally.
- Store SQLite timestamp fields as integer seconds and integer nanoseconds, not text.
- Preserve source filesystem mtimes on upload.
- Report mtimes through FUSE `getattr`.
- Support explicit timestamp updates through FUSE operations.
- Determine and document public precision guarantees after tests cover upload, mount, COW, snapshot, and remount.

Unix mode is represented primarily by `NodeProperties.unix_mode`. `FileNode.is_executable` is derived for compatibility.

## Snapshot

Mounted workspace snapshotting goes through the live `rfsd` process.

`rfs snapshot [mountpoint]`:

1. Discovers the active session from `RFS_HOME`, optionally validates the supplied mountpoint, and sends a snapshot request over the Unix control socket.
2. Daemon enters a short snapshot barrier.
3. If writable handles or pending writes are active, snapshot fails.
4. Daemon walks the overlay graph and dirty ancestors.
5. Unchanged remote-backed blobs and subtrees reuse existing digests.
6. Dirty/new files are streamed, hashed, checked with `FindMissingBlobs`, and uploaded if missing.
7. New or changed `Directory` nodes are encoded canonically and uploaded if missing.
8. Daemon returns the new root digest.

Earliest writable MVP snapshots the full workspace only. Path selection and include/exclude filters are deferred.

## Upload

`rfs upload <local-dir>` is the bootstrap ingestion path.

Upload walks the local filesystem directly and uses the same canonical tree encoder as mounted snapshot. It does not interpret Git metadata, `.gitignore`, or source-control state.

Upload does not follow symlinks by default.

## Observability

Structured logs and command summaries are the first observability layer.

MVP should support:

- Human logs by default.
- JSON logs via flag.
- `rfs status`.
- `rfs status --json`.
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

- SQLite overlay index.
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
