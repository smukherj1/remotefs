# RemoteFS Implementation Plan

This plan breaks the PRD.md and docs/technical-design.md into small implementation steps. Each step should leave the repo buildable, add tests at the lowest useful layer first, and then add integration or end-to-end coverage as soon as the behavior crosses a process, CAS, daemon, or FUSE boundary.

The intended implementation order is:

1. Project boilerplate, REAPI protos, dependencies, and Task workflow.
2. Local development CAS server.
3. CAS client and snapshot encoding primitives.
4. Bootstrap `rfs` CLI commands.
5. `rfsd` process, local state, and control socket.
6. Read-only lazy filesystem core and FUSE mount.
7. Writable copy-on-write overlay.
8. Snapshot upload through the daemon.
9. Observability, hardening, and evaluation tests.

## Ground Rules

- Use `task` as the only documented workflow runner. Do not add `Makefile` targets.
- Keep all behavior that can be tested without FUSE in ordinary Rust modules.
- Treat local integration and end-to-end tests as mandatory for the full test workflow. FUSE tests should fail with a clear prerequisite error when `/dev/fuse` or required mount permissions are unavailable.
- Prefer small PR-sized slices where each slice has a runnable `task test:*` target.
- Use `bazel-remote` as the default integration CAS target, with Buildbarn compatibility deferred until the main REAPI path is stable.
- Keep the repo Linux-focused for the MVP, but make unit tests runnable on ordinary developer machines where possible.

## Initial Assumptions

- MVP implementation targets Linux x86_64 with FUSE.
- `rfs upload <local-dir>` is the only plain local-directory ingestion command.
- `rfs snapshot [mountpoint]` is daemon-session-only and requires an active RemoteFS session under `RFS_HOME`.
- `--instance-name` is required and non-empty for CAS and ByteStream requests.
- `--cas-url` requires an explicit URI scheme. MVP supports `grpc://`; `grpcs://` is deferred.
- The default local CAS for development and evaluation is `bazel-remote`; Buildbarn remains a documented secondary compatibility target until the main REAPI path is stable.
- SQLite state uses `rusqlite` in daemon-owned blocking sections.
- Generated Rust proto code is produced during build from checked-in pinned proto sources under `third_party/remote-apis/`.
- Process exit codes are simple: `0` for success and `1` for any error.
- Strong crash recovery, automatic local cache eviction, TLS, auth, writable mmap, block-level COW, and Buildbarn smoke tests are outside the first MVP implementation sequence.

## Phase 0: Boilerplate and Workflow

### Step 0.1: Crate Layout

Deliverables:

- Restructure from two standalone binaries into a workspace-friendly layout:
  - `src/bin/rfs.rs` remains the CLI entrypoint.
  - `src/bin/rfsd.rs` remains the daemon entrypoint.
  - `src/lib.rs` exposes shared modules.
  - Initial modules: `config`, `digest`, `reapi`, `cas`, `tree`, `upload`, `state`, `control`, `fs`.
- Add dependency groups for:
  - Async runtime and gRPC: `tokio`, `tonic`, `prost`, `bytes`.
  - CLI/config/logging: `clap`, `serde`, `serde_json`, `toml`, `tracing`, `tracing-subscriber`.
  - Errors and utilities: `anyhow`, `thiserror`, `sha2`, `hex`, `tempfile`, `walkdir`.
  - Daemon/FUSE/state: `fuser`, `libc`, `rusqlite`, `uuid`.
  - Tests: `assert_cmd`, `predicates`, `insta` or golden-file helpers.
- Add feature flags if needed:
  - `fuse` for FUSE-specific code.
  - `integration` only if integration tests need opt-in compile-time behavior.

Task targets:

```sh
task fmt
task lint
task build
task test
task test:unit
```

Tests:

- Unit: digest parser skeleton tests.
- Unit: config path resolution tests for `RFS_HOME`, `RFS_CACHE_DIR`, and `RFS_SESSION_DIR`.
- Integration/e2e test setup: verify local test prerequisites where needed, including Docker for CAS tests and `/dev/fuse` plus mount permissions for FUSE tests. Missing prerequisites fail clearly from the test target that needs them.
- Smoke: `task build` produces both `rfs` and `rfsd`.

Definition of done:

- `task fmt`, `task lint`, `task build`, and `task test:unit` pass.
- Both binaries print real CLI help instead of placeholder messages.

### Step 0.2: Proto Setup

Deliverables:

- Add pinned Remote Execution API and ByteStream protos.
- Vendor the pinned REAPI source files under `third_party/remote-apis/`.
- Add RemoteFS-owned protobuf definitions for the local daemon control API.
- Generate Rust code with `tonic-build` and `prost-build`.
- Pin `prost` and `tonic` versions.
- Include only the proto surface needed by the MVP:
  - `build.bazel.remote.execution.v2.Digest`
  - `Directory`, `FileNode`, `DirectoryNode`, `SymlinkNode`
  - `NodeProperties`
  - CAS service methods used for upload/download/existence checks
  - ByteStream read/write messages
- Include control service methods used by `rfs` and `rfsd`:
  - `Status`
  - `Snapshot`
  - `Unmount`
  - protocol/version metadata
- Document how protos are pinned and updated.

- Check in proto source files, not generated Rust, unless build reproducibility becomes painful.

Task targets:

```sh
task proto:check
task proto:generate
task build
```

Tests:

- Unit: encode a minimal `Directory` and verify the resulting digest is stable.
- Unit: representative encoded `Directory` objects have golden digests.
- Unit: verify canonical digest string formatting as `sha256:<hash>/<size>`.
- Unit: control service request/response messages encode and decode expected fields.

Definition of done:

- Generated code is available to the crate during `cargo build`.
- Remote API update mechanics and RemoteFS control proto evolution rules are documented in the Taskfile or a short doc comment.

## Phase 1: Development CAS Server

### Step 1.1: Bazel-Remote Docker Workflow

Deliverables:

- Add `docker-compose.yml` or equivalent container config for local `bazel-remote`.
- Configure local disk cache storage under a disposable dev directory.
- Disable or effectively avoid eviction for MVP testing, or document capacity behavior clearly.
- Expose a stable local gRPC endpoint, for example `grpc://127.0.0.1:9092`.

Task targets:

```sh
task cas:up
task cas:down
task cas:logs
task cas:reset
```

Tests:

- Integration: upload and download a small blob through raw CAS client plumbing once Step 2.1 exists. The test target fails clearly if the local CAS container is unavailable.

Definition of done:

- A developer can run `task cas:up` and then `task test:integration:cas` without reading container docs.

## Phase 2: CAS Client and Snapshot Primitives

### Step 2.1: Digest and CAS Client

Status: implemented, with review cleanup required before building later filesystem code on this surface.

Deliverables:

- Implement `Digest` as a first-class internal type with:
  - SHA-256 hash validation.
  - Size validation.
  - Parse and display for root digest strings.
  - Conversion to and from REAPI proto `Digest`.
- Implement CAS client operations:
  - `find_missing_blobs`.
  - `batch_update_blobs` for small blobs and tree nodes.
  - ByteStream upload for larger blobs.
  - `batch_read_blobs` for small downloads.
  - ByteStream read for larger downloads.
- Default the batch/ByteStream split at 4 MiB, with a conservative request-overhead allowance so protobuf and gRPC framing overhead cannot push a batch request over the practical limit.
- Make the threshold configurable for compatibility testing against different CAS deployments.
- Pack `BatchUpdateBlobs` requests for small blobs and directory nodes:
  - Default request payload budget is 4 MiB minus a small reserved overhead allowance.
  - Add entries using a running payload-size total until the next entry would exceed the budget, then start a new request.
  - Send a near-threshold entry through ByteStream if it cannot fit in the batch budget by itself.
  - Do not require deterministic entry order inside a batch request; it does not affect CAS state, blob digests, tree digests, or final summaries.
- Add digest verification for downloaded blobs before cache admission.
- Use standard uncompressed ByteStream resource names:
  - Read: `{instance_name}/blobs/{hash}/{size}`.
  - Write: `{instance_name}/uploads/{uuid}/blobs/{hash}/{size}`.
  - Require non-empty `instance_name`; do not implement empty-instance resource names.
  - Generate a fresh UUID v4 per ByteStream upload attempt.
  - Do not use compressed ByteStream resource names, digest-function prefixes, or optional metadata in the MVP.
  - Validate `instance_name` early and reject reserved REAPI resource keywords as path segments, including `blobs`, `uploads`, and `compressed-blobs`.
- Add CAS retry and timeout policy:
  - Per-attempt timeout defaults are 10 seconds for `FindMissingBlobs` and 30 seconds for `BatchReadBlobs` and `BatchUpdateBlobs`.
  - ByteStream read/write uses a 30 second idle timeout instead of one whole-stream timeout in the MVP.
  - Retry only transient transport/server failures: unavailable, deadline exceeded, resource exhausted, aborted, and internal errors that indicate a transport reset.
  - Do not retry semantic or authorization failures: not found after mount validation, invalid argument, permission denied, unauthenticated, digest mismatch, and failed precondition.
  - Use exponential backoff with jitter: 100 ms, 250 ms, 500 ms, 1 second, and at most 5 attempts by default.
  - ByteStream retries restart from byte 0 in the MVP.
  - Final errors include operation name, digest or resource name when relevant, attempt count, CAS URL, and `instance_name`.
- Keep the CAS client public API narrow:
  - Public methods are documented with arguments, return values, and error behavior.
  - ByteStream resource-name builders, download verification, batch packing, and transport helpers remain private unless another module has a concrete need for them.
  - Large public methods delegate to private helpers for batch read, ByteStream read, batched upload, and ByteStream upload so the control flow remains readable.
- Represent CAS operation names with a typed enum or equivalent closed set instead of matching hardcoded strings for timeout and retry behavior.
- Validate retry configuration when constructing or connecting the CAS client:
  - `max_attempts` must be at least 1.
  - `max_attempts` must not exceed the package maximum used by the backoff schedule.

Task targets:

```sh
task test:unit
task test:integration:cas
```

Tests:

- Unit: digest parse rejects uppercase hash, missing size, non-SHA-256 prefixes, invalid hex, negative size, and size overflow.
- Unit: hash computation over bytes returns expected digest.
- Unit: downloaded blob verifier rejects hash or size mismatch.
- Unit: ByteStream resource names include the non-empty `instance_name` for read and write.
- Unit: `instance_name` validation rejects empty values and reserved path segments.
- Unit: batch packing respects the request payload budget and moves oversized near-threshold entries to ByteStream.
- Unit: retry classifier retries transient errors and does not retry semantic or authorization errors.
- Unit: timeout configuration applies the expected per-operation defaults.
- Unit: retry attempt validation rejects zero and values above the supported maximum.
- Unit: CAS operation timeout selection is covered without stringly typed operation names.
- Integration: against `task cas:up`, upload, existence-check, download, and verify blobs.
- Integration: idempotent re-upload of the same digest succeeds.

Definition of done:

- CAS calls are hidden behind a small trait so core filesystem tests can use an in-memory fake CAS.
- CAS configuration requires a non-empty REAPI `instance_name` and sends it consistently on CAS and ByteStream calls.
- Review comments in `src/cas.rs` are resolved and removed, either by implementing the requested change or by replacing the comment with a durable explanation of the chosen design.

### Step 2.2: Canonical Tree Encoding

Deliverables:

- Implement the constrained REAPI `Directory` encoder/decoder.
- Enforce canonical entry ordering for files, directories, and symlinks.
- Preserve supported metadata:
  - File type.
  - File size.
  - Unix mode.
  - Executable bit.
  - Mtime signed seconds and normalized nanoseconds.
  - Symlink target.
- Require UTF-8 path component names and symlink targets because REAPI stores them as protobuf strings.
- Encode empty directories as normal `Directory` objects that must exist in CAS.
- Timestamp rules:
  - Preserve nanosecond precision when reported by the local OS/filesystem.
  - Preserve valid pre-1970 mtimes; negative timestamp seconds represent times before the Unix epoch.
  - Encode mtimes into REAPI `NodeProperties` using protobuf `Timestamp`.
  - Reject timestamps outside the protobuf `Timestamp` range or with invalid nanoseconds using structured unsupported-metadata errors.
  - Do not silently clamp or truncate timestamp values.
- Unix mode rules:
  - Store file type through REAPI node kind, not through the mode contract.
  - Preserve permission bits `0o777` for regular files, directories, and symlinks where the platform reports symlink mode.
  - Preserve sticky bit `0o1000` on directories.
  - Mask setuid `0o4000` and setgid `0o2000` and emit a warning/count on upload or snapshot.
  - Ignore UID/GID ownership.
  - Derive `FileNode.is_executable` from `(mode & 0o111) != 0` for regular files.
- Reject or warn on unsupported file types according to CLI mode.
- Preserve symlinks exactly, including absolute and escaping targets; emit warning counts for risky targets.

Task targets:

```sh
task test:unit
task test:integration:cas
```

Tests:

- Unit: canonical ordering is stable regardless of traversal order.
- Unit: mode, executable bit, and mtime round-trip through REAPI objects.
- Unit: permission bits and directory sticky bit round-trip through REAPI objects.
- Unit: setuid/setgid are masked and counted as warnings.
- Unit: `FileNode.is_executable` is derived from stored executable bits.
- Unit: pre-epoch, epoch, nanosecond, and far-future valid mtimes have stable golden digests.
- Unit: out-of-range timestamps return structured unsupported-metadata errors.
- Unit: symlink targets are stored as symlink nodes and are not followed.
- Unit: absolute and escaping symlinks round-trip with warnings.
- Unit: non-UTF-8 names or symlink targets return structured unsupported-metadata errors.
- Unit: empty directories encode to stable `Directory` digests.
- Unit: unsupported node types return structured errors.
- Integration: upload encoded tree nodes to CAS and fetch/decode them by digest.

Definition of done:

- `rfs upload` and mounted snapshot code share this encoder and can build on a common tree-writer abstraction.

## Phase 3: Bootstrap CLI

### Step 3.1: CLI Skeleton and Config

Deliverables:

- Implement `rfs` command structure:
  - `rfs upload <local-dir>`
  - `rfs mount <root-digest> <mountpoint>`
  - `rfs snapshot [mountpoint]`
  - `rfs unmount [mountpoint]`
  - `rfs status [mountpoint]`
  - `rfs cleanup`
- Do not include `rfs doctor` in the MVP; command-specific validation lives in the commands and test targets that need it.
- Implement common flags:
  - `--cas-url`
  - `--instance-name`
  - `--json`
  - `--log-level`
  - `--log-format text|json`
  - `--cache-dir`
  - `--session-dir`
- Use simple process exit codes: `0` for success and `1` for any error. Put detailed error categories in human and JSON diagnostics instead of numeric exit codes.
- Validate `--cas-url` with an explicit scheme; accept `grpc://` in the MVP and reserve `grpcs://`.

Task targets:

```sh
task build
task test:unit
task test:cli
```

Tests:

- Unit: config precedence from CLI flags and environment variables.
- Unit: configuration rejects missing or empty `--instance-name`.
- Unit: configuration rejects missing CAS URL schemes and unsupported schemes.
- Unit: CAS URL and REAPI `instance_name` are included in every CAS and ByteStream request.
- Unit: command parsing for every expected command.
- Unit: optional mountpoint arguments validate against active-session metadata when supplied.
- CLI: `rfs cleanup` refuses to remove active state while a live session lock exists.
- CLI: `rfs --help`, subcommand help, and invalid digest errors via `assert_cmd`.

Definition of done:

- Every MVP command exists, even if some commands return a clear "not implemented yet" error.

### Step 3.2: `rfs upload`

Deliverables:

- Walk local directories without following symlinks.
- Include every entry under the supplied directory; no include/exclude or `.gitignore` semantics in the MVP.
- Fail on unsupported filesystem nodes by default.
- Detect hard links, warn/count them, and store each path as an ordinary file.
- Feed local filesystem entries into the shared tree-writer abstraction; keep local traversal separate from mounted overlay traversal.
- Hash and upload file blobs through a bounded deterministic pipeline:
  - Use one filesystem walker to record metadata and path structure.
  - Stream file hashing through a bounded worker pool. Default workers are `min(available_parallelism, 8)`, with a minimum of 2.
  - Run `FindMissingBlobs` after digests are known.
  - Upload missing small blobs with `BatchUpdateBlobs`.
  - Upload larger blobs with ByteStream using a separate default concurrency cap of 4.
  - Limit buffered in-flight payloads to 64 MiB by default with bounded channels and byte accounting.
  - Reread large ByteStream uploads from disk instead of holding whole files in memory.
- Encode and upload directories bottom-up.
- Use `FindMissingBlobs` before uploading.
- Print the root digest.
- Emit counts for files, dirs, symlinks, uploaded blobs, reused blobs, bytes uploaded, warnings.

Task targets:

```sh
task test:unit
task test:integration:upload
task fixture:upload
```

Tests:

- Unit: filesystem walker handles regular files, dirs, symlinks, mtimes, executable bit, unsupported types.
- Unit: unsupported nodes fail upload by default.
- Unit: hard links are counted and represented as ordinary files.
- Unit: tree encoder produces identical root digest for identical trees.
- Unit: worker completion order does not affect root digest or JSON summary ordering.
- Unit: upload backpressure enforces the configured in-flight byte budget.
- Unit: symlink warnings/counts are stable.
- Integration: upload a fixture directory to `bazel-remote`, fetch all reachable objects, and verify metadata.
- E2E-lite: `task fixture:upload` uploads a checked-in fixture and prints a root digest.

Definition of done:

- A local directory can be ingested into the CAS and represented entirely by one root digest.

## Phase 4: Daemon Foundation

### Step 4.1: Local State and SQLite Session Store

Deliverables:

- Implement `$RFS_HOME` layout:
  - Shared blob cache.
  - Shared directory cache.
  - Active session directory.
  - Active session logs under `RFS_HOME/active/logs/`.
- Use sharded cache paths by hash prefix, for example `cache/blobs/aa/<hash>-<size>` and `cache/dirs/aa/<hash>-<size>`.
- Store raw serialized REAPI `Directory` bytes in the directory cache; keep decoded directory objects in memory only.
- Create one active session under `RFS_HOME/active/`.
- Add an active-session lock so at most one `rfsd` can own an `RFS_HOME` at a time.
- Preserve `RFS_HOME/active/` after clean unmount for inspection.
- Implement `rfs cleanup` to remove stale `RFS_HOME/active/` only when no live active-session lock exists.
- `rfs cleanup` does not prune shared blob or directory caches; cache pruning is a future separate command.
- Add SQLite schema migrations for:
  - Session metadata.
  - Inode table.
  - Remote directory materialization table.
  - Overlay entry table placeholders.
  - Counters.
- Add startup validation and lock handling so a second mount using the same `RFS_HOME` fails clearly while an active session is live.
- Add startup validation so stale `RFS_HOME/active/` without a live lock fails the next mount with an `rfs cleanup` hint.

Task targets:

```sh
task test:unit
task test:integration:state
```

Tests:

- Unit: state path layout honors env overrides.
- Unit: blob and directory cache path derivation is stable and includes hash prefix plus size.
- Unit: migrations are idempotent.
- Unit: inode allocation preserves root inode.
- Integration: create, close, and reopen a session database.
- Integration: clean unmount leaves active session state available for inspection.
- Integration: stale active state blocks the next mount until `rfs cleanup` runs.
- Integration: `rfs cleanup` removes stale active state and refuses to run while a live lock exists.

Definition of done:

- `rfsd` can initialize a durable session and print/log its state paths.

### Step 4.2: Control Socket Protocol

Deliverables:

- Add one active-session Unix control socket under `RFS_HOME/active/`.
- Serve a typed gRPC/protobuf control API over the Unix domain socket.
- Define protobuf request/response messages for:
  - `status`
  - `snapshot`
  - `unmount`
  - protocol/version check
- Make `rfs status` talk to `rfsd` through the generated gRPC client.
- Add socket path discovery from `RFS_HOME/active/` metadata.
- If no live session exists but previous `RFS_HOME/active/` state remains, make `rfs status` report previous/stale session metadata and cleanup guidance.
- If no live or previous session state exists, make `rfs status` report a clean no-session state.
- Keep text-proto-friendly message shapes so debug clients can log readable request and response payloads.

Task targets:

```sh
task test:unit
task test:integration:daemon
```

Tests:

- Unit: control protobuf conversion rejects unknown or incompatible protocol versions cleanly.
- Unit: gRPC status codes map to stable CLI daemon diagnostics.
- CLI: `rfs status` reports clean no-session state when no active or previous state exists.
- CLI: `rfs status` reports previous/stale state when `RFS_HOME/active/` remains without a live lock.
- Integration: start foreground `rfsd` in no-FUSE control mode and query `rfs status`.
- Integration: attempting a second active session in the same `RFS_HOME` fails with the existing session metadata.
- Integration: daemon shutdown through `rfs unmount` control path.

Definition of done:

- Typed CLI-to-daemon communication is testable before a real FUSE mount exists.

## Phase 5: Read-Only Lazy Filesystem

### Step 5.1: Filesystem Core Without FUSE

Deliverables:

- Implement a core filesystem model that can:
  - Validate the root `Directory` object on mount; recursive verification is deferred.
  - Lazily fetch directory objects.
  - Allocate session-stable inodes for materialized entries.
  - Resolve lookup and readdir from remote tree metadata.
  - Read symlink targets.
  - Fetch file blobs on first read through the verified blob cache.
- Keep this layer independent of `fuser`.

Task targets:

```sh
task test:unit
task test:integration:readonly
```

Tests:

- Unit: fake CAS lookup and readdir fetch only the needed directory.
- Unit: child directories are not fetched until accessed.
- Unit: first read fetches and verifies a blob, second read hits cache.
- Unit: verified cache entries are trusted after admission and are not rehashed on ordinary reads.
- Unit: cache admission writes to a temp file in the target cache filesystem and atomically renames only after verification.
- Unit: in-process duplicate reads of the same missing digest coalesce behind a per-digest lock.
- Unit: missing blob or digest mismatch returns a structured error.
- Integration: upload fixture to `bazel-remote`, mount core model against the root digest, read selected files, and assert counters.

Definition of done:

- Lazy behavior is proven without FUSE.

### Step 5.2: Read-Only FUSE Adapter

Deliverables:

- Implement `fuser` operations for read-only snapshots:
  - `init`
  - `lookup`
  - `getattr`
  - `readdir`
  - `open`
  - `read`
  - `readlink`
  - read-only errors for mutation attempts.
- Implement `rfs mount <root-digest> <mountpoint>` starting `rfsd`.
- Keep `rfsd` usable in foreground mode for tests and manual debugging.
- Return from `rfs mount` only after root validation, FUSE mount readiness, and control socket readiness.
- Implement `rfs unmount`.

Task targets:

```sh
task test:unit
task test:integration:readonly
task test:e2e:readonly
task mount:readonly ROOT=sha256:... MOUNT=/tmp/rfs-mnt
```

Tests:

- Unit: FUSE adapter maps core errors to correct errno values.
- Integration: daemon validates root digest and rejects missing root.
- E2E: upload fixture, mount digest, run `find`, `stat`, `cat`, `readlink`, then unmount.
- E2E: remount the same root and verify reads are served from local cache where expected.
- E2E: mutation attempts fail with read-only errors.
- E2E: read-only mmap of a remote-backed file returns correct bytes.

Definition of done:

- Milestone 1 from the technical design works end-to-end:
  `local dir -> rfs upload -> CAS -> rfs mount -> lazy read -> remount same root`.

## Phase 6: Writable Copy-On-Write

### Step 6.1: Overlay Index and Merged View

Deliverables:

- Extend SQLite schema for overlay entries:
  - New files and directories.
  - Copied-up remote files.
  - Tombstones.
  - Renames.
  - Mode changes.
  - Mtime changes.
  - Dirty ancestors.
- Implement merged lookup and readdir precedence:
  - Tombstones hide remote entries.
  - Local entries override remote entries at the same path.
  - Unchanged remote subtree references remain intact.
- Store local file contents in the session overlay data directory.
- Implement transaction boundaries:
  - One SQLite transaction per logical filesystem metadata mutation.
  - Keep remote fetches and large file IO outside SQLite transactions where possible.
  - Commit metadata and dirty-state only after local file data operations succeed.
  - Treat SQLite as the source of truth for visible overlay state.

Task targets:

```sh
task test:unit
task test:integration:overlay
```

Tests:

- Unit: merged lookup precedence for remote, local, and tombstoned entries.
- Unit: directory listings combine local and remote entries in stable order.
- Unit: dirty ancestor marking is minimal and correct.
- Unit: failed SQLite commit after overlay file rename leaves no visible overlay entry.
- Unit: unreferenced overlay files can be identified for cleanup.
- Integration: reopen session database and preserve overlay state.

Definition of done:

- The writable core can represent final workspace state without FUSE writes yet.

### Step 6.2: Whole-File Copy-On-Write

Deliverables:

- Implement first-write copy-up for remote-backed files:
  - Ensure verified blob cache.
  - Copy blob into a temporary overlay file.
  - Apply write or truncate to the temporary overlay file.
  - Atomically rename the temporary overlay file into session overlay data.
  - Commit copied-up state, content dirty state, and dirty ancestors in one SQLite transaction.
- Apply metadata-only copy-up triggers such as chmod or utimens with one SQLite transaction and no data-file copy unless content must become local.
- Support local-only creates, writes, truncates, and metadata updates.
- Emit structured warnings for large-file copy-up and continue by default.

Task targets:

```sh
task test:unit
task test:integration:cow
```

Tests:

- Unit: first write to remote file performs exactly one copy-up.
- Unit: repeated writes use overlay file only.
- Unit: truncate, chmod, and utimens mark expected dirty state.
- Unit: failed remote fetch aborts mutation without creating partial overlay state.
- Unit: copy-up keeps remote fetch and large file copy outside SQLite transactions.
- Unit: copy-up commit records copied-up state and dirty ancestors atomically.
- Integration: core model mutates uploaded fixture and verifies merged readback.

Definition of done:

- Whole-file COW works in core tests without FUSE.

### Step 6.3: Writable FUSE Operations

Deliverables:

- Add FUSE mutation operations:
  - `create`
  - `mkdir`
  - `write`
  - `flush` and `fsync` for local overlay byte synchronization and local IO error reporting
  - `setattr`
  - `unlink`
  - `rmdir`
  - `rename`
- Support rename-over-file and reject rename-over-non-empty-directory.
- Support empty-directory rename and directory rename when the destination does not exist.
- Reject file-over-directory, directory-over-file, and moving a directory into itself or a descendant.
- Keep cross-mount rename behavior clear and Unix-compatible.
- Preserve inode identity for renamed sources within one mounted workspace.
- If the destination existed, tombstone/replace its old path in overlay state.
- Preserve Unix open-handle semantics across rename and replacement.
- Disable FUSE writeback cache for MVP writable mounts.
- Use short entry and attribute TTLs initially.
- Do not support writable mmap in the MVP; reject it where detectable and document it as outside snapshot correctness guarantees otherwise.
- Track FUSE file handles in daemon memory:
  - Record inode/path, flags, read/write capability, and in-flight mutation count.
  - Writable handles block snapshot until release.
  - Read-only handles and read-only mmap do not block snapshot.
  - Rename/unlink of an open file follows Unix mounted-view semantics, but an existing writable handle still blocks snapshot until release.
- Do not defer SQLite dirty-state commits to `flush` or `fsync`; each successful mutation call commits its own overlay metadata before returning.
- Make `flush` and `fsync` synchronize already-written local overlay file bytes and report local IO errors only.

Task targets:

```sh
task test:unit
task test:integration:cow
task test:e2e:writable
```

Tests:

- Unit: errno mapping for unsupported or invalid mutations.
- Unit: rename-over-file replaces destination and preserves source inode identity.
- Unit: directory rename succeeds when destination does not exist.
- Unit: rename over non-empty directory, file-over-directory, directory-over-file, and directory-into-descendant are rejected.
- Unit: cross-mount rename maps to `EXDEV`.
- Unit: open file handle remains usable after rename or path replacement.
- E2E: create, edit, truncate, chmod, touch, delete, and rename through the mounted filesystem.
- E2E: build-tool style write patterns, including temp-file write followed by rename.
- E2E: remounting the original root shows remote snapshot was not modified.
- E2E: writable mmap is rejected where detectable or covered by an explicit unsupported-behavior test.

Definition of done:

- Mounted workspaces are writable with durable session overlay state.

## Phase 7: Snapshot Through Daemon

### Step 7.1: Snapshot Core

Deliverables:

- Implement full-workspace snapshot from merged overlay state.
- Open a short SQLite read transaction after the snapshot barrier passes to read a consistent overlay graph.
- Feed merged overlay entries into the shared tree-writer abstraction; keep overlay traversal separate from local filesystem upload traversal.
- Reuse unchanged remote blob and subtree digests.
- Hash dirty/new files at snapshot time.
- Encode dirty/new directory nodes canonically.
- Use `FindMissingBlobs` before uploading blobs and directory nodes.
- Return the new root digest.

Task targets:

```sh
task test:unit
task test:integration:snapshot
```

Tests:

- Unit: unchanged tree snapshots to the original root digest.
- Unit: single file edit rewrites only that file blob and ancestor directories.
- Unit: delete removes the path from the resulting tree.
- Unit: rename affects final path state, not rename history.
- Unit: replaced rename destination is absent from the snapshot unless recreated.
- Unit: symlinks snapshot as symlink nodes.
- Integration: snapshot mutated fixture into `bazel-remote`, fetch and decode resulting tree, compare expected structure.
- Integration: fake CAS counts prove unchanged blobs are not re-uploaded.

Definition of done:

- Snapshotting the core merged view is correct and deduplicating.

### Step 7.2: Snapshot Control Flow

Deliverables:

- Implement `rfs snapshot [mountpoint]` by sending a request to the active `rfsd` discovered through `RFS_HOME`.
- If a mountpoint is supplied, validate it matches the active session before sending the request.
- Require an active RemoteFS daemon session. Do not support `rfs snapshot <local-dir>` in the MVP; use `rfs upload <local-dir>` for ordinary directories.
- Add a short snapshot barrier.
- Fail snapshot if writable handles or in-flight mutations are active.
- Return human and JSON output with root digest and counters.
- Use the stable JSON envelope for `rfs snapshot --json`: `schema_version`, `command`, `ok`, `warnings`, `error`, and `data`.

Task targets:

```sh
task test:integration:daemon
task test:e2e:snapshot
```

Tests:

- Integration: socket snapshot request returns a digest and counters.
- CLI: `rfs snapshot` without an active session fails clearly and points to `rfs upload <local-dir>` for ordinary directories.
- Integration: open writable handle causes a clear snapshot failure.
- Integration: in-flight mutation causes a clear snapshot failure.
- Integration: read-only handle does not block snapshot.
- Integration: `fsync` does not unblock snapshot while a writable handle remains open.
- E2E: upload, mount, mutate, snapshot, unmount, remount new digest, compare filesystem contents.
- E2E: original digest still mounts to original contents.

Definition of done:

- Milestone 2 from the technical design works end-to-end.

## Phase 8: Observability and Hardening

### Step 8.1: Status, Logs, and Counters

Deliverables:

- Implement structured counters for:
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
- Implement `rfs status` and `rfs status --json`.
- Status output covers live active session, previous/stale session, and clean no-session states.
- Implement stable JSON command summaries for `status`, `upload`, and `snapshot`:
  - Include `schema_version: 1`, `command`, `ok`, `warnings`, `error`, and command-specific `data`.
  - Use `error: null` on success and `error: { code, message, details }` on failure.
  - Treat field names and types as stable within a schema version.
  - Allow later versions to add optional fields without bumping the schema.
  - Require a `schema_version` bump before removing fields or changing field types.
  - Print one JSON object on `--json` failure for machine consumers; keep logs separate.
- Route CLI logs to stderr only. Command results and JSON summaries go to stdout.
- Write daemon logs to `RFS_HOME/active/logs/rfsd.log`.
- Use compact human-readable text logs by default and JSON Lines logs when `--log-format json` is selected.
- Apply `--log-level` and `--log-format` to both `rfs` and any spawned `rfsd`.
- Store effective daemon log level and format in session metadata for `rfs status`.
- Preserve the session log file with `RFS_HOME/active` until `rfs cleanup`; no log rotation in the MVP.
- Include timestamp, level, target/module, session id, operation, path/digest where relevant, and message in daemon log events.

Task targets:

```sh
task test:unit
task test:integration:status
task test:e2e:status
```

Tests:

- Unit: counters update from core operations.
- Unit: JSON output has stable field names.
- Unit: `status`, `upload`, and `snapshot` JSON outputs match golden envelope snapshots.
- Unit: JSON failure output uses the same envelope with `ok: false`.
- Unit: CLI logs use stderr and command summaries use stdout.
- Unit: log format flag selects text or JSON Lines formatting.
- Integration: daemon status includes mount root, cache paths, session paths, and counters.
- Integration: spawned daemon writes `RFS_HOME/active/logs/rfsd.log` and status reports log level/format.
- Integration: status reports previous/stale session metadata after clean unmount leaves inspectable state.
- E2E: read a file through mount and observe counter changes.

Definition of done:

- A CI operator can diagnose whether a job is fetching from CAS or hitting local cache.

### Step 8.2: Failure Semantics

Deliverables:

- Normalize errors across CLI, daemon, CAS, and FUSE paths.
- Include path, digest, operation, and remote context where available.
- Ensure no empty, partial, or unverified content is served after fetch failures.
- Document that automatic cache eviction and cache pruning are deferred; `rfs cleanup` only removes stale session state.

Task targets:

```sh
task test:unit
task test:integration:failures
task test:e2e:failures
```

Tests:

- Unit: partial download never reaches verified cache.
- Unit: CAS error maps to expected CLI/FUSE diagnostic.
- Integration: missing tree node fails lazily on the operation that needs it.
- E2E: stop CAS after mount, then read an uncached file and verify a visible filesystem error.

Definition of done:

- Failure behavior matches PRD integrity and failure requirements.

### Step 8.3: Evaluation Workloads

Deliverables:

- Add reproducible fixtures under `tests/fixtures/`:
  - `basic-tree`: regular files, nested directories, empty directory, symlink, executable file, and fixed mtimes.
  - `metadata-tree`: modes, sticky directory, pre-epoch mtime, nanosecond mtime, risky symlinks, and a hardlink pair if practical.
  - `mutation-tree`: source fixture for writable COW, rename, delete, and snapshot tests.
  - `large-file-tree`: deterministic generated large file for ByteStream and copy-up warning tests.
- Normalize fixture mtimes and modes through a fixture setup helper instead of relying on Git metadata preservation.
- Store golden root digests only for small deterministic fixtures such as `basic-tree` and representative encoded `Directory` objects.
- Do not store golden digests for generated large or evaluation fixtures; assert structural properties and remount equivalence instead.
- Require an explicit update path, for example `UPDATE_GOLDENS=1 task test:unit`, for golden digest changes.
- Add benchmark-like task targets for MVP evaluation.

Task targets:

```sh
task test:e2e
task eval:readonly
task eval:writable
task eval:snapshot
```

Tests:

- E2E: full read-only workflow.
- E2E: full writable snapshot workflow.
- E2E: repeated run shows cache reuse.
- E2E: fixture upload root, mount contents, lazy cache behavior, mutation snapshot remount, and JSON summaries are inspected.
- Evaluation: record mount latency, bytes fetched, bytes uploaded, and snapshot duration.

Definition of done:

- The implementation can demonstrate the PRD value proposition with repeatable local commands.

## Taskfile Target Map

The Taskfile should grow with the implementation, but these names should remain stable:

```sh
task fmt
task lint
task build
task test
task test:unit
task test:cli
task test:integration
task test:integration:cas
task test:integration:upload
task test:integration:readonly
task test:integration:overlay
task test:integration:cow
task test:integration:snapshot
task test:e2e
task test:e2e:readonly
task test:e2e:writable
task test:e2e:snapshot
task cas:up
task cas:down
task cas:logs
task cas:reset
task fixture:upload
task mount:readonly ROOT=sha256:... MOUNT=/tmp/rfs-mnt
task mount:writable ROOT=sha256:... MOUNT=/tmp/rfs-mnt
task eval:readonly
task eval:writable
task eval:snapshot
```

## Suggested PR Sequence

1. Add Taskfile, dependency skeleton, CLI parser, config paths, and digest type.
2. Add REAPI protos and generated client modules.
3. Add local `bazel-remote` Task workflow.
4. Add CAS client with blob upload/download/existence integration tests.
5. Add canonical tree encoder/decoder.
6. Add `rfs upload`.
7. Add daemon state layout and SQLite migrations.
8. Add gRPC/protobuf Unix control socket and `rfs status`.
9. Add read-only filesystem core with fake CAS tests.
10. Add read-only FUSE mount E2E.
11. Add overlay index and merged view core.
12. Add whole-file COW core.
13. Add writable FUSE operations.
14. Add snapshot core.
15. Add `rfs snapshot` daemon control flow.
16. Add observability, failure tests, and evaluation fixtures.

## Deferred Choices

These are intentionally deferred until the core MVP path is working:

- Buildbarn compatibility smoke tests beyond documented secondary-target intent.
- Bounded automatic local cache eviction.
- Manual cache pruning command.
- Configurable hard guardrails for large-file copy-up.
- Recursive root verification command.
- Cache verification command for already-admitted local cache entries.
- Writable mmap support.
- Block-level or chunked COW.
- TLS, auth, and hosted/managed CAS concerns.
