# RemoteFS Implementation Plan

This plan breaks the PRD and technical design into small implementation steps. Each step should leave the repo buildable, add tests at the lowest useful layer first, and then add integration or end-to-end coverage as soon as the behavior crosses a process, CAS, daemon, or FUSE boundary.

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
  - Daemon/FUSE/state: `fuser`, `libc`, `rusqlite` or `sqlx`, `uuid`.
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
task test:preflight
task test:unit
```

Tests:

- Unit: digest parser skeleton tests.
- Unit: config path resolution tests for `RFS_HOME`, `RFS_CACHE_DIR`, and `RFS_SESSION_DIR`.
- Preflight: verify local test prerequisites, including Docker for CAS tests and `/dev/fuse` plus mount permissions for FUSE tests.
- Smoke: `task build` produces both `rfs` and `rfsd`.

Definition of done:

- `task fmt`, `task lint`, `task build`, and `task test:unit` pass.
- Both binaries print real CLI help instead of placeholder messages.

### Step 0.2: Proto Setup

Deliverables:

- Add pinned Remote Execution API and ByteStream protos.
- Add RemoteFS-owned protobuf definitions for the local daemon control API.
- Generate Rust code with `tonic-build` and `prost-build`.
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

Recommended approach:

- Vendor a pinned copy under `third_party/remote-apis/` or use a pinned proto fetch task that writes into a checked-in directory.
- Check in proto source files, not generated Rust, unless build reproducibility becomes painful.

Task targets:

```sh
task proto:check
task proto:generate
task build
```

Tests:

- Unit: encode a minimal `Directory` and verify the resulting digest is stable.
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
task cas:doctor
```

Tests:

- Integration: `task cas:doctor` confirms the CAS endpoint accepts gRPC requests.
- Integration: upload and download a small blob through raw CAS client plumbing once Step 2.1 exists.

Definition of done:

- A developer can run `task cas:up` and then `task cas:doctor` without reading container docs.

## Phase 2: CAS Client and Snapshot Primitives

### Step 2.1: Digest and CAS Client

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
- Default the batch/ByteStream split at 4 MiB, with a conservative serialized-message-size check so protobuf and gRPC framing overhead cannot push a batch request over the practical limit.
- Make the threshold configurable for compatibility testing against different CAS deployments.
- Add digest verification for downloaded blobs before cache admission.

Task targets:

```sh
task test:unit
task test:integration:cas
```

Tests:

- Unit: digest parse rejects uppercase hash, missing size, non-SHA-256 prefixes, invalid hex, negative size, and size overflow.
- Unit: hash computation over bytes returns expected digest.
- Unit: downloaded blob verifier rejects hash or size mismatch.
- Integration: against `task cas:up`, upload, existence-check, download, and verify blobs.
- Integration: idempotent re-upload of the same digest succeeds.

Definition of done:

- CAS calls are hidden behind a small trait so core filesystem tests can use an in-memory fake CAS.
- CAS configuration includes REAPI `instance_name`, defaulting to empty and sent consistently on CAS and ByteStream calls.

### Step 2.2: Canonical Tree Encoding

Deliverables:

- Implement the constrained REAPI `Directory` encoder/decoder.
- Enforce canonical entry ordering for files, directories, and symlinks.
- Preserve supported metadata:
  - File type.
  - File size.
  - Unix mode.
  - Executable bit.
  - Mtime seconds and nanoseconds.
  - Symlink target.
- Reject or warn on unsupported file types according to CLI mode.

Task targets:

```sh
task test:unit
task test:integration:cas
```

Tests:

- Unit: canonical ordering is stable regardless of traversal order.
- Unit: mode, executable bit, and mtime round-trip through REAPI objects.
- Unit: symlink targets are stored as symlink nodes and are not followed.
- Unit: unsupported node types return structured errors.
- Integration: upload encoded tree nodes to CAS and fetch/decode them by digest.

Definition of done:

- `rfs upload` and mounted snapshot code can share this encoder later.

## Phase 3: Bootstrap CLI

### Step 3.1: CLI Skeleton and Config

Deliverables:

- Implement `rfs` command structure:
  - `rfs doctor`
  - `rfs upload <local-dir>`
  - `rfs mount <root-digest> <mountpoint>`
  - `rfs snapshot [mountpoint]`
  - `rfs unmount [mountpoint]`
  - `rfs status [mountpoint]`
  - `rfs cleanup`
- Implement common flags:
  - `--cas-url`
  - `--instance-name`
  - `--json`
  - `--log-level`
  - `--cache-dir`
  - `--session-dir`
- Return stable non-zero exit codes for usage errors, CAS errors, digest errors, and daemon errors.

Task targets:

```sh
task build
task test:unit
task test:cli
```

Tests:

- Unit: config precedence from CLI flags and environment variables.
- Unit: CAS URL and REAPI `instance_name` are included in every CAS and ByteStream request.
- Unit: command parsing for every expected command.
- Unit: optional mountpoint arguments validate against active-session metadata when supplied.
- CLI: `rfs cleanup` refuses to remove active state while a live session lock exists.
- CLI: `rfs --help`, subcommand help, and invalid digest errors via `assert_cmd`.

Definition of done:

- Every command exists, even if some commands return a clear "not implemented yet" error.

### Step 3.2: `rfs doctor`

Deliverables:

- Check CAS reachability.
- Check local state directories are usable.
- Check FUSE availability and report skipped/unavailable status without failing CAS-only workflows.
- Support human and JSON output.

Task targets:

```sh
task doctor
task test:cli
task test:integration:cas
```

Tests:

- Unit: JSON schema shape for doctor output.
- CLI: unreachable CAS returns a clear diagnostic.
- Integration: reachable `bazel-remote` reports CAS OK.

Definition of done:

- `task doctor` is the first command a developer runs after `task cas:up`.

### Step 3.3: `rfs upload`

Deliverables:

- Walk local directories without following symlinks.
- Hash and upload file blobs.
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
- Unit: tree encoder produces identical root digest for identical trees.
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
  - Logs.
- Use sharded cache paths by hash prefix, for example `cache/blobs/aa/<hash>-<size>` and `cache/dirs/aa/<hash>-<size>`.
- Store raw serialized REAPI `Directory` bytes in the directory cache; keep decoded directory objects in memory only.
- Create one active session under `RFS_HOME/active/`.
- Add an active-session lock so at most one `rfsd` can own an `RFS_HOME` at a time.
- Preserve `RFS_HOME/active/` after clean unmount for inspection.
- Implement `rfs cleanup` to remove stale `RFS_HOME/active/` only when no live active-session lock exists.
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
- Keep text-proto-friendly message shapes so debug clients can log readable request and response payloads.

Task targets:

```sh
task test:unit
task test:integration:daemon
```

Tests:

- Unit: control protobuf conversion rejects unknown or incompatible protocol versions cleanly.
- Unit: gRPC status codes map to stable CLI daemon errors.
- Integration: start foreground `rfsd` in no-FUSE control mode and query `rfs status`.
- Integration: attempting a second active session in the same `RFS_HOME` fails with the existing session metadata.
- Integration: daemon shutdown through `rfs unmount` control path.

Definition of done:

- Typed CLI-to-daemon communication is testable before a real FUSE mount exists.

## Phase 5: Read-Only Lazy Filesystem

### Step 5.1: Filesystem Core Without FUSE

Deliverables:

- Implement a core filesystem model that can:
  - Validate root directory digest on mount.
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

Task targets:

```sh
task test:unit
task test:integration:overlay
```

Tests:

- Unit: merged lookup precedence for remote, local, and tombstoned entries.
- Unit: directory listings combine local and remote entries in stable order.
- Unit: dirty ancestor marking is minimal and correct.
- Integration: reopen session database and preserve overlay state.

Definition of done:

- The writable core can represent final workspace state without FUSE writes yet.

### Step 6.2: Whole-File Copy-On-Write

Deliverables:

- Implement first-write copy-up for remote-backed files:
  - Ensure verified blob cache.
  - Copy blob into session overlay data.
  - Apply write, truncate, chmod, or utimens locally.
  - Mark content and ancestors dirty.
- Support local-only creates, writes, truncates, and metadata updates.
- Emit structured diagnostics for large-file copy-up.

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
- Integration: core model mutates uploaded fixture and verifies merged readback.

Definition of done:

- Whole-file COW works in core tests without FUSE.

### Step 6.3: Writable FUSE Operations

Deliverables:

- Add FUSE mutation operations:
  - `create`
  - `mkdir`
  - `write`
  - `flush` or `fsync` as needed for correctness
  - `setattr`
  - `unlink`
  - `rmdir`
  - `rename`
- Support rename-over-file and reject rename-over-non-empty-directory.
- Keep cross-mount rename behavior clear and Unix-compatible.
- Disable FUSE writeback cache for MVP writable mounts.
- Use short entry and attribute TTLs initially.
- Do not support writable mmap in the MVP; reject it where detectable and document it as outside snapshot correctness guarantees otherwise.

Task targets:

```sh
task test:unit
task test:integration:cow
task test:e2e:writable
```

Tests:

- Unit: errno mapping for unsupported or invalid mutations.
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
- Unit: symlinks snapshot as symlink nodes.
- Integration: snapshot mutated fixture into `bazel-remote`, fetch and decode resulting tree, compare expected structure.
- Integration: fake CAS counts prove unchanged blobs are not re-uploaded.

Definition of done:

- Snapshotting the core merged view is correct and deduplicating.

### Step 7.2: Snapshot Control Flow

Deliverables:

- Implement `rfs snapshot [mountpoint]` by sending a request to the active `rfsd` discovered through `RFS_HOME`.
- If a mountpoint is supplied, validate it matches the active session before sending the request.
- Add a short snapshot barrier.
- Fail snapshot if writable handles or pending writes are active.
- Return human and JSON output with root digest and counters.

Task targets:

```sh
task test:integration:daemon
task test:e2e:snapshot
```

Tests:

- Integration: socket snapshot request returns a digest and counters.
- Integration: open writable handle causes a clear snapshot failure.
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
- Add human logs by default and JSON logs via flag.

Task targets:

```sh
task test:unit
task test:integration:status
task test:e2e:status
```

Tests:

- Unit: counters update from core operations.
- Unit: JSON output has stable field names.
- Integration: daemon status includes mount root, cache paths, session paths, and counters.
- E2E: read a file through mount and observe counter changes.

Definition of done:

- A CI operator can diagnose whether a job is fetching from CAS or hitting local cache.

### Step 8.2: Failure Semantics

Deliverables:

- Normalize errors across CLI, daemon, CAS, and FUSE paths.
- Include path, digest, operation, and remote context where available.
- Ensure no empty, partial, or unverified content is served after fetch failures.
- Add manual cache pruning command or documented cleanup workflow if bounded eviction remains deferred.

Task targets:

```sh
task test:unit
task test:integration:failures
task test:e2e:failures
```

Tests:

- Unit: partial download never reaches verified cache.
- Unit: CAS error maps to expected CLI/FUSE error.
- Integration: missing tree node fails lazily on the operation that needs it.
- E2E: stop CAS after mount, then read an uncached file and verify a visible filesystem error.

Definition of done:

- Failure behavior matches PRD integrity and failure requirements.

### Step 8.3: Evaluation Workloads

Deliverables:

- Add reproducible fixtures:
  - Small tree with files, dirs, symlinks, modes, mtimes.
  - Medium tree for cache and lazy-fetch behavior.
  - Mutation-heavy workspace.
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
task cas:doctor
task doctor
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
3. Add local `bazel-remote` Task workflow and CAS doctor.
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

## Critical Choices To Resolve

These choices affect the implementation shape enough that they should be answered before or during the first two PRs:

- Proto sourcing: vendor pinned protos in the repo, or fetch them with a pinned Task target?
- SQLite library: synchronous `rusqlite` inside daemon-owned blocking sections, or async `sqlx` with a runtime pool?
- Generated proto code: checked in for reproducibility, or generated during build?
- Control protocol: use gRPC/protobuf over the active session Unix socket for type safety, versioning, and text-proto-friendly debug tooling.
- Active session model: allow at most one active mount per `RFS_HOME`; second mounts fail clearly and concurrent mounts require separate `RFS_HOME` values.
- CAS namespace: use REAPI `instance_name`, exposed as `--instance-name`, and do not introduce a separate RemoteFS "project" namespace.
- FUSE test policy: full local test runs require privileged FUSE E2E tests; add preflight checks so missing local prerequisites fail clearly.
- Local cache eviction: manual pruning only for earliest MVP, or bounded cache from the start?
- Large-file COW: warn only, fail above a configurable threshold, or allow unbounded whole-file copy-up?
- Snapshot barrier: fail immediately on writable handles, or wait briefly for handles to close?
- Directory mtimes: preserve and expose them strictly from the start, or accept a documented precision/behavior limitation until the FUSE path is stable?
- FUSE caching: disable writeback cache, use short entry/attribute TTLs initially, support read-only mmap, and do not support writable mmap in the MVP.
