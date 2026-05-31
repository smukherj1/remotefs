# Implementation Plan Draft Session Handoff

## Goal

Continue refining the RemoteFS MVP implementation plan from `PRD.md` and `docs/technical-design.md`.

The immediate objective is not to implement code. The objective is to keep grilling critical design and implementation choices until the implementation plan is concrete enough for another coding agent to start building the project in small, testable steps.

Primary planning artifact:

- `docs/implementation-plan.md`

Source documents:

- `PRD.md`
- `docs/technical-design.md`

## Instructions For The Next Agent

- Use the `grill-me` skill if available.
- Ask one critical design question at a time.
- For each question, provide a recommended answer.
- Do not re-ask decisions already captured below unless the user explicitly reopens them.
- If a question can be answered by reading the docs or repo, read first instead of asking.
- When the user settles a choice, update the relevant docs if the decision changes product/design/implementation behavior.
- Keep the implementation plan in small, buildable steps with unit tests first, then integration and e2e tests.
- Use `task` as the workflow runner in all docs. Do not introduce Makefile workflows.
- Local testing is mandatory for now. Do not describe FUSE/e2e tests as optional CI-only tests.

## Current State

The repo is a very small Rust project with placeholder binaries:

- `src/bin/rfs.rs`
- `src/bin/rfsd.rs`
- `Cargo.toml`

The implementation plan has been created and iteratively updated:

- `docs/implementation-plan.md`

The product and technical design docs have also been updated for decisions made during this session.

Current uncommitted docs changes include:

- New `docs/implementation-plan.md`.
- New `docs/implementation-plan-draft-session.md`.
- Updates to `PRD.md`.
- Updates to `docs/technical-design.md`.

## Settled Decisions

- Vendor pinned REAPI proto files under `third_party/remote-apis/`.
- Generate Rust proto code during build with `tonic-build`/`prost-build`; do not check generated Rust into the repo initially.
- Add RemoteFS-owned protobuf definitions for the local daemon control API.
- Use gRPC/protobuf over a Unix domain socket for `rfs` to `rfsd` control traffic.
- Use `rusqlite` for daemon-local SQLite state.
- Use `bazel-remote` as the default local CAS.
- Use `task` for build/test/dev workflows.
- Full local test workflow requires integration and FUSE e2e tests. Missing Docker, `/dev/fuse`, or mount permissions should fail clearly from the test target that needs the prerequisite.
- Manual local cache pruning only for early MVP; bounded eviction later.
- Do not edit the implementation plan yet for future metrics, but remember that latency distributions will be needed later, probably bucketed by logarithmic blob sizes.
- Snapshot barrier behavior: fail immediately if writable handles or in-flight mutations exist.
- Preserve directory mtimes from the start.
- Use batch CAS APIs for small blobs/tree nodes and ByteStream for larger blobs.
- Default batch/ByteStream threshold to 4 MiB, with conservative serialized-message-size checks and a configurable threshold.
- `rfs upload` includes everything under the supplied directory. No include/exclude or `.gitignore` semantics in MVP.
- Unsupported filesystem nodes fail upload by default. A future explicit skip mode can be added later.
- Mount validates only the root `Directory` object; recursive verification can be a later command.
- Cache fetched `Directory` objects in the shared cache by digest, and keep per-session materialization/inode state in SQLite.
- Trust verified blob cache entries after admission; do not rehash on every read or daemon startup.
- Downloaded blobs are written to temp files in the target cache filesystem, verified, then atomically renamed.
- Use in-process per-digest locks for duplicate cache fills; tolerate cross-process duplicate downloads in MVP.
- `rfs mount` starts `rfsd`, waits for readiness, then exits. `rfsd` also supports foreground mode.
- Strong crash recovery is deferred. Stale sessions fail clearly with cleanup guidance.
- Detect hard links on upload, warn/count them, but store each path as an ordinary file.
- Preserve all symlinks exactly by default, including absolute and escaping targets; warn/count risky symlinks. Optional strict mode later.
- MVP requires UTF-8 path component names and symlink targets because REAPI uses protobuf `string` fields.
- Hash deterministic protobuf bytes from the RemoteFS encoder, with canonical entry ordering.
- Pin `prost`/`tonic` versions and add golden digest tests for representative `Directory` objects.
- Empty directories are normal encoded `Directory` objects and must exist in CAS.
- Use REAPI `instance_name` as the CAS namespace selector, exposed as required non-empty `--instance-name`; do not introduce a separate RemoteFS "project" namespace.
- Root digest strings remain `sha256:<hash>/<size>` only. `instance_name` stays configuration/context.
- `--cas-url` requires explicit URI schemes. MVP supports `grpc://`; reserve `grpcs://` for later.
- At most one active RemoteFS mount per `RFS_HOME` on a machine.
- A second mount using the same `RFS_HOME` fails clearly while an active session exists.
- Concurrent mounts require separate `RFS_HOME` values.
- `rfs status`, `rfs snapshot`, and `rfs unmount` default to the active session under `RFS_HOME`; optional mountpoint arguments validate against active-session metadata.
- `RFS_HOME/active` remains after clean unmount for inspection.
- A later mount fails if stale `RFS_HOME/active` exists without a live lock and tells the user to run `rfs cleanup`.
- `rfs cleanup` removes stale active session state only when no live active-session lock exists.
- `rfs cleanup` does not prune the blob/directory cache. Cache pruning should be a separate future command.
- Shared blob and directory cache paths are sharded by hash prefix and include digest size in the filename.
- Directory cache stores raw serialized REAPI `Directory` bytes; decoded representations are in-memory only.
- Cached blob files are opened per FUSE `open`, and the file descriptor lives for that FUSE file handle lifetime. No global file-handle cache in MVP.
- Start with short FUSE entry and attribute TTLs.
- Disable FUSE writeback cache for MVP.
- Support read-only mmap.
- Do not support writable mmap in MVP; reject where detectable and document as outside snapshot correctness guarantees otherwise.
- Large-file COW warns by default and proceeds with whole-file copy-up; a configurable hard size guardrail can be added later as an explicit opt-in.
- `rfs upload` uses a bounded deterministic pipeline: one walker, hashing workers defaulting to `min(available_parallelism, 8)` with minimum 2, `FindMissingBlobs` after digesting, small uploads through `BatchUpdateBlobs`, large uploads through ByteStream with default concurrency 4, and a default 64 MiB buffered in-flight payload budget. Large ByteStream uploads reread from disk instead of buffering whole files. Completion order must not affect root digest or JSON summary ordering.
- CAS client retry/timeout policy: per-attempt defaults are 10 seconds for `FindMissingBlobs`, 30 seconds for `BatchReadBlobs` and `BatchUpdateBlobs`, and a 30 second ByteStream idle timeout. Retry only transient transport/server failures with jittered exponential backoff of 100 ms, 250 ms, 500 ms, then 1 second, max 5 attempts. Do not retry semantic/auth/configuration failures or digest mismatches. ByteStream retries restart from byte 0. Final errors include operation name, digest/resource where relevant, attempt count, CAS URL, and `instance_name`.
- ByteStream resource names use only standard uncompressed REAPI forms with required non-empty `instance_name`: read `{instance_name}/blobs/{hash}/{size}`, write `{instance_name}/uploads/{uuid}/blobs/{hash}/{size}`. Generate a fresh UUID v4 per upload attempt. Do not support compressed names, digest-function prefixes, optional metadata, or empty-instance resource names in the MVP. Validate `instance_name` early and reject reserved REAPI resource keywords as path segments.
- `BatchUpdateBlobs` packs multiple small blobs/tree nodes up to a configurable serialized request budget, default 3.5 MiB. Start a new request before the next entry would exceed the budget. Send a near-threshold entry through ByteStream if it cannot fit in the serialized batch budget by itself. Entry order inside a batch request is not semantically significant and is not a determinism requirement; determinism matters for tree encoding, root digests, and user-visible summaries.
- Timestamp policy: store signed seconds plus normalized nanoseconds, preserve nanosecond precision when the OS/filesystem reports it, preserve valid pre-1970 mtimes as negative timestamp seconds, encode mtimes into REAPI `NodeProperties` using protobuf `Timestamp`, reject out-of-range or invalid-nanosecond timestamps with structured unsupported-metadata errors, and do not silently clamp or truncate values.
- Unix mode policy: file type is represented by REAPI node kind, preserve permission bits `0o777`, preserve sticky bit `0o1000` on directories, mask setuid `0o4000` and setgid `0o2000` with warning/count, ignore UID/GID ownership, derive `FileNode.is_executable` from `(mode & 0o111) != 0`, expose stored modes through FUSE `getattr`, and make mounted `chmod` update only supported mode bits.
- Remove `rfs doctor` from the MVP CLI. Command-specific validation belongs in `mount`, `upload`, and `snapshot`; `rfs status [mountpoint]` reports active, previous/stale, or clean session state. Remove standalone `task doctor`, `task cas:doctor`, and `task test:preflight`; integration/e2e test targets perform prerequisite checks directly and fail clearly when their dependencies are unavailable.
- CLI exit codes are intentionally simple for MVP: `0` for success and `1` for any error. Detailed categories belong in human and JSON diagnostics, not numeric process exit codes.
- JSON command summaries for `status`, `upload`, and `snapshot` use a stable `schema_version: 1` envelope with `command`, `ok`, `warnings`, `error`, and command-specific `data`. Field names/types are stable within a schema version; optional fields can be added later, but removals/type changes require a schema bump. On `--json` failure, print one JSON object for machine consumers and keep logs separate.
- Logging policy: CLI logs go to stderr; command results and JSON summaries go to stdout. Daemon logs go to `RFS_HOME/active/logs/rfsd.log` and are preserved with active/previous session state until `rfs cleanup`. Default log format is compact text; `--log-format text|json` selects text or JSON Lines. `--log-level` and `--log-format` apply to both `rfs` and spawned `rfsd`, and effective daemon settings are stored in session metadata. No log rotation in MVP.
- SQLite transaction policy: use one SQLite transaction per logical filesystem mutation, keep remote fetches and large file IO outside SQLite transactions where possible, commit metadata/dirty-state only after file data operations succeed, treat SQLite as source of truth for visible overlay state, make first-write copy-up stage temp overlay file plus atomic rename before one DB commit, tolerate orphan overlay files if commit fails after rename, update rename/delete directory rows and dirty ancestors atomically, and open snapshot's read transaction only after the snapshot barrier passes.
- Open handle tracking policy: track FUSE handles in daemon memory with inode/path, flags, read/write capability, and in-flight mutation count. Writable handles block snapshot until release; read-only handles and read-only mmap do not. In-flight mutations are currently executing mutation calls, not writes waiting for flush. Each mutation commits SQLite dirty-state before returning. `flush`/`fsync` only synchronize already-written local overlay bytes and report local IO errors; they do not create or commit RemoteFS overlay metadata and do not unblock snapshot while a writable handle remains open.
- Rename policy: support file-to-file rename including replacement, empty-directory rename, and directory rename when destination does not exist. Reject rename over non-empty directory, file-over-directory, directory-over-file, directory-into-self/descendant, and cross-mount rename (`EXDEV`). Preserve source inode identity within a mounted workspace. If destination existed, tombstone/replace its old path in overlay state. Snapshot uses final path state, not rename history. Open handles remain usable for their file object while path lookup observes the new name or replacement.
- `rfs snapshot` is daemon-session-only in the MVP. It requires an active RemoteFS session under `RFS_HOME`; if none exists, it fails clearly and points users to `rfs upload <local-dir>` for ordinary local directories. Do not add `rfs snapshot <local-dir>` as an upload alias.
- `rfs upload` and daemon `rfs snapshot` share a narrow tree-writer abstraction from the start for canonical directory encoding, deterministic digesting, CAS existence checks, batch/ByteStream upload selection, counters, and metadata warnings. Input traversal stays separate: upload walks a local directory; snapshot walks the daemon merged overlay graph after the barrier.
- E2E fixtures live under `tests/fixtures/`: `basic-tree`, `metadata-tree`, `mutation-tree`, and `large-file-tree`. Fixture mtimes/modes are normalized by a setup helper. Store golden root digests only for small deterministic fixtures and representative encoded `Directory` objects; generated large/evaluation fixtures use structural/remount assertions instead. Golden updates require an explicit path such as `UPDATE_GOLDENS=1 task test:unit`.

## Important Corrections Already Made

- The user asked not to revisit settled decisions. Do not ask again about upload include/exclude semantics or symlink preservation unless reopening deliberately.
- The docs were changed from "project" storage terminology to REAPI `instance_name`.
- The docs were changed from per-mount session/socket discovery to one active session per `RFS_HOME`.
- Active-session commands were simplified from required `<mountpoint>` to optional `[mountpoint]`.

## Remaining Areas

The major MVP implementation decisions are settled. Remaining items are non-blocking implementation details and can be handled during coding unless the user reopens planning:

- Exact fixture file contents and sizes.
- Precise JSON `data` payload fields per command.
- Exact SQLite schema names and migration numbering.
- Buildbarn smoke tests are deferred until the main REAPI path is stable.
