# Implementation Plan Draft Session Handoff

## Goal

Continue refining the RemoteFS MVP implementation plan from `PRD.md` and `docs/technical-design.md`.

The immediate objective is not to implement code. The objective is to keep grilling (using the grill-me skill) critical design and implementation choices until the implementation plan is concrete enough for another coding agent to start building the project in small, testable steps.

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
- Full local test workflow requires integration and FUSE e2e tests. Missing Docker, `/dev/fuse`, or mount permissions should fail clearly in preflight.
- Manual local cache pruning only for early MVP; bounded eviction later.
- Do not edit the implementation plan yet for future metrics, but remember that latency distributions will be needed later, probably bucketed by logarithmic blob sizes.
- Snapshot barrier behavior: fail immediately if writable handles or pending writes exist.
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
- Use REAPI `instance_name` as the CAS namespace selector, exposed as `--instance-name`; do not introduce a separate RemoteFS "project" namespace.
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

## Important Corrections Already Made

- The user asked not to revisit settled decisions. Do not ask again about upload include/exclude semantics or symlink preservation unless reopening deliberately.
- The docs were changed from "project" storage terminology to REAPI `instance_name`.
- The docs were changed from per-mount session/socket discovery to one active session per `RFS_HOME`.
- Active-session commands were simplified from required `<mountpoint>` to optional `[mountpoint]`.

## Remaining Areas To Grill

These are likely still worth asking about, one at a time:

- Large-file COW policy was discussed but not fully settled after the metrics aside. Current recommendation was warn by default with optional configurable fail threshold.
- Exact `rfs upload` parallelism model and backpressure: number of hashing/upload workers, memory limits, and ordering.
- CAS retry/timeouts: defaults, which operations retry, and how errors surface.
- ByteStream resource name format and instance-name handling details.
- `BatchUpdateBlobs` request packing: one blob per request near threshold vs multiple small blobs per request.
- Directory encoder metadata precision: exact timestamp range and normalization rules.
- Unix mode policy: which bits are preserved, masked, or rejected.
- `rfs doctor` scope: whether it should verify FUSE, CAS, cache paths, active-session state, and proto compatibility.
- Error code taxonomy for CLI exits.
- JSON output stability and schema/versioning for `doctor`, `status`, `upload`, and `snapshot`.
- Logging format and where daemon logs live under `RFS_HOME`.
- SQLite transaction boundaries for FUSE mutations.
- Open file handle tracking rules for snapshot barrier correctness.
- Rename behavior details for edge cases beyond rename-over-file and non-empty dirs.
- Whether `rfs snapshot` should be allowed on a plain local directory before daemon snapshot support exists, or deferred.
- Whether `rfs upload` and `rfs snapshot` should share exactly one tree-writer abstraction from the first implementation slice.
- How to structure e2e fixtures and whether fixture roots should have golden digests.
- Whether Buildbarn compatibility should get a smoke test in MVP or remain a documented secondary target only.

## Current Recommended Next Question

Ask about large-file COW policy:

Should a mutation of a remote-backed large file warn only, fail above a configurable threshold, or allow unbounded whole-file copy-up?

Recommended answer: warn only by default, with an optional configurable `--max-copy-up-bytes` or config field later. CI workloads may legitimately mutate large generated files, so a default hard failure can break evaluation runs. Metrics and warnings should make the cost visible.
