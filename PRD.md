# RemoteFS MVP PRD

## Overview

RemoteFS is a remote filesystem product for CI/CD infrastructure teams. It enables CI jobs on Linux self-hosted runners to mount source and build-output snapshots almost instantly, fetch file data lazily on demand, and upload the final workspace state back as a new immutable filesystem snapshot.

The core product idea is to replace expensive repeated checkout and cache restore steps with a content-addressed remote filesystem:

- File blobs are addressed by SHA-256 of their byte contents.
- Filesystem snapshots are represented as Merkle trees of directory metadata and child references.
- A snapshot is identified only by its structured root digest, represented to users as `sha256:<hex>/<size>`.
- CI/CD systems are responsible for maintaining their own mapping from commits, branches, builds, cache keys, or job IDs to RemoteFS root digests.

The MVP is intended to validate whether lazy CI workspace materialization and incremental snapshot upload can materially reduce CI setup time, remote data transfer, and incremental build time.

## Target Users

The primary users are CI/CD infrastructure and build platform teams operating Linux self-hosted runners.

These teams are responsible for:

- Installing and operating a Remote Execution API compatible content-addressable storage service.
- Installing the RemoteFS CLI/FUSE client and mount daemon on runners.
- Deciding which source and output snapshots to use for a job.
- Persisting root digests in their own CI metadata systems.
- Comparing RemoteFS-backed builds against existing checkout/cache workflows.

Individual developer workstation usage, hosted CI runners, and managed SaaS operation are outside the MVP scope.

## Problem

CI/CD systems repeatedly spend time and bandwidth doing work that is often redundant:

- Checking out large source trees.
- Restoring build outputs or dependency caches.
- Downloading files that the job never actually reads.
- Uploading outputs that are mostly unchanged from prior builds.

Existing CI cache mechanisms often operate at archive or directory granularity. They tend to require eager download, eager extraction, and coarse invalidation. This makes them inefficient for large repositories, monorepos, and incremental build systems that only need a subset of the available files.

RemoteFS should allow a CI job to begin with a usable filesystem view immediately, while deferring metadata and file-content transfer until the job actually touches specific paths.

## Goals

- Provide near-instant mounting of existing filesystem snapshots on Linux self-hosted runners.
- Lazily fetch directory metadata and file contents as build tools access paths.
- Present a writable local workspace using copy-on-write semantics over immutable remote snapshots.
- Upload local changes and new outputs as a new immutable snapshot without re-uploading unchanged content.
- Reuse existing blobs and unchanged Merkle subtrees by digest.
- Support repeated CI jobs that reuse previous source and output snapshots for incremental builds.
- Provide enough client-side observability, and enough visibility into the configured CAS, for CI infrastructure teams to evaluate performance and diagnose failures.
- Keep the MVP lean by avoiding CI metadata ownership, hosted-runner support, enterprise security, retention, and managed-service concerns.

## Non-Goals

The MVP will not provide:

- Hosted CI runner support.
- macOS, Windows, or ARM support.
- Native GitHub Actions, GitLab CI, Jenkins, Buildkite, or other CI plugins.
- SDKs or client libraries.
- Direct Git provider ingestion or Git-aware storage semantics.
- Service-managed snapshot names, refs, aliases, metadata search, or lifecycle state.
- Authentication, authorization, TLS, SSO, audit logs, or enterprise security controls.
- Snapshot retention policies or garbage collection.
- A custom RemoteFS storage service.
- Managed SaaS deployment.
- High availability, replication, or cross-node durability guarantees.
- Block-level copy-on-write.
- Sparse-file preservation.
- Full POSIX compatibility, including hard links, device files, FIFOs, sockets, extended attributes, ACLs, or host-specific ownership semantics.

These items may be addressed after the MVP if the core workflow proves valuable.

## Product Model

### Blobs

Regular file contents are stored as content-addressed blobs in a Remote Execution API compatible content-addressable store. A blob digest contains both the SHA-256 hash of the file bytes and the blob size.

Identical file contents should deduplicate even when the files appear at different paths or have different filesystem metadata.

### Filesystem Snapshots

A filesystem snapshot is an immutable Merkle tree rooted at a digest.

MVP snapshots use the Remote Execution API `Directory` model as their tree encoding. RemoteFS defines a constrained profile of that model for supported node types, metadata, and digest behavior.

Directory/tree nodes include enough metadata to reconstruct the supported filesystem view, including:

- Entry names.
- Entry types.
- Child blob or tree digests.
- Symlink targets.
- Basic mode metadata, including executable bits.
- File and directory mtimes.
- File sizes where useful for filesystem behavior and diagnostics.

Mtimes are part of snapshot state because many incremental build systems use timestamps for correctness.

Snapshots created by MVP clients must remain readable by later MVP versions.

### Snapshot Identity

The root digest is the only RemoteFS-provided snapshot identifier. It is represented as `sha256:<64-lowercase-hex>/<decimal-size-bytes>`.

RemoteFS must not own CI/CD naming, metadata, refs, aliases, or lookup semantics. CI/CD systems are responsible for storing mappings such as:

- Commit SHA to source snapshot root digest.
- Build ID to output snapshot root digest.
- Cache key to root digest.
- Latest successful build to root digest.

RemoteFS may provide direct validation or lookup by digest, but it must not become a metadata database.

Root digests are capability references, not durable backup handles. A root digest can be mounted only while the configured content-addressable store still contains the root object and all reachable child objects. MVP deployments must disable remote CAS eviction or provision enough remote CAS capacity for snapshots they intend to reuse.

## MVP Components

### Remote Execution API CAS

RemoteFS uses an external Remote Execution API compatible content-addressable storage service for blobs and Merkle tree nodes.

The default MVP evaluation target is `bazel-remote`. Buildbarn storage is the secondary compatibility target.

The configured CAS must support:

- Uploading blobs addressed by SHA-256 digest and size.
- Uploading and retrieving Merkle tree nodes.
- Retrieving blobs and tree nodes by digest.
- Batch existence checks by digest to support deduplicating uploads.
- Concurrent reads of immutable snapshots.
- Idempotent concurrent uploads of the same blob or tree node.
- Single-node crash-safe object writes so completed objects are not corrupted by process failure.

### Linux CLI/FUSE Client and Daemon

The MVP ships as Linux x86_64 binaries for self-hosted runners with FUSE support:

- `rfs`, the user-facing CLI.
- `rfsd`, the mount daemon that owns the FUSE mount and local session state.

The MVP supports at most one active RemoteFS mount per `RFS_HOME` on a machine. Starting another mount with the same `RFS_HOME` while a session is active must fail with a clear error. Operators that need concurrent mounts during MVP evaluation can use separate `RFS_HOME` values.

The CLI must cover both ingestion and materialization workflows:

- Upload a local directory and return a root digest.
- Mount a root digest at a local mount point as a lazy filesystem.
- Present a writable copy-on-write view over the immutable remote snapshot.
- Snapshot a mounted or local workspace and return a new root digest.
- Expose diagnostics for CAS reachability, cache behavior, mount state, and transfer behavior.

The storage protocol is the Remote Execution API. The CLI workflow is the main product interface.

## Core Workflows

### Bootstrap Workflow

The first build for a repository or workspace may use the existing CI process:

1. Perform ordinary checkout and build.
2. Upload the source directory, selected outputs, or full workspace with the RemoteFS CLI.
3. Store the returned root digest in the CI/CD system's own metadata store.

RemoteFS does not need to improve first-build performance. The MVP optimizes repeat builds after useful snapshots already exist.

### Lazy Build Workflow

A repeat build can use RemoteFS snapshots:

1. CI/CD system resolves the desired root digest from its own metadata.
2. Runner has access to a configured Remote Execution API CAS endpoint.
3. `rfs mount` mounts the root digest into the job workspace.
4. Build tools access the mounted filesystem normally.
5. Directory metadata and file contents are fetched lazily as needed.
6. Writes are captured in a local copy-on-write overlay.
7. At the end of the job, the CLI snapshots the full merged workspace.
8. The CLI uploads only new or changed content and returns a new root digest.
9. CI/CD system records that digest externally for future jobs.

### Whole Workspace Snapshot Workflow

RemoteFS must support efficient snapshotting of the final merged workspace.

If a job uploads the entire working directory, unchanged remote-backed source files and subtrees should remain references to existing blobs and Merkle nodes. Local changes, new files, deleted paths, renamed entries, and changed directory metadata produce new Merkle nodes only along affected paths and ancestors.

## Functional Requirements

### Lazy Mounting

- Mounting an existing root digest must not eagerly download file contents.
- Mounting should complete in under 2 seconds for snapshots up to 1 million files, assuming a healthy runner and reachable CAS.
- Directory metadata should be fetched lazily during lookup and directory traversal.
- Directory metadata should be cached locally after fetch.
- File contents should be fetched on first read unless already present in the local cache.

### Copy-on-Write Workspace

- Remote snapshots are immutable.
- The mounted filesystem presents a writable merged view.
- Reads of unchanged files are served lazily from remote blobs or local cache.
- The first mutation of a remote-backed file uses whole-file copy-on-write.
- After a file is copied into the local overlay, future reads and writes use the local copy.
- New files and directories are created only in the local overlay.
- Deletes remove entries from the merged view and are reflected in later snapshots.
- Renames and directory mutations are represented as local changes and reflected in later snapshots.
- Remote snapshots are never modified by writes through the mount.

### Incremental Snapshot Creation

- The client must be able to snapshot the full merged workspace.
- Snapshot creation must upload only blobs and tree nodes missing from the configured Remote Execution API CAS instance.
- Unchanged remote-backed content must remain referenced by digest.
- Creates, modifies, deletes, renames, and metadata changes should rewrite only affected Merkle tree nodes and their ancestors.
- Upload should be parallel, streaming, and able to handle large directories without loading all file contents into memory.
- The client must use batch digest existence checks to skip already-present blobs and tree nodes.

### Filesystem Compatibility

The MVP must support:

- Regular files.
- Directories.
- Symlinks stored as symlink entries, not followed by default during upload.
- Basic mode bits, including executable bits.
- File and directory mtimes.
- Deletes and renames through the local overlay.

The MVP excludes:

- Hard-link identity. Hard-linked regular files may deduplicate by content digest, but link identity is not preserved.
- Device files.
- FIFOs.
- Sockets.
- Extended attributes.
- ACLs.
- Sparse-file preservation.
- UID/GID ownership preservation.

For supported features, a build run against a RemoteFS copy-on-write mount should produce the same result as the same build run against a fully local directory with the same inputs and environment.

### Integrity

- Remote file downloads must be verified against the requested SHA-256 digest before being served or admitted into the local cache.
- Local cache entries may be trusted by default after verified admission.
- The client must not re-hash every cached blob on every read.
- Optional debug or background cache verification may be added later.

### Local Caching

- The FUSE client must maintain a persistent runner-local cache across CI jobs.
- Blob cache entries are keyed by content digest.
- Directory/tree metadata cache entries are keyed by tree digest.
- Cache location must be configurable.
- Automatic local cache eviction may be deferred in the earliest MVP, but evaluation-ready builds must provide a bounded local cache or documented manual pruning workflow.
- Cache hit/miss behavior must be observable.

### Failure Behavior

- Remote fetch failures during lazy reads must surface as job-visible filesystem errors.
- The client must not silently return empty files, partial content, or stale unverified data.
- Failures should include path, digest, operation, and remote error context in structured diagnostics where possible.
- Fallback to ordinary checkout or cache restore is the responsibility of the CI/CD pipeline, not automatic RemoteFS behavior in MVP.

### Observability

The client should emit structured logs and metrics for:

- Mount lifecycle.
- Lazy metadata fetches.
- Lazy blob fetches.
- Cache hits and misses.
- Bytes downloaded and uploaded.
- Upload deduplication ratio.
- Read latency for remote-backed files.
- Snapshot creation duration.
- Remote request failures.
- Digest verification failures.
- Slow paths or high-latency operations.

The configured CAS may expose its own metrics for:

- Request count and latency.
- Error rates.
- Bytes uploaded and downloaded.
- Blob and tree-node existence checks.
- Object counts and storage footprint where practical.
- Digest verification or object-write failures.

## Performance Targets

The MVP should be evaluated against these targets:

- Mount an existing snapshot with up to 1 million files in under 2 seconds.
- Avoid downloading file contents during mount.
- Fetch directory metadata lazily instead of traversing the entire tree during mount.
- Demonstrate reduced source setup time compared with ordinary checkout on repeat builds.
- Demonstrate reduced incremental build time when previous output snapshots are reused.
- Demonstrate lower remote bytes downloaded than archive-based cache restore for builds that read only a subset of files.
- Demonstrate deduplicating upload where unchanged source and output content is not re-uploaded.

## Success Metrics

MVP success should be measured by:

- Mount latency for existing snapshots.
- Source checkout/setup time reduction versus baseline CI.
- Incremental build wall-clock time reduction when reusing previous outputs.
- Remote bytes downloaded during a job.
- Upload time after a job.
- Upload deduplication ratio.
- Persistent local cache hit rate.
- Correctness against a baseline non-RemoteFS build.
- Frequency and diagnosability of lazy-read failures.

## Deployment Assumptions

- Customer runs a Remote Execution API compatible CAS themselves.
- The default MVP CAS deployment uses `bazel-remote` with remote eviction disabled or enough capacity for retained evaluation snapshots.
- Runners are Linux x86_64 machines with FUSE support.
- CI jobs can install and execute `rfs` and `rfsd`.
- CI/CD systems maintain their own root-digest metadata.
- MVP environments are trusted enough that auth, TLS, and multi-user security are not required for initial validation.

## Immediate Future Scope

The following should be considered soon after MVP validation:

- Authentication, authorization, and TLS.
- Project-level retention policies and garbage collection.
- S3-compatible or other object-storage backends.
- Native CI integrations.
- SDKs for common integration languages.
- Hosted runner feasibility.
- Managed service deployment.
- Richer operational dashboards.
- Block-level copy-on-write for very large files.
- Sparse-file preservation.
- Broader POSIX metadata support.
- High availability and replicated durability.

## Milestones

### Milestone 1: Read-Only Lazy Mount

The first implementation milestone should prove the end-to-end storage and filesystem path:

1. Upload a local directory into the configured CAS.
2. Return a root digest in `sha256:<hex>/<size>` form.
3. Mount that root digest read-only.
4. Fetch directory metadata lazily.
5. Fetch and verify file blobs on first read.
6. Remount the uploaded root successfully.

### Milestone 2: Writable Copy-on-Write Workspace

The second implementation milestone should add writable workspace behavior:

1. Track local creates, writes, deletes, renames, and metadata changes.
2. Use whole-file copy-on-write for first mutation of remote-backed files.
3. Snapshot the full merged workspace through the live mount daemon.
4. Upload only missing blobs and directory nodes.
5. Return a new root digest that can be mounted by a later job.

## Open Questions

- What concrete baseline repositories and build workloads will be used to validate MVP performance?
- What local runner cache eviction policy should be the default for self-hosted runners?
- Should snapshot composition from existing subtree digests be exposed as an explicit CLI capability in MVP, or only emerge through upload/snapshot behavior?
- What minimum Linux kernel and FUSE versions should be supported?
- What is the acceptable local disk overhead for overlay data and persistent cache during a CI job?
