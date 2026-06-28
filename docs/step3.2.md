# Step 3.2 Implementation Plan: `rfs upload`

This plan expands Step 3.2 from `docs/implementation-plan.md`. It assumes Steps 2.1, 2.2, and 3.1 are complete enough that `Digest`, `BlobStore`, canonical `Directory` encoding, and CLI config validation already exist.

## Goals

- Turn `rfs upload <local-dir>` into the first complete local-directory ingestion path.
- Preserve the technical-design boundary where traversal, tree encoding, and CAS upload orchestration are separate.
- Keep directory encoding deterministic even though file hashing and uploads run concurrently.
- Bound memory while handling large files.
- Produce stable human and JSON command summaries with the root digest and counters.

## Non-Goals

- Include/exclude filters, `.gitignore`, or source-control awareness.
- Following symlinks.
- Preserving hard-link identity.
- Snapshotting mounted overlay state.
- Full observability polish beyond the upload summary fields required by Step 3.2.

## Suggested Code Organization

Use the existing module set and split Step 3.2 by responsibility:

- `src/cli.rs`
  - Owns command parsing, config resolution, user-facing output, exit behavior, and JSON summary rendering.
  - Should call one high-level upload API and avoid walking files or talking to CAS directly outside client construction.
- `src/upload.rs`
  - Owns the upload pipeline, local filesystem traversal, file hashing scheduler, upload counters, and orchestration over `BlobStore`.
  - Should not know about `clap`, process exits, or daemon/session state.
- `src/tree.rs`
  - Continues to own canonical REAPI `Directory` construction, metadata normalization, deterministic digest calculation, and tree warnings.
  - Should not read local file contents or perform CAS calls.
- `src/cas.rs`
  - Continues to own transport details and the `BlobStore` abstraction.
  - Needs a streaming/upload-from-path boundary for large file uploads; ByteStream resource names and request packing stay private.
- `tests/fixtures/`
  - Add deterministic upload fixtures, including regular files, nested directories, empty directories, symlinks, executable files, and metadata cases.

Avoid adding a new top-level `local_fs` module for this step unless `src/upload.rs` becomes too large. A private `upload::local` submodule is enough if the walker needs separation.

## Public Boundaries

### `upload` Module

Expose one CLI-friendly entrypoint:

```rust
#[derive(Default)]
pub struct UploadOptions {
    pub hash_workers: usize,
    pub bytestream_upload_concurrency: usize,
    pub in_flight_bytes: usize,
    pub fail_on_unsupported_nodes: bool,
}


pub struct UploadSummary {
    pub root_digest: Digest,
    pub files: usize,
    pub directories: usize,
    pub symlinks: usize,
    pub uploaded_blobs: usize,
    pub reused_blobs: usize,
    pub bytes_uploaded: u64,
    pub warnings: UploadWarnings,
}

pub struct UploadWarnings {
    pub hard_links: usize,
    pub masked_setuid: usize,
    pub masked_setgid: usize,
    pub absolute_symlinks: usize,
    pub escaping_symlinks: usize,
}

pub async fn upload_local_directory<S: BlobStore + Send>(
    store: &mut S,
    root: impl AsRef<Path>,
    options: UploadOptions,
) -> Result<UploadSummary, UploadError>;
```

Keep lower-level testable pieces public only if they are useful to Step 7 snapshot code or unit tests:

```rust
pub fn scan_local_directory(root: impl AsRef<Path>) -> Result<LocalTree, UploadError>;

pub async fn hash_files(
    files: &[LocalFile],
    options: &UploadOptions,
) -> Result<Vec<FileDigest>, UploadError>;

pub fn encode_local_tree(
    tree: LocalTree,
    file_digests: Vec<FileDigest>,
) -> Result<EncodedDirectoryTree, UploadError>;
```

Prefer `pub(crate)` for pipeline internals such as channel messages, byte-budget guards, worker spawn helpers, and hard-link tracking.

### Existing `upload` API Changes

The current `scan_local_tree` and `upload_missing` helpers were intentionally minimal for Step 2.2. Step 3.2 should change these boundaries:

- Replace `scan_local_tree(root) -> EncodedDirectoryTree` with separate scan, hash, and encode phases so traversal no longer reads whole file contents.
- Keep `upload_inputs_for_tree` or replace it with an internal conversion that preserves stable ordering.
- Replace `upload_missing(store, inputs)` with an uploader that returns uploaded/reused counts and byte totals. It should not perform a second hidden `FindMissingBlobs` if the caller already checked existence.
- Move all hard-link detection and unsupported-node policy into `src/upload.rs`, not `src/tree.rs`.

### `tree` Module

Keep the canonical encoder upload-free. If Step 3.2 needs richer inputs, extend with explicit data-only types rather than adding local filesystem knowledge:

```rust
pub struct EncodedDirectoryTree {
    pub root_digest: Digest,
    pub directories: Vec<EncodedDirectory>,
    pub file_blobs: Vec<FileBlobRef>,
    pub warnings: TreeWarnings,
}

pub struct FileBlobRef {
    pub digest: Digest,
    pub path: PathBuf,
}
```

### `cas` Module

Keep `BlobStore` narrow, but add a path/stream-oriented upload surface so large files are not forced into memory:

```rust
pub struct Blob {
    pub digest: Digest,
    pub size_bytes: u64,
    pub contents: BlobContents,
}

pub enum BlobContents {
    Bytes(Bytes),
    FilePath(PathBuf),
}

#[async_trait]
pub trait BlobStore {
    async fn find_missing_blobs(&mut self, digests: &[Digest]) -> Result<Vec<Digest>, CasError>;
    async fn upload_blob_sources(&mut self, blobs: Vec<Blob>) -> Result<UploadStats, CasError>;
    async fn download_blob(&mut self, digest: &Digest) -> Result<Bytes, CasError>;
}

pub struct UploadStats {
    pub uploaded_blobs: usize,
    pub bytes_uploaded: u64,
}
```

Production upload should use `Blob` so:

- Small missing files may be read into memory and packed into `BatchUpdateBlobs`.
- Directory nodes use in-memory `Bytes`.
- Large missing files are reread from disk for ByteStream.
- ByteStream concurrency stays private to `CasClient` or is driven by the upload pipeline through a small config value.

Do not expose ByteStream resource-name builders, retry classifiers, batch packing helpers, or tonic clients.

## Implementation Slices

### Slice 1: Data Model and Counters

- Add `UploadOptions`, `UploadSummary`, `UploadWarnings`, and internal `LocalTree`, `LocalNode`, `LocalFile`, and `FileDigest` types.
- Make `UploadOptions::default()` compute:
  - `hash_workers = max(min(available_parallelism, 8), 2)`.
  - `bytestream_upload_concurrency = 4`.
  - `in_flight_bytes = 64 * 1024 * 1024`.
  - `fail_on_unsupported_nodes = true`.
- Add deterministic summary ordering rules:
  - Store paths relative to the upload root.
  - Sort all user-visible path lists by UTF-8 path bytes when they exist.
  - Counters must not depend on worker completion order.

Tests:

- Unit: default worker counts respect low and high CPU counts through an injectable helper.
- Unit: summary serialization order is stable.

### Slice 2: Local Filesystem Walker

- Implement a single-threaded walker that uses `symlink_metadata` and never follows symlinks.
- Record directory structure, metadata, and symlink targets without hashing file bytes.
- Reject non-UTF-8 names and symlink targets through existing structured errors.
- Detect unsupported node types and fail by default.
- Detect hard links by `(dev, ino)` for regular files:
  - First occurrence is normal.
  - Later occurrences increment `warnings.hard_links`.
  - Every path remains a separate regular file entry and points to the content digest once hashing completes.
- Include the root directory metadata in the root `Directory` encoding.

Tests:

- Unit: walker records regular files, dirs, empty dirs, symlinks, mtimes, and modes.
- Unit: walker does not follow symlinked directories.
- Unit: unsupported nodes fail by default.
- Unit: hard links are counted while both paths remain present.

### Slice 3: Bounded File Hashing

- Replace the current whole-file read hashing path with streaming SHA-256 hashing.
- Use a bounded worker pool fed by the single walker output.
- Track in-flight read buffers with a byte budget. The budget should bound buffered chunks, not total input file size.
- Return `FileDigest { relative_path, absolute_path, digest, size_bytes }`.
- Sort final digest results by relative path before directory encoding.

Implementation notes:

- Keep chunk size private, for example 256 KiB or 1 MiB.
- Use blocking file IO inside `tokio::task::spawn_blocking` or keep the whole hashing stage synchronous behind worker threads. Do not block async reactor tasks with large reads.
- Surface path-rich errors when a file disappears or changes during upload. A conservative MVP can fail with a clear filesystem error rather than trying to reconcile races.

Tests:

- Unit: hashing a fixture file returns the expected digest and byte size.
- Unit: worker completion order does not affect sorted digest results.
- Unit: in-flight byte accounting blocks or rejects work above the configured budget in a deterministic fake.

### Slice 4: Bottom-Up Encoding

- Convert the walked tree plus hashed file digests into canonical `DirectoryBuilder` inputs.
- Encode directories bottom-up.
- Preserve current `tree.rs` metadata rules for modes, mtimes, symlinks, and warning counts.
- Keep path traversal order irrelevant by relying on explicit sorting before builder input and the builder's canonical ordering.
- Produce `EncodedDirectoryTree` and merge `TreeWarnings` into `UploadWarnings`.

Tests:

- Unit: identical directory contents produce the same root digest regardless of scan/hash order.
- Unit: executable bits and mtimes are reflected in decoded REAPI nodes.
- Unit: symlink warnings are stable.

### Slice 5: CAS Existence and Upload Orchestration

- Collect digests for all file blobs and encoded directory nodes.
- Call `FindMissingBlobs` once for files after hashing, and once for directory nodes after encoding, or once for the combined set after encoding. Prefer one combined call unless memory or API shape makes that awkward.
- Split missing objects into:
  - Directory node bytes.
  - Small file blobs for `BatchUpdateBlobs`.
  - Large file sources for ByteStream.
- Use the CAS client's configured batch threshold and upload implementation for the final split. Avoid duplicating batch/ByteStream policy in `upload.rs` unless `BlobStore` cannot express sources cleanly.
- Count reused blobs as `total_digest_count - missing_digest_count`.
- Count bytes uploaded from the source sizes of actually missing objects.

Tests:

- Unit with fake `BlobStore`: existing blobs are not uploaded.
- Unit with fake `BlobStore`: uploaded/reused/blob/byte counters are correct for mixed file and directory objects.
- Unit: directory nodes are included in existence checks and upload counts.

### Slice 6: CLI Wiring

- Change `Commands::Upload` in `src/cli.rs` from `NotImplemented` to:
  - Resolve config.
  - Validate that `local_dir` exists and is a directory.
  - Build `CasConfig` and connect `CasClient`.
  - Call `upload::upload_local_directory`.
  - Print the root digest on stdout for human output.
  - Print warnings and counters to stderr or as a compact summary after the digest only if the current CLI style permits it. Logs must remain on stderr.
  - For `--json`, print one JSON object on stdout.
- Keep all upload-specific command execution behind a helper such as:

```rust
async fn run_upload(config: CliConfig, local_dir: PathBuf) -> Result<CommandOutput, CliError>;
```

- Convert `main` to async if needed:

```rust
#[tokio::main]
async fn main() { ... }
```

or keep a small runtime wrapper in `cli::run` if the project prefers a synchronous `main`.

### Slice 7: JSON and Human Output

For human output, make stdout easy to script:

```text
sha256:<hash>/<size>
```

For `--json`, use the stable envelope expected by the technical design:

```json
{
  "schema_version": 1,
  "command": "upload",
  "ok": true,
  "warnings": {
    "hard_links": 1,
    "masked_setuid": 0,
    "masked_setgid": 0,
    "absolute_symlinks": 0,
    "escaping_symlinks": 1
  },
  "error": null,
  "data": {
    "root_digest": "sha256:...",
    "files": 3,
    "directories": 2,
    "symlinks": 1,
    "uploaded_blobs": 4,
    "reused_blobs": 2,
    "bytes_uploaded": 1234
  }
}
```

On failure with `--json`, print the same envelope shape with `ok: false`, `data: null`, and `error: { code, message, details }`.

### Slice 8: Task Targets and Fixtures

- Add `task test:integration:upload`.
- Add `task fixture:upload`, defaulting to a checked-in fixture and the local `bazel-remote` endpoint.
- Add fixture setup helpers if Git cannot preserve the exact mode or mtime required by tests.
- Keep large generated fixture files out of Git unless they are tiny deterministic samples.

## Boundary Changes Summary

- `src/upload.rs` changes from a minimal scanner/uploader helper into the owner of the local upload pipeline.
- `src/tree.rs` remains a pure encoder/decoder boundary. It may accept richer file references, but it should not perform local traversal, hashing, or CAS calls.
- `src/cas.rs` should grow a source-based upload method so production upload can stream large files from disk. Existing in-memory `Blob` upload remains useful for tests and directory nodes.
- `src/cli.rs` becomes the only module that knows how `rfs upload` is rendered for users.
- No daemon, SQLite, FUSE, or control-socket boundaries should be introduced in Step 3.2.

## Definition of Done

- `rfs upload <local-dir>` uploads a local directory into CAS and prints one root digest.
- Upload handles regular files, directories, empty directories, and symlinks without following symlinks.
- Unsupported nodes fail by default with a path-rich error.
- Hard links are warned/counted and represented as ordinary files.
- Hashing and uploading are bounded and deterministic.
- Large files are uploaded through a path/streaming boundary rather than loaded fully into memory.
- Human and JSON summaries are stable.
- `task test:unit`, `task test:integration:upload`, and `task fixture:upload` pass with the local CAS running.
