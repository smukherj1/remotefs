use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;
use sha2::{Digest as ShaDigest, Sha256};
use thiserror::Error;
use tokio::sync::Semaphore;

use crate::shared::cas::{Blob, BlobStore, CasError};
use crate::shared::digest::Digest;
use crate::shared::error_context::ResultContext as _;
use crate::shared::tree::{
    DirectoryBuilder, DirectoryEntry, EncodedDirectory, EncodedDirectoryTree, FileEntry, NodeKind,
    NodeMetadata, SymlinkEntry, TreeError, TreeWarnings,
};

const DEFAULT_IN_FLIGHT_BYTES: usize = 64 * 1024 * 1024;
const MAX_DEFAULT_HASH_WORKERS: usize = 8;
const MIN_DEFAULT_HASH_WORKERS: usize = 2;
const HASH_CHUNK_BYTES: usize = 1024 * 1024;

/// Options controlling local-directory upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadOptions {
    /// Maximum number of blocking worker tasks used to hash regular files.
    ///
    /// Must be greater than zero. Workers read files from disk without
    /// following symlinks discovered by the scanner.
    pub hash_workers: usize,
    /// Approximate maximum bytes buffered by the hashing stage at once.
    ///
    /// Must be at least `hash_workers`, because each worker needs a minimum
    /// one-byte read buffer. The implementation divides this budget across
    /// workers and caps each per-file read buffer at `HASH_CHUNK_BYTES`.
    pub in_flight_bytes: usize,
    /// Whether scanning should reject sockets, devices, FIFOs, and unknown nodes.
    ///
    /// When true, the first unsupported node returns `UploadError::Tree`.
    /// When false, unsupported nodes are skipped and no CAS object is emitted
    /// for them.
    pub fail_on_unsupported_nodes: bool,
}

impl Default for UploadOptions {
    fn default() -> Self {
        Self {
            hash_workers: default_hash_workers(),
            in_flight_bytes: DEFAULT_IN_FLIGHT_BYTES,
            fail_on_unsupported_nodes: true,
        }
    }
}

/// Stable summary returned by `rfs upload`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UploadSummary {
    /// Digest of the root REAPI `Directory` object uploaded or reused in CAS.
    pub root_digest: Digest,
    /// Count of regular file entries scanned under the upload root.
    pub files: usize,
    /// Count of directories scanned, including the upload root.
    pub directories: usize,
    /// Count of symlink entries scanned without following their targets.
    pub symlinks: usize,
    /// Count of unique CAS objects uploaded during this invocation.
    pub uploaded_blobs: usize,
    /// Count of unique CAS objects already present according to `FindMissingBlobs`.
    pub reused_blobs: usize,
    /// Total bytes uploaded for missing file blobs and directory objects.
    pub bytes_uploaded: u64,
    /// Warning counters for supported lossy or surprising filesystem inputs.
    pub warnings: UploadWarnings,
}

/// Warning counters for supported but lossy or surprising upload inputs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct UploadWarnings {
    /// Additional regular-file paths sharing a `(dev, ino)` pair with an earlier path.
    pub hard_links: usize,
    /// File, directory, or symlink metadata entries where setuid was masked.
    pub masked_setuid: usize,
    /// File, directory, or symlink metadata entries where setgid was masked.
    pub masked_setgid: usize,
    /// Symlinks whose target is absolute.
    pub absolute_symlinks: usize,
    /// Symlinks whose relative target escapes the encoded tree.
    pub escaping_symlinks: usize,
}

impl UploadWarnings {
    fn merge_tree(&mut self, warnings: &TreeWarnings) {
        self.masked_setuid += warnings.masked_setuid;
        self.masked_setgid += warnings.masked_setgid;
        self.absolute_symlinks += warnings.absolute_symlinks;
        self.escaping_symlinks += warnings.escaping_symlinks;
    }
}

/// Scanned local filesystem tree before file content hashing.
///
/// Directories are stored deepest-first so bottom-up REAPI encoding can look
/// up already encoded child directory digests. File and symlink counts reflect
/// the scanned metadata only; file contents are read later by `hash_files`.
#[derive(Debug)]
pub(crate) struct LocalTree {
    pub root: PathBuf,
    pub directories: Vec<LocalDirectory>,
    pub files: Vec<LocalFile>,
    pub symlinks: usize,
    pub warnings: UploadWarnings,
}

/// Scanned local directory and its immediate entries.
///
/// Entries are sorted by raw filename bytes during scanning so later phases do
/// not depend on filesystem iteration order.
#[derive(Debug)]
pub(crate) struct LocalDirectory {
    pub relative_path: PathBuf,
    pub absolute_path: PathBuf,
    pub metadata: fs::Metadata,
    pub entries: Vec<LocalDirectoryChild>,
}

/// Scanned child entry of a LocalDirectory.
#[derive(Debug)]
pub(crate) enum LocalDirectoryChild {
    File(LocalFile),
    Directory(LocalSubDirectory),
    Symlink(LocalSymlink),
}

/// Scanned local regular file.
///
/// The scanner records metadata and paths only. `hash_files` is responsible
/// for reading contents and producing the digest used by encoding and upload.
#[derive(Debug, Clone)]
pub(crate) struct LocalFile {
    pub name: String,
    pub relative_path: PathBuf,
    pub absolute_path: PathBuf,
    pub metadata: fs::Metadata,
}

/// File digest produced by the hashing stage.
///
/// Results are sorted by relative path before encoding so worker completion
/// order cannot affect the directory tree or summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileDigest {
    pub relative_path: PathBuf,
    pub absolute_path: PathBuf,
    pub digest: Digest,
    pub size_bytes: u64,
}

/// Reference to a child directory in a LocalDirectory.
#[derive(Debug)]
pub(crate) struct LocalSubDirectory {
    pub name: String,
    pub relative_path: PathBuf,
    pub metadata: fs::Metadata,
}

/// Scanned symlink entry and target text.
#[derive(Debug)]
pub(crate) struct LocalSymlink {
    pub name: String,
    pub target: String,
    pub metadata: fs::Metadata,
}

/// Errors returned while scanning, hashing, encoding, or uploading local inputs.
#[derive(Error, Debug)]
pub enum UploadError {
    /// Canonical tree encoding failed.
    #[error(transparent)]
    Tree(#[from] TreeError),
    /// CAS existence checking or upload failed.
    #[error(transparent)]
    Cas(#[from] CasError),
    /// Local filesystem access failed for the given path.
    #[error("Filesystem error at {path}: {source}")]
    Filesystem {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Caller-provided options or pipeline invariants were invalid.
    #[error("Invalid upload option `{name}`: {reason}")]
    InvalidOption { name: &'static str, reason: String },
    /// Additional operation context attached while preserving the source error.
    #[error("{operation}: {source}")]
    Context {
        operation: String,
        #[source]
        source: Box<UploadError>,
    },
}

impl crate::shared::error_context::ResultContextError for UploadError {
    fn with_context(self, operation: String) -> Self {
        UploadError::Context {
            operation,
            source: Box::new(self),
        }
    }
}

/// Uploads a local directory to CAS and returns a stable summary.
///
/// The pipeline scans metadata without following symlinks, hashes regular files
/// with bounded worker concurrency, encodes REAPI directories bottom-up, checks
/// CAS existence once, and uploads only missing file blobs and directory nodes.
pub async fn upload_local_directory<S: BlobStore + Send>(
    store: &mut S,
    root: impl AsRef<Path>,
    options: UploadOptions,
) -> Result<UploadSummary, UploadError> {
    validate_options(&options)?;
    let tree = scan_local_directory_with_options(root.as_ref(), &options)
        .with_context(|| format!("scan local upload root {}", root.as_ref().display()))?;
    let files = tree.files.len();
    let directories = tree.directories.len();
    let symlinks = tree.symlinks;
    let mut warnings = tree.warnings.clone();
    let file_digests = hash_files(&tree.files, &options)
        .await
        .with_context(|| format!("hash {} file(s) under {}", files, tree.root.display()))?;
    let encoded = encode_local_tree(tree, file_digests)
        .context("encode scanned local tree as canonical REAPI directories")?;
    warnings.merge_tree(&encoded.warnings);
    let upload = upload_encoded_tree(store, &encoded)
        .await
        .context("upload missing file and directory blobs to CAS")?;

    Ok(UploadSummary {
        root_digest: encoded.root_digest,
        files,
        directories,
        symlinks,
        uploaded_blobs: upload.uploaded_blobs,
        reused_blobs: upload.reused_blobs,
        bytes_uploaded: upload.bytes_uploaded,
        warnings,
    })
}

/// Scans a local directory without following symlinks or reading file contents.
#[cfg(test)]
pub(crate) fn scan_local_directory(root: impl AsRef<Path>) -> Result<LocalTree, UploadError> {
    scan_local_directory_with_options(root.as_ref(), &UploadOptions::default())
}

/// Hashes scanned files using bounded worker concurrency.
pub(crate) async fn hash_files(
    files: &[LocalFile],
    options: &UploadOptions,
) -> Result<Vec<FileDigest>, UploadError> {
    validate_options(options)?;
    let semaphore = Arc::new(Semaphore::new(options.hash_workers));
    let chunk_bytes = hash_chunk_bytes(options);
    let mut tasks = Vec::new();

    for file in files {
        let file = file.clone();
        let permit =
            semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| UploadError::InvalidOption {
                    name: "hash_workers",
                    reason: "worker semaphore closed unexpectedly".to_string(),
                })?;
        tasks.push(tokio::task::spawn_blocking(move || {
            let result = hash_file(&file, chunk_bytes);
            drop(permit);
            result
        }));
    }

    let mut digests = Vec::with_capacity(tasks.len());
    for task in tasks {
        digests.push(task.await.map_err(|source| UploadError::InvalidOption {
            name: "hash_workers",
            reason: format!("hash worker panicked: {source}"),
        })??);
    }
    digests.sort_by(|left, right| {
        left.relative_path
            .as_os_str()
            .as_bytes()
            .cmp(right.relative_path.as_os_str().as_bytes())
    });
    Ok(digests)
}

/// Encodes a scanned tree and matching file digests as canonical directories.
pub(crate) fn encode_local_tree(
    tree: LocalTree,
    file_digests: Vec<FileDigest>,
) -> Result<EncodedDirectoryTree, UploadError> {
    let digest_by_path = file_digests
        .iter()
        .map(|file| (file.relative_path.clone(), file.digest.clone()))
        .collect::<HashMap<_, _>>();
    let mut directory_digests: HashMap<PathBuf, Digest> = HashMap::new();
    let mut directories = Vec::with_capacity(tree.directories.len());
    let mut warnings = TreeWarnings::default();

    for local in &tree.directories {
        let encoded = encode_directory(local, &digest_by_path, &directory_digests)
            .with_context(|| format!("encode local directory {}", local.absolute_path.display()))?;
        warnings.merge(&encoded.warnings);
        directory_digests.insert(local.relative_path.clone(), encoded.digest.clone());
        directories.push(encoded);
    }

    let root_digest = directory_digests
        .get(Path::new(""))
        .cloned()
        .ok_or_else(|| UploadError::InvalidOption {
            name: "root",
            reason: "scanned tree did not include a root directory".to_string(),
        })?;
    let file_blobs = file_digests
        .into_iter()
        .map(|file| (file.digest, file.absolute_path))
        .collect();

    Ok(EncodedDirectoryTree {
        root_digest,
        directories,
        file_blobs,
        warnings,
    })
}

/// Scans, hashes, and encodes a local tree for legacy integration tests.
#[cfg(test)]
pub(crate) fn scan_local_tree(root: impl AsRef<Path>) -> Result<EncodedDirectoryTree, TreeError> {
    let options = UploadOptions::default();
    let tree =
        scan_local_directory_with_options(root.as_ref(), &options).map_err(upload_tree_error)?;
    let digests = tree
        .files
        .iter()
        .map(|file| hash_file(file, hash_chunk_bytes(&options)).map_err(upload_tree_error))
        .collect::<Result<Vec<_>, _>>()?;
    encode_local_tree(tree, digests).map_err(upload_tree_error)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct UploadObjectStats {
    uploaded_blobs: usize,
    reused_blobs: usize,
    bytes_uploaded: u64,
}

async fn upload_encoded_tree<S: BlobStore + Send>(
    store: &mut S,
    tree: &EncodedDirectoryTree,
) -> Result<UploadObjectStats, UploadError> {
    let mut sources_by_digest: HashMap<Digest, Blob> = HashMap::new();
    for (digest, path) in &tree.file_blobs {
        sources_by_digest
            .entry(digest.clone())
            .or_insert_with(|| Blob::from_file_path(digest.clone(), path));
    }
    for directory in &tree.directories {
        sources_by_digest
            .entry(directory.digest.clone())
            .or_insert_with(|| Blob::from_bytes(directory.bytes.clone()));
    }

    let digests = sources_by_digest.keys().cloned().collect::<Vec<_>>();
    let missing = store
        .find_missing_blobs(&digests)
        .await
        .map_err(UploadError::from)
        .with_context(|| format!("check {} unique CAS digest(s) for upload", digests.len()))?;
    let missing_sources = missing
        .iter()
        .filter_map(|digest| sources_by_digest.get(digest).cloned())
        .collect::<Vec<_>>();
    let stats = store
        .upload_blobs(missing_sources)
        .await
        .map_err(UploadError::from)
        .with_context(|| format!("upload {} missing CAS object(s)", missing.len()))?;

    Ok(UploadObjectStats {
        uploaded_blobs: stats.uploaded_blobs,
        reused_blobs: digests.len().saturating_sub(missing.len()),
        bytes_uploaded: stats.bytes_uploaded,
    })
}

/// Scans filesystem metadata for the upload root without reading file contents.
///
/// This phase intentionally uses `symlink_metadata` and `read_link` so symlink
/// targets are recorded as links rather than traversed. The returned directory
/// list is arranged for bottom-up encoding.
fn scan_local_directory_with_options(
    root: &Path,
    options: &UploadOptions,
) -> Result<LocalTree, UploadError> {
    validate_options(options)?;
    let metadata = fs::symlink_metadata(root).map_err(|source| UploadError::Filesystem {
        path: root.to_path_buf(),
        source,
    })?;
    if !metadata.is_dir() {
        return Err(UploadError::InvalidOption {
            name: "root",
            reason: format!("{} is not a directory", root.display()),
        });
    }

    let mut state = ScanState::default();
    let root_directory = scan_directory(root, PathBuf::new(), &mut state, options)?;
    state.directories.push(root_directory);
    state.directories.sort_by(|left, right| {
        path_depth(&right.relative_path)
            .cmp(&path_depth(&left.relative_path))
            .then_with(|| {
                left.relative_path
                    .as_os_str()
                    .as_bytes()
                    .cmp(right.relative_path.as_os_str().as_bytes())
            })
    });

    Ok(LocalTree {
        root: root.to_path_buf(),
        directories: state.directories,
        files: state.files,
        symlinks: state.symlinks,
        warnings: state.warnings,
    })
}

/// Mutable state shared through recursive directory scanning.
///
/// Hard-link tracking uses Unix `(dev, ino)` identity. Each path remains in the
/// tree, but duplicate identities increment a warning counter for the summary.
#[derive(Default)]
struct ScanState {
    directories: Vec<LocalDirectory>,
    files: Vec<LocalFile>,
    symlinks: usize,
    warnings: UploadWarnings,
    hard_links: HashSet<(u64, u64)>,
}

/// Recursively scans one directory and records its immediate child nodes.
///
/// Child directories are fully scanned before the returned `LocalDirectory` is
/// constructed so `ScanState::directories` can later be sorted deepest-first.
fn scan_directory(
    path: &Path,
    relative_path: PathBuf,
    state: &mut ScanState,
    options: &UploadOptions,
) -> Result<LocalDirectory, UploadError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| UploadError::Filesystem {
        path: path.to_path_buf(),
        source,
    })?;
    let mut entries = fs::read_dir(path)
        .map_err(|source| UploadError::Filesystem {
            path: path.to_path_buf(),
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| UploadError::Filesystem {
            path: path.to_path_buf(),
            source,
        })?;
    entries.sort_by(|left, right| {
        left.file_name()
            .as_bytes()
            .cmp(right.file_name().as_bytes())
    });

    let mut local_entries = Vec::new();
    for entry in entries {
        let child_path = entry.path();
        let name = component_name(&child_path).map_err(UploadError::from)?;
        let child_relative_path = relative_path.join(&name);
        let child_metadata =
            fs::symlink_metadata(&child_path).map_err(|source| UploadError::Filesystem {
                path: child_path.clone(),
                source,
            })?;
        let file_type = child_metadata.file_type();
        if file_type.is_file() {
            let file = LocalFile {
                name,
                relative_path: child_relative_path,
                absolute_path: child_path,
                metadata: child_metadata,
            };
            if !state
                .hard_links
                .insert((file.metadata.dev(), file.metadata.ino()))
            {
                state.warnings.hard_links += 1;
            }
            state.files.push(file.clone());
            local_entries.push(LocalDirectoryChild::File(file));
        } else if file_type.is_dir() {
            let directory =
                scan_directory(&child_path, child_relative_path.clone(), state, options)
                    .with_context(|| format!("scan child directory {}", child_path.display()))?;
            let directory_ref = LocalSubDirectory {
                name,
                relative_path: child_relative_path,
                metadata: child_metadata,
            };
            state.directories.push(directory);
            local_entries.push(LocalDirectoryChild::Directory(directory_ref));
        } else if file_type.is_symlink() {
            let target = fs::read_link(&child_path).map_err(|source| UploadError::Filesystem {
                path: child_path.clone(),
                source,
            })?;
            let target = target
                .to_str()
                .ok_or_else(|| TreeError::NonUtf8SymlinkTarget {
                    path: child_path.clone(),
                })
                .map(ToOwned::to_owned)
                .map_err(UploadError::from)?;
            state.symlinks += 1;
            local_entries.push(LocalDirectoryChild::Symlink(LocalSymlink {
                name,
                target,
                metadata: child_metadata,
            }));
        } else if options.fail_on_unsupported_nodes {
            return Err(TreeError::UnsupportedNodeType {
                path: child_path,
                kind: node_type_name(file_type).to_string(),
            }
            .into());
        }
    }

    Ok(LocalDirectory {
        relative_path,
        absolute_path: path.to_path_buf(),
        metadata,
        entries: local_entries,
    })
}

/// Encodes one scanned directory using previously computed child digests.
///
/// File and child-directory digests are looked up by relative path. Missing
/// entries indicate a broken pipeline invariant and are reported as upload
/// option errors with the affected path.
fn encode_directory(
    local: &LocalDirectory,
    file_digests: &HashMap<PathBuf, Digest>,
    directory_digests: &HashMap<PathBuf, Digest>,
) -> Result<EncodedDirectory, UploadError> {
    let mut builder =
        DirectoryBuilder::with_metadata(metadata_for(NodeKind::Directory, &local.metadata));
    for entry in &local.entries {
        match entry {
            LocalDirectoryChild::File(file) => {
                let digest = file_digests
                    .get(&file.relative_path)
                    .cloned()
                    .ok_or_else(|| UploadError::InvalidOption {
                        name: "file_digests",
                        reason: format!("missing digest for {}", file.relative_path.display()),
                    })?;
                builder
                    .add_file(FileEntry {
                        name: file.name.clone(),
                        digest,
                        metadata: metadata_for(NodeKind::File, &file.metadata),
                    })
                    .map_err(UploadError::from)?;
            }
            LocalDirectoryChild::Directory(directory) => {
                let digest = directory_digests
                    .get(&directory.relative_path)
                    .cloned()
                    .ok_or_else(|| UploadError::InvalidOption {
                        name: "directory_digests",
                        reason: format!(
                            "missing digest for directory {}",
                            directory.relative_path.display()
                        ),
                    })?;
                builder
                    .add_directory(DirectoryEntry {
                        name: directory.name.clone(),
                        digest,
                        metadata: metadata_for(NodeKind::Directory, &directory.metadata),
                    })
                    .map_err(UploadError::from)?;
            }
            LocalDirectoryChild::Symlink(symlink) => {
                builder
                    .add_symlink(SymlinkEntry {
                        name: symlink.name.clone(),
                        target: symlink.target.clone(),
                        metadata: metadata_for(NodeKind::Symlink, &symlink.metadata),
                    })
                    .map_err(UploadError::from)?;
            }
        }
    }
    builder.encode().map_err(UploadError::from)
}

/// Streams one regular file into a SHA-256 digest using a reusable buffer.
///
/// The file size is compared with metadata captured during scanning so common
/// races, such as a file being appended or truncated during upload, fail with a
/// path-rich error instead of silently producing a mixed snapshot.
fn hash_file(file: &LocalFile, chunk_bytes: usize) -> Result<FileDigest, UploadError> {
    let mut handle =
        fs::File::open(&file.absolute_path).map_err(|source| UploadError::Filesystem {
            path: file.absolute_path.clone(),
            source,
        })?;
    let mut hasher = Sha256::new();
    let mut size_bytes = 0u64;
    let mut buffer = vec![0u8; chunk_bytes];
    loop {
        let read = handle
            .read(&mut buffer)
            .map_err(|source| UploadError::Filesystem {
                path: file.absolute_path.clone(),
                source,
            })?;
        if read == 0 {
            break;
        }
        size_bytes += read as u64;
        hasher.update(&buffer[..read]);
    }
    if size_bytes != file.metadata.len() {
        return Err(UploadError::InvalidOption {
            name: "file_size",
            reason: format!(
                "{} changed size during upload: scanned {} byte(s), read {} byte(s)",
                file.absolute_path.display(),
                file.metadata.len(),
                size_bytes
            ),
        });
    }
    let digest =
        Digest::new(hex::encode(hasher.finalize()), size_bytes as i64).map_err(|source| {
            UploadError::InvalidOption {
                name: "digest",
                reason: source.to_string(),
            }
        })?;
    Ok(FileDigest {
        relative_path: file.relative_path.clone(),
        absolute_path: file.absolute_path.clone(),
        digest,
        size_bytes,
    })
}

/// Converts a filesystem path basename into the UTF-8 name required by REAPI.
fn component_name(path: &Path) -> Result<String, TreeError> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(ToOwned::to_owned)
        .ok_or_else(|| TreeError::NonUtf8Name {
            path: path.to_path_buf(),
        })
}

/// Captures Unix mode and mtime metadata for later canonical normalization.
fn metadata_for(kind: NodeKind, metadata: &fs::Metadata) -> NodeMetadata {
    NodeMetadata::new(
        kind,
        Some(metadata.mode() & 0o7777),
        Some(prost_types::Timestamp {
            seconds: metadata.mtime(),
            nanos: metadata.mtime_nsec() as i32,
        }),
    )
}

/// Returns a stable user-facing name for unsupported Unix file types.
fn node_type_name(file_type: fs::FileType) -> &'static str {
    if file_type.is_block_device() {
        "block device"
    } else if file_type.is_char_device() {
        "character device"
    } else if file_type.is_fifo() {
        "fifo"
    } else if file_type.is_socket() {
        "socket"
    } else {
        "unknown"
    }
}

/// Rejects impossible hashing options before they reach async worker setup.
fn validate_options(options: &UploadOptions) -> Result<(), UploadError> {
    if options.hash_workers == 0 {
        return Err(UploadError::InvalidOption {
            name: "hash_workers",
            reason: "must be at least 1".to_string(),
        });
    }
    if options.in_flight_bytes == 0 {
        return Err(UploadError::InvalidOption {
            name: "in_flight_bytes",
            reason: "must be at least 1".to_string(),
        });
    }
    if options.in_flight_bytes < options.hash_workers {
        return Err(UploadError::InvalidOption {
            name: "in_flight_bytes",
            reason: format!(
                "must be at least hash_workers ({}) so every worker has a read buffer",
                options.hash_workers
            ),
        });
    }
    Ok(())
}

/// Derives the per-worker read buffer from the global in-flight byte budget.
fn hash_chunk_bytes(options: &UploadOptions) -> usize {
    HASH_CHUNK_BYTES.min((options.in_flight_bytes / options.hash_workers).max(1))
}

/// Chooses a conservative default worker count from host parallelism.
fn default_hash_workers() -> usize {
    default_hash_workers_for(std::thread::available_parallelism().map_or(1, usize::from))
}

/// Clamps available CPU count into the supported default worker range.
fn default_hash_workers_for(available: usize) -> usize {
    available.clamp(MIN_DEFAULT_HASH_WORKERS, MAX_DEFAULT_HASH_WORKERS)
}

/// Counts path components for deepest-first directory sorting.
fn path_depth(path: &Path) -> usize {
    path.components().count()
}

#[cfg(test)]
/// Adapts upload scan errors to the legacy test helper's tree-error result.
fn upload_tree_error(error: UploadError) -> TreeError {
    match error {
        UploadError::Tree(error) => error,
        UploadError::Filesystem { path, source } => TreeError::Filesystem { path, source },
        other => TreeError::Context {
            operation: "scan local tree for legacy caller".to_string(),
            source: Box::new(TreeError::InvalidName {
                name: "upload".to_string(),
                reason: other.to_string(),
            }),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::cas::UploadStats;
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::collections::HashSet;
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use tempfile::tempdir;

    #[derive(Default)]
    struct FakeStore {
        missing: HashSet<Digest>,
        uploaded: Vec<Blob>,
    }

    #[async_trait]
    impl BlobStore for FakeStore {
        async fn find_missing_blobs(
            &mut self,
            digests: &[Digest],
        ) -> Result<Vec<Digest>, CasError> {
            Ok(digests
                .iter()
                .filter(|digest| self.missing.contains(*digest))
                .cloned()
                .collect())
        }

        async fn upload_blobs(&mut self, blobs: Vec<Blob>) -> Result<UploadStats, CasError> {
            let stats = UploadStats {
                uploaded_blobs: blobs.len(),
                bytes_uploaded: blobs
                    .iter()
                    .map(|blob| blob.digest.size_bytes() as u64)
                    .sum(),
            };
            self.uploaded.extend(blobs);
            Ok(stats)
        }

        async fn download_blob(&mut self, _digest: &Digest) -> Result<Bytes, CasError> {
            unreachable!("upload tests do not download")
        }
    }

    fn rendered_upload_chain(error: UploadError) -> String {
        anyhow::Error::new(error)
            .chain()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn default_worker_counts_respect_low_and_high_cpu_counts() {
        assert_eq!(default_hash_workers_for(1), 2);
        assert_eq!(default_hash_workers_for(4), 4);
        assert_eq!(default_hash_workers_for(64), 8);
    }

    #[test]
    fn options_require_enough_in_flight_bytes_for_workers() {
        let error = validate_options(&UploadOptions {
            hash_workers: 4,
            in_flight_bytes: 3,
            fail_on_unsupported_nodes: true,
        })
        .unwrap_err();

        assert!(error.to_string().contains("must be at least hash_workers"));
    }

    #[test]
    fn scanner_records_file_directory_symlink_and_hard_link() {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("file.txt"), b"hello").unwrap();
        fs::create_dir(temp.path().join("empty")).unwrap();
        std::os::unix::fs::symlink("../target", temp.path().join("link")).unwrap();
        std::fs::hard_link(temp.path().join("file.txt"), temp.path().join("again.txt")).unwrap();

        let tree = scan_local_directory(temp.path()).unwrap();

        assert_eq!(tree.files.len(), 2);
        assert_eq!(tree.directories.len(), 2);
        assert_eq!(tree.symlinks, 1);
        assert_eq!(tree.warnings.hard_links, 1);
    }

    #[test]
    fn scanner_does_not_follow_symlinked_directories() {
        let temp = tempdir().unwrap();
        fs::create_dir(temp.path().join("real")).unwrap();
        fs::write(temp.path().join("real").join("child.txt"), b"child").unwrap();
        std::os::unix::fs::symlink("real", temp.path().join("link-dir")).unwrap();

        let tree = scan_local_directory(temp.path()).unwrap();

        assert_eq!(tree.directories.len(), 2);
        assert_eq!(tree.symlinks, 1);
    }

    #[test]
    fn scanner_error_includes_root_scan_and_child_context() {
        let temp = tempdir().unwrap();
        std::os::unix::fs::symlink(OsStr::from_bytes(b"\xff"), temp.path().join("bad-link"))
            .unwrap();

        let error =
            scan_local_directory_with_options(temp.path(), &UploadOptions::default()).unwrap_err();
        let rendered = rendered_upload_chain(error);
        assert!(rendered.contains("Symlink target is not UTF-8"));
    }

    #[tokio::test]
    async fn hashing_fixture_file_returns_digest_and_size() {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("file.txt"), b"hello").unwrap();
        let tree = scan_local_directory(temp.path()).unwrap();

        let digests = hash_files(&tree.files, &UploadOptions::default())
            .await
            .unwrap();

        assert_eq!(digests.len(), 1);
        assert_eq!(digests[0].size_bytes, 5);
        assert_eq!(digests[0].digest, Digest::for_bytes(b"hello"));
    }

    #[test]
    fn hashing_rejects_file_size_change_after_scan() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("file.txt");
        fs::write(&path, b"hello").unwrap();
        let tree = scan_local_directory(temp.path()).unwrap();
        fs::write(&path, b"hello world").unwrap();

        let error = hash_file(&tree.files[0], HASH_CHUNK_BYTES).unwrap_err();

        assert!(error.to_string().contains("changed size during upload"));
        assert!(error.to_string().contains("file.txt"));
    }

    #[tokio::test]
    async fn upload_local_directory_uploads_only_missing_unique_objects() {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("file.txt"), b"hello").unwrap();
        let tree = scan_local_tree(temp.path()).unwrap();
        let missing = tree
            .file_blobs
            .iter()
            .map(|(digest, _)| digest.clone())
            .chain(
                tree.directories
                    .iter()
                    .map(|directory| directory.digest.clone()),
            )
            .collect::<HashSet<_>>();
        let mut store = FakeStore {
            missing,
            uploaded: Vec::new(),
        };

        let summary = upload_local_directory(&mut store, temp.path(), UploadOptions::default())
            .await
            .unwrap();

        assert_eq!(summary.files, 1);
        assert_eq!(summary.directories, 1);
        assert_eq!(summary.uploaded_blobs, 2);
        assert_eq!(summary.reused_blobs, 0);
        assert_eq!(store.uploaded.len(), 2);
    }
}
