use std::fs;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use thiserror::Error;

use crate::cas::{Blob, BlobStore, CasError};
use crate::digest::Digest;
use crate::error_context::ResultContext as _;
use crate::tree::{
    DirectoryBuilder, DirectoryEntry, EncodedDirectory, EncodedDirectoryTree, FileEntry, NodeKind,
    NodeMetadata, SymlinkEntry, TreeError, TreeWarnings,
};

/// Uploadable object accepted by the uploader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadInput {
    // The item (File, Directory of Symlink) node being uploaded
    // is available in memory.
    Directory { digest: Digest, data: Bytes },
    File { digest: Digest, path: PathBuf },
}

/// Counts of objects uploaded by kind.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UploadCounts {
    pub file_blobs: usize,
    pub directory_nodes: usize,
}

#[derive(Error, Debug)]
pub enum UploadError {
    #[error(transparent)]
    Tree(#[from] TreeError),
    #[error(transparent)]
    Cas(#[from] CasError),
    #[error("Filesystem error at {path}: {source}")]
    Filesystem {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{operation}: {source}")]
    Context {
        operation: String,
        #[source]
        source: Box<UploadError>,
    },
}

impl crate::error_context::ResultContextError for UploadError {
    fn with_context(self, operation: String) -> Self {
        UploadError::Context {
            operation,
            source: Box::new(self),
        }
    }
}

/// Scans a local directory into a canonical encoded tree.
///
/// The scanner does not follow symlinks. Regular files are hashed locally and
/// returned as file upload inputs; directory objects are encoded bottom-up and
/// returned with the root digest.
pub fn scan_local_tree(root: impl AsRef<Path>) -> Result<EncodedDirectoryTree, TreeError> {
    let root = root.as_ref();
    let mut state = ScanState::default();
    let root_directory = scan_directory(root, &mut state)
        .with_context(|| format!("scan root directory {}", root.display()))?;
    state.warnings.merge(&root_directory.warnings);
    let root_digest = root_directory.digest.clone();
    state.directories.push(root_directory);

    Ok(EncodedDirectoryTree {
        root_digest,
        directories: state.directories,
        file_blobs: state.file_blobs,
        warnings: state.warnings,
    })
}

/// Uploads missing file blobs and directory nodes through a generic blob store.
///
/// The helper performs one existence check across all inputs and uploads only
/// missing blobs. Large-file streaming and worker scheduling remain CAS client
/// concerns for this MVP slice.
pub async fn upload_missing<S: BlobStore + Send>(
    store: &mut S,
    inputs: Vec<UploadInput>,
) -> Result<UploadCounts, UploadError> {
    let digests = inputs.iter().map(|i| i.into()).cloned().collect::<Vec<_>>();
    let missing = store
        .find_missing_blobs(&digests)
        .await
        .map_err(UploadError::from)
        .with_context(|| {
            format!(
                "check which {} upload input digest(s) are missing from CAS",
                digests.len()
            )
        })?;
    let mut counts = UploadCounts::default();
    let mut blobs = Vec::new();

    for input in inputs {
        let digest = (&input).into();
        if !missing.contains(digest) {
            continue;
        }
        match input {
            UploadInput::Directory { digest, data } => {
                counts.directory_nodes += 1;
                blobs.push(Blob {
                    digest,
                    data: data.to_vec(),
                });
            }
            UploadInput::File { digest, path } => {
                counts.file_blobs += 1;
                // Reads the entire file into memory which is going to be a problem for
                // large files. We should stream those into the blob store.
                let data = fs::read(&path).map_err(|source| UploadError::Filesystem {
                    path: path.clone(),
                    source,
                })?;
                blobs.push(Blob { digest, data });
            }
        }
    }

    let upload_count = blobs.len();
    store
        .upload_blobs(blobs)
        .await
        .map_err(UploadError::from)
        .with_context(|| {
            format!(
                "upload {upload_count} missing blob(s) to CAS: {} file blob(s), {} directory node(s)",
                counts.file_blobs, counts.directory_nodes
            )
        })?;
    Ok(counts)
}

/// Converts an encoded tree into upload inputs for the generic uploader.
pub fn upload_inputs_for_tree(tree: &EncodedDirectoryTree) -> Vec<UploadInput> {
    let mut inputs = Vec::new();
    inputs.extend(
        tree.file_blobs
            .iter()
            .map(|(digest, path)| UploadInput::File {
                digest: digest.clone(),
                path: path.clone(),
            }),
    );
    inputs.extend(
        tree.directories
            .iter()
            .map(|directory| UploadInput::Directory {
                digest: directory.digest.clone(),
                data: directory.bytes.clone(),
            }),
    );
    inputs
}

#[derive(Default)]
struct ScanState {
    directories: Vec<EncodedDirectory>,
    file_blobs: Vec<(Digest, PathBuf)>,
    warnings: TreeWarnings,
}

fn scan_directory(path: &Path, state: &mut ScanState) -> Result<EncodedDirectory, TreeError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| TreeError::Filesystem {
        path: path.to_path_buf(),
        source,
    })?;
    let mut builder = DirectoryBuilder::with_metadata(metadata_for(NodeKind::Directory, &metadata));
    let mut entries = fs::read_dir(path)
        .map_err(|source| TreeError::Filesystem {
            path: path.to_path_buf(),
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TreeError::Filesystem {
            path: path.to_path_buf(),
            source,
        })?;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let child_path = entry.path();
        let name = component_name(&child_path).with_context(|| {
            format!("extract child component name for {}", child_path.display())
        })?;
        let metadata =
            fs::symlink_metadata(&child_path).map_err(|source| TreeError::Filesystem {
                path: child_path.clone(),
                source,
            })?;
        let file_type = metadata.file_type();
        if file_type.is_file() {
            // Loads the entire content of a file into memory. This will be an issue for large files.
            let data = fs::read(&child_path).map_err(|source| TreeError::Filesystem {
                path: child_path.clone(),
                source,
            })?;
            let digest = Digest::for_bytes(&data);
            state.file_blobs.push((digest.clone(), child_path.clone()));
            let entry_name = name.clone();
            builder
                .add_file(FileEntry {
                    name,
                    digest,
                    metadata: metadata_for(NodeKind::File, &metadata),
                })
                .with_context(|| {
                    format!("add file entry {entry_name} from {}", child_path.display())
                })?;
        } else if file_type.is_dir() {
            let encoded = scan_directory(&child_path, state)
                .with_context(|| format!("scan child directory {}", child_path.display()))?;
            state.warnings.merge(&encoded.warnings);
            let digest = encoded.digest.clone();
            state.directories.push(encoded);
            let entry_name = name.clone();
            builder
                .add_directory(DirectoryEntry {
                    name,
                    digest,
                    metadata: metadata_for(NodeKind::Directory, &metadata),
                })
                .with_context(|| {
                    format!(
                        "add directory entry {entry_name} from {}",
                        child_path.display()
                    )
                })?;
        } else if file_type.is_symlink() {
            let target = fs::read_link(&child_path).map_err(|source| TreeError::Filesystem {
                path: child_path.clone(),
                source,
            })?;
            let target = target
                .to_str()
                .ok_or_else(|| TreeError::NonUtf8SymlinkTarget {
                    path: child_path.clone(),
                })
                .map(ToOwned::to_owned)
                .with_context(|| {
                    format!("validate symlink target UTF-8 for {}", child_path.display())
                })?;
            let entry_name = name.clone();
            builder
                .add_symlink(SymlinkEntry {
                    name,
                    target,
                    metadata: metadata_for(NodeKind::Symlink, &metadata),
                })
                .with_context(|| {
                    format!(
                        "add symlink entry {entry_name} from {}",
                        child_path.display()
                    )
                })?;
        } else {
            return Err(TreeError::UnsupportedNodeType {
                path: child_path,
                kind: node_type_name(file_type).to_string(),
            });
        }
    }

    builder
        .encode()
        .with_context(|| format!("encode directory {}", path.display()))
}

fn component_name(path: &Path) -> Result<String, TreeError> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(ToOwned::to_owned)
        .ok_or_else(|| TreeError::NonUtf8Name {
            path: path.to_path_buf(),
        })
}

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

impl<'a> From<&'a UploadInput> for &'a Digest {
    fn from(input: &'a UploadInput) -> Self {
        match input {
            UploadInput::Directory { digest, .. } | UploadInput::File { digest, .. } => digest,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use tempfile::tempdir;

    fn rendered_tree_chain(error: TreeError) -> String {
        anyhow::Error::new(error)
            .chain()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn scanner_preserves_file_directory_and_symlink_entries() {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("file.txt"), b"hello").unwrap();
        fs::create_dir(temp.path().join("empty")).unwrap();
        std::os::unix::fs::symlink("../target", temp.path().join("link")).unwrap();

        let tree = scan_local_tree(temp.path()).unwrap();
        let root = tree
            .directories
            .iter()
            .find(|directory| directory.digest == tree.root_digest)
            .unwrap();

        assert_eq!(root.directory.files[0].name, "file.txt");
        assert_eq!(root.directory.directories[0].name, "empty");
        assert_eq!(root.directory.symlinks[0].target, "../target");
        assert_eq!(tree.file_blobs.len(), 1);
        assert_eq!(tree.warnings.escaping_symlinks, 1);
    }

    #[test]
    fn scanner_error_includes_root_scan_and_child_context() {
        let temp = tempdir().unwrap();
        std::os::unix::fs::symlink(OsStr::from_bytes(b"\xff"), temp.path().join("bad-link"))
            .unwrap();

        let error = scan_local_tree(temp.path()).unwrap_err();
        let rendered = rendered_tree_chain(error);
        assert!(rendered.contains(&format!("scan root directory {}", temp.path().display())));
        assert!(rendered.contains("validate symlink target UTF-8"));
        assert!(rendered.contains("Symlink target is not UTF-8"));
    }
}
