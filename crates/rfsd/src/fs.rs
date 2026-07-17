//! Read-only lazy filesystem core.
//!
//! The core materializes one REAPI `Directory` at a time, records stable inode
//! identities in daemon-owned state, and admits fully verified objects to the
//! shared cache before serving them. It deliberately has no FUSE dependency.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use bytes::Bytes;
use prost_types::Timestamp;
use rfs_common::cas::{BlobStore, CasError};
use rfs_common::digest::{Digest, DigestError};
use rfs_common::reapi::build::bazel::remote::execution::v2::{
    Directory, FileNode, NodeProperties, SymlinkNode,
};
use rfs_common::state::{
    FilesystemState, MaterializedInode, ROOT_INODE, RemoteNodeIdentity, RemoteNodeKind, StateError,
};
use rfs_common::tree::{TreeError, decode_directory};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::sync::Mutex;

/// Immutable node kind exposed by the filesystem core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    /// Regular file backed by a remote content digest.
    File,
    /// Directory backed by a remote `Directory` digest.
    Directory,
    /// Symbolic link with an inline target.
    Symlink,
}

/// Metadata and remote identity for one materialized inode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    /// Synthetic session-stable inode.
    pub inode: u64,
    /// Synthetic inode of the parent; root refers to itself.
    pub parent: u64,
    /// UTF-8 name relative to the parent, empty only for root.
    pub name: String,
    /// Immutable remote node kind.
    pub kind: NodeKind,
    /// Content or directory digest; absent only for symlinks.
    pub digest: Option<Digest>,
    /// Preserved supported Unix mode bits when present.
    pub mode: Option<u32>,
    /// Preserved modification time when present.
    pub mtime: Option<Timestamp>,
    /// Exact symlink target; present only for symlinks.
    pub symlink_target: Option<String>,
}

/// Observable lazy-fetch and cache behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadOnlyCounters {
    /// Directory objects downloaded from CAS.
    pub directory_downloads: u64,
    /// Directory objects read from the shared cache.
    pub directory_cache_hits: u64,
    /// File blobs downloaded and admitted to the shared cache.
    pub blob_downloads: u64,
    /// File reads served from an already-admitted cache entry.
    pub blob_cache_hits: u64,
}

#[derive(Default)]
struct AtomicCounters {
    directory_downloads: AtomicU64,
    directory_cache_hits: AtomicU64,
    blob_downloads: AtomicU64,
    blob_cache_hits: AtomicU64,
    cached_blobs: AtomicU64,
}

/// Errors from lazy metadata resolution and verified file reads.
#[derive(Debug, Error)]
pub enum FilesystemError {
    #[error("inode {inode} is not materialized")]
    UnknownInode { inode: u64 },
    #[error("inode {inode} is a {actual:?}, expected {expected:?}")]
    WrongKind {
        inode: u64,
        expected: NodeKind,
        actual: NodeKind,
    },
    #[error("entry `{name}` was not found in directory inode {parent}")]
    NotFound { parent: u64, name: String },
    #[error("remote {kind} entry `{name}` has no digest")]
    MissingDigest { kind: &'static str, name: String },
    #[error("invalid digest for remote entry `{name}`: {source}")]
    InvalidDigest {
        name: String,
        #[source]
        source: DigestError,
    },
    #[error("download {object} {digest} from CAS failed: {source}")]
    Cas {
        object: &'static str,
        digest: Digest,
        #[source]
        source: Box<CasError>,
    },
    #[error("validate remote directory {digest} failed: {source}")]
    Directory {
        digest: Digest,
        #[source]
        source: TreeError,
    },
    #[error("downloaded blob digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch { expected: Digest, actual: Digest },
    #[error("persist filesystem materialization for inode {inode} failed: {source}")]
    State {
        inode: u64,
        #[source]
        source: StateError,
    },
    #[error("filesystem state task for inode {inode} did not complete: {source}")]
    StateTask {
        inode: u64,
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("cache operation `{operation}` on `{path}` failed: {source}")]
    Cache {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// Lazy, read-only view of one immutable remote snapshot.
pub struct ReadOnlyFilesystem<S> {
    store: Mutex<S>,
    state: Arc<dyn FilesystemState>,
    cache_root: PathBuf,
    nodes: Mutex<HashMap<u64, Node>>,
    directories: Mutex<HashMap<u64, Arc<MaterializedDirectory>>>,
    directory_locks: StdMutex<HashMap<Digest, Arc<Mutex<()>>>>,
    blob_locks: StdMutex<HashMap<Digest, Arc<Mutex<()>>>>,
    counters: AtomicCounters,
}

struct MaterializedDirectory {
    entries: Vec<Node>,
    by_name: HashMap<String, Node>,
}

impl<S: BlobStore + Send> ReadOnlyFilesystem<S> {
    /// Validates the root directory and returns a lazy filesystem core.
    ///
    /// Only the root `Directory` is fetched. Descendant directory objects and
    /// file blobs remain unresolved until their own lookup/readdir/read.
    pub async fn mount(
        store: S,
        state: Arc<dyn FilesystemState>,
        cache_root: PathBuf,
        root_digest: Digest,
    ) -> Result<Self, FilesystemError> {
        let root = Node {
            inode: ROOT_INODE,
            parent: ROOT_INODE,
            name: String::new(),
            kind: NodeKind::Directory,
            digest: Some(root_digest),
            mode: None,
            mtime: None,
            symlink_target: None,
        };
        let cached_blobs = count_cache_entries(&cache_root.join("blobs"))?;
        let filesystem = Self {
            store: Mutex::new(store),
            state,
            cache_root,
            nodes: Mutex::new(HashMap::from([(ROOT_INODE, root)])),
            directories: Mutex::new(HashMap::new()),
            directory_locks: StdMutex::new(HashMap::new()),
            blob_locks: StdMutex::new(HashMap::new()),
            counters: AtomicCounters {
                cached_blobs: AtomicU64::new(cached_blobs),
                ..AtomicCounters::default()
            },
        };
        filesystem.ensure_directory(ROOT_INODE).await?;
        Ok(filesystem)
    }

    /// Looks up one direct child without fetching child directory metadata.
    pub async fn lookup(&self, parent: u64, name: &str) -> Result<Node, FilesystemError> {
        let directory = self.ensure_directory(parent).await?;
        directory
            .by_name
            .get(name)
            .cloned()
            .ok_or_else(|| FilesystemError::NotFound {
                parent,
                name: name.to_owned(),
            })
    }

    /// Lists one directory in canonical remote order.
    pub async fn readdir(&self, inode: u64) -> Result<Vec<Node>, FilesystemError> {
        Ok(self.ensure_directory(inode).await?.entries.clone())
    }

    /// Returns metadata and remote identity for one materialized inode.
    pub async fn getattr(&self, inode: u64) -> Result<Node, FilesystemError> {
        self.node(inode).await
    }

    /// Returns the exact stored target for a materialized symlink.
    pub async fn readlink(&self, inode: u64) -> Result<String, FilesystemError> {
        let node = self.node(inode).await?;
        require_kind(&node, NodeKind::Symlink)?;
        Ok(node
            .symlink_target
            .expect("materialized symlinks always contain a target"))
    }

    /// Reads a byte range after ensuring the complete verified blob is cached.
    pub async fn read(
        &self,
        inode: u64,
        offset: u64,
        size: usize,
    ) -> Result<Bytes, FilesystemError> {
        let node = self.node(inode).await?;
        require_kind(&node, NodeKind::File)?;
        let digest = node
            .digest
            .expect("materialized files always contain a digest");
        let path = self.ensure_blob(&digest).await?;
        read_range(&path, offset, size)
    }

    /// Returns a consistent snapshot of fetch/cache counters.
    pub fn counters(&self) -> ReadOnlyCounters {
        ReadOnlyCounters {
            directory_downloads: self.counters.directory_downloads.load(Ordering::Relaxed),
            directory_cache_hits: self.counters.directory_cache_hits.load(Ordering::Relaxed),
            blob_downloads: self.counters.blob_downloads.load(Ordering::Relaxed),
            blob_cache_hits: self.counters.blob_cache_hits.load(Ordering::Relaxed),
        }
    }

    /// Returns the number of verified file blobs present in the shared cache.
    pub fn cached_blobs(&self) -> u64 {
        self.counters.cached_blobs.load(Ordering::Relaxed)
    }

    async fn node(&self, inode: u64) -> Result<Node, FilesystemError> {
        self.nodes
            .lock()
            .await
            .get(&inode)
            .cloned()
            .ok_or(FilesystemError::UnknownInode { inode })
    }

    async fn ensure_directory(
        &self,
        inode: u64,
    ) -> Result<Arc<MaterializedDirectory>, FilesystemError> {
        if let Some(directory) = self.directories.lock().await.get(&inode).cloned() {
            return Ok(directory);
        }
        let node = self.node(inode).await?;
        require_kind(&node, NodeKind::Directory)?;
        let digest = node
            .digest
            .expect("materialized directories always contain a digest");
        let digest_lock = keyed_lock(&self.directory_locks, &digest);
        let _guard = digest_lock.lock().await;
        if let Some(directory) = self.directories.lock().await.get(&inode).cloned() {
            return Ok(directory);
        }

        let directory = self.fetch_directory(&digest).await?;
        let nodes = materialize_nodes(inode, &directory, self.state.clone(), &digest).await?;
        let materialized = Arc::new(MaterializedDirectory {
            by_name: nodes
                .iter()
                .map(|node| (node.name.clone(), node.clone()))
                .collect(),
            entries: nodes.clone(),
        });
        self.nodes
            .lock()
            .await
            .extend(nodes.into_iter().map(|node| (node.inode, node)));
        self.directories
            .lock()
            .await
            .insert(inode, materialized.clone());
        Ok(materialized)
    }

    async fn fetch_directory(&self, digest: &Digest) -> Result<Directory, FilesystemError> {
        let path = cache_path(&self.cache_root.join("dirs"), digest);
        let bytes = if path.is_file() {
            self.counters
                .directory_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            fs::read(&path).map_err(|source| cache_error("read directory", &path, source))?
        } else {
            let bytes = self
                .store
                .lock()
                .await
                .download_blob(digest)
                .await
                .map_err(|source| FilesystemError::Cas {
                    object: "directory",
                    digest: digest.clone(),
                    source: Box::new(source),
                })?;
            let directory = decode_directory(digest.clone(), bytes.clone()).map_err(|source| {
                FilesystemError::Directory {
                    digest: digest.clone(),
                    source,
                }
            })?;
            admit_bytes(&path, &bytes)?;
            self.counters
                .directory_downloads
                .fetch_add(1, Ordering::Relaxed);
            return Ok(directory);
        };
        decode_directory(digest.clone(), Bytes::from(bytes)).map_err(|source| {
            FilesystemError::Directory {
                digest: digest.clone(),
                source,
            }
        })
    }

    async fn ensure_blob(&self, digest: &Digest) -> Result<PathBuf, FilesystemError> {
        let path = cache_path(&self.cache_root.join("blobs"), digest);
        if path.is_file() {
            self.counters
                .blob_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(path);
        }
        let digest_lock = keyed_lock(&self.blob_locks, digest);
        let _guard = digest_lock.lock().await;
        if path.is_file() {
            self.counters
                .blob_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(path);
        }
        let bytes = self
            .store
            .lock()
            .await
            .download_blob(digest)
            .await
            .map_err(|source| FilesystemError::Cas {
                object: "file blob",
                digest: digest.clone(),
                source: Box::new(source),
            })?;
        let actual = Digest::for_bytes(&bytes);
        if actual != *digest {
            return Err(FilesystemError::DigestMismatch {
                expected: digest.clone(),
                actual,
            });
        }
        admit_bytes(&path, &bytes)?;
        self.counters.blob_downloads.fetch_add(1, Ordering::Relaxed);
        self.counters.cached_blobs.fetch_add(1, Ordering::Relaxed);
        Ok(path)
    }
}

async fn materialize_nodes(
    parent: u64,
    directory: &Directory,
    state: Arc<dyn FilesystemState>,
    digest: &Digest,
) -> Result<Vec<Node>, FilesystemError> {
    let mut pending = Vec::with_capacity(
        directory.files.len() + directory.directories.len() + directory.symlinks.len(),
    );
    for file in &directory.files {
        let file_digest = required_digest("file", &file.name, file.digest.as_ref())?;
        pending.push((
            RemoteNodeIdentity {
                name: file.name.clone(),
                kind: RemoteNodeKind::File,
                content_identity: file_digest.to_string(),
            },
            file_node(file, file_digest),
        ));
    }
    for child in &directory.directories {
        let child_digest = required_digest("directory", &child.name, child.digest.as_ref())?;
        pending.push((
            RemoteNodeIdentity {
                name: child.name.clone(),
                kind: RemoteNodeKind::Directory,
                content_identity: child_digest.to_string(),
            },
            Node {
                inode: 0,
                parent,
                name: child.name.clone(),
                kind: NodeKind::Directory,
                digest: Some(child_digest),
                mode: None,
                mtime: None,
                symlink_target: None,
            },
        ));
    }
    for symlink in &directory.symlinks {
        pending.push((
            RemoteNodeIdentity {
                name: symlink.name.clone(),
                kind: RemoteNodeKind::Symlink,
                content_identity: symlink.target.clone(),
            },
            symlink_node(symlink),
        ));
    }
    let identities: Vec<_> = pending
        .iter()
        .map(|(identity, _)| identity.clone())
        .collect();
    let digest = digest.clone();
    let allocated = tokio::task::spawn_blocking(move || {
        state.materialize_remote_directory(parent, &digest, &identities)
    })
    .await
    .map_err(|source| FilesystemError::StateTask {
        inode: parent,
        source,
    })?
    .map_err(|source| FilesystemError::State {
        inode: parent,
        source,
    })?;
    assign_inodes(pending, allocated, parent)
}

fn assign_inodes(
    pending: Vec<(RemoteNodeIdentity, Node)>,
    allocated: Vec<MaterializedInode>,
    parent: u64,
) -> Result<Vec<Node>, FilesystemError> {
    let by_name: HashMap<_, _> = allocated
        .into_iter()
        .map(|value| (value.name, value.inode))
        .collect();
    pending
        .into_iter()
        .map(|(identity, mut node)| {
            node.inode =
                by_name
                    .get(&identity.name)
                    .copied()
                    .ok_or_else(|| FilesystemError::State {
                        inode: parent,
                        source: StateError::StaleSession {
                            path: PathBuf::from("<filesystem-state>"),
                            reason: format!(
                                "inode allocator omitted remote child `{}`",
                                identity.name
                            ),
                        },
                    })?;
            node.parent = parent;
            Ok(node)
        })
        .collect()
}

fn file_node(file: &FileNode, digest: Digest) -> Node {
    let (mode, mtime) = properties(file.node_properties.as_ref());
    Node {
        inode: 0,
        parent: 0,
        name: file.name.clone(),
        kind: NodeKind::File,
        digest: Some(digest),
        mode,
        mtime,
        symlink_target: None,
    }
}

fn symlink_node(symlink: &SymlinkNode) -> Node {
    let (mode, mtime) = properties(symlink.node_properties.as_ref());
    Node {
        inode: 0,
        parent: 0,
        name: symlink.name.clone(),
        kind: NodeKind::Symlink,
        digest: None,
        mode,
        mtime,
        symlink_target: Some(symlink.target.clone()),
    }
}

fn properties(properties: Option<&NodeProperties>) -> (Option<u32>, Option<Timestamp>) {
    properties
        .map(|value| (value.unix_mode, value.mtime.clone()))
        .unwrap_or((None, None))
}

fn required_digest(
    kind: &'static str,
    name: &str,
    digest: Option<&rfs_common::reapi::build::bazel::remote::execution::v2::Digest>,
) -> Result<Digest, FilesystemError> {
    let digest = digest.ok_or_else(|| FilesystemError::MissingDigest {
        kind,
        name: name.to_owned(),
    })?;
    Digest::from_reapi(digest).map_err(|source| FilesystemError::InvalidDigest {
        name: name.to_owned(),
        source,
    })
}

fn require_kind(node: &Node, expected: NodeKind) -> Result<(), FilesystemError> {
    if node.kind == expected {
        Ok(())
    } else {
        Err(FilesystemError::WrongKind {
            inode: node.inode,
            expected,
            actual: node.kind,
        })
    }
}

fn keyed_lock(
    locks: &StdMutex<HashMap<Digest, Arc<Mutex<()>>>>,
    digest: &Digest,
) -> Arc<Mutex<()>> {
    locks
        .lock()
        .expect("digest lock map is not exposed to panicking callers")
        .entry(digest.clone())
        .or_default()
        .clone()
}

fn cache_path(base: &Path, digest: &Digest) -> PathBuf {
    base.join(&digest.hash()[..2])
        .join(format!("{}-{}", digest.hash(), digest.size_bytes()))
}

fn count_cache_entries(path: &Path) -> Result<u64, FilesystemError> {
    if !path.exists() {
        return Ok(0);
    }
    let shards =
        fs::read_dir(path).map_err(|source| cache_error("list blob cache", path, source))?;
    let mut count = 0_u64;
    for shard in shards {
        let shard = shard.map_err(|source| cache_error("read blob cache entry", path, source))?;
        let shard_path = shard.path();
        if !shard_path.is_dir() {
            continue;
        }
        let entries = fs::read_dir(&shard_path)
            .map_err(|source| cache_error("list blob cache shard", &shard_path, source))?;
        for entry in entries {
            let entry = entry.map_err(|source| {
                cache_error("read blob cache shard entry", &shard_path, source)
            })?;
            if entry
                .file_type()
                .map_err(|source| cache_error("stat blob cache entry", &entry.path(), source))?
                .is_file()
            {
                count = count.saturating_add(1);
            }
        }
    }
    Ok(count)
}

fn admit_bytes(path: &Path, bytes: &[u8]) -> Result<(), FilesystemError> {
    let parent = path
        .parent()
        .expect("cache paths always have a shard parent");
    fs::create_dir_all(parent).map_err(|source| cache_error("create shard", parent, source))?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
        .map_err(|source| cache_error("secure shard", parent, source))?;
    let mut temporary = NamedTempFile::new_in(parent)
        .map_err(|source| cache_error("create temporary", parent, source))?;
    temporary
        .write_all(bytes)
        .map_err(|source| cache_error("write temporary", temporary.path(), source))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|source| cache_error("sync temporary", temporary.path(), source))?;
    match temporary.persist_noclobber(path) {
        Ok(_) => Ok(()),
        Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(cache_error("rename verified object", path, error.error)),
    }
}

fn read_range(path: &Path, offset: u64, size: usize) -> Result<Bytes, FilesystemError> {
    let file = File::open(path).map_err(|source| cache_error("open blob", path, source))?;
    let file_size = file
        .metadata()
        .map_err(|source| cache_error("stat blob", path, source))?
        .len();
    if offset >= file_size || size == 0 {
        return Ok(Bytes::new());
    }
    let remaining = file_size - offset;
    let length = usize::try_from(remaining).unwrap_or(usize::MAX).min(size);
    let mut bytes = vec![0; length];
    let read = file
        .read_at(&mut bytes, offset)
        .map_err(|source| cache_error("read blob", path, source))?;
    bytes.truncate(read);
    Ok(Bytes::from(bytes))
}

fn cache_error(operation: &'static str, path: &Path, source: io::Error) -> FilesystemError {
    FilesystemError::Cache {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use rfs_common::cas::{Blob, CasOperation, UploadStats};
    use rfs_common::state::MaterializedInode;
    use rfs_common::tree::{
        DirectoryBuilder, DirectoryEntry, FileEntry, NodeKind as TreeNodeKind, NodeMetadata,
        SymlinkEntry,
    };
    use tempfile::TempDir;

    use super::*;

    #[derive(Default)]
    struct FakeState {
        inodes: StdMutex<HashMap<(u64, String), (u64, RemoteNodeIdentity)>>,
    }

    impl FilesystemState for FakeState {
        fn materialize_remote_directory(
            &self,
            inode: u64,
            _digest: &Digest,
            children: &[RemoteNodeIdentity],
        ) -> Result<Vec<MaterializedInode>, StateError> {
            let mut inodes = self.inodes.lock().unwrap();
            let mut result = Vec::new();
            for child in children {
                let key = (inode, child.name.clone());
                let next = u64::try_from(inodes.len()).unwrap() + 2;
                let (child_inode, stored) =
                    inodes.entry(key).or_insert_with(|| (next, child.clone()));
                assert_eq!(stored, child);
                result.push(MaterializedInode {
                    inode: *child_inode,
                    name: child.name.clone(),
                });
            }
            Ok(result)
        }
    }

    struct FakeStore {
        blobs: HashMap<Digest, Bytes>,
        downloads: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BlobStore for FakeStore {
        async fn find_missing_blobs(
            &mut self,
            _digests: &[Digest],
        ) -> Result<Vec<Digest>, CasError> {
            unreachable!("read-only core does not check upload existence")
        }

        async fn upload_blobs(&mut self, _blobs: Vec<Blob>) -> Result<UploadStats, CasError> {
            unreachable!("read-only core does not upload")
        }

        async fn download_blob(&mut self, digest: &Digest) -> Result<Bytes, CasError> {
            self.downloads.fetch_add(1, Ordering::SeqCst);
            self.blobs
                .get(digest)
                .cloned()
                .ok_or_else(|| CasError::BlobStatus {
                    operation: CasOperation::BatchReadBlobs,
                    digest: digest.clone(),
                    message: "not found".into(),
                })
        }
    }

    struct Fixture {
        root: Digest,
        child: Digest,
        file: Digest,
        blobs: HashMap<Digest, Bytes>,
    }

    fn fixture() -> Fixture {
        let file_bytes = Bytes::from_static(b"hello from remote");
        let file = Digest::for_bytes(&file_bytes);
        let child = DirectoryBuilder::new().encode().unwrap();
        let mut root = DirectoryBuilder::new();
        root.add_file(FileEntry {
            name: "hello.txt".into(),
            digest: file.clone(),
            metadata: NodeMetadata::new(TreeNodeKind::File, Some(0o644), None),
        })
        .unwrap();
        root.add_directory(DirectoryEntry {
            name: "nested".into(),
            digest: child.digest.clone(),
            metadata: NodeMetadata::new(TreeNodeKind::Directory, Some(0o755), None),
        })
        .unwrap();
        root.add_symlink(SymlinkEntry {
            name: "hello-link".into(),
            target: "hello.txt".into(),
            metadata: NodeMetadata::new(TreeNodeKind::Symlink, Some(0o777), None),
        })
        .unwrap();
        let root = root.encode().unwrap();
        Fixture {
            root: root.digest.clone(),
            child: child.digest.clone(),
            file: file.clone(),
            blobs: HashMap::from([
                (root.digest, root.bytes),
                (child.digest, child.bytes),
                (file, file_bytes),
            ]),
        }
    }

    async fn mount_fixture(
        fixture: &Fixture,
        temp: &TempDir,
        downloads: Arc<AtomicUsize>,
    ) -> ReadOnlyFilesystem<FakeStore> {
        ReadOnlyFilesystem::mount(
            FakeStore {
                blobs: fixture.blobs.clone(),
                downloads,
            },
            Arc::new(FakeState::default()),
            temp.path().join("cache"),
            fixture.root.clone(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn mount_lookup_and_readdir_fetch_only_needed_directories() {
        let fixture = fixture();
        let temp = tempfile::tempdir().unwrap();
        let downloads = Arc::new(AtomicUsize::new(0));
        let filesystem = mount_fixture(&fixture, &temp, downloads.clone()).await;
        assert_eq!(downloads.load(Ordering::SeqCst), 1);

        let nested = filesystem.lookup(ROOT_INODE, "nested").await.unwrap();
        assert_eq!(nested.kind, NodeKind::Directory);
        assert_eq!(downloads.load(Ordering::SeqCst), 1);
        assert_eq!(
            filesystem
                .readdir(ROOT_INODE)
                .await
                .unwrap()
                .iter()
                .map(|node| node.name.as_str())
                .collect::<HashSet<_>>(),
            HashSet::from(["hello.txt", "nested", "hello-link"])
        );
        assert_eq!(downloads.load(Ordering::SeqCst), 1);

        assert!(filesystem.readdir(nested.inode).await.unwrap().is_empty());
        assert_eq!(downloads.load(Ordering::SeqCst), 2);
        assert_eq!(filesystem.counters().directory_downloads, 2);
    }

    #[tokio::test]
    async fn readlink_returns_exact_target_without_blob_fetch() {
        let fixture = fixture();
        let temp = tempfile::tempdir().unwrap();
        let downloads = Arc::new(AtomicUsize::new(0));
        let filesystem = mount_fixture(&fixture, &temp, downloads.clone()).await;
        let link = filesystem.lookup(ROOT_INODE, "hello-link").await.unwrap();

        assert_eq!(filesystem.readlink(link.inode).await.unwrap(), "hello.txt");
        assert_eq!(downloads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn first_read_verifies_and_atomically_admits_then_cache_is_trusted() {
        let fixture = fixture();
        let temp = tempfile::tempdir().unwrap();
        let downloads = Arc::new(AtomicUsize::new(0));
        let filesystem = mount_fixture(&fixture, &temp, downloads.clone()).await;
        let file = filesystem.lookup(ROOT_INODE, "hello.txt").await.unwrap();

        assert_eq!(
            filesystem.read(file.inode, 6, 4).await.unwrap(),
            Bytes::from_static(b"from")
        );
        assert_eq!(downloads.load(Ordering::SeqCst), 2);
        let path = cache_path(&temp.path().join("cache/blobs"), &fixture.file);
        let entries = fs::read_dir(path.parent().unwrap()).unwrap().count();
        assert_eq!(entries, 1, "temporary admission file must be renamed away");

        fs::write(&path, b"trusted cache data").unwrap();
        assert_eq!(
            filesystem.read(file.inode, 0, 7).await.unwrap(),
            Bytes::from_static(b"trusted")
        );
        assert_eq!(downloads.load(Ordering::SeqCst), 2);
        assert_eq!(filesystem.counters().blob_cache_hits, 1);
    }

    #[tokio::test]
    async fn duplicate_missing_blob_reads_coalesce() {
        let fixture = fixture();
        let temp = tempfile::tempdir().unwrap();
        let downloads = Arc::new(AtomicUsize::new(0));
        let filesystem = mount_fixture(&fixture, &temp, downloads.clone()).await;
        let file = filesystem.lookup(ROOT_INODE, "hello.txt").await.unwrap();

        let (left, right) = tokio::join!(
            filesystem.read(file.inode, 0, 5),
            filesystem.read(file.inode, 6, 4)
        );
        assert_eq!(left.unwrap(), Bytes::from_static(b"hello"));
        assert_eq!(right.unwrap(), Bytes::from_static(b"from"));
        assert_eq!(downloads.load(Ordering::SeqCst), 2);
        assert_eq!(filesystem.counters().blob_downloads, 1);
    }

    #[tokio::test]
    async fn missing_blob_has_operation_and_digest_context() {
        let mut fixture = fixture();
        fixture.blobs.remove(&fixture.file);
        let temp = tempfile::tempdir().unwrap();
        let filesystem = mount_fixture(&fixture, &temp, Arc::new(AtomicUsize::new(0))).await;
        let file = filesystem.lookup(ROOT_INODE, "hello.txt").await.unwrap();

        let error = filesystem.read(file.inode, 0, 1).await.unwrap_err();
        let rendered = error.to_string();
        assert!(rendered.contains("download file blob"));
        assert!(rendered.contains(&fixture.file.to_string()));
        assert!(rendered.contains("not found"));
    }

    #[tokio::test]
    async fn digest_mismatch_is_rejected_before_cache_admission() {
        let mut fixture = fixture();
        fixture
            .blobs
            .insert(fixture.file.clone(), Bytes::from_static(b"wrong"));
        let temp = tempfile::tempdir().unwrap();
        let filesystem = mount_fixture(&fixture, &temp, Arc::new(AtomicUsize::new(0))).await;
        let file = filesystem.lookup(ROOT_INODE, "hello.txt").await.unwrap();

        assert!(matches!(
            filesystem.read(file.inode, 0, 1).await,
            Err(FilesystemError::DigestMismatch { .. })
        ));
        assert!(!cache_path(&temp.path().join("cache/blobs"), &fixture.file).exists());
    }

    #[tokio::test]
    async fn missing_descendant_directory_fails_only_when_accessed() {
        let mut fixture = fixture();
        fixture.blobs.remove(&fixture.child);
        let temp = tempfile::tempdir().unwrap();
        let filesystem = mount_fixture(&fixture, &temp, Arc::new(AtomicUsize::new(0))).await;

        let nested = filesystem.lookup(ROOT_INODE, "nested").await.unwrap();
        assert!(matches!(
            filesystem.readdir(nested.inode).await,
            Err(FilesystemError::Cas {
                object: "directory",
                ..
            })
        ));
    }
}
