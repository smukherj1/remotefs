use std::collections::HashSet;
use std::path::PathBuf;

use bytes::Bytes;
use prost::Message;
use prost_types::Timestamp;
use thiserror::Error;

use crate::digest::{Digest, DigestError};
use crate::error_context::ResultContext as _;
use crate::reapi::remote_execution::{
    Directory, DirectoryNode, FileNode, NodeProperties, SymlinkNode,
};

const SETUID_BIT: u32 = 0o4000;
const SETGID_BIT: u32 = 0o2000;
const STICKY_BIT: u32 = 0o1000;
const PERMISSION_BITS: u32 = 0o777;
const FILE_MODE_MASK: u32 = PERMISSION_BITS;
const DIRECTORY_MODE_MASK: u32 = PERMISSION_BITS | STICKY_BIT;
const SYMLINK_MODE_MASK: u32 = PERMISSION_BITS;
const MIN_TIMESTAMP_SECONDS: i64 = -62_135_596_800;
const MAX_TIMESTAMP_SECONDS: i64 = 253_402_300_799;

/// Filesystem node kind used when normalizing metadata for REAPI nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    File,
    Directory,
    Symlink,
}

/// NodeMetadata represents the properties of a node that corresponds to
/// NodeProperties in the RE API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeMetadata {
    pub mode: Option<u32>,
    pub mtime: Option<Timestamp>,
    pub kind: NodeKind,
}

impl NodeMetadata {
    /// Creates metadata for a node.
    ///
    /// The raw mode is normalized during encoding: unsupported setuid and
    /// setgid bits are masked and permissions are reduced to the supported
    /// mode bits for the node kind. Invalid protobuf timestamps return
    /// `TreeError::UnsupportedTimestamp`.
    pub fn new(kind: NodeKind, mode: Option<u32>, mtime: Option<Timestamp>) -> Self {
        Self { mode, mtime, kind }
    }

    fn normalize(&self, path: PathBuf) -> Result<(NodeMetadata, TreeWarnings), TreeError> {
        let mut warnings = TreeWarnings::default();
        let mode = self.mode.map(|mode| {
            if mode & SETUID_BIT != 0 {
                warnings.add(TreeWarning::MaskedSetuid { path: path.clone() });
            }
            if mode & SETGID_BIT != 0 {
                warnings.add(TreeWarning::MaskedSetgid { path: path.clone() });
            }
            let mask = match self.kind {
                NodeKind::File => FILE_MODE_MASK,
                NodeKind::Directory => DIRECTORY_MODE_MASK,
                NodeKind::Symlink => SYMLINK_MODE_MASK,
            };
            mode & mask
        });

        if let Some(timestamp) = &self.mtime {
            validate_timestamp(path.clone(), timestamp)
                .with_context(|| format!("validate mtime for {}", path.display()))?;
        }

        Ok((
            NodeMetadata {
                mode,
                mtime: self.mtime.clone(),
                kind: self.kind,
            },
            warnings,
        ))
    }

    fn into_node_properties(self) -> Option<NodeProperties> {
        if self.mode.is_none() && self.mtime.is_none() {
            return None;
        }
        Some(NodeProperties {
            properties: Vec::new(),
            mtime: self.mtime.clone(),
            unix_mode: self.mode,
        })
    }
}

/// Represents a file entry in a directory. Points to the blob
/// (by digest) for the file's contents. Corresponds to a FileNode
/// in the RE API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    // Name of the file relative to the directory containing the file.
    pub name: String,
    // The digest of the file's contents.
    pub digest: Digest,
    // File metadata.
    pub metadata: NodeMetadata,
}

/// Represents a child directory in a directory. Points to the blob for this
/// directory's RE API Directory proto. Corresponds to a DirectoryNode in
/// the RE API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryEntry {
    // Name of the directory relative to the directory containing it. Empty
    // if this is root.
    pub name: String,
    pub digest: Digest,
    pub metadata: NodeMetadata,
}

/// A symbolic link entry in a directory. Corresponds to a SymlinkNode in
/// the RE API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymlinkEntry {
    // Name of the symlink file relative to the directory containing the
    // symlink.
    pub name: String,
    // The symlink target.
    pub target: String,
    pub metadata: NodeMetadata,
}

/// Encoded canonical REAPI directory bytes plus decoded proto state.
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedDirectory {
    pub digest: Digest,
    pub bytes: Bytes,
    pub directory: Directory,
    pub warnings: TreeWarnings,
}

/// Encoded Merkle Tree for a directory containing all the encoded directory and files
/// in this directory. Meant to be provided to an Blob store uploader.
/// This *does not* represent a RE API Tree.
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedDirectoryTree {
    pub root_digest: Digest,
    pub directories: Vec<EncodedDirectory>,
    pub file_blobs: Vec<(Digest, PathBuf)>,
    pub warnings: TreeWarnings,
}

/// Warning counts for issues discovering while traversing a directory tree.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TreeWarnings {
    pub masked_setuid: usize,
    pub masked_setgid: usize,
    pub absolute_symlinks: usize,
    pub escaping_symlinks: usize,
}

impl TreeWarnings {
    /// Records one warning into the stable counters.
    pub fn add(&mut self, warning: TreeWarning) {
        match warning {
            TreeWarning::MaskedSetuid { .. } => self.masked_setuid += 1,
            TreeWarning::MaskedSetgid { .. } => self.masked_setgid += 1,
            TreeWarning::AbsoluteSymlink { .. } => self.absolute_symlinks += 1,
            TreeWarning::EscapingSymlink { .. } => self.escaping_symlinks += 1,
        }
    }

    /// Adds all counters from another warning summary.
    pub fn merge(&mut self, other: &TreeWarnings) {
        self.masked_setuid += other.masked_setuid;
        self.masked_setgid += other.masked_setgid;
        self.absolute_symlinks += other.absolute_symlinks;
        self.escaping_symlinks += other.escaping_symlinks;
    }
}

/// Tree encoding warnings that are lossy or potentially surprising but still supported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeWarning {
    MaskedSetuid { path: PathBuf },
    MaskedSetgid { path: PathBuf },
    AbsoluteSymlink { path: PathBuf, target: String },
    EscapingSymlink { path: PathBuf, target: String },
}

/// Errors returned by canonical tree encoding and decoding.
#[derive(Error, Debug)]
pub enum TreeError {
    #[error("Invalid entry name `{name}`: {reason}")]
    InvalidName { name: String, reason: String },
    #[error("Duplicate tree entry name `{name}`")]
    DuplicateName { name: String },
    #[error("Entry `{name}` is missing digest")]
    MissingDigest { name: String },
    #[error("Path contains a non-UTF-8 name: {path}")]
    NonUtf8Name { path: PathBuf },
    #[error("Symlink target is not UTF-8 for {path}")]
    NonUtf8SymlinkTarget { path: PathBuf },
    #[error("Unsupported node type at {path}: {kind}")]
    UnsupportedNodeType { path: PathBuf, kind: String },
    #[error("Unsupported timestamp at {path}: seconds={seconds} nanos={nanos}")]
    UnsupportedTimestamp {
        path: PathBuf,
        seconds: i64,
        nanos: i32,
    },
    #[error("Failed to decode directory {digest}: {source}")]
    Decode {
        digest: Digest,
        #[source]
        source: prost::DecodeError,
    },
    #[error("Directory digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch { expected: Digest, actual: Digest },
    #[error("Invalid digest in directory entry `{name}`: {source}")]
    InvalidDigest {
        name: String,
        #[source]
        source: DigestError,
    },
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
        source: Box<TreeError>,
    },
}

impl crate::error_context::ResultContextError for TreeError {
    fn with_context(self, operation: String) -> Self {
        TreeError::Context {
            operation,
            source: Box::new(self),
        }
    }
}

/// Builder for one canonical REAPI `Directory`.
#[derive(Debug, Clone)]
pub struct DirectoryBuilder {
    metadata: Option<NodeMetadata>,
    files: Vec<FileEntry>,
    directories: Vec<DirectoryEntry>,
    symlinks: Vec<SymlinkEntry>,
}

impl DirectoryBuilder {
    /// Creates an empty directory builder with no directory metadata.
    pub fn new() -> Self {
        Self {
            metadata: None,
            files: Vec::new(),
            directories: Vec::new(),
            symlinks: Vec::new(),
        }
    }

    /// Creates an empty directory builder with metadata for this directory.
    pub fn with_metadata(metadata: NodeMetadata) -> Self {
        Self {
            metadata: Some(metadata),
            files: Vec::new(),
            directories: Vec::new(),
            symlinks: Vec::new(),
        }
    }

    /// Adds a file entry.
    ///
    /// Entry names must be valid single path components. Duplicate names across
    /// all node kinds are rejected when `encode` is called.
    pub fn add_file(&mut self, entry: FileEntry) -> Result<(), TreeError> {
        validate_name(&entry.name)
            .with_context(|| format!("validate file entry name {}", entry.name))?;
        self.files.push(entry);
        Ok(())
    }

    /// Adds a child directory entry.
    ///
    /// Entry names must be valid single path components. Duplicate names across
    /// all node kinds are rejected when `encode` is called.
    pub fn add_directory(&mut self, entry: DirectoryEntry) -> Result<(), TreeError> {
        validate_name(&entry.name)
            .with_context(|| format!("validate directory entry name {}", entry.name))?;
        self.directories.push(entry);
        Ok(())
    }

    /// Adds a symlink entry.
    ///
    /// Entry names must be valid single path components. The target is stored
    /// exactly as supplied, including absolute paths and `..` components.
    pub fn add_symlink(&mut self, entry: SymlinkEntry) -> Result<(), TreeError> {
        validate_name(&entry.name)
            .with_context(|| format!("validate symlink entry name {}", entry.name))?;
        self.symlinks.push(entry);
        Ok(())
    }

    /// Encodes this directory as canonical REAPI bytes.
    ///
    /// Entries are sorted by bytewise UTF-8 name order inside each REAPI field,
    /// duplicate names across node kinds are rejected, and returned bytes are
    /// hashed exactly as uploaded to CAS.
    pub fn encode(mut self) -> Result<EncodedDirectory, TreeError> {
        sort_entries(&mut self);
        reject_duplicate_names(&self)
            .context("reject duplicate names before encoding directory")?;

        let mut warnings = TreeWarnings::default();
        let node_properties =
            normalize_optional_metadata(self.metadata, PathBuf::from("."), &mut warnings)
                .context("normalize root directory metadata")?;
        let files = self
            .files
            .into_iter()
            .map(|entry| {
                let name = entry.name.clone();
                file_node(entry, &mut warnings).with_context(|| format!("encode file node {name}"))
            })
            .collect::<Result<Vec<_>, _>>()
            .context("encode all file nodes for directory")?;
        let directories = self
            .directories
            .into_iter()
            .map(|entry| {
                let name = entry.name.clone();
                directory_node(entry).with_context(|| format!("encode directory node {name}"))
            })
            .collect::<Result<Vec<_>, _>>()
            .context("encode all directory nodes for directory")?;
        let symlinks = self
            .symlinks
            .into_iter()
            .map(|entry| {
                let name = entry.name.clone();
                symlink_node(entry, &mut warnings)
                    .with_context(|| format!("encode symlink node {name}"))
            })
            .collect::<Result<Vec<_>, _>>()
            .context("encode all symlink nodes for directory")?;
        let directory = Directory {
            files,
            directories,
            symlinks,
            node_properties,
        };
        let bytes = Bytes::from(directory.encode_to_vec());
        let digest = Digest::for_bytes(bytes.as_ref());
        Ok(EncodedDirectory {
            digest,
            bytes,
            directory,
            warnings,
        })
    }
}

impl Default for DirectoryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Decodes and validates canonical REAPI `Directory` bytes.
///
/// The raw bytes must hash to `expected`. The decoded directory must use
/// canonical per-kind ordering, must not contain duplicate names across files,
/// directories, and symlinks, and must contain valid digests and metadata.
pub fn decode_directory(expected: Digest, bytes: Bytes) -> Result<Directory, TreeError> {
    let actual = Digest::for_bytes(bytes.as_ref());
    if actual != expected {
        return Err(TreeError::DigestMismatch { expected, actual });
    }

    let directory = Directory::decode(bytes.as_ref()).map_err(|source| TreeError::Decode {
        digest: expected.clone(),
        source,
    })?;
    validate_directory(&directory)
        .with_context(|| format!("validate decoded REAPI Directory {expected}"))?;
    Ok(directory)
}

fn validate_name(name: &str) -> Result<(), TreeError> {
    let reason = if name.is_empty() {
        Some("must not be empty")
    } else if name == "." || name == ".." {
        Some("must not be `.` or `..`")
    } else if name.contains('/') {
        Some("must not contain `/`")
    } else {
        None
    };
    if let Some(reason) = reason {
        return Err(TreeError::InvalidName {
            name: name.to_string(),
            reason: reason.to_string(),
        });
    }
    Ok(())
}

fn sort_entries(builder: &mut DirectoryBuilder) {
    builder
        .files
        .sort_by(|left, right| left.name.cmp(&right.name));
    builder
        .directories
        .sort_by(|left, right| left.name.cmp(&right.name));
    builder
        .symlinks
        .sort_by(|left, right| left.name.cmp(&right.name));
}

fn reject_duplicate_names(builder: &DirectoryBuilder) -> Result<(), TreeError> {
    let mut names = HashSet::new();
    for name in builder
        .files
        .iter()
        .map(|entry| &entry.name)
        .chain(builder.directories.iter().map(|entry| &entry.name))
        .chain(builder.symlinks.iter().map(|entry| &entry.name))
    {
        if !names.insert(name) {
            return Err(TreeError::DuplicateName { name: name.clone() });
        }
    }
    Ok(())
}

fn normalize_optional_metadata(
    metadata: Option<NodeMetadata>,
    path: PathBuf,
    warnings: &mut TreeWarnings,
) -> Result<Option<NodeProperties>, TreeError> {
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let (metadata, metadata_warnings) = metadata
        .normalize(path.clone())
        .with_context(|| format!("normalize node metadata for {}", path.display()))?;
    warnings.merge(&metadata_warnings);
    Ok(metadata.into_node_properties())
}

fn file_node(entry: FileEntry, warnings: &mut TreeWarnings) -> Result<FileNode, TreeError> {
    let path = PathBuf::from(&entry.name);
    let node_properties = normalize_optional_metadata(Some(entry.metadata), path, warnings)
        .with_context(|| format!("normalize file node metadata for {}", entry.name))?;
    let mode = node_properties
        .as_ref()
        .and_then(|properties| properties.unix_mode)
        .unwrap_or(0);
    Ok(FileNode {
        name: entry.name,
        digest: Some(entry.digest.to_reapi()),
        is_executable: mode & 0o111 != 0,
        node_properties,
    })
}

fn directory_node(entry: DirectoryEntry) -> Result<DirectoryNode, TreeError> {
    Ok(DirectoryNode {
        name: entry.name,
        digest: Some(entry.digest.to_reapi()),
    })
}

fn symlink_node(
    entry: SymlinkEntry,
    warnings: &mut TreeWarnings,
) -> Result<SymlinkNode, TreeError> {
    let path = PathBuf::from(&entry.name);
    if entry.target.starts_with('/') {
        warnings.add(TreeWarning::AbsoluteSymlink {
            path: path.clone(),
            target: entry.target.clone(),
        });
    }
    if target_escapes(&entry.target) {
        warnings.add(TreeWarning::EscapingSymlink {
            path: path.clone(),
            target: entry.target.clone(),
        });
    }
    let node_properties = normalize_optional_metadata(Some(entry.metadata), path, warnings)
        .with_context(|| format!("normalize symlink node metadata for {}", entry.name))?;
    Ok(SymlinkNode {
        name: entry.name,
        target: entry.target,
        node_properties,
    })
}

fn target_escapes(target: &str) -> bool {
    let mut depth = 0i32;
    for component in target.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                if depth == 0 {
                    return true;
                }
                depth -= 1;
            }
            _ => depth += 1,
        }
    }
    false
}

fn validate_timestamp(path: PathBuf, timestamp: &Timestamp) -> Result<(), TreeError> {
    if !(0..=999_999_999).contains(&timestamp.nanos)
        || timestamp.seconds < MIN_TIMESTAMP_SECONDS
        || timestamp.seconds > MAX_TIMESTAMP_SECONDS
    {
        return Err(TreeError::UnsupportedTimestamp {
            path,
            seconds: timestamp.seconds,
            nanos: timestamp.nanos,
        });
    }
    Ok(())
}

fn validate_directory(directory: &Directory) -> Result<(), TreeError> {
    validate_sorted_names(directory.files.iter().map(|node| node.name.as_str()))
        .context("validate sorted file node names")?;
    validate_sorted_names(directory.directories.iter().map(|node| node.name.as_str()))
        .context("validate sorted directory node names")?;
    validate_sorted_names(directory.symlinks.iter().map(|node| node.name.as_str()))
        .context("validate sorted symlink node names")?;

    let mut names: HashSet<&str> = HashSet::new();
    for name in directory
        .files
        .iter()
        .map(|node| node.name.as_str())
        .chain(directory.directories.iter().map(|node| node.name.as_str()))
        .chain(directory.symlinks.iter().map(|node| node.name.as_str()))
    {
        validate_name(name).with_context(|| format!("validate decoded node name {name}"))?;
        if !names.insert(name) {
            return Err(TreeError::DuplicateName {
                name: name.to_string(),
            });
        }
    }

    validate_node_properties(".", directory.node_properties.as_ref())
        .context("validate root directory node properties")?;
    for node in &directory.files {
        let digest = node
            .digest
            .as_ref()
            .ok_or_else(|| TreeError::MissingDigest {
                name: node.name.clone(),
            })?;
        Digest::from_reapi(digest).map_err(|source| TreeError::InvalidDigest {
            name: node.name.clone(),
            source,
        })?;
        validate_node_properties(&node.name, node.node_properties.as_ref())
            .with_context(|| format!("validate file node properties for {}", node.name))?;
    }
    for node in &directory.directories {
        let digest = node
            .digest
            .as_ref()
            .ok_or_else(|| TreeError::MissingDigest {
                name: node.name.clone(),
            })?;
        Digest::from_reapi(digest).map_err(|source| TreeError::InvalidDigest {
            name: node.name.clone(),
            source,
        })?;
    }
    for node in &directory.symlinks {
        validate_node_properties(&node.name, node.node_properties.as_ref())
            .with_context(|| format!("validate symlink node properties for {}", node.name))?;
    }

    Ok(())
}

fn validate_sorted_names<'a>(names: impl Iterator<Item = &'a str>) -> Result<(), TreeError> {
    let mut previous: Option<&str> = None;
    for name in names {
        if previous.is_some_and(|previous| previous >= name) {
            return Err(TreeError::DuplicateName {
                name: name.to_string(),
            });
        }
        previous = Some(name);
    }
    Ok(())
}

fn validate_node_properties(
    name: &str,
    properties: Option<&NodeProperties>,
) -> Result<(), TreeError> {
    if let Some(properties) = properties
        && let Some(timestamp) = &properties.mtime
    {
        validate_timestamp(PathBuf::from(name), timestamp)
            .with_context(|| format!("validate node properties timestamp for {name}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reapi::remote_execution;

    fn digest() -> Digest {
        Digest::for_bytes(b"content")
    }

    fn file(name: &str, mode: u32) -> FileEntry {
        FileEntry {
            name: name.to_string(),
            digest: digest(),
            metadata: NodeMetadata::new(NodeKind::File, Some(mode), None),
        }
    }

    fn dir(name: &str) -> DirectoryEntry {
        DirectoryEntry {
            name: name.to_string(),
            digest: Digest::for_bytes(&[]),
            metadata: NodeMetadata::new(NodeKind::Directory, Some(0o755), None),
        }
    }

    fn symlink(name: &str, target: &str) -> SymlinkEntry {
        SymlinkEntry {
            name: name.to_string(),
            target: target.to_string(),
            metadata: NodeMetadata::new(NodeKind::Symlink, Some(0o777), None),
        }
    }

    fn rendered_chain(error: TreeError) -> String {
        anyhow::Error::new(error)
            .chain()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn canonical_ordering_is_stable() {
        let mut left = DirectoryBuilder::new();
        left.add_file(file("z.txt", 0o644)).unwrap();
        left.add_file(file("a.txt", 0o644)).unwrap();
        left.add_directory(dir("src")).unwrap();
        left.add_directory(dir("bin")).unwrap();
        left.add_symlink(symlink("rel", "a.txt")).unwrap();

        let mut right = DirectoryBuilder::new();
        right.add_symlink(symlink("rel", "a.txt")).unwrap();
        right.add_directory(dir("bin")).unwrap();
        right.add_directory(dir("src")).unwrap();
        right.add_file(file("a.txt", 0o644)).unwrap();
        right.add_file(file("z.txt", 0o644)).unwrap();

        let left = left.encode().unwrap();
        let right = right.encode().unwrap();
        assert_eq!(left.digest, right.digest);
        assert_eq!(left.bytes, right.bytes);
        assert_eq!(left.directory.files[0].name, "a.txt");
    }

    #[test]
    fn duplicate_names_across_kinds_are_rejected() {
        let mut builder = DirectoryBuilder::new();
        builder.add_file(file("same", 0o644)).unwrap();
        builder.add_directory(dir("same")).unwrap();

        let error = builder.encode().unwrap_err();
        let rendered = rendered_chain(error);
        assert!(rendered.contains("reject duplicate names before encoding directory"));
        assert!(rendered.contains("Duplicate tree entry name `same`"));
    }

    #[test]
    fn invalid_component_names_are_rejected() {
        for name in ["", ".", "..", "a/b"] {
            let mut builder = DirectoryBuilder::new();
            let error = builder.add_file(file(name, 0o644)).unwrap_err();
            let rendered = rendered_chain(error);
            assert!(rendered.contains("validate file entry name"));
            assert!(rendered.contains("Invalid entry name"));
        }
    }

    #[test]
    fn file_mode_executable_and_mtime_round_trip() {
        let timestamp = Timestamp {
            seconds: -1,
            nanos: 123,
        };
        let mut builder = DirectoryBuilder::new();
        builder
            .add_file(FileEntry {
                metadata: NodeMetadata::new(NodeKind::File, Some(0o755), Some(timestamp.clone())),
                ..file("run.sh", 0o755)
            })
            .unwrap();

        let encoded = builder.encode().unwrap();
        let decoded = decode_directory(encoded.digest, encoded.bytes).unwrap();
        let file = &decoded.files[0];
        assert!(file.is_executable);
        let properties = file.node_properties.as_ref().unwrap();
        assert_eq!(properties.unix_mode, Some(0o755));
        assert_eq!(properties.mtime, Some(timestamp));
    }

    #[test]
    fn directory_sticky_bit_round_trips_as_directory_metadata() {
        let encoded = DirectoryBuilder::with_metadata(NodeMetadata::new(
            NodeKind::Directory,
            Some(0o1777),
            None,
        ))
        .encode()
        .unwrap();

        assert_eq!(
            encoded.directory.node_properties.unwrap().unix_mode,
            Some(0o1777)
        );
    }

    #[test]
    fn setuid_and_setgid_are_masked_and_warned() {
        let mut builder = DirectoryBuilder::new();
        builder.add_file(file("tool", 0o6755)).unwrap();

        let encoded = builder.encode().unwrap();
        let file = &encoded.directory.files[0];
        assert_eq!(
            file.node_properties.as_ref().unwrap().unix_mode,
            Some(0o755)
        );
        assert_eq!(encoded.warnings.masked_setuid, 1);
        assert_eq!(encoded.warnings.masked_setgid, 1);
    }

    #[test]
    fn empty_directory_digest_is_stable() {
        let encoded = DirectoryBuilder::new().encode().unwrap();
        assert_eq!(
            encoded.digest.to_string(),
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0"
        );
    }

    #[test]
    fn timestamp_range_is_validated() {
        let mut builder = DirectoryBuilder::new();
        builder
            .add_file(FileEntry {
                metadata: NodeMetadata::new(
                    NodeKind::File,
                    Some(0o644),
                    Some(Timestamp {
                        seconds: MAX_TIMESTAMP_SECONDS + 1,
                        nanos: 0,
                    }),
                ),
                ..file("future", 0o644)
            })
            .unwrap();

        let error = builder.encode().unwrap_err();
        let rendered = rendered_chain(error);
        assert!(rendered.contains("encode file node future"));
        assert!(rendered.contains("normalize file node metadata for future"));
        assert!(rendered.contains("Unsupported timestamp at future"));
    }

    #[test]
    fn symlink_targets_round_trip_with_warnings() {
        let mut builder = DirectoryBuilder::new();
        builder
            .add_symlink(symlink("absolute", "/tmp/target"))
            .unwrap();
        builder
            .add_symlink(symlink("escaping", "../../outside"))
            .unwrap();

        let encoded = builder.encode().unwrap();
        assert_eq!(encoded.warnings.absolute_symlinks, 1);
        assert_eq!(encoded.warnings.escaping_symlinks, 1);
        assert_eq!(encoded.directory.symlinks[0].target, "/tmp/target");
        assert_eq!(encoded.directory.symlinks[1].target, "../../outside");
    }

    #[test]
    fn decode_rejects_digest_mismatch() {
        let encoded = DirectoryBuilder::new().encode().unwrap();
        let wrong = Digest::for_bytes(b"wrong");

        assert!(matches!(
            decode_directory(wrong, encoded.bytes),
            Err(TreeError::DigestMismatch { .. })
        ));
    }

    #[test]
    fn decode_rejects_non_canonical_ordering() {
        let directory = Directory {
            files: vec![
                file_node(file("z", 0o644), &mut TreeWarnings::default()).unwrap(),
                file_node(file("a", 0o644), &mut TreeWarnings::default()).unwrap(),
            ],
            directories: Vec::new(),
            symlinks: Vec::new(),
            node_properties: None,
        };
        let bytes = Bytes::from(directory.encode_to_vec());
        let digest = Digest::for_bytes(bytes.as_ref());

        let error = decode_directory(digest.clone(), bytes).unwrap_err();
        let rendered = rendered_chain(error);
        assert!(rendered.contains(&format!("validate decoded REAPI Directory {digest}")));
        assert!(rendered.contains("validate sorted file node names"));
        assert!(rendered.contains("Duplicate tree entry name `a`"));
    }

    #[test]
    fn decode_rejects_invalid_digest() {
        let directory = Directory {
            files: vec![FileNode {
                name: "bad".to_string(),
                digest: Some(remote_execution::Digest {
                    hash: "ABC".to_string(),
                    size_bytes: 1,
                }),
                is_executable: false,
                node_properties: None,
            }],
            directories: Vec::new(),
            symlinks: Vec::new(),
            node_properties: None,
        };
        let bytes = Bytes::from(directory.encode_to_vec());
        let digest = Digest::for_bytes(bytes.as_ref());

        let error = decode_directory(digest.clone(), bytes).unwrap_err();
        let rendered = rendered_chain(error);
        assert!(rendered.contains(&format!("validate decoded REAPI Directory {digest}")));
        assert!(rendered.contains("Invalid digest in directory entry `bad`"));
    }
}
