use std::fs::{self, File};
use std::io::{self, Write};
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use tempfile::{Builder, NamedTempFile};

use crate::digest::Digest;

use super::{SessionError, create_dir_private, fs_error, validate_existing_permissions};

/// Outcome of the authoritative cache recheck before a remote download.
pub enum BlobDownloader {
    /// The expected digest is already admitted.
    Exists,
    /// The caller must stream the remote object and finalize this writer.
    Writer(Box<BlobWriter>),
}

/// Opaque non-cloneable sequential writer for one expected immutable blob.
pub struct BlobWriter {
    expected: Digest,
    destination: PathBuf,
    temporary: NamedTempFile,
    hasher: Sha256,
    written: u64,
}

impl Write for BlobWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let additional = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let expected = u64::try_from(self.expected.size_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "digest size is negative"))?;
        if self.written.saturating_add(additional) > expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "blob {} exceeds its expected size of {expected} bytes",
                    self.expected
                ),
            ));
        }
        let count = self.temporary.write(bytes)?;
        self.hasher.update(&bytes[..count]);
        self.written += u64::try_from(count).expect("write count fits u64");
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.temporary.flush()
    }
}

pub(super) struct BlobCache {
    root: PathBuf,
}

impl BlobCache {
    pub(super) fn open(root: PathBuf) -> Result<Self, SessionError> {
        let cache = root
            .parent()
            .expect("blob cache root always has a cache parent");
        create_dir_private(cache)?;
        create_dir_private(&root)?;
        Ok(Self { root })
    }

    pub(super) fn read_blob(&self, digest: &Digest) -> Result<Option<Bytes>, SessionError> {
        let path = self.path(digest);
        if !validate_blob_path(&path)? {
            return Ok(None);
        }
        fs::read(&path)
            .map(Bytes::from)
            .map(Some)
            .map_err(|source| fs_error("read admitted blob", &path, source))
    }

    pub(super) fn read_range(
        &self,
        digest: &Digest,
        offset: u64,
        size: usize,
    ) -> Result<Option<Bytes>, SessionError> {
        let path = self.path(digest);
        if !validate_blob_path(&path)? {
            return Ok(None);
        }
        read_file_range(&path, offset, size).map(Some)
    }

    pub(super) fn start_download(&self, digest: &Digest) -> Result<BlobDownloader, SessionError> {
        let destination = self.path(digest);
        if validate_blob_path(&destination)? {
            return Ok(BlobDownloader::Exists);
        }
        let shard = destination
            .parent()
            .expect("sharded blob destination always has a parent");
        create_dir_private(shard)?;
        let temporary = Builder::new()
            .prefix(&format!(".{}-{}-", digest.hash(), digest.size_bytes()))
            .suffix(".tmp")
            .tempfile_in(shard)
            .map_err(|source| fs_error("create pending blob", shard, source))?;
        temporary
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|source| fs_error("secure pending blob", temporary.path(), source))?;
        Ok(BlobDownloader::Writer(Box::new(BlobWriter {
            expected: digest.clone(),
            destination,
            temporary,
            hasher: Sha256::new(),
            written: 0,
        })))
    }

    pub(super) fn finalize(&self, mut writer: BlobWriter) -> Result<(), SessionError> {
        let expected_size = u64::try_from(writer.expected.size_bytes()).map_err(|_| {
            SessionError::BlobIntegrity {
                expected: writer.expected.clone(),
                actual: Digest::for_bytes(&[]),
            }
        })?;
        if writer.written != expected_size {
            let actual = Digest::new(
                hex::encode(writer.hasher.clone().finalize()),
                i64::try_from(writer.written).expect("written size is bounded by expected digest"),
            )
            .expect("SHA-256 and non-negative written size form a valid digest");
            return Err(SessionError::BlobIntegrity {
                expected: writer.expected,
                actual,
            });
        }
        let actual = Digest::new(
            hex::encode(writer.hasher.clone().finalize()),
            i64::try_from(writer.written).expect("expected digest size fits i64"),
        )
        .expect("SHA-256 and expected size form a valid digest");
        if actual != writer.expected {
            return Err(SessionError::BlobIntegrity {
                expected: writer.expected,
                actual,
            });
        }
        writer
            .temporary
            .flush()
            .map_err(|source| fs_error("flush pending blob", writer.temporary.path(), source))?;
        writer
            .temporary
            .as_file()
            .sync_all()
            .map_err(|source| fs_error("sync pending blob", writer.temporary.path(), source))?;
        let destination = writer.destination.clone();
        match writer.temporary.persist_noclobber(&destination) {
            Ok(_) => sync_parent(&destination),
            Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                if validate_blob_path(&destination)? {
                    Ok(())
                } else {
                    Err(fs_error(
                        "validate competing blob admission",
                        &destination,
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            "competing cache entry disappeared",
                        ),
                    ))
                }
            }
            Err(error) => Err(fs_error("admit verified blob", &destination, error.error)),
        }
    }

    pub(super) fn entry_count(&self) -> Result<u64, SessionError> {
        if !self.root.exists() {
            return Ok(0);
        }
        let mut count = 0_u64;
        for shard in fs::read_dir(&self.root)
            .map_err(|source| fs_error("list blob-cache shards", &self.root, source))?
        {
            let shard =
                shard.map_err(|source| fs_error("read blob-cache shard", &self.root, source))?;
            if !shard
                .file_type()
                .map_err(|source| fs_error("inspect blob-cache shard", &shard.path(), source))?
                .is_dir()
            {
                continue;
            }
            for entry in fs::read_dir(shard.path())
                .map_err(|source| fs_error("list blob-cache entries", &shard.path(), source))?
            {
                let entry = entry
                    .map_err(|source| fs_error("read blob-cache entry", &shard.path(), source))?;
                if entry
                    .file_type()
                    .map_err(|source| fs_error("inspect blob-cache entry", &entry.path(), source))?
                    .is_file()
                    && !entry.file_name().to_string_lossy().starts_with('.')
                {
                    count = count.saturating_add(1);
                }
            }
        }
        Ok(count)
    }

    fn path(&self, digest: &Digest) -> PathBuf {
        self.root.join(&digest.hash()[..2]).join(format!(
            "{}-{}",
            digest.hash(),
            digest.size_bytes()
        ))
    }
}

fn validate_blob_path(path: &Path) -> Result<bool, SessionError> {
    let Some(shard) = path.parent() else {
        return Err(SessionError::UnsafePath {
            path: path.to_path_buf(),
            reason: "blob path has no shard parent".into(),
        });
    };
    let shard_metadata = match fs::symlink_metadata(shard) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => return Err(fs_error("inspect blob-cache shard", shard, source)),
    };
    if !shard_metadata.file_type().is_dir() {
        return Err(SessionError::UnsafePath {
            path: shard.to_path_buf(),
            reason: "expected a private shard directory and will not follow a symlink".into(),
        });
    }
    validate_existing_permissions(shard, &shard_metadata)?;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => return Err(fs_error("inspect admitted blob", path, source)),
    };
    if !metadata.file_type().is_file() {
        return Err(SessionError::UnsafePath {
            path: path.to_path_buf(),
            reason: "expected a private regular cache file and will not follow a symlink".into(),
        });
    }
    validate_existing_permissions(path, &metadata)?;
    Ok(true)
}

pub(super) fn read_file_range(
    path: &Path,
    offset: u64,
    size: usize,
) -> Result<Bytes, SessionError> {
    let file = File::open(path).map_err(|source| fs_error("open file range", path, source))?;
    let length = file
        .metadata()
        .map_err(|source| fs_error("inspect file range", path, source))?
        .len();
    let available = length.saturating_sub(offset);
    let requested = u64::try_from(size).unwrap_or(u64::MAX);
    let read_size = usize::try_from(available.min(requested)).unwrap_or(size);
    let mut buffer = vec![0; read_size];
    let count = file
        .read_at(&mut buffer, offset)
        .map_err(|source| fs_error("read file range", path, source))?;
    buffer.truncate(count);
    Ok(Bytes::from(buffer))
}

fn sync_parent(path: &Path) -> Result<(), SessionError> {
    let parent = path.parent().expect("cache destination has a parent");
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| fs_error("sync blob-cache shard", parent, source))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache(temp: &tempfile::TempDir) -> BlobCache {
        BlobCache::open(temp.path().join("cache/blobs")).unwrap()
    }

    #[test]
    fn writer_streams_and_atomically_admits_verified_content() {
        let temp = tempfile::tempdir().unwrap();
        let cache = cache(&temp);
        let digest = Digest::for_bytes(b"streamed content");
        let BlobDownloader::Writer(mut writer) = cache.start_download(&digest).unwrap() else {
            panic!("fresh cache unexpectedly contained blob");
        };
        writer.write_all(b"streamed ").unwrap();
        writer.write_all(b"content").unwrap();
        cache.finalize(*writer).unwrap();
        assert_eq!(
            cache.read_blob(&digest).unwrap().unwrap().as_ref(),
            b"streamed content"
        );
    }

    #[test]
    fn writer_rejects_excess_and_drop_removes_unfinished_temporary() {
        let temp = tempfile::tempdir().unwrap();
        let cache = cache(&temp);
        let digest = Digest::for_bytes(b"short");
        let shard = cache.path(&digest).parent().unwrap().to_path_buf();
        {
            let BlobDownloader::Writer(mut writer) = cache.start_download(&digest).unwrap() else {
                panic!("fresh cache unexpectedly contained blob");
            };
            assert_eq!(
                writer.write(b"too many bytes").unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
        assert_eq!(fs::read_dir(shard).unwrap().count(), 0);
    }

    #[test]
    fn finalization_rejects_size_and_hash_mismatches() {
        let temp = tempfile::tempdir().unwrap();
        let cache = cache(&temp);
        let expected = Digest::for_bytes(b"expected");

        let BlobDownloader::Writer(mut short) = cache.start_download(&expected).unwrap() else {
            panic!("fresh cache unexpectedly contained blob");
        };
        short.write_all(b"short").unwrap();
        assert!(matches!(
            cache.finalize(*short),
            Err(SessionError::BlobIntegrity { .. })
        ));

        let BlobDownloader::Writer(mut wrong) = cache.start_download(&expected).unwrap() else {
            panic!("fresh cache unexpectedly contained blob");
        };
        wrong.write_all(b"notright").unwrap();
        assert!(matches!(
            cache.finalize(*wrong),
            Err(SessionError::BlobIntegrity { .. })
        ));
        assert!(cache.read_blob(&expected).unwrap().is_none());
    }

    #[test]
    fn concurrent_writers_admit_without_clobbering() {
        let temp = tempfile::tempdir().unwrap();
        let cache = cache(&temp);
        let digest = Digest::for_bytes(b"same");
        let BlobDownloader::Writer(mut first) = cache.start_download(&digest).unwrap() else {
            panic!("fresh cache unexpectedly contained blob");
        };
        let BlobDownloader::Writer(mut second) = cache.start_download(&digest).unwrap() else {
            panic!("pending blob must not reserve a digest");
        };
        first.write_all(b"same").unwrap();
        second.write_all(b"same").unwrap();
        cache.finalize(*first).unwrap();
        cache.finalize(*second).unwrap();
        assert_eq!(cache.entry_count().unwrap(), 1);
    }
}
