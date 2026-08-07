use std::fs::{self, File};
use std::io::{self, ErrorKind, Write};
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use tempfile::{Builder, NamedTempFile};

use crate::digest::Digest;
use crate::error_context::ResultContext;

use super::{SessionError, create_dir_if_absent, fs_error};

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
    pub fn open(root: PathBuf) -> Result<Self, SessionError> {
        tracing::info!("BlobCache::open(root={})", root.display());
        create_dir_if_absent(&root)?;
        Ok(Self { root })
    }

    // Returns whether the given blob is cached.
    pub fn exists(&self, digest: &Digest) -> bool {
        self.path(digest).exists()
    }

    pub fn read_blob(&self, digest: &Digest) -> Result<Option<Bytes>, SessionError> {
        let path = self.path(digest);

        match fs::read(&path) {
            Ok(data) => Ok(Some(Bytes::from(data))),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(fs_error("read admitted blob", &path, err)),
        }
    }

    pub fn read_range(
        &self,
        digest: &Digest,
        offset: u64,
        size: usize,
    ) -> Result<Option<Bytes>, SessionError> {
        let path = self.path(digest);
        if !path.exists() {
            return Ok(None);
        }
        read_file_range(&path, offset, size).map(Some)
    }

    pub fn start_download(&self, digest: &Digest) -> Result<BlobWriter, SessionError> {
        let destination = self.path(digest);
        let shard = destination
            .parent()
            .expect("sharded blob destination always has a parent");
        create_dir_if_absent(shard).with_context(|| {
            format!(
                "creating shard directory {} to download blob with digest {}",
                shard.display(),
                digest
            )
        })?;
        let temporary = Builder::new()
            .prefix(&format!(".{}-{}-", digest.hash(), digest.size_bytes()))
            .suffix(".tmp")
            .tempfile_in(shard)
            .map_err(|source| fs_error("create pending blob", shard, source))?;
        temporary
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|source| fs_error("secure pending blob", temporary.path(), source))?;
        Ok(BlobWriter {
            expected: digest.clone(),
            destination,
            temporary,
            hasher: Sha256::new(),
            written: 0,
        })
    }

    pub fn finalize(&self, mut writer: BlobWriter) -> Result<(), SessionError> {
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
            // No error if the blob already exists. Likely a concurrent operation downloaded the
            // same blob.
            Err(error) if error.error.kind() == ErrorKind::AlreadyExists => Ok(()),
            Err(error) => Err(fs_error("admit verified blob", &destination, error.error)),
        }
    }

    pub fn entry_count(&self) -> Result<u64, SessionError> {
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

pub fn read_file_range(path: &Path, offset: u64, size: usize) -> Result<Bytes, SessionError> {
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
        BlobCache::open(temp.path().to_path_buf()).unwrap()
    }

    #[test]
    fn writer_streams_and_atomically_admits_verified_content() {
        let temp = tempfile::tempdir().unwrap();
        let cache = cache(&temp);
        let digest = Digest::for_bytes(b"streamed content");
        let mut writer = cache.start_download(&digest).unwrap();
        writer.write_all(b"streamed ").unwrap();
        writer.write_all(b"content").unwrap();
        cache.finalize(writer).unwrap();
        assert_eq!(
            cache.read_blob(&digest).unwrap().unwrap().as_ref(),
            b"streamed content"
        );

        assert!(cache.exists(&digest));
    }

    #[test]
    fn writer_rejects_excess_and_drop_removes_unfinished_temporary() {
        let temp = tempfile::tempdir().unwrap();
        let cache = cache(&temp);
        let digest = Digest::for_bytes(b"short");
        let shard = cache.path(&digest).parent().unwrap().to_path_buf();
        {
            let mut writer = cache.start_download(&digest).unwrap();
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

        let mut short = cache.start_download(&expected).unwrap();
        short.write_all(b"short").unwrap();
        assert!(matches!(
            cache.finalize(short),
            Err(SessionError::BlobIntegrity { .. })
        ));

        let mut wrong = cache.start_download(&expected).unwrap();
        wrong.write_all(b"notright").unwrap();
        assert!(matches!(
            cache.finalize(wrong),
            Err(SessionError::BlobIntegrity { .. })
        ));
        assert!(cache.read_blob(&expected).unwrap().is_none());
    }

    #[test]
    fn concurrent_writers_admit_without_clobbering() {
        let temp = tempfile::tempdir().unwrap();
        let cache = cache(&temp);
        let digest = Digest::for_bytes(b"same");
        let mut first = cache.start_download(&digest).unwrap();
        let mut second = cache.start_download(&digest).unwrap();
        first.write_all(b"same").unwrap();
        second.write_all(b"same").unwrap();
        cache.finalize(first).unwrap();
        cache.finalize(second).unwrap();
        assert_eq!(cache.entry_count().unwrap(), 1);
    }
}
