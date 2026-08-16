use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, ErrorKind, Write};
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use tempfile::{Builder, NamedTempFile};
use tokio::runtime::Handle;

use crate::cas::BlobStore;
use crate::digest::Digest;
use crate::error_context::ResultContext;

use super::{SessionError, create_dir_if_absent, fs_error, internal_error};

/// Synchronous read-through store backed by the shared verified local cache.
pub(super) struct CachedBlobStore {
    /// Root directory containing digest-sharded cache entries.
    root: PathBuf,
    /// Shared remote store used to fill local cache misses.
    blob_store: Box<dyn BlobStore>,
    /// Runtime used to bridge synchronous reads to the asynchronous remote store.
    runtime: Handle,
    /// Per-digest locks that coalesce simultaneous cache misses.
    download_locks: Mutex<HashMap<Digest, Arc<DownloadLock>>>,
}

impl CachedBlobStore {
    /// Opens a cache rooted at `root` with `blob_store` as the remote backing store.
    ///
    /// Returns a ready cache after creating `root` when it is absent. Returns
    /// `SessionError` when the cache directory cannot be initialized.
    pub(super) fn open(
        root: PathBuf,
        blob_store: Box<dyn BlobStore>,
        runtime: Handle,
    ) -> Result<Self, SessionError> {
        tracing::info!("CachedBlobStore::open(root={})", root.display());
        create_dir_if_absent(&root)
            .with_context(|| format!("initialize verified blob cache at {}", root.display()))?;
        Ok(Self {
            root,
            blob_store,
            runtime,
            download_locks: Mutex::new(HashMap::new()),
        })
    }

    /// Returns the complete verified blob and whether this call downloaded it.
    ///
    /// A cache miss is downloaded and admitted before this method returns.
    pub(super) fn read_blob(&self, digest: &Digest) -> Result<(Bytes, bool), SessionError> {
        let downloaded = self.ensure_cached(digest)?;
        let bytes = self.read_cached_blob(digest)?;
        Ok((bytes, downloaded))
    }

    /// Returns up to `size` bytes starting at `offset` and whether this call downloaded it.
    ///
    /// Returns empty bytes when `offset` is at or beyond EOF. A cache miss is
    /// downloaded completely before the range is served.
    pub(super) fn read_range(
        &self,
        digest: &Digest,
        offset: u64,
        size: usize,
    ) -> Result<(Bytes, bool), SessionError> {
        let downloaded = self.ensure_cached(digest)?;
        let bytes = read_file_range(&self.path(digest), offset, size)?;
        Ok((bytes, downloaded))
    }

    /// Ensures `digest` has an admitted cache entry and reports whether this call filled it.
    fn ensure_cached(&self, digest: &Digest) -> Result<bool, SessionError> {
        if self.cache_contains(digest) {
            return Ok(false);
        }

        // TODO: Consider a RAII Guard to acquire and release download lock.
        let lock = self.acquire_download_lock(digest)?;
        let guard = match lock.gate.lock() {
            Ok(guard) => guard,
            Err(_) => {
                self.release_download_lock(digest, &lock)?;
                return Err(internal_error(format!(
                    "wait for blob download {digest}: download gate is poisoned"
                )));
            }
        };
        let result = self.fill_cache_after_lock(digest);
        drop(guard);
        self.release_download_lock(digest, &lock)?;
        result
    }

    /// Rechecks a cache miss while holding its digest lock and fills it if still absent.
    fn fill_cache_after_lock(&self, digest: &Digest) -> Result<bool, SessionError> {
        if self.cache_contains(digest) {
            return Ok(false);
        }

        let mut pending = self.start_pending_blob(digest)?;
        self.runtime
            .block_on(self.blob_store.stream_blob(digest, &mut pending))
            .map_err(|source| internal_error(format!("stream remote blob {digest}: {source}")))?;
        self.admit_pending_blob(pending)?;
        Ok(true)
    }

    /// Reads an admitted complete blob and reports a stable error if it disappeared.
    fn read_cached_blob(&self, digest: &Digest) -> Result<Bytes, SessionError> {
        let path = self.path(digest);
        match fs::read(&path) {
            Ok(data) => Ok(Bytes::from(data)),
            Err(source) if source.kind() == ErrorKind::NotFound => Err(internal_error(format!(
                "read admitted blob {digest} at {}: entry is missing",
                path.display()
            ))),
            Err(source) => Err(fs_error("read admitted blob", &path, source)),
        }
    }

    /// Returns whether the final digest path currently exists.
    fn cache_contains(&self, digest: &Digest) -> bool {
        self.path(digest).exists()
    }

    /// Joins the digest-specific download cohort before waiting on its lock.
    fn acquire_download_lock(&self, digest: &Digest) -> Result<Arc<DownloadLock>, SessionError> {
        let mut locks = self.download_locks.lock().map_err(|_| {
            internal_error(format!(
                "coordinate blob downloads for {digest}: lock map is poisoned"
            ))
        })?;
        let lock = locks
            .entry(digest.clone())
            .or_insert_with(|| Arc::new(DownloadLock::new()))
            .clone();
        lock.participants.fetch_add(1, Ordering::Relaxed);
        Ok(lock)
    }

    /// Leaves a digest-specific download cohort and removes it after its final participant.
    fn release_download_lock(
        &self,
        digest: &Digest,
        lock: &Arc<DownloadLock>,
    ) -> Result<(), SessionError> {
        let mut locks = self.download_locks.lock().map_err(|_| {
            internal_error(format!(
                "release blob download coordination for {digest}: lock map is poisoned"
            ))
        })?;
        let previous = lock.participants.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0, "download lock participant count underflowed");
        if previous == 1
            && locks
                .get(digest)
                .is_some_and(|current| Arc::ptr_eq(current, lock))
        {
            locks.remove(digest);
        }
        Ok(())
    }

    /// Creates an exclusive shard-local temporary file for one expected digest.
    fn start_pending_blob(&self, digest: &Digest) -> Result<PendingBlob, SessionError> {
        let destination = self.path(digest);
        let shard = destination
            .parent()
            .expect("sharded blob destination always has a parent");
        create_dir_if_absent(shard).with_context(|| {
            format!("create cache shard {} for blob {}", shard.display(), digest)
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
        Ok(PendingBlob::new(digest.clone(), destination, temporary))
    }

    /// Verifies, syncs, and atomically admits a completed remote stream.
    fn admit_pending_blob(&self, mut pending: PendingBlob) -> Result<(), SessionError> {
        pending.verify()?;
        pending
            .temporary
            .flush()
            .map_err(|source| fs_error("flush pending blob", pending.temporary.path(), source))?;
        pending
            .temporary
            .as_file()
            .sync_all()
            .map_err(|source| fs_error("sync pending blob", pending.temporary.path(), source))?;
        let destination = pending.destination.clone();
        match pending.temporary.persist_noclobber(&destination) {
            Ok(_) => Ok(()),
            Err(error) if error.error.kind() == ErrorKind::AlreadyExists => Ok(()),
            Err(error) => Err(fs_error("admit verified blob", &destination, error.error)),
        }
    }

    /// Builds the fixed shard-local cache path for `digest`.
    fn path(&self, digest: &Digest) -> PathBuf {
        let hash = digest.hash();
        assert!(
            hash.len() >= 2,
            "valid digest hash must have at least two characters for cache sharding: {hash}"
        );
        self.root
            .join(&hash[..2])
            .join(format!("{}-{}", hash, digest.size_bytes()))
    }
}

/// Digest-specific admission gate and its set of joined readers.
struct DownloadLock {
    /// Serializes the cache check, remote stream, verification, and admission.
    gate: Mutex<()>,
    /// Readers that joined before this lock can be removed from the coordination map.
    participants: AtomicUsize,
}

impl DownloadLock {
    /// Creates an idle digest-specific admission gate.
    fn new() -> Self {
        Self {
            gate: Mutex::new(()),
            participants: AtomicUsize::new(0),
        }
    }
}

/// Private sequential writer that hashes and bounds one pending cache object.
struct PendingBlob {
    /// Digest expected from the remote store.
    expected: Digest,
    /// Final no-clobber cache destination.
    destination: PathBuf,
    /// Shard-local file removed automatically when not admitted.
    temporary: NamedTempFile,
    /// Running SHA-256 over accepted bytes.
    hasher: Sha256,
    /// Exact number of accepted bytes.
    written: u64,
}

impl PendingBlob {
    /// Creates a pending writer for `expected` and the already-created temporary file.
    fn new(expected: Digest, destination: PathBuf, temporary: NamedTempFile) -> Self {
        Self {
            expected,
            destination,
            temporary,
            hasher: Sha256::new(),
            written: 0,
        }
    }

    /// Verifies the pending file size and SHA-256 digest before it becomes visible.
    fn verify(&self) -> Result<(), SessionError> {
        let actual = Digest::new(
            hex::encode(self.hasher.clone().finalize()),
            i64::try_from(self.written).expect("pending bytes are bounded by a valid digest"),
        )
        .expect("SHA-256 output and non-negative pending size form a valid digest");
        if actual == self.expected {
            return Ok(());
        }
        Err(internal_error(format!(
            "verify pending blob: expected {}, got {actual}",
            self.expected
        )))
    }
}

impl Write for PendingBlob {
    /// Writes bytes to the pending file while rejecting writes beyond the expected size.
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let expected = u64::try_from(self.expected.size_bytes())
            .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "digest size is negative"))?;
        let additional = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if self.written.saturating_add(additional) > expected {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("blob {} exceeds expected size {expected}", self.expected),
            ));
        }
        let count = self.temporary.write(bytes)?;
        self.hasher.update(&bytes[..count]);
        self.written += u64::try_from(count).expect("write count fits u64");
        Ok(count)
    }

    /// Flushes buffered bytes to the pending file without admitting it.
    fn flush(&mut self) -> io::Result<()> {
        self.temporary.flush()
    }
}

/// Reads a bounded range of bytes at an offset from a file.
///
/// Returns empty bytes at or beyond EOF. Returns `SessionError` when opening,
/// inspecting, or reading `path` fails.
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Barrier;
    use std::thread;

    use async_trait::async_trait;

    use crate::cas::{Blob, CasError, CasOperation, UploadStats};

    use super::*;

    /// Fake remote store whose streams can be counted and held at a test barrier.
    #[derive(Clone)]
    struct FakeBlobStore {
        /// Objects served by digest.
        blobs: Arc<Mutex<HashMap<Digest, Bytes>>>,
        /// Number of streaming calls made by digest.
        streams: Arc<Mutex<HashMap<Digest, usize>>>,
        /// One optional stream error consumed by the next remote read.
        failure: Arc<Mutex<Option<CasError>>>,
        /// Optional stream rendezvous proving reads reached remote storage concurrently.
        barrier: Arc<Mutex<Option<Arc<Barrier>>>>,
    }

    #[async_trait]
    impl BlobStore for FakeBlobStore {
        async fn find_missing_blobs(&self, _digests: &[Digest]) -> Result<Vec<Digest>, CasError> {
            Ok(Vec::new())
        }

        async fn upload_blobs(&self, _blobs: Vec<Blob>) -> Result<UploadStats, CasError> {
            Ok(UploadStats::default())
        }

        async fn stream_blob(
            &self,
            digest: &Digest,
            destination: &mut (dyn Write + Send),
        ) -> Result<(), CasError> {
            *self
                .streams
                .lock()
                .unwrap()
                .entry(digest.clone())
                .or_default() += 1;
            if let Some(error) = self.failure.lock().unwrap().take() {
                return Err(error);
            }
            let barrier = self.barrier.lock().unwrap().clone();
            if let Some(barrier) = barrier {
                barrier.wait();
            }
            destination
                .write_all(self.blobs.lock().unwrap().get(digest).unwrap())
                .unwrap();
            Ok(())
        }
    }

    /// Creates a cache using a multi-thread runtime suitable for concurrent synchronous reads.
    fn cache(
        temp: &tempfile::TempDir,
        blobs: impl IntoIterator<Item = (Digest, Bytes)>,
    ) -> (
        Arc<CachedBlobStore>,
        Arc<FakeBlobStore>,
        tokio::runtime::Runtime,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let store = Arc::new(FakeBlobStore {
            blobs: Arc::new(Mutex::new(blobs.into_iter().collect())),
            streams: Arc::new(Mutex::new(HashMap::new())),
            failure: Arc::new(Mutex::new(None)),
            barrier: Arc::new(Mutex::new(None)),
        });
        (
            Arc::new(
                CachedBlobStore::open(
                    temp.path().to_path_buf(),
                    Box::new(store.as_ref().clone()),
                    runtime.handle().clone(),
                )
                .unwrap(),
            ),
            store,
            runtime,
        )
    }

    #[test]
    fn complete_read_downloads_once_then_serves_the_verified_cache() {
        // Set up one remote blob behind an empty shared cache.
        let temp = tempfile::tempdir().unwrap();
        let digest = Digest::for_bytes(b"streamed content");
        let (cache, _fake_blob_store, _runtime) = cache(
            &temp,
            [(digest.clone(), Bytes::from_static(b"streamed content"))],
        );

        // The first read succeeds by downloading; the second succeeds from the admitted file.
        assert_eq!(
            cache.read_blob(&digest).unwrap(),
            (Bytes::from_static(b"streamed content"), true)
        );
        assert_eq!(
            cache.read_blob(&digest).unwrap(),
            (Bytes::from_static(b"streamed content"), false)
        );
    }

    #[test]
    fn ranged_read_downloads_the_whole_blob_and_handles_eof() {
        // Set up an empty cache with one remote object larger than the requested range.
        let temp = tempfile::tempdir().unwrap();
        let digest = Digest::for_bytes(b"streamed content");
        let (cache, _fake_blob_store, _runtime) = cache(
            &temp,
            [(digest.clone(), Bytes::from_static(b"streamed content"))],
        );

        // The first range downloads the object, while later and EOF ranges use the cache.
        assert_eq!(
            cache.read_range(&digest, 9, 4).unwrap(),
            (Bytes::from_static(b"cont"), true)
        );
        assert_eq!(
            cache.read_range(&digest, 99, 4).unwrap(),
            (Bytes::new(), false)
        );
    }

    #[test]
    fn concurrent_readers_coalesce_one_digest_download() {
        // Set up two synchronous readers for one uncached digest.
        let temp = tempfile::tempdir().unwrap();
        let digest = Digest::for_bytes(b"same blob");
        let (cache, fake_blob_store, _runtime) =
            cache(&temp, [(digest.clone(), Bytes::from_static(b"same blob"))]);
        let first_cache = Arc::clone(&cache);
        let first_digest = digest.clone();
        let first = thread::spawn(move || first_cache.read_blob(&first_digest).unwrap());
        let second_cache = Arc::clone(&cache);
        let second_digest = digest.clone();
        let second = thread::spawn(move || second_cache.read_blob(&second_digest).unwrap());

        // Exactly one caller fills the cache; the waiting caller observes the admitted entry.
        let results = [first.join().unwrap(), second.join().unwrap()];
        assert_eq!(
            results.iter().filter(|(_, downloaded)| *downloaded).count(),
            1
        );
        assert_eq!(
            results.iter().filter(|(_, downloaded)| !downloaded).count(),
            1
        );
        assert_eq!(
            fake_blob_store.streams.lock().unwrap().get(&digest),
            Some(&1)
        );
    }

    #[test]
    fn readers_of_different_digests_stream_concurrently() {
        // Set up two distinct misses and require both remote streams to rendezvous.
        let temp = tempfile::tempdir().unwrap();
        let first_digest = Digest::for_bytes(b"first");
        let second_digest = Digest::for_bytes(b"second");
        let (cache, fake_blob_store, _runtime) = cache(
            &temp,
            [
                (first_digest.clone(), Bytes::from_static(b"first")),
                (second_digest.clone(), Bytes::from_static(b"second")),
            ],
        );
        *fake_blob_store.barrier.lock().unwrap() = Some(Arc::new(Barrier::new(2)));

        // Both reads complete only if their distinct digest locks allow concurrent streaming.
        let first_cache = Arc::clone(&cache);
        let first = thread::spawn(move || first_cache.read_blob(&first_digest).unwrap());
        let second_cache = Arc::clone(&cache);
        let second = thread::spawn(move || second_cache.read_blob(&second_digest).unwrap());
        assert!(first.join().unwrap().1);
        assert!(second.join().unwrap().1);
    }

    #[test]
    fn failed_fill_keeps_the_digest_cohort_until_waiting_readers_leave() {
        // Set up one remote failure and manually join two readers to the same digest cohort.
        let temp = tempfile::tempdir().unwrap();
        let digest = Digest::for_bytes(b"retry");
        let (cache, fake_blob_store, _runtime) =
            cache(&temp, [(digest.clone(), Bytes::from_static(b"retry"))]);
        *fake_blob_store.failure.lock().unwrap() = Some(CasError::BlobStatus {
            operation: CasOperation::ByteStreamRead,
            digest: digest.clone(),
            message: "transient test failure".to_string(),
        });
        let first = cache.acquire_download_lock(&digest).unwrap();
        let waiting = cache.acquire_download_lock(&digest).unwrap();

        // The first reader fails, but its departure must not remove the waiting reader's cohort.
        let guard = first.gate.lock().unwrap();
        assert!(matches!(
            cache.fill_cache_after_lock(&digest),
            Err(SessionError::InternalError { .. })
        ));
        drop(guard);
        cache.release_download_lock(&digest, &first).unwrap();
        let waiting_guard = waiting.gate.lock().unwrap();

        // A third reader joins the waiting cohort instead of creating a concurrent retry lock.
        let third = cache.acquire_download_lock(&digest).unwrap();
        assert!(Arc::ptr_eq(&waiting, &third));
        drop(waiting_guard);
        cache.release_download_lock(&digest, &waiting).unwrap();
        cache.release_download_lock(&digest, &third).unwrap();
    }

    #[test]
    fn malformed_and_failed_streams_leave_no_cache_entry_and_allow_retry() {
        // Set up a digest whose first remote response has valid length but the wrong hash.
        let temp = tempfile::tempdir().unwrap();
        let digest = Digest::for_bytes(b"expected");
        let (cache, fake_blob_store, _runtime) =
            cache(&temp, [(digest.clone(), Bytes::from_static(b"notright"))]);

        // The malformed response fails integrity verification and its temporary file is removed.
        assert!(matches!(
            cache.read_blob(&digest),
            Err(SessionError::InternalError { .. })
        ));
        assert!(!cache.path(&digest).exists());
        assert_eq!(
            fs::read_dir(cache.path(&digest).parent().unwrap())
                .unwrap()
                .count(),
            0
        );

        // A source-preserving remote error also leaves no entry, after which valid bytes retry successfully.
        *fake_blob_store.failure.lock().unwrap() = Some(CasError::BlobStatus {
            operation: CasOperation::ByteStreamRead,
            digest: digest.clone(),
            message: "test stream failure".to_string(),
        });
        assert!(matches!(
            cache.read_blob(&digest),
            Err(SessionError::InternalError { .. })
        ));
        assert!(!cache.path(&digest).exists());
        fake_blob_store
            .blobs
            .lock()
            .unwrap()
            .insert(digest.clone(), Bytes::from_static(b"expected"));
        assert!(cache.read_blob(&digest).unwrap().1);
    }

    #[test]
    fn no_clobber_admission_preserves_an_existing_entry() {
        // Set up a pending blob and independently admit the same valid object first.
        let temp = tempfile::tempdir().unwrap();
        let digest = Digest::for_bytes(b"same");
        let (cache, _fake_blob_store, _runtime) =
            cache(&temp, [(digest.clone(), Bytes::from_static(b"same"))]);
        let mut pending = cache.start_pending_blob(&digest).unwrap();
        pending.write_all(b"same").unwrap();
        let destination = cache.path(&digest);
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        fs::write(&destination, b"same").unwrap();

        // This private admission check is intentional: it verifies the atomic no-clobber invariant.
        cache.admit_pending_blob(pending).unwrap();
        assert_eq!(fs::read(&destination).unwrap(), b"same");
    }
}
