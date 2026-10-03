//! Shared in-memory test doubles. Compiled only for tests or the `test-support` feature.

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::cas::{Blob, BlobContents, BlobStore, CasError, CasOperation, UploadStats};
use crate::digest::Digest;

/// Longest a test waits for a checkpoint event before panicking instead of hanging.
const HOLD_TIMEOUT: Duration = Duration::from_secs(10);

/// In-memory `BlobStore` whose behavior is configured per digest.
///
/// Clones share one state, so a test can keep a handle after boxing a clone into a
/// `Session` or `CachedBlobStore`.
#[derive(Clone, Default)]
pub struct InMemoryBlobStore {
    /// Entries by digest, shared by every clone.
    blobs: Arc<Mutex<HashMap<Digest, BlobEntry>>>,
}

/// What `stream_blob` returns for one digest.
#[derive(Clone)]
pub enum BlobResult {
    /// Write these bytes and succeed. The bytes may differ from the digest, so a
    /// test can serve a malformed blob.
    Contents(Bytes),
    /// Return a `CasError` without writing anything.
    Fail,
}

/// Counting rendezvous between store calls and a test.
///
/// Calls arrive in order. The n-th arrival waits until the test has called
/// `release` n times in total.
#[derive(Default)]
pub struct Checkpoint {
    /// Arrival and release counts.
    counts: Mutex<CheckpointCounts>,
    /// Signals every change to `counts`.
    changed: Condvar,
}

/// Configuration and observations for one digest.
struct BlobEntry {
    /// What the next `stream_blob` call returns once it reads the entry.
    result: BlobResult,
    /// Number of `stream_blob` calls made for this digest.
    streams: usize,
    /// When set, every `stream_blob` call waits here after it is counted and
    /// before it reads `result`.
    before_result: Option<Arc<Checkpoint>>,
}

/// Counts behind a `Checkpoint`.
#[derive(Default)]
struct CheckpointCounts {
    /// Number of calls that have reached the checkpoint.
    arrived: usize,
    /// Number of calls the test has let continue.
    released: usize,
}

impl InMemoryBlobStore {
    /// Creates a store with one `BlobResult::Contents` entry per pair, no holds, and
    /// zero stream counts.
    pub fn new(blobs: impl IntoIterator<Item = (Digest, Bytes)>) -> Self {
        let store = Self::default();
        store.lock().extend(blobs.into_iter().map(|(digest, bytes)| {
            (digest, BlobEntry::new(BlobResult::Contents(bytes)))
        }));
        store
    }

    /// Sets what later `stream_blob` calls for `digest` return. Creates the entry if it
    /// is absent, and keeps its stream count and checkpoint if it exists.
    ///
    /// A call already waiting at `before_result` reads the new result when released.
    pub fn set_result(&self, digest: Digest, result: BlobResult) {
        match self.lock().entry(digest) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().result = result;
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(BlobEntry::new(result));
            }
        }
    }

    /// Makes every later `stream_blob` call for `digest` wait at a new checkpoint, and
    /// returns that checkpoint. Replaces any earlier checkpoint for `digest`.
    ///
    /// Panics if `digest` has no entry. Calls already past the checkpoint stage are
    /// not affected.
    pub fn hold_streams(&self, digest: &Digest) -> Arc<Checkpoint> {
        let checkpoint = Arc::new(Checkpoint::default());
        self.lock()
            .get_mut(digest)
            .expect("hold_streams needs an existing entry")
            .before_result = Some(Arc::clone(&checkpoint));
        checkpoint
    }

    /// Returns how many `stream_blob` calls were made for `digest`, or 0 if it has
    /// no entry. A call is counted when it starts, before it waits at a checkpoint.
    pub fn streams(&self, digest: &Digest) -> usize {
        self.lock().get(digest).map_or(0, |entry| entry.streams)
    }

    /// Locks the shared entries, panicking if a test thread poisoned them.
    fn lock(&self) -> MutexGuard<'_, HashMap<Digest, BlobEntry>> {
        self.blobs.lock().expect("in-memory blob store state")
    }

    /// Counts a stream call for `digest` and returns its checkpoint, if any.
    /// Fails if `digest` has no entry.
    fn begin_stream(&self, digest: &Digest) -> Result<Option<Arc<Checkpoint>>, CasError> {
        let mut blobs = self.lock();
        let entry = blobs
            .get_mut(digest)
            .ok_or_else(|| Self::stream_error(digest, "blob is not stored"))?;
        entry.streams += 1;
        Ok(entry.before_result.clone())
    }

    /// Reads the current result for `digest`. Fails if the entry has disappeared.
    fn current_result(&self, digest: &Digest) -> Result<BlobResult, CasError> {
        self.lock()
            .get(digest)
            .map(|entry| entry.result.clone())
            .ok_or_else(|| Self::stream_error(digest, "blob is not stored"))
    }

    /// Builds the error returned for an injected or missing-blob failure.
    fn stream_error(digest: &Digest, message: &str) -> CasError {
        CasError::BlobStatus {
            operation: CasOperation::ByteStreamRead,
            digest: digest.clone(),
            message: message.to_owned(),
        }
    }
}

impl Checkpoint {
    /// Blocks until at least `count` calls have reached the checkpoint in total.
    ///
    /// Panics after `HOLD_TIMEOUT` (10 s) instead of hanging the test.
    pub fn wait_arrivals(&self, count: usize) {
        let counts = self.counts.lock().expect("checkpoint counts");
        let (_counts, timeout) = self
            .changed
            .wait_timeout_while(counts, HOLD_TIMEOUT, |counts| counts.arrived < count)
            .expect("checkpoint counts");
        assert!(!timeout.timed_out(), "checkpoint never reached {count} arrivals");
    }

    /// Lets one more call continue: the earliest arrival not yet released, or the
    /// next call to arrive if none is waiting. Never blocks.
    pub fn release(&self) {
        self.counts.lock().expect("checkpoint counts").released += 1;
        self.changed.notify_all();
    }

    /// Store side: records an arrival, then blocks until this arrival is released.
    ///
    /// Panics after `HOLD_TIMEOUT` if the test never releases it.
    fn arrive(&self) {
        let mut counts = self.counts.lock().expect("checkpoint counts");
        counts.arrived += 1;
        let position = counts.arrived;
        self.changed.notify_all();
        let (_counts, timeout) = self
            .changed
            .wait_timeout_while(counts, HOLD_TIMEOUT, |counts| counts.released < position)
            .expect("checkpoint counts");
        assert!(!timeout.timed_out(), "held stream was never released");
    }
}

impl BlobEntry {
    /// Creates an entry returning `result`, with no calls counted and no checkpoint.
    fn new(result: BlobResult) -> Self {
        Self {
            result,
            streams: 0,
            before_result: None,
        }
    }
}

#[async_trait]
impl BlobStore for InMemoryBlobStore {
    /// Reports digests that have no `BlobResult::Contents` entry.
    async fn find_missing_blobs(&self, digests: &[Digest]) -> Result<Vec<Digest>, CasError> {
        let blobs = self.lock();
        Ok(digests
            .iter()
            .filter(|digest| {
                !matches!(
                    blobs.get(*digest).map(|entry| &entry.result),
                    Some(BlobResult::Contents(_))
                )
            })
            .cloned()
            .collect())
    }

    /// Reads each blob's bytes and stores them as `Contents`. Returns `CasError::Io`
    /// if a file cannot be read; blobs before it stay stored.
    async fn upload_blobs(&self, blobs: Vec<Blob>) -> Result<UploadStats, CasError> {
        let mut stats = UploadStats::default();
        for blob in blobs {
            let bytes = match blob.contents {
                BlobContents::Bytes(bytes) => bytes,
                BlobContents::FilePath(path) => {
                    Bytes::from(std::fs::read(&path).map_err(|source| CasError::Io {
                        operation: CasOperation::BatchUpdateBlobs,
                        path,
                        source,
                    })?)
                }
            };
            stats.uploaded_blobs += 1;
            stats.bytes_uploaded += bytes.len() as u64;
            self.set_result(blob.digest, BlobResult::Contents(bytes));
        }
        Ok(stats)
    }

    /// Counts the call, waits at the digest's checkpoint if one is set, then returns
    /// the digest's current result. Missing digests fail without being counted.
    async fn stream_blob(
        &self,
        digest: &Digest,
        destination: &mut (dyn Write + Send),
    ) -> Result<(), CasError> {
        if let Some(checkpoint) = self.begin_stream(digest)? {
            checkpoint.arrive();
        }
        match self.current_result(digest)? {
            BlobResult::Fail => Err(Self::stream_error(digest, "injected stream failure")),
            BlobResult::Contents(bytes) => {
                destination
                    .write_all(&bytes)
                    .map_err(|source| CasError::StreamWrite {
                        digest: digest.clone(),
                        source,
                    })
            }
        }
    }
}
