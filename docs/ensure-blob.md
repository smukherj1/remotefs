# Ensuring Blobs in the Local Cache

`FilesystemService::ensure_blob` is the single cache-admission boundary for
remote file and directory blobs. Its contract is:

1. Check the cache without acquiring a download lock and return `false` on a
   hit.
2. On a miss, acquire the digest-specific lock.
3. Recheck the cache and return `false` if another caller admitted the blob.
4. Otherwise stream the blob from CAS, verify and admit it, then return `true`.

The boolean reports whether that invocation performed the download; callers
use it to distinguish download counters from cache-hit counters.

Callers that have a remote digest invoke `ensure_blob` before reading local
content. Once it succeeds, session read APIs may rely on the blob being present
instead of repeating cache-miss and download control flow. The second check
under the digest lock is still required to coalesce concurrent misses.
