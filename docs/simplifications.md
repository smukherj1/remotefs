# Simplification opportunities

Reviewed against the current working tree, including the uncommitted changes,
the technical design, implementation plan, and Step 6.1 mini-designs. The
read-only milestone and 6.1.1 error mapping are implemented; the remaining
overlay mutations, copy-up, writable FUSE, and snapshot work are still ahead.
These are recommendations, in decreasing order of importance, not implemented
changes. Findings come from source inspection; tests were not run for this
document.

## 1. Give daemon teardown one owner and one completion condition

Evidence: [control_service.rs](../crates/rfsd/src/control_service.rs),
particularly `ControlService::unmount` and `serve`, repeats mount teardown and
socket removal across the RPC, listener-failure, and server-exit paths. The RPC
signals shutdown and returns success, while `Session::close` happens afterward
in `serve`. Consequently, a successful response does not establish that the
durable session close succeeded. `Session::close` is currently one-shot, despite
the technical design describing an idempotent close.

Introduce one daemon-owned teardown operation that coordinates taking the mount,
joining FUSE, closing the session, and removing the socket. Successful unmount
must wait for the required work to finish. Server exit and startup failure should
use the same cleanup logic for whatever resources were acquired. Keep the
one-shot database transition if desired; the coordinator can record completion
and avoid repeating it. Preserve the original error if subsequent cleanup also
fails.

Test the control-service boundary: success means retained state is already
closed; a close failure cannot produce success; concurrent requests cannot tear
down twice; signal-driven shutdown also closes state. Do not duplicate lifecycle
row tests here. This becomes more valuable when writable handles and snapshot
work must also finish or be rejected during shutdown.

## 2. Stop constructing new remote children as invalid stored inodes

Evidence: `remote_file`, `remote_directory`, and `remote_symlink` in
[session.rs](../crates/rfs-common/src/session.rs) construct the full private
store inode with `InodeId::INVALID`, no parent, and many unrelated nullable
fields. In [store.rs](../crates/rfs-common/src/session/store.rs),
`validated_remote_dir_child_inodes_for_creation` checks those placeholders,
clones each child, fills its parent, and validates it. `insert_inode` repeats
creation validation.

Use a small private remote-child input containing name, optional metadata, and
kind-specific remote content. Let the store supply parent and allocated identity
when inserting. This removes sentinel-ID and caller-supplied-parent cases from
the remote-materialization interface, repeated field initialization, and the
intermediate cloned vector. Step 6.1.4 already plans a `NewInode` input for local
creation; keep remote and local inputs consistent without conflating their
different backing rules.

Retain raw SQLite row decoding and validation: persisted values still need to
be checked, and absent mode/mtime must remain distinguishable from effective
defaults. A wholesale rewrite of the SQL representation is unnecessary.

Keep store-boundary tests for stable allocation, idempotent materialization,
duplicate names, all-or-nothing insertion, and malformed persisted rows. Remove
input tests only when the new input type makes the invalid combination
unrepresentable. Session tests should verify decoded metadata and visible
children, not recreate the store's entire invalid-row matrix.

## 3. Organize tests around each module's own interface

Treat `pub(super)` methods on private components as their module boundary;
public-behavior testing does not require making these components crate-public
or moving every test into `tests/`. Private setup for corruption or fault
injection is useful when the operation and assertions exercise that boundary.

The clearest changes are:

| Current test or suite | Recommended change |
| --- | --- |
| Cache `failed_fill_keeps_the_digest_cohort_until_waiting_readers_leave` | Replace manual acquire/release and `Arc::ptr_eq` assertions with overlapping calls to `read_blob` through a controllable fake store. Check that a failed fill permits a successful retry without concurrent fills for that digest. |
| Cache `no_clobber_admission_preserves_an_existing_entry` | Race reads through two cache instances sharing a directory. Hold both remote streams at a barrier and verify successful reads and preservation of the first admitted file's identity. The current same-bytes assertion would also pass after an overwrite. |
| Filesystem `maps_every_existing_session_error_to_its_filesystem_category` | Keep coverage of the public `From<SessionError>` contract, but use compact input/expected cases instead of reproducing the production match in the test. Retain a focused context/source-preservation case. |
| `readonly_integration.rs` | Move synthetic failure and ordinary wrong-kind/missing-entry cases into local filesystem-service tests. Keep the real-CAS workflow for upload-to-session interoperability. Assert error category and useful context, not an exact number of nested `Context` variants. |
| Planned Step 6.1 tests | Let the store own atomicity, allocation, dirty ancestors, and visibility queries; Session own loading, overlay publication, and merged reads; filesystem own policy; FUSE own errno and kernel-visible behavior. Avoid repeating a complete mutation matrix at all four layers. |

Keep cache integrity, persistence validation, and transaction rollback coverage.
The managed-home trust policy does not make those tests irrelevant. Keep the
Cargo dependency-boundary test as an explicit architecture check; it enforces
a documented constraint that ordinary module behavior tests cannot prove.

## 4. Test CAS retry and streaming behavior through the client

Evidence: [cas.rs](../crates/rfs-common/src/cas.rs) directly tests private
timeout selection, backoff, retry classification, resource-name construction,
and verification helpers. These tests can pass while their orchestration is
wrong: `bytestream_read_into` retries opening the RPC, but errors or idle
timeouts while consuming the stream return directly. The plan requires
ByteStream retries to restart at byte zero.

Replace helper-by-helper coverage with a small scripted local gRPC server
driven through `CasClient` operations. Verify transient failure followed by
success, non-retryable failures, attempt exhaustion, stalled/failed streams,
resource namespace propagation, batch-budget behavior, and corrupt responses.
Use short configured timeouts and deterministic synchronization. Retain
constructor validation tests and real `bazel-remote` compatibility tests.

Resolve the streaming retry contract alongside this change: a caller-provided
`Write` cannot generally be rewound, so simply retrying the whole download into
the same destination would append duplicate prefixes. Any implementation must
define how a failed attempt is discarded before restarting. The cache's
temporary-file boundary is relevant, but do not add a second independent retry
policy there. Delete superseded private-helper tests only after client-level
coverage protects the corresponding behavior.

## 5. Remove the legacy test-only upload pipeline; extract reuse at the actual boundary

Evidence: [upload.rs](../crates/rfs-common/src/upload.rs) has test-only
`scan_local_directory`, `scan_local_tree`, and `upload_tree_error` helpers.
`scan_local_tree` repeats scan/hash/encode orchestration. Its error adapter even
turns unrelated upload failures into `TreeError::InvalidName`. The upload
deduplication test uses this alternate pipeline to derive the fake store's
expected objects from the same implementation it is testing.

Use an initially empty in-memory `BlobStore` that computes missing objects from
its own contents. Call `upload_local_directory`, inspect uploaded directories
and file bytes, then upload again and assert reuse. Add two files with identical
contents to make deduplication an actual exercised behavior. Scanner tests can
assert symlink preservation, unsupported-node errors, and warning counts through
this same public upload operation. Remove the alternate pipeline and error
adapter once their callers are replaced. Make `hash_files`, `encode_local_tree`,
and their intermediate types private where no other module consumes them.

Preserve deterministic coverage of a file changing size during upload; do not
replace its controlled setup with a timing-dependent concurrent write. Use a
narrow test synchronization hook if exercising that case through the public
operation requires one.

For Step 7.1, extract the existing digest deduplication / missing-object upload
logic from `upload_encoded_tree` into a capability accepting candidate `Blob`s.
Both traversal paths can use it. Keep local traversal separate from snapshot
traversal, and exclude unchanged snapshot digests from existence checks. The
shared `DirectoryBuilder` already supplies the encoder; another generic tree
walker would add complexity without removing a current duplicate.

## 6. Make download-lock cleanup automatic

Evidence: `ensure_cached`, `acquire_download_lock`, and
`release_download_lock` in
[cache.rs](../crates/rfs-common/src/session/cache.rs) manually track participant
counts and balance release on success and lock-poisoning paths. A cleanup error
can replace the original fill error. The file already notes the opportunity for
an RAII guard.

Use a private scoped registration whose drop releases participation, keeping
the gate alive for all waiting readers. Retain the recheck after locking and
remove a map entry only when no participant can still use that gate. Avoid
introducing a generic synchronization framework just to share a few lines with
Session's inode locks: their lifetimes and purposes differ.

Validate this through cache reads: one fill per digest, independent digests
progress concurrently, failures allow retry, and no partial bytes are served.
The boundary tests from item 3 should survive a change of lock implementation.

## 7. Centralize the small repeated parts of upcoming mutations

Evidence: the Step 6.1 mini-designs repeat directory materialization before a
transaction, lifecycle and visibility checks, and ancestor invalidation.
`SessionStore::with_transaction` already centralizes transaction mechanics, and
the planned `create_local_child` already shares file/directory/symlink insertion.
Preserve these existing simplifications.

As the first mutations are implemented, give ancestor invalidation one store
helper and share preparation only where multiple operations really need it.
Deduplicate overlapping ancestor chains for rename. Keep transaction-local
precondition checks in the store even when Session has checked earlier: remote
I/O between preparation and commit means earlier observations can become stale.
Keep fetch/copy work outside SQLite transactions and publication before metadata
commit. Do not build a generic command enum or mutation engine for the future
operations.

Test dirty propagation and rollback at the store boundary. At the Session
boundary, concentrate on lazy fetches, no file download for metadata-only
changes, visibility after commit, and readback. Snapshot/remount equivalence
belongs to the later snapshot workflow, not every mutation's unit suite.

## 8. Build CLI envelopes and daemon discovery once

Evidence: [cli.rs](../crates/rfs/src/cli.rs) constructs JSON independently for
mount, upload, active status, retained status, no-session status, and errors.
Success objects omit fields present in the error envelope. `run_status` repeats
the unavailable/fatal distinction around connect and status. The discovery
error path also reconstructs the obsolete `active/control.sock` path even
though Session owns the current layout.

Introduce one CLI-owned serializable envelope and keep command-specific data
separate. Compose connect-and-status into one result before selecting live
status versus retained inspection. Let Session's endpoint discovery supply the
path, including diagnostic context, rather than inventing a fallback layout.
Remove the unused `render_error` wrapper if no supported caller needs its
`"unknown"` command value.

Test parsed JSON shape, exit status, and output stream through the CLI boundary;
avoid tests for each formatting helper. Resolve the existing documentation
disagreement about JSON failure output on stdout versus stderr explicitly when
standardizing output. Keep protocol/semantic errors distinct from unavailable
daemon errors so a live daemon failure is not hidden by retained status.

## 9. Trim obsolete test scaffolding and repair suite ownership

Evidence and concrete follow-ups:

- [reapi.rs](../crates/rfs-common/src/reapi.rs) tests Prost encoding of manually
  assembled messages. Remove its duplicate empty-directory golden: the tree
  module already tests that through `DirectoryBuilder`. Replace the raw
  representative golden with a supported-metadata golden through the tree API;
  keep one authoritative canonical-encoding fixture instead of parallel tests
  of generated bindings.
- [CLI tests](../crates/rfs/tests/cli.rs) repeat removed-state-flag rejection
  already covered in `cli.rs`. Consolidate command-surface rejection cases at
  the CLI boundary. Move `test_rfsd_help` into the daemon package, which owns
  that executable.
- CAS integration suites repeat endpoint probes, and
  [cas_prerequisites.rs](../crates/rfs-common/tests/cas_prerequisites.rs) both
  tests reachability alone and probes before real operations. Keep clear
  prerequisite diagnostics around actual integration operations; remove the
  standalone reachability test and the Docker-CLI requirement from tests that
  only need a reachable CAS. Docker remains a requirement of `task cas:up`.
- [Taskfile.yml](../Taskfile.yml) routes `test:integration:session` to a missing
  `session_integration` test target. Restore the documented entry point by
  routing it to the owned lifecycle behavior suite or adding that suite, rather
  than creating duplicate tests simply to satisfy the name.

Keep real-CAS interoperability and FUSE end-to-end tests. They exercise external
boundaries that module-local fakes cannot cover. Consolidate shared fixture and
prerequisite setup within each package as repetition warrants it; no new test
framework or workspace package is needed.
