# Step 4.1 Design: Local State and SQLite Session Store

## Purpose

Step 4.1 establishes the durable local state used by the later daemon, FUSE, overlay, and control-socket work. It does not mount FUSE, serve the control API, fetch from CAS, or make the workspace writable. Its outcome is a foreground `rfsd` process that exclusively owns one local session, a migrated SQLite metadata database, stable cache and overlay paths, inspectable clean-session metadata, and a guarded full-reset implementation for `rfs cleanup`.

## Scope and non-goals

In scope:

- Resolve one configurable `RFS_HOME` and create its fixed state layout.
- Derive safe cache paths from the validated SHA-256 `Digest` type.
- Acquire and retain one exclusive active-session lock.
- Distinguish absent, live, cleanly closed, and stale/malformed session state.
- Create, migrate, and close `session.db`.
- Persist the minimal session metadata needed for lifecycle checks and inspection.
- Wire the state owner into foreground `rfsd`.
- Implement `rfs cleanup` as a guarded full local reset.

Out of scope:

- Background daemon startup and `rfs mount` integration.
- Control socket creation and RPC handling (Step 4.2).
- FUSE mounting, CAS reads, cache population/verification, and cache eviction policy.
- Overlay mutations, inode allocation, remote-directory materialization, counters, snapshot barriers, and snapshotting. Their SQLite tables arrive with their first consumers in later steps.
- Recovering or resuming an interrupted session. Unclean state requires `rfs cleanup`.
- Adversarial concurrency and sudden-power-loss durability. The lock provides cooperative single-owner sanity checks and SQLite provides process-crash transaction consistency.

## Configuration and filesystem layout

`RFS_HOME` is the only local-state path setting. If it is unset, it defaults to `$HOME/.rfs`. Remove `RFS_CACHE_DIR`, `RFS_SESSION_DIR`, `--cache-dir`, and `--session-dir`; users who need another filesystem can set or symlink `RFS_HOME`.

If `RFS_HOME` does not exist, create it privately and then canonicalize it. An existing `RFS_HOME` may itself be a symlink; resolve it once to an absolute canonical directory before deriving any child path. A dangling symlink or non-directory is an error.

```text
RFS_HOME/
  active.lock
  cache/
    blobs/
      ab/
        <64-lowercase-hex>-<size>
    dirs/
      ab/
        <64-lowercase-hex>-<size>
  active/
    session.db
    rfsd.log
    overlay/
      data/
      tmp/
```

`StatePaths` is the only module that constructs these paths. Before using or cleaning the home, validate only its direct children: `active.lock` may be a regular file, and `cache` and `active` may be real directories. Any other direct child, symlink, or expected entry of the wrong type causes the operation to stop with manual-inspection guidance. Do not attempt ownership-marker or old-layout migration in the MVP.

Create directories with mode `0700` and regular files with mode `0600`, without relying solely on umask. Existing expected directories must not be group- or world-writable, and existing files must be owned by the effective user. Reject unsafe state rather than repairing it automatically.

### Cache entry paths

For a validated `Digest { hash, size_bytes }`:

```text
blob_path = RFS_HOME/cache/blobs/<hash[0..2]>/<hash>-<size_bytes>
dir_path  = RFS_HOME/cache/dirs/<hash[0..2]>/<hash>-<size_bytes>
```

`Digest` is the validation boundary: its fields are not directly constructible outside the digest module. Cache path helpers therefore accept `&Digest` and are infallible. Directory cache files contain exact serialized REAPI `Directory` bytes. Cache admission and validation remain owned by the later read path.

Startup and automatic closed-session replacement inspect only the cache directory itself, never its shards or entries. Verified cache entries remain trusted after admission.

### Session layout validation

For automatic replacement of a clean session, validate only the fixed session structure:

- `session.db` and `rfsd.log` are regular files.
- `overlay`, `overlay/data`, and `overlay/tmp` are real directories.
- Later fixed artifacts, such as `control.sock`, are added to this shallow validation with their implementing migration/step.

Do not enumerate or validate files beneath `overlay/data`, `overlay/tmp`, or the cache. Removal deletes these trees without following symlinks. `overlay/data` will contain copied-up and new file contents; `overlay/tmp` is only for temporary overlay creation/copy-up. Cache-download temporaries must be created in the target cache filesystem so verified admission can use atomic rename.

## State module boundary

The state module exposes a small synchronous API. `rusqlite` is blocking, so later async daemon code invokes it from daemon-owned blocking sections.

```rust
pub struct StatePaths { /* canonical RFS_HOME and fixed child paths */ }
pub struct SessionLock { /* open active.lock descriptor; unlocks on Drop */ }
pub struct SessionStore { /* writable Connection + paths + retained lock */ }

impl StatePaths {
    pub fn from_config(config: &Config) -> Result<Self, StateError>;
    pub fn blob_cache_path(&self, digest: &Digest) -> PathBuf;
    pub fn directory_cache_path(&self, digest: &Digest) -> PathBuf;
    pub fn cleanup(&self) -> Result<(), StateError>;
}

impl SessionStore {
    pub fn create(paths: StatePaths, startup: SessionStartup) -> Result<Self, StateError>;
    pub fn paths(&self) -> &StatePaths;
    pub fn close_cleanly(self) -> Result<(), StateError>;
}
```

Names may differ, but these boundaries matter:

- `SessionStore` is the only session owner. It holds both the writable SQLite connection and the lock for its lifetime.
- There is no public resume or general-purpose `open_existing` API in this step.
- Stale status later uses a narrow read-only metadata helper after acquiring the lock; it is not a second session abstraction.
- No public inspection result may be acted upon after releasing the lock. Inspection, deletion, replacement, and creation happen while the same lock descriptor remains held.

## Session lock and lifecycle

### Stable lock

`RFS_HOME/active.lock` is outside the removable `active/` tree. Open it read/write, acquire non-blocking `flock(LOCK_EX | LOCK_NB)`, and retain its descriptor for the full daemon lifetime. Linux `flock` is available through `libc`; do not add another locking crate.

The JSON record is diagnostic only:

```json
{
  "record_version": 1,
  "session_id": "<uuid-v4>",
  "pid": 1234
}
```

After acquiring the lock, truncate and rewrite the record before publishing the session. No explicit `sync_all` durability protocol is required. The advisory lock alone determines liveness; PID existence is never consulted.

- Held lock with invalid/unreadable JSON: report a live possible owner with unavailable diagnostics; mount and cleanup refuse.
- Available lock with invalid/unreadable JSON: state is malformed; automatic startup replacement refuses, while explicit cleanup may proceed if the top-level layout is otherwise safe.

Every operation resolves and validates `RFS_HOME`, opens/acquires or probes `active.lock`, and retains the descriptor while inspecting or changing `active/`. Dedicated adversarial race handling is out of scope; unexpected lock/filesystem conflicts must stop without knowingly overwriting state and explain that another RemoteFS operation may be active.

### Startup

Before creating state, foreground `rfsd` must parse the root digest and require the mountpoint to exist as a directory. Canonicalize the mountpoint to an absolute path, resolving symlinks, and store that exact path. Optional mountpoint arguments to later `status`, `snapshot`, and `unmount` commands use the same canonicalization before comparison.

Startup ordering:

1. Resolve/canonicalize configuration and validate the direct `RFS_HOME` layout.
2. Validate the root digest and canonicalize the existing mountpoint.
3. Open/create and acquire `active.lock`.
4. If `active/` is absent, continue. If it is confidently cleanly closed, remove only `active/`. Otherwise fail with an `rfs cleanup` remedy.
5. Create `cache/blobs`, `cache/dirs`, and the fixed active-session layout with private permissions.
6. Create/open `session.db`, apply migrations, and insert metadata in state `initializing`.
7. Create a fresh `rfsd.log`, establish foreground daemon ownership, and transactionally change state to `active`.
8. Return one `SessionStore` retaining the lock and writable connection.

If an action after active-state creation begins fails, release resources but leave `active/` as stale inspectable state. Do not automatically remove failed or partially initialized state.

Automatic replacement is allowed only when all of these hold while the lock is retained:

- Lock JSON is valid and supported.
- `session.db` opens read-only with the supported schema and exactly one valid metadata row.
- State is `closed` and the closed timestamp is present.
- Lock and database session IDs match.
- The fixed active layout passes shallow type and permission checks.

Any failed check requires explicit cleanup. Automatic replacement preserves `cache/`.

### Shutdown and logging

Each session creates a fresh `active/rfsd.log` with mode `0600`; there is no rotation in the MVP. Failures before `active/` exists go to stderr. Later failures are logged when possible and leave stale state.

On clean shutdown, write the final log event, transactionally set state to `closed` with its closed timestamp, close SQLite, and release the lock. If the database update fails, the session is not clean and the next startup requires cleanup. A clean session remains under `active/` for status inspection until the next startup replaces it.

### Full cleanup

`rfs cleanup` is an explicit full reset, not cache pruning:

1. Resolve and validate the top-level home layout.
2. Acquire `active.lock`; refuse if another process holds it.
3. Remove `active/` and `cache/` wholesale without following symlinks.
4. Unlink `active.lock` last, then release its descriptor.
5. Leave the canonical `RFS_HOME` directory empty.

Cleanup succeeds when these recognized entries are absent. It may remove malformed or partial contents below a valid top-level layout, but refuses if `RFS_HOME` has unknown direct children or recognized children of the wrong type. It never recursively validates cache or overlay contents before deletion.

## Session metadata and lifecycle

`session.db` is the durable source of session metadata. The lock JSON is not a second metadata store. Lifecycle values are:

- `initializing`: initial metadata transaction is complete but daemon startup is not.
- `active`: daemon startup completed and the owner retains the lock.
- `closed`: clean shutdown committed before releasing the lock.

An available lock with `initializing`, `active`, an unknown state, missing data, or malformed data is stale and requires cleanup. A held lock is live regardless of the stored state. Later FUSE/control readiness belongs in separate status fields or live control responses, not this lifecycle column.

The metadata stores only session ID, daemon PID, lifecycle state, root digest hash/size, canonical mountpoint, and created/closed timestamps. Cache and session paths are derived from canonical `RFS_HOME` and are not duplicated in SQLite.

## SQLite database

Use SQLite's rollback journal, foreign keys, and a short busy timeout suitable for same-process blocking sections. WAL is deferred until a real concurrent database reader requires it. Migrations are transactional: each migration advances `PRAGMA user_version` in the same transaction. Do not duplicate the database version in a metadata column.

Use signed integer seconds plus normalized nanoseconds for timestamps, and separate digest hash and size columns.

### Version 1 schema

Schema v1 contains only metadata. Inodes and remote directories arrive in Step 5.1; overlay state arrives in Step 6.1; counters arrive with observability.

```sql
CREATE TABLE session_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    session_id TEXT NOT NULL CHECK (length(session_id) > 0),
    daemon_pid INTEGER NOT NULL CHECK (daemon_pid > 0),
    state TEXT NOT NULL CHECK (state IN ('initializing', 'active', 'closed')),
    root_digest_hash TEXT NOT NULL
        CHECK (length(root_digest_hash) = 64
               AND root_digest_hash NOT GLOB '*[^0-9a-f]*'),
    root_digest_size INTEGER NOT NULL CHECK (root_digest_size >= 0),
    mountpoint TEXT NOT NULL CHECK (length(mountpoint) > 0),
    created_at_seconds INTEGER NOT NULL,
    created_at_nanos INTEGER NOT NULL
        CHECK (created_at_nanos BETWEEN 0 AND 999999999),
    closed_at_seconds INTEGER,
    closed_at_nanos INTEGER
        CHECK (closed_at_nanos IS NULL
               OR closed_at_nanos BETWEEN 0 AND 999999999),
    CHECK ((state = 'closed'
            AND closed_at_seconds IS NOT NULL
            AND closed_at_nanos IS NOT NULL)
           OR
           (state != 'closed'
            AND closed_at_seconds IS NULL
            AND closed_at_nanos IS NULL))
);
```

Readers validate the same invariants and require exactly the singleton row. Power-loss persistence is best effort: SQLite transactions provide logical consistency, but Step 4.1 does not implement directory `fsync` or claim that the latest metadata/log event survives sudden power loss.

## Error behavior

- Unsafe, unknown, or wrong-type top-level state: refuse with the offending path and manual-inspection guidance.
- Exclusive lock held elsewhere: `ActiveSession`, including parsed diagnostics when available and otherwise explaining that another daemon may own the home.
- Unlocked unclean or malformed active state: `StaleSession` with an `rfs cleanup` remedy.
- Unsupported database or lock-record version: malformed/stale state; never migrate or delete it automatically.
- SQLite or filesystem errors retain the target path and underlying source.
- Cleanup with no recognized state is successful and idempotent.

No Step 4.1 error may overwrite an unclean session, follow a state symlink, or knowingly start a second owner.

## Test design

Add `task test:integration:state`. Unit tests remain in `src/state.rs`; process/filesystem lifecycle tests live under `tests/`.

Unit coverage:

- Default and overridden `RFS_HOME` produce the expected canonical fixed paths; removed cache/session overrides are not accepted.
- A symlinked `RFS_HOME` resolves correctly; dangling or non-directory homes fail.
- Unknown direct children, wrong entry types, unsafe permissions, and wrong ownership fail closed.
- Blob and directory cache paths use the expected shard and size for validated digests.
- Schema v1 migration and reopen are idempotent and validate metadata constraints.
- Mountpoint canonicalization resolves relative paths and symlinks and requires an existing directory.
- Clean-session recognition requires matching IDs, valid lock/database versions, closed state/timestamp, and the fixed shallow layout.

Integration coverage:

- Foreground `rfsd` creates and retains a session; a second daemon reports a possible active owner and does not alter it.
- Clean shutdown leaves inspectable closed state, and the next daemon removes only the old `active/` while retaining cache entries.
- Interrupted/initializing/malformed state blocks startup until cleanup.
- Cleanup refuses a held lock, fully resets recognized local state including cache, removes the lock last, and is idempotent afterward.
- Cleanup refuses unknown direct children and does not follow symlinks inside removable trees.
- A failed migration or metadata write leaves stale state and is never mistaken for a clean session.

The lock tests use separate processes or descriptors so they exercise real `flock` behavior. Dedicated adversarial race/retry tests are not required for the MVP.

## Completion checklist

Step 4.1 is complete when foreground `rfsd` validates its digest and canonical mountpoint, initializes and retains one session under canonical `RFS_HOME`, writes its log, and shuts down into inspectable `closed` state. A subsequent daemon automatically replaces only valid closed state while preserving cache. Unclean state requires explicit cleanup, and `rfs cleanup` safely returns recognized `RFS_HOME` contents to an empty state when no possible live owner holds the lock.
