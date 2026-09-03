# Step 6.1 Overlay Index and Merged View Test Cases

## Scope

These cases verify namespace mutations and digest invalidation through the
`Session`, `SessionStore`, and `OverlayStore` APIs. FUSE callbacks, content
writes, copy-up, and snapshot upload are out of scope.

## Test conventions

- Use a fake `BlobStore` containing a remote tree with a regular
  file, a non-empty directory, an empty directory, and a symlink. Record every
  requested digest to observe directory materialization and file download.
- Give every remote file and directory a distinct digest. This makes an
  incorrect clear, reuse, or download observable.
- Each unit case calls only the API named by its section. `Session` cases may
  inspect requests recorded by the fake `BlobStore`; `SessionStore` cases do
  not open raw SQLite connections or call private decoders or transaction
  helpers.
- `OverlayStore` cases use `OverlayFileId`; they do not construct paths below
  `data/`.
- After a mutation failure, repeat component reads to verify that no partial
  row or dirty-state change is visible.

## Digest expectations

| Mutation | Inode file digest | Inode directory digest | Parent and ancestor directory digests |
| --- | --- | --- | --- |
| Create file, directory, or symlink | New file has none; new directory has none; symlink never has one | New directory is digest-free and dirty | Cleared |
| File chmod or mtime | Retained | Not applicable | Cleared |
| Directory chmod or mtime | Not applicable | Cleared after materialization | Cleared |
| Symlink chmod or mtime | No symlink digest exists | Not applicable | Cleared |
| File, directory, or symlink rename | Retained for file | Retained for moved directory | Cleared for source and destination chains |
| Unlink or rmdir | Retained on tombstoned row | Retained on tombstoned row | Cleared |
| Replace a symlink | No symlink digest exists | Not applicable | Cleared |
| Write or truncate a remote file | Deferred to step 6.2 | Not applicable | Deferred to step 6.2 |

This table is the test oracle for the digest rules in
[`technical-design.md`](technical-design.md). Directory tests materialize all
children before clearing a directory digest; file metadata and path mutations
retain the content digest; symlinks have no standalone digest.

## `Session` unit cases

### S61-SES-001 — Remote lookup remains visible

- Arrange an unloaded remote root containing all fixture node kinds.
- Call `lookup_child` for one name, then `list_directory`.
- Expect one root `Directory` read, every remote entry exactly once in basename
  order, and stable inode IDs.

### S61-SES-002 — Local file creation publishes data before visibility

- Call `create_file` below the remote root with explicit mode and mtime.
- Expect a new regular-file inode with size zero and the supplied metadata.
- Expect `read_range` to return empty bytes without a CAS file request.
- Expect lookup and listing to expose the same inode only after creation
  succeeds.

### S61-SES-003 — Local directory creation is immediately loaded

- Call `create_directory`, then list the returned inode.
- Expect an empty list without a CAS request and expect the new name to appear
  in its parent in basename order.

### S61-SES-004 — Local symlink creation stores the exact target

- Create relative, absolute, broken, and `..`-containing targets in separate
  table-driven runs.
- Expect each returned inode and subsequent lookup to preserve the exact target
  and target byte length without following it or requesting CAS content.

### S61-SES-005 — Visible local replacement wins over a tombstone

- Unlink a remote file, then create a local file with the same parent/name.
- Expect lookup and listing to return only the new inode.
- Expect the new inode ID to differ from the tombstoned remote inode ID.

### S61-SES-006 — Tombstone hides an entry

- Unlink a remote file and separately unlink a symlink.
- Expect lookup to return `NotFound` and listing to omit the name.
- Expect unrelated siblings to remain visible with stable IDs.

### S61-SES-007 — Empty remote directory removal loads before mutation

- Remove an initially unloaded empty remote directory.
- Expect its `Directory` blob to be requested before the mutation succeeds.
- Expect the directory to become invisible and its siblings to remain unchanged.

### S61-SES-008 — Non-empty directory removal is atomic

- Attempt to remove an initially unloaded non-empty remote directory.
- Expect its `Directory` blob to be requested, followed by
  `DirectoryNotEmpty`.
- Expect the directory and every child to remain visible with the same IDs.

### S61-SES-009 — Directory metadata update materializes first

- Apply a new mode and mtime to an initially unloaded remote directory.
- Expect one directory CAS read before the update returns.
- Expect the updated metadata and complete original child set to remain visible.
- Repeat the update and expect no second CAS read.

### S61-SES-010 — File metadata update avoids content copy-up

- Apply mode, mtime, and combined updates to a remote regular file.
- Expect updated effective metadata and unchanged file bytes on `read_range`.
- Expect the metadata operation to make no file-content CAS request; a later
  read may fetch the unchanged content.

### S61-SES-011 — Symlink metadata update changes only inline metadata

- Apply mode and mtime updates to a remote symlink.
- Expect the exact target and inode ID to remain unchanged and no standalone
  blob request for the symlink.

### S61-SES-012 — Metadata no-op remains clean

- Read effective metadata, then apply exactly those stored values.
- Expect the same inode result and no extra directory materialization or
  observable namespace change.

### S61-SES-013 — Rename preserves source inode identity

- Rename a remote file, remote directory, local file, and symlink in separate
  runs within one directory.
- Expect each old name to be absent, each new name to resolve to the original
  inode ID, and file or directory content to remain readable.

### S61-SES-014 — Cross-directory rename updates both views

- Move one source into a different loaded directory.
- Expect source-parent listing to lose the old name and destination-parent
  listing to gain the new name in stable order with the same inode ID.

### S61-SES-015 — Compatible rename replacement retains both identities

- Rename a file over a file and an empty directory over an empty directory.
- Begin with the remote destination directory unloaded and expect one CAS read
  to establish that it is empty before replacement.
- Expect the moved source inode at the destination name.
- Expect the replaced inode to be invisible through `Session` lookup and
  listing.

### S61-SES-016 — Incompatible rename replacement fails atomically

- Cover file over directory, directory over file, and directory over non-empty
  directory.
- Expect the documented kind or non-empty error.
- Expect both paths, inode IDs, and directory listings to remain unchanged.

### S61-SES-017 — Directory cycle is rejected

- Attempt to move a directory below itself and below a descendant.
- Expect `InvalidArgument` and an unchanged tree from root through the deepest
  child.

### S61-SES-018 — Concurrent create has one visible winner

- Start two creates for the same parent/name using distinct metadata.
- Expect exactly one success and one `AlreadyExists`.
- Expect lookup and listing to expose one inode.

### S61-SES-019 — Closed session rejects mutation

- Close a session, then attempt every step 6.1 mutation method.
- Expect `FailedPreconditionError` for each and no visible namespace change.

## `SessionStore` unit cases

### S61-STO-001 — Version-2 root state is valid

- Create a store and read root through `inode`.
- Expect inode `1`, remote root digest present, `directory_loaded = false`,
  `directory_tree_dirty = false`, and all non-directory fields null.

### S61-STO-002 — Remote materialization initializes clean kind state

- Materialize one file, directory, and symlink below root.
- Expect the file digest with no overlay path and clean content state.
- Expect the child-directory digest with unloaded and clean tree state.
- Expect the symlink target with no digest or dirty field.

### S61-STO-003 — Local kind creation initializes dirty state

- Create all local kinds through `create_local_child`.
- Expect the file to be overlay-backed, remote-digest-free, and content-dirty.
- Expect the directory to be loaded, remote-digest-free, and tree-dirty.
- Expect the symlink to contain only target and metadata kind fields.

### S61-STO-004 — Partial visible-name uniqueness preserves history

- Tombstone a remote row, then create a local row with the same parent/name.
- Expect `child` to return the local row.
- Expect `inode` to return both identities when addressed individually and
  `get_directory_children` to return the visible and tombstoned rows in stable
  name/inode order.

### S61-STO-005 — Two visible rows are rejected

- Attempt a second local create at an existing visible name.
- Expect `AlreadyExists` and no allocation or dirty-state change visible through
  component reads.

### S61-STO-006 — Child mutation clears the complete ancestor chain

- Build three loaded remote directory levels, then create a child in the
  deepest directory.
- Expect all three directory digests cleared and tree-dirty flags set.
- Expect sibling directory branches and their digests to remain clean.

### S61-STO-007 — Unloaded dirty ancestor is rejected

- Supply a mutation whose parent chain contains an unloaded remote directory.
- Expect a precondition/invariant error and no new row, digest clear, or dirty
  flag. This proves `Session` must materialize before calling the mutation API.

### S61-STO-008 — Directory metadata clears self and ancestors

- Update mode or mtime on a loaded clean remote directory.
- Expect its own digest and every ancestor digest cleared in the same result.
- Expect its child rows and loaded state retained.

### S61-STO-009 — File metadata retains content backing

- Update a remote file's mode and mtime.
- Expect the same `file_remote_digest`, no overlay path, clean file-content
  state, and cleared parent/ancestor directory digests.

### S61-STO-010 — Symlink metadata invalidates only directories

- Update a symlink's metadata.
- Expect the exact target and absence of per-symlink digest fields to remain,
  with parent/ancestor directory digests cleared.

### S61-STO-011 — Effective metadata no-op does not dirty

- Apply absent update values and separately apply values equal to the stored
  fields.
- Expect an unchanged inode, directory digests, and dirty flags.

### S61-STO-012 — Tombstone commits with dirty propagation

- Unlink a file and rmdir an empty directory in separate runs.
- Expect the target tombstone and parent-chain digest clears to become visible
  together, never separately.

### S61-STO-013 — Removal failures change nothing

- Cover missing name, unlink of a directory, rmdir of a file, and non-empty
  rmdir.
- Expect the specific error and byte-for-byte equivalent component-level inode
  values before and after the call.

### S61-STO-014 — Rename retains reusable source digest

- Rename a remote file and a remote directory in separate runs.
- Expect the original inode ID plus original file-content or child-directory
  digest on the moved row.
- Expect source/destination parent chains cleared and dirty.

### S61-STO-015 — Rename dirties the union of both ancestor chains

- Move a child between cousins that share an ancestor.
- Expect every unique directory in the two paths dirty, the common path updated
  once, and unrelated branches untouched.

### S61-STO-016 — Rename replacement is one transaction

- Rename over compatible visible destinations.
- Expect the destination tombstone, moved source, and dirty chains together.
- Inject a transaction failure and expect the original two visible rows and
  clean state to remain.

### S61-STO-017 — Directory rename safety checks are atomic

- Cover descendant cycles and non-empty directory replacement.
- Expect no source movement, destination tombstone, digest clear, or dirty flag.

### S61-STO-018 — Referenced overlay set includes tombstones

- Create two local files and tombstone one.
- Expect `referenced_overlay_files` to return both identities so step 6.1 does
  not collect bytes that a future open handle may still use.

### S61-STO-019 — Concurrent mutations serialize complete transactions

- Run independent creates in different directories and competing operations in
  one directory.
- Expect all successful operations represented completely, conflicts reported
  deterministically by domain error, unique inode IDs, and valid dirty chains.

## `OverlayStore` unit cases

### S61-OVL-001 — Open creates the fixed layout idempotently

- Open the same root twice.
- Expect usable `data/` and `tmp/` directories and no data entries.

### S61-OVL-002 — Empty file is atomically published

- Call `create_empty_file`, then `read_range` at offsets zero and beyond EOF.
- Expect a unique valid identity and empty bytes from both reads.
- Expect no temporary entry to remain after success.

### S61-OVL-003 — Published identities never collide

- Create many files sequentially and concurrently.
- Expect distinct `OverlayFileId` values and no replacement of earlier files.

### S61-OVL-004 — Stored identity validation confines reads

- Cover empty, absolute, parent-component, nested, and non-UTF-8 paths.
- Expect validation errors before any out-of-root file is opened.

### S61-OVL-005 — Orphan discovery is sorted and non-destructive

- Publish three files and pass two as referenced.
- Expect the third identity only, stable ordering on repeated calls, and all
  three files still readable afterward.

### S61-OVL-006 — Temporary files are outside orphan discovery

- Arrange an interrupted temporary alongside a published orphan using the
  component's failure harness.
- Expect only the published data identity from `unreferenced_files`.

### S61-OVL-007 — Publication failures leave no visible partial file

- Inject failures at create, sync, and rename boundaries.
- Expect contextual errors, no returned identity, and no partially published
  data entry. Best-effort temporary cleanup is asserted where the injected
  filesystem operation permits it.

## Overlay integration cases

### S61-INT-001 — Core mutations survive later reads

- Open a session over the remote fixture; create all local kinds, update remote
  metadata, tombstone entries, and rename across directories.
- Re-read every affected directory and inode through `Session`.
- Expect the final merged workspace, stable inode identities, exact metadata,
  and local empty-file reads described above.

### S61-INT-002 — Failed database commit cannot publish a namespace entry

- Inject a SQLite commit failure after `OverlayStore` has published the empty
  file for `Session::create_file`.
- Expect the create error, absent lookup/listing name, and unchanged parent
  inode result.
- Pass `SessionStore::referenced_overlay_files` to
  `OverlayStore::unreferenced_files` and expect the published file as an orphan.

### S61-INT-003 — Retained inspection is read-only after mutations

- Mutate and close a session, then record the database and overlay data files.
- Run `Session::inspect` one or more times.
- Expect retained session metadata and identical database and overlay bytes.
  Do not treat the retained overlay as resumable session state.

### S61-INT-004 — A new mount discards retained overlay but reuses cache

- After S61-INT-003, open a new session under the same `RFS_HOME` and original
  root digest.
- Expect the prior session database and overlay namespace to be replaced, the
  original remote names to reappear, and already verified cache blobs to remain
  reusable per the fresh-session policy.

### S61-INT-005 — Concurrent merged operations remain internally consistent

- Run directory materialization, local creates, lookups, listings, metadata
  updates, and independent renames from several threads.
- Expect no duplicate visible names, partial listings, invalid backing
  combinations, or dirty directory with a retained stale digest.

## Step 6.2 handoff

Copy-up coverage belongs to step 6.2 as specified in
[`implementation-plan.md`](implementation-plan.md#step-62-whole-file-copy-on-write).
