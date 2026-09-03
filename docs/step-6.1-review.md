# Step 6.1 review responses

Reviewing commit: `8288552`

1. `docs/step-6.1-program-design.md:32`

   This means that adding, removing, or renaming a child makes the containing
   directory's serialized REAPI `Directory` digest stale. The transaction sets
   `directory_tree_dirty = 1` and clears `directory_remote_digest` on that
   directory, then does the same for each ancestor up to the root because each
   ancestor will eventually contain a changed child-directory digest. A
   cross-directory rename applies this to both containing-directory chains. It
   does not clear the directory row or its materialized children. I will reword
   the sentence to say this explicitly.

2. `docs/step-6.1-program-design.md:34`

   Yes, the symlink is in one immediate parent directory. "Parent directory
   chain" means that containing directory followed by its ancestors up to the
   root. A symlink has no standalone CAS object: its target and metadata are
   encoded directly in the containing `Directory`. Replacing it therefore
   changes that directory's digest, which in turn changes the digest referenced
   by its parent, and so on. I will use "the containing directory and its
   ancestors" instead.

3. `docs/step-6.1-program-design.md:48`

   Yes. The pinned `fuser` 0.14 `Filesystem::setattr` callback supplies mode,
   size, atime, mtime, and other attributes as independent `Option` arguments,
   so one callback can request more than one field change. Keeping one
   `MetadataUpdate` lets mode and mtime be validated and committed atomically.
   Step 6.1 handles the supported metadata-only fields; size joins the combined
   mutation path when truncate/copy-up is added in step 6.2. Separate public
   `update_mode` and `update_mtime` methods would lose that atomic combined-call
   shape, so I will retain the aggregate update type.

4. `docs/step-6.1-program-design.md:61`

   Yes. These are distinct session-domain failures required for precise POSIX
   mappings: `AlreadyExists` maps through `FilesystemError` to `EEXIST`,
   `DirectoryNotEmpty` to `ENOTEMPTY`, and `InvalidArgument` to `EINVAL`.
   `FilesystemError::InvalidArgument` already exists; the filesystem layer will
   gain the other two typed cases when writable operations are connected. None
   of the existing `SessionError` variants accurately represents these
   conditions—using `FailedPreconditionError` or `InternalError` would currently
   collapse them to `EIO`. I will add a comment beside these `SessionError`
   variants explaining that they preserve mutation failures for the later
   filesystem/errno mapping; they do not introduce FUSE types into `Session`.

5. `docs/step-6.1-program-design.md:94`

   The public signature and the existing materialization behavior do not
   change. The effective merged-view behavior becomes observable once mutations
   can create tombstoned history and a new visible row can reuse the same
   parent/name: lookup must select the one non-tombstoned row, return `NotFound`
   when only tombstones remain, and never return an arbitrary historical row.
   The implementation change is primarily in `SessionStore::child`, whose query
   becomes visibility-aware under the new partial unique index. I will label
   this as unchanged API with changed backing-store behavior rather than imply a
   new `Session` method contract.

6. `docs/step-6.1-program-design.md:105`

   As with lookup, the signature, ordering, and existing tombstone-filtering
   contract do not change. What becomes newly exercised is that a directory may
   contain tombstoned historical rows alongside one visible replacement with
   the same basename. Listing must omit all tombstones, emit the replacement
   exactly once, and preserve basename order. I will describe this as unchanged
   public behavior applied to the new lossless store representation.
