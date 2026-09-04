# Step 6.1 Test Case Index

## Purpose

Step 6.1 tests are specified beside the functionality they verify. This index
provides the execution order without maintaining a second, divergent test
specification.

## Test sequence

| Order | Test specification | Primary coverage |
| --- | --- | --- |
| 6.1.1 | [Existing session error mapping](step-6.1/01-existing-error-mapping.md#unit-tests) | Existing session-to-filesystem mappings and FUSE errno preservation. |
| 6.1.2 | [Visible child lookup](step-6.1/02-visible-child-lookup.md#unit-tests) | Visible-row selection by parent and name. |
| 6.1.3 | [Visible directory listing](step-6.1/03-visible-directory-listing.md#unit-tests) | Tombstone filtering and stable basename order. |
| 6.1.4 | [Create an empty file](step-6.1/04-create-file.md#unit-tests) | Schema version 2, dirty propagation, overlay publication, file creation, and orphan discovery. |
| 6.1.5 | [Create a directory](step-6.1/05-create-directory.md#unit-tests) | Loaded local directories and parent invalidation. |
| 6.1.6 | [Create a symlink](step-6.1/06-create-symlink.md#unit-tests) | Exact inline symlink targets and directory invalidation. |
| 6.1.7 | [Update inode metadata](step-6.1/07-set-metadata.md#unit-tests) | Effective no-ops and kind-specific digest retention. |
| 6.1.8 | [Unlink a non-directory](step-6.1/08-unlink.md#unit-tests) | Tombstone visibility, same-name recreation, and retained backing. |
| 6.1.9 | [Remove an empty directory](step-6.1/09-remove-directory.md#unit-tests) | Remote materialization, emptiness checks, and atomic failure. |
| 6.1.10 | [Rename an entry](step-6.1/10-rename.md#unit-tests) | Identity preservation, replacement, cycle checks, concurrency, and final merged-state integration. |

Each linked document also has an `Integration tests` subsection. Tests in a
later document assume all earlier mini-designs are present; no test requires a
future `Session` method.

## Common conventions

- Use a fake `BlobStore` whose remote tree has a regular file, a non-empty
  directory, an empty directory, and a symlink. Give every file and directory a
  distinct digest and record every requested digest.
- A component unit test calls only that component's API. `SessionStore` tests
  do not use raw SQLite connections, private row decoders, or transaction
  helpers. `OverlayStore` tests use `OverlayFileId`, not constructed paths
  beneath `data/`.
- After a failed mutation, read the same state again through component APIs and
  verify that no partial inode, tombstone, digest, or dirty-state change is
  visible.
- Integration tests use `Session` for namespace observations. A failure harness
  may inject overlay or SQLite boundary failures but may not inspect private
  state directly.

## Out of scope

FUSE mutation callbacks, file-content writes and copy-up, and snapshot upload
are covered by Steps 6.2, 6.3, and 7.1 respectively.
