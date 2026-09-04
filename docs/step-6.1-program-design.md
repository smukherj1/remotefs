# Step 6.1 Overlay Index and Merged View Program Designs

## Purpose

Implement Step 6.1 as a sequence of independently buildable changes. Each
mini-program design changes one `Session` capability and includes the store,
overlay, error, unit-test, and integration-test work required by that change.

## Implementation order

The order is normative. Each design starts from the completed preceding design.

| Order | Mini-program design | `Session` change |
| --- | --- | --- |
| 6.1.1 | [Existing session error mapping](step-6.1/01-existing-error-mapping.md) | Align errors already returned by the read-only `Session` API. |
| 6.1.2 | [Visible child lookup](step-6.1/02-visible-child-lookup.md) | Update `Session::lookup_child` to select only the visible row. |
| 6.1.3 | [Visible directory listing](step-6.1/03-visible-directory-listing.md) | Update `Session::list_directory` to omit tombstones. |
| 6.1.4 | [Create an empty file](step-6.1/04-create-file.md) | Add `Session::create_file`. |
| 6.1.5 | [Create a directory](step-6.1/05-create-directory.md) | Add `Session::create_directory`. |
| 6.1.6 | [Create a symlink](step-6.1/06-create-symlink.md) | Add `Session::create_symlink`. |
| 6.1.7 | [Update inode metadata](step-6.1/07-set-metadata.md) | Add `Session::set_metadata`. |
| 6.1.8 | [Unlink a non-directory](step-6.1/08-unlink.md) | Add `Session::unlink`. |
| 6.1.9 | [Remove an empty directory](step-6.1/09-remove-directory.md) | Add `Session::remove_directory`. |
| 6.1.10 | [Rename an entry](step-6.1/10-rename.md) | Add `Session::rename`. |

## Incremental error rule

Design 6.1.1 replaces the existing catch-all filesystem/session boundary for
the `SessionError` variants that already exist. Later designs do not restate or
replace either complete enum. A later design adds a `SessionError` and its
matching `FilesystemError` only when the `Session` method introduced by that
design can produce that new category:

- 6.1.4 adds `SessionError::AlreadyExists` and
  `SessionError::InvalidArgument`, adds `FilesystemError::AlreadyExists`, and
  maps the already existing filesystem `InvalidArgument` variant from the new
  session category.
- 6.1.9 adds `DirectoryNotEmpty` for directory removal.
- Every other mini-design reuses the error categories available at that point.

This keeps the mirrored error boundary compiling and exhaustively mapped after
every incremental change.

## Test ownership

Unit and integration cases live in the mini-program design that first makes the
behavior possible. [`step-6.1-test-cases.md`](step-6.1-test-cases.md) is an
index; it no longer owns a separate test specification.

## Completion

Step 6.1 is complete only after all ten mini-program designs are implemented in
order and the final overlay integration workflow passes. File-content copy-up,
writable FUSE callbacks, and snapshot upload remain in later implementation
plan steps.
