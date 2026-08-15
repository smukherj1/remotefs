# Objective

Aggressively simplify the APIs between Session, ActiveSession, BlobCache and SessionStore.

# Data Model (SQL)

Session metadata singleton (unchanged)

A inode table containing:

- Inode ID
- Parent Inode ID: empty for root
- name
- kind: file, dir, symlink
- remote_digest: empty for inodes created directly in overlay
- overlay_file: location of overlay file if file was created locally or edited after download. For files that
  were modifed after download, both remote_digest and overlay_file will be populated.
- unix mode
- mtime columns
- tombstoned (whether inode was locally deleted)

Directory materialization (removed)

Session store need not store whether a blob was materialized. Session can check if the blob is present in the
cache instead.

# SessionStore

- Inode struct whose fields map 1:1 to the inode table. Use type safe enum for kind.
- Provide the minimum methods to create an inode or fetch an inode. Not aware of
  why the inode is being created or whether a blob is cached or needs materialization or
  anything else.
- Discuss every other method that you think with me and expect to be challenged aggressively.
- Don't add any methods we'll need in future implementation steps.

# ActiveSession (remove)

- Functionality is too small to keep. Fold into Session.

# BlobCache

- Keep unchanged

# OverlayStore

- Keep unchanged

# Session

Dependencies:

- SessionStore
- BlobCache
- OverlayStore
- BlobStore

Owns ensuring inode and blob digest transactions are concurrent safe.

Responsible for loading the state of an inode from db, checking if it's present in the
BlobCache and serving that if present. Otherwise, downloading the blob from BlobStore,
admitting it to the BlobCache and returning that.

# FileSystemService

Move BlobStore dependency into Session. No longer responsible for starting a download if session
reports the blob as not present.
