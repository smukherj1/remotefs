//! Synchronous FUSE adapter for the read-only filesystem service.

use std::ffi::OsStr;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{self, SyncSender};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fuser::{
    BackgroundSession, FileAttr, FileType, Filesystem, KernelConfig, MountOption, ReplyAttr,
    ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request,
    TimeOrNow,
};
use libc::{EINVAL, EIO, EISDIR, ENOENT, ENOTDIR, EROFS};
use rfs_common::session::{Inode, InodeId, NodeKind};

use crate::filesystem::{FilesystemError, FilesystemService};

const ATTRIBUTE_TTL: Duration = Duration::from_secs(1);

/// Owns a live kernel mount. Dropping it unmounts and joins the FUSE thread.
pub(crate) struct FuseMount {
    session: BackgroundSession,
}

impl FuseMount {
    /// Mounts the validated core and waits until the kernel finishes FUSE init.
    pub(crate) fn mount(filesystem: Arc<FilesystemService>, mountpoint: &Path) -> io::Result<Self> {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let adapter = FuseAdapter {
            filesystem,
            ready: Some(ready_tx),
        };
        let session = fuser::spawn_mount2(
            adapter,
            mountpoint,
            &[
                MountOption::RO,
                MountOption::FSName("remotefs".to_owned()),
                MountOption::Subtype("remotefs".to_owned()),
                MountOption::DefaultPermissions,
                MountOption::NoDev,
                MountOption::NoSuid,
                MountOption::NoAtime,
            ],
        )?;
        ready_rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "kernel did not initialize FUSE mount `{}`: {error}",
                        mountpoint.display()
                    ),
                )
            })?;
        Ok(Self { session })
    }

    /// Unmounts and joins the FUSE thread, blocking until teardown completes.
    pub(crate) fn unmount(self) {
        self.session.join();
    }
}

struct FuseAdapter {
    filesystem: Arc<FilesystemService>,
    ready: Option<SyncSender<()>>,
}

impl FuseAdapter {
    fn lookup_node(&self, parent: u64, name: &OsStr) -> Result<Inode, i32> {
        let name = name.to_str().ok_or(ENOENT)?;
        let parent = InodeId::new(parent).map_err(|_| EINVAL)?;
        self.filesystem
            .lookup_dir_child(parent, name)
            .map_err(errno_for_error)
    }

    fn node(&self, inode: u64) -> Result<Inode, i32> {
        let inode = InodeId::new(inode).map_err(|_| EINVAL)?;
        self.filesystem.getattr(inode).map_err(errno_for_error)
    }

    /// Returns every readdir entry from `offset` onward as `(inode, next_offset, kind, name)`.
    ///
    /// Entry 0 is `.`, entry 1 is `..`, followed by the children in basename order. An
    /// entry's `next_offset` is its index plus one, so the kernel resumes after it.
    /// Fails with `EINVAL` for a negative offset or invalid inode, `ENOTDIR` for a
    /// non-directory, and the mapped filesystem errno otherwise.
    fn directory_entries(
        &self,
        inode: u64,
        offset: i64,
    ) -> Result<Vec<(u64, i64, FileType, String)>, i32> {
        if offset < 0 {
            return Err(EINVAL);
        }
        let node = self.node(inode)?;
        if node.kind != NodeKind::Directory {
            return Err(ENOTDIR);
        }
        let children = self
            .filesystem
            .readdir(node.inode)
            .map_err(errno_for_error)?;
        let entries = [
            (inode, FileType::Directory, ".".to_owned()),
            (node.parent.get(), FileType::Directory, "..".to_owned()),
        ]
        .into_iter()
        .chain(
            children
                .into_iter()
                .map(|child| (child.inode.get(), file_type(child.kind), child.name)),
        );
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        Ok(entries
            .enumerate()
            .skip(start)
            .map(|(index, (entry_inode, kind, name))| {
                let next_offset = i64::try_from(index + 1).unwrap_or(i64::MAX);
                (entry_inode, next_offset, kind, name)
            })
            .collect())
    }

    /// Admits only read-only opens of regular files.
    ///
    /// Write, append, and truncate flags fail with `EROFS` before the inode is looked
    /// up. A directory fails with `EISDIR`, a symlink with `EINVAL`, and lookup
    /// failures with their mapped errno.
    fn open_file(&self, inode: u64, flags: i32) -> Result<(), i32> {
        if flags & libc::O_ACCMODE != libc::O_RDONLY
            || flags & (libc::O_APPEND | libc::O_TRUNC) != 0
        {
            return Err(EROFS);
        }
        let node = self.node(inode)?;
        match node.kind {
            NodeKind::File => Ok(()),
            NodeKind::Directory => Err(EISDIR),
            NodeKind::Symlink => Err(EINVAL),
        }
    }

    /// Reads up to `size` bytes at `offset`; a negative offset or invalid inode is `EINVAL`.
    fn read_bytes(&self, inode: u64, offset: i64, size: u32) -> Result<Bytes, i32> {
        let offset = u64::try_from(offset).map_err(|_| EINVAL)?;
        let inode = InodeId::new(inode).map_err(|_| EINVAL)?;
        let size = usize::try_from(size).expect("u32 read size fits usize on supported targets");
        self.filesystem
            .read(inode, offset, size)
            .map_err(errno_for_error)
    }
}

impl Filesystem for FuseAdapter {
    fn init(&mut self, _request: &Request<'_>, _config: &mut KernelConfig) -> Result<(), i32> {
        if let Some(ready) = self.ready.take() {
            let _ = ready.send(());
        }
        Ok(())
    }

    fn lookup(&mut self, _request: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.lookup_node(parent, name) {
            Ok(node) => reply.entry(&ATTRIBUTE_TTL, &file_attr(&node), 0),
            Err(errno) => reply.error(errno),
        }
    }

    fn getattr(&mut self, _request: &Request<'_>, inode: u64, reply: ReplyAttr) {
        match self.node(inode) {
            Ok(node) => reply.attr(&ATTRIBUTE_TTL, &file_attr(&node)),
            Err(errno) => reply.error(errno),
        }
    }

    fn readdir(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        _handle: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let entries = match self.directory_entries(inode, offset) {
            Ok(entries) => entries,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        for (entry_inode, next_offset, kind, name) in entries {
            // A full reply buffer stops the listing; the kernel resumes from the last offset.
            if reply.add(entry_inode, next_offset, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _request: &Request<'_>, inode: u64, flags: i32, reply: ReplyOpen) {
        match self.open_file(inode, flags) {
            Ok(()) => reply.opened(0, 0),
            Err(errno) => reply.error(errno),
        }
    }

    fn read(
        &mut self,
        _request: &Request<'_>,
        inode: u64,
        _handle: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.read_bytes(inode, offset, size) {
            Ok(bytes) => reply.data(&bytes),
            Err(errno) => reply.error(errno),
        }
    }

    fn readlink(&mut self, _request: &Request<'_>, inode: u64, reply: ReplyData) {
        let inode = match InodeId::new(inode) {
            Ok(inode) => inode,
            Err(_) => {
                reply.error(EINVAL);
                return;
            }
        };
        match self.filesystem.readlink(inode) {
            Ok(target) => reply.data(target.as_bytes()),
            Err(error) => reply.error(errno_for_error(error)),
        }
    }

    fn setattr(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _handle: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        reply.error(EROFS);
    }

    fn write(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _handle: u64,
        _offset: i64,
        _data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        reply.error(EROFS);
    }

    fn mknod(
        &mut self,
        _request: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        reply.error(EROFS);
    }

    fn mkdir(
        &mut self,
        _request: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        reply.error(EROFS);
    }

    fn unlink(&mut self, _request: &Request<'_>, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(EROFS);
    }

    fn rmdir(&mut self, _request: &Request<'_>, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(EROFS);
    }

    fn symlink(
        &mut self,
        _request: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _target: &Path,
        reply: ReplyEntry,
    ) {
        reply.error(EROFS);
    }

    fn rename(
        &mut self,
        _request: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _new_parent: u64,
        _new_name: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(EROFS);
    }

    fn link(
        &mut self,
        _request: &Request<'_>,
        _inode: u64,
        _new_parent: u64,
        _new_name: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(EROFS);
    }

    fn create(
        &mut self,
        _request: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        reply.error(EROFS);
    }
}

fn file_attr(node: &Inode) -> FileAttr {
    let size = node.size;
    let mtime = timestamp_to_system_time(node.mtime);
    FileAttr {
        ino: node.inode.get(),
        size,
        blocks: size.div_ceil(512),
        atime: mtime,
        mtime,
        ctime: mtime,
        crtime: mtime,
        kind: file_type(node.kind),
        perm: permission_bits(node),
        nlink: if node.kind == NodeKind::Directory {
            2
        } else {
            1
        },
        // RemoteFS does not preserve ownership; mounts are owned by the daemon user.
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

fn permission_bits(node: &Inode) -> u16 {
    u16::try_from(node.mode & 0o7777).unwrap_or(0)
}

fn file_type(kind: NodeKind) -> FileType {
    match kind {
        NodeKind::File => FileType::RegularFile,
        NodeKind::Directory => FileType::Directory,
        NodeKind::Symlink => FileType::Symlink,
    }
}

fn timestamp_to_system_time(timestamp: rfs_common::session::NodeTime) -> SystemTime {
    if timestamp.seconds() >= 0 {
        UNIX_EPOCH
            + Duration::new(
                u64::try_from(timestamp.seconds()).expect("non-negative timestamp fits u64"),
                timestamp.nanos(),
            )
    } else if timestamp.nanos() == 0 {
        UNIX_EPOCH - Duration::from_secs(timestamp.seconds().unsigned_abs())
    } else {
        UNIX_EPOCH
            - Duration::new(
                timestamp.seconds().unsigned_abs() - 1,
                1_000_000_000 - timestamp.nanos(),
            )
    }
}

fn errno_for_error(error: FilesystemError) -> i32 {
    match error {
        FilesystemError::Context { source, .. } => errno_for_error(*source),
        FilesystemError::NotFound { .. } => ENOENT,
        FilesystemError::NotDirectory { .. } => ENOTDIR,
        FilesystemError::IsDirectory { .. } => EISDIR,
        FilesystemError::InvalidArgument { .. } => EINVAL,
        FilesystemError::InternalError { .. }
        | FilesystemError::FailedPreconditionError { .. }
        | FilesystemError::InvalidInode { .. } => EIO,
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::ffi::OsStrExt;

    use crate::filesystem::test_support::{FILE_CONTENTS, fixture_store, service};

    use super::*;

    #[test]
    fn converts_remote_metadata_to_read_only_attributes() {
        let node = Inode {
            inode: InodeId::new(9).unwrap(),
            parent: InodeId::ROOT,
            name: "tool".to_owned(),
            kind: NodeKind::File,
            size: 3,
            mode: 0o100755,
            mtime: rfs_common::session::NodeTime::new(123, 456).unwrap(),
            symlink_target: None,
        };
        let attr = file_attr(&node);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.size, 3);
        assert_eq!(attr.perm, 0o755);
        assert_eq!(attr.mtime, UNIX_EPOCH + Duration::new(123, 456));
    }

    /// Builds an adapter over the fixture; the returned guards keep the session alive.
    fn adapter() -> (
        FuseAdapter,
        std::sync::Arc<rfs_common::session::Session>,
        impl Sized,
    ) {
        let (store, root) = fixture_store();
        let (temp, runtime, session, service) = service(store, root);
        let adapter = FuseAdapter {
            filesystem: Arc::new(service),
            ready: None,
        };
        (adapter, session, (temp, runtime))
    }

    /// Returns the raw inode of `name` under the root.
    fn raw_child(adapter: &FuseAdapter, name: &str) -> u64 {
        adapter
            .lookup_node(InodeId::ROOT.get(), OsStr::new(name))
            .unwrap()
            .inode
            .get()
    }

    #[test]
    fn readdir_lists_dot_entries_then_children_and_resumes_from_offset() {
        // Open the fixture: root holds `dir`, `file.txt`, `link`; `dir` is empty.
        let (adapter, _session, _guards) = adapter();
        let root = InodeId::ROOT.get();
        let dir = raw_child(&adapter, "dir");

        // From offset 0 the root lists `.`, `..`, then children in name order with kinds.
        let entries = adapter.directory_entries(root, 0).unwrap();
        let summary: Vec<_> = entries
            .iter()
            .map(|(_, _, kind, name)| (name.as_str(), *kind))
            .collect();
        assert_eq!(
            summary,
            [
                (".", FileType::Directory),
                ("..", FileType::Directory),
                ("dir", FileType::Directory),
                ("file.txt", FileType::RegularFile),
                ("link", FileType::Symlink),
            ]
        );
        assert_eq!((entries[0].0, entries[1].0), (root, root));
        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(entry.1, i64::try_from(index + 1).unwrap());
        }

        // A subdirectory's `..` is its parent, and `.` is itself.
        let nested = adapter.directory_entries(dir, 0).unwrap();
        assert_eq!((nested[0].0, nested[1].0), (dir, root));
        assert_eq!(nested.len(), 2);

        // Resuming from each next offset yields exactly the remaining suffix.
        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(
                adapter.directory_entries(root, entry.1).unwrap(),
                entries[index + 1..]
            );
        }

        // An offset past the end lists nothing.
        assert!(adapter.directory_entries(root, 99).unwrap().is_empty());

        // Invalid requests fail: negative offset is EINVAL, a file is ENOTDIR.
        assert_eq!(adapter.directory_entries(root, -1).unwrap_err(), EINVAL);
        let file = raw_child(&adapter, "file.txt");
        assert_eq!(adapter.directory_entries(file, 0).unwrap_err(), ENOTDIR);
    }

    #[test]
    fn open_admits_only_read_only_file_opens() {
        // Open the fixture and pick a file, a directory, a symlink, and a missing inode.
        let (adapter, _session, _guards) = adapter();
        let file = raw_child(&adapter, "file.txt");
        let dir = raw_child(&adapter, "dir");
        let link = raw_child(&adapter, "link");
        let missing = 9999;

        // Each row is (inode, flags, expected result); write-style flags fail before lookup.
        let rows = [
            (file, libc::O_RDONLY, Ok(())),
            (file, libc::O_WRONLY, Err(EROFS)),
            (file, libc::O_RDWR, Err(EROFS)),
            (file, libc::O_RDONLY | libc::O_APPEND, Err(EROFS)),
            (file, libc::O_RDONLY | libc::O_TRUNC, Err(EROFS)),
            (missing, libc::O_WRONLY, Err(EROFS)),
            (dir, libc::O_RDONLY, Err(EISDIR)),
            (link, libc::O_RDONLY, Err(EINVAL)),
            (missing, libc::O_RDONLY, Err(ENOENT)),
        ];
        for (inode, flags, expected) in rows {
            assert_eq!(
                adapter.open_file(inode, flags),
                expected,
                "inode {inode} flags {flags:#x}"
            );
        }
    }

    #[test]
    fn inode_and_offset_arguments_are_validated_before_the_service() {
        // Open the fixture; inode 0 and negative offsets never reach the service.
        let (adapter, session, _guards) = adapter();
        let file = raw_child(&adapter, "file.txt");

        // Invalid arguments fail with EINVAL; a non-UTF-8 name is simply not found.
        assert_eq!(adapter.node(0).unwrap_err(), EINVAL);
        assert_eq!(adapter.directory_entries(0, 0).unwrap_err(), EINVAL);
        assert_eq!(adapter.read_bytes(0, 0, 1).unwrap_err(), EINVAL);
        assert_eq!(adapter.read_bytes(file, -1, 1).unwrap_err(), EINVAL);
        let bad_name = OsStr::from_bytes(&[0xff, 0xfe]);
        assert_eq!(
            adapter
                .lookup_node(InodeId::ROOT.get(), bad_name)
                .unwrap_err(),
            ENOENT
        );

        // A valid read succeeds, and a service failure after close reaches the kernel as EIO.
        assert_eq!(
            adapter.read_bytes(file, 0, 5).unwrap().as_ref(),
            &FILE_CONTENTS[..5]
        );
        session.close().unwrap();
        assert_eq!(adapter.node(InodeId::ROOT.get()).unwrap_err(), EIO);
    }
}
