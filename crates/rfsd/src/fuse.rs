//! Synchronous FUSE adapter for the read-only filesystem service.

use std::ffi::OsStr;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{self, SyncSender};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

    /// Unmounts synchronously so a successful control response means teardown completed.
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
        if offset < 0 {
            reply.error(EINVAL);
            return;
        }
        let node = match self.node(inode) {
            Ok(node) if node.kind == NodeKind::Directory => node,
            Ok(_) => {
                reply.error(ENOTDIR);
                return;
            }
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let inode_id = match InodeId::new(inode) {
            Ok(inode) => inode,
            Err(_) => {
                reply.error(EINVAL);
                return;
            }
        };
        let children = match self.filesystem.readdir(inode_id) {
            Ok(children) => children,
            Err(error) => {
                reply.error(errno_for_error(error));
                return;
            }
        };
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
        for (index, (entry_inode, kind, name)) in entries.enumerate().skip(start) {
            let next_offset = i64::try_from(index + 1).unwrap_or(i64::MAX);
            if reply.add(entry_inode, next_offset, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _request: &Request<'_>, inode: u64, flags: i32, reply: ReplyOpen) {
        if flags & libc::O_ACCMODE != libc::O_RDONLY
            || flags & (libc::O_APPEND | libc::O_TRUNC) != 0
        {
            reply.error(EROFS);
            return;
        }
        match self.node(inode) {
            Ok(node) if node.kind == NodeKind::File => reply.opened(0, 0),
            Ok(node) if node.kind == NodeKind::Directory => reply.error(EISDIR),
            Ok(_) => reply.error(EINVAL),
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
        let offset = match u64::try_from(offset) {
            Ok(offset) => offset,
            Err(_) => {
                reply.error(EINVAL);
                return;
            }
        };
        let inode = match InodeId::new(inode) {
            Ok(inode) => inode,
            Err(_) => {
                reply.error(EINVAL);
                return;
            }
        };
        let size = usize::try_from(size).expect("u32 read size fits usize on supported targets");
        match self.filesystem.read(inode, offset, size) {
            Ok(bytes) => reply.data(&bytes),
            Err(error) => reply.error(errno_for_error(error)),
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
}
