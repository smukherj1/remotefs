//! Synchronous FUSE adapter for the asynchronous read-only filesystem core.
//!
//! The kernel callbacks run on fuser's session thread and enter the daemon's
//! Tokio runtime only while a core operation is in flight. The mount is
//! reported ready only after the kernel has completed `FUSE_INIT`.

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
use rfs_common::cas::BlobStore;
use tokio::runtime::Handle;

use crate::fs::{FilesystemError, Node, NodeKind, ReadOnlyFilesystem};

const ATTRIBUTE_TTL: Duration = Duration::from_secs(1);

/// Owns a live kernel mount. Dropping it unmounts and joins the FUSE thread.
pub(crate) struct FuseMount {
    session: BackgroundSession,
}

impl FuseMount {
    /// Mounts the validated core and waits until the kernel finishes FUSE init.
    pub(crate) fn mount<S>(
        filesystem: Arc<ReadOnlyFilesystem<S>>,
        mountpoint: &Path,
        runtime: Handle,
    ) -> io::Result<Self>
    where
        S: BlobStore + Send + 'static,
    {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let adapter = FuseAdapter {
            filesystem,
            runtime,
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

struct FuseAdapter<S> {
    filesystem: Arc<ReadOnlyFilesystem<S>>,
    runtime: Handle,
    ready: Option<SyncSender<()>>,
}

impl<S: BlobStore + Send + 'static> FuseAdapter<S> {
    fn lookup_node(&self, parent: u64, name: &OsStr) -> Result<Node, i32> {
        let name = name.to_str().ok_or(ENOENT)?;
        self.runtime
            .block_on(self.filesystem.lookup(parent, name))
            .map_err(errno_for_error)
    }

    fn node(&self, inode: u64) -> Result<Node, i32> {
        self.runtime
            .block_on(self.filesystem.getattr(inode))
            .map_err(errno_for_error)
    }
}

impl<S: BlobStore + Send + 'static> Filesystem for FuseAdapter<S> {
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
        let children = match self.runtime.block_on(self.filesystem.readdir(inode)) {
            Ok(children) => children,
            Err(error) => {
                reply.error(errno_for_error(error));
                return;
            }
        };
        let entries = [
            (inode, FileType::Directory, ".".to_owned()),
            (node.parent, FileType::Directory, "..".to_owned()),
        ]
        .into_iter()
        .chain(
            children
                .into_iter()
                .map(|child| (child.inode, file_type(child.kind), child.name)),
        );
        for (index, (entry_inode, kind, name)) in entries.enumerate().skip(offset as usize) {
            if reply.add(entry_inode, (index + 1) as i64, kind, name) {
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
        match self
            .runtime
            .block_on(self.filesystem.read(inode, offset, size as usize))
        {
            Ok(bytes) => reply.data(&bytes),
            Err(error) => reply.error(errno_for_error(error)),
        }
    }

    fn readlink(&mut self, _request: &Request<'_>, inode: u64, reply: ReplyData) {
        match self.runtime.block_on(self.filesystem.readlink(inode)) {
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

fn file_attr(node: &Node) -> FileAttr {
    let size = match node.kind {
        NodeKind::File => node.digest.as_ref().map_or(0, |digest| {
            u64::try_from(digest.size_bytes()).expect("validated digest sizes are non-negative")
        }),
        NodeKind::Symlink => node
            .symlink_target
            .as_ref()
            .map_or(0, |target| target.len() as u64),
        NodeKind::Directory => 0,
    };
    let mtime = node
        .mtime
        .as_ref()
        .and_then(timestamp_to_system_time)
        .unwrap_or(UNIX_EPOCH);
    FileAttr {
        ino: node.inode,
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

fn permission_bits(node: &Node) -> u16 {
    let default = match node.kind {
        NodeKind::File => 0o444,
        NodeKind::Directory => 0o555,
        NodeKind::Symlink => 0o777,
    };
    u16::try_from(node.mode.unwrap_or(default) & 0o7777).unwrap_or(default as u16)
}

fn file_type(kind: NodeKind) -> FileType {
    match kind {
        NodeKind::File => FileType::RegularFile,
        NodeKind::Directory => FileType::Directory,
        NodeKind::Symlink => FileType::Symlink,
    }
}

fn timestamp_to_system_time(timestamp: &prost_types::Timestamp) -> Option<SystemTime> {
    let seconds = u64::try_from(timestamp.seconds).ok()?;
    let nanos = u32::try_from(timestamp.nanos).ok()?;
    (nanos < 1_000_000_000).then(|| UNIX_EPOCH + Duration::new(seconds, nanos))
}

fn errno_for_error(error: FilesystemError) -> i32 {
    match error {
        FilesystemError::UnknownInode { .. } | FilesystemError::NotFound { .. } => ENOENT,
        FilesystemError::WrongKind {
            expected: NodeKind::Directory,
            ..
        } => ENOTDIR,
        FilesystemError::WrongKind {
            actual: NodeKind::Directory,
            ..
        } => EISDIR,
        FilesystemError::WrongKind { .. } => EINVAL,
        FilesystemError::MissingDigest { .. }
        | FilesystemError::InvalidDigest { .. }
        | FilesystemError::Cas { .. }
        | FilesystemError::Directory { .. }
        | FilesystemError::DigestMismatch { .. }
        | FilesystemError::State { .. }
        | FilesystemError::StateTask { .. }
        | FilesystemError::Cache { .. } => EIO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_lookup_and_kind_errors_to_posix_errno() {
        assert_eq!(
            errno_for_error(FilesystemError::NotFound {
                parent: 1,
                name: "missing".to_owned(),
            }),
            ENOENT
        );
        assert_eq!(
            errno_for_error(FilesystemError::WrongKind {
                inode: 2,
                expected: NodeKind::Directory,
                actual: NodeKind::File,
            }),
            ENOTDIR
        );
        assert_eq!(
            errno_for_error(FilesystemError::WrongKind {
                inode: 2,
                expected: NodeKind::File,
                actual: NodeKind::Directory,
            }),
            EISDIR
        );
    }

    #[test]
    fn converts_remote_metadata_to_read_only_attributes() {
        let node = Node {
            inode: 9,
            parent: 1,
            name: "tool".to_owned(),
            kind: NodeKind::File,
            digest: Some(rfs_common::digest::Digest::for_bytes(b"abc")),
            mode: Some(0o100755),
            mtime: Some(prost_types::Timestamp {
                seconds: 123,
                nanos: 456,
            }),
            symlink_target: None,
        };
        let attr = file_attr(&node);
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.size, 3);
        assert_eq!(attr.perm, 0o755);
        assert_eq!(attr.mtime, UNIX_EPOCH + Duration::new(123, 456));
    }
}
