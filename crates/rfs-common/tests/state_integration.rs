use std::fs;
use std::os::fd::AsRawFd;
use std::sync::{Arc, Barrier};

use rfs_common::config::Config;
use rfs_common::digest::Digest;
use rfs_common::session::{
    InodeId, NodeKind, RemoteChild, RemoteContent, Session, SessionLifecycle,
};

fn open_session(temp: &tempfile::TempDir, root: Digest) -> Arc<Session> {
    let mountpoint = temp.path().join("mount");
    if !mountpoint.exists() {
        fs::create_dir(&mountpoint).unwrap();
    }
    Arc::new(
        Session::open(
            Config {
                rfs_home: temp.path().join("home"),
            },
            root,
            mountpoint,
        )
        .unwrap(),
    )
}

#[test]
fn closed_session_is_replaced_but_cache_is_retained() {
    let temp = tempfile::tempdir().unwrap();
    let root = Digest::for_bytes(b"root");
    let session = open_session(&temp, root.clone());
    session.close().unwrap();
    session.close().unwrap();
    fs::write(temp.path().join("home/cache/retained"), b"data").unwrap();

    let replacement = open_session(&temp, root);
    replacement.close().unwrap();
    assert!(temp.path().join("home/cache/retained").exists());
    assert!(temp.path().join("home/active/session.db").exists());
}

#[test]
fn stale_session_is_preserved_and_requires_manual_deletion() {
    let temp = tempfile::tempdir().unwrap();
    let root = Digest::for_bytes(b"root");
    drop(open_session(&temp, root.clone()));

    let mountpoint = temp.path().join("mount");
    let error = Session::open(
        Config {
            rfs_home: temp.path().join("home"),
        },
        root,
        mountpoint,
    )
    .err()
    .expect("stale state must block startup");
    assert!(error.to_string().contains("delete `RFS_HOME`"));
    assert!(temp.path().join("home/active/session.db").exists());
}

#[test]
fn inspection_is_read_only_and_does_not_hold_daemon_lock() {
    let temp = tempfile::tempdir().unwrap();
    let root = Digest::for_bytes(b"root");
    let session = open_session(&temp, root);
    session.close().unwrap();
    let config = Config {
        rfs_home: temp.path().join("home"),
    };
    let database = config.rfs_home.join("active/session.db");
    let before = fs::read(&database).unwrap();

    let retained = Session::inspect(&config).unwrap().unwrap();
    assert_eq!(retained.state, SessionLifecycle::Closed);
    assert_eq!(fs::read(&database).unwrap(), before);

    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(config.rfs_home.join("active.lock"))
        .unwrap();
    let result = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(result, 0, "inspection unexpectedly acquired daemon lock");
    unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_UN) };
}

#[test]
fn concurrent_materialization_uses_stable_atomic_allocations() {
    let temp = tempfile::tempdir().unwrap();
    let root = Digest::for_bytes(b"root");
    let session = open_session(&temp, root.clone());
    let children = vec![
        RemoteChild {
            name: "directory".into(),
            content: RemoteContent::Directory(Digest::for_bytes(b"directory")),
            mode: None,
            mtime: None,
        },
        RemoteChild {
            name: "file".into(),
            content: RemoteContent::File(Digest::for_bytes(b"file")),
            mode: Some(0o640),
            mtime: None,
        },
    ];
    let barrier = Arc::new(Barrier::new(9));
    let mut threads = Vec::new();
    for _ in 0..8 {
        let session = Arc::clone(&session);
        let root = root.clone();
        let children = children.clone();
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            session
                .materialize_directory(InodeId::ROOT, &root, children)
                .unwrap()
        }));
    }
    barrier.wait();
    let allocations = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    assert!(allocations.windows(2).all(|window| window[0] == window[1]));
    assert!(allocations[0].iter().all(|node| node.inode > InodeId::ROOT));
    session.close().unwrap();
}

#[test]
fn sqlite_is_authoritative_and_applies_effective_metadata_defaults() {
    let temp = tempfile::tempdir().unwrap();
    let root = Digest::for_bytes(b"root");
    let session = open_session(&temp, root.clone());
    let nodes = session
        .materialize_directory(
            InodeId::ROOT,
            &root,
            vec![RemoteChild {
                name: "file".into(),
                content: RemoteContent::File(Digest::for_bytes(b"contents")),
                mode: None,
                mtime: None,
            }],
        )
        .unwrap();
    let file = &nodes[0];
    assert_eq!(file.kind, NodeKind::File);
    assert_eq!(file.mode, 0o444);
    assert_eq!(file.mtime.seconds(), 0);
    assert_eq!(session.node(file.inode).unwrap(), *file);
    session.close().unwrap();
}

#[test]
fn missing_home_inspection_creates_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let config = Config {
        rfs_home: temp.path().join("missing"),
    };
    assert!(Session::inspect(&config).unwrap().is_none());
    assert!(!config.rfs_home.exists());
}
