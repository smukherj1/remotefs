use std::fs;
use std::sync::{Arc, Barrier};

use rfs_common::config::Config;
use rfs_common::digest::Digest;
use rfs_common::logging;
use rfs_common::session::{InodeId, NodeKind, RemoteChild, RemoteContent, Session};

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
fn session_inspection() {
    logging::init_test();
    let temp = tempfile::tempdir().unwrap();
    let root = Digest::for_bytes(b"root");
    let session = open_session(&temp, root.clone());
    session.close().unwrap();
    let config = Config {
        rfs_home: temp.path().join("home"),
    };

    let session = Session::inspect(&config).unwrap().unwrap();
    assert_eq!(session.root_digest, root);
}

#[test]
fn concurrent_materialization_uses_stable_atomic_allocations() {
    logging::init_test();
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
    logging::init_test();
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
    logging::init_test();
    let temp = tempfile::tempdir().unwrap();
    let config = Config {
        rfs_home: temp.path().join("missing"),
    };
    // Inspection of missing session directory is expected to fail.
    assert!(Session::inspect(&config).is_err());
    assert!(!config.rfs_home.exists());
}
