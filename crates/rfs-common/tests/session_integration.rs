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
    assert_eq!(
        file.size,
        Digest::for_bytes(b"contents").size_bytes() as u64
    );
    assert_eq!(file.mode, 0o444);
    assert_eq!(file.mtime.seconds(), 0);
    assert_eq!(session.node(file.inode).unwrap(), *file);
    session.close().unwrap();
}

#[test]
fn materialization_preserves_nullable_metadata_and_translates_every_ready_inode() {
    logging::init_test();
    let temp = tempfile::tempdir().unwrap();
    let root = Digest::for_bytes(b"root");
    let session = open_session(&temp, root.clone());
    let epoch = rfs_common::session::NodeTime::new(0, 0).unwrap();
    let children = vec![
        RemoteChild {
            name: "absent".into(),
            content: RemoteContent::File(Digest::for_bytes(b"absent")),
            mode: None,
            mtime: None,
        },
        RemoteChild {
            name: "explicit".into(),
            content: RemoteContent::File(Digest::for_bytes(b"explicit")),
            mode: Some(0o444),
            mtime: Some(epoch),
        },
    ];
    let first = session
        .materialize_directory(InodeId::ROOT, &root, children.clone())
        .unwrap();
    let second = session
        .materialize_directory(InodeId::ROOT, &root, children)
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(
        first
            .iter()
            .map(|inode| inode.name.as_str())
            .collect::<Vec<_>>(),
        ["absent", "explicit"]
    );
    assert!(first.iter().all(|inode| inode.mode == 0o444));
    assert!(first.iter().all(|inode| inode.mtime == epoch));

    let database_path = temp.path().join("home/session/session.db");
    let connection = rusqlite::Connection::open(&database_path).unwrap();
    let absent: (Option<i64>, Option<i64>, Option<i64>) = connection
        .query_row(
            "SELECT mode, mtime_seconds, mtime_nanos FROM inodes WHERE name = 'absent'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let explicit: (Option<i64>, Option<i64>, Option<i64>) = connection
        .query_row(
            "SELECT mode, mtime_seconds, mtime_nanos FROM inodes WHERE name = 'explicit'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(absent, (None, None, None));
    assert_eq!(explicit, (Some(0o444), Some(0), Some(0)));

    connection
        .execute("UPDATE inodes SET tombstone = 1 WHERE name = 'absent'", [])
        .unwrap();
    let rfs_common::session::Lookup::Ready(visible) =
        session.list_directory(InodeId::ROOT).unwrap()
    else {
        panic!("materialized directory unexpectedly required another fetch");
    };
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].name, "explicit");

    connection
        .execute("UPDATE inodes SET mode = -1 WHERE name = 'explicit'", [])
        .unwrap();
    let message = session
        .list_directory(InodeId::ROOT)
        .unwrap_err()
        .to_string();
    assert!(message.contains("inode"), "{message}");
    assert!(message.contains("invalid mode"), "{message}");
    session.close().unwrap();
}

#[test]
fn rejected_materialization_leaves_no_partial_rows_or_marker() {
    logging::init_test();
    let temp = tempfile::tempdir().unwrap();
    let root = Digest::for_bytes(b"root");
    let session = open_session(&temp, root.clone());
    let duplicate = RemoteChild {
        name: "duplicate".into(),
        content: RemoteContent::File(Digest::for_bytes(b"contents")),
        mode: None,
        mtime: None,
    };
    assert!(
        session
            .materialize_directory(InodeId::ROOT, &root, vec![duplicate.clone(), duplicate])
            .is_err()
    );

    let database_path = temp.path().join("home/session/session.db");
    let connection = rusqlite::Connection::open(database_path).unwrap();
    let inode_count: i64 = connection
        .query_row("SELECT count(*) FROM inodes", [], |row| row.get(0))
        .unwrap();
    let marker_count: i64 = connection
        .query_row(
            "SELECT count(*) FROM directory_materializations",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(inode_count, 1);
    assert_eq!(marker_count, 0);
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

#[test]
fn node_projects_symlink_size_mode_and_target() {
    logging::init_test();
    let temp = tempfile::tempdir().unwrap();
    let root = Digest::for_bytes(b"root");
    let session = open_session(&temp, root.clone());
    let target = "../target";
    let nodes = session
        .materialize_directory(
            InodeId::ROOT,
            &root,
            vec![RemoteChild {
                name: "link".into(),
                content: RemoteContent::Symlink(target.into()),
                mode: None,
                mtime: None,
            }],
        )
        .unwrap();
    let link = &nodes[0];
    assert_eq!(link.kind, NodeKind::Symlink);
    assert_eq!(link.size, target.len() as u64);
    assert_eq!(link.mode, 0o777);
    assert_eq!(link.symlink_target.as_deref(), Some(target));
    session.close().unwrap();
}

#[test]
fn node_rejects_malformed_persisted_inode_fields() {
    logging::init_test();
    for (target, sql, expected) in [
        (
            "file",
            "UPDATE inodes SET mode = -1 WHERE name = 'entry'",
            "invalid mode",
        ),
        (
            "file",
            "UPDATE inodes SET name = 'bad/name' WHERE name = 'entry'",
            "invalid name",
        ),
        (
            "file",
            "UPDATE inodes SET remote_digest = 'not-a-digest' WHERE name = 'entry'",
            "invalid remote digest",
        ),
        (
            "file",
            "UPDATE inodes SET mtime_seconds = 0, mtime_nanos = NULL WHERE name = 'entry'",
            "partial modification time",
        ),
        (
            "file",
            "UPDATE inodes SET overlay_file = '/absolute' WHERE name = 'entry'",
            "invalid overlay file",
        ),
        (
            "file",
            "UPDATE inodes SET symlink_target = 'target' WHERE name = 'entry'",
            "fields do not match its kind",
        ),
        (
            "file",
            "UPDATE inodes SET parent_inode = NULL WHERE name = 'entry'",
            "has no parent but is not the root inode",
        ),
        (
            "root",
            "UPDATE inodes SET parent_inode = 1 WHERE inode = 1",
            "root inode has a parent",
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = Digest::for_bytes(b"root");
        let session = open_session(&temp, root.clone());
        let inode = if target == "root" {
            InodeId::ROOT
        } else {
            session
                .materialize_directory(
                    InodeId::ROOT,
                    &root,
                    vec![RemoteChild {
                        name: "entry".into(),
                        content: RemoteContent::File(Digest::for_bytes(b"contents")),
                        mode: None,
                        mtime: None,
                    }],
                )
                .unwrap()[0]
                .inode
        };
        let database_path = temp.path().join("home/session/session.db");
        let connection = rusqlite::Connection::open(&database_path).unwrap();
        connection.execute(sql, []).unwrap();
        let message = session.node(inode).unwrap_err().to_string();
        assert!(message.contains(expected), "sql `{sql}`: {message}");
        session.close().unwrap();
    }
}
