use std::fs;
use std::os::fd::AsRawFd;

use rfs_common::config::Config;
use rfs_common::digest::Digest;
use rfs_common::state::{SessionStartup, open_daemon, open_reader};

#[test]
fn closed_session_is_replaced_but_cache_is_retained() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let mountpoint = temp.path().join("mount");
    fs::create_dir(&mountpoint).unwrap();
    let startup = SessionStartup::new(Digest::for_bytes(b"root"), mountpoint);

    open_daemon(
        Config {
            rfs_home: home.clone(),
        },
        startup.clone(),
    )
    .unwrap()
    .close()
    .unwrap();
    fs::write(home.join("cache/retained"), b"data").unwrap();

    open_daemon(
        Config {
            rfs_home: home.clone(),
        },
        startup,
    )
    .unwrap()
    .close()
    .unwrap();
    assert!(home.join("cache/retained").exists());
    assert!(home.join("active/session.db").exists());
}

#[test]
fn stale_session_is_preserved_and_requires_manual_deletion() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let mountpoint = temp.path().join("mount");
    fs::create_dir(&mountpoint).unwrap();
    let startup = SessionStartup::new(Digest::for_bytes(b"root"), mountpoint);

    drop(
        open_daemon(
            Config {
                rfs_home: home.clone(),
            },
            startup.clone(),
        )
        .unwrap(),
    );
    let error = open_daemon(
        Config {
            rfs_home: home.clone(),
        },
        startup,
    )
    .err()
    .expect("stale state must block startup");
    assert!(error.to_string().contains("delete `RFS_HOME`"));
    assert!(home.join("active/session.db").exists());
}

#[test]
fn reader_is_read_only_and_does_not_hold_daemon_lock() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let mountpoint = temp.path().join("mount");
    fs::create_dir(&mountpoint).unwrap();
    open_daemon(
        Config {
            rfs_home: home.clone(),
        },
        SessionStartup::new(Digest::for_bytes(b"root"), mountpoint),
    )
    .unwrap()
    .close()
    .unwrap();
    let database = home.join("active/session.db");
    let before = fs::read(&database).unwrap();

    let reader = open_reader(Config {
        rfs_home: home.clone(),
    })
    .unwrap();
    let session = reader.session().unwrap().unwrap();
    assert_eq!(session.state, "closed");
    assert_eq!(fs::read(&database).unwrap(), before);

    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(home.join("active.lock"))
        .unwrap();
    let result = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(
        result, 0,
        "read-only state unexpectedly acquired daemon lock"
    );
    unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_UN) };
}
