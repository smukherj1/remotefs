use std::fs;

use remotefs::config::Config;
use remotefs::digest::Digest;
use remotefs::state::{SessionStartup, SessionStore, StatePaths};

fn state_paths(temp: &tempfile::TempDir) -> StatePaths {
    StatePaths::from_config(&Config {
        rfs_home: temp.path().join("home"),
    })
    .unwrap()
}

#[test]
fn closed_session_is_replaced_but_cache_is_retained() {
    let temp = tempfile::tempdir().unwrap();
    let paths = state_paths(&temp);
    let mountpoint = temp.path().join("mount");
    fs::create_dir(&mountpoint).unwrap();
    let startup = SessionStartup::new(Digest::for_bytes(b"root"), mountpoint);

    SessionStore::create(paths.clone(), startup.clone())
        .unwrap()
        .close_cleanly()
        .unwrap();
    fs::write(paths.cache().join("retained"), b"data").unwrap();

    SessionStore::create(paths.clone(), startup)
        .unwrap()
        .close_cleanly()
        .unwrap();
    assert!(paths.cache().join("retained").exists());
    assert!(paths.database().exists());
}

#[test]
fn stale_session_requires_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let paths = state_paths(&temp);
    let mountpoint = temp.path().join("mount");
    fs::create_dir(&mountpoint).unwrap();
    let startup = SessionStartup::new(Digest::for_bytes(b"root"), mountpoint);

    drop(SessionStore::create(paths.clone(), startup.clone()).unwrap());
    assert!(SessionStore::create(paths.clone(), startup.clone()).is_err());

    paths.cleanup().unwrap();
    SessionStore::create(paths, startup)
        .unwrap()
        .close_cleanly()
        .unwrap();
}
