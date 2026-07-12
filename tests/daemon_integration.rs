use std::fs;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use assert_cmd::cargo::cargo_bin;
use predicates::prelude::*;

const ROOT_DIGEST: &str =
    "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0";

#[test]
fn daemon_status_second_session_and_unmount_control_path() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let mountpoint = temp.path().join("mount");
    fs::create_dir(&mountpoint).unwrap();

    let mut daemon = Command::new(cargo_bin("rfsd"))
        .env("RFS_HOME", &home)
        .args([ROOT_DIGEST, mountpoint.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    wait_for_path(&home.join("active/control.sock"), &mut daemon);

    assert_cmd::Command::cargo_bin("rfs")
        .unwrap()
        .env("RFS_HOME", &home)
        .arg("status")
        .assert()
        .success()
        .stdout(predicate::str::contains("active session"))
        .stdout(predicate::str::contains(ROOT_DIGEST));

    assert_cmd::Command::cargo_bin("rfsd")
        .unwrap()
        .env("RFS_HOME", &home)
        .args([ROOT_DIGEST, mountpoint.to_str().unwrap()])
        .assert()
        .failure()
        .stderr(predicate::str::contains("another RemoteFS session"));

    assert_cmd::Command::cargo_bin("rfs")
        .unwrap()
        .env("RFS_HOME", &home)
        .args(["unmount", mountpoint.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("unmount requested"));

    let status = daemon.wait().unwrap();
    assert!(status.success(), "daemon did not close cleanly: {status}");

    assert_cmd::Command::cargo_bin("rfs")
        .unwrap()
        .env("RFS_HOME", &home)
        .arg("status")
        .assert()
        .success()
        .stdout(predicate::str::contains("closed session"))
        .stdout(predicate::str::contains("run `rfs cleanup`").not());
}

fn wait_for_path(path: &std::path::Path, child: &mut std::process::Child) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        assert!(
            child.try_wait().unwrap().is_none(),
            "daemon exited before creating control socket"
        );
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("daemon did not create {}", path.display());
}
