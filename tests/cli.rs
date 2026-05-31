use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_rfs_help() {
    let mut cmd = Command::cargo_bin("rfs").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "content-addressed remote filesystem",
        ))
        .stdout(predicate::str::contains("Upload a local directory"))
        .stdout(predicate::str::contains("Mount a RemoteFS snapshot"));
}

#[test]
fn test_rfsd_help() {
    let mut cmd = Command::cargo_bin("rfsd").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "background daemon that owns the FUSE mount",
        ))
        .stdout(predicate::str::contains(
            "Root digest of the snapshot to mount",
        ));
}
