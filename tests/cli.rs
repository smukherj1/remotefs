use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use std::os::unix::fs::PermissionsExt;

#[test]
fn test_rfs_help() {
    let mut cmd = Command::cargo_bin("rfs").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "content-addressed remote filesystem",
        ))
        .stdout(predicate::str::contains("--output-format"))
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
        .stdout(predicate::str::contains("--output-format"))
        .stdout(predicate::str::contains(
            "Root digest of the snapshot to mount",
        ));
}

#[test]
fn test_rfs_subcommand_help() {
    let mut cmd = Command::cargo_bin("rfs").unwrap();
    cmd.args(["upload", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Path to the local directory"));
}

#[test]
fn test_rfs_mount_invalid_digest() {
    let mut cmd = Command::cargo_bin("rfs").unwrap();
    cmd.args(["mount", "not-a-digest", "/tmp/rfs-mnt"])
        .assert()
        .failure()
        .code(1)
        .stderr(predicate::str::contains("invalid_digest"))
        .stderr(predicate::str::contains("invalid root digest"));
}

#[test]
fn test_rfs_json_error_diagnostic() {
    let mut cmd = Command::cargo_bin("rfs").unwrap();
    cmd.args([
        "--output-format",
        "json",
        "mount",
        "not-a-digest",
        "/tmp/rfs-mnt",
    ])
    .assert()
    .failure()
    .code(1)
    .stderr(predicate::str::contains("\"code\":\"invalid_digest\""));
}

#[test]
fn test_rfs_invalid_log_level_exits_one() {
    let mut cmd = Command::cargo_bin("rfs").unwrap();
    cmd.args(["--log-level", "bogus", "status"])
        .assert()
        .failure()
        .code(1)
        .stderr(predicate::str::contains("invalid value"));
}

#[test]
fn test_rfs_cleanup_resets_rfs_home() {
    let temp = tempfile::tempdir().unwrap();
    let mut cmd = Command::cargo_bin("rfs").unwrap();
    cmd.env("RFS_HOME", temp.path().join("home"))
        .arg("cleanup")
        .assert()
        .success();
}

#[test]
fn test_rfs_status_reports_clean_no_session() {
    let temp = tempfile::tempdir().unwrap();
    let mut cmd = Command::cargo_bin("rfs").unwrap();
    cmd.env("RFS_HOME", temp.path().join("home"))
        .arg("status")
        .assert()
        .success()
        .stdout(predicate::str::contains("no RemoteFS session"));
}

#[test]
fn test_rfs_status_reports_stale_retained_state_with_cleanup_guidance() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    fs::create_dir(&home).unwrap();
    fs::set_permissions(&home, fs::Permissions::from_mode(0o700)).unwrap();
    fs::create_dir(home.join("active")).unwrap();
    fs::set_permissions(home.join("active"), fs::Permissions::from_mode(0o700)).unwrap();

    let mut cmd = Command::cargo_bin("rfs").unwrap();
    cmd.env("RFS_HOME", &home)
        .arg("status")
        .assert()
        .failure()
        .stderr(predicate::str::contains("stale_session"))
        .stderr(predicate::str::contains("rfs cleanup"));
}

#[test]
fn test_removed_state_path_flags_are_rejected() {
    let mut cmd = Command::cargo_bin("rfs").unwrap();
    cmd.args(["--session-dir", "/tmp/active", "cleanup"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unexpected argument"));
}
