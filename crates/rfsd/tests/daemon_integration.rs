use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use assert_cmd::cargo::cargo_bin;
use predicates::prelude::*;

const LOCAL_CAS_ADDR: &str = "127.0.0.1:9092";

#[test]
fn daemon_rejects_a_missing_root_before_mounting_fuse() {
    verify_cas_prerequisite();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let mountpoint = temp.path().join("mount");
    std::fs::create_dir(&mountpoint).unwrap();
    let missing = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/123";

    assert_cmd::Command::new(cargo_bin("rfsd"))
        .env("RFS_HOME", &home)
        .args([
            missing,
            mountpoint.to_str().unwrap(),
            "--cas-url",
            "grpc://127.0.0.1:9092",
            "--instance-name",
            "remotefs/daemon-integration",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "validate root directory before FUSE mount",
        ))
        .stderr(predicate::str::contains(missing));
}

fn verify_cas_prerequisite() {
    let addr: SocketAddr = LOCAL_CAS_ADDR.parse().unwrap();
    TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap_or_else(|error| {
        panic!(
            "PREREQUISITE FAILED: local bazel-remote CAS is not reachable at \
             grpc://{LOCAL_CAS_ADDR}: {error}. Run `task cas:up` before \
             `task test:integration:daemon`."
        )
    });
}
