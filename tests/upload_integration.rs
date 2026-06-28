use std::fs;
use std::net::{SocketAddr, TcpStream};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::process::Command as StdCommand;
use std::time::Duration;

use anyhow::{Context, Result};
use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;
use tempfile::tempdir;

const LOCAL_CAS_ADDR: &str = "127.0.0.1:9092";

fn verify_prerequisites() {
    let docker = StdCommand::new("docker").arg("--version").output();
    match docker {
        Ok(output) if output.status.success() => {}
        _ => {
            panic!(
                "PREREQUISITE FAILED: Docker CLI is required for upload integration tests. Run `task cas:up` after installing Docker."
            );
        }
    }

    let addr: SocketAddr = LOCAL_CAS_ADDR.parse().unwrap();
    TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap_or_else(|err| {
        panic!(
            "PREREQUISITE FAILED: local bazel-remote CAS is not reachable at grpc://{LOCAL_CAS_ADDR}: {err}. Run `task cas:up` before `task test:integration:upload`."
        )
    });
}

#[test]
fn rfs_upload_fixture_prints_digest_and_json_summary() {
    verify_prerequisites();

    let source = tempdir().unwrap();
    copy_tree(Path::new("tests/fixtures/tree-roundtrip"), source.path()).unwrap();
    let instance = format!("remotefs/upload-integration/{}", std::process::id());

    let mut human = Command::cargo_bin("rfs").unwrap();
    human
        .args([
            "--cas-url",
            &format!("grpc://{LOCAL_CAS_ADDR}"),
            "--instance-name",
            &instance,
            "upload",
            source.path().to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::starts_with("sha256:"))
        .stderr(predicate::str::contains("uploaded_blobs="));

    let mut json = Command::cargo_bin("rfs").unwrap();
    let output = json
        .args([
            "--json",
            "--cas-url",
            &format!("grpc://{LOCAL_CAS_ADDR}"),
            "--instance-name",
            &instance,
            "upload",
            source.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["schema_version"], 1);
    assert_eq!(payload["command"], "upload");
    assert_eq!(payload["ok"], true);
    assert_eq!(payload["data"]["files"], 3);
    assert_eq!(payload["data"]["directories"], 6);
    assert_eq!(payload["data"]["symlinks"], 3);
    assert!(
        payload["data"]["root_digest"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
}

fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)
        .with_context(|| format!("create fixture destination {}", destination.display()))?;
    for entry in fs::read_dir(source)
        .with_context(|| format!("read fixture source directory {}", source.display()))?
    {
        let entry = entry.with_context(|| format!("read entry in {}", source.display()))?;
        if entry.file_name() == ".gitkeep" {
            continue;
        }
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&source_path)
            .with_context(|| format!("read metadata for fixture {}", source_path.display()))?;
        if metadata.file_type().is_symlink() {
            let target = fs::read_link(&source_path)
                .with_context(|| format!("read symlink target {}", source_path.display()))?;
            symlink(target, &destination_path).with_context(|| {
                format!("create fixture symlink {}", destination_path.display())
            })?;
        } else if metadata.is_dir() {
            copy_tree(&source_path, &destination_path).with_context(|| {
                format!(
                    "copy fixture directory {} to {}",
                    source_path.display(),
                    destination_path.display()
                )
            })?;
        } else {
            fs::copy(&source_path, &destination_path).with_context(|| {
                format!(
                    "copy fixture file {} to {}",
                    source_path.display(),
                    destination_path.display()
                )
            })?;
            fs::set_permissions(
                &destination_path,
                fs::Permissions::from_mode(metadata.permissions().mode()),
            )
            .with_context(|| format!("set mode on fixture {}", destination_path.display()))?;
        }
    }
    Ok(())
}
