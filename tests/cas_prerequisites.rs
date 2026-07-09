use std::collections::BTreeSet;
use std::fs;
use std::net::{SocketAddr, TcpStream};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use remotefs::cas::{Blob, CasClient, CasConfig};
use remotefs::digest::Digest;
use remotefs::tree::decode_directory;
use remotefs::upload::{UploadOptions, upload_local_directory};
use tempfile::tempdir;

const LOCAL_CAS_ADDR: &str = "127.0.0.1:9092";

fn verify_prerequisites() {
    let docker = Command::new("docker").arg("--version").output();
    match docker {
        Ok(output) if output.status.success() => {}
        _ => {
            panic!(
                "PREREQUISITE FAILED: Docker CLI is required for CAS integration tests. Run `task cas:up` after installing Docker."
            );
        }
    }

    let addr: SocketAddr = LOCAL_CAS_ADDR.parse().unwrap();
    TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap_or_else(|err| {
        panic!(
            "PREREQUISITE FAILED: local bazel-remote CAS is not reachable at grpc://{LOCAL_CAS_ADDR}: {err}. Run `task cas:up` before `task test:integration:cas`."
        )
    });
}

#[test]
fn local_bazel_remote_grpc_endpoint_is_reachable() {
    verify_prerequisites();
}

#[tokio::test]
async fn local_bazel_remote_upload_check_download_and_reupload() {
    verify_prerequisites();

    let blob = Blob::from_bytes("remotefs integration blob\n");
    let config = CasConfig::new(format!("grpc://{LOCAL_CAS_ADDR}"), "remotefs/tests").unwrap();
    let mut client = CasClient::connect(config).await.unwrap();

    let missing_before = client
        .find_missing_blobs(std::slice::from_ref(&blob.digest))
        .await
        .unwrap();
    if missing_before.contains(&blob.digest) {
        client.upload_blobs(vec![blob.clone()]).await.unwrap();
    }

    assert_eq!(
        client
            .find_missing_blobs(std::slice::from_ref(&blob.digest))
            .await
            .unwrap(),
        Vec::<Digest>::new()
    );

    let downloaded = client.download_blob(&blob.digest).await.unwrap();
    assert_eq!(downloaded.as_ref(), b"remotefs integration blob\n");

    client.upload_blobs(vec![blob]).await.unwrap();
}

#[tokio::test]
async fn tree_fixture_round_trips_through_local_cas() {
    verify_prerequisites();

    let source = tempdir().unwrap();
    copy_tree(Path::new("tests/fixtures/tree-roundtrip"), source.path()).unwrap();

    let config = CasConfig::new(format!("grpc://{LOCAL_CAS_ADDR}"), "remotefs/tests").unwrap();
    let mut client = CasClient::connect(config).await.unwrap();
    let summary = upload_local_directory(&mut client, source.path(), UploadOptions::default())
        .await
        .unwrap();
    assert!(summary.files > 0);
    assert!(summary.directories > 0);

    let reconstructed = tempdir().unwrap();
    reconstruct_directory(&mut client, &summary.root_digest, reconstructed.path()).await;
    compare_trees(source.path(), reconstructed.path());
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
                fs::Permissions::from_mode(metadata.permissions().mode() & 0o777),
            )
            .with_context(|| {
                format!("set fixture permissions on {}", destination_path.display())
            })?;
        }
    }
    Ok(())
}

async fn reconstruct_directory(client: &mut CasClient, digest: &Digest, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    let bytes = client.download_blob(digest).await.unwrap();
    let directory = decode_directory(digest.clone(), bytes).unwrap();

    for file in directory.files {
        let digest = Digest::from_reapi(file.digest.as_ref().unwrap()).unwrap();
        let data = client.download_blob(&digest).await.unwrap();
        let path = destination.join(file.name);
        fs::write(&path, data).unwrap();
        if let Some(mode) = file
            .node_properties
            .as_ref()
            .and_then(|properties| properties.unix_mode)
        {
            fs::set_permissions(&path, fs::Permissions::from_mode(mode & 0o777)).unwrap();
        }
    }

    for child in directory.directories {
        let digest = Digest::from_reapi(child.digest.as_ref().unwrap()).unwrap();
        let path = destination.join(child.name);
        Box::pin(reconstruct_directory(client, &digest, &path)).await;
    }

    for link in directory.symlinks {
        symlink(link.target, destination.join(link.name)).unwrap();
    }
}

fn compare_trees(left: &Path, right: &Path) {
    let left_paths = relative_paths(left);
    let right_paths = relative_paths(right);
    assert_eq!(left_paths, right_paths);

    for relative in left_paths {
        let left_path = left.join(&relative);
        let right_path = right.join(&relative);
        let left_metadata = fs::symlink_metadata(&left_path).unwrap();
        let right_metadata = fs::symlink_metadata(&right_path).unwrap();
        assert_eq!(
            left_metadata.file_type().is_symlink(),
            right_metadata.file_type().is_symlink(),
            "{relative:?}"
        );
        assert_eq!(
            left_metadata.is_dir(),
            right_metadata.is_dir(),
            "{relative:?}"
        );
        assert_eq!(
            left_metadata.is_file(),
            right_metadata.is_file(),
            "{relative:?}"
        );

        if left_metadata.file_type().is_symlink() {
            assert_eq!(
                fs::read_link(left_path).unwrap(),
                fs::read_link(right_path).unwrap()
            );
        } else if left_metadata.is_file() {
            assert_eq!(fs::read(left_path).unwrap(), fs::read(right_path).unwrap());
            assert_eq!(
                left_metadata.permissions().mode() & 0o111,
                right_metadata.permissions().mode() & 0o111,
                "{relative:?}"
            );
        }
    }
}

fn relative_paths(root: &Path) -> BTreeSet<PathBuf> {
    let mut paths = BTreeSet::new();
    collect_relative_paths(root, root, &mut paths);
    paths
}

fn collect_relative_paths(root: &Path, current: &Path, paths: &mut BTreeSet<PathBuf>) {
    let mut entries = fs::read_dir(current)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        paths.insert(path.strip_prefix(root).unwrap().to_path_buf());
        if fs::symlink_metadata(&path).unwrap().is_dir() {
            collect_relative_paths(root, &path, paths);
        }
    }
}
