use std::fs;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use anyhow::{Context, Result};
use rfs_common::cas::{CasClient, CasConfig};
use rfs_common::config::Config;
use rfs_common::state::{ROOT_INODE, SessionStartup, open_daemon};
use rfs_common::upload::{UploadOptions, upload_local_directory};
use rfsd::fs::ReadOnlyFilesystem;

const LOCAL_CAS_ADDR: &str = "127.0.0.1:9092";

fn verify_prerequisite() {
    let addr: SocketAddr = LOCAL_CAS_ADDR.parse().unwrap();
    TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap_or_else(|error| {
        panic!(
            "PREREQUISITE FAILED: local bazel-remote CAS is not reachable at \
             grpc://{LOCAL_CAS_ADDR}: {error}. Run `task cas:up` before \
             `task test:integration:readonly`."
        )
    });
}

#[tokio::test]
async fn uploaded_fixture_is_read_lazily_through_verified_cache() -> Result<()> {
    verify_prerequisite();
    let temp = tempfile::tempdir().context("create read-only integration root")?;
    let source = temp.path().join("source");
    let nested = source.join("nested");
    fs::create_dir_all(&nested).context("create integration fixture directories")?;
    fs::write(source.join("root.txt"), b"root contents").context("write root fixture file")?;
    fs::write(nested.join("child.txt"), b"child contents").context("write nested fixture file")?;

    let instance = format!("remotefs/readonly-integration/{}", std::process::id());
    let cas_config = CasConfig::new(format!("grpc://{LOCAL_CAS_ADDR}"), instance)?;
    let mut uploader = CasClient::connect(cas_config.clone()).await?;
    let summary = upload_local_directory(&mut uploader, &source, UploadOptions::default()).await?;

    let mountpoint = temp.path().join("mount");
    fs::create_dir(&mountpoint).context("create integration mountpoint")?;
    let state = open_daemon(
        Config {
            rfs_home: temp.path().join("rfs-home"),
        },
        SessionStartup::new(summary.root_digest.clone(), mountpoint),
    )?;
    let session = state
        .session()?
        .context("active integration session is missing")?;
    let filesystem_state = state.filesystem_state();
    let reader = CasClient::connect(cas_config).await?;
    let filesystem = ReadOnlyFilesystem::mount(
        reader,
        filesystem_state,
        session.cache_path,
        summary.root_digest,
    )
    .await?;

    let nested = filesystem.lookup(ROOT_INODE, "nested").await?;
    let child = filesystem.lookup(nested.inode, "child.txt").await?;
    assert_eq!(
        filesystem.read(child.inode, 0, 64).await?.as_ref(),
        b"child contents"
    );
    let counters = filesystem.counters();
    assert_eq!(counters.directory_downloads, 2);
    assert_eq!(counters.blob_downloads, 1);

    drop(filesystem);
    state.close()?;
    Ok(())
}
