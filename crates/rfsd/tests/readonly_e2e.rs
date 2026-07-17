#![cfg(target_os = "linux")]

use std::fs;
use std::net::{SocketAddr, TcpStream};
use std::os::unix::fs::symlink;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use assert_cmd::cargo::cargo_bin;

const LOCAL_CAS_ADDR: &str = "127.0.0.1:9092";

#[test]
fn upload_mount_lazy_read_remount_and_unmount() -> Result<()> {
    verify_prerequisites();
    let temp = tempfile::tempdir().context("create e2e directory")?;
    let home = temp.path().join("home");
    let source = temp.path().join("source");
    let mountpoint = temp.path().join("mount");
    fs::create_dir_all(source.join("nested"))?;
    fs::create_dir(&mountpoint)?;
    fs::write(source.join("root.txt"), b"root contents")?;
    fs::write(source.join("nested/child.txt"), b"child contents")?;
    symlink("nested/child.txt", source.join("child-link"))?;
    let instance = format!("remotefs/readonly-e2e/{}", std::process::id());

    let upload = assert_cmd::Command::new(cargo_bin("rfs"))
        .env("RFS_HOME", &home)
        .args([
            "--cas-url",
            "grpc://127.0.0.1:9092",
            "--instance-name",
            &instance,
            "upload",
            source.to_str().unwrap(),
        ])
        .output()?;
    if !upload.status.success() {
        bail!(
            "fixture upload failed: {}",
            String::from_utf8_lossy(&upload.stderr)
        );
    }
    let digest = String::from_utf8(upload.stdout)?.trim().to_owned();

    mount(&home, &instance, &digest, &mountpoint)?;
    assert_command_success(Command::new("find").arg(&mountpoint))?;
    assert_command_success(Command::new("stat").arg(mountpoint.join("root.txt")))?;
    let contents = Command::new("cat")
        .arg(mountpoint.join("nested/child.txt"))
        .output()?;
    assert!(contents.status.success());
    assert_eq!(contents.stdout, b"child contents");
    let link = Command::new("readlink")
        .arg(mountpoint.join("child-link"))
        .output()?;
    assert!(link.status.success());
    assert_eq!(link.stdout, b"nested/child.txt\n");
    assert_eq!(mmap_file(&mountpoint.join("root.txt"))?, b"root contents");

    assert!(fs::write(mountpoint.join("root.txt"), b"changed").is_err());
    assert!(fs::create_dir(mountpoint.join("new-directory")).is_err());
    unmount(&home, &mountpoint)?;

    mount(&home, &instance, &digest, &mountpoint)?;
    assert_eq!(
        fs::read(mountpoint.join("nested/child.txt"))?,
        b"child contents"
    );
    let status = assert_cmd::Command::new(cargo_bin("rfs"))
        .env("RFS_HOME", &home)
        .args(["--output-format", "json", "status"])
        .output()?;
    assert!(status.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status.stdout)?;
    assert!(
        status["data"]["cached_blobs"].as_u64().unwrap_or(0) >= 1,
        "remount read should hit the verified local blob cache: {status}"
    );
    unmount(&home, &mountpoint)?;
    Ok(())
}

fn mount(
    home: &std::path::Path,
    instance: &str,
    digest: &str,
    mountpoint: &std::path::Path,
) -> Result<()> {
    assert_cmd::Command::new(cargo_bin("rfs"))
        .env("RFS_HOME", home)
        .args([
            "--cas-url",
            "grpc://127.0.0.1:9092",
            "--instance-name",
            instance,
            "mount",
            digest,
            mountpoint.to_str().unwrap(),
        ])
        .assert()
        .success();
    Ok(())
}

fn unmount(home: &std::path::Path, mountpoint: &std::path::Path) -> Result<()> {
    assert_cmd::Command::new(cargo_bin("rfs"))
        .env("RFS_HOME", home)
        .args(["unmount", mountpoint.to_str().unwrap()])
        .assert()
        .success();
    Ok(())
}

fn assert_command_success(command: &mut Command) -> Result<()> {
    let output = command.output()?;
    if !output.status.success() {
        bail!(
            "command failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

fn mmap_file(path: &std::path::Path) -> Result<Vec<u8>> {
    use std::os::fd::AsRawFd;

    let file = fs::File::open(path)?;
    let length = usize::try_from(file.metadata()?.len())?;
    if length == 0 {
        return Ok(Vec::new());
    }
    let address = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            length,
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            file.as_raw_fd(),
            0,
        )
    };
    if address == libc::MAP_FAILED {
        return Err(std::io::Error::last_os_error().into());
    }
    let bytes = unsafe { std::slice::from_raw_parts(address.cast::<u8>(), length).to_vec() };
    let result = unsafe { libc::munmap(address, length) };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(bytes)
}

fn verify_prerequisites() {
    let addr: SocketAddr = LOCAL_CAS_ADDR.parse().unwrap();
    TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap_or_else(|error| {
        panic!(
            "PREREQUISITE FAILED: local bazel-remote CAS is not reachable at \
             grpc://{LOCAL_CAS_ADDR}: {error}. Run `task cas:up` before \
             `task test:e2e:readonly`."
        )
    });
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/fuse")
        .unwrap_or_else(|error| {
            panic!(
                "PREREQUISITE FAILED: /dev/fuse is unavailable or inaccessible: {error}. \
                 Run this test on Linux with FUSE mount permission."
            )
        });
}
