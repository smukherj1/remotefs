#![cfg(target_os = "linux")]

use std::fs;
use std::net::{SocketAddr, TcpStream};
use std::os::unix::fs::symlink;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use assert_cmd::cargo::cargo_bin;
use predicates::prelude::*;
use rfs_common::config::Config;
use rfs_common::logging::daemon_log_paths;
use rfs_common::session::Session;

const LOCAL_CAS_ADDR: &str = "127.0.0.1:9092";
/// Well-formed root digest whose blob is never uploaded to the CAS.
const MISSING_ROOT: &str =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/123";
/// A second missing root digest, distinct from `MISSING_ROOT`.
const OTHER_MISSING_ROOT: &str =
    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/456";

/// Uploads a small tree to the local CAS, mounts it through `rfs mount`, reads
/// it lazily through FUSE, then unmounts.
///
/// Setup: a fresh `RFS_HOME`, a source tree with a file, a nested file, and a
/// symlink, and an empty mountpoint. Reads and mmap are expected to succeed;
/// writes are expected to fail because the mount is read-only. `rfs unmount`
/// is expected to succeed only after the daemon released everything, which
/// the final checks confirm through retained inspection and the process table.
/// The daemon trace log in `RFS_HOME` is expected to record the session
/// becoming active and, after unmount, closed. A second `rfs mount` while the
/// first is active is expected to fail without spawning a daemon, so neither
/// log file changes.
#[test]
fn upload_mount_lazy_read() -> Result<()> {
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
    let logs = daemon_log_paths(&home);
    let trace = fs::read_to_string(&logs.trace).context("read daemon trace log")?;
    assert!(
        trace.contains("daemon session active"),
        "trace log after mount: {trace}"
    );
    assert_second_mount_rejected(&home, &instance, &digest, &mountpoint)?;
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
    unmount(&home)?;
    assert_daemon_released(&home)?;
    let trace = fs::read_to_string(&logs.trace).context("read daemon trace log")?;
    assert!(
        trace.contains("daemon session closed"),
        "trace log after unmount: {trace}"
    );
    Ok(())
}

/// Mounts a root digest that the CAS does not hold.
///
/// Setup: a fresh `RFS_HOME` and an empty mountpoint. The daemon is expected
/// to exit before readiness because root validation fails. The CLI does not
/// read daemon output, so its error is expected to name both log files, and
/// the daemon's final error is expected to be in the stdout/stderr log.
#[test]
fn mount_failure_names_logs_and_keeps_daemon_error() -> Result<()> {
    verify_cas_prerequisite();
    let temp = tempfile::tempdir().context("create e2e directory")?;
    let home = temp.path().join("home");
    let mountpoint = temp.path().join("mount");
    fs::create_dir(&mountpoint)?;
    let logs = daemon_log_paths(&home);

    mount_command(&home, "remotefs/mount-failure", MISSING_ROOT, &mountpoint)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "daemon exited before mount readiness",
        ))
        .stderr(predicate::str::contains(logs.stdio.to_str().unwrap()))
        .stderr(predicate::str::contains(logs.trace.to_str().unwrap()));

    let stdio = fs::read_to_string(&logs.stdio).context("read daemon stdout/stderr log")?;
    assert!(
        stdio.contains("validate root directory before FUSE mount"),
        "stdout/stderr log: {stdio}"
    );
    Ok(())
}

/// Runs two failing mounts in a row on one `RFS_HOME`.
///
/// Setup: a fresh `RFS_HOME` and an empty mountpoint; each mount names a
/// different root digest that the CAS does not hold. Both mounts are expected
/// to fail. Each `rfs mount` truncates the stdout/stderr log before spawn, so
/// the log is expected to hold only the second daemon's error.
#[test]
fn mount_after_failed_mount_keeps_only_latest_daemon_output() -> Result<()> {
    verify_cas_prerequisite();
    let temp = tempfile::tempdir().context("create e2e directory")?;
    let home = temp.path().join("home");
    let mountpoint = temp.path().join("mount");
    fs::create_dir(&mountpoint)?;
    let instance = "remotefs/mount-failure";

    mount_command(&home, instance, MISSING_ROOT, &mountpoint)
        .assert()
        .failure();
    mount_command(&home, instance, OTHER_MISSING_ROOT, &mountpoint)
        .assert()
        .failure();

    let stdio = fs::read_to_string(daemon_log_paths(&home).stdio)
        .context("read daemon stdout/stderr log")?;
    assert!(
        stdio.contains(OTHER_MISSING_ROOT),
        "stdout/stderr log: {stdio}"
    );
    assert!(!stdio.contains(MISSING_ROOT), "stdout/stderr log: {stdio}");
    Ok(())
}

fn mount(
    home: &std::path::Path,
    instance: &str,
    digest: &str,
    mountpoint: &std::path::Path,
) -> Result<()> {
    mount_command(home, instance, digest, mountpoint)
        .assert()
        .success();
    Ok(())
}

/// Builds an `rfs mount` command for `digest` at `mountpoint` in `home`.
fn mount_command(
    home: &std::path::Path,
    instance: &str,
    digest: &str,
    mountpoint: &std::path::Path,
) -> assert_cmd::Command {
    let mut command = assert_cmd::Command::new(cargo_bin("rfs"));
    command.env("RFS_HOME", home).args([
        "--cas-url",
        "grpc://127.0.0.1:9092",
        "--instance-name",
        instance,
        "mount",
        digest,
        mountpoint.to_str().unwrap(),
    ]);
    command
}

/// Checks that `rfs mount` refuses to start while a daemon is active in `home`.
///
/// The second mount is expected to fail with the already-active error before
/// spawning a daemon. A spawned daemon would truncate both log files, so
/// unchanged log contents show that none was started.
fn assert_second_mount_rejected(
    home: &std::path::Path,
    instance: &str,
    digest: &str,
    mountpoint: &std::path::Path,
) -> Result<()> {
    let logs = daemon_log_paths(home);
    let stdio_before = fs::read(&logs.stdio).context("read daemon stdout/stderr log")?;
    let trace_before = fs::read(&logs.trace).context("read daemon trace log")?;

    mount_command(home, instance, digest, mountpoint)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "a RemoteFS session is already active at",
        ))
        .stderr(predicate::str::contains("run `rfs unmount` first"));

    assert_eq!(fs::read(&logs.stdio)?, stdio_before);
    assert_eq!(fs::read(&logs.trace)?, trace_before);
    Ok(())
}

fn unmount(home: &std::path::Path) -> Result<()> {
    assert_cmd::Command::new(cargo_bin("rfs"))
        .env("RFS_HOME", home)
        .arg("unmount")
        .assert()
        .success();
    Ok(())
}

/// Checks that a successful `rfs unmount` left the daemon fully released: the
/// daemon process has exited, the retained session is `closed`, and the
/// control socket is gone. These are the guarantees `rfs unmount` promises,
/// so they are read through public retained inspection rather than internals.
fn assert_daemon_released(home: &std::path::Path) -> Result<()> {
    let config = Config {
        rfs_home: home.to_path_buf(),
    };
    let session = Session::inspect(&config)
        .context("inspect retained session after unmount")?
        .context("retained session is missing after unmount")?;
    let pid = libc::pid_t::try_from(session.daemon_pid)?;
    // SAFETY: signal 0 performs only the existence and permission check.
    let alive = unsafe { libc::kill(pid, 0) } == 0
        || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH);
    assert!(!alive, "daemon pid {pid} is still running after unmount");
    assert!(
        session.closed,
        "retained session is not closed after unmount"
    );
    assert!(
        !session.control_endpoint.exists(),
        "control socket {} still exists after unmount",
        session.control_endpoint.display()
    );
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
    verify_cas_prerequisite();
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

fn verify_cas_prerequisite() {
    let addr: SocketAddr = LOCAL_CAS_ADDR.parse().unwrap();
    TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap_or_else(|error| {
        panic!(
            "PREREQUISITE FAILED: local bazel-remote CAS is not reachable at \
             grpc://{LOCAL_CAS_ADDR}: {error}. Run `task cas:up` before \
             `task test:e2e:readonly`."
        )
    });
}
