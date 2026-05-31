use std::fs;
use std::process::Command;

#[test]
fn test_docker_prerequisite() {
    let output = Command::new("docker").arg("--version").output();

    match output {
        Ok(out) if out.status.success() => {
            println!(
                "Docker is available: {}",
                String::from_utf8_lossy(&out.stdout).trim()
            );
        }
        _ => {
            panic!(
                "PREREQUISITE FAILED: Docker CLI is required for CAS integration tests but is not available."
            );
        }
    }
}

#[test]
fn test_fuse_prerequisite() {
    let fuse_dev = std::path::Path::new("/dev/fuse");
    if !fuse_dev.exists() {
        panic!(
            "PREREQUISITE FAILED: /dev/fuse is not present on the host. FUSE mounts are unsupported."
        );
    }

    let metadata = fs::metadata(fuse_dev);
    match metadata {
        Ok(_) => {
            if fs::OpenOptions::new().write(true).open(fuse_dev).is_err() {
                println!(
                    "Warning: /dev/fuse is present but not writable by the current user. FUSE mounts might fail without sudo/permissions."
                );
            }
        }
        Err(e) => {
            panic!(
                "PREREQUISITE FAILED: Failed to access /dev/fuse: {}. FUSE mounts are unsupported.",
                e
            );
        }
    }

    let fusermount_exists = Command::new("fusermount3").arg("-V").output().is_ok()
        || Command::new("fusermount").arg("-V").output().is_ok();

    if !fusermount_exists {
        panic!(
            "PREREQUISITE FAILED: Neither fusermount nor fusermount3 utility was found in PATH. FUSE mounts will fail."
        );
    }
}
