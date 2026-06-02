use std::net::{SocketAddr, TcpStream};
use std::process::Command;
use std::time::Duration;

const LOCAL_CAS_ADDR: &str = "127.0.0.1:9092";

#[test]
fn local_bazel_remote_grpc_endpoint_is_reachable() {
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
