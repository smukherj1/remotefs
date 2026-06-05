use std::net::{SocketAddr, TcpStream};
use std::process::Command;
use std::time::Duration;

use remotefs::cas::{Blob, CasClient, CasConfig};
use remotefs::digest::Digest;

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

    let blob = Blob::new(b"remotefs integration blob\n".to_vec());
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
    assert_eq!(downloaded.as_ref(), blob.data.as_slice());

    client.upload_blobs(vec![blob]).await.unwrap();
}
