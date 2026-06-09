use std::path::PathBuf;

use anyhow::{Context, Result};

fn main() -> Result<()> {
    let third_party = PathBuf::from("third_party/remote-apis");
    let control = PathBuf::from("proto");

    let protos = [
        "third_party/remote-apis/build/bazel/remote/execution/v2/remote_execution.proto",
        "third_party/remote-apis/google/bytestream/bytestream.proto",
        "proto/remotefs/control/v1/control.proto",
    ];

    let mut prost_config = prost_build::Config::new();
    prost_config.disable_comments(["."]);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .disable_comments(".")
        .compile_with_config(prost_config, &protos, &[third_party, control])
        .with_context(|| format!("compile RemoteFS protobuf definitions: {protos:?}"))?;

    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    println!("cargo:rerun-if-changed=third_party/remote-apis");
    println!("cargo:rerun-if-changed=proto");

    Ok(())
}
