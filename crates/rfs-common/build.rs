use std::path::PathBuf;

use anyhow::{Context, Result};

fn main() -> Result<()> {
    let root = PathBuf::from("../..");
    let third_party = root.join("third_party/remote-apis");
    let protos = [
        third_party.join("build/bazel/remote/execution/v2/remote_execution.proto"),
        third_party.join("google/bytestream/bytestream.proto"),
    ];
    let mut prost_config = prost_build::Config::new();
    prost_config.disable_comments(["."]);
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .disable_comments(".")
        .compile_with_config(prost_config, &protos, &[third_party])
        .with_context(|| format!("compile RemoteFS storage protobuf definitions: {protos:?}"))?;

    let proto_root = root.join("proto");
    let control_proto = proto_root.join("remotefs/control/v1/control.proto");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .disable_comments(".")
        .compile(std::slice::from_ref(&control_proto), &[proto_root])
        .with_context(|| {
            format!("compile RemoteFS control protobuf definition: {control_proto:?}")
        })?;
    Ok(())
}
