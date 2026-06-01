# Protobuf Sources

RemoteFS checks in protobuf source files and generates Rust bindings at build time with `tonic-build` and `prost-build`. Generated Rust is written to Cargo's `OUT_DIR` and is not checked in.

Pinned sources:

- `third_party/remote-apis/build/bazel/**`: `bazelbuild/remote-apis` tag `v2.3.0`, commit `e95641649b5b4d3c582c89daabfaabeb8189dd77`.
- `third_party/remote-apis/google/**`: `googleapis/googleapis` commit `0054cdcbd8ea44298f329d916d8173dd736fbaaa`.
- `proto/remotefs/control/v1/control.proto`: RemoteFS-owned daemon control protocol.

Update rules:

- Update upstream protos by replacing the checked-in source files and recording the new immutable tag or commit here.
- Keep `proto/remotefs/control/v1` backward compatible within v1: add fields with new numbers, do not renumber fields, and reserve removed field numbers and names.
- Run `task proto:check`, `task build`, and `task test:unit` after any proto change.
