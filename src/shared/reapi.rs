// Remote Execution API (REAPI) proto bindings.

pub mod build {
    pub mod bazel {
        pub mod remote {
            pub mod execution {
                pub mod v2 {
                    #![allow(clippy::doc_lazy_continuation)]
                    #![allow(clippy::doc_overindented_list_items)]
                    tonic::include_proto!("build.bazel.remote.execution.v2");
                }
            }
        }

        pub mod semver {
            #![allow(clippy::doc_lazy_continuation)]
            #![allow(clippy::doc_overindented_list_items)]
            tonic::include_proto!("build.bazel.semver");
        }
    }
}

pub mod google {
    pub mod api {
        #![allow(clippy::doc_lazy_continuation)]
        #![allow(clippy::doc_overindented_list_items)]
        tonic::include_proto!("google.api");
    }

    pub mod bytestream {
        #![allow(clippy::doc_lazy_continuation)]
        #![allow(clippy::doc_overindented_list_items)]
        tonic::include_proto!("google.bytestream");
    }

    pub mod longrunning {
        #![allow(clippy::doc_lazy_continuation)]
        #![allow(clippy::doc_overindented_list_items)]
        tonic::include_proto!("google.longrunning");
    }

    pub mod rpc {
        #![allow(clippy::doc_lazy_continuation)]
        #![allow(clippy::doc_overindented_list_items)]
        tonic::include_proto!("google.rpc");
    }
}

pub use build::bazel::remote::execution::v2 as remote_execution;
pub use google::bytestream;

#[cfg(test)]
mod tests {
    use super::remote_execution::{Directory, DirectoryNode, FileNode, NodeProperties};
    use crate::shared::{digest::Digest, reapi::remote_execution::NodeProperty};
    use prost::Message;
    use prost_types::Timestamp;

    fn encoded_digest(message: &impl Message) -> Digest {
        let bytes = message.encode_to_vec();
        Digest::for_bytes(&bytes)
    }

    #[test]
    fn minimal_directory_has_stable_digest() {
        let directory = Directory::default();

        assert_eq!(
            encoded_digest(&directory).to_string(),
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0"
        );
    }

    #[test]
    fn representative_directory_has_golden_digest() {
        let directory = Directory {
            files: vec![FileNode {
                name: "hello.txt".to_string(),
                digest: Some(super::remote_execution::Digest {
                    hash: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
                        .to_string(),
                    size_bytes: 5,
                }),
                is_executable: false,
                node_properties: None,
            }],
            directories: vec![DirectoryNode {
                name: "src".to_string(),
                digest: Some(super::remote_execution::Digest {
                    hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                        .to_string(),
                    size_bytes: 0,
                }),
            }],
            symlinks: Vec::new(),
            node_properties: Some(NodeProperties {
                properties: vec![
                    NodeProperty {
                        name: String::from("name"),
                        value: String::from("value"),
                    },
                    NodeProperty {
                        name: String::from("name2"),
                        value: String::from("value2"),
                    },
                ],
                mtime: Some(Timestamp {
                    seconds: 100,
                    nanos: 1000,
                }),
                unix_mode: Some(777),
            }),
        };

        assert_eq!(
            encoded_digest(&directory).to_string(),
            "sha256:ba6a1a525cc35320ce6c495131b2879c015be3dc1e6d46cf3fd464f47dc604b9/204"
        );
    }
}
