// Daemon control socket gRPC service bindings.

pub mod v1 {
    #![allow(clippy::doc_lazy_continuation)]
    #![allow(clippy::doc_overindented_list_items)]
    tonic::include_proto!("remotefs.control.v1");
}
