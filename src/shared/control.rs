//! Typed daemon control API over the active session Unix socket.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::shared::error_context::{ResultContext, ResultContextError};
use thiserror::Error;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, oneshot};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Code, Request, Response, Status};
use tower::service_fn;

use crate::shared::state::{SessionMetadata, StatePaths};

/// Version of the CLI-to-daemon control protocol implemented by this crate.
pub const PROTOCOL_VERSION: u32 = 1;

/// Generated protobuf messages and gRPC client/server bindings.
pub mod v1 {
    #![allow(clippy::doc_lazy_continuation)]
    #![allow(clippy::doc_overindented_list_items)]
    tonic::include_proto!("remotefs.control.v1");
}

/// Failures while creating, serving, or calling the daemon control socket.
#[derive(Debug, Error)]
pub enum ControlError {
    /// A filesystem operation on the Unix socket failed.
    #[error("control socket operation on `{path}` failed: {source}")]
    Socket {
        /// Path of the socket involved in the failed operation.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Tonic could not establish or serve the gRPC transport.
    #[error("control transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// The daemon rejected a gRPC request.
    #[error("daemon request failed: {0}")]
    Rpc(#[source] Box<tonic::Status>),
    /// The daemon and client implement different protocol versions.
    #[error("daemon returned incompatible protocol version {actual}; client requires {expected}")]
    IncompatibleProtocol {
        /// Protocol version required by this client.
        expected: u32,
        /// Protocol version returned by the daemon.
        actual: u32,
    },
    /// Additional operation context attached while preserving the source error.
    #[error("{operation}: {source}")]
    Context {
        /// Operation being attempted when the source error occurred.
        operation: String,
        /// Original typed control error.
        #[source]
        source: Box<ControlError>,
    },
}

impl ResultContextError for ControlError {
    fn with_context(self, operation: String) -> Self {
        ControlError::Context {
            operation,
            source: Box::new(self),
        }
    }
}

/// Validates a protocol version returned by the daemon.
pub fn validate_protocol(actual: u32) -> Result<(), ControlError> {
    if actual == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ControlError::IncompatibleProtocol {
            expected: PROTOCOL_VERSION,
            actual,
        })
    }
}

/// Maps a gRPC status code to a stable CLI diagnostic category.
pub fn diagnostic_category(code: Code) -> &'static str {
    match code {
        Code::Unavailable => "daemon_unavailable",
        Code::FailedPrecondition => "daemon_precondition",
        Code::Unimplemented => "daemon_unsupported",
        Code::InvalidArgument => "daemon_invalid_request",
        _ => "daemon_error",
    }
}

#[derive(Clone)]
struct ControlService {
    paths: StatePaths,
    metadata: SessionMetadata,
    shutdown: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl ControlService {
    fn protocol_error(&self, version: u32) -> Option<Status> {
        if version == PROTOCOL_VERSION {
            None
        } else {
            Some(Status::failed_precondition(format!(
                "incompatible protocol version {version}; daemon requires {PROTOCOL_VERSION}"
            )))
        }
    }
}

#[tonic::async_trait]
impl v1::control_server::Control for ControlService {
    async fn check_protocol(
        &self,
        request: Request<v1::ProtocolRequest>,
    ) -> Result<Response<v1::ProtocolResponse>, Status> {
        if let Some(error) = self.protocol_error(request.into_inner().protocol_version) {
            return Err(error);
        }
        Ok(Response::new(v1::ProtocolResponse {
            protocol_version: PROTOCOL_VERSION,
        }))
    }

    async fn status(
        &self,
        request: Request<v1::StatusRequest>,
    ) -> Result<Response<v1::StatusResponse>, Status> {
        if let Some(error) = self.protocol_error(request.into_inner().protocol_version) {
            return Err(error);
        }
        Ok(Response::new(v1::StatusResponse {
            mounted: false,
            root_digest: self.metadata.root_digest.to_string(),
            mountpoint: self.metadata.mountpoint.to_string_lossy().into_owned(),
            cached_blobs: 0,
            dirty_files: 0,
            protocol_version: PROTOCOL_VERSION,
            daemon_pid: self.metadata.daemon_pid,
            control_socket: self.paths.control_socket().to_string_lossy().into_owned(),
            cache_path: self.paths.cache().to_string_lossy().into_owned(),
            session_path: self.paths.active().to_string_lossy().into_owned(),
            dirty: false,
            snapshot_blockers: vec!["FUSE mount is not implemented".into()],
        }))
    }

    async fn snapshot(
        &self,
        request: Request<v1::SnapshotRequest>,
    ) -> Result<Response<v1::SnapshotResponse>, Status> {
        if let Some(error) = self.protocol_error(request.into_inner().protocol_version) {
            return Err(error);
        }
        Err(Status::unimplemented("snapshot is not implemented yet"))
    }

    async fn unmount(
        &self,
        request: Request<v1::UnmountRequest>,
    ) -> Result<Response<v1::UnmountResponse>, Status> {
        if let Some(error) = self.protocol_error(request.into_inner().protocol_version) {
            return Err(error);
        }
        let sender =
            self.shutdown.lock().await.take().ok_or_else(|| {
                Status::failed_precondition("daemon shutdown is already in progress")
            })?;
        sender
            .send(())
            .map_err(|_| Status::unavailable("daemon shutdown channel is closed"))?;
        Ok(Response::new(v1::UnmountResponse {}))
    }
}

/// Serves the active-session control API until Ctrl-C or a successful unmount request.
///
/// The socket is created at [`StatePaths::control_socket`] with mode `0600` and
/// removed before this function returns. Socket and transport failures retain
/// the socket path and serving operation in their error chain.
pub async fn serve(paths: StatePaths, metadata: SessionMetadata) -> Result<(), ControlError> {
    let socket = paths.control_socket();
    let listener = UnixListener::bind(&socket)
        .map_err(|source| ControlError::Socket {
            path: socket.clone(),
            source,
        })
        .with_context(|| format!("unable to start listening on socket {}", socket.display()))?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))
        .map_err(|source| ControlError::Socket {
            path: socket.clone(),
            source,
        })
        .with_context(|| {
            format!(
                "unable to set read-write permissions on socket {}",
                socket.display()
            )
        })?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = ControlService {
        paths,
        metadata,
        shutdown: Arc::new(Mutex::new(Some(shutdown_tx))),
    };
    let shutdown = async move {
        tokio::select! {
            _ = shutdown_rx => {},
            _ = tokio::signal::ctrl_c() => {},
        }
    };
    let result = Server::builder()
        .add_service(v1::control_server::ControlServer::new(service))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await;
    let remove_result = tokio::fs::remove_file(&socket).await;
    if let Err(source) = remove_result
        && source.kind() != std::io::ErrorKind::NotFound
    {
        return Err(ControlError::Socket {
            path: socket,
            source,
        });
    }
    result
        .map_err(ControlError::Transport)
        .with_context(|| format!("serve daemon control API on {}", socket.display()))
}

/// Connects to a daemon control socket and verifies protocol compatibility.
pub async fn connect(
    path: &Path,
) -> Result<v1::control_client::ControlClient<Channel>, ControlError> {
    let path = path.to_path_buf();
    let connector_path = path.clone();
    let endpoint = Endpoint::try_from("http://[::]:50051")
        .map_err(ControlError::Transport)
        .context("construct local daemon control endpoint")?;
    let channel = endpoint
        .connect_with_connector(service_fn(move |_| {
            UnixStream::connect(connector_path.clone())
        }))
        .await
        .map_err(ControlError::Transport)
        .with_context(|| format!("connect to daemon control socket {}", path.display()))?;
    let mut client = v1::control_client::ControlClient::new(channel);
    let response = client
        .check_protocol(v1::ProtocolRequest {
            protocol_version: PROTOCOL_VERSION,
        })
        .await
        .map_err(|status| ControlError::Rpc(Box::new(status)))?
        .into_inner();
    validate_protocol(response.protocol_version)
        .with_context(|| format!("check daemon protocol on {}", path.display()))?;
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_conversion_rejects_unknown_versions() {
        assert!(validate_protocol(PROTOCOL_VERSION).is_ok());
        assert!(matches!(
            validate_protocol(99),
            Err(ControlError::IncompatibleProtocol { .. })
        ));
    }

    #[test]
    fn grpc_codes_have_stable_diagnostics() {
        assert_eq!(diagnostic_category(Code::Unavailable), "daemon_unavailable");
        assert_eq!(
            diagnostic_category(Code::FailedPrecondition),
            "daemon_precondition"
        );
        assert_eq!(
            diagnostic_category(Code::Unimplemented),
            "daemon_unsupported"
        );
    }
}
