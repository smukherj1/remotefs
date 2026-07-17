use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rfs_common::control_protocol as protocol;
use rfs_common::state::{DaemonState, SessionView, StateError};
use thiserror::Error;
use tokio::net::UnixListener;
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub(crate) enum ControlError {
    #[error("control socket operation on `{path}` failed: {source}")]
    Socket {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("control transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("close daemon state: {0}")]
    State(#[from] StateError),
    #[error("access daemon state during control-service teardown: state lock is poisoned")]
    StateLockPoisoned,
}

#[derive(Clone)]
struct ControlService {
    session: SessionView,
    state: Arc<Mutex<Option<Box<dyn DaemonState>>>>,
    shutdown: Arc<AsyncMutex<Option<oneshot::Sender<()>>>>,
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
impl protocol::control_server::Control for ControlService {
    async fn check_protocol(
        &self,
        request: Request<protocol::ProtocolRequest>,
    ) -> Result<Response<protocol::ProtocolResponse>, Status> {
        if let Some(error) = self.protocol_error(request.into_inner().protocol_version) {
            return Err(error);
        }
        Ok(Response::new(protocol::ProtocolResponse {
            protocol_version: PROTOCOL_VERSION,
        }))
    }

    async fn status(
        &self,
        request: Request<protocol::StatusRequest>,
    ) -> Result<Response<protocol::StatusResponse>, Status> {
        if let Some(error) = self.protocol_error(request.into_inner().protocol_version) {
            return Err(error);
        }
        Ok(Response::new(protocol::StatusResponse {
            mounted: false,
            root_digest: self.session.root_digest.to_string(),
            mountpoint: self.session.mountpoint.to_string_lossy().into_owned(),
            cached_blobs: 0,
            dirty_files: 0,
            protocol_version: PROTOCOL_VERSION,
            daemon_pid: self.session.daemon_pid,
            control_socket: self.session.control_endpoint.to_string_lossy().into_owned(),
            cache_path: self.session.cache_path.to_string_lossy().into_owned(),
            session_path: self.session.session_path.to_string_lossy().into_owned(),
            dirty: false,
            snapshot_blockers: vec!["FUSE mount is not implemented".into()],
        }))
    }

    async fn snapshot(
        &self,
        request: Request<protocol::SnapshotRequest>,
    ) -> Result<Response<protocol::SnapshotResponse>, Status> {
        if let Some(error) = self.protocol_error(request.into_inner().protocol_version) {
            return Err(error);
        }
        Err(Status::unimplemented("snapshot is not implemented yet"))
    }

    async fn unmount(
        &self,
        request: Request<protocol::UnmountRequest>,
    ) -> Result<Response<protocol::UnmountResponse>, Status> {
        if let Some(error) = self.protocol_error(request.into_inner().protocol_version) {
            return Err(error);
        }
        let state = self
            .state
            .lock()
            .map_err(|_| Status::internal("daemon state lock is poisoned"))?
            .take()
            .ok_or_else(|| Status::failed_precondition("daemon shutdown is already in progress"))?;
        state.close().map_err(|error| {
            tracing::error!(operation = "unmount", error = %error, "clean session close failed");
            Status::internal("daemon could not close its session cleanly")
        })?;
        self.shutdown
            .lock()
            .await
            .take()
            .ok_or_else(|| Status::failed_precondition("daemon shutdown is already in progress"))?
            .send(())
            .map_err(|_| Status::unavailable("daemon shutdown channel is closed"))?;
        tracing::info!(operation = "unmount", "session teardown completed");
        Ok(Response::new(protocol::UnmountResponse {}))
    }
}

pub(crate) async fn serve(state: Box<dyn DaemonState>) -> Result<(), ControlError> {
    let session = state.session()?.ok_or_else(|| StateError::StaleSession {
        path: PathBuf::new(),
        reason: "daemon state has no active session".into(),
    })?;
    let socket = session.control_endpoint.clone();
    let listener = UnixListener::bind(&socket).map_err(|source| ControlError::Socket {
        path: socket.clone(),
        source,
    })?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).map_err(
        |source| ControlError::Socket {
            path: socket.clone(),
            source,
        },
    )?;
    let state = Arc::new(Mutex::new(Some(state)));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = ControlService {
        session,
        state: Arc::clone(&state),
        shutdown: Arc::new(AsyncMutex::new(Some(shutdown_tx))),
    };
    let shutdown = async move {
        tokio::select! {
            _ = shutdown_rx => {},
            _ = tokio::signal::ctrl_c() => {},
        }
    };
    let result = Server::builder()
        .add_service(protocol::control_server::ControlServer::new(service))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await;
    let remaining_state = state
        .lock()
        .map_err(|_| ControlError::StateLockPoisoned)?
        .take();
    if let Some(state) = remaining_state {
        state.close()?;
    }
    if let Err(source) = tokio::fs::remove_file(&socket).await
        && source.kind() != std::io::ErrorKind::NotFound
    {
        return Err(ControlError::Socket {
            path: socket,
            source,
        });
    }
    result.map_err(ControlError::Transport)
}
