use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rfs_common::control_protocol as protocol;
use rfs_common::session::{Session, SessionError, SessionInfo};
use thiserror::Error;
use tokio::net::UnixListener;
use tokio::sync::{Mutex as AsyncMutex, oneshot};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::fuse::FuseMount;

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
    State(#[from] SessionError),
    #[error("access daemon resources during control-service teardown: lock is poisoned")]
    ResourceLockPoisoned,
}

#[derive(Clone)]
struct ControlService {
    info: SessionInfo,
    mount: Arc<Mutex<Option<FuseMount>>>,
    shutdown: Arc<AsyncMutex<Option<oneshot::Sender<()>>>>,
}

impl ControlService {
    fn protocol_error(&self, version: u32) -> Option<Status> {
        if version == PROTOCOL_VERSION {
            return None;
        }
        Some(Status::failed_precondition(format!(
            "incompatible protocol version {version}; daemon requires {PROTOCOL_VERSION}"
        )))
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
            mounted: true,
            root_digest: self.info.root_digest.to_string(),
            mountpoint: self.info.mountpoint.to_string_lossy().into_owned(),
            dirty_files: 0,
            protocol_version: PROTOCOL_VERSION,
            daemon_pid: self.info.daemon_pid,
            control_socket: self.info.control_endpoint.to_string_lossy().into_owned(),
            dirty: false,
            snapshot_blockers: vec!["snapshot is not implemented".into()],
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
        let mount = self
            .mount
            .lock()
            .map_err(|_| Status::internal("FUSE mount lock is poisoned"))?
            .take()
            .ok_or_else(|| Status::failed_precondition("daemon shutdown is already in progress"))?;
        tokio::task::spawn_blocking(move || mount.unmount())
            .await
            .map_err(|error| {
                tracing::error!(operation = "unmount", error = %error, "FUSE teardown task failed");
                Status::internal("daemon could not unmount its FUSE session cleanly")
            })?;
        remove_socket(&self.info.control_endpoint)
            .await
            .map_err(|error| {
                tracing::error!(operation = "unmount", error = %error, "control socket removal failed");
                Status::internal("daemon could not remove its control socket cleanly")
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

pub(crate) async fn serve(session: Arc<Session>, mount: FuseMount) -> Result<(), ControlError> {
    let info = session.info();
    let socket = info.control_endpoint.clone();
    let listener = match prepare_listener(&socket) {
        Ok(listener) => listener,
        Err(error) => {
            tokio::task::spawn_blocking(move || mount.unmount())
                .await
                .map_err(|_| ControlError::ResourceLockPoisoned)?;
            remove_socket(&socket).await?;
            session.close()?;
            return Err(error);
        }
    };
    let mount = Arc::new(Mutex::new(Some(mount)));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = ControlService {
        info,
        mount: Arc::clone(&mount),
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
    let remaining_mount = mount
        .lock()
        .map_err(|_| ControlError::ResourceLockPoisoned)?
        .take();
    if let Some(mount) = remaining_mount {
        tokio::task::spawn_blocking(move || mount.unmount())
            .await
            .map_err(|_| ControlError::ResourceLockPoisoned)?;
    }
    remove_socket(&socket).await?;
    session.close()?;
    result.map_err(ControlError::Transport)
}

async fn remove_socket(socket: &PathBuf) -> Result<(), ControlError> {
    match tokio::fs::remove_file(socket).await {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ControlError::Socket {
            path: socket.clone(),
            source,
        }),
    }
}

fn prepare_listener(socket: &PathBuf) -> Result<UnixListener, ControlError> {
    let listener = UnixListener::bind(socket).map_err(|source| ControlError::Socket {
        path: socket.clone(),
        source,
    })?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600)).map_err(|source| {
        ControlError::Socket {
            path: socket.clone(),
            source,
        }
    })?;
    Ok(listener)
}
