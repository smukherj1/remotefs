//! Unix control service and the single owner of daemon shutdown.
//!
//! The control service holds every daemon resource in one `Option`. Shutdown,
//! whether requested over RPC, by a termination signal, or caused by the
//! server exiting, detaches the resources exactly once and hands them to
//! `serve`, which releases them inline before stopping the server.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use rfs_common::control_protocol as protocol;
use rfs_common::error_context::{ResultContext, ResultContextError};
use rfs_common::session::{Session, SessionError, SessionInfo};
use thiserror::Error;
use tokio::net::UnixListener;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::fuse::FuseMount;

/// Control protocol version implemented by this daemon.
const PROTOCOL_VERSION: u32 = 1;

/// Failures while serving the control socket or releasing daemon resources.
#[derive(Debug, Error)]
pub(crate) enum ControlError {
    /// A filesystem operation on the control socket path failed.
    #[error("control socket operation on `{path}` failed")]
    Socket {
        /// Control socket path that was being changed.
        path: PathBuf,
        /// Underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// The tonic server stopped with an error.
    #[error("control transport failed")]
    Transport(#[source] tonic::transport::Error),
    /// A termination signal handler could not be installed.
    #[error("install termination signal handler")]
    Signal(#[source] std::io::Error),
    /// The session could not durably close.
    #[error("close daemon state")]
    State(#[source] SessionError),
    /// A blocking release step panicked or was cancelled.
    #[error("join blocking release task")]
    Task(#[source] JoinError),
    /// Adds the operation that was being attempted to a failure.
    #[error("{operation}")]
    Context {
        /// Operation that failed.
        operation: String,
        /// Original failure.
        #[source]
        source: Box<ControlError>,
    },
}

/// Resources owned by one fully constructed daemon.
pub(crate) struct DaemonResources {
    /// Session state and the home ownership lock.
    session: Arc<Session>,
    /// Live FUSE mount.
    mount: FuseMount,
    /// Control socket path created by this daemon's bind.
    socket: PathBuf,
}

/// SIGINT and SIGTERM streams that request a daemon shutdown.
pub(crate) struct TerminationSignals {
    /// Interrupt signal, sent by Ctrl-C.
    interrupt: Signal,
    /// Termination signal, sent by `kill` and process supervisors.
    terminate: Signal,
}

impl ResultContextError for ControlError {
    fn with_context(self, operation: String) -> Self {
        Self::Context {
            operation,
            source: Box::new(self),
        }
    }
}

impl DaemonResources {
    /// Groups the resources of a fully constructed daemon.
    ///
    /// Inputs: the open `session`, the live FUSE `mount` that serves it, and
    /// the `socket` path this daemon bound. Returns the owner that `serve`
    /// releases on shutdown. Has no side effects.
    pub(crate) fn new(session: Arc<Session>, mount: FuseMount, socket: PathBuf) -> Self {
        Self {
            session,
            mount,
            socket,
        }
    }

    /// Unmounts FUSE, durably closes the session, and removes the socket, in
    /// that order.
    ///
    /// Consumes `self`, so release runs at most once. Stops at the first
    /// failure; the remaining resources are dropped and the caller must treat
    /// daemon state as undefined.
    ///
    /// Errors: `Task` (wrapped in `Context` naming the step) when a blocking
    /// step panics, `State` when the session cannot close, and `Socket`
    /// (wrapped in `Context`) when the socket cannot be removed. A socket that is already
    /// gone counts as removed.
    pub(crate) async fn release(self) -> Result<(), ControlError> {
        let Self {
            session,
            mount,
            socket,
        } = self;
        tokio::task::spawn_blocking(move || mount.unmount())
            .await
            .map_err(ControlError::Task)
            .context("unmount FUSE")?;
        // The outer `Result` is the join of the blocking task; the inner one
        // is the result of `Session::close` itself.
        let close_result = tokio::task::spawn_blocking(move || session.close())
            .await
            .map_err(ControlError::Task)
            .context("join session close task")?;
        // `State` already names the step, so no extra context is added.
        close_result.map_err(ControlError::State)?;
        remove_socket(&socket)
            .await
            .context("remove control socket")?;
        tracing::info!(operation = "shutdown", "daemon resource release completed");
        Ok(())
    }
}

impl TerminationSignals {
    /// Installs the SIGINT and SIGTERM handlers.
    ///
    /// Must run inside a Tokio runtime. From then on these signals no longer
    /// terminate the process; they are delivered to `recv`.
    ///
    /// Errors: `Signal` when either handler cannot be installed.
    pub(crate) fn install() -> Result<Self, ControlError> {
        Ok(Self {
            interrupt: signal(SignalKind::interrupt()).map_err(ControlError::Signal)?,
            terminate: signal(SignalKind::terminate()).map_err(ControlError::Signal)?,
        })
    }

    /// Waits until either SIGINT or SIGTERM arrives.
    async fn wait(&mut self) {
        tokio::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
        }
    }
}

/// Serves control RPCs until resources are detached, releases them, then
/// drains tonic.
///
/// Inputs: the daemon's `resources`, the bound control `listener`, and the
/// installed termination `signals`. Shutdown starts on the first of: a
/// `Shutdown` RPC, SIGINT or SIGTERM, or the server exiting on its own.
///
/// Side effects: releases every daemon resource exactly once. While release
/// runs, new requests on open connections get `unavailable`.
///
/// Returns a `Transport` error if the server failed; any release error is
/// then logged. Otherwise returns the release result.
pub(crate) async fn serve(
    resources: DaemonResources,
    listener: UnixListener,
    mut signals: TerminationSignals,
) -> Result<(), ControlError> {
    let (resource_tx, mut resource_rx) = mpsc::channel(1);
    let service = ControlService {
        resources: Arc::new(Mutex::new(Some(resources))),
        resource_tx,
    };
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let server = Server::builder()
        .add_service(protocol::control_server::ControlServer::new(
            service.clone(),
        ))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async {
            // A dropped sender also stops the server.
            let _ = stop_rx.await;
        });
    tokio::pin!(server);

    let (resources, finished_server) = tokio::select! {
        // `recv` cannot return `None` because `service` keeps a sender alive.
        Some(resources) = resource_rx.recv() => (resources, None),
        () = signals.wait() => {
            tracing::info!(operation = "shutdown", "termination signal received");
            (detach_or_receive(&service, &mut resource_rx).await, None)
        }
        result = &mut server => {
            tracing::warn!(operation = "shutdown", "control server exited before shutdown");
            (detach_or_receive(&service, &mut resource_rx).await, Some(result))
        }
    };
    let release_result = resources.release().await;

    // The server keeps running during release so it can finish in-flight
    // requests, including the `Shutdown` response. Stop it now.
    let _ = stop_tx.send(());
    let server_result = match finished_server {
        Some(result) => result,
        None => server.await,
    };
    let Err(transport_error) = server_result else {
        return release_result;
    };
    if let Err(release_error) = release_result {
        tracing::error!(
            operation = "shutdown",
            error = ?anyhow::Error::from(release_error),
            "daemon resource release failed after control transport failure"
        );
    }
    Err(ControlError::Transport(transport_error))
}

/// Control RPC handler that shares daemon resources with `serve`.
#[derive(Clone)]
struct ControlService {
    /// `Some` until shutdown detaches it; `None` makes every endpoint unavailable.
    resources: Arc<Mutex<Option<DaemonResources>>>,
    /// Sends the resources released by `ControlService` to `serve` during
    /// shutdown; capacity 1.
    resource_tx: mpsc::Sender<DaemonResources>,
}

impl ControlService {
    /// Takes the resources and returns them, or `None` if already detached.
    fn detach(&self) -> Option<DaemonResources> {
        self.lock_resources().take()
    }

    /// Locks the resource slot.
    ///
    /// Recovers from poisoning: code under the lock only reads or takes the
    /// option, so a panic cannot leave it half-updated.
    fn lock_resources(&self) -> MutexGuard<'_, Option<DaemonResources>> {
        self.resources
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Returns the active session's startup facts for a request.
    ///
    /// Input: the client's protocol `version`. Errors: `unavailable` once
    /// shutdown has detached the resources, then `failed_precondition` for an
    /// incompatible version.
    #[allow(clippy::result_large_err)] // `Status` is what the RPC handlers return.
    fn active_info(&self, version: u32) -> Result<SessionInfo, Status> {
        let resources = self.lock_resources();
        let Some(resources) = resources.as_ref() else {
            return Err(shutting_down());
        };
        validate_protocol(version)?;
        Ok(resources.session.info())
    }
}

#[tonic::async_trait]
impl protocol::control_server::Control for ControlService {
    async fn check_protocol(
        &self,
        request: Request<protocol::ProtocolRequest>,
    ) -> Result<Response<protocol::ProtocolResponse>, Status> {
        self.active_info(request.into_inner().protocol_version)?;
        Ok(Response::new(protocol::ProtocolResponse {
            protocol_version: PROTOCOL_VERSION,
        }))
    }

    async fn status(
        &self,
        request: Request<protocol::StatusRequest>,
    ) -> Result<Response<protocol::StatusResponse>, Status> {
        let info = self.active_info(request.into_inner().protocol_version)?;
        Ok(Response::new(protocol::StatusResponse {
            mounted: true,
            root_digest: info.root_digest.to_string(),
            mountpoint: info.mountpoint.to_string_lossy().into_owned(),
            dirty_files: 0,
            protocol_version: PROTOCOL_VERSION,
            daemon_pid: info.daemon_pid,
            control_socket: info.control_endpoint.to_string_lossy().into_owned(),
            dirty: false,
            snapshot_blockers: vec!["snapshot is not implemented".into()],
        }))
    }

    async fn snapshot(
        &self,
        request: Request<protocol::SnapshotRequest>,
    ) -> Result<Response<protocol::SnapshotResponse>, Status> {
        self.active_info(request.into_inner().protocol_version)?;
        Err(Status::unimplemented("snapshot is not implemented yet"))
    }

    /// Detaches the resources and hands them to `serve` for release.
    ///
    /// Everything runs under one lock with no `.await`, so client
    /// cancellation cannot lose the resources, and once the option is `None`
    /// they are either released or waiting in the channel. An incompatible
    /// request never detaches anything. Returns once the handoff is done, not
    /// when release finishes.
    async fn shutdown(
        &self,
        request: Request<protocol::ShutdownRequest>,
    ) -> Result<Response<protocol::ShutdownResponse>, Status> {
        let version = request.into_inner().protocol_version;
        let mut resources = self.lock_resources();
        if resources.is_none() {
            return Err(shutting_down());
        }
        validate_protocol(version)?;
        if let Some(detached) = resources.take() {
            // Cannot fail: only one caller can take the resources, the
            // channel has capacity 1, and `serve` holds the receiver.
            self.resource_tx
                .try_send(detached)
                .map_err(|_| Status::internal("hand off daemon resources for release"))?;
        }
        tracing::info!(operation = "shutdown", "shutdown request accepted");
        Ok(Response::new(protocol::ShutdownResponse {}))
    }
}

/// Returns the detached resources for signal or server-exit shutdown.
///
/// Detaches them from `service` directly, skipping protocol validation. If a
/// `Shutdown` RPC detached them first, receives them from `resource_rx` instead;
/// the RPC sends under the same lock that emptied the option, so they are
/// already in the channel.
async fn detach_or_receive(
    service: &ControlService,
    resource_rx: &mut mpsc::Receiver<DaemonResources>,
) -> DaemonResources {
    if let Some(resources) = service.detach() {
        return resources;
    }
    resource_rx
        .recv()
        .await
        .expect("`service` holds a sender, so the handoff channel stays open")
}

/// Checks that the client speaks this daemon's protocol version.
///
/// Errors: `failed_precondition` naming both versions when they differ.
#[allow(clippy::result_large_err)] // `Status` is what the RPC handlers return.
fn validate_protocol(version: u32) -> Result<(), Status> {
    if version == PROTOCOL_VERSION {
        return Ok(());
    }
    Err(Status::failed_precondition(format!(
        "incompatible protocol version {version}; daemon requires {PROTOCOL_VERSION}"
    )))
}

/// Status returned by every endpoint after shutdown detached the resources.
fn shutting_down() -> Status {
    Status::unavailable("daemon is shutting down")
}

/// Removes the control socket at `socket`.
///
/// A missing socket counts as removed. Errors: `Socket` for any other I/O
/// failure.
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
