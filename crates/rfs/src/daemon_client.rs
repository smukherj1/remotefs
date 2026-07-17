//! Command-oriented client for one immutable RemoteFS daemon session.

use std::path::PathBuf;

use rfs_common::control_protocol as protocol;
use rfs_common::diagnostics::Diagnostic;
use rfs_common::digest::Digest;
use thiserror::Error;
use tokio::net::UnixStream;
use tonic::Code;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

const PROTOCOL_VERSION: u32 = 1;

/// Unix control endpoint discovered through read-only session state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlEndpoint(pub PathBuf);

/// Identity returned after protocol readiness validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonIdentity {
    /// Compatible protocol version.
    pub protocol_version: u32,
}

/// Domain status returned by the daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionStatus {
    pub root_digest: Digest,
    pub mountpoint: PathBuf,
    pub daemon_pid: u32,
    pub control_socket: PathBuf,
    pub cache_path: PathBuf,
    pub session_path: PathBuf,
    pub dirty: bool,
    pub dirty_files: u64,
    pub cached_blobs: u64,
    pub snapshot_blockers: Vec<String>,
}

/// Stable daemon client failures that do not expose tonic transport types.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("connect to daemon endpoint `{endpoint}`: {message}")]
    Connect { endpoint: PathBuf, message: String },
    #[error("daemon protocol version {actual} is incompatible; client requires {expected}")]
    IncompatibleProtocol { expected: u32, actual: u32 },
    #[error("daemon request failed ({code}): {message}")]
    Remote { code: &'static str, message: String },
    #[error("daemon returned invalid root digest `{value}`: {message}")]
    InvalidResponse { value: String, message: String },
}

impl ClientError {
    /// Stable diagnostic category suitable for CLI rendering.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Connect { .. } => "daemon_unavailable",
            Self::IncompatibleProtocol { .. } => "daemon_protocol",
            Self::Remote { code, .. } => code,
            Self::InvalidResponse { .. } => "daemon_invalid_response",
        }
    }
}

impl Diagnostic for ClientError {
    fn code(&self) -> &'static str {
        self.code()
    }
}

/// Concrete client for command calls to a session daemon.
pub struct DaemonClient {
    inner: protocol::control_client::ControlClient<Channel>,
}

impl DaemonClient {
    /// Connects to a Unix endpoint and validates protocol compatibility.
    pub async fn connect(endpoint: ControlEndpoint) -> Result<Self, ClientError> {
        let path = endpoint.0;
        let connector_path = path.clone();
        let channel = Endpoint::try_from("http://[::]:50051")
            .map_err(|error| ClientError::Connect {
                endpoint: path.clone(),
                message: error.to_string(),
            })?
            .connect_with_connector(service_fn(move |_| {
                UnixStream::connect(connector_path.clone())
            }))
            .await
            .map_err(|error| ClientError::Connect {
                endpoint: path,
                message: error.to_string(),
            })?;
        let mut client = Self {
            inner: protocol::control_client::ControlClient::new(channel),
        };
        client.ready().await?;
        Ok(client)
    }

    /// Checks protocol readiness and returns the daemon identity.
    pub async fn ready(&mut self) -> Result<DaemonIdentity, ClientError> {
        let response = self
            .inner
            .check_protocol(protocol::ProtocolRequest {
                protocol_version: PROTOCOL_VERSION,
            })
            .await
            .map_err(remote_error)?
            .into_inner();
        if response.protocol_version != PROTOCOL_VERSION {
            return Err(ClientError::IncompatibleProtocol {
                expected: PROTOCOL_VERSION,
                actual: response.protocol_version,
            });
        }
        Ok(DaemonIdentity {
            protocol_version: response.protocol_version,
        })
    }

    /// Returns mounted-session status as domain values.
    pub async fn status(&mut self) -> Result<SessionStatus, ClientError> {
        let response = self
            .inner
            .status(protocol::StatusRequest {
                protocol_version: PROTOCOL_VERSION,
            })
            .await
            .map_err(remote_error)?
            .into_inner();
        let root_digest = response.root_digest.parse::<Digest>().map_err(|error| {
            ClientError::InvalidResponse {
                value: response.root_digest.clone(),
                message: error.to_string(),
            }
        })?;
        Ok(SessionStatus {
            root_digest,
            mountpoint: response.mountpoint.into(),
            daemon_pid: response.daemon_pid,
            control_socket: response.control_socket.into(),
            cache_path: response.cache_path.into(),
            session_path: response.session_path.into(),
            dirty: response.dirty,
            dirty_files: response.dirty_files,
            cached_blobs: response.cached_blobs,
            snapshot_blockers: response.snapshot_blockers,
        })
    }

    /// Requests a snapshot and returns its root digest.
    #[allow(dead_code)] // Kept for the Phase 8 command without exposing this internal module.
    pub async fn snapshot(&mut self) -> Result<Digest, ClientError> {
        let response = self
            .inner
            .snapshot(protocol::SnapshotRequest {
                protocol_version: PROTOCOL_VERSION,
            })
            .await
            .map_err(remote_error)?
            .into_inner();
        response
            .root_digest
            .parse::<Digest>()
            .map_err(|error| ClientError::InvalidResponse {
                value: response.root_digest,
                message: error.to_string(),
            })
    }

    /// Completes daemon teardown; this client is consumed after the request.
    pub async fn unmount(mut self) -> Result<(), ClientError> {
        self.inner
            .unmount(protocol::UnmountRequest {
                protocol_version: PROTOCOL_VERSION,
            })
            .await
            .map_err(remote_error)?;
        Ok(())
    }
}

fn remote_error(status: tonic::Status) -> ClientError {
    ClientError::Remote {
        code: match status.code() {
            Code::Unavailable => "daemon_unavailable",
            Code::FailedPrecondition => "daemon_precondition",
            Code::Unimplemented => "daemon_unsupported",
            Code::InvalidArgument => "daemon_invalid_request",
            _ => "daemon_error",
        },
        message: status.message().to_owned(),
    }
}
