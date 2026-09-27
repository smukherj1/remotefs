//! Daemon-owned startup, state lifecycle, and control service.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use rfs_common::cas::{CasClient, CasConfig};
use rfs_common::config::Config;
use rfs_common::digest::Digest;
use rfs_common::logging::{self, LogFormat};
use rfs_common::session::Session;
use tokio::net::UnixListener;

use crate::control_service::{DaemonResources, TerminationSignals};

mod control_service;
pub mod filesystem;
mod fuse;

/// Command-line arguments accepted by `rfsd`.
#[derive(Parser, Debug)]
#[command(
    name = "rfsd",
    version,
    about = "RemoteFS Mount Daemon - manages FUSE mount and local session state",
    long_about = "rfsd is the background daemon that owns the FUSE mount, lazy metadata/blob retrieval, SQLite transaction index, copy-on-write overlay, and the CLI control socket."
)]
pub struct Cli {
    #[arg(help = "Root digest of the snapshot to mount (e.g., sha256:<hex>/<size>)")]
    root_digest: String,
    #[arg(help = "Path where the FUSE filesystem should be mounted")]
    mountpoint: PathBuf,
    #[arg(
        long,
        help = "Remote Execution API CAS endpoint (e.g., grpc://127.0.0.1:9092)"
    )]
    cas_url: Option<String>,
    #[arg(long, help = "Remote Execution API instance name")]
    instance_name: Option<String>,
    #[arg(long, default_value = "info", value_enum, help = "Log level")]
    log_level: LogLevel,
    #[arg(long, default_value = "text", value_enum, help = "Daemon log format")]
    output_format: OutputFormat,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
}

impl LogLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

/// Installs signal handlers, constructs the daemon, and serves it until release.
///
/// Signal handlers are installed first so that a failed installation has
/// nothing to clean up. Returns `Ok(())` only after every daemon resource was
/// released and the session durably closed.
///
/// Errors: signal installation, any startup step (see `construct_daemon`),
/// a control transport failure, or a failed release. After a failed release
/// the daemon state is undefined and the process should exit.
pub async fn run(cli: Cli) -> Result<()> {
    let signals = TerminationSignals::install().context("install daemon termination signals")?;
    let (resources, listener) = construct_daemon(cli).await.context("construct daemon")?;
    if let Err(error) = control_service::serve(resources, listener, signals).await {
        let error = anyhow::Error::from(error).context("serve and release daemon resources");
        tracing::error!(operation = "daemon_stop", error = ?error, "daemon shutdown failed");
        return Err(error);
    }
    tracing::info!(operation = "daemon_stop", "daemon session closed");
    Ok(())
}

/// Constructs all daemon dependencies.
///
/// Steps, in order: resolve configuration and connect to CAS; open the
/// session and initialize logging; validate the root directory; mount FUSE;
/// bind the control socket and restrict its permissions.
///
/// Returns the complete resources and the bound control listener. On failure,
/// drops anything acquired and returns the startup error: dropping the FUSE
/// mount unmounts it and dropping the session releases the home lock. The
/// session is not marked `closed`, so retained state looks like an unclean
/// exit, which the next `Session::open` replaces.
async fn construct_daemon(cli: Cli) -> Result<(DaemonResources, UnixListener)> {
    let digest: Digest = cli
        .root_digest
        .parse()
        .with_context(|| format!("parse daemon root digest {}", cli.root_digest))?;
    let cas = connect_cas(cli.cas_url, cli.instance_name)
        .await
        .context("set up daemon CAS client")?;
    let config = Config::new().context("load daemon state configuration")?;
    let session = Arc::new(
        Session::open(
            config,
            digest,
            &cli.mountpoint,
            Box::new(cas),
            tokio::runtime::Handle::current(),
        )
        .with_context(|| format!("create daemon session for {}", cli.mountpoint.display()))?,
    );
    let info = session.info();
    logging::init_daemon(
        &info.log_path,
        cli.log_level.as_str(),
        match cli.output_format {
            OutputFormat::Text => LogFormat::Text,
            OutputFormat::Json => LogFormat::Json,
        },
    )
    .context("initialize daemon session logging")?;
    tracing::info!(
        operation = "daemon_start",
        mountpoint = %info.mountpoint.display(),
        digest = %info.root_digest,
        "daemon session active"
    );
    let filesystem_session = Arc::clone(&session);
    let filesystem =
        tokio::task::spawn_blocking(move || filesystem::FilesystemService::new(filesystem_session))
            .await
            .context("join root-directory validation task")?
            .context("validate root directory before FUSE mount")?;
    let mount = fuse::FuseMount::mount(Arc::new(filesystem), &info.mountpoint)
        .with_context(|| format!("mount FUSE filesystem at {}", info.mountpoint.display()))?;
    let listener = bind_control_socket(&info.control_endpoint).context("set up control socket")?;
    Ok((
        DaemonResources::new(session, mount, info.control_endpoint),
        listener,
    ))
}

/// Resolves CAS settings and connects to CAS.
///
/// Inputs: the `--cas-url` and `--instance-name` flags, which override
/// `RFS_CAS_URL` and `RFS_INSTANCE_NAME`. Returns a connected client.
///
/// Errors: a missing or invalid setting, or a failed connection.
async fn connect_cas(cas_url: Option<String>, instance_name: Option<String>) -> Result<CasClient> {
    let cas_url = cas_url
        .or_else(|| std::env::var("RFS_CAS_URL").ok())
        .context("missing CAS URL; pass --cas-url or set RFS_CAS_URL")?;
    let instance_name = instance_name
        .or_else(|| std::env::var("RFS_INSTANCE_NAME").ok())
        .context("missing instance name; pass --instance-name or set RFS_INSTANCE_NAME")?;
    let cas_config =
        CasConfig::new(cas_url, instance_name).context("validate daemon CAS configuration")?;
    CasClient::connect(cas_config)
        .await
        .context("connect daemon to CAS")
}

/// Binds the control socket at `socket` and makes it owner-only (0600).
///
/// Side effects: creates the socket file. If setting permissions fails, the
/// socket just created is removed before returning. A failed bind never
/// removes an existing path.
///
/// Errors: the bind or permission change failed.
fn bind_control_socket(socket: &Path) -> Result<UnixListener> {
    let listener = UnixListener::bind(socket)
        .with_context(|| format!("bind control socket {}", socket.display()))?;
    if let Err(error) = std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600)) {
        if let Err(remove_error) = std::fs::remove_file(socket) {
            tracing::warn!(
                operation = "daemon_start",
                socket = %socket.display(),
                error = %remove_error,
                "could not remove control socket after permission failure"
            );
        }
        return Err(error)
            .with_context(|| format!("restrict control socket permissions {}", socket.display()));
    }
    Ok(listener)
}
