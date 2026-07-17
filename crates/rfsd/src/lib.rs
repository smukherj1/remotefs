//! Daemon-owned startup, state lifecycle, and control service.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use rfs_common::cas::{CasClient, CasConfig};
use rfs_common::config::Config;
use rfs_common::digest::Digest;
use rfs_common::logging::{self, LogFormat};
use rfs_common::state::{SessionStartup, open_daemon};

mod control_service;
pub mod fs;
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

/// Runs the foreground daemon until control shutdown or Ctrl-C completes.
pub async fn run(cli: Cli) -> Result<()> {
    let digest: Digest = cli
        .root_digest
        .parse()
        .with_context(|| format!("parse daemon root digest {}", cli.root_digest))?;
    let cas_url = cli
        .cas_url
        .or_else(|| std::env::var("RFS_CAS_URL").ok())
        .context("missing CAS URL; pass --cas-url or set RFS_CAS_URL")?;
    let instance_name = cli
        .instance_name
        .or_else(|| std::env::var("RFS_INSTANCE_NAME").ok())
        .context("missing instance name; pass --instance-name or set RFS_INSTANCE_NAME")?;
    let cas_config =
        CasConfig::new(cas_url, instance_name).context("validate daemon CAS configuration")?;
    let cas = CasClient::connect(cas_config)
        .await
        .context("connect daemon to CAS")?;
    let config = Config::new().context("load daemon state configuration")?;
    let state = open_daemon(
        config,
        SessionStartup::new(digest.clone(), cli.mountpoint.clone()),
    )
    .with_context(|| format!("create daemon session for {}", cli.mountpoint.display()))?;
    let session = state
        .session()
        .context("read active daemon session metadata")?
        .context("new daemon state did not contain a session")?;
    logging::init_daemon(
        &session.session_path.join("rfsd.log"),
        cli.log_level.as_str(),
        match cli.output_format {
            OutputFormat::Text => LogFormat::Text,
            OutputFormat::Json => LogFormat::Json,
        },
    )
    .context("initialize daemon session logging")?;
    tracing::info!(
        operation = "daemon_start",
        session_path = %session.session_path.display(),
        mountpoint = %session.mountpoint.display(),
        digest = %session.root_digest,
        "daemon session active"
    );
    let filesystem = match fs::ReadOnlyFilesystem::mount(
        cas,
        state.filesystem_state(),
        session.cache_path.clone(),
        digest,
    )
    .await
    {
        Ok(filesystem) => std::sync::Arc::new(filesystem),
        Err(error) => {
            state
                .close()
                .context("close daemon state after root validation failure")?;
            return Err(error).context("validate root directory before FUSE mount");
        }
    };
    let mount = match fuse::FuseMount::mount(
        std::sync::Arc::clone(&filesystem),
        &session.mountpoint,
        tokio::runtime::Handle::current(),
    ) {
        Ok(mount) => mount,
        Err(error) => {
            state
                .close()
                .context("close daemon state after FUSE mount failure")?;
            return Err(error).with_context(|| {
                format!("mount FUSE filesystem at {}", session.mountpoint.display())
            });
        }
    };
    control_service::serve(state, mount, filesystem)
        .await
        .context("serve active daemon control socket")?;
    tracing::info!(operation = "daemon_stop", "daemon session closed");
    Ok(())
}
