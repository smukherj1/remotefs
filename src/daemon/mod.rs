//! Daemon-owned startup and filesystem behavior.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use crate::shared::config::Config;
use crate::shared::control;
use crate::shared::digest::Digest;
use crate::shared::state::{SessionStartup, SessionStore, StatePaths};

pub mod fs;

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

    #[arg(
        long,
        default_value = "info",
        help = "Log level (error, warn, info, debug, trace)"
    )]
    log_level: String,

    #[arg(
        long,
        default_value = "text",
        help = "Output format for daemon logs (text, json)"
    )]
    output_format: String,
}

/// Runs the foreground daemon until its control server shuts down.
pub async fn run(cli: Cli) -> Result<()> {
    let digest: Digest = cli
        .root_digest
        .parse()
        .with_context(|| format!("parse daemon root digest {}", cli.root_digest))?;
    let config = Config::new().context("load daemon state configuration")?;
    let paths = StatePaths::from_config(&config)
        .with_context(|| format!("validate daemon state home {}", config.rfs_home.display()))?;
    let store = SessionStore::create(paths, SessionStartup::new(digest, cli.mountpoint.clone()))
        .with_context(|| format!("create daemon session for {}", cli.mountpoint.display()))?;
    let metadata = store
        .metadata()
        .context("read active daemon session metadata")?;
    control::serve(store.paths().clone(), metadata)
        .await
        .context("serve active daemon control socket")?;
    store
        .close_cleanly()
        .context("close daemon session after control server shutdown")?;
    Ok(())
}
