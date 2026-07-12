use clap::Parser;
use remotefs::config::Config;
use remotefs::digest::Digest;
use remotefs::state::{SessionStartup, SessionStore, StatePaths};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "rfsd",
    version,
    about = "RemoteFS Mount Daemon - manages FUSE mount and local session state",
    long_about = "rfsd is the background daemon that owns the FUSE mount, lazy metadata/blob retrieval, SQLite transaction index, copy-on-write overlay, and the CLI control socket."
)]
struct Cli {
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

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(error) = run(cli).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let digest: Digest = cli.root_digest.parse()?;
    let paths = StatePaths::from_config(&Config::new()?)?;
    let store = SessionStore::create(paths, SessionStartup::new(digest, cli.mountpoint))?;
    tokio::signal::ctrl_c().await?;
    store.close_cleanly()?;
    Ok(())
}
