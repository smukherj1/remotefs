use clap::Parser;
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

    #[arg(long, default_value = "text", help = "Log format (text, json)")]
    log_format: String,

    #[arg(long, help = "Path to custom cache directory")]
    cache_dir: Option<PathBuf>,

    #[arg(long, help = "Path to custom active session directory")]
    session_dir: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();
    println!("Starting RemoteFS Daemon with config: {:?}", cli);
}
