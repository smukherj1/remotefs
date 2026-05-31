use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "rfs",
    version,
    about = "RemoteFS CLI - lazy content-addressed remote filesystem for CI/CD",
    long_about = "RemoteFS is a content-addressed remote filesystem. It enables CI jobs to mount source and build-output snapshots almost instantly, fetch file data lazily on demand, and upload changes back."
)]
struct Cli {
    #[arg(
        long,
        global = true,
        help = "Remote Execution API CAS endpoint (e.g., grpc://127.0.0.1:9092)"
    )]
    cas_url: Option<String>,

    #[arg(long, global = true, help = "Remote Execution API instance name")]
    instance_name: Option<String>,

    #[arg(long, global = true, help = "Output machine-readable JSON summaries")]
    json: bool,

    #[arg(
        long,
        global = true,
        default_value = "info",
        help = "Log level (error, warn, info, debug, trace)"
    )]
    log_level: String,

    #[arg(
        long,
        global = true,
        default_value = "text",
        help = "Log format (text, json)"
    )]
    log_format: String,

    #[arg(long, global = true, help = "Path to custom cache directory")]
    cache_dir: Option<PathBuf>,

    #[arg(long, global = true, help = "Path to custom active session directory")]
    session_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    #[command(about = "Upload a local directory to the CAS and return a root digest")]
    Upload {
        #[arg(help = "Path to the local directory to upload")]
        local_dir: PathBuf,
    },
    #[command(about = "Mount a RemoteFS snapshot at a local mountpoint")]
    Mount {
        #[arg(help = "Root digest of the snapshot (e.g., sha256:<hex>/<size>)")]
        root_digest: String,
        #[arg(help = "Path where the filesystem should be mounted")]
        mountpoint: PathBuf,
    },
    #[command(about = "Create a new snapshot of the mounted workspace")]
    Snapshot {
        #[arg(help = "Optional path to the mountpoint")]
        mountpoint: Option<PathBuf>,
    },
    #[command(about = "Unmount a mounted RemoteFS workspace")]
    Unmount {
        #[arg(help = "Optional path to the mountpoint")]
        mountpoint: Option<PathBuf>,
    },
    #[command(about = "Report current active session status and counters")]
    Status {
        #[arg(help = "Optional path to the mountpoint")]
        mountpoint: Option<PathBuf>,
    },
    #[command(about = "Clean up stale active session state and locks")]
    Cleanup,
}

fn main() {
    let cli = Cli::parse();
    // For Phase 0.1, we parse successfully and print a summary or exit.
    if cli.json {
        println!("{{\"command\": \"{:?}\"}}", cli.command);
    } else {
        println!("Executing command: {:?}", cli.command);
    }
}
