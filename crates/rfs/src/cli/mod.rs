use std::env;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use thiserror::Error;

use crate::bootstrap_upload::{BootstrapUploadConfig, BootstrapUploader};
use crate::daemon_client::{ControlEndpoint, DaemonClient, SessionStatus};
use rfs_common::config::{Config, ConfigError};
use rfs_common::digest::{Digest, DigestError};
use rfs_common::logging::{self, LogFormat};
use rfs_common::state::{SessionStateReader, SessionView, canonicalize_mountpoint, open_reader};

/// Parsed `rfs` command line.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "rfs",
    version,
    about = "RemoteFS CLI - lazy content-addressed remote filesystem for CI/CD",
    long_about = "RemoteFS is a content-addressed remote filesystem. It enables CI jobs to mount source and build-output snapshots almost instantly, fetch file data lazily on demand, and upload changes back."
)]
pub struct Cli {
    #[arg(
        long,
        global = true,
        help = "Remote Execution API CAS endpoint (e.g., grpc://127.0.0.1:9092)"
    )]
    cas_url: Option<String>,

    #[arg(long, global = true, help = "Remote Execution API instance name")]
    instance_name: Option<String>,

    #[arg(
        long,
        global = true,
        default_value = "info",
        value_enum,
        help = "Log level (error, warn, info, debug, trace)"
    )]
    log_level: LogLevel,

    #[arg(
        long,
        global = true,
        default_value = "text",
        value_enum,
        help = "Output format for command summaries and logs (text, json)"
    )]
    output_format: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

impl Cli {
    /// Returns whether CLI diagnostics should be rendered as JSON.
    pub fn json_output(&self) -> bool {
        self.output_format == OutputFormat::Json
    }

    /// Returns the selected subcommand name for diagnostics.
    pub fn command_name(&self) -> &'static str {
        self.command.name()
    }
}

/// Supported `rfs` subcommands for the MVP command surface.
#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
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
}

impl Commands {
    fn name(&self) -> &'static str {
        match self {
            Self::Upload { .. } => "upload",
            Self::Mount { .. } => "mount",
            Self::Snapshot { .. } => "snapshot",
            Self::Unmount { .. } => "unmount",
            Self::Status { .. } => "status",
        }
    }
}

/// Output format for cli logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum OutputFormat {
    Text,
    Json,
}

/// Fully resolved CLI configuration after applying flag and environment precedence.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CliConfig {
    cas_url: String,
    instance_name: String,
}

/// Supported log level values accepted by `rfs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
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

/// CLI execution or validation error with a stable diagnostic category.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum CliError {
    #[error("{message}")]
    InvalidConfig { message: String },
    #[error("invalid root digest `{value}`: {source}")]
    InvalidDigest { value: String, source: DigestError },
    #[error("mountpoint `{supplied}` does not match active session mountpoint `{active}`")]
    MountpointMismatch { supplied: PathBuf, active: PathBuf },
    #[error("{command} is not implemented yet")]
    NotImplemented { command: &'static str },
    #[error("{message}")]
    CommandFailed {
        category: &'static str,
        message: String,
    },
}

impl CliError {
    /// Returns a stable machine-readable category for this error.
    pub fn category(&self) -> &'static str {
        match self {
            Self::InvalidConfig { .. } => "invalid_config",
            Self::InvalidDigest { .. } => "invalid_digest",
            Self::MountpointMismatch { .. } => "mountpoint_mismatch",
            Self::NotImplemented { .. } => "not_implemented",
            Self::CommandFailed { category, .. } => category,
        }
    }
}

/// Applies CLI flag precedence over environment variables and validates CAS config.
///
/// `--cas-url` and `--instance-name` override
/// `RFS_CAS_URL` and `RFS_INSTANCE_NAME`. Missing or invalid CAS fields are
/// returned as `CliError::InvalidConfig`.
fn resolve_cli_config(cli: &Cli) -> Result<CliConfig, CliError> {
    let cas_url = cli
        .cas_url
        .clone()
        .or_else(|| env::var("RFS_CAS_URL").ok())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| CliError::InvalidConfig {
            message: "missing CAS URL; pass --cas-url or set RFS_CAS_URL".to_string(),
        })?;
    validate_cas_url(&cas_url)?;

    let instance_name = cli
        .instance_name
        .clone()
        .or_else(|| env::var("RFS_INSTANCE_NAME").ok())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| CliError::InvalidConfig {
            message: "missing instance name; pass --instance-name or set RFS_INSTANCE_NAME"
                .to_string(),
        })?;
    Ok(CliConfig {
        cas_url,
        instance_name,
    })
}

/// Executes the current command.
///
/// Commands that depend on later phases return `CliError::NotImplemented`
/// after validating the arguments that already have stable rules.
pub async fn run(cli: Cli) -> Result<(), CliError> {
    logging::init_cli(
        cli.log_level.as_str(),
        match cli.output_format {
            OutputFormat::Text => LogFormat::Text,
            OutputFormat::Json => LogFormat::Json,
        },
    )
    .map_err(|error| CliError::CommandFailed {
        category: "logging",
        message: error.to_string(),
    })?;
    match &cli.command {
        Commands::Upload { local_dir } => {
            let config = resolve_cli_config(&cli)?;
            let digest = run_upload(config, local_dir.clone()).await?;
            print_upload_output(digest, cli.json_output());
            Ok(())
        }
        Commands::Mount { root_digest, .. } => {
            parse_root_digest(root_digest)?;
            resolve_cli_config(&cli)?;
            Err(CliError::NotImplemented { command: "mount" })
        }
        Commands::Snapshot { mountpoint } => {
            validate_optional_mountpoint(mountpoint.as_deref(), None)?;
            Err(CliError::NotImplemented {
                command: "snapshot",
            })
        }
        Commands::Unmount { mountpoint } => {
            let config = Config::new().map_err(config_error)?;
            let reader = open_reader(config).map_err(state_error)?;
            run_unmount(reader.as_ref(), mountpoint.as_deref()).await
        }
        Commands::Status { mountpoint } => {
            let config = Config::new().map_err(config_error)?;
            let reader = open_reader(config).map_err(state_error)?;
            run_status(reader.as_ref(), mountpoint.as_deref(), cli.json_output()).await
        }
    }
}

async fn daemon_client(reader: &dyn SessionStateReader) -> Result<DaemonClient, CliError> {
    DaemonClient::connect(ControlEndpoint(reader.control_endpoint()))
        .await
        .map_err(client_error)
}

async fn run_status(
    reader: &dyn SessionStateReader,
    supplied: Option<&Path>,
    json: bool,
) -> Result<(), CliError> {
    if let Ok(mut client) = daemon_client(reader).await
        && let Ok(status) = client.status().await
    {
        validate_supplied_mountpoint(supplied, &status.mountpoint)?;
        render_active_status(status, json);
        return Ok(());
    }
    match reader.session().map_err(state_error)? {
        None => {
            validate_optional_mountpoint(supplied, None)?;
            if json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"command":"status","ok":true,"data":{"state":"none"}})
                );
            } else {
                println!("no RemoteFS session");
            }
            Ok(())
        }
        Some(session) => {
            validate_supplied_mountpoint(supplied, &session.mountpoint)?;
            render_retained_status(session, json);
            Ok(())
        }
    }
}

fn render_active_status(status: SessionStatus, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({"schema_version":1,"command":"status","ok":true,"data":{"state":"active","mountpoint":status.mountpoint,"root_digest":status.root_digest,"daemon_pid":status.daemon_pid,"control_socket":status.control_socket,"cache_path":status.cache_path,"session_path":status.session_path,"dirty":status.dirty,"dirty_files":status.dirty_files,"cached_blobs":status.cached_blobs,"snapshot_blockers":status.snapshot_blockers}})
        );
    } else {
        println!(
            "active session: mountpoint={} root_digest={} daemon_pid={} socket={} dirty={}",
            status.mountpoint.display(),
            status.root_digest,
            status.daemon_pid,
            status.control_socket.display(),
            status.dirty
        );
    }
}

fn render_retained_status(session: SessionView, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({"schema_version":1,"command":"status","ok":true,"data":{"state":session.state,"mountpoint":session.mountpoint,"root_digest":session.root_digest,"daemon_pid":session.daemon_pid}})
        );
    } else {
        println!(
            "{} session: mountpoint={} root_digest={} daemon_pid={}",
            session.state,
            session.mountpoint.display(),
            session.root_digest,
            session.daemon_pid
        );
    }
}

async fn run_unmount(
    reader: &dyn SessionStateReader,
    supplied: Option<&Path>,
) -> Result<(), CliError> {
    let mut client = daemon_client(reader).await?;
    let status = client.status().await.map_err(client_error)?;
    validate_supplied_mountpoint(supplied, &status.mountpoint)?;
    let mountpoint = status.mountpoint;
    client.unmount().await.map_err(client_error)?;
    println!("unmounted {}", mountpoint.display());
    Ok(())
}

fn client_error(error: crate::daemon_client::ClientError) -> CliError {
    CliError::CommandFailed {
        category: error.code(),
        message: error.to_string(),
    }
}

fn validate_supplied_mountpoint(supplied: Option<&Path>, active: &Path) -> Result<(), CliError> {
    let supplied = supplied
        .map(canonicalize_mountpoint)
        .transpose()
        .map_err(state_error)?;
    validate_optional_mountpoint(supplied.as_deref(), Some(active))
}

async fn run_upload(config: CliConfig, local_dir: PathBuf) -> Result<Digest, CliError> {
    let mut uploader = BootstrapUploader::connect(BootstrapUploadConfig {
        cas_url: config.cas_url,
        instance_name: config.instance_name,
    })
    .await
    .map_err(|error| CliError::CommandFailed {
        category: "upload",
        message: error.to_string(),
    })?;
    uploader
        .upload(&local_dir)
        .await
        .map_err(|error| CliError::CommandFailed {
            category: "upload",
            message: error.to_string(),
        })
}

fn print_upload_output(digest: Digest, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({"schema_version":1,"command":"upload","ok":true,"data":{"root_digest":digest}})
        );
    } else {
        println!("{digest}");
    }
}

/// Parses and validates an MVP root digest string.
fn parse_root_digest(value: &str) -> Result<Digest, CliError> {
    value.parse().map_err(|source| CliError::InvalidDigest {
        value: value.to_string(),
        source,
    })
}

/// Validates a user-supplied mountpoint against known active-session metadata.
///
/// When no active mountpoint metadata is available, this accepts the supplied
/// value so early command parsing can remain independent of the daemon state
/// implementation.
fn validate_optional_mountpoint(
    supplied: Option<&Path>,
    active: Option<&Path>,
) -> Result<(), CliError> {
    match (supplied, active) {
        (Some(supplied), Some(active)) if supplied != active => Err(CliError::MountpointMismatch {
            supplied: supplied.to_path_buf(),
            active: active.to_path_buf(),
        }),
        _ => Ok(()),
    }
}

/// Renders a CLI error as a one-line human message or JSON diagnostic.
pub fn render_error(error: &CliError, json: bool) -> String {
    render_error_for_command(error, json, "unknown")
}

/// Renders a CLI error with a command-aware JSON envelope.
pub fn render_error_for_command(error: &CliError, json: bool, command: &'static str) -> String {
    if json {
        serde_json::json!({
            "schema_version": 1,
            "command": command,
            "ok": false,
            "warnings": null,
            "error": {
                "code": error.category(),
                "message": error.to_string(),
                "details": {}
            },
            "data": null
        })
        .to_string()
    } else {
        format!("{}: {}", error.category(), error)
    }
}

fn validate_cas_url(cas_url: &str) -> Result<(), CliError> {
    if cas_url
        .strip_prefix("grpc://")
        .is_some_and(|rest| !rest.is_empty())
    {
        return Ok(());
    }
    Err(CliError::InvalidConfig {
        message: format!("unsupported CAS URL `{cas_url}`; MVP supports grpc:// endpoints only"),
    })
}

fn config_error(error: ConfigError) -> CliError {
    CliError::InvalidConfig {
        message: error.to_string(),
    }
}

fn state_error(error: rfs_common::state::StateError) -> CliError {
    CliError::CommandFailed {
        category: match error {
            rfs_common::state::StateError::ActiveSession { .. } => "active_session",
            rfs_common::state::StateError::StaleSession { .. } => "stale_session",
            rfs_common::state::StateError::UnsafePath { .. } => "unsafe_state",
            _ => "state",
        },
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clear_env() {
        unsafe {
            env::remove_var("RFS_HOME");
            env::remove_var("RFS_CACHE_DIR");
            env::remove_var("RFS_SESSION_DIR");
            env::remove_var("RFS_CAS_URL");
            env::remove_var("RFS_INSTANCE_NAME");
        }
    }

    #[test]
    fn parses_every_mvp_command() {
        let upload = Cli::try_parse_from(["rfs", "upload", "."]).unwrap();
        assert!(matches!(upload.command, Commands::Upload { .. }));

        let digest = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0";
        let mount = Cli::try_parse_from(["rfs", "mount", digest, "/mnt/rfs"]).unwrap();
        assert!(matches!(mount.command, Commands::Mount { .. }));

        let snapshot = Cli::try_parse_from(["rfs", "snapshot", "/mnt/rfs"]).unwrap();
        assert!(matches!(snapshot.command, Commands::Snapshot { .. }));

        let unmount = Cli::try_parse_from(["rfs", "unmount"]).unwrap();
        assert!(matches!(unmount.command, Commands::Unmount { .. }));

        let status = Cli::try_parse_from(["rfs", "status", "/mnt/rfs"]).unwrap();
        assert!(matches!(status.command, Commands::Status { .. }));

        assert!(Cli::try_parse_from(["rfs", "cleanup"]).is_err());
    }

    #[test]
    fn cas_flags_override_environment_config() {
        let _guard = crate::test_env::lock();
        clear_env();
        unsafe {
            env::set_var("HOME", "/home/testuser");
            env::set_var("RFS_HOME", "/state/home");
            env::set_var("RFS_CAS_URL", "grpc://env.example:9092");
            env::set_var("RFS_INSTANCE_NAME", "env/instance");
        }
        let cli = Cli::try_parse_from([
            "rfs",
            "--cas-url",
            "grpc://flag.example:9092",
            "--instance-name",
            "flag/instance",
            "upload",
            ".",
        ])
        .unwrap();

        let config = resolve_cli_config(&cli).unwrap();
        assert_eq!(config.cas_url, "grpc://flag.example:9092");
        assert_eq!(config.instance_name, "flag/instance");
    }

    #[test]
    fn removed_state_path_flags_are_rejected() {
        assert!(Cli::try_parse_from(["rfs", "--cache-dir", "/tmp/cache", "status"]).is_err());
        assert!(Cli::try_parse_from(["rfs", "--session-dir", "/tmp/active", "status"]).is_err());
    }

    #[test]
    fn environment_config_is_used_when_flags_are_absent() {
        let _guard = crate::test_env::lock();
        clear_env();
        unsafe {
            env::set_var("HOME", "/home/testuser");
            env::set_var("RFS_CAS_URL", "grpc://env.example:9092");
            env::set_var("RFS_INSTANCE_NAME", "env/instance");
        }
        let cli = Cli::try_parse_from(["rfs", "upload", "."]).unwrap();

        let config = resolve_cli_config(&cli).unwrap();
        assert_eq!(config.cas_url, "grpc://env.example:9092");
        assert_eq!(config.instance_name, "env/instance");
    }

    #[test]
    fn config_rejects_missing_or_empty_instance_name() {
        let _guard = crate::test_env::lock();
        clear_env();
        unsafe {
            env::set_var("HOME", "/home/testuser");
            env::set_var("RFS_CAS_URL", "grpc://127.0.0.1:9092");
        }
        let missing = Cli::try_parse_from(["rfs", "upload", "."]).unwrap();
        assert!(matches!(
            resolve_cli_config(&missing),
            Err(CliError::InvalidConfig { .. })
        ));

        let empty = Cli::try_parse_from([
            "rfs",
            "--cas-url",
            "grpc://127.0.0.1:9092",
            "--instance-name",
            "",
            "upload",
            ".",
        ])
        .unwrap();
        assert!(matches!(
            resolve_cli_config(&empty),
            Err(CliError::InvalidConfig { .. })
        ));
    }

    #[test]
    fn config_rejects_missing_or_unsupported_cas_url_scheme() {
        let _guard = crate::test_env::lock();
        clear_env();
        unsafe {
            env::set_var("HOME", "/home/testuser");
            env::set_var("RFS_INSTANCE_NAME", "remotefs/tests");
        }
        let missing = Cli::try_parse_from(["rfs", "upload", "."]).unwrap();
        assert!(matches!(
            resolve_cli_config(&missing),
            Err(CliError::InvalidConfig { .. })
        ));

        let unsupported = Cli::try_parse_from([
            "rfs",
            "--cas-url",
            "http://127.0.0.1:9092",
            "--instance-name",
            "remotefs/tests",
            "upload",
            ".",
        ])
        .unwrap();
        assert!(matches!(
            resolve_cli_config(&unsupported),
            Err(CliError::InvalidConfig { .. })
        ));

        let no_scheme = Cli::try_parse_from([
            "rfs",
            "--cas-url",
            "127.0.0.1:9092",
            "--instance-name",
            "remotefs/tests",
            "upload",
            ".",
        ])
        .unwrap();
        assert!(matches!(
            resolve_cli_config(&no_scheme),
            Err(CliError::InvalidConfig { .. })
        ));
    }

    #[test]
    fn invalid_log_level_is_rejected_for_commands_without_cas_config() {
        let error = Cli::try_parse_from(["rfs", "--log-level", "bogus", "status"]).unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::InvalidValue);
    }

    #[test]
    fn optional_mountpoint_matches_active_session_metadata() {
        validate_optional_mountpoint(Some(Path::new("/mnt/rfs")), Some(Path::new("/mnt/rfs")))
            .unwrap();
        assert!(matches!(
            validate_optional_mountpoint(Some(Path::new("/tmp/rfs")), Some(Path::new("/mnt/rfs"))),
            Err(CliError::MountpointMismatch { .. })
        ));
        validate_optional_mountpoint(Some(Path::new("/tmp/rfs")), None).unwrap();
    }

    #[test]
    fn invalid_root_digest_is_reported_before_mount_stub() {
        assert!(matches!(
            parse_root_digest("not-a-digest"),
            Err(CliError::InvalidDigest { .. })
        ));
    }
}
