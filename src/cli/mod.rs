use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use thiserror::Error;

use crate::shared::cas::{CasClient, CasConfig};
use crate::shared::config::{Config, ConfigError};
use crate::shared::control::{self, PROTOCOL_VERSION};
use crate::shared::digest::{Digest, DigestError};
use crate::shared::state::{
    RetainedSession, StatePaths, canonicalize_mountpoint, inspect_retained_session,
};
use crate::shared::upload::{UploadOptions, UploadSummary, upload_local_directory};

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
    #[command(about = "Clean up stale active session state and locks")]
    Cleanup,
}

impl Commands {
    fn name(&self) -> &'static str {
        match self {
            Self::Upload { .. } => "upload",
            Self::Mount { .. } => "mount",
            Self::Snapshot { .. } => "snapshot",
            Self::Unmount { .. } => "unmount",
            Self::Status { .. } => "status",
            Self::Cleanup => "cleanup",
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
    state: Config,
    cas_url: String,
    instance_name: String,
    output_format: OutputFormat,
    log_level: LogLevel,
}

/// Supported log level values accepted by `rfs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
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
    let state = Config::new().map_err(config_error)?;
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
    CasConfig::new(cas_url.clone(), instance_name.clone()).map_err(|source| {
        CliError::InvalidConfig {
            message: source.to_string(),
        }
    })?;

    Ok(CliConfig {
        state,
        cas_url,
        instance_name,
        output_format: cli.output_format,
        log_level: cli.log_level,
    })
}

/// Executes the current command.
///
/// Commands that depend on later phases return `CliError::NotImplemented`
/// after validating the arguments that already have stable rules.
pub async fn run(cli: Cli) -> Result<(), CliError> {
    match &cli.command {
        Commands::Upload { local_dir } => {
            let config = resolve_cli_config(&cli)?;
            let output = run_upload(config, local_dir.clone()).await?;
            print_command_output(output);
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
            let paths = StatePaths::from_config(&config).map_err(state_error)?;
            run_unmount(&paths, mountpoint.as_deref()).await
        }
        Commands::Status { mountpoint } => {
            let config = Config::new().map_err(config_error)?;
            let paths = StatePaths::from_config(&config).map_err(state_error)?;
            run_status(&paths, mountpoint.as_deref(), cli.json_output()).await
        }
        Commands::Cleanup => {
            let config = Config::new().map_err(config_error)?;
            let paths =
                crate::shared::state::StatePaths::from_config(&config).map_err(state_error)?;
            paths.cleanup().map_err(state_error)
        }
    }
}

async fn query_daemon(
    paths: &StatePaths,
) -> Result<crate::shared::control::v1::StatusResponse, CliError> {
    let mut client = control::connect(&paths.control_socket())
        .await
        .map_err(control_error)?;
    client
        .status(crate::shared::control::v1::StatusRequest {
            protocol_version: PROTOCOL_VERSION,
        })
        .await
        .map(|response| response.into_inner())
        .map_err(|status| CliError::CommandFailed {
            category: control::diagnostic_category(status.code()),
            message: status.to_string(),
        })
}

async fn run_status(
    paths: &StatePaths,
    supplied: Option<&Path>,
    json: bool,
) -> Result<(), CliError> {
    if paths.control_socket().exists()
        && let Ok(status) = query_daemon(paths).await
    {
        validate_supplied_mountpoint(supplied, Path::new(&status.mountpoint))?;
        if json {
            println!(
                "{}",
                serde_json::json!({"schema_version":1,"command":"status","ok":true,"data":{"state":"active","mountpoint":status.mountpoint,"root_digest":status.root_digest,"daemon_pid":status.daemon_pid,"control_socket":status.control_socket,"cache_path":status.cache_path,"session_path":status.session_path,"dirty":status.dirty,"dirty_files":status.dirty_files,"cached_blobs":status.cached_blobs,"snapshot_blockers":status.snapshot_blockers}})
            );
        } else {
            println!(
                "active session: mountpoint={} root_digest={} daemon_pid={} socket={} dirty={}",
                status.mountpoint,
                status.root_digest,
                status.daemon_pid,
                status.control_socket,
                status.dirty
            );
        }
        return Ok(());
    }
    match inspect_retained_session(paths) {
        RetainedSession::None => {
            if supplied.is_some() {
                validate_optional_mountpoint(supplied, None)?;
            }
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
        RetainedSession::Closed(metadata) => {
            validate_supplied_mountpoint(supplied, &metadata.mountpoint)?;
            if json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"command":"status","ok":true,"data":{"state":"closed","mountpoint":metadata.mountpoint,"root_digest":metadata.root_digest.to_string(),"daemon_pid":metadata.daemon_pid}})
                );
            } else {
                println!(
                    "closed session: mountpoint={} root_digest={} daemon_pid={}",
                    metadata.mountpoint.display(),
                    metadata.root_digest,
                    metadata.daemon_pid
                );
            }
            Ok(())
        }
        RetainedSession::Stale(reason) => Err(CliError::CommandFailed {
            category: "stale_session",
            message: format!("stale session state; run `rfs cleanup`: {reason}"),
        }),
    }
}

async fn run_unmount(paths: &StatePaths, supplied: Option<&Path>) -> Result<(), CliError> {
    let status = query_daemon(paths).await?;
    validate_supplied_mountpoint(supplied, Path::new(&status.mountpoint))?;
    let mut client = control::connect(&paths.control_socket())
        .await
        .map_err(control_error)?;
    client
        .unmount(crate::shared::control::v1::UnmountRequest {
            protocol_version: PROTOCOL_VERSION,
        })
        .await
        .map_err(|status| CliError::CommandFailed {
            category: control::diagnostic_category(status.code()),
            message: status.to_string(),
        })?;
    println!("unmount requested for {}", status.mountpoint);
    Ok(())
}

fn validate_supplied_mountpoint(supplied: Option<&Path>, active: &Path) -> Result<(), CliError> {
    let supplied = supplied
        .map(canonicalize_mountpoint)
        .transpose()
        .map_err(state_error)?;
    validate_optional_mountpoint(supplied.as_deref(), Some(active))
}

fn control_error(error: control::ControlError) -> CliError {
    let category = match &error {
        control::ControlError::Rpc(status) => control::diagnostic_category(status.code()),
        control::ControlError::IncompatibleProtocol { .. } => "daemon_protocol",
        _ => "daemon_unavailable",
    };
    CliError::CommandFailed {
        category,
        message: error.to_string(),
    }
}

#[derive(Debug)]
struct CommandOutput {
    json: bool,
    summary: UploadSummary,
}

#[derive(Serialize)]
struct JsonEnvelope<'a> {
    schema_version: u32,
    command: &'static str,
    ok: bool,
    warnings: &'a crate::shared::upload::UploadWarnings,
    error: Option<JsonError>,
    data: Option<UploadData<'a>>,
}

#[derive(Serialize)]
struct UploadData<'a> {
    root_digest: &'a Digest,
    files: usize,
    directories: usize,
    symlinks: usize,
    uploaded_blobs: usize,
    reused_blobs: usize,
    bytes_uploaded: u64,
}

#[derive(Serialize)]
struct JsonError {
    code: String,
    message: String,
    details: serde_json::Value,
}

async fn run_upload(config: CliConfig, local_dir: PathBuf) -> Result<CommandOutput, CliError> {
    let metadata = fs::symlink_metadata(&local_dir).map_err(|source| CliError::CommandFailed {
        category: "filesystem",
        message: format!(
            "read metadata for upload root {}: {source}",
            local_dir.display()
        ),
    })?;
    if !metadata.is_dir() {
        return Err(CliError::InvalidConfig {
            message: format!("upload root `{}` is not a directory", local_dir.display()),
        });
    }

    let cas_config =
        CasConfig::new(config.cas_url.clone(), config.instance_name.clone()).map_err(|source| {
            CliError::InvalidConfig {
                message: format!(
                    "unable to create configuration to connect to backend CAS: {}",
                    source
                ),
            }
        })?;
    let mut client =
        CasClient::connect(cas_config)
            .await
            .map_err(|source| CliError::CommandFailed {
                category: "cas",
                message: format!("unable to connect to CAS server: {}", source),
            })?;
    let summary = upload_local_directory(&mut client, local_dir, UploadOptions::default())
        .await
        .map_err(|source| CliError::CommandFailed {
            category: "upload",
            message: source.to_string(),
        })?;
    Ok(CommandOutput {
        json: config.output_format == OutputFormat::Json,
        summary,
    })
}

fn print_command_output(output: CommandOutput) {
    if output.json {
        println!("{}", render_upload_success(&output.summary));
    } else {
        println!("{}", output.summary.root_digest);
        eprintln!(
            "uploaded_blobs={} reused_blobs={} bytes_uploaded={} files={} directories={} symlinks={}",
            output.summary.uploaded_blobs,
            output.summary.reused_blobs,
            output.summary.bytes_uploaded,
            output.summary.files,
            output.summary.directories,
            output.summary.symlinks
        );
    }
}

fn render_upload_success(summary: &UploadSummary) -> String {
    serde_json::to_string(&JsonEnvelope {
        schema_version: 1,
        command: "upload",
        ok: true,
        warnings: &summary.warnings,
        error: None,
        data: Some(UploadData {
            root_digest: &summary.root_digest,
            files: summary.files,
            directories: summary.directories,
            symlinks: summary.symlinks,
            uploaded_blobs: summary.uploaded_blobs,
            reused_blobs: summary.reused_blobs,
            bytes_uploaded: summary.bytes_uploaded,
        }),
    })
    .expect("upload success envelope is serializable")
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

fn state_error(error: crate::shared::state::StateError) -> CliError {
    CliError::CommandFailed {
        category: match error {
            crate::shared::state::StateError::ActiveSession { .. } => "active_session",
            crate::shared::state::StateError::StaleSession { .. } => "stale_session",
            crate::shared::state::StateError::UnsafePath { .. } => "unsafe_state",
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

        let cleanup = Cli::try_parse_from(["rfs", "cleanup"]).unwrap();
        assert_eq!(cleanup.command, Commands::Cleanup);
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
        assert_eq!(config.state.rfs_home, PathBuf::from("/state/home"));
        assert_eq!(config.cas_url, "grpc://flag.example:9092");
        assert_eq!(config.instance_name, "flag/instance");
    }

    #[test]
    fn removed_state_path_flags_are_rejected() {
        assert!(Cli::try_parse_from(["rfs", "--cache-dir", "/tmp/cache", "cleanup"]).is_err());
        assert!(Cli::try_parse_from(["rfs", "--session-dir", "/tmp/active", "cleanup"]).is_err());
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
