use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use thiserror::Error;

use crate::cas::{CasClient, CasConfig};
use crate::config::{Config, ConfigError};
use crate::digest::{Digest, DigestError};
use crate::upload::{UploadOptions, UploadSummary, upload_local_directory};

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

    #[arg(long, global = true, help = "Path to custom cache directory")]
    cache_dir: Option<PathBuf>,

    #[arg(long, global = true, help = "Path to custom active session directory")]
    session_dir: Option<PathBuf>,

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
    #[error("cleanup refused because active session lock `{lock_path}` belongs to live pid {pid}")]
    ActiveSessionLock { lock_path: PathBuf, pid: u32 },
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
            Self::ActiveSessionLock { .. } => "active_session_lock",
            Self::NotImplemented { .. } => "not_implemented",
            Self::CommandFailed { category, .. } => category,
        }
    }
}

/// Applies CLI flag precedence over environment variables and validates CAS config.
///
/// `--cache-dir` and `--session-dir` override `RFS_CACHE_DIR` and
/// `RFS_SESSION_DIR`. `--cas-url` and `--instance-name` override
/// `RFS_CAS_URL` and `RFS_INSTANCE_NAME`. Missing or invalid CAS fields are
/// returned as `CliError::InvalidConfig`.
fn resolve_cli_config(cli: &Cli) -> Result<CliConfig, CliError> {
    let state = Config::from_overrides(cli.cache_dir.clone(), cli.session_dir.clone())
        .map_err(config_error)?;
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
            validate_optional_mountpoint(mountpoint.as_deref(), None)?;
            Err(CliError::NotImplemented { command: "unmount" })
        }
        Commands::Status { mountpoint } => {
            validate_optional_mountpoint(mountpoint.as_deref(), None)?;
            Err(CliError::NotImplemented { command: "status" })
        }
        Commands::Cleanup => {
            let state = Config::from_overrides(cli.cache_dir.clone(), cli.session_dir.clone())
                .map_err(config_error)?;
            refuse_live_cleanup(&state.rfs_session_dir)?;
            Err(CliError::NotImplemented { command: "cleanup" })
        }
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
    warnings: &'a crate::upload::UploadWarnings,
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

fn refuse_live_cleanup(session_dir: &Path) -> Result<(), CliError> {
    let lock_path = session_dir.join("session.lock");
    let Ok(contents) = fs::read_to_string(&lock_path) else {
        return Ok(());
    };
    let Ok(pid) = contents.trim().parse::<u32>() else {
        return Ok(());
    };
    if is_live_pid(pid) {
        return Err(CliError::ActiveSessionLock { lock_path, pid });
    }
    Ok(())
}

fn is_live_pid(pid: u32) -> bool {
    Path::new("/proc").join(pid.to_string()).exists()
}

fn config_error(error: ConfigError) -> CliError {
    CliError::InvalidConfig {
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
    fn cli_flags_override_environment_config() {
        let _guard = crate::test_env::lock();
        clear_env();
        unsafe {
            env::set_var("HOME", "/home/testuser");
            env::set_var("RFS_CACHE_DIR", "/env/cache");
            env::set_var("RFS_SESSION_DIR", "/env/session");
            env::set_var("RFS_CAS_URL", "grpc://env.example:9092");
            env::set_var("RFS_INSTANCE_NAME", "env/instance");
        }
        let cli = Cli::try_parse_from([
            "rfs",
            "--cache-dir",
            "/flag/cache",
            "--session-dir",
            "/flag/session",
            "--cas-url",
            "grpc://flag.example:9092",
            "--instance-name",
            "flag/instance",
            "upload",
            ".",
        ])
        .unwrap();

        let config = resolve_cli_config(&cli).unwrap();
        assert_eq!(config.state.rfs_cache_dir, PathBuf::from("/flag/cache"));
        assert_eq!(config.state.rfs_session_dir, PathBuf::from("/flag/session"));
        assert_eq!(config.cas_url, "grpc://flag.example:9092");
        assert_eq!(config.instance_name, "flag/instance");
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
