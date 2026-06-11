use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use thiserror::Error;

use crate::cas::CasConfig;
use crate::config::{Config, ConfigError};
use crate::digest::{Digest, DigestError};

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

    #[arg(long, global = true, help = "Output machine-readable JSON summaries")]
    json: bool,

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
        help = "Log format (text, json)"
    )]
    log_format: LogFormat,

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
        self.json
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

/// Output format for CLI diagnostics and future logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum LogFormat {
    Text,
    Json,
}

/// Fully resolved CLI configuration after applying flag and environment precedence.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CliConfig {
    state: Config,
    cas_url: String,
    instance_name: String,
    json: bool,
    log_level: LogLevel,
    log_format: LogFormat,
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
        json: cli.json,
        log_level: cli.log_level,
        log_format: cli.log_format,
    })
}

/// Executes the currently implemented step-3.1 command skeleton.
///
/// Commands that depend on later phases return `CliError::NotImplemented`
/// after validating the arguments that already have stable rules.
pub fn run(cli: Cli) -> Result<(), CliError> {
    match &cli.command {
        Commands::Upload { .. } => {
            resolve_cli_config(&cli)?;
            Err(CliError::NotImplemented { command: "upload" })
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
    if json {
        serde_json::json!({
            "error": {
                "category": error.category(),
                "message": error.to_string(),
            }
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
