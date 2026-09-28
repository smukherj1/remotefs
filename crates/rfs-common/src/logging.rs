//! Small process-level logging configuration shared by the CLI and daemon.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, Once};

use thiserror::Error;
use tracing_subscriber::EnvFilter;

/// File name of the daemon `tracing` log under `RFS_HOME`.
const DAEMON_TRACE_LOG: &str = "rfsd.log";
/// File name of the daemon stdout and stderr log under `RFS_HOME`.
const DAEMON_STDIO_LOG: &str = "rfsd_stdout_stderr.log";

/// Supported process log formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Human-readable single-line events.
    Text,
    /// Structured JSON Lines events.
    Json,
}

/// Logging initialization failures.
#[derive(Debug, Error)]
pub enum LoggingError {
    /// The daemon trace log could not be created or truncated.
    #[error("open daemon trace log `{path}`: {source}")]
    Open {
        /// Requested log path.
        path: PathBuf,
        /// Underlying filesystem failure.
        #[source]
        source: io::Error,
    },
    /// A process-global subscriber was already installed.
    #[error("logging is already initialized: {0}")]
    AlreadyInitialized(#[from] tracing::subscriber::SetGlobalDefaultError),
}

/// Daemon log files under `RFS_HOME`. Neither lives in the session tree, so
/// replacing the session tree never removes them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonLogPaths {
    /// Daemon stdout and stderr. The launcher (`rfs mount`) truncates it
    /// before spawning the daemon and passes it as fd 1 and fd 2.
    pub stdio: PathBuf,
    /// Daemon `tracing` output, truncated when the daemon starts logging.
    pub trace: PathBuf,
}

/// Returns the daemon log paths for the RemoteFS home `home`.
///
/// `home` is the configured `RFS_HOME`, used as given. Both returned paths are
/// direct children of `home`. Performs no I/O and cannot fail.
pub fn daemon_log_paths(home: &Path) -> DaemonLogPaths {
    DaemonLogPaths {
        stdio: home.join(DAEMON_STDIO_LOG),
        trace: home.join(DAEMON_TRACE_LOG),
    }
}

/// Initializes CLI logging to stderr.
pub fn init_cli(level: &str, format: LogFormat) -> Result<(), LoggingError> {
    let filter = EnvFilter::new(level);
    match format {
        LogFormat::Text => tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_writer(io::stderr)
                .with_target(false)
                .finish(),
        )?,
        LogFormat::Json => tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .with_writer(io::stderr)
                .with_target(false)
                .finish(),
        )?,
    }
    Ok(())
}

/// Creates or truncates the daemon trace log and installs the global subscriber.
///
/// Inputs: `home` is the configured `RFS_HOME` and must already exist;
/// `level` is an `EnvFilter` directive such as `info`; `format` selects text
/// or JSON Lines events.
///
/// Precondition: the caller holds the session lock. Truncating without the
/// lock could wipe the trace log of a live daemon.
///
/// Side effects: truncates `daemon_log_paths(home).trace` (creating it if
/// missing) and installs the process-global `tracing` subscriber.
///
/// Errors: `LoggingError::Open` if the trace log cannot be created, and
/// `LoggingError::AlreadyInitialized` if a global subscriber already exists.
pub fn init_daemon(home: &Path, level: &str, format: LogFormat) -> Result<(), LoggingError> {
    let path = daemon_log_paths(home).trace;
    let file = File::create(&path).map_err(|source| LoggingError::Open { path, source })?;
    let filter = EnvFilter::new(level);
    match format {
        LogFormat::Text => install_file_subscriber(file, filter, false)?,
        LogFormat::Json => install_file_subscriber(file, filter, true)?,
    }
    Ok(())
}

/// Initializes capture-aware text logging once for the current test executable.
///
/// The `RUST_LOG` environment variable overrides the default `info` filter.
/// Cargo builds each unit or integration test target as a separate executable,
/// so each target that needs logs must call this helper.
pub fn init_test() {
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .with_target(false)
            .try_init();
    });
}

fn install_file_subscriber(
    file: File,
    filter: EnvFilter,
    json: bool,
) -> Result<(), tracing::subscriber::SetGlobalDefaultError> {
    if json {
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .with_writer(Mutex::new(file))
                .with_target(false)
                .finish(),
        )
    } else {
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_writer(Mutex::new(file))
                .with_target(false)
                .finish(),
        )
    }
}

pub use tracing::{debug, error, info, warn};
