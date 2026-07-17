//! Small process-level logging configuration shared by the CLI and daemon.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use thiserror::Error;
use tracing_subscriber::EnvFilter;

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
    /// The daemon log could not be opened.
    #[error("open daemon log `{path}`: {source}")]
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

/// Initializes daemon logging by appending to the active session log.
pub fn init_daemon(path: &Path, level: &str, format: LogFormat) -> Result<(), LoggingError> {
    let file = OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(|source| LoggingError::Open {
            path: path.to_path_buf(),
            source,
        })?;
    let filter = EnvFilter::new(level);
    match format {
        LogFormat::Text => install_file_subscriber(file, filter, false)?,
        LogFormat::Json => install_file_subscriber(file, filter, true)?,
    }
    Ok(())
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
