use std::env;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ConfigError {
    #[error(
        "Unable to determine directory to use as home, neither RFS_HOME nor the HOME env variable was set"
    )]
    RfsHomeNotResolved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub rfs_home: PathBuf,
    pub rfs_cache_dir: PathBuf,
    pub rfs_session_dir: PathBuf,
}

impl Config {
    /// Resolves local RemoteFS state paths from environment variables.
    ///
    /// `RFS_HOME` controls the default state root. `RFS_CACHE_DIR` and
    /// `RFS_SESSION_DIR` override their individual directories when present.
    /// Returns `ConfigError::RfsHomeNotResolved` if neither `RFS_HOME` nor
    /// `HOME` is available.
    pub fn new() -> Result<Self, ConfigError> {
        Self::from_overrides(None, None)
    }

    /// Resolves local RemoteFS state paths with explicit CLI overrides.
    ///
    /// `cache_dir` and `session_dir` take precedence over `RFS_CACHE_DIR` and
    /// `RFS_SESSION_DIR`. Other path defaults are the same as `Config::new`.
    pub fn from_overrides(
        cache_dir: Option<PathBuf>,
        session_dir: Option<PathBuf>,
    ) -> Result<Self, ConfigError> {
        let rfs_home = match env::var_os("RFS_HOME") {
            Some(val) => PathBuf::from(val),
            None => {
                let home = env::var_os("HOME").ok_or(ConfigError::RfsHomeNotResolved)?;
                Path::new(&home).join(".rfs")
            }
        };

        let rfs_cache_dir = cache_dir.unwrap_or_else(|| match env::var_os("RFS_CACHE_DIR") {
            Some(val) => PathBuf::from(val),
            None => rfs_home.join("cache"),
        });

        let rfs_session_dir = session_dir.unwrap_or_else(|| match env::var_os("RFS_SESSION_DIR") {
            Some(val) => PathBuf::from(val),
            None => rfs_home.join("active"),
        });

        Ok(Config {
            rfs_home,
            rfs_cache_dir,
            rfs_session_dir,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn clear_env() {
        unsafe {
            env::remove_var("RFS_HOME");
            env::remove_var("RFS_CACHE_DIR");
            env::remove_var("RFS_SESSION_DIR");
        }
    }

    #[test]
    fn test_default_paths() {
        let _guard = crate::test_env::lock();
        clear_env();
        unsafe {
            env::set_var("HOME", "/home/testuser");
        }

        let config = Config::new().unwrap();
        assert_eq!(config.rfs_home, PathBuf::from("/home/testuser/.rfs"));
        assert_eq!(
            config.rfs_cache_dir,
            PathBuf::from("/home/testuser/.rfs/cache")
        );
        assert_eq!(
            config.rfs_session_dir,
            PathBuf::from("/home/testuser/.rfs/active")
        );
    }

    #[test]
    fn test_rfs_home_override() {
        let _guard = crate::test_env::lock();
        clear_env();
        unsafe {
            env::set_var("HOME", "/home/testuser");
            env::set_var("RFS_HOME", "/custom/rfs/home");
        }

        let config = Config::new().unwrap();
        assert_eq!(config.rfs_home, PathBuf::from("/custom/rfs/home"));
        assert_eq!(
            config.rfs_cache_dir,
            PathBuf::from("/custom/rfs/home/cache")
        );
        assert_eq!(
            config.rfs_session_dir,
            PathBuf::from("/custom/rfs/home/active")
        );
    }

    #[test]
    fn test_all_overrides() {
        let _guard = crate::test_env::lock();
        clear_env();
        unsafe {
            env::set_var("HOME", "/home/testuser");
            env::set_var("RFS_HOME", "/custom/rfs/home");
            env::set_var("RFS_CACHE_DIR", "/custom/rfs/cache");
            env::set_var("RFS_SESSION_DIR", "/custom/rfs/session");
        }

        let config = Config::new().unwrap();
        assert_eq!(config.rfs_home, PathBuf::from("/custom/rfs/home"));
        assert_eq!(config.rfs_cache_dir, PathBuf::from("/custom/rfs/cache"));
        assert_eq!(config.rfs_session_dir, PathBuf::from("/custom/rfs/session"));
    }

    #[test]
    fn test_no_home_var() {
        let _guard = crate::test_env::lock();
        clear_env();
        unsafe {
            env::remove_var("HOME");
        }

        let config = Config::new();
        assert_eq!(config, Err(ConfigError::RfsHomeNotResolved));
    }
}
