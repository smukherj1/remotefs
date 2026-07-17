use std::env;
use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ConfigError {
    #[error("unable to determine RFS_HOME because neither RFS_HOME nor HOME is set")]
    RfsHomeNotResolved,
}

/// Unresolved process configuration for RemoteFS local state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub rfs_home: PathBuf,
}

impl Config {
    /// Resolves `RFS_HOME`, defaulting to `$HOME/.rfs`.
    ///
    /// Filesystem creation and canonicalization are performed by `StatePaths`
    /// at the boundary where local state is used.
    pub fn new() -> Result<Self, ConfigError> {
        let rfs_home = match env::var_os("RFS_HOME") {
            Some(value) => PathBuf::from(value),
            None => {
                let home = env::var_os("HOME").ok_or(ConfigError::RfsHomeNotResolved)?;
                Path::new(&home).join(".rfs")
            }
        };
        Ok(Self { rfs_home })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clear_env() {
        unsafe {
            env::remove_var("RFS_HOME");
        }
    }

    #[test]
    fn default_path_uses_home() {
        let _guard = crate::test_env::lock();
        clear_env();
        unsafe { env::set_var("HOME", "/home/testuser") };
        assert_eq!(
            Config::new().unwrap().rfs_home,
            PathBuf::from("/home/testuser/.rfs")
        );
    }

    #[test]
    fn rfs_home_is_the_only_state_override() {
        let _guard = crate::test_env::lock();
        clear_env();
        unsafe {
            env::set_var("HOME", "/home/testuser");
            env::set_var("RFS_HOME", "/custom/rfs");
            env::set_var("RFS_CACHE_DIR", "/ignored/cache");
            env::set_var("RFS_SESSION_DIR", "/ignored/active");
        }
        assert_eq!(
            Config::new().unwrap().rfs_home,
            PathBuf::from("/custom/rfs")
        );
    }

    #[test]
    fn missing_home_is_an_error() {
        let _guard = crate::test_env::lock();
        clear_env();
        unsafe { env::remove_var("HOME") };
        assert_eq!(Config::new(), Err(ConfigError::RfsHomeNotResolved));
    }
}
