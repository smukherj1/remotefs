use std::env;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ConfigError {
    #[error("Could not determine user home directory")]
    HomeDirNotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub rfs_home: PathBuf,
    pub rfs_cache_dir: PathBuf,
    pub rfs_session_dir: PathBuf,
}

impl Config {
    pub fn new() -> Result<Self, ConfigError> {
        let rfs_home = match env::var_os("RFS_HOME") {
            Some(val) => PathBuf::from(val),
            None => {
                let home = env::var_os("HOME").ok_or(ConfigError::HomeDirNotFound)?;
                Path::new(&home).join(".rfs")
            }
        };

        let rfs_cache_dir = match env::var_os("RFS_CACHE_DIR") {
            Some(val) => PathBuf::from(val),
            None => rfs_home.join("cache"),
        };

        let rfs_session_dir = match env::var_os("RFS_SESSION_DIR") {
            Some(val) => PathBuf::from(val),
            None => rfs_home.join("active"),
        };

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
        clear_env();
        unsafe {
            env::remove_var("HOME");
        }

        let config = Config::new();
        assert_eq!(config, Err(ConfigError::HomeDirNotFound));
    }
}
