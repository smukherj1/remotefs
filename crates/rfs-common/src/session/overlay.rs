use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::cache::read_file_range;
use super::{SessionError, create_dir_if_absent};

/// Owns physical local content beneath one active session.
pub(super) struct OverlayStore {
    root: PathBuf,
}

impl OverlayStore {
    pub fn open(root: PathBuf) -> Result<Self, SessionError> {
        tracing::info!("OverlayStore::open(root={})", root.display());
        create_dir_if_absent(&root)?;
        create_dir_if_absent(&root.join("data"))?;
        create_dir_if_absent(&root.join("tmp"))?;
        Ok(Self { root })
    }

    pub fn read_range(
        &self,
        relative: &Path,
        offset: u64,
        size: usize,
    ) -> Result<Bytes, SessionError> {
        read_file_range(&self.root.join("data").join(relative), offset, size)
    }
}
