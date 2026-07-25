use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::cache::read_file_range;
use super::{SessionError, create_dir_private};

/// Owns physical local content beneath one active session.
pub(super) struct OverlayStore {
    root: PathBuf,
}

impl OverlayStore {
    pub(super) fn open(root: PathBuf) -> Result<Self, SessionError> {
        create_dir_private(&root)?;
        create_dir_private(&root.join("data"))?;
        create_dir_private(&root.join("tmp"))?;
        Ok(Self { root })
    }

    pub(super) fn read_range(
        &self,
        relative: &Path,
        offset: u64,
        size: usize,
    ) -> Result<Bytes, SessionError> {
        read_file_range(&self.root.join("data").join(relative), offset, size)
    }
}
