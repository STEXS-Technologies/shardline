use std::{
    fs::{self, Metadata},
    io::ErrorKind,
    path::Path,
};

use super::store::LocalObjectStoreError;
use super::util::map_directory_path_error;

pub fn object_file_metadata(path: &Path) -> Result<Option<Metadata>, LocalObjectStoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure_regular_file_metadata(&metadata)?;
            Ok(Some(metadata))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LocalObjectStoreError::Io(error)),
    }
}

/// Returns the file's last-modified time as unix nanoseconds since the epoch,
/// when the filesystem exposes one at or after the epoch.
///
/// The returned timestamp is observed by the LOCAL object store backend (the
/// GC's own view of the file), so it is immune to divergence between the
/// writer's wall clock and the GC host's wall clock.
#[must_use]
pub fn modified_unix_nanos(metadata: &Metadata) -> Option<u64> {
    let modified = metadata.modified().ok()?;
    let since_epoch = modified.duration_since(std::time::UNIX_EPOCH).ok()?;
    u64::try_from(since_epoch.as_nanos()).ok()
}

// This function is `pub(super)`-accessible; used from mod.rs, io.rs, and walk.rs.
pub fn ensure_parent_directories_are_not_symlinked(
    root: &Path,
    path: &Path,
) -> Result<(), LocalObjectStoreError> {
    ensure_directory_path_components_are_not_symlinked(root)?;
    let parent = if path == root {
        root
    } else {
        path.parent().unwrap_or(root)
    };
    ensure_directory_path_components_are_not_symlinked(parent)?;

    Ok(())
}

pub(super) fn ensure_directory_path_components_are_not_symlinked(
    path: &Path,
) -> Result<(), LocalObjectStoreError> {
    crate::ensure_directory_path_components_are_not_symlinked(path)
        .map_err(map_directory_path_error)
}

pub fn ensure_regular_file_metadata(metadata: &Metadata) -> Result<(), LocalObjectStoreError> {
    if metadata.file_type().is_file() {
        return Ok(());
    }

    Err(LocalObjectStoreError::InvalidObjectPath)
}
