use std::{
    fs::{self, ReadDir},
    io::ErrorKind,
    path::Path,
};

use crate::{ObjectKey, ObjectMetadata};

use super::metadata::{
    ensure_directory_path_components_are_not_symlinked, ensure_regular_file_metadata,
    modified_unix_nanos,
};
use super::store::LocalObjectStoreError;

pub fn collect_metadata_recursive<Visitor, VisitorError>(
    root: &Path,
    directory: &Path,
    prefix: &str,
    visitor: &mut Visitor,
) -> Result<(), VisitorError>
where
    LocalObjectStoreError: Into<VisitorError>,
    Visitor: FnMut(ObjectMetadata) -> Result<(), VisitorError>,
{
    let Some(entries) = read_dir_if_exists(directory).map_err(Into::into)? else {
        return Ok(());
    };

    for entry in entries {
        let entry = entry
            .map_err(LocalObjectStoreError::Io)
            .map_err(Into::into)?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(LocalObjectStoreError::Io)
            .map_err(Into::into)?;
        if file_type.is_dir() {
            collect_metadata_recursive(root, &path, prefix, visitor)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        let relative = path
            .strip_prefix(root)
            .map_err(|_error| LocalObjectStoreError::InvalidStoredKey)
            .map_err(Into::into)?;
        let relative = relative
            .to_str()
            .ok_or(LocalObjectStoreError::InvalidStoredKey)
            .map_err(Into::into)?;
        let relative = relative.replace('\\', "/");
        if !relative.starts_with(prefix) {
            continue;
        }

        let key = ObjectKey::parse(&relative)
            .map_err(|_error| LocalObjectStoreError::InvalidStoredKey)
            .map_err(Into::into)?;
        let fs_metadata = fs::symlink_metadata(&path)
            .map_err(LocalObjectStoreError::Io)
            .map_err(Into::into)?;
        ensure_regular_file_metadata(&fs_metadata).map_err(Into::into)?;
        let mut metadata = ObjectMetadata::new(key, fs_metadata.len(), None);
        // The observed mtime is backend truth, so attach it when available.
        if let Some(modified) = modified_unix_nanos(&fs_metadata) {
            metadata = metadata.with_modified(modified);
        }
        visitor(metadata)?;
    }

    Ok(())
}

pub fn read_dir_if_exists(directory: &Path) -> Result<Option<ReadDir>, LocalObjectStoreError> {
    ensure_directory_path_components_are_not_symlinked(directory)?;
    match fs::read_dir(directory) {
        Ok(entries) => Ok(Some(entries)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LocalObjectStoreError::Io(error)),
    }
}

pub fn remove_empty_ancestors(path: &Path, root: &Path) -> Result<(), LocalObjectStoreError> {
    let mut current = path.parent();
    while let Some(directory) = current {
        if directory == root {
            break;
        }

        match fs::remove_dir(directory) {
            Ok(()) => {
                current = directory.parent();
            }
            Err(error) if error.kind() == ErrorKind::DirectoryNotEmpty => break,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                current = directory.parent();
            }
            Err(error) => return Err(LocalObjectStoreError::Io(error)),
        }
    }

    Ok(())
}
