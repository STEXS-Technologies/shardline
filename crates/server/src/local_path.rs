use std::{
    io::{Error, ErrorKind},
    path::Path,
};

use shardline_storage::{
    DirectoryPathError,
    ensure_directory_path_components_are_not_symlinked as ensure_directory_path_components_are_not_symlinked_shared,
};

use crate::ServerError;

pub(crate) fn ensure_directory_path_components_are_not_symlinked(
    path: &Path,
) -> Result<(), ServerError> {
    ensure_directory_path_components_are_not_symlinked_shared(path)
        .map_err(map_directory_path_error)
}

fn map_directory_path_error(error: DirectoryPathError) -> ServerError {
    match error {
        DirectoryPathError::UnsupportedPrefix => ServerError::Io(Error::new(
            ErrorKind::InvalidInput,
            "directory path contains an unsupported prefix component",
        )),
        DirectoryPathError::SymlinkedComponent(path) => ServerError::Io(Error::new(
            ErrorKind::InvalidData,
            format!(
                "directory path contains a symlinked component: {}",
                path.display()
            ),
        )),
        DirectoryPathError::NonDirectoryComponent(path) => ServerError::Io(Error::new(
            ErrorKind::InvalidData,
            format!(
                "directory path contains a non-directory component: {}",
                path.display()
            ),
        )),
        DirectoryPathError::Io(error) => ServerError::Io(error),
    }
}
