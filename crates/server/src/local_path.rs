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

#[cfg(test)]
mod tests {
    use std::io::{Error, ErrorKind};
    use std::path::PathBuf;

    use shardline_storage::DirectoryPathError;

    use crate::ServerError;

    #[test]
    fn map_unsupported_prefix() {
        let result = super::map_directory_path_error(DirectoryPathError::UnsupportedPrefix);
        assert!(
            matches!(&result, ServerError::Io(e) if e.kind() == ErrorKind::InvalidInput),
            "expected Io(InvalidInput), got {result:?}"
        );
    }

    #[test]
    fn map_symlinked_component() {
        let path = PathBuf::from("/test");
        let result = super::map_directory_path_error(DirectoryPathError::SymlinkedComponent(
            path.clone(),
        ));
        let msg = match &result {
            ServerError::Io(e) if e.kind() == ErrorKind::InvalidData => e.to_string(),
            other => panic!("expected Io(InvalidData), got {other:?}"),
        };
        assert!(
            msg.contains("/test"),
            "expected message to contain the path, got: {msg}"
        );
    }

    #[test]
    fn map_non_directory_component() {
        let path = PathBuf::from("/test");
        let result =
            super::map_directory_path_error(DirectoryPathError::NonDirectoryComponent(path));
        match result {
            ServerError::Io(e) if e.kind() == ErrorKind::InvalidData => {
                // expected
            }
            other => panic!("expected Io(InvalidData), got {other:?}"),
        }
    }

    #[test]
    fn map_io_error() {
        let io_err = Error::new(ErrorKind::NotFound, "test");
        let result = super::map_directory_path_error(DirectoryPathError::Io(io_err));
        match result {
            ServerError::Io(e) if e.kind() == ErrorKind::NotFound => {
                // expected
            }
            other => panic!("expected Io(NotFound), got {other:?}"),
        }
    }
}
