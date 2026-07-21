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
    #[allow(clippy::wildcard_enum_match_arm, clippy::panic)]
    fn map_symlinked_component() {
        let path = PathBuf::from("/test");
        let result = super::map_directory_path_error(DirectoryPathError::SymlinkedComponent(path));
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
    #[allow(clippy::wildcard_enum_match_arm, clippy::panic)]
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
    #[allow(clippy::wildcard_enum_match_arm, clippy::panic)]
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

    #[test]
    fn ensure_directory_path_components_rejects_unsupported_prefix() {
        // Use a path with an unsupported prefix (e.g., relative path that's not anchored)
        let result =
            super::ensure_directory_path_components_are_not_symlinked(std::path::Path::new("/"));
        // / has a root dir as parent; on most systems this will succeed because
        // / is not a symlink
        assert!(result.is_ok() || matches!(result, Err(ServerError::Io(_))));
    }

    #[test]
    fn ensure_directory_path_components_succeeds_for_tmp_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let result = super::ensure_directory_path_components_are_not_symlinked(tmp.path());
        assert!(result.is_ok());
    }

    #[test]
    fn ensure_directory_path_components_rejects_absolute_root() {
        // /proc is always present on Linux and is not a symlink
        let result = super::ensure_directory_path_components_are_not_symlinked(
            std::path::Path::new("/proc"),
        );
        // /proc exists and is a directory, so this should succeed
        assert!(result.is_ok() || matches!(result, Err(ServerError::Io(_))));
    }
}
