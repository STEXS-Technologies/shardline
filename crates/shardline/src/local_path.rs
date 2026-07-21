use std::{
    io::{self, ErrorKind},
    path::Path,
};

use shardline_storage::{
    DirectoryPathError,
    ensure_directory_path_components_are_not_symlinked as ensure_directory_path_components_are_not_symlinked_shared,
};

/// Rejects directory paths that traverse existing symlink components.
///
/// # Errors
///
/// Returns [`io::Error`] when one existing component is a symlink or non-directory.
pub(crate) fn ensure_directory_path_components_are_not_symlinked(path: &Path) -> io::Result<()> {
    ensure_directory_path_components_are_not_symlinked_shared(path)
        .map_err(map_directory_path_error)
}

fn map_directory_path_error(error: DirectoryPathError) -> io::Error {
    match error {
        DirectoryPathError::UnsupportedPrefix => io::Error::new(
            ErrorKind::InvalidInput,
            "directory path contains an unsupported prefix component",
        ),
        DirectoryPathError::SymlinkedComponent(path) => io::Error::new(
            ErrorKind::InvalidInput,
            format!(
                "directory path contains a symlinked component: {}",
                path.display()
            ),
        ),
        DirectoryPathError::NonDirectoryComponent(path) => io::Error::new(
            ErrorKind::InvalidInput,
            format!(
                "directory path contains a non-directory component: {}",
                path.display()
            ),
        ),
        DirectoryPathError::Io(error) => error,
    }
}

use crate::config::effective_root;
use shardline_server::ServerConfigError;
use std::path::PathBuf;

/// Resolves the server root directory, falling back to the effective root when
/// no override is provided.
///
/// # Errors
///
/// Returns [`ServerConfigError`] when the root cannot be determined from the
/// environment or the provided override.
pub fn resolve_root(root_override: Option<&Path>) -> Result<PathBuf, ServerConfigError> {
    effective_root(root_override)
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::ErrorKind as IoErrorKind};

    use super::{
        ensure_directory_path_components_are_not_symlinked, map_directory_path_error, resolve_root,
    };

    #[test]
    fn preserves_invalid_input_mapping_for_shared_path_errors() {
        let sandbox = tempfile::tempdir().unwrap();
        let file_path = sandbox.path().join("file");
        let file = File::create(&file_path);
        assert!(file.is_ok());

        let result = ensure_directory_path_components_are_not_symlinked(&file_path.join("child"));

        assert!(matches!(
            result,
            Err(error) if error.kind() == IoErrorKind::InvalidInput
        ));
    }

    #[test]
    fn unsupported_prefix_maps_to_invalid_input() {
        use shardline_storage::DirectoryPathError;
        let err = map_directory_path_error(DirectoryPathError::UnsupportedPrefix);
        assert_eq!(err.kind(), IoErrorKind::InvalidInput);
        let msg = err.to_string();
        assert!(msg.contains("unsupported prefix"));
    }

    #[test]
    fn io_error_is_passed_through_unwrapped() {
        use shardline_storage::DirectoryPathError;
        let inner = std::io::Error::new(IoErrorKind::PermissionDenied, "disk error");
        let err = map_directory_path_error(DirectoryPathError::Io(inner));
        assert_eq!(err.kind(), IoErrorKind::PermissionDenied);
        assert!(err.to_string().contains("disk error"));
    }

    #[test]
    fn symlinked_component_maps_to_invalid_input() {
        use shardline_storage::DirectoryPathError;
        let err = map_directory_path_error(DirectoryPathError::SymlinkedComponent(
            std::path::PathBuf::from("/etc/passwd"),
        ));
        assert_eq!(err.kind(), IoErrorKind::InvalidInput);
        assert!(err.to_string().contains("symlinked component"));
        assert!(err.to_string().contains("etc/passwd"));
    }

    #[test]
    fn non_directory_component_maps_to_invalid_input() {
        use shardline_storage::DirectoryPathError;
        let err = map_directory_path_error(DirectoryPathError::NonDirectoryComponent(
            std::path::PathBuf::from("/dev/null"),
        ));
        assert_eq!(err.kind(), IoErrorKind::InvalidInput);
        assert!(err.to_string().contains("non-directory component"));
        assert!(err.to_string().contains("dev/null"));
    }

    // ── resolve_root ─────────────────────────────────────────────────

    #[test]
    fn resolve_root_with_temp_dir_succeeds() {
        let sandbox = tempfile::tempdir().unwrap();
        let result = resolve_root(Some(sandbox.path()));
        assert!(result.is_ok());
        let root = result.unwrap();
        assert!(root.is_absolute());
    }
}
