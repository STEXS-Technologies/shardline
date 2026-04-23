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

#[cfg(test)]
mod tests {
    use std::{fs::File, io::ErrorKind as IoErrorKind};

    use super::ensure_directory_path_components_are_not_symlinked;

    #[test]
    fn preserves_invalid_input_mapping_for_shared_path_errors() {
        let sandbox = tempfile::tempdir();
        assert!(sandbox.is_ok());
        let Ok(sandbox) = sandbox else {
            return;
        };
        let file_path = sandbox.path().join("file");
        let file = File::create(&file_path);
        assert!(file.is_ok());

        let result = ensure_directory_path_components_are_not_symlinked(&file_path.join("child"));

        assert!(matches!(
            result,
            Err(error) if error.kind() == IoErrorKind::InvalidInput
        ));
    }
}
