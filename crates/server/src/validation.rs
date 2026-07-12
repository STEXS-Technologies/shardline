use std::{
    io::{Error, ErrorKind},
    path::Path,
};

#[cfg(not(unix))]
use tokio::fs;
#[cfg(unix)]
#[allow(unused_imports)]
use std::os::unix::fs::OpenOptionsExt;

use crate::ServerError;

const MAX_IDENTIFIER_BYTES: usize = 1024;

pub(crate) fn validate_identifier(value: &str) -> Result<(), ServerError> {
    if value.trim().is_empty()
        || value == "."
        || value.len() > MAX_IDENTIFIER_BYTES
        || value.starts_with('/')
        || value.contains("..")
        || value.contains('\\')
        || value.contains('/')
        || value.chars().any(char::is_control)
    {
        return Err(ServerError::InvalidFileId);
    }

    Ok(())
}

pub(crate) fn validate_content_hash(value: &str) -> Result<(), ServerError> {
    shardline_server_core::validate_content_hash_with(value, || ServerError::InvalidContentHash)
}

/// Validates that `path` exists, is a directory, and is not a symlink.
///
/// Uses `O_NOFOLLOW` on Unix (via `std::fs::OpenOptions::custom_flags`) to
/// open the directory without following any trailing symlink, then stats
/// the fd to validate the type.  This eliminates the TOCTOU window between
/// the `symlink_metadata` check and subsequent file operations by callers.
///
/// On non-Unix platforms, falls back to `symlink_metadata` with a best-effort
/// check.  Callers on non-Unix should perform their own TOCTOU-safe open.
#[cfg(unix)]
pub(crate) async fn ensure_directory(path: &Path) -> Result<(), ServerError> {
    let file = tokio::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .await
        .map_err(|e| {
            ServerError::Io(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "failed to open directory at {}: {e}",
                    path.display()
                ),
            ))
        })?;
    let metadata = file.metadata().await?;
    if !metadata.is_dir() {
        return Err(ServerError::Io(Error::new(
            ErrorKind::InvalidData,
            format!("expected directory at {}", path.display()),
        )));
    }
    Ok(())
}

/// Non-Unix fallback: uses `symlink_metadata` which has a TOCTOU window
/// between this check and subsequent file operations.  Callers on these
/// platforms should add their own fd-based validation if the path is
/// attacker-influenced.
#[cfg(not(unix))]
pub(crate) async fn ensure_directory(path: &Path) -> Result<(), ServerError> {
    let metadata = fs::symlink_metadata(path).await?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ServerError::Io(Error::new(
            ErrorKind::InvalidData,
            format!("expected directory at {}", path.display()),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{io::ErrorKind, path::Path};

    use tokio::fs::create_dir_all;

    use super::{MAX_IDENTIFIER_BYTES, validate_content_hash, validate_identifier};
    use crate::ServerError;

    #[test]
    fn identifier_rejects_dot_alias() {
        let result = validate_identifier(".");

        assert!(matches!(result, Err(ServerError::InvalidFileId)));
    }

    #[test]
    fn identifier_rejects_path_separator() {
        let result = validate_identifier("nested/file");

        assert!(matches!(result, Err(ServerError::InvalidFileId)));
    }

    #[test]
    fn identifier_rejects_oversized_values() {
        let oversized = "f".repeat(MAX_IDENTIFIER_BYTES + 1);
        let result = validate_identifier(&oversized);

        assert!(matches!(result, Err(ServerError::InvalidFileId)));
    }

    #[test]
    fn content_hash_accepts_lowercase_hex() {
        let result = validate_content_hash(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        );

        assert!(result.is_ok());
    }

    #[test]
    fn content_hash_rejects_uppercase_hex() {
        let result = validate_content_hash(
            "0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef",
        );

        assert!(matches!(result, Err(ServerError::InvalidContentHash)));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ensure_directory_rejects_symlinked_directory() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir();
        assert!(temp.is_ok());
        let Ok(temp) = temp else {
            return;
        };
        let target = temp.path().join("target");
        let create = create_dir_all(&target).await;
        assert!(create.is_ok());
        let link = temp.path().join("link");
        let linked = symlink(Path::new(&target), Path::new(&link));
        assert!(linked.is_ok());

        let result = super::ensure_directory(&link).await;

        assert!(matches!(
            result,
            Err(ServerError::Io(error)) if error.kind() == ErrorKind::InvalidData
        ));
    }
}
