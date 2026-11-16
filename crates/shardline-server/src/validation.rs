use std::{
    io::{Error, ErrorKind},
    path::Path,
};

#[cfg(not(unix))]
use tokio::fs;

use crate::ServerError;

pub(crate) fn validate_identifier(value: &str) -> Result<(), ServerError> {
    shardline_server_core::validate_identifier(value).map_err(|_error| ServerError::InvalidFileId)
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
                format!("failed to open directory at {}: {e}", path.display()),
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
    use super::{validate_content_hash, validate_identifier};
    use crate::ServerError;

    #[test]
    fn identifier_validation_preserves_server_error_mapping() {
        assert!(validate_identifier("my-file_v1.2.bin").is_ok());
        for value in [
            ".".to_owned(),
            "nested/file".to_owned(),
            "f".repeat(1025),
            String::new(),
            "   ".to_owned(),
            "/etc/passwd".to_owned(),
            "safe/../etc".to_owned(),
            "safe\\..\\etc".to_owned(),
            "test\x00file".to_owned(),
        ] {
            assert!(
                matches!(validate_identifier(&value), Err(ServerError::InvalidFileId)),
                "{value:?}"
            );
        }
    }

    #[test]
    fn content_hash_validation_preserves_server_error_mapping() {
        assert!(
            validate_content_hash(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            )
            .is_ok()
        );
        assert!(matches!(
            validate_content_hash(
                "0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef"
            ),
            Err(ServerError::InvalidContentHash)
        ));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ensure_directory_accepts_only_existing_non_symlink_directories() {
        use std::{io::ErrorKind, os::unix::fs::symlink};
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("file");
        tokio::fs::write(&file, b"content").await.unwrap();
        let missing = temp.path().join("missing");
        let link = temp.path().join("link");
        symlink(temp.path(), &link).unwrap();
        for (path, valid) in [
            (temp.path(), true),
            (&file, false),
            (&missing, false),
            (&link, false),
        ] {
            let result = super::ensure_directory(path).await;
            if valid {
                assert!(result.is_ok(), "{}: {result:?}", path.display());
            } else {
                assert!(
                    matches!(&result, Err(ServerError::Io(error)) if error.kind() == ErrorKind::InvalidData),
                    "{}: {result:?}",
                    path.display()
                );
            }
        }
    }
}
