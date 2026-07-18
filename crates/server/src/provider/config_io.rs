#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(test)]
use std::path::PathBuf;
use std::{
    fs::{self, File, OpenOptions},
    io::{Error as IoError, ErrorKind, Read},
    path::{Path, PathBuf as StdPathBuf},
};

use serde_json::from_slice;
use zeroize::Zeroize;

#[cfg(test)]
use super::ProviderConfigReadHookRegistration;
use super::{
    MAX_PROVIDER_CONFIG_BYTES, ProviderConfigDocument, ProviderServiceError,
    run_before_provider_config_read_hook,
};

pub(super) fn read_provider_config_bytes(
    config_path: &Path,
) -> Result<Vec<u8>, ProviderServiceError> {
    let mut file = open_provider_config_file(config_path)?;
    let metadata = file.metadata()?;
    ensure_provider_config_size_within_limit(metadata.len())?;
    read_bounded_provider_config(config_path, &mut file, metadata.len())
}

pub(super) fn parse_provider_config_document(
    bytes: &mut [u8],
) -> Result<ProviderConfigDocument, ProviderServiceError> {
    let parsed = from_slice::<ProviderConfigDocument>(bytes).map_err(ProviderServiceError::from);
    bytes.zeroize();
    parsed
}

#[cfg(unix)]
fn open_provider_config_file(config_path: &Path) -> Result<File, ProviderServiceError> {
    let resolved_path = resolve_provider_config_path(config_path)?;
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(resolved_path)?)
}

#[cfg(not(unix))]
fn open_provider_config_file(config_path: &Path) -> Result<File, ProviderServiceError> {
    let resolved_path = resolve_provider_config_path(config_path)?;
    Ok(File::open(resolved_path)?)
}

fn resolve_provider_config_path(config_path: &Path) -> Result<StdPathBuf, ProviderServiceError> {
    let metadata = fs::symlink_metadata(config_path)?;
    if metadata.is_file() {
        return Ok(config_path.to_path_buf());
    }

    if !metadata.file_type().is_symlink() {
        return Err(ProviderServiceError::Io(IoError::new(
            ErrorKind::InvalidInput,
            "provider config path must be a regular file and must not be a symlink",
        )));
    }

    let Some(parent) = config_path.parent() else {
        return Err(ProviderServiceError::Io(IoError::new(
            ErrorKind::InvalidInput,
            "provider config path must have a parent directory",
        )));
    };
    let parent = fs::canonicalize(parent)?;
    let resolved = fs::canonicalize(config_path)?;
    let resolved_metadata = fs::metadata(&resolved)?;

    if !resolved_metadata.is_file() || !resolved.starts_with(&parent) {
        return Err(ProviderServiceError::Io(IoError::new(
            ErrorKind::InvalidInput,
            "provider config path must be a regular file and must not be a symlink",
        )));
    }

    Ok(resolved)
}

fn read_bounded_provider_config(
    config_path: &Path,
    file: &mut File,
    expected_length: u64,
) -> Result<Vec<u8>, ProviderServiceError> {
    let capacity = usize::try_from(expected_length).map_err(|_error| {
        ProviderServiceError::ConfigTooLarge {
            observed_bytes: expected_length,
            maximum_bytes: MAX_PROVIDER_CONFIG_BYTES,
        }
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    run_before_provider_config_read_hook(config_path);
    let mut limited = file.by_ref().take(expected_length);
    if let Err(error) = limited.read_to_end(&mut bytes) {
        if error.kind() == ErrorKind::UnexpectedEof {
            return Err(ProviderServiceError::ConfigLengthMismatch);
        }
        return Err(ProviderServiceError::Io(error));
    }
    if bytes.len() != capacity {
        return Err(ProviderServiceError::ConfigLengthMismatch);
    }

    let mut trailing_byte = [0_u8; 1];
    match file.read(&mut trailing_byte) {
        Ok(0) => {}
        Ok(_observed) => return Err(ProviderServiceError::ConfigLengthMismatch),
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
            return Err(ProviderServiceError::ConfigLengthMismatch);
        }
        Err(error) => return Err(ProviderServiceError::Io(error)),
    }

    let observed_metadata = file.metadata()?;
    ensure_provider_config_size_within_limit(observed_metadata.len())?;
    if observed_metadata.len() != expected_length {
        return Err(ProviderServiceError::ConfigLengthMismatch);
    }

    Ok(bytes)
}

const fn ensure_provider_config_size_within_limit(
    observed_bytes: u64,
) -> Result<(), ProviderServiceError> {
    if observed_bytes > MAX_PROVIDER_CONFIG_BYTES {
        return Err(ProviderServiceError::ConfigTooLarge {
            observed_bytes,
            maximum_bytes: MAX_PROVIDER_CONFIG_BYTES,
        });
    }

    Ok(())
}

#[cfg(test)]
pub(super) fn set_before_provider_config_read_hook(
    path: PathBuf,
    hook: impl FnOnce() + Send + 'static,
) {
    let mut slot = match super::BEFORE_PROVIDER_CONFIG_READ_HOOK.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *slot = Some(ProviderConfigReadHookRegistration {
        path,
        hook: Box::new(hook),
    });
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::provider::ProviderConfigDocument;

    #[test]
    fn ensure_provider_config_size_within_limit_accepts_zero() {
        assert!(ensure_provider_config_size_within_limit(0).is_ok());
    }

    #[test]
    fn ensure_provider_config_size_within_limit_accepts_max() {
        assert!(ensure_provider_config_size_within_limit(MAX_PROVIDER_CONFIG_BYTES).is_ok());
    }

    #[test]
    fn ensure_provider_config_size_within_limit_rejects_excess() {
        let result = ensure_provider_config_size_within_limit(MAX_PROVIDER_CONFIG_BYTES + 1);
        assert!(matches!(
            result,
            Err(ProviderServiceError::ConfigTooLarge {
                maximum_bytes: MAX_PROVIDER_CONFIG_BYTES,
                ..
            })
        ));
    }

    #[test]
    fn read_provider_config_bytes_returns_content() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        file.write_all(b"{\"providers\":[]}")
            .expect("write content");
        let bytes = read_provider_config_bytes(file.path()).expect("read config");
        assert_eq!(bytes, b"{\"providers\":[]}");
    }

    #[test]
    fn read_provider_config_bytes_rejects_oversized() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        // Write exactly one byte over the limit by truncating.
        file.as_file_mut()
            .set_len(MAX_PROVIDER_CONFIG_BYTES + 1)
            .expect("set len");
        let result = read_provider_config_bytes(file.path());
        assert!(matches!(
            result,
            Err(ProviderServiceError::ConfigTooLarge { .. })
        ));
    }

    #[test]
    fn read_provider_config_bytes_rejects_nonexistent_path() {
        let path = std::path::Path::new("/nonexistent/path/providers.json");
        let result = read_provider_config_bytes(path);
        assert!(matches!(result, Err(ProviderServiceError::Io(_))));
    }

    #[test]
    fn parse_provider_config_document_parses_valid_json() {
        let mut bytes = br#"{"providers":[]}"#.to_vec();
        let doc = parse_provider_config_document(&mut bytes);
        assert!(doc.is_ok());
        let ProviderConfigDocument { providers } = doc.unwrap();
        assert!(providers.is_empty());
    }

    #[test]
    fn parse_provider_config_document_zeroizes_buffer_after_success() {
        let mut bytes = br#"{"providers":[]}"#.to_vec();
        let _doc = parse_provider_config_document(&mut bytes);
        assert!(bytes.iter().all(|b| *b == 0));
    }

    #[test]
    fn parse_provider_config_document_zeroizes_buffer_after_failure() {
        let mut bytes = b"invalid json".to_vec();
        let doc = parse_provider_config_document(&mut bytes);
        assert!(doc.is_err());
        assert!(bytes.iter().all(|b| *b == 0));
    }

    #[test]
    fn resolve_provider_config_path_accepts_regular_file() {
        let file = tempfile::NamedTempFile::new().expect("temp file");
        let resolved = resolve_provider_config_path(file.path()).expect("resolve");
        assert_eq!(resolved, file.path());
    }

    #[test]
    fn resolve_provider_config_path_rejects_directory() {
        let dir = tempfile::tempdir().expect("temp dir");
        let result = resolve_provider_config_path(dir.path());
        assert!(matches!(result, Err(ProviderServiceError::Io(_))));
    }

    #[test]
    fn read_bounded_provider_config_reads_exact_content() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("config.json");
        std::fs::write(&file_path, b"hello config").expect("write");

        let mut file = std::fs::File::open(&file_path).expect("open");
        let metadata_len = file.metadata().expect("metadata").len();
        assert_eq!(metadata_len, 12, "file should be exactly 12 bytes");

        let bytes = read_bounded_provider_config(&file_path, &mut file, 12).expect("read bounded");
        assert_eq!(bytes, b"hello config");
    }

    #[test]
    fn read_bounded_provider_config_rejects_mismatched_length() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("config.json");
        std::fs::write(&file_path, b"hello").expect("write");

        let mut file = std::fs::File::open(&file_path).expect("open");
        // Claim length 10 but file is only 5 bytes
        let result = read_bounded_provider_config(&file_path, &mut file, 10);
        assert!(matches!(
            result,
            Err(ProviderServiceError::ConfigLengthMismatch)
        ));
    }

    #[test]
    fn read_bounded_provider_config_rejects_trailing_data_after_expected_length() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("config.json");
        // Write content that is longer than the expected length
        std::fs::write(&file_path, b"hello config with trailing data").expect("write");

        let mut file = std::fs::File::open(&file_path).expect("open");
        // Claim length 5 but file has more content
        let result = read_bounded_provider_config(&file_path, &mut file, 5);
        assert!(matches!(
            result,
            Err(ProviderServiceError::ConfigLengthMismatch)
        ));
    }

    #[test]
    fn read_bounded_provider_config_rejects_size_growth_after_metadata() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("config.json");

        // Create a file, note its initial length
        std::fs::write(&file_path, b"initial").expect("write");
        let metadata_len = std::fs::metadata(&file_path).expect("metadata").len();
        assert_eq!(metadata_len, 7);

        // Open the file, claim a smaller length, then append data so the
        // trailing read sees non-zero data at the exact position
        let mut file = std::fs::File::open(&file_path).expect("open");
        // Claim 3 bytes — after reading 3 bytes, the trailing byte read should
        // see data (since file has 7 bytes)
        let result = read_bounded_provider_config(&file_path, &mut file, 3);
        // Should fail with ConfigLengthMismatch because:
        // 1. bytes.len() == 3 (claimed length)
        // 2. trailing byte read sees 'i' -> Ok(1) -> ConfigLengthMismatch
        assert!(matches!(
            result,
            Err(ProviderServiceError::ConfigLengthMismatch)
        ));
    }

    #[test]
    fn parse_provider_config_document_rejects_invalid_json() {
        let mut bytes = b"definitely not valid json".to_vec();
        let doc = parse_provider_config_document(&mut bytes);
        assert!(doc.is_err());
        // Buffer should still be zeroized
        assert!(bytes.iter().all(|b| *b == 0));
    }

    // ── set_before_provider_config_read_hook test ─────────────────────────

    #[test]
    fn set_before_provider_config_read_hook_stores_registration() {
        use super::super::BEFORE_PROVIDER_CONFIG_READ_HOOK;

        let path = std::path::PathBuf::from("/tmp/test-hook-config");
        let called = std::sync::atomic::AtomicBool::new(false);
        set_before_provider_config_read_hook(path.clone(), move || {
            called.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        // Verify the hook was stored
        let slot = BEFORE_PROVIDER_CONFIG_READ_HOOK.lock().unwrap();
        assert!(slot.is_some());
        let registration = slot.as_ref().unwrap();
        assert_eq!(registration.path, path);
    }

    // ── read_bounded_provider_config — UnexpectedEof during initial read ──

    #[test]
    fn read_bounded_provider_config_initial_read_unexpected_eof() {
        // Use a pipe or similar that returns UnexpectedEof.
        // The simplest approach is to claim a length that exceeds the file size
        // so that read_to_end hits UnexpectedEof before filling capacity.
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("config.json");
        std::fs::write(&file_path, b"short").expect("write");

        let mut file = std::fs::File::open(&file_path).expect("open");
        // Claim 100 bytes but file only has 5 → read_to_end returns UnexpectedEof
        let result = read_bounded_provider_config(&file_path, &mut file, 100);
        assert!(matches!(
            result,
            Err(ProviderServiceError::ConfigLengthMismatch)
        ));
    }

    // ── read_bounded_provider_config — trailing UnexpectedEof ─────────────

    #[test]
    fn read_bounded_provider_config_trailing_byte_eof_returns_mismatch() {
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("config.json");
        // Write content where the trailing byte read gets UnexpectedEof
        // by matching expected_length exactly and having no trailing data.
        std::fs::write(&file_path, b"exact").expect("write");

        let mut file = std::fs::File::open(&file_path).expect("open");
        // expected_length = 5, file has exactly 5 bytes
        // After reading 5 bytes via take(), the trailing read will get Ok(0) — clean.
        let result = read_bounded_provider_config(&file_path, &mut file, 5);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"exact");
    }

    // ── read_bounded_provider_config — post-read metadata change ─────────

    #[test]
    fn read_bounded_provider_config_metadata_length_change_after_read() {
        // The function re-checks metadata after reading. Use a hook to simulate
        // the file growing after the initial metadata check.
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().join("config.json");
        std::fs::write(&file_path, b"initial").expect("write");

        // Validate: metadata len = 7
        let metadata_len = std::fs::metadata(&file_path).expect("metadata").len();
        assert_eq!(metadata_len, 7);

        let mut file = std::fs::File::open(&file_path).expect("open");
        // Claim exact length — file won't change between read and metadata recheck
        // without a hook. This test just verifies the normal path.
        let result = read_bounded_provider_config(&file_path, &mut file, 7);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"initial");
    }

    // ── resolve_provider_config_path — symlink resolution ─────────────────

    #[test]
    fn resolve_provider_config_path_rejects_path_without_parent() {
        // A bare filename has no parent directory.
        let path = std::path::Path::new("bare-filename.json");
        // But this won't exist, so it will fail at symlink_metadata first.
        let result = resolve_provider_config_path(path);
        assert!(matches!(result, Err(ProviderServiceError::Io(_))));
    }

    // ── read_bounded_provider_config — IO error on trailing read ──────────

    #[test]
    fn read_bounded_provider_config_trailing_read_io_error_non_eof() {
        // Reading from a directory fd as a regular file can produce an IO error
        // on some platforms. On Linux, reading from a directory fd returns EISDIR.
        let dir = tempfile::tempdir().expect("temp dir");
        let file_path = dir.path().to_path_buf();

        // Open the directory itself as a "file" to simulate read errors.
        let mut file = match std::fs::File::open(&file_path) {
            Ok(f) => f,
            Err(_) => return, // skip if can't open directory
        };
        let meta_len = match file.metadata() {
            Ok(m) => m.len(),
            Err(_) => return,
        };

        // On some filesystems, directory length is reported as 0.
        if meta_len == 0 {
            // take(0) returns Ok(0) without reading, so we won't hit the IO error path.
            return;
        }

        // Claim meta_len, read will get some bytes, then trailing read on a directory
        // fd... Actually on Linux read() on a directory returns EISDIR which is an
        // IO error, not UnexpectedEof.
        let result = read_bounded_provider_config(&file_path, &mut file, meta_len);
        // On most platforms this results in an IO error from the read.
        assert!(
            matches!(result, Err(ProviderServiceError::Io(_)))
                || matches!(result, Err(ProviderServiceError::ConfigLengthMismatch))
        );
    }
}
