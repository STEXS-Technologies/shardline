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
