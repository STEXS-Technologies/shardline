#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs::{self, File, OpenOptions},
    io::{Error as IoError, ErrorKind, Read, Seek, SeekFrom},
    path::Path,
};

use shardline_protocol::ShardlineHash;

use crate::local_fs::hard_link_file_if_absent;
use crate::{ObjectIntegrity, PutOutcome};

use super::metadata::{ensure_parent_directories_are_not_symlinked, ensure_regular_file_metadata};
use super::store::LocalObjectStoreError;
use super::util::{VERIFY_BUFFER_A, VERIFY_BUFFER_B, VERIFY_BUFFER_BYTES};

pub fn verify_file_integrity(
    path: &Path,
    integrity: &ObjectIntegrity,
) -> Result<(), LocalObjectStoreError> {
    let file = open_existing_object_file(path)?;
    verify_open_file_integrity(file, integrity)
}

pub fn verify_open_file_integrity(
    mut file: File,
    integrity: &ObjectIntegrity,
) -> Result<(), LocalObjectStoreError> {
    file.seek(SeekFrom::Start(0))?;
    with_verify_buffer(|buffer| {
        let mut hasher = blake3::Hasher::new();
        let mut length = 0_u64;
        loop {
            let read = file.read(buffer)?;
            if read == 0 {
                break;
            }
            let read = u64::try_from(read)
                .map_err(|_error| LocalObjectStoreError::IntegrityLengthMismatch)?;
            length = length
                .checked_add(read)
                .ok_or(LocalObjectStoreError::IntegrityLengthMismatch)?;
            let read = usize::try_from(read)
                .map_err(|_error| LocalObjectStoreError::IntegrityLengthMismatch)?;
            hasher.update(
                buffer
                    .get(..read)
                    .ok_or(LocalObjectStoreError::IntegrityLengthMismatch)?,
            );
        }

        if length != integrity.length() {
            return Err(LocalObjectStoreError::IntegrityLengthMismatch);
        }

        let actual = ShardlineHash::from_bytes(*hasher.finalize().as_bytes());
        if actual != integrity.hash() {
            return Err(LocalObjectStoreError::IntegrityHashMismatch);
        }

        Ok(())
    })
}

pub fn link_temporary_file_if_absent(
    root: &Path,
    path: &Path,
    temporary: &Path,
    _integrity: &ObjectIntegrity,
    temporary_bytes: Option<&[u8]>,
) -> Result<PutOutcome, LocalObjectStoreError> {
    ensure_parent_directories_are_not_symlinked(root, path)?;
    match hard_link_file_if_absent(root, path, temporary) {
        Ok(()) => {
            remove_temporary_file(temporary)?;
            Ok(PutOutcome::Inserted)
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            let outcome = existing_object_outcome(path, temporary, temporary_bytes);
            remove_temporary_file(temporary)?;
            outcome
        }
        Err(error) => {
            remove_temporary_file(temporary)?;
            Err(LocalObjectStoreError::Io(error))
        }
    }
}

pub fn existing_object_outcome(
    path: &Path,
    temporary: &Path,
    temporary_bytes: Option<&[u8]>,
) -> Result<PutOutcome, LocalObjectStoreError> {
    let existing = open_existing_object_file(path)?;
    if let Some(temporary_bytes) = temporary_bytes {
        ensure_file_matches_bytes(existing, temporary_bytes)?;
        return Ok(PutOutcome::AlreadyExists);
    }

    let temporary = open_existing_object_file(temporary)?;
    ensure_files_match(existing, temporary)?;
    Ok(PutOutcome::AlreadyExists)
}

pub fn open_existing_object_file(path: &Path) -> Result<File, LocalObjectStoreError> {
    let file = open_regular_file(path).map_err(map_object_open_error)?;
    ensure_regular_file_metadata(&file.metadata()?)?;
    Ok(file)
}

#[cfg(unix)]
fn open_regular_file(path: &Path) -> Result<File, IoError> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(not(unix))]
fn open_regular_file(path: &Path) -> Result<File, IoError> {
    OpenOptions::new().read(true).open(path)
}

fn map_object_open_error(error: IoError) -> LocalObjectStoreError {
    if is_symlink_open_error(&error) {
        return LocalObjectStoreError::InvalidObjectPath;
    }

    LocalObjectStoreError::Io(error)
}

#[cfg(unix)]
fn is_symlink_open_error(error: &IoError) -> bool {
    error.raw_os_error() == Some(libc::ELOOP)
}

#[cfg(not(unix))]
fn is_symlink_open_error(_error: &IoError) -> bool {
    false
}

pub fn ensure_file_matches_bytes(
    mut file: File,
    expected: &[u8],
) -> Result<(), LocalObjectStoreError> {
    file.seek(SeekFrom::Start(0))?;
    let expected_length = u64::try_from(expected.len())
        .map_err(|_error| LocalObjectStoreError::IntegrityLengthMismatch)?;
    if file.metadata()?.len() != expected_length {
        return Err(LocalObjectStoreError::IntegrityLengthMismatch);
    }

    with_verify_buffer(|buffer| {
        let mut compared = 0_usize;
        loop {
            let read = file.read(buffer)?;
            if read == 0 {
                break;
            }
            let end = compared
                .checked_add(read)
                .ok_or(LocalObjectStoreError::IntegrityLengthMismatch)?;
            let Some(expected_slice) = expected.get(compared..end) else {
                return Err(LocalObjectStoreError::IntegrityLengthMismatch);
            };
            let Some(actual_slice) = buffer.get(..read) else {
                return Err(LocalObjectStoreError::IntegrityLengthMismatch);
            };
            if actual_slice != expected_slice {
                return Err(LocalObjectStoreError::IntegrityHashMismatch);
            }
            compared = end;
        }

        if compared != expected.len() {
            return Err(LocalObjectStoreError::IntegrityLengthMismatch);
        }

        Ok(())
    })
}

pub fn ensure_files_match(
    mut existing: File,
    mut expected: File,
) -> Result<(), LocalObjectStoreError> {
    existing.seek(SeekFrom::Start(0))?;
    expected.seek(SeekFrom::Start(0))?;
    if existing.metadata()?.len() != expected.metadata()?.len() {
        return Err(LocalObjectStoreError::IntegrityLengthMismatch);
    }

    with_verify_buffers(|existing_buffer, expected_buffer| {
        loop {
            let existing_read = existing.read(existing_buffer)?;
            let expected_read = expected.read(expected_buffer)?;
            if existing_read != expected_read {
                return Err(LocalObjectStoreError::IntegrityLengthMismatch);
            }
            if existing_read == 0 {
                break;
            }
            let Some(existing_slice) = existing_buffer.get(..existing_read) else {
                return Err(LocalObjectStoreError::IntegrityLengthMismatch);
            };
            let Some(expected_slice) = expected_buffer.get(..expected_read) else {
                return Err(LocalObjectStoreError::IntegrityLengthMismatch);
            };
            if existing_slice != expected_slice {
                return Err(LocalObjectStoreError::IntegrityHashMismatch);
            }
        }

        Ok(())
    })
}

fn with_verify_buffer<T>(
    callback: impl FnOnce(&mut [u8]) -> Result<T, LocalObjectStoreError>,
) -> Result<T, LocalObjectStoreError> {
    VERIFY_BUFFER_A.with(|buffer| {
        let mut buffer = buffer.borrow_mut();
        ensure_verify_buffer_length(&mut buffer);
        callback(buffer.as_mut_slice())
    })
}

fn with_verify_buffers<T>(
    callback: impl FnOnce(&mut [u8], &mut [u8]) -> Result<T, LocalObjectStoreError>,
) -> Result<T, LocalObjectStoreError> {
    VERIFY_BUFFER_A.with(|first| {
        VERIFY_BUFFER_B.with(|second| {
            let mut first = first.borrow_mut();
            let mut second = second.borrow_mut();
            ensure_verify_buffer_length(&mut first);
            ensure_verify_buffer_length(&mut second);
            callback(first.as_mut_slice(), second.as_mut_slice())
        })
    })
}

fn ensure_verify_buffer_length(buffer: &mut Vec<u8>) {
    if buffer.len() != VERIFY_BUFFER_BYTES {
        buffer.resize(VERIFY_BUFFER_BYTES, 0);
    }
}

pub fn remove_temporary_file(path: &Path) -> Result<(), LocalObjectStoreError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LocalObjectStoreError::Io(error)),
    }
}
