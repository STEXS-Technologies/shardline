use std::cell::RefCell;

use shardline_protocol::ShardlineHash;

use crate::{DirectoryPathError, ObjectIntegrity};

use super::LocalObjectStoreError;

/// Number of bytes used for integrity verification buffers.
pub const VERIFY_BUFFER_BYTES: usize = 256 * 1024;

thread_local! {
    pub static VERIFY_BUFFER_A: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    pub static VERIFY_BUFFER_B: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

pub fn verify_integrity(
    bytes: &[u8],
    integrity: &ObjectIntegrity,
) -> Result<(), LocalObjectStoreError> {
    let body_length = u64::try_from(bytes.len())
        .map_err(|_error| LocalObjectStoreError::IntegrityLengthMismatch)?;
    if body_length != integrity.length() {
        return Err(LocalObjectStoreError::IntegrityLengthMismatch);
    }

    let actual = chunk_hash(bytes);
    if actual != integrity.hash() {
        return Err(LocalObjectStoreError::IntegrityHashMismatch);
    }

    Ok(())
}

pub fn chunk_hash(bytes: &[u8]) -> ShardlineHash {
    let digest = blake3::hash(bytes);
    ShardlineHash::from_bytes(*digest.as_bytes())
}

pub fn map_directory_path_error(error: DirectoryPathError) -> LocalObjectStoreError {
    match error {
        DirectoryPathError::UnsupportedPrefix
        | DirectoryPathError::SymlinkedComponent(_)
        | DirectoryPathError::NonDirectoryComponent(_) => LocalObjectStoreError::InvalidObjectPath,
        DirectoryPathError::Io(error) => LocalObjectStoreError::Io(error),
    }
}
