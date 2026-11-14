use std::cell::RefCell;

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
    integrity.verify_bytes(
        bytes,
        LocalObjectStoreError::IntegrityLengthMismatch,
        LocalObjectStoreError::IntegrityHashMismatch,
    )
}

#[cfg(test)]
pub(crate) use crate::object::chunk_hash;

pub fn map_directory_path_error(error: DirectoryPathError) -> LocalObjectStoreError {
    match error {
        DirectoryPathError::UnsupportedPrefix
        | DirectoryPathError::SymlinkedComponent(_)
        | DirectoryPathError::NonDirectoryComponent(_) => LocalObjectStoreError::InvalidObjectPath,
        DirectoryPathError::Io(error) => LocalObjectStoreError::Io(error),
    }
}
