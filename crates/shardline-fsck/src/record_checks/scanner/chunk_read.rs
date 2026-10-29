//! Bounded chunk snapshots shared by the generic and native scanners.
use shardline_server_core::{BoundedObjectReadError, ServerObjectStore, ServerObjectStoreError};
use shardline_storage::{ObjectKey, ObjectStore};

use crate::FsckError;

pub(super) const MAX_CHUNK_BYTES: u64 = 2 * 1024 * 1024;

pub(super) enum ChunkRead {
    Missing,
    Oversized(u64),
    Body(Vec<u8>, u64),
}

pub(super) fn read_chunk(
    store: &ServerObjectStore,
    key: &ObjectKey,
) -> Result<ChunkRead, FsckError> {
    match store.read_object_if_present(key, MAX_CHUNK_BYTES) {
        Ok(Some((bytes, length))) => Ok(ChunkRead::Body(bytes, length)),
        Ok(None) => Ok(ChunkRead::Missing),
        Err(BoundedObjectReadError::Store(error)) => Err(error.into()),
        Err(BoundedObjectReadError::ObjectTooLarge) => {
            // Read metadata only for an exact oversized-object diagnostic.
            // A source changing between observations must not appear clean.
            let metadata = store
                .metadata(key)?
                .ok_or(ServerObjectStoreError::StoredObjectLengthMismatch)?;
            if metadata.length() <= MAX_CHUNK_BYTES {
                return Err(ServerObjectStoreError::StoredObjectLengthMismatch.into());
            }
            Ok(ChunkRead::Oversized(metadata.length()))
        }
    }
}
