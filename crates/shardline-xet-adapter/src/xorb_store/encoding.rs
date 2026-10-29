//! Preserve the first immutable encoding when verified Xorb content is identical.

use super::*;
use shardline_server_core::ServerObjectStoreError;
use shardline_storage::{LocalObjectStoreError, S3ObjectStoreError};

pub(super) fn resolve_conflict(
    store: &ServerObjectStore,
    key: &ObjectKey,
    uploaded: &ValidatedXorb,
    result: Result<PutOutcome, ServerObjectStoreError>,
) -> Result<PutOutcome, XetAdapterError> {
    let error = match result {
        Ok(outcome) => return Ok(outcome),
        Err(error) => error,
    };
    let conflict = matches!(
        &error,
        ServerObjectStoreError::Local(
            LocalObjectStoreError::IntegrityLengthMismatch
                | LocalObjectStoreError::IntegrityHashMismatch
                | LocalObjectStoreError::ExistingObjectConflict
        ) | ServerObjectStoreError::S3(S3ObjectStoreError::ExistingObjectConflict)
    );
    if !conflict {
        return Err(error.into());
    }
    let Some((mut source, _)) = store.materialize_bounded_object_if_present(key, 0)? else {
        return Err(error.into());
    };
    // Verify every stored member, not just the footer or object metadata. Packed
    // offsets may differ, but content hashes, lengths and order must agree.
    let existing = match validate_serialized_xorb(&mut source, uploaded.hash()) {
        Ok(existing) => existing,
        // The incoming container already verified. Corrupt retained bytes are
        // a storage failure, not a malformed request from this client.
        Err(_) => return Err(error.into()),
    };
    if existing.unpacked_length() != uploaded.unpacked_length()
        || existing.chunks().len() != uploaded.chunks().len()
        || !existing
            .chunks()
            .iter()
            .zip(uploaded.chunks())
            .all(|(stored, incoming)| {
                stored.hash() == incoming.hash() && stored.unpacked_len() == incoming.unpacked_len()
            })
    {
        return Err(error.into());
    }
    Ok(PutOutcome::AlreadyExists)
}
