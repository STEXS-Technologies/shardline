use shardline_index::FileChunkRecord;
use shardline_xet_adapter::{DecodedXorbChunk, ValidatedXorb};

use crate::{InvalidSerializedShardError, ServerError};

/// A retained local file handle can change after initial container validation.
/// Recheck selected payloads before exposing them, without another storage read.
pub(super) fn validate_member(chunk: &DecodedXorbChunk) -> Result<(), ServerError> {
    let actual: [u8; 32] = shardline_xet_core::merklehash::compute_data_hash(chunk.data()).into();
    if chunk.descriptor().hash().as_bytes() != &actual {
        return Err(ServerError::XorbHashMismatch);
    }
    Ok(())
}

/// Check the authenticated member lengths before either response path exposes data.
pub(super) fn validate_length(
    term: &FileChunkRecord,
    validated: &ValidatedXorb,
) -> Result<(), ServerError> {
    if term.range_end <= term.range_start {
        return Err(InvalidSerializedShardError::NativeXetTermEmptyOrInvertedChunkRange.into());
    }
    let start = usize::try_from(term.range_start)?;
    let end = usize::try_from(term.range_end)?;
    let members = validated
        .chunks()
        .get(start..end)
        .ok_or(InvalidSerializedShardError::NativeXetTermRangeExceededXorbChunkCount)?;
    let length = members.iter().try_fold(0_u64, |total, member| {
        let length = member
            .unpacked_end()
            .checked_sub(member.unpacked_start())
            .ok_or(ServerError::Overflow)?;
        total.checked_add(length).ok_or(ServerError::Overflow)
    })?;
    if length != term.length {
        return Err(ServerError::ObjectStore(
            crate::error::ObjectStoreError::StoredLengthMismatch,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_protocol::ShardlineHash;
    use shardline_xet_adapter::ValidatedXorbChunk;

    #[test]
    fn changed_payload_cannot_reuse_a_verified_member_descriptor() {
        let original = b"verified payload";
        let hash = shardline_xet_core::merklehash::compute_data_hash(original);
        let descriptor = ValidatedXorbChunk::new(
            ShardlineHash::from_bytes(hash.into()),
            0,
            24,
            0,
            original.len() as u64,
        );
        validate_member(&DecodedXorbChunk::new(
            descriptor.clone(),
            original.to_vec(),
        ))
        .unwrap();
        let mut changed = original.to_vec();
        changed[0] ^= 1;
        assert!(matches!(
            validate_member(&DecodedXorbChunk::new(descriptor, changed)),
            Err(ServerError::XorbHashMismatch)
        ));
    }

    #[test]
    fn generic_and_native_payload_identities_are_distinct() {
        let payload = b"shared bytes do not imply a shared hash domain";
        let native: [u8; 32] = shardline_xet_core::merklehash::compute_data_hash(payload).into();
        assert_ne!(
            crate::local_backend::chunk_hash(payload).as_bytes(),
            &native
        );
    }
}
