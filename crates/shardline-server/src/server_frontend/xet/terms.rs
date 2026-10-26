use shardline_index::FileChunkRecord;
use shardline_xet_adapter::ValidatedXorb;

use crate::{InvalidSerializedShardError, ServerError};

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
