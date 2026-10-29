//! Bind client-provided CAS membership to fully verified container contents.

use shardline_server_core::InvalidSerializedShardError;
use shardline_xet_core::{
    merklehash::MerkleHash,
    metadata_shard::{file_structs::FileDataSequenceEntry, xorb_structs::MDBXorbInfo},
};

use crate::{ValidatedXorbChunk, error::XetAdapterError};

/// Repeated reconstruction terms can describe many more members than the shard
/// contains. Stop aggregation when its request is cancelled without accepting
/// the identity of the partially consumed sequence.
pub(super) fn verify_file_hash(
    expected: MerkleHash,
    members: impl Iterator<Item = (MerkleHash, u64)>,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<(), XetAdapterError> {
    let mut cancellation = None;
    let members = members.take_while(|_| match checkpoint() {
        Ok(()) => true,
        Err(error) => {
            cancellation = Some(error);
            false
        }
    });
    let actual = shardline_xet_core::merklehash::file_hash_from_iter(members);
    if let Some(error) = cancellation {
        return Err(error.into());
    }
    checkpoint()?;
    if actual != expected {
        return Err(InvalidSerializedShardError::ParserRejectedMetadata.into());
    }
    Ok(())
}

pub(super) fn verify_term_length(
    supplied: &FileDataSequenceEntry,
    verified: &[ValidatedXorbChunk],
) -> Result<(), XetAdapterError> {
    let start = usize::try_from(supplied.chunk_index_start)?;
    let end = usize::try_from(supplied.chunk_index_end)?;
    let members = verified
        .get(start..end)
        .filter(|members| !members.is_empty())
        .ok_or(InvalidSerializedShardError::NativeXetTermRangeExceededXorbChunkCount)?;
    let length = members.iter().try_fold(0_u64, |total, member| {
        total
            .checked_add(member.unpacked_len())
            .ok_or(XetAdapterError::Overflow)
    })?;
    if length != supplied.unpacked_segment_bytes {
        return Err(InvalidSerializedShardError::ParserRejectedMetadata.into());
    }
    Ok(())
}

pub(super) fn verify_xorb_members(
    supplied: &MDBXorbInfo,
    verified: &[ValidatedXorbChunk],
) -> Result<(), XetAdapterError> {
    if supplied.chunks.len() != verified.len()
        || supplied.metadata.num_entries != u64::try_from(verified.len())?
        || supplied.metadata.num_bytes_in_xorb
            != verified.last().map_or(0, ValidatedXorbChunk::unpacked_end)
    {
        return Err(InvalidSerializedShardError::ParserRejectedMetadata.into());
    }
    for (claimed, actual) in supplied.chunks.iter().zip(verified) {
        if claimed.chunk_hash != MerkleHash::from(*actual.hash().as_bytes())
            || claimed.unpacked_segment_bytes != actual.unpacked_len()
            || claimed.chunk_byte_range_start != actual.unpacked_start()
        {
            return Err(InvalidSerializedShardError::ParserRejectedMetadata.into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_index::parse_xet_hash_hex;
    use shardline_xet_core::{
        merklehash::compute_data_hash,
        metadata_shard::xorb_structs::{XorbChunkSequenceEntry, XorbChunkSequenceHeader},
    };

    #[test]
    fn file_identity_preserves_wire_hash_and_rejects_cancelled_prefix() {
        use shardline_xet_core::merklehash::file_hash;
        use std::cell::Cell;

        let members: Vec<_> = (0..1024_u64)
            .map(|index| (compute_data_hash(&index.to_le_bytes()), index + 1))
            .collect();
        let expected = file_hash(&members);
        verify_file_hash(expected, members.iter().copied(), &|| Ok(())).unwrap();
        assert!(
            verify_file_hash(MerkleHash::default(), members.iter().copied(), &|| Ok(())).is_err()
        );
        let checks = Cell::new(0);
        let error = verify_file_hash(expected, members.iter().copied(), &|| {
            let seen = checks.get();
            checks.set(seen + 1);
            if seen == 3 {
                Err(std::io::ErrorKind::Interrupted.into())
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert!(matches!(error, XetAdapterError::Io(ref error)
            if error.kind() == std::io::ErrorKind::Interrupted));
        assert_eq!(checks.get(), 4);
        verify_file_hash(file_hash(&[]), std::iter::empty(), &|| Ok(())).unwrap();
        assert!(
            verify_file_hash(file_hash(&[]), std::iter::empty(), &|| {
                Err(std::io::ErrorKind::Interrupted.into())
            })
            .is_err()
        );
    }

    #[test]
    fn term_length_is_checked_against_only_the_selected_verified_members() {
        let verified = [
            ValidatedXorbChunk::new(
                shardline_protocol::ShardlineHash::from_bytes([0; 32]),
                0,
                14,
                0,
                6,
            ),
            ValidatedXorbChunk::new(
                shardline_protocol::ShardlineHash::from_bytes([1; 32]),
                14,
                29,
                6,
                13,
            ),
        ];
        let hash = compute_data_hash(b"xorb");
        assert!(verify_term_length(&FileDataSequenceEntry::new(hash, 7, 1, 2), &verified).is_ok());
        assert!(verify_term_length(&FileDataSequenceEntry::new(hash, 13, 0, 2), &verified).is_ok());
        for (length, start, end) in [(13, 1, 2), (7, 0, 2), (0, 1, 1), (7, 1, 3)] {
            assert!(
                verify_term_length(
                    &FileDataSequenceEntry::new(hash, length, start, end),
                    &verified
                )
                .is_err()
            );
        }
    }

    #[test]
    fn rejects_forged_membership_lengths_offsets_and_counts() {
        let hash = compute_data_hash(b"member");
        let verified = [ValidatedXorbChunk::new(
            parse_xet_hash_hex(&hash.hex()).expect("valid fixture hash"),
            0,
            14,
            0,
            6,
        )];
        let valid = MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(compute_data_hash(b"xorb"), 1, 6),
            chunks: vec![XorbChunkSequenceEntry::new(hash, 6, 0)],
        };
        verify_xorb_members(&valid, &verified).expect("matching fixture");
        for mutation in 0..6 {
            let mut forged = valid.clone();
            let member = forged.chunks.first_mut().expect("fixture member");
            match mutation {
                0 => member.chunk_hash = compute_data_hash(b"different member"),
                1 => member.unpacked_segment_bytes = 7,
                2 => member.chunk_byte_range_start = 1,
                3 => forged.metadata.num_entries = 2,
                4 => forged.metadata.num_bytes_in_xorb = 7,
                _ => forged.chunks.clear(),
            }
            assert!(matches!(
                verify_xorb_members(&forged, &verified),
                Err(XetAdapterError::InvalidSerializedShard(
                    InvalidSerializedShardError::ParserRejectedMetadata
                ))
            ));
        }
    }
}
