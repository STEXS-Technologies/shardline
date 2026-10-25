//! Reuse verified native content while retaining the installed encoding's offsets.
use shardline_storage::{LocalObjectStoreError, S3ObjectStoreError};

use super::*;
use crate::{error::ObjectStoreError, upload_ingest::xorb_packer::PackedXorb};

pub(super) const fn is_encoding_conflict(error: &ServerError) -> bool {
    matches!(
        error,
        ServerError::ObjectStore(
            ObjectStoreError::Local(
                LocalObjectStoreError::IntegrityLengthMismatch
                    | LocalObjectStoreError::IntegrityHashMismatch
                    | LocalObjectStoreError::ExistingObjectConflict
            ) | ObjectStoreError::S3(S3ObjectStoreError::ExistingObjectConflict)
        )
    )
}

pub(super) fn reuse_retained(
    store: &ServerObjectStore,
    packed: &mut PackedXorb,
) -> Result<Option<u64>, ServerError> {
    let key = crate::xet_adapter::xorb_object_key(&packed.xorb_hash_hex)?;
    let Some((mut source, _)) = store.materialize_bounded_object_if_present(&key, 0)? else {
        return Ok(None);
    };
    let expected = shardline_index::parse_xet_hash_hex(&packed.xorb_hash_hex)?;
    // Full member validation recomputes the native root from decoded content.
    // A footer, object ETag or client descriptor alone cannot establish reuse.
    let retained = shardline_xet_adapter::validate_serialized_xorb(&mut source, expected)?;
    if retained.chunks().len() != packed.chunk_entries.len()
        || retained
            .chunks()
            .iter()
            .zip(&packed.chunk_entries)
            .any(|(chunk, entry)| chunk.unpacked_len() != entry.raw_length)
    {
        return Ok(None);
    }
    let mut entries = packed.chunk_entries.clone();
    for (entry, chunk) in entries.iter_mut().zip(retained.chunks()) {
        entry.packed_offset = u32::try_from(chunk.packed_start())?;
        entry.packed_length = u32::try_from(
            chunk
                .packed_end()
                .checked_sub(chunk.packed_start())
                .ok_or(ServerError::Overflow)?,
        )?;
    }
    packed.chunk_entries = entries;
    Ok(Some(retained.total_length()))
}
