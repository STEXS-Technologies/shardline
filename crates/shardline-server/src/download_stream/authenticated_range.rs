//! Authenticate an xorb footer commitment and fetch only the chunks needed by
//! a file range. Full downloads continue through complete-container validation.

use std::collections::BTreeMap;

use axum::body::Bytes;
use shardline_index::{FileRecord, parse_xet_hash_hex};
use shardline_protocol::ByteRange;
use shardline_storage::{AsyncObjectStore, ObjectKey};

use crate::{ServerError, object_store::ServerObjectStore};

const MAX_XORB_FOOTER_BYTES: u64 = 16 * 1024 * 1024;

pub(super) enum RangeRead {
    Served(Bytes),
    MissingObject,
    Fallback,
}

/// Returns verified bytes for an S3-backed file range when its xorb has a
/// footer format suitable for authenticated subrange reads. Legacy footers
/// request the existing full-container path; a missing xorb is distinguished
/// so callers can avoid repeating the failed object probe.
pub(super) async fn read(
    object_store: &ServerObjectStore,
    record: &FileRecord,
    range: ByteRange,
) -> Result<RangeRead, ServerError> {
    let Some(first_term) = record.chunks.first() else {
        return Err(ServerError::Overflow);
    };
    if !record
        .chunks
        .iter()
        .all(|term| term.hash == first_term.hash)
        || record.chunks.iter().any(|term| term.packed_end == 0)
    {
        return Ok(RangeRead::Fallback);
    }

    let expected_hash = parse_xet_hash_hex(&first_term.hash)?;
    let key = crate::xet_adapter::xorb_object_key(&first_term.hash)?;
    let Some((trailer, total_length)) =
        AsyncObjectStore::read_suffix(object_store, &key, 8).await?
    else {
        return Ok(RangeRead::MissingObject);
    };
    let Ok(info_length_bytes) = <[u8; 8]>::try_from(trailer.as_slice()) else {
        return Ok(RangeRead::Fallback);
    };
    let info_length = u64::from_le_bytes(info_length_bytes);
    let footer_length = info_length.checked_add(8).ok_or(ServerError::Overflow)?;
    if info_length == 0 || footer_length > MAX_XORB_FOOTER_BYTES || footer_length > total_length {
        return Ok(RangeRead::Fallback);
    }
    let footer_start = total_length
        .checked_sub(footer_length)
        .ok_or(ServerError::Overflow)?;
    let footer = read_exact_range(object_store, &key, footer_start, total_length - 1).await?;
    let Some(validated) = shardline_xet_adapter::validate_serialized_xorb_footer(
        &footer,
        total_length,
        expected_hash,
    )?
    else {
        return Ok(RangeRead::Fallback);
    };

    let requested_start = range.start();
    let requested_end_exclusive = range
        .end_inclusive()
        .checked_add(1)
        .ok_or(ServerError::Overflow)?;
    if requested_end_exclusive > record.total_bytes {
        return Err(ServerError::RangeNotSatisfiable);
    }

    // The index record is itself part of Shardline's durable metadata. Check
    // each term against the authenticated footer before using its offsets.
    let mut logical_chunks = BTreeMap::<usize, (u64, u64)>::new();
    for term in &record.chunks {
        let first = usize::try_from(term.range_start)?;
        let limit = usize::try_from(term.range_end)?;
        let selected = validated
            .chunks()
            .get(first..limit)
            .ok_or_else(stored_length_mismatch)?;
        let first_chunk = selected.first().ok_or_else(stored_length_mismatch)?;
        let last_chunk = selected.last().ok_or_else(stored_length_mismatch)?;
        let selected_length = last_chunk
            .unpacked_end()
            .checked_sub(first_chunk.unpacked_start())
            .ok_or_else(stored_length_mismatch)?;
        if term.length != selected_length
            || term.packed_start != first_chunk.packed_start()
            || term.packed_end != last_chunk.packed_end()
        {
            return Err(stored_length_mismatch());
        }
        for (relative_index, descriptor) in selected.iter().enumerate() {
            let index = first
                .checked_add(relative_index)
                .ok_or(ServerError::Overflow)?;
            let logical_start = term
                .offset
                .checked_add(
                    descriptor
                        .unpacked_start()
                        .checked_sub(first_chunk.unpacked_start())
                        .ok_or_else(stored_length_mismatch)?,
                )
                .ok_or(ServerError::Overflow)?;
            let logical_end = logical_start
                .checked_add(descriptor.unpacked_len())
                .ok_or(ServerError::Overflow)?;
            if logical_chunks
                .insert(index, (logical_start, logical_end))
                .is_some()
            {
                return Err(stored_length_mismatch());
            }
        }
    }

    let selected_indices: Vec<_> = logical_chunks
        .iter()
        .filter_map(|(index, (start, end))| {
            (*start < requested_end_exclusive && *end > requested_start).then_some(*index)
        })
        .collect();
    let Some(first_index) = selected_indices.first().copied() else {
        return Err(stored_length_mismatch());
    };
    let last_index = selected_indices
        .last()
        .copied()
        .ok_or(ServerError::Overflow)?;
    let first_descriptor = validated
        .chunks()
        .get(first_index)
        .ok_or_else(stored_length_mismatch)?;
    let last_descriptor = validated
        .chunks()
        .get(last_index)
        .ok_or_else(stored_length_mismatch)?;
    let packed_start = first_descriptor.packed_start();
    let packed_end = last_descriptor.packed_end();
    let packed_bytes = read_exact_range(
        object_store,
        &key,
        packed_start,
        packed_end.checked_sub(1).ok_or(ServerError::Overflow)?,
    )
    .await?;

    let output_length = usize::try_from(
        requested_end_exclusive
            .checked_sub(requested_start)
            .ok_or(ServerError::Overflow)?,
    )?;
    let mut output = Vec::with_capacity(output_length);
    let mut packed_cursor = 0_usize;
    for index in first_index..=last_index {
        let descriptor = validated
            .chunks()
            .get(index)
            .ok_or_else(stored_length_mismatch)?;
        let packed_length = usize::try_from(
            descriptor
                .packed_end()
                .checked_sub(descriptor.packed_start())
                .ok_or(ServerError::Overflow)?,
        )?;
        let packed_limit = packed_cursor
            .checked_add(packed_length)
            .ok_or(ServerError::Overflow)?;
        let bytes = packed_bytes
            .get(packed_cursor..packed_limit)
            .ok_or_else(stored_length_mismatch)?;
        let decoded =
            shardline_xet_adapter::decode_authenticated_xorb_chunk(&validated, index, bytes)?;
        if let Some((logical_start, logical_end)) = logical_chunks.get(&index).copied() {
            let copy_start = requested_start.max(logical_start);
            let copy_end = requested_end_exclusive.min(logical_end);
            if copy_start < copy_end {
                let start = usize::try_from(copy_start - logical_start)?;
                let end = usize::try_from(copy_end - logical_start)?;
                output.extend_from_slice(
                    decoded
                        .data()
                        .get(start..end)
                        .ok_or_else(stored_length_mismatch)?,
                );
            }
        }
        packed_cursor = packed_limit;
    }
    if output.len() != output_length || packed_cursor != packed_bytes.len() {
        return Err(stored_length_mismatch());
    }
    Ok(RangeRead::Served(Bytes::from(output)))
}

async fn read_exact_range(
    object_store: &ServerObjectStore,
    key: &ObjectKey,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, ServerError> {
    let range = ByteRange::new(start, end).map_err(|_error| ServerError::Overflow)?;
    let bytes = AsyncObjectStore::read_range(object_store, key, range).await?;
    let expected_length = end
        .checked_sub(start)
        .and_then(|length| length.checked_add(1))
        .ok_or(ServerError::Overflow)?;
    if u64::try_from(bytes.len())? != expected_length {
        return Err(stored_length_mismatch());
    }
    Ok(bytes)
}

fn stored_length_mismatch() -> ServerError {
    ServerError::ObjectStore(crate::error::ObjectStoreError::StoredLengthMismatch)
}
