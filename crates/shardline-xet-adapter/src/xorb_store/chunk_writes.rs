//! Bounded immutable member writes; all source bytes were validated beforehand.
use super::*;
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::io::{Read, Seek, SeekFrom};

const MAX_PENDING: usize = 4;
const MAX_PENDING_BYTES: u64 = 256 * 1024;

#[cfg(test)]
mod tests;

pub(super) async fn store<R: Read + Seek>(
    objects: &ServerObjectStore,
    reader: &mut R,
    validated: &ValidatedXorb,
) -> Result<u64, XetAdapterError> {
    reader.seek(SeekFrom::Start(0))?;
    let mut pending = FuturesUnordered::new();
    let mut pending_bytes = 0_u64;
    let mut unpacked = 0_u64;
    let mut packed_end = 0_u64;
    let mut stored = 0_u64;
    // Local persistence uses blocking jobs: preserve its existing serial work
    // lifetime instead of multiplying jobs that can outlive async cancellation.
    let max_pending = if matches!(objects, ServerObjectStore::S3(_)) {
        MAX_PENDING
    } else {
        1
    };
    for descriptor in validated.chunks() {
        let length = descriptor.unpacked_len();
        // Oversized members run alone. Small members share a bounded byte budget,
        // rather than allocating four protocol-maximum decompression buffers.
        while !pending.is_empty()
            && (pending.len() >= max_pending
                || pending_bytes
                    .checked_add(length)
                    .is_none_or(|n| n > MAX_PENDING_BYTES))
        {
            let (completed, inserted) = pending
                .next()
                .await
                .ok_or(XetAdapterError::InvalidSerializedXorb)??;
            pending_bytes = pending_bytes
                .checked_sub(completed)
                .ok_or(XetAdapterError::Overflow)?;
            stored = stored
                .checked_add(inserted)
                .ok_or(XetAdapterError::Overflow)?;
        }
        let (data, packed, decoded) = shardline_xet_core::xorb_object::deserialize_chunk(reader)
            .map_err(|_error| XetAdapterError::InvalidSerializedXorb)?;
        let next_end = packed_end
            .checked_add(u64::try_from(packed)?)
            .ok_or(XetAdapterError::Overflow)?;
        if descriptor.packed_start() != packed_end
            || descriptor.packed_end() != next_end
            || length != u64::from(decoded)
            || length != u64::try_from(data.len())?
        {
            return Err(XetAdapterError::InvalidSerializedXorb);
        }
        unpacked = unpacked
            .checked_add(length)
            .ok_or(XetAdapterError::Overflow)?;
        if unpacked > MAX_XORB_UNPACKED_BYTES {
            return Err(XetAdapterError::XorbUnpackedLengthExceedsCap);
        }
        packed_end = next_end;
        let key = chunk_object_key_local(&xet_hash_hex_string(descriptor.hash()))?;
        pending_bytes = pending_bytes
            .checked_add(length)
            .ok_or(XetAdapterError::Overflow)?;
        pending.push(async move {
            let integrity = ObjectIntegrity::new(chunk_hash(&data), length);
            let result = AsyncObjectStore::put_if_absent(
                objects,
                &key,
                ObjectBody::from_slice(&data),
                &integrity,
            )
            .await?;
            Ok::<_, XetAdapterError>((
                length,
                if matches!(result, PutOutcome::Inserted) {
                    length
                } else {
                    0
                },
            ))
        });
    }
    if unpacked != validated.unpacked_length() || packed_end != validated.packed_content_length() {
        return Err(XetAdapterError::InvalidSerializedXorb);
    }
    while let Some(result) = pending.next().await {
        let (_, inserted) = result?;
        stored = stored
            .checked_add(inserted)
            .ok_or(XetAdapterError::Overflow)?;
    }
    Ok(stored)
}
