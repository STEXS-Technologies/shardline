//! Footer commitments authenticate selected members, never whole-source health.
use super::*;
use shardline_xet_core::merklehash::xorb_hash;

/// An authenticated list of member hashes and lengths. Payloads remain unverified.
/// This deliberately cannot be passed to full-container trusted visitors.
///
/// ```compile_fail
/// use shardline_xet_adapter::{AuthenticatedXorbFooter, ValidatedXorb};
/// fn require_full_validation(footer: AuthenticatedXorbFooter) {
///     let _: ValidatedXorb = footer;
/// }
/// ```
#[derive(Debug)]
pub struct AuthenticatedXorbFooter {
    chunks: Vec<ValidatedXorbChunk>,
}

impl AuthenticatedXorbFooter {
    /// Member descriptors whose hash and unpacked length bind to the expected root.
    /// Packed offsets are structural hints; every fetched member must still be decoded.
    #[must_use]
    pub fn chunks(&self) -> &[ValidatedXorbChunk] {
        &self.chunks
    }
}

/// Authenticates a footer against an independently trusted Xorb root.
/// Legacy boundaries return `None` and require complete-container validation.
///
/// # Errors
/// Returns an error for malformed metadata, excessive decoded lengths or a root mismatch.
pub fn validate_serialized_xorb_footer(
    footer: &[u8],
    total_length: u64,
    expected_hash: ShardlineHash,
) -> Result<Option<AuthenticatedXorbFooter>, XorbParseError> {
    const MAX_FOOTER_BYTES: usize = 16 * 1024 * 1024;
    if footer.len() > MAX_FOOTER_BYTES {
        return Err(XorbInvalidFormatError::StructuralValidationFailed.into());
    }
    let parsed = XorbObject::deserialize(&mut Cursor::new(footer))
        .map_err(|error| map_core_error(&error))?;
    let footer_length = parsed
        .info_length
        .checked_add(8)
        .ok_or(XorbInvalidFormatError::MetadataSectionLengthMismatch)?;
    if footer_length != u64::try_from(footer.len())? {
        return Err(XorbInvalidFormatError::MetadataSectionLengthMismatch.into());
    }
    let info = parsed.info;
    if info.boundaries_version != 1 {
        return Ok(None);
    }
    let count = usize::try_from(info.num_chunks)?;
    if count == 0
        || info.chunk_hashes.len() != count
        || info.chunk_boundary_offsets.len() != count
        || info.unpacked_chunk_offsets.len() != count
    {
        return Err(XorbInvalidFormatError::StructuralValidationFailed.into());
    }
    let expected = shardline_hash_to_merkle_hash(expected_hash)?;
    if info.xorb_hash != expected {
        return Err(XorbParseError::HashMismatch);
    }
    let mut chunks = Vec::with_capacity(count);
    let mut leaves = Vec::with_capacity(count);
    let mut packed_start = 0;
    let mut unpacked_start = 0;
    let max_packed_chunk = MAX_CHUNK_SIZE
        .load(Ordering::Relaxed)
        .checked_mul(2)
        .and_then(|length| {
            length.checked_add(
                u64::try_from(std::mem::size_of::<
                    shardline_xet_core::xorb_object::XorbChunkHeader,
                >())
                .ok()?,
            )
        })
        .ok_or(XorbInvalidFormatError::PackedChunkLengthOverflow)?;
    for ((hash, packed_end), unpacked_end) in info
        .chunk_hashes
        .into_iter()
        .zip(info.chunk_boundary_offsets)
        .zip(info.unpacked_chunk_offsets)
    {
        if packed_end <= packed_start || unpacked_end <= unpacked_start {
            return Err(XorbInvalidFormatError::NonMonotonicChunkBoundaries.into());
        }
        let length = unpacked_end
            .checked_sub(unpacked_start)
            .ok_or(XorbInvalidFormatError::NonMonotonicChunkBoundaries)?;
        if packed_end
            .checked_sub(packed_start)
            .is_none_or(|packed_length| packed_length > max_packed_chunk)
        {
            return Err(XorbInvalidFormatError::ChunkPayloadMetadataMismatch.into());
        }
        if length > MAX_CHUNK_SIZE.load(Ordering::Relaxed) || unpacked_end > MAX_XORB_UNPACKED_BYTES
        {
            return Err(XorbInvalidFormatError::UnpackedLengthExceedsCap.into());
        }
        leaves.push((hash, length));
        chunks.push(ValidatedXorbChunk::new(
            merkle_hash_to_shardline_hash(hash),
            packed_start,
            packed_end,
            unpacked_start,
            unpacked_end,
        ));
        packed_start = packed_end;
        unpacked_start = unpacked_end;
    }
    if packed_start.checked_add(footer_length) != Some(total_length) {
        return Err(XorbInvalidFormatError::PackedContentLengthMismatch.into());
    }
    if xorb_hash(&leaves) != expected {
        return Err(XorbParseError::HashMismatch);
    }
    Ok(Some(AuthenticatedXorbFooter { chunks }))
}

/// Decodes and authenticates exactly one selected member before returning any bytes.
///
/// # Errors
/// Returns an error for malformed, truncated, extra, or hash-mismatched member bytes.
pub fn decode_authenticated_xorb_chunk(
    footer: &AuthenticatedXorbFooter,
    index: usize,
    packed_bytes: &[u8],
) -> Result<DecodedXorbChunk, XorbParseError> {
    let descriptor = footer
        .chunks
        .get(index)
        .ok_or(XorbInvalidFormatError::MetadataSectionLengthMismatch)?;
    let expected_length = descriptor
        .packed_end
        .checked_sub(descriptor.packed_start)
        .ok_or(XorbInvalidFormatError::NonMonotonicChunkBoundaries)?;
    if u64::try_from(packed_bytes.len())? != expected_length {
        return Err(XorbInvalidFormatError::ChunkPayloadMetadataMismatch.into());
    }
    let (data, packed, unpacked) = deserialize_chunk(&mut Cursor::new(packed_bytes))
        .map_err(|error| map_core_error(&error))?;
    if u64::try_from(packed)? != expected_length
        || u64::from(unpacked) != descriptor.unpacked_len()
        || u64::try_from(data.len())? != descriptor.unpacked_len()
        || merkle_hash_to_shardline_hash(compute_data_hash(&data)) != descriptor.hash()
    {
        return Err(XorbInvalidFormatError::ChunkPayloadHashMismatch.into());
    }
    Ok(DecodedXorbChunk::new(descriptor.clone(), data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_xet_core::xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    };

    #[test]
    fn footer_members_require_payload_authentication_and_cannot_hide_selected_corruption() {
        let first = b"selected member";
        let second = b"unselected member";
        let leaves = [
            (compute_data_hash(first), first.len() as u64),
            (compute_data_hash(second), second.len() as u64),
        ];
        let root = xorb_hash(&leaves);
        let encoded = serialized_xorb_object_from_components(
            &root,
            [first.as_slice(), second.as_slice()].concat(),
            vec![
                (leaves[0].0, first.len() as u64),
                (leaves[1].0, (first.len() + second.len()) as u64),
            ],
            CompressionScheme::LZ4,
        )
        .unwrap();
        let start = usize::try_from(encoded.footer_start.unwrap()).unwrap();
        let raw_footer = &encoded.serialized_data[start..];
        let footer = validate_serialized_xorb_footer(
            raw_footer,
            encoded.serialized_data.len() as u64,
            merkle_hash_to_shardline_hash(root),
        )
        .unwrap()
        .unwrap();
        let descriptor = &footer.chunks()[0];
        let packed = &encoded.serialized_data
            [descriptor.packed_start() as usize..descriptor.packed_end() as usize];
        assert_eq!(
            decode_authenticated_xorb_chunk(&footer, 0, packed)
                .unwrap()
                .data(),
            first
        );
        let mut corrupt = packed.to_vec();
        corrupt[8] ^= 1;
        assert!(decode_authenticated_xorb_chunk(&footer, 0, &corrupt).is_err());
        assert!(decode_authenticated_xorb_chunk(&footer, 1, packed).is_err());
        assert!(decode_authenticated_xorb_chunk(&footer, 0, &packed[..packed.len() - 1]).is_err());
        assert!(
            validate_serialized_xorb_footer(
                raw_footer,
                encoded.serialized_data.len() as u64,
                merkle_hash_to_shardline_hash(compute_data_hash(b"wrong root"))
            )
            .is_err()
        );
        let parsed = XorbObject::deserialize(&mut Cursor::new(raw_footer)).unwrap();
        for variant in 0..5 {
            let mut info = parsed.info.clone();
            match variant {
                0 => info.unpacked_chunk_offsets[1] += 1,
                1 => info.chunk_hashes[1] = compute_data_hash(b"forged unselected leaf"),
                2 => info.chunk_boundary_offsets[0] = 0,
                3 => info.unpacked_chunk_offsets[1] = MAX_XORB_UNPACKED_BYTES + 1,
                _ => info.chunk_boundary_offsets[1] = MAX_CHUNK_SIZE.load(Ordering::Relaxed) * 3,
            }
            let mut forged = Vec::new();
            let length = info.serialize(&mut forged).unwrap();
            forged.extend_from_slice(&(length as u64).to_le_bytes());
            let total = if variant == 4 {
                // The root does not authenticate packed offsets. Even with a
                // self-consistent object length, impossible member spans must
                // not become instructions for an oversized backend read.
                info.chunk_boundary_offsets[1] + forged.len() as u64
            } else {
                encoded.serialized_data.len() as u64
            };
            assert!(
                validate_serialized_xorb_footer(
                    &forged,
                    total,
                    merkle_hash_to_shardline_hash(root)
                )
                .is_err()
            );
        }
    }
}
