//! Full-payload compatibility for footers without decoded member offsets.
use super::*;

/// Derive bounded framing metadata, then require the ordinary full validator to
/// verify every payload, decoded length and root before descriptors are trusted.
pub(super) fn populate_decoded_offsets<R: Read + Seek>(
    reader: &mut R,
    parsed: &mut XorbObject,
) -> Result<(), XorbParseError> {
    if parsed.info.boundaries_version == 1 {
        return Ok(());
    }
    let count = usize::try_from(parsed.info.num_chunks)?;
    if count == 0 || parsed.info.chunk_boundary_offsets.len() != count {
        return Err(XorbInvalidFormatError::MetadataSectionLengthMismatch.into());
    }
    let content_end = reader
        .seek(SeekFrom::End(0))?
        .checked_sub(parsed.info_length)
        .and_then(|length| length.checked_sub(8))
        .ok_or(XorbInvalidFormatError::PackedContentLengthMismatch)?;
    let mut offsets = Vec::with_capacity(count);
    let mut packed_start = 0;
    let mut unpacked = 0_u64;
    for packed_end in &parsed.info.chunk_boundary_offsets {
        reader.seek(SeekFrom::Start(packed_start))?;
        let header = deserialize_chunk_header(reader).map_err(|error| map_core_error(&error))?;
        if reader
            .stream_position()?
            .checked_add(u64::from(header.get_compressed_length()))
            != Some(*packed_end)
            || *packed_end > content_end
        {
            return Err(XorbInvalidFormatError::ChunkPayloadMetadataMismatch.into());
        }
        let length = u64::from(header.get_uncompressed_length());
        unpacked = unpacked
            .checked_add(length)
            .ok_or(XorbInvalidFormatError::UnpackedLengthExceedsCap)?;
        if length == 0
            || length > MAX_CHUNK_SIZE.load(Ordering::Relaxed)
            || unpacked > MAX_XORB_UNPACKED_BYTES
        {
            return Err(XorbInvalidFormatError::UnpackedLengthExceedsCap.into());
        }
        offsets.push(unpacked);
        packed_start = *packed_end;
    }
    if packed_start != content_end {
        return Err(XorbInvalidFormatError::PackedContentLengthMismatch.into());
    }
    // In-memory normalization only: the original stored footer is unchanged.
    // Version 1 makes the full validator compare these offsets with decoding.
    parsed.info.unpacked_chunk_offsets = offsets;
    parsed.info.boundaries_version = 1;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use shardline_xet_core::{
        merklehash::xorb_hash,
        xorb_object::{
            CompressionScheme, XorbObjectInfoV0,
            xorb_format_test_utils::serialized_xorb_object_from_components,
        },
    };

    #[test]
    #[allow(deprecated)] // Deliberately generate the old on-disk format.
    fn v0_footer_requires_full_payload_validation_and_derives_exact_lengths() {
        for count in [2, 513] {
            let chunk = vec![0x6b; 4096];
            let member = compute_data_hash(&chunk);
            let root = xorb_hash(&vec![(member, 4096); count]);
            let canonical = serialized_xorb_object_from_components(
                &root,
                chunk.repeat(count),
                (1..=count).map(|i| (member, (i * 4096) as u64)).collect(),
                CompressionScheme::None,
            )
            .unwrap();
            let parsed =
                XorbObject::deserialize(&mut Cursor::new(&canonical.serialized_data)).unwrap();
            let mut legacy = XorbObjectInfoV0::default();
            legacy.xorb_hash = root;
            legacy.num_chunks = count as u32;
            legacy.chunk_hashes = vec![member; count];
            legacy.chunk_boundary_offsets = parsed
                .info
                .chunk_boundary_offsets
                .iter()
                .map(|offset| u32::try_from(*offset).unwrap())
                .collect();
            let mut footer = Vec::new();
            legacy.serialize(&mut footer).unwrap();
            let length = footer.len() as u64;
            footer.extend_from_slice(&length.to_le_bytes());
            let mut bytes =
                canonical.serialized_data[..canonical.footer_start.unwrap() as usize].to_vec();
            bytes.extend_from_slice(&footer);
            let expected = merkle_hash_to_shardline_hash(root);
            assert!(
                validate_serialized_xorb_footer(&footer, bytes.len() as u64, expected)
                    .unwrap()
                    .is_none()
            );
            let validated = validate_serialized_xorb(&mut Cursor::new(&bytes), expected).unwrap();
            assert_eq!(validated.chunks().len(), count);
            assert_eq!(
                validated.chunks().last().unwrap().unpacked_end(),
                (count * 4096) as u64
            );
            if count == 513 {
                // Reject the aggregate declared budget before decompressing any
                // member, even though the old footer supplies no decoded lengths.
                let mut bomb = bytes.clone();
                let mut start = 0;
                for end in &legacy.chunk_boundary_offsets {
                    let mut header =
                        deserialize_chunk_header(&mut Cursor::new(&bomb[start..])).unwrap();
                    header.set_uncompressed_length(0x00ff_ffff);
                    shardline_xet_core::xorb_object::write_chunk_header(
                        &mut &mut bomb[start..start + 8],
                        &header,
                    )
                    .unwrap();
                    start = *end as usize;
                }
                let mut parsed = XorbObject::deserialize(&mut Cursor::new(&bomb)).unwrap();
                assert!(matches!(
                    super::populate_decoded_offsets(&mut Cursor::new(&bomb), &mut parsed),
                    Err(XorbParseError::InvalidFormat(
                        XorbInvalidFormatError::UnpackedLengthExceedsCap
                    ))
                ));
            }
            bytes[12] ^= 0x40;
            assert!(validate_serialized_xorb(&mut Cursor::new(&bytes), expected).is_err());
        }
    }
}
