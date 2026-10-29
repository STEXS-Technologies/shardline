//! Rebuild a legacy lookup descriptor from verified immutable source content.

use super::*;

#[cfg(test)]
mod tests;

/// Rebuilds a native descriptor by authenticating its complete immutable Xorb.
/// The result is derived metadata, not proof of repository authorization.
///
/// # Errors
/// Returns an error for missing, corrupt or oversized source content.
pub fn recover_xorb_descriptor(
    store: &ServerObjectStore,
    hash: &str,
) -> Result<shardline_index::XetXorbDescriptor, XetAdapterError> {
    let info = load_xorb_range_info(store, hash, None)?;
    let descriptor = info.descriptor(hash);
    if !descriptor.has_valid_structure() {
        return Err(XetAdapterError::InvalidSerializedXorb);
    }
    Ok(descriptor)
}

/// Recovers a bounded descriptor for an old chunk-to-shard mapping.
///
/// Retained shard metadata is only a discovery hint. The selected Xorb is fully
/// validated and its advertised members are compared before returning metadata.
/// No file, mapping or object is published or rewritten by this operation.
///
/// # Errors
/// Missing sources are skipped because retained mappings are discovery hints.
/// Returns an error for malformed metadata, corrupt source content or parser
/// limits exceeded.
pub fn recover_dedup_xorb_descriptor(
    store: &ServerObjectStore,
    shard: &[u8],
    chunk_hash: &str,
    limits: ShardMetadataLimits,
) -> Result<Option<shardline_index::XetXorbDescriptor>, XetAdapterError> {
    let candidates = dedup_xorb_candidates(shard, chunk_hash, limits)?;
    recover_dedup_xorb_descriptor_from_candidates(store, &candidates, chunk_hash)
}

/// Parses bounded discovery hints without reading any referenced Xorb.
/// Callers may filter these claims against authoritative repository terms before
/// validating source content. Claims alone never establish member integrity.
///
/// # Errors
/// Returns an error for malformed metadata, hashes or exceeded parser limits.
pub fn dedup_xorb_candidates(
    shard: &[u8],
    chunk_hash: &str,
    limits: ShardMetadataLimits,
) -> Result<Vec<MDBXorbInfo>, XetAdapterError> {
    let expected = MerkleHash::from(*parse_xet_hash_hex(chunk_hash)?.as_bytes());
    let mut reader = Cursor::new(shard);
    let header = MDBShardFileHeader::deserialize(&mut reader)
        .map_err(|error| invalid_serialized_shard(&error))?;
    let sections = read_bounded_shard_sections(&mut reader, limits, header.version)?;
    Ok(sections
        .xorb_infos
        .into_iter()
        .filter(|claimed| {
            claimed
                .chunks
                .iter()
                .any(|chunk| chunk.chunk_hash == expected)
        })
        .collect())
}

/// Fully validates a source selected from previously bounded discovery hints.
/// Missing sources are skipped; source/member validation remains mandatory.
///
/// # Errors
/// Returns an error for corrupt sources, inconsistent claims or invalid hashes.
pub fn recover_dedup_xorb_descriptor_from_candidates(
    store: &ServerObjectStore,
    candidates: &[MDBXorbInfo],
    chunk_hash: &str,
) -> Result<Option<shardline_index::XetXorbDescriptor>, XetAdapterError> {
    let expected = MerkleHash::from(*parse_xet_hash_hex(chunk_hash)?.as_bytes());
    for claimed in candidates {
        if !claimed
            .chunks
            .iter()
            .any(|chunk| chunk.chunk_hash == expected)
        {
            continue;
        }
        let hash = claimed.metadata.xorb_hash.hex();
        let info = match load_xorb_range_info(store, &hash, None) {
            Ok(info) => info,
            Err(XetAdapterError::MissingReferencedXorb) => continue,
            Err(error) => return Err(error),
        };
        verification::verify_xorb_members(claimed, &info.verified_chunks)?;
        if info.verified_chunks.len() <= shardline_index::MAX_XET_DESCRIPTOR_CHUNKS {
            return Ok(Some(info.descriptor(&hash)));
        }
    }
    Ok(None)
}
