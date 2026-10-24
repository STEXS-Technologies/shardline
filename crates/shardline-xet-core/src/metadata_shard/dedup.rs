//! Deduplication replies use the upstream wire format, independently of the
//! historical internal shard serialization used for retained upload metadata.

use std::{io::Cursor, time::Duration};

use xet_core_structures::{
    merklehash::MerkleHash as WireHash,
    metadata_shard::{
        shard_format::MDBShardInfo as WireShardInfo,
        shard_in_memory::MDBInMemoryShard as WireShard,
        xorb_structs::{
            MDBXorbInfo as WireXorb, XorbChunkSequenceEntry as WireChunk,
            XorbChunkSequenceHeader as WireXorbHeader,
        },
    },
};

use super::xorb_structs::MDBXorbInfo;
use crate::{CoreError, error::Result, merklehash::HMACKey};

/// Serializes Xorb descriptors as an expiring, keyed deduplication reply.
///
/// The caller must supply only verified descriptors authorized for the caller.
/// No file records are included. A nonzero, unpredictable key protects raw
/// member hashes; it is included in the standard footer for client matching.
/// Existing retained shard encodings are unaffected.
///
/// # Errors
/// Returns an error for an empty key/lifetime, inconsistent descriptors, values
/// outside the upstream wire limits, or serialization failure.
pub fn serialize_keyed_dedup_shard(
    xorbs: &[MDBXorbInfo],
    key: HMACKey,
    valid_for: Duration,
) -> Result<Vec<u8>> {
    if key == HMACKey::default() || valid_for.is_zero() {
        return Err(CoreError::InvalidArguments);
    }
    let mut shard = WireShard::default();
    for xorb in xorbs {
        let chunk_count =
            u32::try_from(xorb.chunks.len()).map_err(|_error| CoreError::InvalidRange)?;
        if u64::from(chunk_count) != xorb.metadata.num_entries {
            return Err(CoreError::InvalidArguments);
        }
        let mut header = WireXorbHeader::new(
            wire_hash(xorb.metadata.xorb_hash),
            chunk_count,
            narrow(xorb.metadata.num_bytes_in_xorb)?,
        );
        header.xorb_flags = xorb.metadata.xorb_flags;
        header.num_bytes_on_disk = narrow(xorb.metadata.num_bytes_on_disk)?;
        let mut chunks = Vec::with_capacity(xorb.chunks.len());
        for chunk in &xorb.chunks {
            let mut entry = WireChunk::new(
                wire_hash(chunk.chunk_hash),
                narrow(chunk.unpacked_segment_bytes)?,
                narrow(chunk.chunk_byte_range_start)?,
            );
            entry.flags = chunk.flags;
            chunks.push(entry);
        }
        shard
            .add_xorb_block(WireXorb {
                metadata: header,
                chunks,
            })
            .map_err(CoreError::invalid_shard)?;
    }
    let unkeyed = shard.to_bytes().map_err(CoreError::invalid_shard)?;
    let mut result = Vec::with_capacity(unkeyed.len());
    WireShardInfo::export_as_keyed_shard_streaming(
        &mut Cursor::new(unkeyed),
        &mut result,
        wire_hash(key),
        valid_for,
        false,
        true,
        true,
    )
    .map_err(CoreError::invalid_shard)?;
    Ok(result)
}

/// Builds a repository-scoped lookup shard without advertising hidden members.
///
/// Original chunk positions and lengths must be retained for valid Xet file
/// terms. Hidden positions therefore use opaque, independently keyed labels;
/// they cannot match a client's content hash under the public matching key.
/// `private_key` must be independently random and must never be sent to clients.
/// This is a transient lookup index, not a retained source descriptor: callers
/// must authenticate the original descriptors before applying this projection.
/// No file metadata or source bytes are included.
///
/// # Errors
/// Returns an error for inconsistent masks, empty visibility, unsafe keys, or
/// values rejected by the standard wire serializer.
pub fn serialize_scoped_keyed_dedup_shard(
    xorbs: &[MDBXorbInfo],
    visible: &[Vec<bool>],
    matching_key: HMACKey,
    private_key: HMACKey,
    valid_for: Duration,
) -> Result<Vec<u8>> {
    if xorbs.len() != visible.len()
        || private_key == HMACKey::default()
        || private_key == matching_key
    {
        return Err(CoreError::InvalidArguments);
    }
    let private_bytes: [u8; 32] = private_key.into();
    let mut projected = Vec::with_capacity(xorbs.len());
    for (xorb, mask) in xorbs.iter().zip(visible) {
        if xorb.chunks.len() != mask.len() || !mask.iter().any(|allowed| *allowed) {
            return Err(CoreError::InvalidArguments);
        }
        let mut entry = xorb.clone();
        for (index, (chunk, allowed)) in entry.chunks.iter_mut().zip(mask).enumerate() {
            if !allowed {
                let mut label = blake3::Hasher::new_keyed(&private_bytes);
                label.update(b"shardline-xet-hidden-member-v1\0");
                label.update(&<[u8; 32]>::from(xorb.metadata.xorb_hash));
                label.update(
                    &u64::try_from(index)
                        .map_err(|_error| CoreError::InvalidRange)?
                        .to_le_bytes(),
                );
                label.update(&<[u8; 32]>::from(chunk.chunk_hash));
                chunk.chunk_hash = (*label.finalize().as_bytes()).into();
                chunk.flags = 0;
            }
        }
        projected.push(entry);
    }
    serialize_keyed_dedup_shard(&projected, matching_key, valid_for)
}

fn narrow(value: u64) -> Result<u32> {
    u32::try_from(value).map_err(|_error| CoreError::InvalidRange)
}

fn wire_hash(value: crate::MerkleHash) -> WireHash {
    WireHash::from(<[u8; 32]>::from(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        merklehash::compute_data_hash,
        metadata_shard::xorb_structs::{XorbChunkSequenceEntry, XorbChunkSequenceHeader},
    };

    #[test]
    fn official_client_parses_keyed_reply_and_matches_original_chunk_indices() -> Result<()> {
        let first = compute_data_hash(b"first chunk");
        let second = compute_data_hash(b"second chunk");
        let xorb_hash = compute_data_hash(b"container");
        let xorb = MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(xorb_hash, 2, 23),
            chunks: vec![
                XorbChunkSequenceEntry::new(first, 11, 0),
                XorbChunkSequenceEntry::new(second, 12, 11),
            ],
        };
        let key = compute_data_hash(b"unpredictable per-response test key");
        let bytes = serialize_keyed_dedup_shard(&[xorb], key, Duration::from_secs(3600))?;
        let mut cursor = Cursor::new(bytes);
        let parsed =
            WireShardInfo::load_from_reader(&mut cursor).map_err(CoreError::invalid_shard)?;
        assert_eq!(parsed.num_file_entries(), 0);
        assert_eq!(parsed.num_xorb_entries(), 1);
        assert_eq!(parsed.chunk_hmac_key(), Some(wire_hash(key)));
        let descriptors = parsed
            .read_all_xorb_blocks_full(&mut cursor)
            .map_err(CoreError::invalid_shard)?;
        assert_eq!(
            descriptors[0].chunks[0].chunk_hash,
            wire_hash(first).hmac(wire_hash(key))
        );
        assert_ne!(descriptors[0].chunks[0].chunk_hash, wire_hash(first));
        let matched = parsed
            .chunk_hash_dedup_query(&mut cursor, &[wire_hash(second)])
            .map_err(CoreError::invalid_shard)?;
        let Some((count, term)) = matched else {
            return Err(CoreError::InvalidShard(
                "official client could not match keyed chunk".into(),
            ));
        };
        assert_eq!(count, 1);
        assert_eq!(term.xorb_hash, wire_hash(xorb_hash));
        assert_eq!(term.chunk_index_start, 1);
        assert_eq!(term.chunk_index_end, 2);
        assert!(parsed.metadata.shard_key_expiry > parsed.metadata.shard_creation_timestamp);
        Ok(())
    }

    #[test]
    fn rejects_unprotected_and_out_of_range_descriptors() {
        assert!(
            serialize_keyed_dedup_shard(&[], HMACKey::default(), Duration::from_secs(1)).is_err()
        );
        let key = compute_data_hash(b"key");
        assert!(serialize_keyed_dedup_shard(&[], key, Duration::ZERO).is_err());
        let oversized = MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(compute_data_hash(b"xorb"), 0, u64::MAX),
            chunks: vec![],
        };
        assert!(serialize_keyed_dedup_shard(&[oversized], key, Duration::from_secs(1)).is_err());
    }

    #[test]
    fn scoped_lookup_preserves_indices_without_matching_hidden_content() -> Result<()> {
        let hidden = compute_data_hash(b"other repository private content");
        let allowed = compute_data_hash(b"authorized content");
        let source = MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(compute_data_hash(b"container"), 2, 8),
            chunks: vec![
                XorbChunkSequenceEntry::new(hidden, 4, 0),
                XorbChunkSequenceEntry::new(allowed, 4, 4),
            ],
        };
        let matching = compute_data_hash(b"public matching key");
        let private = compute_data_hash(b"private independent key");
        let bytes = serialize_scoped_keyed_dedup_shard(
            std::slice::from_ref(&source),
            &[vec![false, true]],
            matching,
            private,
            Duration::from_secs(900),
        )?;
        let mut reader = Cursor::new(bytes);
        let shard =
            WireShardInfo::load_from_reader(&mut reader).map_err(CoreError::invalid_shard)?;
        assert_eq!(shard.num_file_entries(), 0);
        assert_eq!(shard.chunk_hmac_key(), Some(wire_hash(matching)));
        assert!(
            shard
                .chunk_hash_dedup_query(&mut reader, &[wire_hash(hidden)])
                .map_err(CoreError::invalid_shard)?
                .is_none()
        );
        let (count, term) = shard
            .chunk_hash_dedup_query(&mut reader, &[wire_hash(allowed)])
            .map_err(CoreError::invalid_shard)?
            .ok_or(CoreError::InvalidArguments)?;
        assert_eq!(count, 1);
        assert_eq!((term.chunk_index_start, term.chunk_index_end), (1, 2));
        assert_eq!(term.xorb_hash, wire_hash(source.metadata.xorb_hash));
        assert_eq!(
            source.chunks[0].chunk_hash, hidden,
            "retained metadata must not change"
        );
        for (mask, key) in [
            (vec![true], private),
            (vec![false, false], private),
            (vec![true, true], matching),
        ] {
            assert!(
                serialize_scoped_keyed_dedup_shard(
                    std::slice::from_ref(&source),
                    &[mask],
                    matching,
                    key,
                    Duration::from_secs(900)
                )
                .is_err()
            );
        }
        Ok(())
    }
}
