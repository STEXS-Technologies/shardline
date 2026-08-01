use shardline_index::xet_hash_hex_string;
use std::io::{self, Cursor};

use axum::body::Bytes;
use shardline_protocol::ShardlineHash;
use shardline_xet_core::{
    merklehash::{HashedWrite, MerkleHash, compute_data_hash, file_hash, xorb_hash},
    metadata_shard::{
        file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
        shard_format::MDBShardInfo,
        shard_in_memory::MDBInMemoryShard,
        xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
    },
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

#[must_use]
pub fn xet_hash_hex(hash: &MerkleHash) -> String {
    let bytes: [u8; 32] = (*hash).into();
    xet_hash_hex_string(ShardlineHash::from_bytes(bytes))
}

/// # Panics
///
/// Panics if serialized xorb object creation fails.
#[must_use]
pub fn single_chunk_xorb(bytes: &[u8]) -> (Bytes, String) {
    let chunk_hash = compute_data_hash(bytes);
    let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(bytes.len()).unwrap_or(0))]);
    let serialized = serialized_xorb_object_from_components(
        &xorb_hash,
        bytes.to_vec(),
        vec![(chunk_hash, u64::try_from(bytes.len()).unwrap_or(0))],
        CompressionScheme::None,
    )
    .ok();
    assert!(serialized.is_some());
    let Some(serialized) = serialized else {
        return (Bytes::new(), String::new());
    };
    (
        Bytes::from(serialized.serialized_data),
        xet_hash_hex(&xorb_hash),
    )
}

/// # Panics
///
/// Panics if any xorb hash, xorb block, or file reconstruction info cannot be added.
#[must_use]
pub fn single_file_shard(parts: &[(&[u8], &str)]) -> (Bytes, String) {
    let mut shard = MDBInMemoryShard::default();
    let mut file_segments = Vec::with_capacity(parts.len());
    let mut file_chunks = Vec::with_capacity(parts.len());

    for (bytes, xorb_hash_hex) in parts {
        let xorb_hash = MerkleHash::from_hex(xorb_hash_hex).ok();
        assert!(xorb_hash.is_some());
        let Some(xorb_hash) = xorb_hash else {
            return (Bytes::new(), String::new());
        };
        let chunk_hash = compute_data_hash(bytes);
        let add_xorb = shard
            .add_xorb_block(MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(
                    xorb_hash,
                    1_u64,
                    u64::try_from(bytes.len()).unwrap_or(0),
                ),
                chunks: vec![XorbChunkSequenceEntry::new(
                    chunk_hash,
                    u64::try_from(bytes.len()).unwrap_or(0),
                    0_u64,
                )],
            })
            .ok();
        assert!(add_xorb.is_some());
        if add_xorb.is_none() {
            return (Bytes::new(), String::new());
        }
        file_segments.push(FileDataSequenceEntry::new(
            xorb_hash,
            u64::try_from(bytes.len()).unwrap_or(0),
            0_u64,
            1_u64,
        ));
        file_chunks.push((chunk_hash, u64::try_from(bytes.len()).unwrap_or(0)));
    }

    let file_hash = file_hash(&file_chunks);
    let add_file = shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(
                file_hash,
                u64::try_from(file_segments.len()).unwrap_or(0),
                false,
                false,
            ),
            segments: file_segments,
            verification: Vec::new(),
            metadata_ext: None,
        })
        .ok();
    assert!(add_file.is_some());
    if add_file.is_none() {
        return (Bytes::new(), String::new());
    }

    let mut serialized = Vec::new();
    let serialized_result = MDBShardInfo::serialize_from(&mut serialized, &shard, None).ok();
    assert!(serialized_result.is_some());
    if serialized_result.is_none() {
        return (Bytes::new(), String::new());
    }
    (Bytes::from(serialized), xet_hash_hex(&file_hash))
}

/// # Panics
///
/// Panics if the byte copy into the hashed writer fails.
#[must_use]
pub fn shard_hash_hex(bytes: &[u8]) -> String {
    let mut sink = HashedWrite::new(io::sink());
    let copied = io::copy(&mut Cursor::new(bytes), &mut sink);
    assert!(copied.is_ok());
    xet_hash_hex(&sink.hash())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_chunk_xorb_returns_bytes_and_valid_hash() {
        let (bytes, hash) = single_chunk_xorb(b"hello world");
        assert!(!bytes.is_empty());
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn single_chunk_xorb_empty_input() {
        let (bytes, hash) = single_chunk_xorb(b"");
        assert!(!bytes.is_empty());
        assert!(!hash.is_empty());
    }

    #[test]
    fn xet_hash_hex_returns_64_char_hex() {
        let hash = MerkleHash::from_hex("0".repeat(64).as_str()).unwrap();
        let hex = xet_hash_hex(&hash);
        assert_eq!(hex.len(), 64);
    }

    #[test]
    fn shard_hash_hex_returns_deterministic_hash() {
        let h1 = shard_hash_hex(b"test data");
        let h2 = shard_hash_hex(b"test data");
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64);
    }

    #[test]
    fn shard_hash_hex_differs_for_different_input() {
        let h1 = shard_hash_hex(b"data1");
        let h2 = shard_hash_hex(b"data2");
        assert_ne!(h1, h2);
    }
}
