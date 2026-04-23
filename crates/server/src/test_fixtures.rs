use std::io::{self, Cursor};

use axum::body::Bytes;
use shardline_protocol::ShardlineHash;
use xet_core_structures::{
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

pub(crate) fn xet_hash_hex(hash: &MerkleHash) -> String {
    let bytes: [u8; 32] = hash.as_bytes().try_into().unwrap_or([0; 32]);
    ShardlineHash::from_bytes(bytes).api_hex_string()
}

pub(crate) fn single_chunk_xorb(bytes: &[u8]) -> (Bytes, String) {
    let chunk_hash = compute_data_hash(bytes);
    let xorb_hash = xorb_hash(&[(chunk_hash, u64::try_from(bytes.len()).unwrap_or(0))]);
    let serialized = serialized_xorb_object_from_components(
        &xorb_hash,
        bytes.to_vec(),
        vec![(chunk_hash, u32::try_from(bytes.len()).unwrap_or(0))],
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

pub(crate) fn single_file_shard(parts: &[(&[u8], &str)]) -> (Bytes, String) {
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
                    1_u32,
                    u32::try_from(bytes.len()).unwrap_or(0),
                ),
                chunks: vec![XorbChunkSequenceEntry::new(
                    chunk_hash,
                    u32::try_from(bytes.len()).unwrap_or(0),
                    0_u32,
                )],
            })
            .ok();
        assert!(add_xorb.is_some());
        if add_xorb.is_none() {
            return (Bytes::new(), String::new());
        }
        file_segments.push(FileDataSequenceEntry::new(
            xorb_hash,
            u32::try_from(bytes.len()).unwrap_or(0),
            0_u32,
            1_u32,
        ));
        file_chunks.push((chunk_hash, u64::try_from(bytes.len()).unwrap_or(0)));
    }

    let file_hash = file_hash(&file_chunks);
    let add_file = shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash, file_segments.len(), false, false),
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

pub(crate) fn shard_hash_hex(bytes: &[u8]) -> String {
    let mut sink = HashedWrite::new(io::sink());
    let copied = io::copy(&mut Cursor::new(bytes), &mut sink);
    assert!(copied.is_ok());
    xet_hash_hex(&sink.hash())
}
