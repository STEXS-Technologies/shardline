//! Shared metadata fixtures for bounded parsing and ingestion tests.

use shardline_xet_core::{
    merklehash::{MerkleHash, compute_data_hash, file_hash},
    metadata_shard::{
        file_structs::{
            FileDataSequenceEntry, FileDataSequenceHeader, FileVerificationEntry, MDBFileInfo,
        },
        shard_format::MDBShardInfo,
        shard_in_memory::MDBInMemoryShard,
        xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
    },
};

pub(crate) fn serialize_test_shard(
    file_infos: Vec<MDBFileInfo>,
    xorb_infos: Vec<MDBXorbInfo>,
) -> Vec<u8> {
    let mut shard = MDBInMemoryShard::default();
    for file_info in file_infos {
        assert!(shard.add_file_reconstruction_info(file_info).is_ok());
    }
    for xorb_info in xorb_infos {
        assert!(shard.add_xorb_block(xorb_info).is_ok());
    }

    let mut serialized = Vec::new();
    assert!(MDBShardInfo::serialize_from(&mut serialized, &shard, None).is_ok());
    serialized
}

pub(crate) fn single_chunk_shard(
    xorb_hash: MerkleHash,
    verification: Vec<FileVerificationEntry>,
) -> Vec<u8> {
    let chunk_hash = compute_data_hash(&[0_u8; 256]);
    let file_hash = file_hash(&[(chunk_hash, 256_u64)]);
    serialize_test_shard(
        vec![MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash, 1u64, !verification.is_empty(), false),
            segments: vec![FileDataSequenceEntry::new(xorb_hash, 256_u64, 0_u64, 1_u64)],
            verification,
            metadata_ext: None,
        }],
        vec![MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u64, 256_u64),
            chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 256_u64, 0_u64)],
        }],
    )
}
