//! SDK/server container interoperability, including the production validation path.

use std::io::Cursor;

use bytes::Bytes;
use sdx::{
    hash::parse_xet_hash_hex,
    xorb_build::{MAX_XORB_CHUNKS, build_xorb},
};
use shardline_test_support::lcg_bytes as lcg_data;

fn chunk_pairs(chunks: &[Vec<u8>]) -> Vec<(Bytes, u64)> {
    let mut offset = 0u64;
    chunks
        .iter()
        .map(|data| {
            let pair = (Bytes::copy_from_slice(data), offset);
            offset = offset.saturating_add(u64::try_from(data.len()).unwrap());
            pair
        })
        .collect()
}

/// Asserts byte-identity with the server packer on the same chunk inputs.
fn assert_byte_identical(chunks: &[Vec<u8>]) {
    let pairs = chunk_pairs(chunks);
    let server = shardline_server::upload_ingest::xorb_packer::pack_chunks_into_xorb(
        &pairs
            .iter()
            .map(|(data, offset)| (data.to_vec(), *offset))
            .collect::<Vec<_>>(),
    )
    .unwrap();

    let built = build_xorb(&pairs).unwrap();

    assert_eq!(
        built.serialized, server.serialized,
        "serialized bytes differ from server packer"
    );
    assert_eq!(
        built.xorb_hash_hex, server.xorb_hash_hex,
        "xorb hash hex differs from server packer"
    );
    assert_eq!(
        built.chunk_entries.len(),
        server.chunk_entries.len(),
        "chunk entry count differs"
    );
    for (mine, theirs) in built.chunk_entries.iter().zip(server.chunk_entries.iter()) {
        assert_eq!(mine.chunk_index, theirs.chunk_index);
        assert_eq!(mine.raw_offset, theirs.raw_offset);
        assert_eq!(mine.raw_length, theirs.raw_length);
        assert_eq!(mine.packed_offset, u64::from(theirs.packed_offset));
        assert_eq!(mine.packed_length, u64::from(theirs.packed_length));
    }
    // Footer start equals the server's packed content length.
    let _ = parse_xet_hash_hex(&built.xorb_hash_hex).unwrap();
    assert_eq!(built.payload().len(), built.footer_start);
    assert!(!built.payload().is_empty());
    assert!(built.serialized.len() >= built.footer_start);
}

#[test]
fn build_xorb_matches_server_single_chunk() {
    assert_byte_identical(&[b"hello world".to_vec()]);
    assert_byte_identical(&[b"A".repeat(2_000_000)]);
    assert_byte_identical(&[lcg_data(1024 * 1024, 1)]);
    assert_byte_identical(&[vec![0u8; 65536]]);
}

#[test]
fn build_xorb_matches_server_multiple_chunks() {
    assert_byte_identical(&[b"hello ".to_vec(), b"world".to_vec(), b"!".to_vec()]);
    assert_byte_identical(&[vec![0xABu8; 65536], vec![0xCDu8; 65536], lcg_data(4096, 5)]);
}

#[test]
fn build_xorb_matches_server_max_chunks() {
    // 8192 tiny chunks (the max per xorb).
    let chunks: Vec<Vec<u8>> = (0..MAX_XORB_CHUNKS)
        .map(|i| vec![(i & 0xFF) as u8; 64])
        .collect();
    assert_byte_identical(&chunks);
}

#[test]
fn build_xorb_matches_server_binary_and_incompressible() {
    let chunks: Vec<Vec<u8>> = (0..256)
        .map(|i| (0..64).map(|j| ((i + j) & 0xFF) as u8).collect())
        .collect();
    assert_byte_identical(&chunks);
    assert_byte_identical(&(0..64).map(|i| lcg_data(65536, i)).collect::<Vec<_>>());
}

#[test]
fn build_xorb_roundtrips_through_server_validation_path() {
    let chunks = chunk_pairs(&[
        b"server validation chunk".to_vec(),
        lcg_data(50_000, 21),
        b"tail".to_vec(),
    ]);
    let built = build_xorb(&chunks).unwrap();

    let expected: shardline_protocol::ShardlineHash = {
        let hash: xet_core_structures::merklehash::MerkleHash =
            parse_xet_hash_hex(&built.xorb_hash_hex).unwrap();
        let bytes: [u8; 32] = hash.into();
        shardline_protocol::ShardlineHash::from_bytes(bytes)
    };

    let mut cursor = Cursor::new(built.serialized.as_slice());
    let validated = shardline_server::validate_serialized_xorb(&mut cursor, expected).unwrap();
    let decoded = shardline_server::decode_serialized_xorb_chunks(&mut cursor, &validated).unwrap();
    assert_eq!(decoded.len(), chunks.len());
    for (decoded_chunk, (expected, _)) in decoded.iter().zip(chunks.iter()) {
        assert_eq!(decoded_chunk.data(), expected.as_ref());
    }
}
