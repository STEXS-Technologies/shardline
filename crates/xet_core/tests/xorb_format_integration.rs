use std::io::Cursor;

use shardline_xet_core::merklehash::MerkleHash;
use shardline_xet_core::xorb_object::{
    Chunk, CompressionScheme, RawXorbData,
    xorb_chunk_format::{
        XORB_CHUNK_HEADER_LENGTH, deserialize_chunk, deserialize_chunks_to_writer,
        parse_chunk_header, serialize_chunk,
    },
    xorb_format_test_utils::{
        ChunkSize, build_raw_xorb, build_xorb_object, serialized_xorb_object_from_components,
    },
    xorb_object_format::{
        SerializedXorbObject, XorbObject, XorbObjectInfoV1, reconstruct_xorb_with_footer,
    },
};

// ============================================================================
// Full round-trip: create chunks → build xorb → serialize → deserialize →
// validate → decode. Tests the interaction between xorb_object_format,
// xorb_chunk_format, compression_scheme, and merklehash.
// ============================================================================

/// Full round-trip with no compression
#[test]
fn xorb_roundtrip_none_compression() {
    let (obj, chunk_data, raw_data, _boundaries) =
        build_xorb_object(3, ChunkSize::Fixed(256), CompressionScheme::None).unwrap();

    assert_eq!(obj.info.num_chunks, 3);
    assert_ne!(obj.info.xorb_hash, MerkleHash::default());

    // Build a complete xorb buffer: chunk data + info footer + info length
    let mut buf = Vec::new();
    buf.extend_from_slice(&chunk_data);
    let info_len = obj.info.serialize(&mut buf).unwrap();
    buf.extend_from_slice(&(info_len as u64).to_le_bytes());

    // Deserialize the xorb object
    let mut cursor = Cursor::new(&buf);
    let deserialized = XorbObject::deserialize(&mut cursor).unwrap();
    assert_eq!(deserialized.info.num_chunks, 3);
    assert_eq!(deserialized.info.xorb_hash, obj.info.xorb_hash);
    assert_eq!(
        deserialized.info.chunk_boundary_offsets,
        obj.info.chunk_boundary_offsets
    );

    // Validate against the computed hash
    let mut cursor2 = Cursor::new(&buf);
    let validated = XorbObject::validate_xorb_object(&mut cursor2, &obj.info.xorb_hash)
        .unwrap()
        .expect("validation should succeed");
    assert_eq!(validated.info.num_chunks, 3);

    // Decode: deserialize chunks to writer
    let mut cursor3 = Cursor::new(&chunk_data);
    let mut decoded = Vec::new();
    let (_, indices) = deserialize_chunks_to_writer(&mut cursor3, &mut decoded).unwrap();
    assert_eq!(decoded, raw_data);
    assert_eq!(indices.len(), 4); // 3 chunks => 4 boundary indices
    assert_eq!(indices[3] as usize, raw_data.len());
}

/// Full round-trip with LZ4 compression
#[test]
fn xorb_roundtrip_lz4_compression() {
    let (obj, chunk_data, raw_data, _boundaries) =
        build_xorb_object(5, ChunkSize::Fixed(128), CompressionScheme::LZ4).unwrap();

    assert_eq!(obj.info.num_chunks, 5);

    // Roundtrip through serialization
    let mut buf = Vec::new();
    buf.extend_from_slice(&chunk_data);
    let info_len = obj.info.serialize(&mut buf).unwrap();
    buf.extend_from_slice(&(info_len as u64).to_le_bytes());

    let mut cursor = Cursor::new(&buf);
    let deserialized = XorbObject::deserialize(&mut cursor).unwrap();
    assert_eq!(deserialized.info.xorb_hash, obj.info.xorb_hash);

    // Validate
    let mut cursor2 = Cursor::new(&buf);
    let validated = XorbObject::validate_xorb_object(&mut cursor2, &obj.info.xorb_hash)
        .unwrap()
        .expect("validation should succeed");
    assert_eq!(validated.info.num_chunks, 5);

    // Decode chunks
    let mut cursor3 = Cursor::new(&chunk_data);
    let mut decoded = Vec::new();
    deserialize_chunks_to_writer(&mut cursor3, &mut decoded).unwrap();
    assert_eq!(decoded, raw_data);
}

/// Full round-trip with ByteGrouping4LZ4 compression
#[test]
fn xorb_roundtrip_bg4_lz4_compression() {
    let (obj, chunk_data, raw_data, _boundaries) = build_xorb_object(
        2,
        ChunkSize::Fixed(512),
        CompressionScheme::ByteGrouping4LZ4,
    )
    .unwrap();

    assert_eq!(obj.info.num_chunks, 2);

    let mut buf = Vec::new();
    buf.extend_from_slice(&chunk_data);
    let info_len = obj.info.serialize(&mut buf).unwrap();
    buf.extend_from_slice(&(info_len as u64).to_le_bytes());

    let mut cursor = Cursor::new(&buf);
    let deserialized = XorbObject::deserialize(&mut cursor).unwrap();
    assert_eq!(deserialized.info.xorb_hash, obj.info.xorb_hash);

    // Decode and verify decompressed data
    let mut cursor3 = Cursor::new(&chunk_data);
    let mut decoded = Vec::new();
    deserialize_chunks_to_writer(&mut cursor3, &mut decoded).unwrap();
    assert_eq!(decoded, raw_data);
}

/// Single chunk round-trip
#[test]
fn xorb_roundtrip_single_chunk() {
    let (obj, chunk_data, raw_data, _boundaries) =
        build_xorb_object(1, ChunkSize::Fixed(64), CompressionScheme::None).unwrap();

    assert_eq!(obj.info.num_chunks, 1);

    let mut buf = Vec::new();
    buf.extend_from_slice(&chunk_data);
    let info_len = obj.info.serialize(&mut buf).unwrap();
    buf.extend_from_slice(&(info_len as u64).to_le_bytes());

    let mut cursor = Cursor::new(&buf);
    let deserialized = XorbObject::deserialize(&mut cursor).unwrap();
    assert_eq!(deserialized.info.num_chunks, 1);

    let mut cursor2 = Cursor::new(&buf);
    let validated = XorbObject::validate_xorb_object(&mut cursor2, &obj.info.xorb_hash)
        .unwrap()
        .expect("single chunk validation should succeed");
    assert_eq!(validated.info.num_chunks, 1);

    let mut decoded = Vec::new();
    deserialize_chunks_to_writer(&mut Cursor::new(&chunk_data), &mut decoded).unwrap();
    assert_eq!(decoded, raw_data);
}

/// Many chunks round-trip (10 chunks)
#[test]
fn xorb_roundtrip_many_chunks() {
    let (obj, chunk_data, raw_data, _boundaries) =
        build_xorb_object(10, ChunkSize::Fixed(64), CompressionScheme::LZ4).unwrap();

    assert_eq!(obj.info.num_chunks, 10);

    let mut buf = Vec::new();
    buf.extend_from_slice(&chunk_data);
    let info_len = obj.info.serialize(&mut buf).unwrap();
    buf.extend_from_slice(&(info_len as u64).to_le_bytes());

    let mut cursor = Cursor::new(&buf);
    let deserialized = XorbObject::deserialize(&mut cursor).unwrap();
    assert_eq!(deserialized.info.num_chunks, 10);

    let mut cursor2 = Cursor::new(&buf);
    let validated = XorbObject::validate_xorb_object(&mut cursor2, &obj.info.xorb_hash)
        .unwrap()
        .expect("many chunks validation should succeed");
    assert_eq!(validated.info.num_chunks, 10);

    let mut decoded = Vec::new();
    deserialize_chunks_to_writer(&mut Cursor::new(&chunk_data), &mut decoded).unwrap();
    assert_eq!(decoded, raw_data);
}

/// Edge case: zero chunks should produce a valid xorb with num_chunks=0
#[test]
fn xorb_roundtrip_zero_chunks() {
    let info = XorbObjectInfoV1::default();
    let mut buf = Vec::new();
    let n = info.serialize(&mut buf).unwrap();
    buf.extend_from_slice(&(n as u64).to_le_bytes());

    let mut cursor = Cursor::new(&buf);
    let deserialized = XorbObject::deserialize(&mut cursor).unwrap();
    assert_eq!(deserialized.info.num_chunks, 0);
}

/// Test reconstruct_xorb_with_footer: build chunk, reconstruct, verify hash
#[test]
fn xorb_reconstruct_with_footer_roundtrip() {
    let data = b"Hello, this is test data for xorb reconstruction!";
    let mut chunk_writer = Cursor::new(Vec::new());
    serialize_chunk(data, &mut chunk_writer, CompressionScheme::LZ4).unwrap();
    let chunk_data = chunk_writer.into_inner();

    let mut output = Cursor::new(Vec::new());
    let (xorb_obj, hash) = reconstruct_xorb_with_footer(&mut output, &chunk_data).unwrap();

    assert_ne!(hash, MerkleHash::default());
    assert_eq!(xorb_obj.info.num_chunks, 1);
    assert_eq!(xorb_obj.info.xorb_hash, hash);

    // Deserialize and validate
    let mut cursor = Cursor::new(output.into_inner());
    let deserialized = XorbObject::deserialize(&mut cursor).unwrap();
    assert_eq!(deserialized.info.xorb_hash, hash);
    assert_eq!(deserialized.info.num_chunks, 1);
}

/// Test SerializedXorbObject::from_xorb_with_compression with footer
#[test]
fn serialized_xorb_from_xorb_with_footer() {
    let chunks = vec![
        Chunk {
            hash: shardline_xet_core::merklehash::compute_data_hash(b"c1"),
            data: vec![0xABu8; 100].into(),
        },
        Chunk {
            hash: shardline_xet_core::merklehash::compute_data_hash(b"c2"),
            data: vec![0xCDu8; 200].into(),
        },
    ];
    let raw = RawXorbData::from_chunks(&chunks, vec![0, 100]);
    let serialized =
        SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
            .unwrap();

    assert!(serialized.footer_start.is_some());
    assert_eq!(serialized.num_chunks, 2);
    assert_ne!(serialized.hash, MerkleHash::default());
    assert_eq!(serialized.raw_num_bytes, 300);

    // Can deserialize from the serialized data
    let mut cursor = Cursor::new(&serialized.serialized_data);
    let obj = XorbObject::deserialize(&mut cursor).unwrap();
    assert_eq!(obj.info.num_chunks, 2);
    assert_eq!(obj.info.xorb_hash, serialized.hash);
}

/// Test SerializedXorbObject::from_xorb_with_compression without footer
#[test]
fn serialized_xorb_from_xorb_without_footer() {
    let chunks = vec![Chunk {
        hash: shardline_xet_core::merklehash::compute_data_hash(b"single"),
        data: vec![0xFFu8; 50].into(),
    }];
    let raw = RawXorbData::from_chunks(&chunks, vec![0]);
    let serialized =
        SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, false)
            .unwrap();

    assert!(serialized.footer_start.is_none());
    assert_eq!(serialized.num_chunks, 1);
    assert!(!serialized.serialized_data.is_empty());
}

/// Test serialized_xorb_object_from_components utility
#[test]
fn xorb_serialize_from_components_roundtrip() {
    let hash = shardline_xet_core::merklehash::compute_data_hash(b"test_hash");
    let data = vec![0x42u8; 1024];
    let chunk_boundaries = vec![
        (
            shardline_xet_core::merklehash::compute_data_hash(b"c1"),
            512,
        ),
        (
            shardline_xet_core::merklehash::compute_data_hash(b"c2"),
            1024,
        ),
    ];

    let serialized = serialized_xorb_object_from_components(
        &hash,
        data,
        chunk_boundaries,
        CompressionScheme::None,
    )
    .unwrap();

    assert_eq!(serialized.hash, hash);
    assert_eq!(serialized.raw_num_bytes, 1024);
    assert_eq!(serialized.num_chunks, 2);
    assert!(serialized.footer_start.is_some());

    // Deserialize
    let mut cursor = Cursor::new(&serialized.serialized_data);
    let obj = XorbObject::deserialize(&mut cursor).unwrap();
    assert_eq!(obj.info.xorb_hash, hash);
    assert_eq!(obj.info.num_chunks, 2);
}

/// Test that corrupted chunk data fails validation
#[test]
fn xorb_corrupted_chunk_data_rejected() {
    let (obj, chunk_data, _raw_data, _boundaries) =
        build_xorb_object(2, ChunkSize::Fixed(128), CompressionScheme::None).unwrap();

    let mut buf = Vec::new();
    buf.extend_from_slice(&chunk_data);
    // Corrupt a byte in the middle of the chunk data
    if buf.len() > 50 {
        buf[25] ^= 0xFF;
    }
    let info_len = obj.info.serialize(&mut buf).unwrap();
    buf.extend_from_slice(&(info_len as u64).to_le_bytes());

    let mut cursor = Cursor::new(&buf);
    let result = XorbObject::validate_xorb_object(&mut cursor, &obj.info.xorb_hash).unwrap();
    // Validation should return None (chunk mismatch)
    assert!(result.is_none());
}

/// Test that wrong hash fails validation
#[test]
fn xorb_wrong_hash_rejected() {
    let (obj, chunk_data, _raw_data, _boundaries) =
        build_xorb_object(1, ChunkSize::Fixed(64), CompressionScheme::None).unwrap();

    let mut buf = Vec::new();
    buf.extend_from_slice(&chunk_data);
    let info_len = obj.info.serialize(&mut buf).unwrap();
    buf.extend_from_slice(&(info_len as u64).to_le_bytes());

    let wrong_hash = shardline_xet_core::merklehash::compute_data_hash(b"wrong");
    let mut cursor = Cursor::new(&buf);
    let result = XorbObject::validate_xorb_object(&mut cursor, &wrong_hash).unwrap();
    assert!(result.is_none());
}

/// Test deserialize_chunks_to_writer with no chunks (empty input)
#[test]
fn deserialize_chunks_to_writer_empty() {
    let mut reader = Cursor::new(Vec::new());
    let mut writer = Vec::new();
    let (compressed, indices) = deserialize_chunks_to_writer(&mut reader, &mut writer).unwrap();
    assert_eq!(compressed, 0);
    assert_eq!(indices, vec![0]);
    assert!(writer.is_empty());
}

/// Test serialize_chunk + deserialize_chunk roundtrip with various data sizes
#[test]
fn serialize_deserialize_chunk_various_sizes() {
    for &size in &[0usize, 1, 10, 100, 1000, 10000] {
        let data = vec![0xABu8; size];
        let mut writer = Cursor::new(Vec::new());
        let written = serialize_chunk(&data, &mut writer, CompressionScheme::LZ4).unwrap();
        assert!(written >= XORB_CHUNK_HEADER_LENGTH);

        writer.set_position(0);
        let (decompressed, comp_size, uncomp_size) = deserialize_chunk(&mut writer).unwrap();
        assert_eq!(decompressed, data, "size={size} mismatch");
        assert_eq!(
            uncomp_size as usize,
            data.len(),
            "size={size} uncompressed_len mismatch"
        );
        assert!(comp_size > 0);
    }
}

/// Test that parsing a chunk header with an invalid compression scheme fails
#[test]
fn parse_chunk_header_invalid_scheme() {
    let mut buf = [0u8; XORB_CHUNK_HEADER_LENGTH];
    buf[1..4].copy_from_slice(&100u32.to_le_bytes()[0..3]);
    buf[4] = 255; // invalid compression scheme
    buf[5..8].copy_from_slice(&100u32.to_le_bytes()[0..3]);
    assert!(parse_chunk_header(buf).is_err());
}

/// Test that parsing a chunk header with "XETBLOB" ident fails (reserved)
#[test]
fn parse_chunk_header_rejects_xetblob_ident() {
    let mut buf = [0u8; XORB_CHUNK_HEADER_LENGTH];
    buf[..7].copy_from_slice(b"XETBLOB");
    assert!(parse_chunk_header(buf).is_err());
}

/// Test XorbObjectInfoV1 serialization roundtrip with real chunk hashes
#[test]
fn xorb_info_v1_roundtrip_with_real_hashes() {
    let mut info = XorbObjectInfoV1::default();
    info.xorb_hash = shardline_xet_core::merklehash::compute_data_hash(b"xorb_hash");
    info.num_chunks = 2;
    info.chunk_hashes = vec![
        shardline_xet_core::merklehash::compute_data_hash(b"chunk_a"),
        shardline_xet_core::merklehash::compute_data_hash(b"chunk_b"),
    ];
    info.chunk_boundary_offsets = vec![50, 120];
    info.unpacked_chunk_offsets = vec![48, 100];
    info.fill_in_boundary_offsets();

    let mut buf = Vec::new();
    let written = info.serialize(&mut buf).unwrap();
    assert!(written > 0);

    let mut cursor = Cursor::new(buf);
    let (deser, bytes_read) = XorbObjectInfoV1::deserialize(&mut cursor).unwrap();
    assert_eq!(bytes_read, written as u32);
    assert_eq!(deser.xorb_hash, info.xorb_hash);
    assert_eq!(deser.num_chunks, 2);
    assert_eq!(deser.chunk_hashes, info.chunk_hashes);
    assert_eq!(deser.chunk_boundary_offsets, info.chunk_boundary_offsets);
}

/// Test build_raw_xorb with different chunk sizes
#[test]
fn build_raw_xorb_various_sizes() {
    let raw = build_raw_xorb(4, ChunkSize::Fixed(1024));
    assert_eq!(raw.data.len(), 4);
    assert_eq!(raw.num_bytes(), 4096);
    assert_ne!(raw.hash(), MerkleHash::default());
}

/// Cross-module: compute data hash, then xorb hash, then verify consistency
#[test]
fn cross_module_hash_consistency() {
    use shardline_xet_core::merklehash::{compute_data_hash, file_hash, xorb_hash};

    let data_slices: &[&[u8]] = &[b"hello", b"world", b"integration", b"test"];
    let hashes: Vec<_> = data_slices.iter().map(|d| compute_data_hash(d)).collect();
    let sizes: Vec<u64> = data_slices.iter().map(|d| d.len() as u64).collect();

    let pairs: Vec<_> = hashes.iter().copied().zip(sizes.iter().copied()).collect();
    let xh = xorb_hash(&pairs);
    let fh = file_hash(&pairs);

    assert_ne!(xh, MerkleHash::default());
    assert_ne!(fh, MerkleHash::default());
    assert_ne!(xh, fh, "xorb_hash and file_hash should differ");

    // Verify determinism
    assert_eq!(xorb_hash(&pairs), xorb_hash(&pairs));
    assert_eq!(file_hash(&pairs), file_hash(&pairs));
}
