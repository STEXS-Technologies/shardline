#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use,
    clippy::format_push_string
)]

//! Integration tests for xorb ingest → validate → decode round-trip.
//!
//! Tests exercise the public API as an external consumer:
//! - `store_uploaded_xorb_bytes` from `shardline_xet_adapter::ingest`
//! - `validate_serialized_xorb` from `shardline_xet_adapter::xorb`
//! - `decode_serialized_xorb_chunks` from `shardline_xet_adapter::xorb`
//!
//! Fixtures are built with `shardline_xet_core::xorb_object::xorb_format_test_utils`.

use std::io::Cursor;

use shardline_index::parse_xet_hash_hex;
use shardline_protocol::ShardlineHash;
use shardline_server_core::ServerObjectStore;
use shardline_xet_adapter::{
    decode_serialized_xorb_chunks, store_uploaded_xorb_bytes, validate_serialized_xorb,
};
use shardline_xet_core::{
    merklehash::compute_data_hash,
    xorb_object::{
        CompressionScheme, SerializedXorbObject,
        xorb_format_test_utils::{
            ChunkSize, build_raw_xorb, serialized_xorb_object_from_components,
        },
    },
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Build a serialized xorb with `num_chunks` chunks of `chunk_size` bytes each
/// using `build_raw_xorb` + `from_xorb_with_compression` (the same pattern
/// used by all existing unit tests).
///
/// Returns `(serialized_bytes, hash_hex, expected_hash)`.
fn build_xorb_simple(
    num_chunks: u32,
    chunk_size: u32,
    compression: CompressionScheme,
) -> (Vec<u8>, String, ShardlineHash) {
    let raw = build_raw_xorb(num_chunks, ChunkSize::Fixed(chunk_size));
    let serialized = SerializedXorbObject::from_xorb_with_compression(raw, compression, true)
        .expect("xorb serialization");
    let hash_hex = serialized.hash.hex();
    let shardline_hash = parse_xet_hash_hex(&hash_hex).expect("valid hash hex");
    (serialized.serialized_data, hash_hex, shardline_hash)
}

/// Build a serialized xorb with distinguishable chunk content so we can verify
/// content after the round-trip.  Uses `serialized_xorb_object_from_components`.
///
/// Returns `(serialized_bytes, hash_hex, expected_hash, original_chunks)`.
fn build_xorb_with_content(
    num_chunks: usize,
    chunk_size: usize,
    compression: CompressionScheme,
) -> (Vec<u8>, String, ShardlineHash, Vec<Vec<u8>>) {
    let mut all_data = Vec::with_capacity(num_chunks * chunk_size);
    let mut boundaries = Vec::with_capacity(num_chunks);
    let mut chunk_specs = Vec::with_capacity(num_chunks);
    let mut original_chunks = Vec::with_capacity(num_chunks);

    for i in 0..num_chunks {
        // Vary the byte pattern so no two chunks are identical.
        let chunk: Vec<u8> = (0..chunk_size).map(|j| ((i + j) & 0xFF) as u8).collect();
        let chunk_hash = compute_data_hash(&chunk);
        original_chunks.push(chunk.clone());
        all_data.extend_from_slice(&chunk);
        let boundary = u64::try_from((i + 1) * chunk_size).expect("boundary fits in u64");
        boundaries.push((chunk_hash, boundary));
        chunk_specs.push((
            chunk_hash,
            u64::try_from(chunk_size).expect("size fits in u64"),
        ));
    }

    let xorb_hash = {
        use shardline_xet_core::merklehash::xorb_hash;
        xorb_hash(&chunk_specs)
    };

    let SerializedXorbObject {
        serialized_data, ..
    } = serialized_xorb_object_from_components(&xorb_hash, all_data, boundaries, compression)
        .expect("test xorb fixture should build");

    let hash_hex = xorb_hash.hex();
    let shardline_hash = parse_xet_hash_hex(&hash_hex).expect("valid hash hex");

    (serialized_data, hash_hex, shardline_hash, original_chunks)
}

// ---------------------------------------------------------------------------
// full round-trip with content verification
// ---------------------------------------------------------------------------

#[test]
fn xorb_full_round_trip_content_verification() {
    let temp = TempDir::new().expect("tempdir");
    let object_store =
        ServerObjectStore::local(temp.path().join("objects")).expect("local object store");

    // Build a xorb with 3 distinguishable chunks.
    let (serialized, hash_hex, expected_hash, original_chunks) =
        build_xorb_with_content(3, 1024, CompressionScheme::LZ4);

    // --- store ---
    let response = store_uploaded_xorb_bytes(&object_store, &hash_hex, &serialized)
        .expect("store_uploaded_xorb_bytes should succeed");
    assert!(response.was_inserted, "xorb should be newly inserted");

    // --- validate ---
    let mut reader = Cursor::new(serialized.as_slice());
    let validated = validate_serialized_xorb(&mut reader, expected_hash)
        .expect("validate_serialized_xorb should succeed");

    assert_eq!(validated.hash(), expected_hash);
    assert_eq!(validated.chunks().len(), 3);
    assert_eq!(
        validated.unpacked_length(),
        u64::try_from(3 * 1024).expect("fits in u64"),
        "unpacked length should match total raw data"
    );

    // Each chunk's packed offsets should be monotonic.
    for window in validated.chunks().windows(2) {
        assert!(
            window[0].packed_end() <= window[1].packed_start(),
            "chunk boundaries must be monotonic"
        );
    }

    // --- decode ---
    let decoded = decode_serialized_xorb_chunks(&mut reader, &validated)
        .expect("decode_serialized_xorb_chunks should succeed");
    assert_eq!(decoded.len(), 3, "should decode exactly 3 chunks");

    // --- content verification ---
    for (i, decoded_chunk) in decoded.iter().enumerate() {
        assert_eq!(
            decoded_chunk.data(),
            original_chunks[i].as_slice(),
            "decoded chunk {i} content must match original"
        );
        assert_eq!(
            decoded_chunk.descriptor().hash(),
            validated.chunks()[i].hash(),
            "chunk {i} hash should match validated descriptor"
        );
    }
}

// ---------------------------------------------------------------------------
// idempotent store
// ---------------------------------------------------------------------------

#[test]
fn xorb_store_is_idempotent() {
    let temp = TempDir::new().expect("tempdir");
    let object_store =
        ServerObjectStore::local(temp.path().join("objects")).expect("local object store");

    let (serialized, hash_hex, _expected_hash) = build_xorb_simple(1, 256, CompressionScheme::None);

    let first = store_uploaded_xorb_bytes(&object_store, &hash_hex, &serialized)
        .expect("first store should succeed");
    assert!(first.was_inserted, "first store should insert");

    let second = store_uploaded_xorb_bytes(&object_store, &hash_hex, &serialized)
        .expect("second store should succeed");
    assert!(
        !second.was_inserted,
        "second store should report not inserted"
    );
}

// ---------------------------------------------------------------------------
// wrong hash rejection
// ---------------------------------------------------------------------------

#[test]
fn xorb_rejects_wrong_hash() {
    let temp = TempDir::new().expect("tempdir");
    let object_store =
        ServerObjectStore::local(temp.path().join("objects")).expect("local object store");

    let (serialized, _hash_hex, _expected_hash) =
        build_xorb_simple(1, 256, CompressionScheme::None);
    let wrong_hash = "00".repeat(32);

    let result = store_uploaded_xorb_bytes(&object_store, &wrong_hash, &serialized);
    assert!(result.is_err(), "store with wrong hash should be rejected");
}

// ---------------------------------------------------------------------------
// validation rejects corrupted bytes
// ---------------------------------------------------------------------------

#[test]
fn xorb_validation_rejects_corrupted_data() {
    let (_serialized, _hash_hex, expected_hash) = build_xorb_simple(2, 512, CompressionScheme::LZ4);
    let mut serialized = _serialized;

    // Corrupt a byte in the middle of the serialized payload.
    let mid = serialized.len() / 2;
    serialized[mid] = serialized[mid].wrapping_add(1);

    // Validation must fail.
    let mut reader = Cursor::new(serialized.as_slice());
    let result = validate_serialized_xorb(&mut reader, expected_hash);
    assert!(result.is_err(), "corrupted xorb validation must fail");
}

// ---------------------------------------------------------------------------
// validation rejects truncated data
// ---------------------------------------------------------------------------

#[test]
fn xorb_validation_rejects_truncated_data() {
    let (mut serialized, _hash_hex, expected_hash) =
        build_xorb_simple(1, 256, CompressionScheme::None);

    // Remove the last byte.
    serialized.pop();

    let mut reader = Cursor::new(serialized.as_slice());
    let result = validate_serialized_xorb(&mut reader, expected_hash);
    assert!(result.is_err(), "truncated xorb validation must fail");
}

// ---------------------------------------------------------------------------
// validation rejects empty / tiny buffers
// ---------------------------------------------------------------------------

#[test]
fn xorb_validation_rejects_empty_bytes() {
    let hash = ShardlineHash::from_bytes([0; 32]);
    let mut reader = Cursor::new(Vec::new());
    let result = validate_serialized_xorb(&mut reader, hash);
    assert!(result.is_err(), "empty bytes must fail validation");
}

#[test]
fn xorb_store_rejects_empty_body() {
    let temp = TempDir::new().expect("tempdir");
    let object_store =
        ServerObjectStore::local(temp.path().join("objects")).expect("local object store");

    let hash = "ab".repeat(32);
    let result = store_uploaded_xorb_bytes(&object_store, &hash, b"");
    assert!(
        result.is_err(),
        "store with empty body and valid hash must fail"
    );
}

// ---------------------------------------------------------------------------
// round-trip with no compression
// ---------------------------------------------------------------------------

#[test]
fn xorb_round_trip_no_compression() {
    let temp = TempDir::new().expect("tempdir");
    let object_store =
        ServerObjectStore::local(temp.path().join("objects")).expect("local object store");

    let (serialized, hash_hex, expected_hash) = build_xorb_simple(4, 128, CompressionScheme::None);

    let response = store_uploaded_xorb_bytes(&object_store, &hash_hex, &serialized)
        .expect("store should succeed");
    assert!(response.was_inserted);

    let mut reader = Cursor::new(serialized.as_slice());
    let validated =
        validate_serialized_xorb(&mut reader, expected_hash).expect("validation should succeed");
    assert_eq!(validated.chunks().len(), 4);

    let decoded =
        decode_serialized_xorb_chunks(&mut reader, &validated).expect("decode should succeed");
    assert_eq!(decoded.len(), 4);
    // Verify decoded data length matches chunk size.
    for chunk in &decoded {
        assert_eq!(chunk.data().len(), 128);
    }
}

// ---------------------------------------------------------------------------
// round-trip with ByteGrouping4LZ4
// ---------------------------------------------------------------------------

#[test]
fn xorb_round_trip_bg4lz4_compression() {
    let temp = TempDir::new().expect("tempdir");
    let object_store =
        ServerObjectStore::local(temp.path().join("objects")).expect("local object store");

    let (serialized, hash_hex, expected_hash) =
        build_xorb_simple(2, 256, CompressionScheme::ByteGrouping4LZ4);

    let response = store_uploaded_xorb_bytes(&object_store, &hash_hex, &serialized)
        .expect("store should succeed");
    assert!(response.was_inserted);

    let mut reader = Cursor::new(serialized.as_slice());
    let validated =
        validate_serialized_xorb(&mut reader, expected_hash).expect("validation should succeed");
    assert_eq!(validated.chunks().len(), 2);

    let decoded =
        decode_serialized_xorb_chunks(&mut reader, &validated).expect("decode should succeed");
    assert_eq!(decoded.len(), 2);
    // Each chunk should be 256 bytes when decompressed.
    for chunk in &decoded {
        assert_eq!(chunk.data().len(), 256);
    }
}

// ---------------------------------------------------------------------------
// decode correctly reports chunk metadata
// ---------------------------------------------------------------------------

#[test]
fn xorb_decode_reports_correct_chunk_metadata() {
    let (_serialized, _hash_hex, expected_hash) = build_xorb_simple(3, 512, CompressionScheme::LZ4);
    let serialized = _serialized;

    let mut reader = Cursor::new(serialized.as_slice());
    let validated =
        validate_serialized_xorb(&mut reader, expected_hash).expect("validation should succeed");

    let decoded =
        decode_serialized_xorb_chunks(&mut reader, &validated).expect("decode should succeed");

    // Check that descriptor metadata is consistent.
    for (i, chunk) in decoded.iter().enumerate() {
        let desc = chunk.descriptor();
        assert_eq!(
            desc.unpacked_len(),
            512,
            "chunk {i} should report correct unpacked length"
        );
        assert!(
            desc.packed_end() > desc.packed_start(),
            "chunk {i} packed range must be positive"
        );

        // The decoded data length must equal the unpacked length.
        assert_eq!(
            u64::try_from(chunk.data().len()).expect("len fits u64"),
            desc.unpacked_len(),
            "chunk {i} data length must match descriptor"
        );

        // Hash in descriptor must match actual data hash.
        let actual_hash = compute_data_hash(chunk.data());
        let expected_hash_from_desc = desc.hash();
        let actual_shardline =
            ShardlineHash::from_bytes(actual_hash.as_bytes().try_into().expect("32 bytes"));
        assert_eq!(
            expected_hash_from_desc, actual_shardline,
            "chunk {i} descriptor hash must match computed data hash"
        );
    }
}
