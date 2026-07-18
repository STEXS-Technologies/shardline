#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use,
    clippy::format_push_string
)]

//! Integration tests for shard upload round-trip.
//!
//! Tests exercise the public API as an external consumer:
//! - `store_uploaded_xorb_bytes` from `shardline_xet_adapter::ingest`
//! - `register_uploaded_shard_bytes` from `shardline_xet_adapter::ingest`
//! - `retained_shard_chunk_hashes` from `shardline_xet_adapter::shard_store`
//!
//! Fixtures are built with `shardline_xet_core` metadata-shard and xorb types.

use shardline_protocol::{RepositoryProvider, RepositoryScope};
use shardline_server_core::{DEFAULT_SHARD_METADATA_LIMITS, ServerObjectStore};
use shardline_xet_adapter::{
    register_uploaded_shard_bytes, retained_shard_chunk_hashes, store_uploaded_xorb_bytes,
};
use shardline_xet_core::{
    merklehash::{compute_data_hash, file_hash, xorb_hash},
    metadata_shard::{
        file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
        shard_format::MDBShardInfo,
        shard_in_memory::MDBInMemoryShard,
        xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
    },
    xorb_object::{
        CompressionScheme, SerializedXorbObject,
        xorb_format_test_utils::{ChunkSize, build_raw_xorb},
    },
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Serialize a shard from file info and xorb info vectors.
fn serialize_shard(file_infos: Vec<MDBFileInfo>, xorb_infos: Vec<MDBXorbInfo>) -> Vec<u8> {
    let mut shard = MDBInMemoryShard::default();
    for file_info in file_infos {
        shard
            .add_file_reconstruction_info(file_info)
            .expect("add_file_reconstruction_info");
    }
    for xorb_info in xorb_infos {
        shard.add_xorb_block(xorb_info).expect("add_xorb_block");
    }
    let mut serialized = Vec::new();
    MDBShardInfo::serialize_from(&mut serialized, &shard, None).expect("shard serialization");
    serialized
}

/// Build and store a xorb, returning the xorb hash.
fn store_simple_xorb(
    object_store: &ServerObjectStore,
    num_chunks: u32,
    chunk_size: u32,
) -> shardline_xet_core::merklehash::MerkleHash {
    let raw = build_raw_xorb(num_chunks, ChunkSize::Fixed(chunk_size));
    let serialized =
        SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
            .expect("xorb serialization");
    let hash_hex = serialized.hash.hex();
    store_uploaded_xorb_bytes(object_store, &hash_hex, &serialized.serialized_data)
        .expect("store xorb for shard test");
    serialized.hash
}

// ---------------------------------------------------------------------------
// shard upload round-trip
// ---------------------------------------------------------------------------

#[test]
fn shard_upload_round_trip() {
    let temp = TempDir::new().expect("tempdir");
    let object_store =
        ServerObjectStore::local(temp.path().join("objects")).expect("local object store");

    // 1. Store a xorb so the shard parser can look up chunk range info.
    let xorb_hash = store_simple_xorb(&object_store, 2, 512);

    // 2. Build a single-file shard referencing that xorb.
    let chunk_hash = compute_data_hash(b"test-chunk-data");
    let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
    let shard_bytes = serialize_shard(
        vec![MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash, 1_usize, false, false),
            segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u32, 0_u32, 1_u32)],
            verification: Vec::new(),
            metadata_ext: None,
        }],
        vec![MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u32, 1_u32),
            chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u32, 0_u32)],
        }],
    );

    // 3. Register the shard.
    let mut committed_records = None;
    let mut committed_mappings = None;

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let response = rt
        .block_on(register_uploaded_shard_bytes(
            &object_store,
            &shard_bytes,
            None,
            DEFAULT_SHARD_METADATA_LIMITS,
            |records, mappings| {
                committed_records = Some(records);
                committed_mappings = Some(mappings);
                async { Ok(()) }
            },
        ))
        .expect("register_uploaded_shard_bytes should succeed");

    assert_eq!(response.result, 1, "shard should be newly inserted");

    // 4. Verify retained chunk hashes match expectations.
    let retained = retained_shard_chunk_hashes(&shard_bytes, DEFAULT_SHARD_METADATA_LIMITS)
        .expect("retained_shard_chunk_hashes should succeed");
    assert!(
        retained.contains(&chunk_hash.hex()),
        "retained hashes should contain the file-start chunk hash"
    );

    // 5. Verify commit received expected data.
    let records = committed_records.expect("commit closure should have been called");
    assert_eq!(records.len(), 1, "should commit one file record");
    assert_eq!(
        records[0].chunks.len(),
        1,
        "file record should have one chunk"
    );
    assert!(
        !records[0].file_id.is_empty(),
        "file_id should be populated"
    );

    let mappings = committed_mappings.expect("commit closure should have received mappings");
    assert!(
        !mappings.is_empty(),
        "should have at least one dedupe mapping"
    );
}

// ---------------------------------------------------------------------------
// multi-file shard
// ---------------------------------------------------------------------------

#[test]
fn shard_upload_multi_file() {
    let temp = TempDir::new().expect("tempdir");
    let object_store =
        ServerObjectStore::local(temp.path().join("objects")).expect("local object store");

    // Store one xorb that both files will reference.
    let xorb_hash = store_simple_xorb(&object_store, 3, 256);

    let chunk_a_hash = compute_data_hash(b"file-a-data");
    let chunk_b_hash = compute_data_hash(b"file-b-data");

    let file_a_hash = file_hash(&[(chunk_a_hash, 1_u64)]);
    let file_b_hash = file_hash(&[(chunk_b_hash, 1_u64)]);

    let shard_bytes = serialize_shard(
        vec![
            // File A references chunk index [0, 1) → chunk 0
            MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_a_hash, 1_usize, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u32, 0_u32, 1_u32)],
                verification: Vec::new(),
                metadata_ext: None,
            },
            // File B references chunk index [1, 2) → chunk 1
            MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_b_hash, 1_usize, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u32, 1_u32, 2_u32)],
                verification: Vec::new(),
                metadata_ext: None,
            },
        ],
        vec![MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(xorb_hash, 2_u32, 2_u32),
            chunks: vec![
                XorbChunkSequenceEntry::new(chunk_a_hash, 1_u32, 0_u32),
                XorbChunkSequenceEntry::new(chunk_b_hash, 1_u32, 0_u32),
            ],
        }],
    );

    let mut file_count = 0_usize;
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let response = rt
        .block_on(register_uploaded_shard_bytes(
            &object_store,
            &shard_bytes,
            None,
            DEFAULT_SHARD_METADATA_LIMITS,
            |records, _mappings| {
                file_count = records.len();
                async { Ok(()) }
            },
        ))
        .expect("multi-file shard registration should succeed");

    assert_eq!(response.result, 1, "shard should be newly inserted");
    assert_eq!(file_count, 2, "should commit two file records");

    let retained = retained_shard_chunk_hashes(&shard_bytes, DEFAULT_SHARD_METADATA_LIMITS)
        .expect("retained hashes");
    // Both chunks are file-start chunks (different chunk_index_start).
    assert_eq!(retained.len(), 2, "both chunks should be retained");
    assert!(retained.contains(&chunk_a_hash.hex()));
    assert!(retained.contains(&chunk_b_hash.hex()));
}

// ---------------------------------------------------------------------------
// shard upload with repository scope
// ---------------------------------------------------------------------------

#[test]
fn shard_upload_with_repository_scope() {
    let temp = TempDir::new().expect("tempdir");
    let object_store =
        ServerObjectStore::local(temp.path().join("objects")).expect("local object store");

    let xorb_hash = store_simple_xorb(&object_store, 1, 256);

    let chunk_hash = compute_data_hash(b"scoped-data");
    let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
    let shard_bytes = serialize_shard(
        vec![MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash, 1_usize, false, false),
            segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u32, 0_u32, 1_u32)],
            verification: Vec::new(),
            metadata_ext: None,
        }],
        vec![MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u32, 1_u32),
            chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u32, 0_u32)],
        }],
    );

    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "test-owner", "test-repo", None)
        .expect("valid RepositoryScope");

    let mut committed_scope: Option<Option<RepositoryScope>> = None;
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let response = rt
        .block_on(register_uploaded_shard_bytes(
            &object_store,
            &shard_bytes,
            Some(&scope),
            DEFAULT_SHARD_METADATA_LIMITS,
            |records, _mappings| {
                committed_scope = Some(records[0].repository_scope.clone());
                async { Ok(()) }
            },
        ))
        .expect("register with scope should succeed");

    assert_eq!(response.result, 1, "shard should be newly inserted");
    let actual_scope = committed_scope.expect("commit should have been called");
    assert_eq!(
        actual_scope,
        Some(scope),
        "file record should carry the repository scope"
    );
}

// ---------------------------------------------------------------------------
// shard rejects empty bytes
// ---------------------------------------------------------------------------

#[test]
fn shard_upload_rejects_empty_bytes() {
    let temp = TempDir::new().expect("tempdir");
    let object_store =
        ServerObjectStore::local(temp.path().join("objects")).expect("local object store");

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let result = rt.block_on(register_uploaded_shard_bytes(
        &object_store,
        b"",
        None,
        DEFAULT_SHARD_METADATA_LIMITS,
        |_records, _mappings| async { Ok(()) },
    ));

    assert!(result.is_err(), "register with empty shard must fail");
}

// ---------------------------------------------------------------------------
// shard rejects missing referenced xorb
// ---------------------------------------------------------------------------

#[test]
fn shard_upload_rejects_missing_referenced_xorb() {
    let temp = TempDir::new().expect("tempdir");
    let object_store =
        ServerObjectStore::local(temp.path().join("objects")).expect("local object store");

    // Build a shard referencing a xorb hash that has never been stored.
    let xorb_hash = xorb_hash(&[(compute_data_hash(b"missing"), 1_u64)]);
    let chunk_hash = compute_data_hash(b"orphan");
    let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
    let shard_bytes = serialize_shard(
        vec![MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash, 1_usize, false, false),
            segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u32, 0_u32, 1_u32)],
            verification: Vec::new(),
            metadata_ext: None,
        }],
        vec![MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u32, 1_u32),
            chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u32, 0_u32)],
        }],
    );

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let result = rt.block_on(register_uploaded_shard_bytes(
        &object_store,
        &shard_bytes,
        None,
        DEFAULT_SHARD_METADATA_LIMITS,
        |_records, _mappings| async { Ok(()) },
    ));

    assert!(
        result.is_err(),
        "shard referencing a missing xorb must fail"
    );
}

// ---------------------------------------------------------------------------
// shard idempotent store
// ---------------------------------------------------------------------------

#[test]
fn shard_upload_is_idempotent() {
    let temp = TempDir::new().expect("tempdir");
    let object_store =
        ServerObjectStore::local(temp.path().join("objects")).expect("local object store");

    let xorb_hash = store_simple_xorb(&object_store, 1, 256);

    let chunk_hash = compute_data_hash(b"idempotent-test");
    let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
    let shard_bytes = serialize_shard(
        vec![MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash, 1_usize, false, false),
            segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u32, 0_u32, 1_u32)],
            verification: Vec::new(),
            metadata_ext: None,
        }],
        vec![MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u32, 1_u32),
            chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u32, 0_u32)],
        }],
    );

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    // First registration.
    let first = rt
        .block_on(register_uploaded_shard_bytes(
            &object_store,
            &shard_bytes,
            None,
            DEFAULT_SHARD_METADATA_LIMITS,
            |_records, _mappings| async { Ok(()) },
        ))
        .expect("first shard registration");
    assert_eq!(first.result, 1, "first registration should be new");

    // Second registration of identical shard.
    let second = rt
        .block_on(register_uploaded_shard_bytes(
            &object_store,
            &shard_bytes,
            None,
            DEFAULT_SHARD_METADATA_LIMITS,
            |_records, _mappings| async { Ok(()) },
        ))
        .expect("second shard registration");
    assert_eq!(
        second.result, 0,
        "second registration should report already exists"
    );
}

// ---------------------------------------------------------------------------
// retained_shard_chunk_hashes as standalone parse
// ---------------------------------------------------------------------------

#[test]
fn retained_shard_chunk_hashes_standalone() {
    // This test verifies the public `retained_shard_chunk_hashes` function
    // works as a pure parser (no object store needed).
    let chunk_hash = compute_data_hash(b"standalone-chunk");
    let xorb_hash = xorb_hash(&[(chunk_hash, 1_u64)]);
    let file_hash = file_hash(&[(chunk_hash, 1_u64)]);

    let shard_bytes = serialize_shard(
        vec![MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash, 1_usize, false, false),
            segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u32, 0_u32, 1_u32)],
            verification: Vec::new(),
            metadata_ext: None,
        }],
        vec![MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u32, 1_u32),
            chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u32, 0_u32)],
        }],
    );

    let hashes = retained_shard_chunk_hashes(&shard_bytes, DEFAULT_SHARD_METADATA_LIMITS)
        .expect("retained_shard_chunk_hashes should succeed");
    assert_eq!(hashes.len(), 1, "one chunk should be retained");
    assert_eq!(hashes[0], chunk_hash.hex(), "retained hash must match");
}

// ---------------------------------------------------------------------------
// retained_shard_chunk_hashes rejects empty bytes
// ---------------------------------------------------------------------------

#[test]
fn retained_shard_chunk_hashes_rejects_empty() {
    let result = retained_shard_chunk_hashes(b"", DEFAULT_SHARD_METADATA_LIMITS);
    assert!(
        result.is_err(),
        "retained_shard_chunk_hashes on empty bytes must fail"
    );
}
