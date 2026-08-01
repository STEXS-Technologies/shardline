//! Integration tests for `shardline-fsck`.
//!
//! These tests exercise the public API (`run_fsck_with_stores`, `run_local_fsck`)
//! against in-memory and filesystem-backed stores in various states:
//!
//! - Empty stores → clean report
//! - Valid records with matching chunks → clean report
//! - Missing chunks → detected as `MissingChunk`
//! - Corrupted chunk data → detected as `ChunkHashMismatch`
//! - Chunk length mismatch → detected as `ChunkLengthMismatch`
//! - Content hash mismatch → detected as `RecordHashMismatch`
//! - Missing version record → detected as `MissingVersionRecord`
//! - Mismatched version record → detected as `MismatchedVersionRecord`
//! - Invalid reconstruction plan → detected as `NonContiguousChunks`
//! - `run_local_fsck` on an empty directory → clean

// Test code — allow panicky helpers that are standard in test fixtures.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::shadow_unrelated
)]

use std::path::PathBuf;

use shardline_fsck::{FsckIssueKind, run_fsck_with_stores, run_local_fsck};
use shardline_index::{
    FileChunkRecord, FileRecord, LocalRecordStore, MemoryIndexStore, RecordMutation,
    xet_hash_hex_string,
};
use shardline_server_core::{
    DEFAULT_SHARD_METADATA_LIMITS, ServerObjectStore, chunk_hash, chunk_object_key, content_hash,
};

// ============================================================================
// Helpers
// ============================================================================

/// Creates a temporary directory with an empty record store, index store,
/// and object store ready for use in tests.
struct TestFixture {
    _root: PathBuf,
    /// The chunks subdirectory (object store root).
    object_root: PathBuf,
    /// Filesystem-backed record store.
    record_store: LocalRecordStore,
    /// In-memory index store (dedupe, reconstruction, lifecycle metadata).
    index_store: MemoryIndexStore,
    /// Local filesystem object store.
    object_store: ServerObjectStore,
}

impl TestFixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().to_path_buf();
        let object_root = root.join("chunks");
        std::fs::create_dir_all(&object_root).expect("create chunks dir");
        let record_store = LocalRecordStore::open(root.clone());
        let index_store = MemoryIndexStore::new();
        let object_store =
            ServerObjectStore::local(object_root.clone()).expect("create local object store");
        Self {
            _root: root,
            object_root,
            record_store,
            index_store,
            object_store,
        }
    }

    /// Writes a chunk object directly to the filesystem at the path dictated by its hash.
    fn write_chunk(&self, data: &[u8]) -> String {
        let hash = chunk_hash(data);
        let hex = xet_hash_hex_string(hash);
        let key = chunk_object_key(&hex).expect("chunk key");
        let path = self.object_root.join(key.as_str());
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create chunk parent dir");
        }
        std::fs::write(&path, data).expect("write chunk file");
        hex
    }

    /// Runs fsck and returns the report.
    async fn run_fsck(&self) -> shardline_fsck::FsckReport {
        run_fsck_with_stores(
            &self.record_store,
            &self.index_store,
            &self.object_root,
            &self.object_store,
            DEFAULT_SHARD_METADATA_LIMITS,
        )
        .await
        .expect("fsck should succeed")
    }

    /// Runs fsck and asserts the report is clean, returning it for further inspection.
    async fn assert_clean(&self) -> shardline_fsck::FsckReport {
        let report = self.run_fsck().await;
        assert!(
            report.is_clean(),
            "expected clean report, got {} issues: {:#?}",
            report.issue_count(),
            report.issues
        );
        report
    }

    /// Runs fsck and asserts at least one issue of the given kind exists.
    async fn assert_has_issue(&self, kind: FsckIssueKind) -> shardline_fsck::FsckReport {
        let report = self.run_fsck().await;
        assert!(
            report.issues.iter().any(|i| i.kind == kind),
            "expected at least one {kind:?} issue, got: {:#?}",
            report.issues,
        );
        report
    }
}

/// Creates a simple single-chunk file record.
fn make_record(
    file_id: &str,
    chunk_hash_hex: &str,
    chunk_data: &[u8],
    extra_chunks: Vec<FileChunkRecord>,
) -> FileRecord {
    let mut chunks = vec![FileChunkRecord {
        hash: chunk_hash_hex.to_owned(),
        offset: 0,
        length: chunk_data.len() as u64,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: chunk_data.len() as u64,
    }];
    chunks.extend(extra_chunks);

    let total_bytes: u64 = chunks.iter().map(|c| c.length).sum();
    let chunk_size = 4096;
    let ch = content_hash(total_bytes, chunk_size, &chunks);

    FileRecord {
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        file_id: file_id.to_owned(),
        content_hash: ch,
        total_bytes,
        chunk_size,
        repository_scope: None,
        chunks,
    }
}

/// Writes both a latest and version record for a file.
async fn write_latest_and_version(record_store: &LocalRecordStore, record: &FileRecord) {
    RecordMutation::write_version_record(record_store, record)
        .await
        .expect("write version record");
    RecordMutation::write_latest_record(record_store, record)
        .await
        .expect("write latest record");
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn empty_store_returns_clean() {
    let fixture = TestFixture::new();
    let report = fixture.assert_clean().await;
    assert_eq!(report.latest_records, 0);
    assert_eq!(report.version_records, 0);
    assert_eq!(report.inspected_chunk_references, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn store_with_valid_records_returns_clean() {
    let fixture = TestFixture::new();

    // Write a chunk and a record that matches it
    let chunk_data = b"hello fsck integration test";
    let chunk_hash_hex = fixture.write_chunk(chunk_data);
    let record = make_record("valid-file-1", &chunk_hash_hex, chunk_data, vec![]);
    write_latest_and_version(&fixture.record_store, &record).await;

    let report = fixture.assert_clean().await;
    assert_eq!(report.latest_records, 1);
    assert_eq!(report.version_records, 1);
    assert_eq!(report.inspected_chunk_references, 2); // latest + version
}

#[tokio::test(flavor = "multi_thread")]
async fn store_with_multiple_valid_records_returns_clean() {
    let fixture = TestFixture::new();

    for i in 0..3 {
        let chunk_data = format!("multi-valid-record-chunk-{i}");
        let chunk_hash_hex = fixture.write_chunk(chunk_data.as_bytes());
        let record = make_record(
            &format!("valid-file-{i}"),
            &chunk_hash_hex,
            chunk_data.as_bytes(),
            vec![],
        );
        write_latest_and_version(&fixture.record_store, &record).await;
    }

    let report = fixture.assert_clean().await;
    assert_eq!(report.latest_records, 3);
    assert_eq!(report.version_records, 3);
    assert_eq!(report.inspected_chunk_references, 6);
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_chunk_is_detected() {
    let fixture = TestFixture::new();

    // Write a record referencing a chunk that does NOT exist on disk
    let chunk_data = b"data for missing chunk test";
    let hash = chunk_hash(chunk_data);
    let chunk_hash_hex = xet_hash_hex_string(hash);
    let record = make_record("missing-chunk-file", &chunk_hash_hex, chunk_data, vec![]);
    write_latest_and_version(&fixture.record_store, &record).await;

    // Note: we did NOT call fixture.write_chunk(), so the chunk file is missing
    let report = fixture.assert_has_issue(FsckIssueKind::MissingChunk).await;
    assert_eq!(report.latest_records, 1);
    assert_eq!(report.version_records, 1);
    // Each record references 1 chunk → 2 references
    assert_eq!(report.inspected_chunk_references, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn corrupted_chunk_data_detected_as_hash_mismatch() {
    let fixture = TestFixture::new();

    // Write a chunk with original data, but replace it with different data
    let original = b"original correct data";
    let hash = chunk_hash(original);
    let chunk_hash_hex = xet_hash_hex_string(hash);

    // Write the chunk key with *different* data
    let key = chunk_object_key(&chunk_hash_hex).expect("chunk key");
    let path = fixture.object_root.join(key.as_str());
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent");
    }
    std::fs::write(&path, b"corrupted data that does not match the hash")
        .expect("write corrupted chunk");

    let record = make_record("corrupted-chunk-file", &chunk_hash_hex, original, vec![]);
    write_latest_and_version(&fixture.record_store, &record).await;

    let report = fixture
        .assert_has_issue(FsckIssueKind::ChunkHashMismatch)
        .await;
    assert_eq!(report.inspected_chunk_references, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn chunk_length_mismatch_detected() {
    let fixture = TestFixture::new();

    // Write a chunk whose actual length is different from what the record declares
    let chunk_data = b"this chunk is 33 bytes long";
    let chunk_hash_hex = fixture.write_chunk(chunk_data);

    // Record says the chunk is 100 bytes (but it's actually 33)
    let mut record = make_record("length-mismatch-file", &chunk_hash_hex, chunk_data, vec![]);
    record.chunks[0].length = 100;
    record.total_bytes = 100;
    // content_hash is now wrong too, but that's a separate issue — we test length mismatch
    write_latest_and_version(&fixture.record_store, &record).await;

    let report = fixture
        .assert_has_issue(FsckIssueKind::ChunkLengthMismatch)
        .await;
    assert_eq!(report.inspected_chunk_references, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn content_hash_mismatch_detected() {
    let fixture = TestFixture::new();

    let chunk_data = b"content hash mismatch test data";
    let chunk_hash_hex = fixture.write_chunk(chunk_data);

    // Record has a deliberately wrong content_hash
    let mut record = make_record("content-hash-mismatch", &chunk_hash_hex, chunk_data, vec![]);
    record.content_hash = "a".repeat(64);

    write_latest_and_version(&fixture.record_store, &record).await;

    let report = fixture
        .assert_has_issue(FsckIssueKind::RecordHashMismatch)
        .await;
    assert_eq!(report.inspected_chunk_references, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_version_record_detected() {
    let fixture = TestFixture::new();

    // Only write a latest record, skip the version record
    let chunk_data = b"data for missing version test";
    let chunk_hash_hex = fixture.write_chunk(chunk_data);
    let record = make_record("missing-version-file", &chunk_hash_hex, chunk_data, vec![]);
    RecordMutation::write_latest_record(&fixture.record_store, &record)
        .await
        .expect("write latest record");

    let report = fixture
        .assert_has_issue(FsckIssueKind::MissingVersionRecord)
        .await;
    assert_eq!(report.latest_records, 1);
    assert_eq!(report.version_records, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn mismatched_version_record_detected() {
    let fixture = TestFixture::new();

    // We need both records to have the SAME content_hash so the version-record
    // locator (derived from the latest record) resolves.  Differing total_bytes
    // then triggers a MismatchedVersionRecord instead of MissingVersionRecord.
    let chunk_data = b"shared chunk data for matching locator";
    let chunk_hash_hex = fixture.write_chunk(chunk_data);

    // Version record uses 1 chunk with the correct total_bytes.
    let version_record = FileRecord {
        file_id: "mismatch-file".to_owned(),
        content_hash: "b".repeat(64), // will be overridden below
        total_bytes: chunk_data.len() as u64,
        chunk_size: 4096,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash: chunk_hash_hex.clone(),
            offset: 0,
            length: chunk_data.len() as u64,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: chunk_data.len() as u64,
        }],
    };
    // Compute the real content_hash
    let content_hash_val = content_hash(
        version_record.total_bytes,
        version_record.chunk_size,
        &version_record.chunks,
    );

    let version_record = FileRecord {
        content_hash: content_hash_val.clone(),
        ..version_record
    };
    RecordMutation::write_version_record(&fixture.record_store, &version_record)
        .await
        .expect("write version record");

    // Latest record has the SAME file_id and content_hash but DIFFERENT total_bytes
    let latest_record = FileRecord {
        file_id: "mismatch-file".to_owned(),
        content_hash: content_hash_val.clone(), // same as version
        total_bytes: 9999,                      // different from version
        chunk_size: 4096,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash: chunk_hash_hex,
            offset: 0,
            length: chunk_data.len() as u64, // actual chunk length is 33, not 9999
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: chunk_data.len() as u64,
        }],
    };
    RecordMutation::write_latest_record(&fixture.record_store, &latest_record)
        .await
        .expect("write latest record");

    let _report = fixture
        .assert_has_issue(FsckIssueKind::MismatchedVersionRecord)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn non_contiguous_chunks_detected_via_reconstruction_plan() {
    let fixture = TestFixture::new();

    // Write two chunks
    let chunk1 = b"first chunk data";
    let hash1 = fixture.write_chunk(chunk1);
    let chunk2 = b"second chunk data";
    let hash2 = fixture.write_chunk(chunk2);

    // Create record where chunk offsets are non-contiguous (both start at 0)
    let chunks = vec![
        FileChunkRecord {
            hash: hash1.clone(),
            offset: 0,
            length: chunk1.len() as u64,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: chunk1.len() as u64,
        },
        FileChunkRecord {
            hash: hash2.clone(),
            offset: 0, // BUG: should be chunk1.len()
            length: chunk2.len() as u64,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: chunk2.len() as u64,
        },
    ];
    let total_bytes: u64 = chunks.iter().map(|c| c.length).sum();
    let record = FileRecord {
        file_id: "non-contiguous-file".to_owned(),
        content_hash: "b".repeat(64),
        total_bytes,
        chunk_size: 4096,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks,
    };

    write_latest_and_version(&fixture.record_store, &record).await;

    let _report = fixture
        .assert_has_issue(FsckIssueKind::NonContiguousChunks)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_local_fsck_on_empty_dir_returns_clean() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().to_path_buf();

    let report = run_local_fsck(root).await.expect("run_local_fsck");
    assert!(
        report.is_clean(),
        "expected clean report from empty dir, got {} issues",
        report.issue_count()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn run_local_fsck_on_non_existent_dir_errors() {
    let root = PathBuf::from("/nonexistent-fsck-integration-test-dir");

    let result = run_local_fsck(root).await;
    assert!(result.is_err(), "expected error for non-existent directory");
}

#[tokio::test(flavor = "multi_thread")]
async fn store_with_invalid_chunk_hash_detected() {
    let fixture = TestFixture::new();

    // Record has an invalid (non-hex) chunk hash
    let record = FileRecord {
        file_id: "invalid-hash-file".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 10,
        chunk_size: 4096,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash: "not-a-valid-hex-hash!!!!".to_owned(),
            offset: 0,
            length: 10,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 10,
        }],
    };
    write_latest_and_version(&fixture.record_store, &record).await;

    let report = fixture
        .assert_has_issue(FsckIssueKind::InvalidContentHash)
        .await;
    // Invalid hash is caught before chunk inspection
    assert_eq!(report.inspected_chunk_references, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn mixed_valid_and_corrupted_records_reports_correct_counts() {
    let fixture = TestFixture::new();

    // Valid record 1
    let data1 = b"valid record one";
    let hash1 = fixture.write_chunk(data1);
    let r1 = make_record("valid-1", &hash1, data1, vec![]);
    write_latest_and_version(&fixture.record_store, &r1).await;

    // Valid record 2
    let data2 = b"valid record two";
    let hash2 = fixture.write_chunk(data2);
    let r2 = make_record("valid-2", &hash2, data2, vec![]);
    write_latest_and_version(&fixture.record_store, &r2).await;

    // Corrupted record (missing chunk)
    let data3 = b"missing chunk data";
    let hash3 = chunk_hash(data3);
    let hash3_hex = xet_hash_hex_string(hash3);
    let r3 = make_record("missing-chunk-3", &hash3_hex, data3, vec![]);
    write_latest_and_version(&fixture.record_store, &r3).await;

    let report = fixture.run_fsck().await;
    assert_eq!(report.latest_records, 3);
    assert_eq!(report.version_records, 3);
    // 4 valid chunk refs (2 records × 2 kinds) + 2 missing refs
    assert_eq!(report.inspected_chunk_references, 6);

    let missing_count = report
        .issues
        .iter()
        .filter(|i| i.kind == FsckIssueKind::MissingChunk)
        .count();
    assert_eq!(
        missing_count, 2,
        "expected 2 MissingChunk issues (one per latest+version)"
    );
}
