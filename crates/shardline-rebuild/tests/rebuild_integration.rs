//! Integration tests for `shardline-rebuild`.
//!
//! These tests exercise the public API — [`IndexRebuildReport`],
//! [`run_index_rebuild_with_stores`], and issue collection — against
//! filesystem-backed record stores, in-memory index stores, and blackhole
//! object stores.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::shadow_unrelated
)]

use shardline_index::{
    FileChunkRecord, FileRecord, LocalRecordStore, MemoryIndexStore, RecordMutation,
};
use shardline_rebuild::{IndexRebuildIssueDetail, IndexRebuildIssueKind, IndexRebuildReport};
use shardline_server_core::{ServerObjectStore, ShardMetadataLimits};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns a 64-character hex string for use as a content hash.
fn valid_hash() -> String {
    "a".repeat(64)
}

/// Returns a [`FileRecord`] with valid default fields.
fn test_record() -> FileRecord {
    FileRecord {
        file_id: "readme.md".to_owned(),
        content_hash: valid_hash(),
        total_bytes: 8,
        chunk_size: 8,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash: "b".repeat(64),
            offset: 0,
            length: 8,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 8,
        }],
    }
}

/// Creates a temporary directory and returns a [`LocalRecordStore`] rooted
/// inside it.
fn create_local_record_store(dir: &tempfile::TempDir) -> LocalRecordStore {
    LocalRecordStore::open(dir.path().to_path_buf())
}

/// Returns [`ShardMetadataLimits::default()`] (relaxed limits that accept
/// typical test data).
fn relaxed_limits() -> ShardMetadataLimits {
    ShardMetadataLimits::default()
}

// ---------------------------------------------------------------------------
// IndexRebuildReport — unit-like construction and query methods
// ---------------------------------------------------------------------------

#[test]
fn report_is_clean_when_no_issues() {
    let report = IndexRebuildReport {
        scanned_version_records: 42,
        scanned_retained_shards: 7,
        rebuilt_latest_records: 10,
        unchanged_latest_records: 30,
        removed_stale_latest_records: 2,
        scanned_reconstructions: 5,
        unchanged_reconstructions: 3,
        removed_stale_reconstructions: 2,
        rebuilt_dedupe_shard_mappings: 1,
        unchanged_dedupe_shard_mappings: 0,
        removed_stale_dedupe_shard_mappings: 0,
        preserved_latest_records_unreadable_version: Vec::new(),
        issues: Vec::new(),
    };
    assert!(report.is_clean());
    assert_eq!(report.issue_count(), 0);
}

#[test]
fn report_not_clean_when_issues_present() {
    let report = IndexRebuildReport {
        scanned_version_records: 1,
        scanned_retained_shards: 0,
        rebuilt_latest_records: 0,
        unchanged_latest_records: 0,
        removed_stale_latest_records: 0,
        scanned_reconstructions: 0,
        unchanged_reconstructions: 0,
        removed_stale_reconstructions: 0,
        rebuilt_dedupe_shard_mappings: 0,
        unchanged_dedupe_shard_mappings: 0,
        removed_stale_dedupe_shard_mappings: 0,
        preserved_latest_records_unreadable_version: Vec::new(),
        issues: vec![shardline_rebuild::IndexRebuildIssue {
            kind: IndexRebuildIssueKind::InvalidVersionRecordJson,
            location: "test-location".to_owned(),
            detail: IndexRebuildIssueDetail::RecordJsonInvalid,
        }],
    };
    assert!(!report.is_clean());
    assert_eq!(report.issue_count(), 1);
}

#[test]
fn report_counts_multiple_issues() {
    let issues = (0..5)
        .map(|i| shardline_rebuild::IndexRebuildIssue {
            kind: IndexRebuildIssueKind::InvalidVersionFileId,
            location: format!("loc-{i}"),
            detail: IndexRebuildIssueDetail::InvalidFileId {
                file_id: format!("bad-{i}"),
            },
        })
        .collect();
    let report = IndexRebuildReport {
        scanned_version_records: 5,
        scanned_retained_shards: 0,
        rebuilt_latest_records: 0,
        unchanged_latest_records: 0,
        removed_stale_latest_records: 0,
        scanned_reconstructions: 0,
        unchanged_reconstructions: 0,
        removed_stale_reconstructions: 0,
        rebuilt_dedupe_shard_mappings: 0,
        unchanged_dedupe_shard_mappings: 0,
        removed_stale_dedupe_shard_mappings: 0,
        preserved_latest_records_unreadable_version: Vec::new(),
        issues,
    };
    assert_eq!(report.issue_count(), 5);
    assert!(!report.is_clean());
}

// ---------------------------------------------------------------------------
// run_index_rebuild_with_stores — end-to-end integration
// ---------------------------------------------------------------------------

#[test]
fn rebuild_empty_store_returns_clean_report() {
    let dir = tempfile::tempdir().expect("tempdir");
    let record_store = create_local_record_store(&dir);
    let index_store = MemoryIndexStore::new();
    let object_store = ServerObjectStore::blackhole();

    let rt = tokio::runtime::Runtime::new().expect("tokio rt");
    let report = rt
        .block_on(shardline_rebuild::run_index_rebuild_with_stores(
            &record_store,
            &index_store,
            &object_store,
            relaxed_limits(),
        ))
        .expect("rebuild should succeed on empty store");

    assert!(report.is_clean(), "expected clean report: {report:?}");
    assert_eq!(report.scanned_version_records, 0);
    assert_eq!(report.scanned_retained_shards, 0);
    assert_eq!(report.rebuilt_latest_records, 0);
    assert_eq!(report.unchanged_latest_records, 0);
    assert_eq!(report.removed_stale_latest_records, 0);
    assert_eq!(report.removed_stale_reconstructions, 0);
}

#[test]
fn rebuild_with_valid_version_records_clean_report() {
    let dir = tempfile::tempdir().expect("tempdir");
    let record_store = create_local_record_store(&dir);
    let index_store = MemoryIndexStore::new();
    let object_store = ServerObjectStore::blackhole();

    // Insert a valid version record.
    let record = test_record();
    let rt = tokio::runtime::Runtime::new().expect("tokio rt");
    rt.block_on(record_store.write_version_record(&record))
        .expect("write version record");

    let report = rt
        .block_on(shardline_rebuild::run_index_rebuild_with_stores(
            &record_store,
            &index_store,
            &object_store,
            relaxed_limits(),
        ))
        .expect("rebuild should succeed");

    assert!(report.is_clean(), "expected clean report: {report:?}");
    assert_eq!(report.scanned_version_records, 1);
    assert_eq!(report.rebuilt_latest_records, 1);
    assert_eq!(
        report.unchanged_latest_records, 0,
        "latest record did not exist yet"
    );
}

#[test]
fn rebuild_idempotent_unchanged_latest_records() {
    let dir = tempfile::tempdir().expect("tempdir");
    let record_store = create_local_record_store(&dir);
    let index_store = MemoryIndexStore::new();
    let object_store = ServerObjectStore::blackhole();

    let record = test_record();
    let rt = tokio::runtime::Runtime::new().expect("tokio rt");

    // First run: rebuild creates the latest record.
    rt.block_on(record_store.write_version_record(&record))
        .expect("write version record");

    let report1 = rt
        .block_on(shardline_rebuild::run_index_rebuild_with_stores(
            &record_store,
            &index_store,
            &object_store,
            relaxed_limits(),
        ))
        .expect("first rebuild");
    assert_eq!(report1.rebuilt_latest_records, 1);

    // Second run: latest record already matches, should be unchanged.
    let report2 = rt
        .block_on(shardline_rebuild::run_index_rebuild_with_stores(
            &record_store,
            &index_store,
            &object_store,
            relaxed_limits(),
        ))
        .expect("second rebuild");
    assert_eq!(report2.unchanged_latest_records, 1);
    assert_eq!(report2.rebuilt_latest_records, 0);
    assert!(report2.is_clean());
}

#[test]
fn rebuild_with_invalid_version_record_detects_issue() {
    let dir = tempfile::tempdir().expect("tempdir");
    let record_store = create_local_record_store(&dir);
    let index_store = MemoryIndexStore::new();
    let object_store = ServerObjectStore::blackhole();

    // Write a record with an invalid file_id (path traversal).
    let bad_record = FileRecord {
        file_id: "../etc/passwd".to_owned(),
        ..test_record()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio rt");
    rt.block_on(record_store.write_version_record(&bad_record))
        .expect("write bad version record");

    let report = rt
        .block_on(shardline_rebuild::run_index_rebuild_with_stores(
            &record_store,
            &index_store,
            &object_store,
            relaxed_limits(),
        ))
        .expect("rebuild should not fail on issue");

    assert!(!report.is_clean(), "expected issues: {report:?}");
    assert_eq!(report.issue_count(), 1);
    assert_eq!(
        report.issues[0].kind,
        IndexRebuildIssueKind::InvalidVersionFileId
    );
}

#[test]
fn rebuild_with_multiple_valid_records_counts_them_all() {
    let dir = tempfile::tempdir().expect("tempdir");
    let record_store = create_local_record_store(&dir);
    let index_store = MemoryIndexStore::new();
    let object_store = ServerObjectStore::blackhole();

    let rt = tokio::runtime::Runtime::new().expect("tokio rt");

    for i in 0..5 {
        let record = FileRecord {
            file_id: format!("file-{i}.txt"),
            ..test_record()
        };
        rt.block_on(record_store.write_version_record(&record))
            .expect("write version record");
    }

    let report = rt
        .block_on(shardline_rebuild::run_index_rebuild_with_stores(
            &record_store,
            &index_store,
            &object_store,
            relaxed_limits(),
        ))
        .expect("rebuild should succeed");

    assert!(report.is_clean());
    assert_eq!(report.scanned_version_records, 5);
    assert_eq!(report.rebuilt_latest_records, 5);
}

#[test]
fn rebuild_with_mixed_valid_invalid_records() {
    let dir = tempfile::tempdir().expect("tempdir");
    let record_store = create_local_record_store(&dir);
    let index_store = MemoryIndexStore::new();
    let object_store = ServerObjectStore::blackhole();

    let rt = tokio::runtime::Runtime::new().expect("tokio rt");

    // Valid record.
    let valid = test_record();
    rt.block_on(record_store.write_version_record(&valid))
        .expect("write valid version record");

    // Invalid record (bad content hash).
    let bad_hash = FileRecord {
        file_id: "bad-hash.bin".to_owned(),
        content_hash: "not-a-valid-hex".to_owned(),
        ..test_record()
    };
    rt.block_on(record_store.write_version_record(&bad_hash))
        .expect("write bad-hash version record");

    let report = rt
        .block_on(shardline_rebuild::run_index_rebuild_with_stores(
            &record_store,
            &index_store,
            &object_store,
            relaxed_limits(),
        ))
        .expect("rebuild should succeed");

    assert!(!report.is_clean(), "expected issues: {report:?}");
    assert_eq!(report.issue_count(), 1);
    assert_eq!(
        report.issues[0].kind,
        IndexRebuildIssueKind::InvalidVersionContentHash
    );
    // Only the valid candidate should result in a rebuilt latest record.
    assert_eq!(report.rebuilt_latest_records, 1);
    assert_eq!(report.scanned_version_records, 2);
}

// ---------------------------------------------------------------------------
// Edge cases: corrupted records (non-JSON payload)
// ---------------------------------------------------------------------------

#[test]
fn rebuild_with_corrupted_json_detected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_path_buf();
    let record_store = create_local_record_store(&dir);
    let index_store = MemoryIndexStore::new();
    let object_store = ServerObjectStore::blackhole();

    let rt = tokio::runtime::Runtime::new().expect("tokio rt");

    // Insert a valid init record to initialise the database.
    let init = FileRecord {
        file_id: "init".to_owned(),
        ..test_record()
    };
    rt.block_on(record_store.write_version_record(&init))
        .expect("write init version record");

    // Open the SQLite database directly and insert a record with
    // non-JSON bytes (corrupted payload).
    let conn = open_db(&root);
    let bad_payload = b"this is not valid json at all {{}}";
    let file_id = "corrupted.bin";
    let content_hash_val = valid_hash();
    let record_key = format!("7:version8:6:global13:{}64:{}", file_id, content_hash_val);
    let scope_key = "6:global";
    conn.execute(
        "INSERT INTO shardline_file_records
            (record_key, record_kind, scope_key, file_id, content_hash, record, updated_at_unix_seconds)
         VALUES (?1, 'version', ?2, ?3, ?4, ?5, 1000)",
        rusqlite::params![&record_key, scope_key, file_id, &content_hash_val, &bad_payload[..]],
    )
    .expect("insert corrupted record");

    let report = rt
        .block_on(shardline_rebuild::run_index_rebuild_with_stores(
            &record_store,
            &index_store,
            &object_store,
            relaxed_limits(),
        ))
        .expect("rebuild should succeed");

    assert!(!report.is_clean(), "expected issues: {report:?}");
    assert_eq!(report.issue_count(), 1);
    assert_eq!(
        report.issues[0].kind,
        IndexRebuildIssueKind::InvalidVersionRecordJson
    );
    // Only the init record should have been rebuilt.
    assert_eq!(report.rebuilt_latest_records, 1);
}

/// Helper: open the SQLite DB that backs a [`LocalRecordStore`].
fn open_db(root: &std::path::Path) -> rusqlite::Connection {
    rusqlite::Connection::open(root.join("metadata.sqlite3"))
        .expect("failed to open metadata sqlite3 database")
}
