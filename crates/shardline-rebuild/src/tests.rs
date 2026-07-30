use std::collections::HashSet;

use shardline_index::AsyncIndexStore;
use shardline_server_core::ServerObjectStore;

use super::*;

fn empty_report() -> IndexRebuildReport {
    IndexRebuildReport {
        scanned_version_records: 0,
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
        issues: Vec::new(),
    }
}

#[test]
fn report_is_clean_when_no_issues() {
    let report = empty_report();
    assert!(report.is_clean());
    assert_eq!(report.issue_count(), 0);
}

#[test]
fn report_is_not_clean_with_issues() {
    let mut report = empty_report();
    report.issues.push(IndexRebuildIssue {
        kind: IndexRebuildIssueKind::InvalidVersionRecordJson,
        location: "test/path".to_owned(),
        detail: IndexRebuildIssueDetail::RecordJsonInvalid,
    });
    assert!(!report.is_clean());
    assert_eq!(report.issue_count(), 1);
}

#[test]
fn issue_count_matches_vec_len() {
    let mut report = empty_report();
    for i in 0..5 {
        report.issues.push(IndexRebuildIssue {
            kind: IndexRebuildIssueKind::InvalidVersionFileId,
            location: format!("loc/{i}"),
            detail: IndexRebuildIssueDetail::InvalidFileId {
                file_id: format!("fid-{i}"),
            },
        });
    }
    assert_eq!(report.issue_count(), 5);
}

#[test]
fn index_rebuild_report_equality() {
    let a = empty_report();
    let mut b = empty_report();
    b.scanned_version_records = 42;
    assert_ne!(a, b);

    b.scanned_version_records = 0;
    assert_eq!(a, b);
}

#[test]
fn issue_kind_as_str_returns_expected_labels() {
    assert_eq!(
        IndexRebuildIssueKind::OversizedVersionRecordMetadata.as_str(),
        "oversized_version_record_metadata"
    );
    assert_eq!(
        IndexRebuildIssueKind::InvalidVersionRecordJson.as_str(),
        "invalid_version_record_json"
    );
    assert_eq!(
        IndexRebuildIssueKind::InvalidVersionFileId.as_str(),
        "invalid_version_file_id"
    );
    assert_eq!(
        IndexRebuildIssueKind::InvalidVersionContentHash.as_str(),
        "invalid_version_content_hash"
    );
    assert_eq!(
        IndexRebuildIssueKind::InvalidVersionRepositoryScope.as_str(),
        "invalid_version_repository_scope"
    );
    assert_eq!(
        IndexRebuildIssueKind::VersionPathMismatch.as_str(),
        "version_path_mismatch"
    );
    assert_eq!(
        IndexRebuildIssueKind::InvalidVersionReconstructionPlan.as_str(),
        "invalid_version_reconstruction_plan"
    );
    assert_eq!(
        IndexRebuildIssueKind::InvalidRetainedShard.as_str(),
        "invalid_retained_shard"
    );
}

#[test]
fn issue_detail_display_messages() {
    let detail = IndexRebuildIssueDetail::OversizedVersionRecordMetadata;
    assert!(!detail.to_string().is_empty());

    let detail = IndexRebuildIssueDetail::RecordJsonInvalid;
    assert!(!detail.to_string().is_empty());

    let detail = IndexRebuildIssueDetail::InvalidFileId {
        file_id: "bad-id".to_owned(),
    };
    assert!(detail.to_string().contains("bad-id"));

    let detail = IndexRebuildIssueDetail::InvalidContentHash {
        content_hash: "abc123".to_owned(),
    };
    assert!(detail.to_string().contains("abc123"));

    let detail = IndexRebuildIssueDetail::InvalidRepositoryScope;
    assert!(!detail.to_string().is_empty());

    let detail = IndexRebuildIssueDetail::VersionPathMismatch {
        expected_locator: "/expected/path".to_owned(),
    };
    assert!(detail.to_string().contains("/expected/path"));

    let detail = IndexRebuildIssueDetail::InvalidReconstructionPlan(
        IndexRebuildReconstructionPlanDetail::ChunkHashInvalid,
    );
    assert!(!detail.to_string().is_empty());

    let detail = IndexRebuildIssueDetail::InvalidRetainedShard(
        shardline_server_core::InvalidSerializedShardError::ParserRejectedMetadata,
    );
    assert!(!detail.to_string().is_empty());
}

#[test]
fn push_issue_increments_count() {
    let mut report = empty_report();
    push_issue(
        &mut report,
        IndexRebuildIssueKind::InvalidVersionFileId,
        "loc".to_owned(),
        IndexRebuildIssueDetail::InvalidFileId {
            file_id: "x".to_owned(),
        },
    )
    .unwrap();
    assert_eq!(report.issue_count(), 1);
}

// ---- desired_reconstruction_file_ids tests ----

fn make_candidate_with_file_id(file_id: &str) -> VersionCandidate<&'static str> {
    VersionCandidate {
        record: shardline_index::FileRecord {
            file_id: file_id.to_owned(),
            content_hash: "a".repeat(64),
            total_bytes: 0,
            chunk_size: 0,
            storage_repr: shardline_index::StorageRepresentation::WholeFileV1,
            repository_scope: None,
            chunks: Vec::new(),
        },
        locator: "loc",
        modified_since_epoch: std::time::Duration::from_secs(0),
    }
}

#[test]
fn desired_reconstruction_file_ids_empty_candidates_returns_empty_set() {
    let candidates: Vec<VersionCandidate<&str>> = Vec::new();
    let result = desired_reconstruction_file_ids(&candidates);
    assert!(result.is_empty());
}

#[test]
fn desired_reconstruction_file_ids_includes_valid_xet_hash() {
    let valid_hash = "a".repeat(64);
    let candidates = vec![make_candidate_with_file_id(&valid_hash)];
    let result = desired_reconstruction_file_ids(&candidates);
    assert!(result.contains(&valid_hash));
    assert_eq!(result.len(), 1);
}

#[test]
fn desired_reconstruction_file_ids_filters_non_hash_file_ids() {
    let valid_hash = "a".repeat(64);
    let candidates = vec![
        make_candidate_with_file_id(&valid_hash),
        make_candidate_with_file_id("not-a-hash"),
        make_candidate_with_file_id("path/to/file.txt"),
    ];
    let result = desired_reconstruction_file_ids(&candidates);
    assert_eq!(result.len(), 1);
    assert!(result.contains(&valid_hash));
}

#[test]
fn desired_reconstruction_file_ids_deduplicates() {
    let valid_hash = "b".repeat(64);
    let candidates = vec![
        make_candidate_with_file_id(&valid_hash),
        make_candidate_with_file_id(&valid_hash),
        make_candidate_with_file_id(&valid_hash),
    ];
    let result = desired_reconstruction_file_ids(&candidates);
    assert_eq!(result.len(), 1);
}

#[test]
fn desired_reconstruction_file_ids_multiple_unique_hashes() {
    let hash1 = "a".repeat(64);
    let hash2 = "b".repeat(64);
    let hash3 = "c".repeat(64);
    let candidates = vec![
        make_candidate_with_file_id(&hash1),
        make_candidate_with_file_id("invalid"),
        make_candidate_with_file_id(&hash2),
        make_candidate_with_file_id(&hash3),
    ];
    let result = desired_reconstruction_file_ids(&candidates);
    assert_eq!(result.len(), 3);
    assert!(result.contains(&hash1));
    assert!(result.contains(&hash2));
    assert!(result.contains(&hash3));
}

// ---- RebuildError From conversions ----

#[test]
#[allow(clippy::wildcard_enum_match_arm, clippy::panic)]
fn rebuild_error_from_parse_stored_file_record_too_large() {
    let source = shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
        observed_bytes: 999,
        maximum_bytes: 100,
    };
    let error: RebuildError = source.into();
    match error {
        RebuildError::StoredFileMetadataTooLarge {
            observed_bytes,
            maximum_bytes,
        } => {
            assert_eq!(observed_bytes, 999);
            assert_eq!(maximum_bytes, 100);
        }
        other => panic!("expected StoredFileMetadataTooLarge, got {other:?}"),
    }
}

#[test]
fn rebuild_error_from_parse_stored_file_record_json() {
    let source = shardline_server_core::ParseStoredFileRecordError::Json(
        serde_json::from_str::<serde_json::Value>("invalid json!").unwrap_err(),
    );
    let error: RebuildError = source.into();
    assert!(matches!(error, RebuildError::Json(_)));
}

#[test]
fn rebuild_error_from_validate_identifier_error() {
    let source = shardline_server_core::ValidateIdentifierError;
    let error: RebuildError = source.into();
    assert!(matches!(error, RebuildError::InvalidFileId));
}

#[test]
fn rebuild_error_from_validate_content_hash_error() {
    let source = shardline_server_core::ValidateContentHashError;
    let error: RebuildError = source.into();
    assert!(matches!(error, RebuildError::InvalidContentHash));
}

#[test]
fn rebuild_error_from_rebuild_overflow_error() {
    let source = shardline_server_core::RebuildOverflowError;
    let error: RebuildError = source.into();
    assert!(matches!(error, RebuildError::Overflow));
}

// ---- RebuildError display messages ----

#[test]
fn rebuild_error_display_messages_are_non_empty() {
    let errors: Vec<RebuildError> = vec![
        RebuildError::Io(std::io::Error::other("test")),
        RebuildError::Json(serde_json::from_str::<serde_json::Value>("bad json}}").unwrap_err()),
        RebuildError::NumericConversion(u64::try_from(-1i32).unwrap_err()),
        RebuildError::InvalidContentHash,
        RebuildError::InvalidFileId,
        RebuildError::Overflow,
        RebuildError::ObjectPrefix(shardline_storage::ObjectPrefixError::UnsafePath),
        RebuildError::LocalObjectStore(shardline_storage::LocalObjectStoreError::Io(
            std::io::Error::other("test"),
        )),
        RebuildError::StoredFileMetadataTooLarge {
            observed_bytes: 1,
            maximum_bytes: 0,
        },
    ];

    for error in &errors {
        let msg = error.to_string();
        assert!(!msg.is_empty(), "display message was empty for: {error:?}");
    }
}

#[test]
fn rebuild_error_stored_file_metadata_too_large_display_includes_sizes() {
    let error = RebuildError::StoredFileMetadataTooLarge {
        observed_bytes: 2048,
        maximum_bytes: 1024,
    };
    let msg = error.to_string();
    assert!(!msg.is_empty());
    assert!(
        msg.contains("stored file metadata"),
        "expected metadata mention in display, got: {msg}"
    );
}

// ---- prune_stale_reconstructions early-return on dirty state ----

#[tokio::test]
async fn prune_stale_reconstructions_returns_early_when_report_is_dirty() {
    use shardline_index::MemoryIndexStore;

    let index_store = MemoryIndexStore::new();
    let desired = HashSet::from(["a".repeat(64)]);

    let mut report = empty_report();
    push_issue(
        &mut report,
        IndexRebuildIssueKind::InvalidVersionRecordJson,
        "corrupt/record".to_owned(),
        IndexRebuildIssueDetail::RecordJsonInvalid,
    )
    .unwrap();
    assert!(!report.is_clean());

    prune_stale_reconstructions(&index_store, &desired, &mut report)
        .await
        .unwrap();

    // Report should still have the original issue and no reconstruction stats.
    assert_eq!(report.issue_count(), 1);
    assert_eq!(report.scanned_reconstructions, 0);
    assert_eq!(report.removed_stale_reconstructions, 0);
}

// ---- run_index_rebuild_with_stores integration ----

#[tokio::test(flavor = "multi_thread")]
async fn run_index_rebuild_with_empty_stores_returns_clean_report() {
    use shardline_index::MemoryIndexStore;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let object_root = root.join("chunks");
    std::fs::create_dir_all(&object_root).unwrap();
    let object_store = ServerObjectStore::local(&object_root).unwrap();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = MemoryIndexStore::new();

    let report = run_index_rebuild_with_stores(
        &record_store,
        &index_store,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();

    assert!(report.is_clean(), "expected clean report, got: {report:?}");
    assert_eq!(report.scanned_version_records, 0);
    assert_eq!(report.scanned_retained_shards, 0);
    assert_eq!(report.rebuilt_latest_records, 0);
    assert_eq!(report.unchanged_latest_records, 0);
    assert_eq!(report.removed_stale_latest_records, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn run_index_rebuild_with_single_version_record_produces_latest() {
    use shardline_index::{MemoryIndexStore, RecordMutation};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let object_root = root.join("chunks");
    std::fs::create_dir_all(&object_root).unwrap();
    let object_store = ServerObjectStore::local(&object_root).unwrap();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = MemoryIndexStore::new();

    // Write a valid version record
    let chunks = Vec::new();
    let content_hash = shardline_server_core::content_hash(0, 0, &chunks);
    let record = shardline_index::FileRecord {
        file_id: "test-file-id".to_owned(),
        content_hash,
        total_bytes: 0,
        chunk_size: 0,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks,
    };
    record_store.write_version_record(&record).await.unwrap();

    let report = run_index_rebuild_with_stores(
        &record_store,
        &index_store,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();

    assert!(report.is_clean(), "expected clean report, got: {report:?}");
    assert_eq!(report.scanned_version_records, 1);
    assert_eq!(report.rebuilt_latest_records, 1);
    assert_eq!(report.unchanged_latest_records, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn run_index_rebuild_with_existing_latest_unchanged() {
    use shardline_index::{MemoryIndexStore, RecordMutation};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let object_root = root.join("chunks");
    std::fs::create_dir_all(&object_root).unwrap();
    let object_store = ServerObjectStore::local(&object_root).unwrap();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = MemoryIndexStore::new();

    let chunks = Vec::new();
    let content_hash = shardline_server_core::content_hash(0, 0, &chunks);
    let record = shardline_index::FileRecord {
        file_id: "test-file-id".to_owned(),
        content_hash,
        total_bytes: 0,
        chunk_size: 0,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks,
    };
    record_store.write_version_record(&record).await.unwrap();
    record_store.write_latest_record(&record).await.unwrap();

    let report = run_index_rebuild_with_stores(
        &record_store,
        &index_store,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();

    assert!(report.is_clean(), "expected clean report, got: {report:?}");
    assert_eq!(report.scanned_version_records, 1);
    assert_eq!(report.rebuilt_latest_records, 0);
    assert_eq!(report.unchanged_latest_records, 1);
}

// ---- desired_reconstruction_file_ids preserves dirty report state ----

#[test]
fn desired_reconstruction_file_ids_does_not_modify_report_state() {
    let mut report = empty_report();
    push_issue(
        &mut report,
        IndexRebuildIssueKind::InvalidVersionFileId,
        "loc".to_owned(),
        IndexRebuildIssueDetail::InvalidFileId {
            file_id: "bad".to_owned(),
        },
    )
    .unwrap();
    assert!(!report.is_clean());

    let candidates = vec![make_candidate_with_file_id(&"a".repeat(64))];
    let _result = desired_reconstruction_file_ids(&candidates);

    // Report is still dirty — the function does not touch the report.
    assert!(!report.is_clean());
    assert_eq!(report.issue_count(), 1);
}

// ---- push_issue with many issues ----

#[test]
fn push_issue_many_issues_tracks_all() {
    let mut report = empty_report();
    for i in 0..100 {
        push_issue(
            &mut report,
            IndexRebuildIssueKind::InvalidVersionContentHash,
            format!("loc/{i}"),
            IndexRebuildIssueDetail::InvalidContentHash {
                content_hash: format!("hash-{i}"),
            },
        )
        .unwrap();
    }
    assert_eq!(report.issue_count(), 100);
    assert!(!report.is_clean());
}

// ---- RebuildError From impls for basic error types ----

#[test]
fn rebuild_error_from_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "missing file");
    let error: RebuildError = io_err.into();
    assert!(matches!(error, RebuildError::Io(_)));
    assert!(!error.to_string().is_empty());
}

#[test]
fn rebuild_error_from_json_error() {
    let json_err = serde_json::from_str::<serde_json::Value>("not json!!!").unwrap_err();
    let error: RebuildError = json_err.into();
    assert!(matches!(error, RebuildError::Json(_)));
    assert!(!error.to_string().is_empty());
}

#[test]
fn rebuild_error_from_try_from_int_error() {
    let int_err = u64::try_from(-1i32).unwrap_err();
    let error: RebuildError = int_err.into();
    assert!(matches!(error, RebuildError::NumericConversion(_)));
    assert!(!error.to_string().is_empty());
}

// ---- RebuildError From impls via #[from] for storage/index adapter errors ----

#[test]
fn rebuild_error_from_object_prefix_error() {
    let err = shardline_storage::ObjectPrefixError::UnsafePath;
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::ObjectPrefix(_)));
    assert!(!error.to_string().is_empty());
}

#[test]
fn rebuild_error_from_local_object_store_error() {
    let err = shardline_storage::LocalObjectStoreError::Io(std::io::Error::other("test"));
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::LocalObjectStore(_)));
    assert!(!error.to_string().is_empty());
}

#[test]
fn rebuild_error_from_s3_object_store_error() {
    let err = shardline_storage::S3ObjectStoreError::Io(std::io::Error::other("test"));
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::S3ObjectStore(_)));
    assert!(!error.to_string().is_empty());
}

#[test]
fn rebuild_error_from_xet_adapter_error() {
    let err = shardline_xet_adapter::XetAdapterError::NotFound;
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::XetAdapter(_)));
    assert!(!error.to_string().is_empty());
}

#[test]
fn rebuild_error_from_index_store_error() {
    let err = shardline_index::LocalIndexStoreError::Io(std::io::Error::other("test"));
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::IndexStore(_)));
    assert!(!error.to_string().is_empty());
}

#[test]
fn rebuild_error_from_memory_index_store_error() {
    let err = shardline_index::MemoryIndexStoreError::LockPoisoned("test".to_owned());
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::MemoryIndexStore(_)));
    assert!(!error.to_string().is_empty());
}

#[test]
fn rebuild_error_from_memory_record_store_error() {
    let err = shardline_index::MemoryRecordStoreError::LockPoisoned("test".to_owned());
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::MemoryRecordStore(_)));
    assert!(!error.to_string().is_empty());
}

#[test]
fn rebuild_error_from_hash_parse_error() {
    let err = shardline_protocol::HashParseError::InvalidLength;
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::HashParse(_)));
    assert!(!error.to_string().is_empty());
}

#[test]
fn rebuild_error_from_invalid_serialized_shard_error() {
    let err = shardline_server_core::InvalidSerializedShardError::ParserRejectedMetadata;
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::XetAdapter(_)));
    assert!(!error.to_string().is_empty());
}

#[test]
fn rebuild_error_from_server_object_store_error_not_found() {
    let err = shardline_server_core::ServerObjectStoreError::NotFound;
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::Overflow));
}

#[test]
fn rebuild_error_from_server_object_store_error_overflow() {
    let err = shardline_server_core::ServerObjectStoreError::Overflow;
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::Overflow));
}

#[test]
fn rebuild_error_from_server_object_store_error_invalid_content_hash() {
    let err = shardline_server_core::ServerObjectStoreError::InvalidContentHash;
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::Overflow));
}

#[test]
fn rebuild_error_from_server_object_store_error_stored_length_mismatch() {
    let err = shardline_server_core::ServerObjectStoreError::StoredObjectLengthMismatch;
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::Overflow));
}

#[test]
fn rebuild_error_from_server_object_store_error_local() {
    let inner = shardline_storage::LocalObjectStoreError::Io(std::io::Error::other("test"));
    let err = shardline_server_core::ServerObjectStoreError::Local(inner);
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::LocalObjectStore(_)));
}

#[test]
fn rebuild_error_from_server_object_store_error_s3() {
    let inner = shardline_storage::S3ObjectStoreError::Io(std::io::Error::other("test"));
    let err = shardline_server_core::ServerObjectStoreError::S3(inner);
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::S3ObjectStore(_)));
}

#[test]
fn rebuild_error_from_server_object_store_error_io() {
    let err = shardline_server_core::ServerObjectStoreError::Io(std::io::Error::other("test"));
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::Io(_)));
}

#[test]
fn rebuild_error_from_server_object_store_error_numeric_conversion() {
    let inner = u64::try_from(-1i32).unwrap_err();
    let err = shardline_server_core::ServerObjectStoreError::NumericConversion(inner);
    let error: RebuildError = err.into();
    assert!(matches!(error, RebuildError::NumericConversion(_)));
}

// ---- RebuildError Display for ALL variants ----

#[test]
fn rebuild_error_display_all_variants_non_empty() {
    let errors: Vec<RebuildError> = vec![
        RebuildError::Io(std::io::Error::other("test")),
        RebuildError::Json(serde_json::from_str::<serde_json::Value>("bad json}}").unwrap_err()),
        RebuildError::NumericConversion(u64::try_from(-1i32).unwrap_err()),
        RebuildError::InvalidContentHash,
        RebuildError::InvalidFileId,
        RebuildError::Overflow,
        RebuildError::ObjectPrefix(shardline_storage::ObjectPrefixError::UnsafePath),
        RebuildError::LocalObjectStore(shardline_storage::LocalObjectStoreError::Io(
            std::io::Error::other("test"),
        )),
        RebuildError::S3ObjectStore(shardline_storage::S3ObjectStoreError::Io(
            std::io::Error::other("test"),
        )),
        RebuildError::XetAdapter(shardline_xet_adapter::XetAdapterError::NotFound),
        RebuildError::IndexStore(shardline_index::LocalIndexStoreError::Io(
            std::io::Error::other("test"),
        )),
        RebuildError::MemoryIndexStore(shardline_index::MemoryIndexStoreError::LockPoisoned(
            "test".to_owned(),
        )),
        RebuildError::MemoryRecordStore(shardline_index::MemoryRecordStoreError::LockPoisoned(
            "test".to_owned(),
        )),
        RebuildError::HashParse(shardline_protocol::HashParseError::InvalidLength),
        RebuildError::StoredFileMetadataTooLarge {
            observed_bytes: 1,
            maximum_bytes: 0,
        },
    ];

    for error in &errors {
        let msg = error.to_string();
        assert!(!msg.is_empty(), "display message was empty for: {error:?}");
    }
}

#[test]
fn rebuild_error_display_postgres_metadata() {
    let err = RebuildError::PostgresMetadata(
        shardline_index::PostgresMetadataStoreError::IntegerOutOfRange("test".to_owned()),
    );
    let msg = err.to_string();
    assert_eq!(msg, "postgres metadata adapter operation failed");
}

#[test]
fn rebuild_error_display_io_contains_detail() {
    let err = RebuildError::Io(std::io::Error::other("disk full"));
    let msg = err.to_string();
    assert_eq!(msg, "local storage operation failed");
}

#[test]
fn rebuild_error_display_json_contains_detail() {
    let json_err = serde_json::from_str::<serde_json::Value>("{broken}").unwrap_err();
    let err = RebuildError::Json(json_err);
    let msg = err.to_string();
    assert_eq!(msg, "json operation failed");
}

#[test]
fn rebuild_error_display_numeric_conversion() {
    let int_err = u64::try_from(-1i32).unwrap_err();
    let err = RebuildError::NumericConversion(int_err);
    let msg = err.to_string();
    assert_eq!(msg, "numeric conversion exceeded supported bounds");
}

#[test]
fn rebuild_error_display_invalid_content_hash() {
    let msg = RebuildError::InvalidContentHash.to_string();
    assert_eq!(msg, "content hash must be 64 hexadecimal characters");
}

#[test]
fn rebuild_error_display_invalid_file_id() {
    let msg = RebuildError::InvalidFileId.to_string();
    assert_eq!(
        msg,
        "file identifier must be relative and must not contain traversal or control characters"
    );
}

#[test]
fn rebuild_error_display_overflow() {
    let msg = RebuildError::Overflow.to_string();
    assert_eq!(msg, "arithmetic overflow");
}

#[test]
fn rebuild_error_display_object_prefix() {
    let err = RebuildError::ObjectPrefix(shardline_storage::ObjectPrefixError::UnsafePath);
    assert_eq!(err.to_string(), "object storage prefix validation failed");
}

#[test]
fn rebuild_error_display_hash_parse() {
    let err = RebuildError::HashParse(shardline_protocol::HashParseError::InvalidLength);
    assert_eq!(err.to_string(), "hash parsing failed");
}

#[test]
fn rebuild_error_display_xet_adapter() {
    let err = RebuildError::XetAdapter(shardline_xet_adapter::XetAdapterError::NotFound);
    assert_eq!(err.to_string(), "xet adapter operation failed");
}

#[test]
fn rebuild_error_display_stored_file_metadata_too_large() {
    let err = RebuildError::StoredFileMetadataTooLarge {
        observed_bytes: 999,
        maximum_bytes: 100,
    };
    let msg = err.to_string();
    assert!(msg.contains("stored file metadata exceeded the bounded parser ceiling"));
}

// ---- IndexRebuildReconstructionPlanDetail Display tests ----

#[test]
fn reconstruction_plan_detail_display_chunk_hash_invalid() {
    let detail = IndexRebuildReconstructionPlanDetail::ChunkHashInvalid;
    assert_eq!(detail.to_string(), "record chunk hash is invalid");
}

#[test]
fn reconstruction_plan_detail_display_empty_chunk() {
    let detail = IndexRebuildReconstructionPlanDetail::EmptyChunk;
    assert_eq!(detail.to_string(), "record contains an empty chunk");
}

#[test]
fn reconstruction_plan_detail_display_non_contiguous() {
    let detail = IndexRebuildReconstructionPlanDetail::NonContiguousChunkOffsets;
    assert_eq!(detail.to_string(), "record chunks are not contiguous");
}

#[test]
fn reconstruction_plan_detail_display_invalid_chunk_range() {
    let detail = IndexRebuildReconstructionPlanDetail::InvalidChunkRange;
    assert_eq!(detail.to_string(), "record chunk range is invalid");
}

#[test]
fn reconstruction_plan_detail_display_invalid_packed_range() {
    let detail = IndexRebuildReconstructionPlanDetail::InvalidPackedRange;
    assert_eq!(detail.to_string(), "record packed range is invalid");
}

#[test]
fn reconstruction_plan_detail_display_length_overflow() {
    let detail = IndexRebuildReconstructionPlanDetail::LengthOverflow;
    assert_eq!(detail.to_string(), "record length overflowed");
}

#[test]
fn reconstruction_plan_detail_display_total_bytes_mismatch() {
    let detail = IndexRebuildReconstructionPlanDetail::TotalBytesMismatch;
    assert_eq!(
        detail.to_string(),
        "record total byte count did not match chunks"
    );
}

// ---- run_index_rebuild_with_stores stale latest records ----

#[tokio::test(flavor = "multi_thread")]
async fn run_index_rebuild_removes_stale_latest_records() {
    use shardline_index::{MemoryIndexStore, RecordMutation};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let object_root = root.join("chunks");
    std::fs::create_dir_all(&object_root).unwrap();
    let object_store = ServerObjectStore::local(&object_root).unwrap();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = MemoryIndexStore::new();

    // Write a version record that will produce a candidate
    let chunks = Vec::new();
    let content_hash = shardline_server_core::content_hash(0, 0, &chunks);
    let record = shardline_index::FileRecord {
        file_id: "test-file".to_owned(),
        content_hash,
        total_bytes: 0,
        chunk_size: 0,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks,
    };
    record_store.write_version_record(&record).await.unwrap();

    // Write a latest record for a different file that has NO version record
    let stale_record = shardline_index::FileRecord {
        file_id: "stale-file".to_owned(),
        content_hash: "b".repeat(64),
        total_bytes: 0,
        chunk_size: 0,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks: Vec::new(),
    };
    record_store
        .write_latest_record(&stale_record)
        .await
        .unwrap();

    let report = run_index_rebuild_with_stores(
        &record_store,
        &index_store,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();

    assert!(report.is_clean(), "expected clean report, got: {report:?}");
    assert_eq!(report.scanned_version_records, 1);
    assert_eq!(report.removed_stale_latest_records, 1);
    assert_eq!(report.rebuilt_latest_records, 1);
    assert_eq!(report.unchanged_latest_records, 0);
}

// ---- prune_stale_reconstructions full path ----

#[tokio::test(flavor = "multi_thread")]
async fn prune_stale_reconstructions_removes_undesired() {
    use shardline_index::{
        FileId, FileReconstruction, MemoryIndexStore, ReconstructionTerm, XorbId,
    };
    use shardline_protocol::{ChunkRange, ShardlineHash};

    let index_store = MemoryIndexStore::new();

    // Insert a reconstruction that we will later treat as stale
    let hash = ShardlineHash::from_bytes([1; 32]);
    let xorb_id = XorbId::new(hash);
    let range = ChunkRange::new(0, 1).unwrap();
    let terms = vec![ReconstructionTerm::new(xorb_id, range, 64)];
    let reconstruction = FileReconstruction::new(terms);
    let file_id = FileId::new(ShardlineHash::from_bytes([2; 32]));
    index_store
        .insert_reconstruction(&file_id, &reconstruction)
        .unwrap();

    // desired set is empty, so the reconstruction above is stale
    let desired = HashSet::new();
    let mut report = empty_report();

    prune_stale_reconstructions(&index_store, &desired, &mut report)
        .await
        .unwrap();

    assert!(report.is_clean());
    assert_eq!(report.scanned_reconstructions, 1);
    assert_eq!(report.removed_stale_reconstructions, 1);
    assert_eq!(report.unchanged_reconstructions, 0);

    // Verify the reconstruction was deleted
    let remaining = index_store.list_reconstruction_file_ids().await.unwrap();
    assert!(remaining.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn prune_stale_reconstructions_preserves_desired() {
    use shardline_index::{
        FileId, FileReconstruction, MemoryIndexStore, ReconstructionTerm, XorbId,
    };
    use shardline_protocol::{ChunkRange, ShardlineHash};

    let index_store = MemoryIndexStore::new();

    // Insert a reconstruction whose file_id hex matches the desired set
    let file_id = FileId::new(ShardlineHash::from_bytes([3; 32]));
    let hash = ShardlineHash::from_bytes([4; 32]);
    let xorb_id = XorbId::new(hash);
    let range = ChunkRange::new(0, 1).unwrap();
    let terms = vec![ReconstructionTerm::new(xorb_id, range, 64)];
    let reconstruction = FileReconstruction::new(terms);
    index_store
        .insert_reconstruction(&file_id, &reconstruction)
        .unwrap();
    // file_id_hex for ShardlineHash::from_bytes([3; 32])
    let file_id_hex = format!("{:02x}", 3).repeat(64 / 2);
    let mut desired = HashSet::new();
    desired.insert(file_id_hex.clone());

    let mut report = empty_report();

    prune_stale_reconstructions(&index_store, &desired, &mut report)
        .await
        .unwrap();

    assert!(report.is_clean());
    assert_eq!(report.scanned_reconstructions, 1);
    assert_eq!(report.unchanged_reconstructions, 1);
    assert_eq!(report.removed_stale_reconstructions, 0);

    // Verify the reconstruction is still present
    let remaining = index_store.list_reconstruction_file_ids().await.unwrap();
    assert_eq!(remaining.len(), 1);
}

// ---- StoredFileMetadataTooLarge display ----

#[test]
fn rebuild_error_stored_file_metadata_too_large_display() {
    let err = RebuildError::StoredFileMetadataTooLarge {
        observed_bytes: 1_073_741_825,
        maximum_bytes: 1_073_741_824,
    };
    let msg = err.to_string();
    // The #[error] attribute uses a static string without field interpolation.
    assert!(
        msg.contains("stored file metadata exceeded the bounded parser ceiling"),
        "display should mention stored file metadata: {msg}"
    );
    // The large byte values are not included in the static display string.
    // Field inclusion would require an explicit format string change.
}

// ---- rebuild_dedupe_shard_mappings paths ----

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_dedupe_shard_mappings_empty_produces_clean_report() {
    let storage = shardline_test_support::TempStorage::new();
    let index_store = shardline_index::MemoryIndexStore::new();
    let object_root = storage.path().join("chunks");
    std::fs::create_dir_all(&object_root).unwrap();
    let object_store = ServerObjectStore::local(&object_root).unwrap();

    let mut report = empty_report();
    rebuild_dedupe_shard_mappings(
        &index_store,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        &mut report,
    )
    .await
    .unwrap();

    assert!(report.is_clean());
    assert_eq!(report.scanned_retained_shards, 0);
    assert_eq!(report.removed_stale_dedupe_shard_mappings, 0);
    assert_eq!(report.rebuilt_dedupe_shard_mappings, 0);
    assert_eq!(report.unchanged_dedupe_shard_mappings, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_dedupe_shard_mappings_invalid_shard_bytes_reports_issue() {
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey};

    let storage = shardline_test_support::TempStorage::new();
    let index_store = shardline_index::MemoryIndexStore::new();
    let object_root = storage.path().join("chunks");
    std::fs::create_dir_all(&object_root).unwrap();
    let object_store = ServerObjectStore::local(&object_root).unwrap();

    // Write garbage bytes as a shard file to trigger InvalidSerializedShard
    let shard_key = ObjectKey::parse("shards/ab/invalid_shard.shard").unwrap();
    let garbage = b"this is not a valid shard file at all!!!!";
    object_store
        .put_overwrite(
            &shard_key,
            ObjectBody::from_slice(garbage),
            &ObjectIntegrity::new(
                shardline_server_core::chunk_hash(garbage),
                garbage.len() as u64,
            ),
        )
        .unwrap();

    let mut report = empty_report();
    rebuild_dedupe_shard_mappings(
        &index_store,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        &mut report,
    )
    .await
    .unwrap();

    assert!(!report.is_clean());
    assert_eq!(report.scanned_retained_shards, 1);
    assert_eq!(report.issue_count(), 1);
    assert_eq!(
        report.issues[0].kind,
        IndexRebuildIssueKind::InvalidRetainedShard
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_dedupe_shard_mappings_removes_stale_existing_mappings() {
    use shardline_index::{DedupeShardMapping, DedupeStore};
    use shardline_protocol::ShardlineHash;
    use shardline_storage::ObjectKey;

    let storage = shardline_test_support::TempStorage::new();
    let index_store = shardline_index::MemoryIndexStore::new();
    let object_root = storage.path().join("chunks");
    std::fs::create_dir_all(&object_root).unwrap();
    let object_store = ServerObjectStore::local(&object_root).unwrap();

    // Insert a dedupe mapping pointing to a shard that no longer exists.
    // The rebuild will see no shards, so this mapping is stale.
    let chunk_hash = ShardlineHash::from_bytes([1; 32]);
    let shard_key = ObjectKey::parse("shards/ab/nonexistent.shard").unwrap();
    let mapping = DedupeShardMapping::new(chunk_hash, shard_key);
    index_store.upsert_dedupe_shard_mapping(&mapping).unwrap();

    let mut report = empty_report();
    rebuild_dedupe_shard_mappings(
        &index_store,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        &mut report,
    )
    .await
    .unwrap();

    assert!(report.is_clean());
    assert_eq!(report.scanned_retained_shards, 0);
    assert_eq!(report.removed_stale_dedupe_shard_mappings, 1);
    assert_eq!(report.rebuilt_dedupe_shard_mappings, 0);
    assert_eq!(report.unchanged_dedupe_shard_mappings, 0);

    // Verify the stale mapping was actually removed from the index store
    let remaining = DedupeStore::list_dedupe_shard_mappings(&index_store).unwrap();
    assert!(remaining.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_dedupe_shard_mappings_with_valid_shard_unchanged_mapping() {
    use shardline_index::{DedupeShardMapping, parse_xet_hash_hex};
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey};
    use shardline_xet_core::{
        merklehash::{compute_data_hash, file_hash, xorb_hash},
        metadata_shard::{
            file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
            shard_in_memory::MDBInMemoryShard,
            xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
        },
    };

    // Build a minimal valid shard with one chunk that has the global-dedup flag
    // so that `collect_dedupe_chunk_hashes` includes it.
    let chunk_data = b"test chunk data for rebuild dedupe test";
    let chunk_hash = compute_data_hash(chunk_data);
    let xorb_hash = xorb_hash(&[(chunk_hash, chunk_data.len() as u64)]);
    let file_hash_val = file_hash(&[(chunk_hash, chunk_data.len() as u64)]);

    let mut shard = MDBInMemoryShard::default();
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash_val, 1u64, false, false),
            segments: vec![FileDataSequenceEntry::new(
                xorb_hash,
                chunk_data.len() as u64,
                0_u64,
                1_u64,
            )],
            verification: Vec::new(),
            metadata_ext: None,
        })
        .unwrap();
    shard
        .add_xorb_block(MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u64, chunk_data.len() as u64),
            chunks: vec![
                XorbChunkSequenceEntry::new(chunk_hash, chunk_data.len() as u64, 0_u64)
                    .with_global_dedup_flag(true),
            ],
        })
        .unwrap();
    let shard_bytes = shard.to_bytes().unwrap();
    let chunk_hash_hex = chunk_hash.hex();

    let storage = shardline_test_support::TempStorage::new();
    let index_store = shardline_index::MemoryIndexStore::new();
    let object_root = storage.path().join("chunks");
    std::fs::create_dir_all(&object_root).unwrap();
    let object_store = ServerObjectStore::local(&object_root).unwrap();

    // Store the shard in the object store using a realistic shard key
    let shard_key_str = format!("shards/{}/{}", &chunk_hash_hex[..2], chunk_hash_hex);
    let shard_key = ObjectKey::parse(&shard_key_str).unwrap();
    object_store
        .put_overwrite(
            &shard_key,
            ObjectBody::from_slice(&shard_bytes),
            &ObjectIntegrity::new(
                shardline_server_core::chunk_hash(&shard_bytes),
                shard_bytes.len() as u64,
            ),
        )
        .unwrap();

    // Insert a matching dedupe mapping so the rebuild finds it unchanged
    let shardline_chunk_hash = parse_xet_hash_hex(&chunk_hash_hex).unwrap();
    let existing_mapping = DedupeShardMapping::new(shardline_chunk_hash, shard_key.clone());
    index_store
        .upsert_dedupe_shard_mapping(&existing_mapping)
        .unwrap();

    let mut report = empty_report();
    rebuild_dedupe_shard_mappings(
        &index_store,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        &mut report,
    )
    .await
    .unwrap();

    assert!(report.is_clean(), "expected clean report, got: {report:?}");
    assert_eq!(report.scanned_retained_shards, 1);
    assert_eq!(report.unchanged_dedupe_shard_mappings, 1);
    assert_eq!(report.rebuilt_dedupe_shard_mappings, 0);
    assert_eq!(report.removed_stale_dedupe_shard_mappings, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_dedupe_shard_mappings_with_valid_shard_rebuilds_changed_mapping() {
    use shardline_index::{DedupeShardMapping, DedupeStore, parse_xet_hash_hex};
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey};
    use shardline_xet_core::{
        merklehash::{compute_data_hash, file_hash, xorb_hash},
        metadata_shard::{
            file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
            shard_in_memory::MDBInMemoryShard,
            xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
        },
    };

    // Build a minimal valid shard with one chunk (global-dedup flagged)
    let chunk_data = b"rebuild test data for changed mapping";
    let chunk_hash = compute_data_hash(chunk_data);
    let xorb_hash = xorb_hash(&[(chunk_hash, chunk_data.len() as u64)]);
    let file_hash_val = file_hash(&[(chunk_hash, chunk_data.len() as u64)]);

    let mut shard = MDBInMemoryShard::default();
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash_val, 1u64, false, false),
            segments: vec![FileDataSequenceEntry::new(
                xorb_hash,
                chunk_data.len() as u64,
                0_u64,
                1_u64,
            )],
            verification: Vec::new(),
            metadata_ext: None,
        })
        .unwrap();
    shard
        .add_xorb_block(MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u64, chunk_data.len() as u64),
            chunks: vec![
                XorbChunkSequenceEntry::new(chunk_hash, chunk_data.len() as u64, 0_u64)
                    .with_global_dedup_flag(true),
            ],
        })
        .unwrap();
    let shard_bytes = shard.to_bytes().unwrap();
    let chunk_hash_hex = chunk_hash.hex();

    let storage = shardline_test_support::TempStorage::new();
    let index_store = shardline_index::MemoryIndexStore::new();
    let object_root = storage.path().join("chunks");
    std::fs::create_dir_all(&object_root).unwrap();
    let object_store = ServerObjectStore::local(&object_root).unwrap();

    // Store the shard
    let shard_key_str = format!("shards/{}/{}", &chunk_hash_hex[..2], chunk_hash_hex);
    let shard_key = ObjectKey::parse(&shard_key_str).unwrap();
    object_store
        .put_overwrite(
            &shard_key,
            ObjectBody::from_slice(&shard_bytes),
            &ObjectIntegrity::new(
                shardline_server_core::chunk_hash(&shard_bytes),
                shard_bytes.len() as u64,
            ),
        )
        .unwrap();

    // Insert an existing mapping that points to a DIFFERENT shard key.
    // The rebuild will see that the desired mapping (pointing to the real shard)
    // differs from the existing mapping (pointing to a fake shard) and will
    // upsert the correct one.
    let shardline_chunk_hash = parse_xet_hash_hex(&chunk_hash_hex).unwrap();
    let fake_shard_key = ObjectKey::parse("shards/ab/a_different_shard.shard").unwrap();
    let old_mapping = DedupeShardMapping::new(shardline_chunk_hash, fake_shard_key);
    index_store
        .upsert_dedupe_shard_mapping(&old_mapping)
        .unwrap();

    let mut report = empty_report();
    rebuild_dedupe_shard_mappings(
        &index_store,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        &mut report,
    )
    .await
    .unwrap();

    assert!(report.is_clean(), "expected clean report, got: {report:?}");
    assert_eq!(report.scanned_retained_shards, 1);
    assert_eq!(report.rebuilt_dedupe_shard_mappings, 1);
    assert_eq!(report.unchanged_dedupe_shard_mappings, 0);
    assert_eq!(report.removed_stale_dedupe_shard_mappings, 0);

    // Verify the mapping was updated to point to the correct shard key
    let updated = DedupeStore::dedupe_shard_mapping(&index_store, &shardline_chunk_hash).unwrap();
    assert!(
        updated.is_some(),
        "mapping should still exist after rebuild"
    );
    if let Some(updated) = updated {
        assert_eq!(
            updated.shard_object_key(),
            &shard_key,
            "mapping should be updated to point to the real shard"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_dedupe_shard_mappings_two_shards_same_chunk_hash_keeps_smaller_key() {
    // Two shards sharing the same chunk hash. The first visited (lexicographically
    // smaller key) inserts into `desired`; the second hits the
    // `Some(existing) if existing.shard_object_key() <= new => {}` arm (lines 524-526).
    use shardline_index::{DedupeShardMapping, parse_xet_hash_hex};
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey};
    use shardline_xet_core::{
        merklehash::{compute_data_hash, file_hash, xorb_hash},
        metadata_shard::{
            file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
            shard_in_memory::MDBInMemoryShard,
            xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
        },
    };

    let chunk_data = b"shared-chunk-data-for-two-shards";
    let chunk_hash = compute_data_hash(chunk_data);
    let xorb_hash = xorb_hash(&[(chunk_hash, chunk_data.len() as u64)]);
    let file_hash_val = file_hash(&[(chunk_hash, chunk_data.len() as u64)]);

    // Build shard #1 (will be stored at a lexicographically smaller key)
    let mut shard1 = MDBInMemoryShard::default();
    shard1
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash_val, 1u64, false, false),
            segments: vec![FileDataSequenceEntry::new(
                xorb_hash,
                chunk_data.len() as u64,
                0_u64,
                1_u64,
            )],
            verification: Vec::new(),
            metadata_ext: None,
        })
        .unwrap();
    shard1
        .add_xorb_block(MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u64, chunk_data.len() as u64),
            chunks: vec![
                XorbChunkSequenceEntry::new(chunk_hash, chunk_data.len() as u64, 0_u64)
                    .with_global_dedup_flag(true),
            ],
        })
        .unwrap();
    let shard1_bytes = shard1.to_bytes().unwrap();

    // Build shard #2 using the SAME chunk data but a different file hash
    // so the shard content differs (different serialized bytes).
    let file_hash_val2 = file_hash(&[(chunk_hash, chunk_data.len() as u64 + 1)]);
    let mut shard2 = MDBInMemoryShard::default();
    shard2
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash_val2, 1u64, false, false),
            segments: vec![FileDataSequenceEntry::new(
                xorb_hash,
                chunk_data.len() as u64,
                0_u64,
                1_u64,
            )],
            verification: Vec::new(),
            metadata_ext: None,
        })
        .unwrap();
    shard2
        .add_xorb_block(MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u64, chunk_data.len() as u64),
            chunks: vec![
                XorbChunkSequenceEntry::new(chunk_hash, chunk_data.len() as u64, 0_u64)
                    .with_global_dedup_flag(true),
            ],
        })
        .unwrap();
    let shard2_bytes = shard2.to_bytes().unwrap();

    let chunk_hash_hex = chunk_hash.hex();

    let storage = shardline_test_support::TempStorage::new();
    let index_store = shardline_index::MemoryIndexStore::new();
    let object_root = storage.path().join("chunks");
    std::fs::create_dir_all(&object_root).unwrap();
    let object_store = ServerObjectStore::local(&object_root).unwrap();

    // Store shard #1 with a smaller key ("aa" prefix < "zz" prefix)
    let key1 = ObjectKey::parse(&format!("shards/aa/{}", chunk_hash_hex)).unwrap();
    object_store
        .put_overwrite(
            &key1,
            ObjectBody::from_slice(&shard1_bytes),
            &ObjectIntegrity::new(
                shardline_server_core::chunk_hash(&shard1_bytes),
                shard1_bytes.len() as u64,
            ),
        )
        .unwrap();

    // Store shard #2 with a larger key ("zz" prefix > "aa" prefix)
    let key2 = ObjectKey::parse(&format!("shards/zz/{}", chunk_hash_hex)).unwrap();
    object_store
        .put_overwrite(
            &key2,
            ObjectBody::from_slice(&shard2_bytes),
            &ObjectIntegrity::new(
                shardline_server_core::chunk_hash(&shard2_bytes),
                shard2_bytes.len() as u64,
            ),
        )
        .unwrap();

    // Also add a matching dedupe mapping so the rebuild finds it unchanged
    let shardline_chunk_hash = parse_xet_hash_hex(&chunk_hash_hex).unwrap();
    let existing_mapping = DedupeShardMapping::new(shardline_chunk_hash, key1.clone());
    index_store
        .upsert_dedupe_shard_mapping(&existing_mapping)
        .unwrap();

    let mut report = empty_report();
    rebuild_dedupe_shard_mappings(
        &index_store,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        &mut report,
    )
    .await
    .unwrap();

    // Both shards scanned, one mapping unchanged (the smaller key was kept)
    assert!(report.is_clean(), "report: {report:?}");
    assert_eq!(report.scanned_retained_shards, 2);
    assert_eq!(report.unchanged_dedupe_shard_mappings, 1);
    assert_eq!(report.rebuilt_dedupe_shard_mappings, 0);
    assert_eq!(report.removed_stale_dedupe_shard_mappings, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_dedupe_shard_mappings_two_shards_same_chunk_hash_replaces_with_smaller_key() {
    // Two shards sharing the same chunk hash. The first visited has a larger key,
    // so when the second (smaller key) is processed, it replaces the larger key
    // in `desired` (the `_` arm of the match, lines 527-528).
    use shardline_index::parse_xet_hash_hex;
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey};
    use shardline_xet_core::{
        merklehash::{compute_data_hash, file_hash, xorb_hash},
        metadata_shard::{
            file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
            shard_in_memory::MDBInMemoryShard,
            xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
        },
    };

    let chunk_data = b"shared-chunk-two-shards-replacement";
    let chunk_hash = compute_data_hash(chunk_data);
    let xorb_hash = xorb_hash(&[(chunk_hash, chunk_data.len() as u64)]);
    let file_hash_val = file_hash(&[(chunk_hash, chunk_data.len() as u64)]);

    // Build shard stored at larger key (visited first due to prefix ordering)
    let mut shard_big = MDBInMemoryShard::default();
    shard_big
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash_val, 1u64, false, false),
            segments: vec![FileDataSequenceEntry::new(
                xorb_hash,
                chunk_data.len() as u64,
                0_u64,
                1_u64,
            )],
            verification: Vec::new(),
            metadata_ext: None,
        })
        .unwrap();
    shard_big
        .add_xorb_block(MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u64, chunk_data.len() as u64),
            chunks: vec![
                XorbChunkSequenceEntry::new(chunk_hash, chunk_data.len() as u64, 0_u64)
                    .with_global_dedup_flag(true),
            ],
        })
        .unwrap();
    let shard_big_bytes = shard_big.to_bytes().unwrap();

    // Build shard stored at smaller key (visited second, replaces the first)
    let file_hash_val2 = file_hash(&[(chunk_hash, chunk_data.len() as u64 + 2)]);
    let mut shard_small = MDBInMemoryShard::default();
    shard_small
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash_val2, 1u64, false, false),
            segments: vec![FileDataSequenceEntry::new(
                xorb_hash,
                chunk_data.len() as u64,
                0_u64,
                1_u64,
            )],
            verification: Vec::new(),
            metadata_ext: None,
        })
        .unwrap();
    shard_small
        .add_xorb_block(MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u64, chunk_data.len() as u64),
            chunks: vec![
                XorbChunkSequenceEntry::new(chunk_hash, chunk_data.len() as u64, 0_u64)
                    .with_global_dedup_flag(true),
            ],
        })
        .unwrap();
    let shard_small_bytes = shard_small.to_bytes().unwrap();

    let chunk_hash_hex = chunk_hash.hex();

    let storage = shardline_test_support::TempStorage::new();
    let index_store = shardline_index::MemoryIndexStore::new();
    let object_root = storage.path().join("chunks");
    std::fs::create_dir_all(&object_root).unwrap();
    let object_store = ServerObjectStore::local(&object_root).unwrap();

    // Store larger-key shard first ("zz" > "aa")
    let key_big = ObjectKey::parse(&format!("shards/zz/{}", chunk_hash_hex)).unwrap();
    object_store
        .put_overwrite(
            &key_big,
            ObjectBody::from_slice(&shard_big_bytes),
            &ObjectIntegrity::new(
                shardline_server_core::chunk_hash(&shard_big_bytes),
                shard_big_bytes.len() as u64,
            ),
        )
        .unwrap();

    // Store smaller-key shard second ("aa" < "zz")
    let key_small = ObjectKey::parse(&format!("shards/aa/{}", chunk_hash_hex)).unwrap();
    object_store
        .put_overwrite(
            &key_small,
            ObjectBody::from_slice(&shard_small_bytes),
            &ObjectIntegrity::new(
                shardline_server_core::chunk_hash(&shard_small_bytes),
                shard_small_bytes.len() as u64,
            ),
        )
        .unwrap();

    // No pre-existing mapping so the rebuild will insert the desired one
    let mut report = empty_report();
    rebuild_dedupe_shard_mappings(
        &index_store,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        &mut report,
    )
    .await
    .unwrap();

    assert!(report.is_clean(), "report: {report:?}");
    assert_eq!(report.scanned_retained_shards, 2);
    // The smaller key mapping should win (rebuilt)
    assert_eq!(report.rebuilt_dedupe_shard_mappings, 1);
    assert_eq!(report.unchanged_dedupe_shard_mappings, 0);
    assert_eq!(report.removed_stale_dedupe_shard_mappings, 0);

    // Verify the stored mapping uses the smaller key
    let shardline_chunk_hash = parse_xet_hash_hex(&chunk_hash_hex).unwrap();
    let stored =
        shardline_index::DedupeStore::dedupe_shard_mapping(&index_store, &shardline_chunk_hash)
            .unwrap();
    assert!(stored.is_some(), "mapping should exist after rebuild");
    if let Some(stored) = stored {
        assert_eq!(
            stored.shard_object_key(),
            &key_small,
            "expected the smaller shard key to be used"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_dedupe_shard_mappings_too_many_shard_terms_propagates_error() {
    // Exercise the Err(error) => return Err(error.into()) path (line 517).
    // We write a valid shard but use ShardMetadataLimits with max_files=1
    // while the shard contains 2 file records, triggering TooManyShardTerms.
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey};
    use shardline_xet_core::{
        merklehash::{compute_data_hash, file_hash, xorb_hash},
        metadata_shard::{
            file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
            shard_in_memory::MDBInMemoryShard,
            xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
        },
    };
    use std::num::NonZeroUsize;

    // Build a valid shard with two file records (different file content)
    // and one xorb block. With max_files = NonZeroUsize::MIN (=1),
    // processing the second file record triggers TooManyShardTerms.
    let chunk_data1 = b"file-one-data-aaaa";
    let chunk_data2 = b"file-two-data-bbbb";
    let chunk_hash1 = compute_data_hash(chunk_data1);
    let chunk_hash2 = compute_data_hash(chunk_data2);
    let xorb_hash = xorb_hash(&[
        (chunk_hash1, chunk_data1.len() as u64),
        (chunk_hash2, chunk_data2.len() as u64),
    ]);
    let file_hash_val = file_hash(&[(chunk_hash1, chunk_data1.len() as u64)]);
    let file_hash_val2 = file_hash(&[(chunk_hash2, chunk_data2.len() as u64)]);

    let mut shard = MDBInMemoryShard::default();

    // File record #1
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash_val, 1u64, false, false),
            segments: vec![FileDataSequenceEntry::new(
                xorb_hash,
                chunk_data1.len() as u64,
                0_u64,
                1_u64,
            )],
            verification: Vec::new(),
            metadata_ext: None,
        })
        .unwrap();

    // File record #2
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash_val2, 1u64, false, false),
            segments: vec![FileDataSequenceEntry::new(
                xorb_hash,
                chunk_data2.len() as u64,
                1_u64,
                1_u64,
            )],
            verification: Vec::new(),
            metadata_ext: None,
        })
        .unwrap();
    assert_eq!(
        shard.num_file_entries(),
        2,
        "expected two distinct file records in the shard"
    );

    // Xorb block covering both chunks
    shard
        .add_xorb_block(MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(
                xorb_hash,
                2_u64,
                (chunk_data1.len() + chunk_data2.len()) as u64,
            ),
            chunks: vec![
                XorbChunkSequenceEntry::new(chunk_hash1, chunk_data1.len() as u64, 0_u64)
                    .with_global_dedup_flag(true),
                XorbChunkSequenceEntry::new(chunk_hash2, chunk_data2.len() as u64, 1_u64)
                    .with_global_dedup_flag(true),
            ],
        })
        .unwrap();
    let shard_bytes = shard.to_bytes().unwrap();

    // Verify the shard triggers TooManyShardTerms with max_files=1 directly.
    {
        let tight_limits = shardline_server_core::ShardMetadataLimits::new(
            NonZeroUsize::MIN,
            shardline_server_core::DEFAULT_MAX_SHARD_XORBS,
            shardline_server_core::DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS,
            shardline_server_core::DEFAULT_MAX_SHARD_XORB_CHUNKS,
        );
        let result = shardline_xet_adapter::retained_shard_chunk_hashes(&shard_bytes, tight_limits);
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                shardline_xet_adapter::XetAdapterError::TooManyShardTerms
            ),
            "expected TooManyShardTerms, got: {err:?}"
        );
    }

    // Use a hash from the shard for the object key
    let chunk_hash_hex = chunk_hash1.hex();
    let storage = shardline_test_support::TempStorage::new();
    let index_store = shardline_index::MemoryIndexStore::new();
    let object_root = storage.path().join("chunks");
    std::fs::create_dir_all(&object_root).unwrap();
    let object_store = ServerObjectStore::local(&object_root).unwrap();

    let shard_key = ObjectKey::parse(&format!("shards/ab/{}", chunk_hash_hex)).unwrap();
    object_store
        .put_overwrite(
            &shard_key,
            ObjectBody::from_slice(&shard_bytes),
            &ObjectIntegrity::new(
                shardline_server_core::chunk_hash(&shard_bytes),
                shard_bytes.len() as u64,
            ),
        )
        .unwrap();

    let tight_limits = shardline_server_core::ShardMetadataLimits::new(
        NonZeroUsize::MIN,
        shardline_server_core::DEFAULT_MAX_SHARD_XORBS,
        shardline_server_core::DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS,
        shardline_server_core::DEFAULT_MAX_SHARD_XORB_CHUNKS,
    );

    let mut report = empty_report();
    let result =
        rebuild_dedupe_shard_mappings(&index_store, &object_store, tight_limits, &mut report).await;

    let err = result.unwrap_err();
    assert!(
        matches!(
            &err,
            RebuildError::XetAdapter(shardline_xet_adapter::XetAdapterError::TooManyShardTerms)
        ),
        "expected TooManyShardTerms error, got: {err:?}"
    );
    // One shard was scanned before the error occurred
    assert_eq!(report.scanned_retained_shards, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_dedupe_shard_mappings_early_return_when_issues_present() {
    // Test the early return at lines 535-537 of rebuild_dedupe_shard_mappings.
    // When there are already issues in the report, the function skips
    // the mapping reconciliation entirely.
    use shardline_index::DedupeStore;
    use shardline_protocol::ShardlineHash;

    let storage = shardline_test_support::TempStorage::new();
    let index_store = shardline_index::MemoryIndexStore::new();
    let object_root = storage.path().join("chunks");
    std::fs::create_dir_all(&object_root).unwrap();
    let object_store = ServerObjectStore::local(&object_root).unwrap();

    // Insert a stale dedupe mapping
    let chunk_hash = ShardlineHash::from_bytes([42; 32]);
    let shard_key = shardline_storage::ObjectKey::parse("shards/ab/stale.shard").unwrap();
    let mapping = shardline_index::DedupeShardMapping::new(chunk_hash, shard_key);
    index_store.upsert_dedupe_shard_mapping(&mapping).unwrap();

    // Write an invalid shard to trigger an issue during scan.
    use shardline_storage::{ObjectBody, ObjectIntegrity};
    let bad_key = shardline_storage::ObjectKey::parse("shards/ab/garbage.shard").unwrap();
    object_store
        .put_overwrite(
            &bad_key,
            ObjectBody::from_slice(b"not a shard"),
            &ObjectIntegrity::new(shardline_server_core::chunk_hash(b"not a shard"), 11),
        )
        .unwrap();

    let mut report = empty_report();
    rebuild_dedupe_shard_mappings(
        &index_store,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        &mut report,
    )
    .await
    .unwrap();

    // Issue was created for the invalid shard
    assert!(!report.is_clean());
    assert_eq!(report.scanned_retained_shards, 1);
    assert_eq!(report.issue_count(), 1);

    // The stale dedupe mapping should NOT have been removed because the
    // function returned early due to issues.
    let remaining = DedupeStore::list_dedupe_shard_mappings(&index_store).unwrap();
    assert_eq!(
        remaining.len(),
        1,
        "stale mapping should not be removed when early-returning due to issues"
    );
}

// ---- run_index_rebuild_with_stores through reconstruction path ----

#[tokio::test(flavor = "multi_thread")]
async fn run_index_rebuild_reconstruction_preserved_when_file_id_is_hash() {
    use shardline_index::{
        FileId, FileReconstruction, MemoryIndexStore, ReconstructionTerm, RecordMutation,
    };
    use shardline_protocol::{ChunkRange, ShardlineHash};

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let object_root = root.join("chunks");
    std::fs::create_dir_all(&object_root).unwrap();
    let object_store = ServerObjectStore::local(&object_root).unwrap();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = MemoryIndexStore::new();

    // Use a consistent ShardlineHash for both file_id and reconstruction.
    let record_file_id_hash = ShardlineHash::from_bytes([0x0a; 32]);
    let file_id_hex = shardline_index::xet_hash_hex_string(record_file_id_hash);
    let chunks = Vec::new();
    let content_hash = shardline_server_core::content_hash(0, 0, &chunks);
    let record = shardline_index::FileRecord {
        file_id: file_id_hex,
        content_hash,
        total_bytes: 0,
        chunk_size: 0,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks,
    };
    record_store.write_version_record(&record).await.unwrap();

    // Also insert a reconstruction with the same file_id hex
    // so it should be preserved (not stale).
    let file_id_obj = FileId::new(record_file_id_hash);
    let xorb_id = shardline_index::XorbId::new(ShardlineHash::from_bytes([0x0b; 32]));
    let range = ChunkRange::new(0, 1).unwrap();
    let term = ReconstructionTerm::new(xorb_id, range, 64);
    let reconstruction = FileReconstruction::new(vec![term]);
    index_store
        .insert_reconstruction(&file_id_obj, &reconstruction)
        .unwrap();

    let report = run_index_rebuild_with_stores(
        &record_store,
        &index_store,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();
    assert!(report.is_clean(), "report: {report:?}");
    assert_eq!(report.scanned_version_records, 1);
    assert_eq!(report.rebuilt_latest_records, 1);

    // The reconstruction should have been preserved (unchanged) because
    // the file_id hex matches the desired set.
    assert_eq!(
        report.scanned_reconstructions, 1,
        "should scan one reconstruction"
    );
    assert_eq!(
        report.unchanged_reconstructions, 1,
        "reconstruction with matching file_id should be unchanged"
    );
    assert_eq!(report.removed_stale_reconstructions, 0);
}
