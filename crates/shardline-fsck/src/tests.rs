use std::path::PathBuf;

use shardline_index::{FileRecord, FileRecordInvariantError};
use shardline_server_core::{
    InvalidSerializedShardError, OpsRecordKind, ServerObjectStore, chunk_hash, content_hash,
};
use shardline_storage::ObjectKey;
use shardline_xet_adapter::XorbParseError;

use super::*;

fn clean_report() -> FsckReport {
    FsckReport {
        latest_records: 0,
        version_records: 0,
        inspected_chunk_references: 0,
        inspected_dedupe_shard_mappings: 0,
        inspected_reconstructions: 0,
        inspected_webhook_deliveries: 0,
        inspected_provider_repository_states: 0,
        issues: Vec::new(),
    }
}

#[test]
fn is_clean_returns_true_for_empty_issues() {
    assert!(clean_report().is_clean());
}

#[test]
fn is_clean_returns_false_when_issues_present() {
    let mut report = clean_report();
    report.issues.push(FsckIssue {
        kind: FsckIssueKind::MissingChunk,
        location: "test".to_owned(),
        detail: FsckIssueDetail::HashMismatch {
            expected_hash: "a".repeat(64),
            observed_hash: "b".repeat(64),
        },
    });
    assert!(!report.is_clean());
}

#[test]
fn issue_count_zero_for_clean_report() {
    assert_eq!(clean_report().issue_count(), 0);
}

#[test]
fn issue_count_matches_issues_length() {
    let mut report = clean_report();
    report.issues.push(FsckIssue {
        kind: FsckIssueKind::MissingChunk,
        location: "a".to_owned(),
        detail: FsckIssueDetail::HashMismatch {
            expected_hash: "a".repeat(64),
            observed_hash: "b".repeat(64),
        },
    });
    report.issues.push(FsckIssue {
        kind: FsckIssueKind::ChunkHashMismatch,
        location: "b".to_owned(),
        detail: FsckIssueDetail::InvalidChunkHash {
            chunk_hash: "c".repeat(64),
        },
    });
    assert_eq!(report.issue_count(), 2);
}

// ── push_issue ──────────────────────────────────────────────────────

#[test]
fn push_issue_appends_to_report() {
    let mut report = clean_report();
    let detail = FsckIssueDetail::HashMismatch {
        expected_hash: "a".repeat(64),
        observed_hash: "b".repeat(64),
    };
    push_issue(
        &mut report,
        FsckIssueKind::MissingChunk,
        "loc1".to_owned(),
        detail.clone(),
    )
    .unwrap();
    assert_eq!(report.issue_count(), 1);
    assert_eq!(report.issues[0].kind, FsckIssueKind::MissingChunk);
    assert_eq!(report.issues[0].location, "loc1");
    assert_eq!(report.issues[0].detail, detail);
}

#[test]
fn push_issue_increments_count_for_multiple_issues() {
    let mut report = clean_report();
    for i in 0..5 {
        push_issue(
            &mut report,
            FsckIssueKind::EmptyChunk,
            format!("loc{i}"),
            FsckIssueDetail::ReconstructionContainedNoTerms,
        )
        .unwrap();
    }
    assert_eq!(report.issue_count(), 5);
}

// ── push_reconstruction_plan_issue ──────────────────────────────────

#[test]
fn reconstruction_plan_issue_chunk_hash_maps_to_invalid_content_hash() {
    let mut report = clean_report();
    let err =
        FileRecordInvariantError::ChunkHash(shardline_protocol::HashParseError::InvalidLength);
    push_reconstruction_plan_issue(&mut report, "loc".to_owned(), &err).unwrap();
    assert_eq!(report.issue_count(), 1);
    assert_eq!(report.issues[0].kind, FsckIssueKind::InvalidContentHash);
    assert_eq!(
        report.issues[0].detail,
        FsckIssueDetail::InvalidReconstructionPlan(FsckReconstructionPlanDetail::ChunkHashInvalid)
    );
}

#[test]
fn reconstruction_plan_issue_empty_chunk() {
    let mut report = clean_report();
    let err = FileRecordInvariantError::EmptyChunk;
    push_reconstruction_plan_issue(&mut report, "loc".to_owned(), &err).unwrap();
    assert_eq!(report.issues[0].kind, FsckIssueKind::EmptyChunk);
    assert_eq!(
        report.issues[0].detail,
        FsckIssueDetail::InvalidReconstructionPlan(FsckReconstructionPlanDetail::EmptyChunk)
    );
}

#[test]
fn reconstruction_plan_issue_non_contiguous_chunk_offsets() {
    let mut report = clean_report();
    let err = FileRecordInvariantError::NonContiguousChunkOffsets;
    push_reconstruction_plan_issue(&mut report, "loc".to_owned(), &err).unwrap();
    assert_eq!(report.issues[0].kind, FsckIssueKind::NonContiguousChunks);
    assert_eq!(
        report.issues[0].detail,
        FsckIssueDetail::InvalidReconstructionPlan(
            FsckReconstructionPlanDetail::NonContiguousChunkOffsets
        )
    );
}

#[test]
fn reconstruction_plan_issue_invalid_chunk_range() {
    let mut report = clean_report();
    let err = FileRecordInvariantError::InvalidChunkRange;
    push_reconstruction_plan_issue(&mut report, "loc".to_owned(), &err).unwrap();
    assert_eq!(report.issues[0].kind, FsckIssueKind::InvalidChunkRange);
    assert_eq!(
        report.issues[0].detail,
        FsckIssueDetail::InvalidReconstructionPlan(FsckReconstructionPlanDetail::InvalidChunkRange)
    );
}

#[test]
fn reconstruction_plan_issue_invalid_packed_range() {
    let mut report = clean_report();
    let err = FileRecordInvariantError::InvalidPackedRange;
    push_reconstruction_plan_issue(&mut report, "loc".to_owned(), &err).unwrap();
    assert_eq!(report.issues[0].kind, FsckIssueKind::InvalidPackedRange);
    assert_eq!(
        report.issues[0].detail,
        FsckIssueDetail::InvalidReconstructionPlan(
            FsckReconstructionPlanDetail::InvalidPackedRange
        )
    );
}

#[test]
fn reconstruction_plan_issue_length_overflow() {
    let mut report = clean_report();
    let err = FileRecordInvariantError::LengthOverflow;
    push_reconstruction_plan_issue(&mut report, "loc".to_owned(), &err).unwrap();
    assert_eq!(report.issues[0].kind, FsckIssueKind::TotalBytesMismatch);
    assert_eq!(
        report.issues[0].detail,
        FsckIssueDetail::InvalidReconstructionPlan(FsckReconstructionPlanDetail::LengthOverflow)
    );
}

#[test]
fn reconstruction_plan_issue_total_bytes_mismatch() {
    let mut report = clean_report();
    let err = FileRecordInvariantError::TotalBytesMismatch;
    push_reconstruction_plan_issue(&mut report, "loc".to_owned(), &err).unwrap();
    assert_eq!(report.issues[0].kind, FsckIssueKind::TotalBytesMismatch);
    assert_eq!(
        report.issues[0].detail,
        FsckIssueDetail::InvalidReconstructionPlan(
            FsckReconstructionPlanDetail::TotalBytesMismatch
        )
    );
}

// ── reconstruction_plan_error_detail ─────────────────────────────────

#[test]
fn reconstruction_plan_error_detail_chunk_hash() {
    let err = FileRecordInvariantError::ChunkHash(
        shardline_protocol::HashParseError::InvalidCharacter("test".to_owned()),
    );
    assert_eq!(
        reconstruction_plan_error_detail(&err),
        FsckReconstructionPlanDetail::ChunkHashInvalid
    );
}

#[test]
fn reconstruction_plan_error_detail_empty_chunk() {
    let err = FileRecordInvariantError::EmptyChunk;
    assert_eq!(
        reconstruction_plan_error_detail(&err),
        FsckReconstructionPlanDetail::EmptyChunk
    );
}

#[test]
fn reconstruction_plan_error_detail_non_contiguous() {
    let err = FileRecordInvariantError::NonContiguousChunkOffsets;
    assert_eq!(
        reconstruction_plan_error_detail(&err),
        FsckReconstructionPlanDetail::NonContiguousChunkOffsets
    );
}

#[test]
fn reconstruction_plan_error_detail_invalid_chunk_range() {
    let err = FileRecordInvariantError::InvalidChunkRange;
    assert_eq!(
        reconstruction_plan_error_detail(&err),
        FsckReconstructionPlanDetail::InvalidChunkRange
    );
}

#[test]
fn reconstruction_plan_error_detail_invalid_packed_range() {
    let err = FileRecordInvariantError::InvalidPackedRange;
    assert_eq!(
        reconstruction_plan_error_detail(&err),
        FsckReconstructionPlanDetail::InvalidPackedRange
    );
}

#[test]
fn reconstruction_plan_error_detail_length_overflow() {
    let err = FileRecordInvariantError::LengthOverflow;
    assert_eq!(
        reconstruction_plan_error_detail(&err),
        FsckReconstructionPlanDetail::LengthOverflow
    );
}

#[test]
fn reconstruction_plan_error_detail_total_bytes_mismatch() {
    let err = FileRecordInvariantError::TotalBytesMismatch;
    assert_eq!(
        reconstruction_plan_error_detail(&err),
        FsckReconstructionPlanDetail::TotalBytesMismatch
    );
}

// ── FsckIssueKind::as_str ───────────────────────────────────────────

#[test]
fn fsck_issue_kind_as_str_all_variants() {
    let cases: &[(FsckIssueKind, &str)] = &[
        (
            FsckIssueKind::OversizedRecordMetadata,
            "oversized_record_metadata",
        ),
        (FsckIssueKind::InvalidRecordJson, "invalid_record_json"),
        (FsckIssueKind::InvalidFileId, "invalid_file_id"),
        (FsckIssueKind::InvalidContentHash, "invalid_content_hash"),
        (FsckIssueKind::RecordPathMismatch, "record_path_mismatch"),
        (FsckIssueKind::NonContiguousChunks, "non_contiguous_chunks"),
        (FsckIssueKind::EmptyChunk, "empty_chunk"),
        (FsckIssueKind::TotalBytesMismatch, "total_bytes_mismatch"),
        (FsckIssueKind::InvalidChunkRange, "invalid_chunk_range"),
        (FsckIssueKind::InvalidPackedRange, "invalid_packed_range"),
        (FsckIssueKind::RecordHashMismatch, "record_hash_mismatch"),
        (FsckIssueKind::MissingChunk, "missing_chunk"),
        (FsckIssueKind::ChunkHashMismatch, "chunk_hash_mismatch"),
        (FsckIssueKind::ChunkLengthMismatch, "chunk_length_mismatch"),
        (
            FsckIssueKind::MissingVersionRecord,
            "missing_version_record",
        ),
        (
            FsckIssueKind::MismatchedVersionRecord,
            "mismatched_version_record",
        ),
        (
            FsckIssueKind::MissingDedupeShardObject,
            "missing_dedupe_shard_object",
        ),
        (
            FsckIssueKind::InvalidRetainedShard,
            "invalid_retained_shard",
        ),
        (
            FsckIssueKind::InvalidDedupeShardMapping,
            "invalid_dedupe_shard_mapping",
        ),
        (FsckIssueKind::EmptyReconstruction, "empty_reconstruction"),
        (
            FsckIssueKind::MissingReconstructionXorb,
            "missing_reconstruction_xorb",
        ),
        (
            FsckIssueKind::InvalidQuarantineCandidate,
            "invalid_quarantine_candidate",
        ),
        (
            FsckIssueKind::MissingQuarantinedObject,
            "missing_quarantined_object",
        ),
        (
            FsckIssueKind::QuarantineLengthMismatch,
            "quarantine_length_mismatch",
        ),
        (
            FsckIssueKind::ReachableQuarantinedObject,
            "reachable_quarantined_object",
        ),
        (
            FsckIssueKind::InvalidRetentionHold,
            "invalid_retention_hold",
        ),
        (FsckIssueKind::MissingHeldObject, "missing_held_object"),
        (
            FsckIssueKind::HeldQuarantinedObject,
            "held_quarantined_object",
        ),
        (
            FsckIssueKind::InvalidWebhookDeliveryTimestamp,
            "invalid_webhook_delivery_timestamp",
        ),
        (
            FsckIssueKind::InvalidProviderRepositoryState,
            "invalid_provider_repository_state",
        ),
        (
            FsckIssueKind::InvalidProviderRepositoryStateTimestamp,
            "invalid_provider_repository_state_timestamp",
        ),
    ];
    for &(kind, expected) in cases {
        assert_eq!(kind.as_str(), expected, "variant {kind:?}");
    }
}

// ── ProviderRepositoryStateTimestampField::as_str ────────────────────

#[test]
fn provider_repository_state_timestamp_field_as_str_all_variants() {
    let cases: &[(ProviderRepositoryStateTimestampField, &str)] = &[
        (
            ProviderRepositoryStateTimestampField::LastAccessChangedAtUnixSeconds,
            "last_access_changed_at_unix_seconds",
        ),
        (
            ProviderRepositoryStateTimestampField::LastRevisionPushedAtUnixSeconds,
            "last_revision_pushed_at_unix_seconds",
        ),
        (
            ProviderRepositoryStateTimestampField::LastCacheInvalidatedAtUnixSeconds,
            "last_cache_invalidated_at_unix_seconds",
        ),
        (
            ProviderRepositoryStateTimestampField::LastAuthorizationRecheckedAtUnixSeconds,
            "last_authorization_rechecked_at_unix_seconds",
        ),
        (
            ProviderRepositoryStateTimestampField::LastDriftCheckedAtUnixSeconds,
            "last_drift_checked_at_unix_seconds",
        ),
    ];
    for &(field, expected) in cases {
        assert_eq!(field.as_str(), expected, "variant {field:?}");
    }
}

// ── ProviderRepositoryStateTimestampField Display ────────────────────

#[test]
fn provider_repository_state_timestamp_field_display_matches_as_str() {
    let all_fields = [
        ProviderRepositoryStateTimestampField::LastAccessChangedAtUnixSeconds,
        ProviderRepositoryStateTimestampField::LastRevisionPushedAtUnixSeconds,
        ProviderRepositoryStateTimestampField::LastCacheInvalidatedAtUnixSeconds,
        ProviderRepositoryStateTimestampField::LastAuthorizationRecheckedAtUnixSeconds,
        ProviderRepositoryStateTimestampField::LastDriftCheckedAtUnixSeconds,
    ];
    for field in all_fields {
        assert_eq!(
            field.to_string(),
            field.as_str(),
            "Display mismatch for {field:?}"
        );
    }
}

// ── RecordKind::ops ─────────────────────────────────────────────────

#[test]
fn record_kind_latest_ops_returns_ops_record_kind_latest() {
    assert_eq!(RecordKind::Latest.ops(), OpsRecordKind::Latest);
}

#[test]
fn record_kind_version_ops_returns_ops_record_kind_version() {
    assert_eq!(RecordKind::Version.ops(), OpsRecordKind::Version);
}

// ── object_key_storage_path ──────────────────────────────────────────

#[test]
fn object_key_storage_path_joins_root_and_key() {
    let root = PathBuf::from("/data/chunks");
    let key = ObjectKey::parse("shards/aa/bb/hash.xorb").unwrap();
    let result = object_key_storage_path(&root, &key);
    assert_eq!(result, PathBuf::from("/data/chunks/shards/aa/bb/hash.xorb"));
}

#[test]
fn object_key_storage_path_handles_single_segment_key() {
    let root = PathBuf::from("/storage");
    let key = ObjectKey::parse("file.bin").unwrap();
    let result = object_key_storage_path(&root, &key);
    assert_eq!(result, PathBuf::from("/storage/file.bin"));
}

// ── Error conversions ────────────────────────────────────────────────

#[test]
fn fsck_error_from_stored_file_metadata_too_large() {
    let err = shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
        observed_bytes: 999,
        maximum_bytes: 100,
    };
    let fsck_err: FsckError = err.into();
    assert!(matches!(
        fsck_err,
        FsckError::StoredFileMetadataTooLarge {
            observed_bytes: 999,
            maximum_bytes: 100,
        }
    ));
}

#[test]
fn fsck_error_from_stored_file_record_json_error() {
    let serde_err = serde_json::from_str::<serde_json::Value>("not valid json {{{").unwrap_err();
    let err = shardline_server_core::ParseStoredFileRecordError::Json(serde_err);
    let fsck_err: FsckError = err.into();
    assert!(matches!(fsck_err, FsckError::Json(_)));
}

#[test]
fn fsck_error_from_validate_identifier_error() {
    let err = shardline_server_core::ValidateIdentifierError;
    let fsck_err: FsckError = err.into();
    assert!(matches!(fsck_err, FsckError::Overflow));
}

#[test]
fn fsck_error_from_validate_content_hash_error() {
    let err = shardline_server_core::ValidateContentHashError;
    let fsck_err: FsckError = err.into();
    assert!(matches!(fsck_err, FsckError::Overflow));
}

#[test]
fn fsck_error_from_rebuild_overflow_error() {
    let err = shardline_server_core::RebuildOverflowError;
    let fsck_err: FsckError = err.into();
    assert!(matches!(fsck_err, FsckError::Overflow));
}

#[test]
fn fsck_error_from_xorb_parse_error() {
    let err = XorbParseError::HashMismatch;
    let fsck_err: FsckError = err.into();
    assert!(matches!(fsck_err, FsckError::Overflow));
}

#[test]
fn fsck_error_from_hash_parse_error() {
    let err = shardline_protocol::HashParseError::InvalidLength;
    let fsck_err: FsckError = err.into();
    assert!(matches!(fsck_err, FsckError::Overflow));
}

// ── run_fsck_with_stores integration ─────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn run_fsck_with_empty_stores_returns_clean_report() {
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().to_path_buf();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = shardline_index::LocalIndexStore::open(root.clone());
    let object_root = root.join("chunks");
    let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

    let report = run_fsck_with_stores(
        &record_store,
        &index_store,
        &object_root,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();

    assert!(
        report.is_clean(),
        "empty stores should produce clean report"
    );
    assert_eq!(report.latest_records, 0);
    assert_eq!(report.version_records, 0);
    assert_eq!(report.inspected_dedupe_shard_mappings, 0);
    assert_eq!(report.inspected_reconstructions, 0);
    assert_eq!(report.inspected_webhook_deliveries, 0);
    assert_eq!(report.inspected_provider_repository_states, 0);
}

// ── FsckIssueDetail Display ────────────────────────────────────────

#[test]
fn fsck_issue_detail_display_all_variants() {
    let cases: &[(FsckIssueDetail, &str)] = &[
        (
            FsckIssueDetail::MissingVersionRecord {
                version_locator: "loc".to_owned(),
            },
            "version record",
        ),
        (FsckIssueDetail::OversizedRecordMetadata, "ceiling"),
        (FsckIssueDetail::RecordJsonInvalid, "json"),
        (
            FsckIssueDetail::InvalidFileId {
                file_id: "bad".to_owned(),
            },
            "bad",
        ),
        (
            FsckIssueDetail::InvalidContentHash {
                content_hash: "badhash".to_owned(),
            },
            "badhash",
        ),
        (
            FsckIssueDetail::RecordPathMismatch {
                expected_locator: "expected".to_owned(),
            },
            "expected",
        ),
        (FsckIssueDetail::RecordFileIdPathMismatch, "path"),
        (FsckIssueDetail::RecordContentHashPathMismatch, "hash"),
        (
            FsckIssueDetail::InvalidChunkHash {
                chunk_hash: "chunk-hash".to_owned(),
            },
            "chunk-hash",
        ),
        (
            FsckIssueDetail::InvalidXorbHash {
                xorb_hash: "xorb-hash".to_owned(),
            },
            "xorb-hash",
        ),
        (
            FsckIssueDetail::ReferencedByRecord {
                record_location: "loc".to_owned(),
            },
            "loc",
        ),
        (
            FsckIssueDetail::ReferencedByNativeXetRecord {
                record_location: "native-loc".to_owned(),
            },
            "native-loc",
        ),
        (
            FsckIssueDetail::ReferencedByNativeXetXorb {
                xorb_location: "xorb-loc".to_owned(),
            },
            "xorb-loc",
        ),
        (
            FsckIssueDetail::HashMismatch {
                expected_hash: "abc".to_owned(),
                observed_hash: "def".to_owned(),
            },
            "abc",
        ),
        (
            FsckIssueDetail::LengthMismatch {
                expected_length: 10,
                observed_length: 5,
            },
            "10",
        ),
        (
            FsckIssueDetail::XorbRangeExceededChunkCount {
                range_start: 0,
                range_end: 5,
                chunk_count: 3,
            },
            "5",
        ),
        (
            FsckIssueDetail::MismatchedVersionRecord {
                version_locator: "vloc".to_owned(),
            },
            "vloc",
        ),
        (
            FsckIssueDetail::MappedChunkHash {
                chunk_hash: "mapped-hash".to_owned(),
            },
            "mapped-hash",
        ),
        (
            FsckIssueDetail::MappedChunkHashAbsentFromRetainedShard {
                chunk_hash: "absent-hash".to_owned(),
            },
            "absent-hash",
        ),
        (FsckIssueDetail::ReconstructionListedUnreadableRow, "row"),
        (FsckIssueDetail::ReconstructionContainedNoTerms, "terms"),
        (
            FsckIssueDetail::MissingReconstructionXorb {
                xorb_hash: "missing-xorb".to_owned(),
            },
            "missing-xorb",
        ),
    ];
    for (detail, substring) in cases {
        let msg = detail.to_string();
        assert!(!msg.is_empty(), "empty display for {detail:?}");
        assert!(
            msg.contains(substring),
            "expected '{substring}' in '{msg}' from {detail:?}"
        );
    }
}

#[test]
fn fsck_issue_detail_invalid_retained_shard_display() {
    let detail =
        FsckIssueDetail::InvalidRetainedShard(InvalidSerializedShardError::ParserRejectedMetadata);
    let msg = detail.to_string();
    assert!(!msg.is_empty(), "empty display for {detail:?}");
}

#[test]
fn fsck_reconstruction_plan_detail_display_all_variants() {
    let cases: &[(FsckReconstructionPlanDetail, &str)] = &[
        (FsckReconstructionPlanDetail::ChunkHashInvalid, "hash"),
        (FsckReconstructionPlanDetail::EmptyChunk, "empty"),
        (
            FsckReconstructionPlanDetail::NonContiguousChunkOffsets,
            "contiguous",
        ),
        (FsckReconstructionPlanDetail::InvalidChunkRange, "range"),
        (FsckReconstructionPlanDetail::InvalidPackedRange, "range"),
        (FsckReconstructionPlanDetail::LengthOverflow, "overflow"),
        (FsckReconstructionPlanDetail::TotalBytesMismatch, "total"),
    ];
    for (detail, substring) in cases {
        let msg = detail.to_string();
        assert!(!msg.is_empty(), "empty display for {detail:?}");
        assert!(
            msg.contains(substring),
            "expected '{substring}' in '{msg}' from {detail:?}"
        );
    }
}

// ── FsckReconstructionPlanDetail Display ──────────────────────────

#[test]
fn fsck_reconstruction_plan_detail_invalid_reconstruction_plan_display() {
    let detail =
        FsckIssueDetail::InvalidReconstructionPlan(FsckReconstructionPlanDetail::ChunkHashInvalid);
    let msg = detail.to_string();
    assert!(!msg.is_empty());
}

// ── FsckError display ──────────────────────────────────────────────

#[test]
fn fsck_error_display_all_variants() {
    let cases: &[(FsckError, &str)] = &[
        (FsckError::Io(std::io::Error::other("test")), "storage"),
        (
            FsckError::Json(serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err()),
            "json",
        ),
        (
            FsckError::NumericConversion(u64::try_from(-1i32).unwrap_err()),
            "bounds",
        ),
        (FsckError::Overflow, "overflow"),
        (
            FsckError::StoredFileMetadataTooLarge {
                observed_bytes: 999,
                maximum_bytes: 100,
            },
            "ceiling",
        ),
    ];
    for (error, substring) in cases {
        let msg = error.to_string();
        assert!(!msg.is_empty(), "empty display for {error:?}");
        assert!(
            msg.contains(substring),
            "expected '{substring}' in '{msg}' from {error:?}"
        );
    }
}

#[test]
fn fsck_error_from_xorb_parse_error_display() {
    let error: FsckError = XorbParseError::HashMismatch.into();
    let msg = error.to_string();
    assert!(!msg.is_empty());
}

// ── object_location_display: blackhole fallback ────────────────────

#[test]
fn object_location_display_with_blackhole_falls_back_to_storage_path() {
    let object_root = PathBuf::from("/tmp/test-root");
    let object_store = ServerObjectStore::blackhole();
    let key = ObjectKey::parse("ab/cdef1234").unwrap();
    let display = object_location_display(&object_root, &object_store, &key);
    assert!(
        display.contains("ab/cdef1234"),
        "expected key in display, got: {display}"
    );
    assert!(
        display.contains("/tmp/test-root"),
        "expected root in display, got: {display}"
    );
}

// ── run_fsck_with_stores: missing dedupe shard object ────────────

#[tokio::test(flavor = "multi_thread")]
async fn run_fsck_with_missing_dedupe_shard_object_detected() {
    use shardline_index::{DedupeShardMapping, MemoryIndexStore};
    use shardline_protocol::ShardlineHash;

    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().to_path_buf();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = MemoryIndexStore::new();
    let object_root = root.join("chunks");
    let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

    // Insert a dedupe shard mapping pointing to a shard that does not exist in object storage
    let chunk_hash = ShardlineHash::from_bytes([42; 32]);
    let shard_key = ObjectKey::parse("shards/aa/missing.shard").unwrap();
    let mapping = DedupeShardMapping::new(chunk_hash, shard_key);
    index_store.upsert_dedupe_shard_mapping(&mapping).unwrap();

    let report = run_fsck_with_stores(
        &record_store,
        &index_store,
        &object_root,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();

    assert!(
        !report.is_clean(),
        "expected issues for missing shard object"
    );
    assert_eq!(report.inspected_dedupe_shard_mappings, 1);
    assert!(
        report
            .issues
            .iter()
            .any(|i| i.kind == FsckIssueKind::MissingDedupeShardObject),
        "expected MissingDedupeShardObject issue, got: {:#?}",
        report.issues
    );
}

// ── run_fsck_with_stores: invalid retained shard ──────────────────

#[tokio::test(flavor = "multi_thread")]
async fn run_fsck_with_invalid_retained_shard_detected() {
    use shardline_index::{DedupeShardMapping, MemoryIndexStore};
    use shardline_protocol::ShardlineHash;

    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().to_path_buf();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = MemoryIndexStore::new();
    let object_root = root.join("chunks");
    let _object_store = ServerObjectStore::local(object_root.clone()).unwrap();

    // Insert a dedupe shard mapping pointing to a shard key
    let chunk_hash = ShardlineHash::from_bytes([42; 32]);
    let shard_key = ObjectKey::parse("shards/aa/invalid.shard").unwrap();
    let mapping = DedupeShardMapping::new(chunk_hash, shard_key.clone());
    index_store.upsert_dedupe_shard_mapping(&mapping).unwrap();

    // Create an object at the shard key with garbage bytes (not a valid shard).
    // Write directly to the local filesystem.
    let shard_path = object_root.join(shard_key.as_str());
    if let Some(parent) = shard_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&shard_path, b"not a valid shard").unwrap();

    // Re-create the object store so it picks up the written file
    let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

    let report = run_fsck_with_stores(
        &record_store,
        &index_store,
        &object_root,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();

    assert!(
        !report.is_clean(),
        "expected issues for invalid retained shard"
    );
    assert_eq!(report.inspected_dedupe_shard_mappings, 1);
    assert!(
        report
            .issues
            .iter()
            .any(|i| i.kind == FsckIssueKind::InvalidRetainedShard),
        "expected InvalidRetainedShard issue, got: {:#?}",
        report.issues
    );
}

// ── run_fsck_with_stores: empty reconstruction ───────────────────

#[tokio::test(flavor = "multi_thread")]
async fn run_fsck_with_empty_reconstruction_detected() {
    use shardline_index::{FileId, FileReconstruction, MemoryIndexStore};
    use shardline_protocol::ShardlineHash;
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().to_path_buf();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = MemoryIndexStore::new();
    let object_root = root.join("chunks");
    let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

    // Insert a reconstruction with empty terms
    let hash = ShardlineHash::from_bytes([99; 32]);
    let file_id = FileId::new(hash);
    let reconstruction = FileReconstruction::new(vec![]);
    index_store
        .insert_reconstruction(&file_id, &reconstruction)
        .unwrap();

    let report = run_fsck_with_stores(
        &record_store,
        &index_store,
        &object_root,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();

    assert!(
        !report.is_clean(),
        "expected issues for empty reconstruction"
    );
    assert_eq!(report.inspected_reconstructions, 1);
    assert!(
        report
            .issues
            .iter()
            .any(|i| i.kind == FsckIssueKind::EmptyReconstruction),
        "expected EmptyReconstruction issue, got: {:#?}",
        report.issues
    );
}

// ── FsckError From impls for store errors ───────────────────────────

#[test]
fn fsck_error_from_local_object_store_error() {
    use shardline_storage::LocalObjectStoreError;
    let io_err = std::io::Error::other("test");
    let err = FsckError::from(LocalObjectStoreError::Io(io_err));
    let msg = err.to_string();
    assert!(!msg.is_empty());
}

#[test]
fn fsck_error_from_s3_object_store_error() {
    use shardline_storage::S3ObjectStoreError;
    let s3_err = S3ObjectStoreError::IncompleteCredentials;
    let err = FsckError::from(s3_err);
    let msg = err.to_string();
    assert!(!msg.is_empty());
}

#[test]
fn fsck_error_from_server_object_store_error() {
    use shardline_server_core::ServerObjectStoreError;
    let err = FsckError::from(ServerObjectStoreError::NotFound);
    let msg = err.to_string();
    assert!(!msg.is_empty());
}

#[test]
fn fsck_error_from_xet_adapter_error() {
    use shardline_xet_adapter::XetAdapterError;
    let io_err = std::io::Error::other("test");
    let err = FsckError::from(XetAdapterError::Io(io_err));
    let msg = err.to_string();
    assert!(!msg.is_empty());
}

#[test]
fn fsck_error_from_local_index_store_error() {
    use shardline_index::LocalIndexStoreError;
    let io_err = std::io::Error::other("test");
    let err = FsckError::from(LocalIndexStoreError::Io(io_err));
    let msg = err.to_string();
    assert!(!msg.is_empty());
}

#[test]
fn fsck_error_from_memory_index_store_error() {
    use shardline_index::MemoryIndexStoreError;
    let err = FsckError::from(MemoryIndexStoreError::LockPoisoned("test".to_owned()));
    let msg = err.to_string();
    assert!(!msg.is_empty());
}

#[test]
fn fsck_error_from_memory_record_store_error() {
    use shardline_index::MemoryRecordStoreError;
    let err = FsckError::from(MemoryRecordStoreError::RecordNotFound);
    let msg = err.to_string();
    assert!(!msg.is_empty());
}

#[test]
fn fsck_error_from_postgres_metadata_store_error() {
    use shardline_index::PostgresMetadataStoreError;
    let json_err = serde_json::from_str::<()>("invalid").unwrap_err();
    let err = FsckError::from(PostgresMetadataStoreError::Json(json_err));
    let msg = err.to_string();
    assert!(!msg.is_empty());
}

// ── unix_now_seconds_checked ────────────────────────────────────

#[test]
fn unix_now_seconds_checked_returns_ok() {
    let result = unix_now_seconds_checked();
    assert!(result.is_ok());
    let seconds = result.unwrap();
    assert!(seconds > 1_700_000_000, "timestamp should be plausible");
}

// ── object_location_display ─────────────────────────────────────

#[test]
fn object_location_display_with_local_store() {
    let storage = shardline_test_support::TempStorage::new();
    let object_root = storage.path().join("chunks");
    let object_store = ServerObjectStore::local(object_root.clone()).unwrap();
    let key = ObjectKey::parse("ab/cdef1234").unwrap();
    let display = object_location_display(&object_root, &object_store, &key);
    assert!(!display.is_empty());
    assert!(
        display.contains("ab"),
        "expected display to contain 'ab', got: {display}"
    );
    assert!(
        display.contains("cdef1234"),
        "expected display to contain 'cdef1234', got: {display}"
    );
}

// ── run_local_fsck ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn run_local_fsck_with_temp_dir_returns_clean() {
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().to_path_buf();
    let report = run_local_fsck(root).await.unwrap();
    assert!(report.is_clean());
}

#[tokio::test(flavor = "multi_thread")]
async fn run_local_fsck_with_non_existent_dir_errors() {
    let root = PathBuf::from("/nonexistent/fsck-test-dir-12345");
    let result = run_local_fsck(root).await;
    assert!(result.is_err());
}

// ── run_fsck_with_stores with LocalRecordStore ──────────────────

#[tokio::test(flavor = "multi_thread")]
async fn run_fsck_with_memory_index_store_returns_clean() {
    use shardline_index::MemoryIndexStore;
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().to_path_buf();
    let object_root = root.join("chunks");
    let object_store = ServerObjectStore::local(object_root.clone()).unwrap();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = MemoryIndexStore::new();

    let report = run_fsck_with_stores(
        &record_store,
        &index_store,
        &object_root,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();

    assert!(report.is_clean());
    assert_eq!(report.latest_records, 0);
    assert_eq!(report.version_records, 0);
}

// ── record_path ─────────────────────────────────────────────────

#[test]
fn record_path_latest_returns_latest_record_locator() {
    use shardline_index::RecordTraversal;
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path_buf();
    let record_store = shardline_index::LocalRecordStore::open(root);
    let record = FileRecord {
        file_id: "test-file".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 0,
        chunk_size: 0,
        repository_scope: None,
        chunks: Vec::new(),
    };
    let locator = record_path(&record_store, RecordKind::Latest, &record);
    let expected = record_store.latest_record_locator(&record);
    assert_eq!(locator, expected);
}

#[test]
fn record_path_version_returns_version_record_locator() {
    use shardline_index::RecordTraversal;
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path_buf();
    let record_store = shardline_index::LocalRecordStore::open(root);
    let record = FileRecord {
        file_id: "test-file".to_owned(),
        content_hash: "b".repeat(64),
        total_bytes: 0,
        chunk_size: 0,
        repository_scope: None,
        chunks: Vec::new(),
    };
    let locator = record_path(&record_store, RecordKind::Version, &record);
    let expected = record_store.version_record_locator(&record);
    assert_eq!(locator, expected);
}

// ── FsckIssue Debug ─────────────────────────────────────────────

#[test]
fn fsck_issue_debug_format() {
    let issue = FsckIssue {
        kind: FsckIssueKind::MissingChunk,
        location: "test/location".to_owned(),
        detail: FsckIssueDetail::RecordJsonInvalid,
    };
    let debug = format!("{issue:?}");
    assert!(debug.contains("MissingChunk"), "debug: {debug}");
    assert!(debug.contains("test/location"), "debug: {debug}");
}

#[test]
fn fsck_issue_debug_with_detailed_variant() {
    let issue = FsckIssue {
        kind: FsckIssueKind::ChunkHashMismatch,
        location: "chunk/loc".to_owned(),
        detail: FsckIssueDetail::HashMismatch {
            expected_hash: "abc".to_owned(),
            observed_hash: "def".to_owned(),
        },
    };
    let debug = format!("{issue:?}");
    assert!(debug.contains("ChunkHashMismatch"), "debug: {debug}");
    assert!(debug.contains("abc"), "debug: {debug}");
}

// ── Additional FsckIssueDetail Display variants ─────────────────

#[test]
fn fsck_issue_detail_display_invalid_quarantine_timeline() {
    let detail = FsckIssueDetail::InvalidQuarantineTimeline {
        delete_after_unix_seconds: 10,
        first_seen_unreachable_at_unix_seconds: 100,
    };
    let msg = detail.to_string();
    assert!(msg.contains("10"), "msg: {msg}");
    assert!(msg.contains("100"), "msg: {msg}");
}

#[test]
fn fsck_issue_detail_display_invalid_retention_timeline() {
    let detail = FsckIssueDetail::InvalidRetentionTimeline {
        release_after_unix_seconds: 50,
        held_at_unix_seconds: 100,
    };
    let msg = detail.to_string();
    assert!(msg.contains("50"), "msg: {msg}");
    assert!(msg.contains("100"), "msg: {msg}");
}

#[test]
fn fsck_issue_detail_display_active_retention_hold_reason() {
    let detail = FsckIssueDetail::ActiveRetentionHoldReason {
        reason: "legal hold".to_owned(),
    };
    let msg = detail.to_string();
    assert!(msg.contains("legal hold"), "msg: {msg}");
}

#[test]
fn fsck_issue_detail_display_active_retention_hold_quarantined() {
    let detail = FsckIssueDetail::ActiveRetentionHoldQuarantined;
    let msg = detail.to_string();
    assert!(!msg.is_empty());
}

#[test]
fn fsck_issue_detail_display_webhook_delivery_timestamp_exceeded() {
    let detail = FsckIssueDetail::WebhookDeliveryTimestampExceeded {
        processed_at_unix_seconds: 2000,
        max_allowed_unix_seconds: 1000,
    };
    let msg = detail.to_string();
    assert!(msg.contains("2000"), "msg: {msg}");
}

#[test]
fn fsck_issue_detail_display_provider_repository_identity_invalid() {
    let detail = FsckIssueDetail::ProviderRepositoryIdentityInvalid;
    let msg = detail.to_string();
    assert!(!msg.is_empty());
}

#[test]
fn fsck_issue_detail_display_provider_state_timestamp_exceeded() {
    let detail = FsckIssueDetail::ProviderRepositoryStateTimestampExceeded {
        field: ProviderRepositoryStateTimestampField::LastAccessChangedAtUnixSeconds,
        timestamp: 2000,
        max_allowed_unix_seconds: 1000,
    };
    let msg = detail.to_string();
    assert!(msg.contains("2000"), "msg: {msg}");
    assert!(
        msg.contains("last_access_changed_at_unix_seconds"),
        "msg: {msg}"
    );
}

#[test]
fn fsck_issue_detail_display_quarantine_referenced_missing_object() {
    let detail = FsckIssueDetail::QuarantineReferencedMissingObject;
    let msg = detail.to_string();
    assert!(msg.contains("missing object"), "msg: {msg}");
}

#[test]
fn fsck_issue_detail_display_quarantine_targeted_reachable_object() {
    let detail = FsckIssueDetail::QuarantineTargetedReachableObject;
    let msg = detail.to_string();
    assert!(msg.contains("reachable"), "msg: {msg}");
}

// ── Extra FsckError Display coverage ─────────────────────────────

#[test]
fn fsck_error_display_numeric_conversion() {
    let err = FsckError::NumericConversion(u64::try_from(-1i32).unwrap_err());
    let msg = err.to_string();
    assert!(msg.contains("numeric conversion"), "msg: {msg}");
}

#[test]
fn fsck_error_display_local_object_store() {
    use shardline_storage::LocalObjectStoreError;
    let err = FsckError::LocalObjectStore(LocalObjectStoreError::Io(std::io::Error::other(
        "disk error",
    )));
    let msg = err.to_string();
    assert!(msg.contains("local storage"), "msg: {msg}");
}

#[test]
fn fsck_error_display_s3_object_store() {
    use shardline_storage::S3ObjectStoreError;
    let err = FsckError::S3ObjectStore(S3ObjectStoreError::IncompleteCredentials);
    let msg = err.to_string();
    assert!(msg.contains("s3 object"), "msg: {msg}");
}

#[test]
fn fsck_error_display_xet_adapter() {
    use shardline_xet_adapter::XetAdapterError;
    let err = FsckError::XetAdapter(XetAdapterError::NotFound);
    let msg = err.to_string();
    assert!(msg.contains("xet adapter"), "msg: {msg}");
}

#[test]
fn fsck_error_display_memory_index_store() {
    use shardline_index::MemoryIndexStoreError;
    let err = FsckError::MemoryIndexStore(MemoryIndexStoreError::LockPoisoned("test".to_owned()));
    let msg = err.to_string();
    assert!(msg.contains("memory index"), "msg: {msg}");
}

#[test]
fn fsck_error_display_memory_record_store() {
    use shardline_index::MemoryRecordStoreError;
    let err = FsckError::MemoryRecordStore(MemoryRecordStoreError::RecordNotFound);
    let msg = err.to_string();
    assert!(msg.contains("memory record"), "msg: {msg}");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_fsck_with_missing_reconstruction_detected() {
    use shardline_index::{FileId, FileReconstruction, ReconstructionTerm, StoredObjectId};
    use shardline_protocol::ChunkRange;

    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().to_path_buf();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = shardline_index::MemoryIndexStore::new();
    let object_root = root.join("chunks");
    let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

    // Insert a reconstruction that references a missing xorb
    let hash = shardline_protocol::ShardlineHash::from_bytes([1; 32]);
    let file_id = FileId::new(hash);
    let xorb_hash = shardline_protocol::ShardlineHash::from_bytes([2; 32]);
    let object_id = StoredObjectId::new(xorb_hash);
    let chunk_range = ChunkRange::new(0, 1).unwrap();
    let term = ReconstructionTerm::new(object_id, chunk_range, 100);
    let reconstruction = FileReconstruction::new(vec![term]);
    index_store
        .insert_reconstruction(&file_id, &reconstruction)
        .unwrap();

    let report = run_fsck_with_stores(
        &record_store,
        &index_store,
        &object_root,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();

    assert!(!report.is_clean());
    assert!(
        report
            .issues
            .iter()
            .any(|i| i.kind == FsckIssueKind::MissingReconstructionXorb)
    );
}

// ── run_fsck_with_invalid_dedupe_shard_mapping (hash not in shard) ─

#[tokio::test(flavor = "multi_thread")]
async fn run_fsck_with_invalid_dedupe_shard_mapping_detected() {
    use shardline_index::{DedupeShardMapping, MemoryIndexStore};
    use shardline_protocol::ShardlineHash;
    use shardline_storage::ObjectKey;
    use shardline_xet_core::{
        merklehash::{compute_data_hash, file_hash, xorb_hash},
        metadata_shard::{
            file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
            shard_in_memory::MDBInMemoryShard,
            xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
        },
    };

    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().to_path_buf();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = MemoryIndexStore::new();
    let object_root = root.join("chunks");

    // Build a valid minimal shard with one global-dedup chunk
    let chunk_data = b"valid shard chunk content";
    let chunk_hash = compute_data_hash(chunk_data);
    let xorb_val = xorb_hash(&[(chunk_hash, chunk_data.len() as u64)]);
    let file_hash_val = file_hash(&[(chunk_hash, chunk_data.len() as u64)]);
    let mut shard = MDBInMemoryShard::default();
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash_val, 1u64, false, false),
            segments: vec![FileDataSequenceEntry::new(
                xorb_val,
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
            metadata: XorbChunkSequenceHeader::new(xorb_val, 1_u64, chunk_data.len() as u64),
            chunks: vec![
                XorbChunkSequenceEntry::new(chunk_hash, chunk_data.len() as u64, 0_u64)
                    .with_global_dedup_flag(true),
            ],
        })
        .unwrap();
    let shard_bytes = shard.to_bytes().unwrap();
    let shard_chunk_hash_hex = chunk_hash.hex();

    // Store the shard in object storage
    let shard_key_str = format!(
        "shards/{}/{}",
        &shard_chunk_hash_hex[..2],
        shard_chunk_hash_hex
    );
    let shard_key = ObjectKey::parse(&shard_key_str).unwrap();
    let shard_path = object_root.join(shard_key.as_str());
    if let Some(parent) = shard_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&shard_path, &shard_bytes).unwrap();

    // Re-create the object store after writing
    let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

    // Insert a dedupe mapping for a DIFFERENT chunk hash (not in the shard)
    let missing_chunk_hash = ShardlineHash::from_bytes([99; 32]);
    let mapping = DedupeShardMapping::new(missing_chunk_hash, shard_key);
    index_store.upsert_dedupe_shard_mapping(&mapping).unwrap();

    let report = run_fsck_with_stores(
        &record_store,
        &index_store,
        &object_root,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();

    assert!(
        !report.is_clean(),
        "expected issues for invalid dedupe shard mapping"
    );
    assert_eq!(report.inspected_dedupe_shard_mappings, 1);
    assert!(
        report
            .issues
            .iter()
            .any(|i| i.kind == FsckIssueKind::InvalidDedupeShardMapping),
        "expected InvalidDedupeShardMapping, got: {:#?}",
        report.issues
    );
}

// ── run_fsck_with_dedupe_shard_added_to_reachability ─────────────

#[tokio::test(flavor = "multi_thread")]
async fn run_fsck_with_dedupe_shard_added_to_reachability() {
    use shardline_index::{DedupeShardMapping, MemoryIndexStore, RecordMutation};
    use shardline_storage::ObjectKey;
    use shardline_xet_core::{
        merklehash::{compute_data_hash, file_hash, xorb_hash},
        metadata_shard::{
            file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
            shard_in_memory::MDBInMemoryShard,
            xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
        },
    };

    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().to_path_buf();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = MemoryIndexStore::new();
    let object_root = root.join("chunks");
    let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

    // Build a valid minimal shard
    let chunk_data = b"shard data for reachability test";
    let chunk_hash = compute_data_hash(chunk_data);
    let xorb_val = xorb_hash(&[(chunk_hash, chunk_data.len() as u64)]);
    let file_hash_val = file_hash(&[(chunk_hash, chunk_data.len() as u64)]);
    let mut shard = MDBInMemoryShard::default();
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash_val, 1u64, false, false),
            segments: vec![FileDataSequenceEntry::new(
                xorb_val,
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
            metadata: XorbChunkSequenceHeader::new(xorb_val, 1_u64, chunk_data.len() as u64),
            chunks: vec![
                XorbChunkSequenceEntry::new(chunk_hash, chunk_data.len() as u64, 0_u64)
                    .with_global_dedup_flag(true),
            ],
        })
        .unwrap();
    let shard_bytes = shard.to_bytes().unwrap();
    let chunk_hash_hex = chunk_hash.hex();

    // Store the shard in object storage
    let shard_key_str = format!("shards/{}/{}", &chunk_hash_hex[..2], chunk_hash_hex);
    let shard_key = ObjectKey::parse(&shard_key_str).unwrap();
    let shard_path = object_root.join(shard_key.as_str());
    if let Some(parent) = shard_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&shard_path, &shard_bytes).unwrap();

    // Write a latest record whose chunk hash matches the shard's chunk hash
    // so live_dedupe_chunk_hashes contains it.
    let chunks = vec![shardline_index::FileChunkRecord {
        hash: chunk_hash_hex.clone(),
        offset: 0,
        length: chunk_data.len() as u64,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: chunk_data.len() as u64,
    }];
    let total_bytes = chunk_data.len() as u64;
    let chunk_size = 4096_u64;
    let content_hash_val = content_hash(total_bytes, chunk_size, &chunks);
    let record = FileRecord {
        file_id: "reachable-file".to_owned(),
        content_hash: content_hash_val,
        total_bytes,
        chunk_size,
        repository_scope: None,
        chunks,
    };
    record_store.write_latest_record(&record).await.unwrap();

    // Also need to write the chunk object so the latest record scans cleanly
    let chunk_key = shardline_server_core::chunk_object_key(&chunk_hash_hex).unwrap();
    let chunk_path = object_root.join(chunk_key.as_str());
    if let Some(parent) = chunk_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&chunk_path, chunk_data).unwrap();

    // Insert a dedupe mapping matching the same chunk hash
    let shardline_chunk_hash = shardline_index::parse_xet_hash_hex(&chunk_hash_hex).unwrap();
    let mapping = DedupeShardMapping::new(shardline_chunk_hash, shard_key);
    index_store.upsert_dedupe_shard_mapping(&mapping).unwrap();

    let report = run_fsck_with_stores(
        &record_store,
        &index_store,
        &object_root,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();

    // The dedupe shard should be cleanly scanned and the mapping valid
    assert_eq!(report.inspected_dedupe_shard_mappings, 1);
    // No InvalidDedupeShardMapping issues
    assert!(
        !report
            .issues
            .iter()
            .any(|i| i.kind == FsckIssueKind::InvalidDedupeShardMapping),
        "unexpected InvalidDedupeShardMapping: {:#?}",
        report.issues
    );
    // Should have MissingVersionRecord since no version was written
    assert!(
        report
            .issues
            .iter()
            .any(|i| i.kind == FsckIssueKind::MissingVersionRecord),
        "expected MissingVersionRecord"
    );
}

// ── run_fsck_with_stores: seeded records → clean ─────────────────

#[tokio::test(flavor = "multi_thread")]
async fn run_fsck_with_seeded_valid_records_returns_clean() {
    use shardline_index::{FileChunkRecord, FileRecord, RecordMutation};
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().to_path_buf();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = shardline_index::MemoryIndexStore::new();
    let object_root = root.join("chunks");
    let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

    // Seed a record with a valid chunk that exists on disk
    let chunk_data = b"valid chunk content for fsck test";
    let shardline_hash = chunk_hash(chunk_data);
    let chunk_hash_hex = shardline_index::xet_hash_hex_string(shardline_hash);
    let chunk_key = shardline_server_core::chunk_object_key(&chunk_hash_hex).unwrap();
    let chunk_path = object_root.join(chunk_key.as_str());
    if let Some(parent) = chunk_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&chunk_path, chunk_data).unwrap();

    let record = FileRecord {
        file_id: "seeded-valid-file".to_owned(),
        content_hash: content_hash(
            chunk_data.len() as u64,
            4096,
            &[FileChunkRecord {
                hash: chunk_hash_hex.clone(),
                offset: 0,
                length: chunk_data.len() as u64,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: chunk_data.len() as u64,
            }],
        ),
        total_bytes: chunk_data.len() as u64,
        chunk_size: 4096,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash: chunk_hash_hex,
            offset: 0,
            length: chunk_data.len() as u64,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: chunk_data.len() as u64,
        }],
    };
    RecordMutation::write_version_record(&record_store, &record)
        .await
        .unwrap();
    RecordMutation::write_latest_record(&record_store, &record)
        .await
        .unwrap();

    let report = run_fsck_with_stores(
        &record_store,
        &index_store,
        &object_root,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();

    // Should have scanned the records cleanly (no MissingVersionRecord since we have both)
    assert_eq!(report.latest_records, 1);
    assert_eq!(report.version_records, 1);
    assert_eq!(report.inspected_chunk_references, 2); // one from latest, one from version
    // No MissingVersionRecord issues — we have both latest and version
    assert!(
        !report
            .issues
            .iter()
            .any(|i| i.kind == FsckIssueKind::MissingVersionRecord),
        "expected no MissingVersionRecord with both records present"
    );
    // Should not have chunk hash or length issues
    assert!(
        !report.issues.iter().any(|i| matches!(
            i.kind,
            FsckIssueKind::ChunkHashMismatch
                | FsckIssueKind::ChunkLengthMismatch
                | FsckIssueKind::MissingChunk
        )),
        "expected no chunk integrity issues: {:#?}",
        report.issues
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn run_fsck_with_corrupted_chunk_data_detects_hash_mismatch() {
    use shardline_index::{FileChunkRecord, FileRecord, RecordMutation};
    let storage = shardline_test_support::TempStorage::new();
    let root = storage.path().to_path_buf();
    let record_store = shardline_index::LocalRecordStore::open(root.clone());
    let index_store = shardline_index::MemoryIndexStore::new();
    let object_root = root.join("chunks");
    let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

    // Write a chunk with "good data"
    let chunk_data = b"original correct data for the chunk";
    let shardline_hash = chunk_hash(chunk_data);
    let chunk_hash_hex = shardline_index::xet_hash_hex_string(shardline_hash);
    let chunk_key = shardline_server_core::chunk_object_key(&chunk_hash_hex).unwrap();
    let chunk_path = object_root.join(chunk_key.as_str());
    if let Some(parent) = chunk_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&chunk_path, b"corrupted data that doesn't match the hash").unwrap();

    let record = FileRecord {
        file_id: "corrupted-chunk-file".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: chunk_data.len() as u64,
        chunk_size: 4096,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash: chunk_hash_hex,
            offset: 0,
            length: chunk_data.len() as u64,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: chunk_data.len() as u64,
        }],
    };
    RecordMutation::write_version_record(&record_store, &record)
        .await
        .unwrap();
    RecordMutation::write_latest_record(&record_store, &record)
        .await
        .unwrap();

    let report = run_fsck_with_stores(
        &record_store,
        &index_store,
        &object_root,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
    .unwrap();

    // Should detect chunk hash mismatch
    assert!(
        report
            .issues
            .iter()
            .any(|i| i.kind == FsckIssueKind::ChunkHashMismatch),
        "expected ChunkHashMismatch issue for corrupted chunk: {:#?}",
        report.issues
    );
    assert_eq!(report.inspected_chunk_references, 2); // one from latest, one from version
}
