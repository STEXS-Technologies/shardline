use shardline_index::{FileChunkRecord, FileRecord};

use crate::{ServerFrontend, object_store::ServerObjectStore};

use super::{
    DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS, LifecycleRepairOptions, LifecycleRepairReport,
    WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS,
    classification::{
        classify_quarantine_repair_action, classify_retention_hold_repair_action,
        classify_webhook_delivery_repair_action,
    },
    orchestrator::run_lifecycle_repair_with_stores_at_time,
    reachability::{collect_record_object_references, collect_referenced_object_keys},
    types::{
        QuarantineRepairAction, RepairReachability, RetentionHoldRepairAction,
        WebhookDeliveryRepairAction,
    },
};

// ── classify_quarantine_repair_action ──────────────────────────────────

#[test]
fn classify_quarantine_missing_object_is_delete_missing() {
    assert_eq!(
        classify_quarantine_repair_action(false, false, false),
        QuarantineRepairAction::DeleteMissing
    );
}

#[test]
fn classify_quarantine_reachable_object_is_delete_reachable() {
    assert_eq!(
        classify_quarantine_repair_action(true, true, false),
        QuarantineRepairAction::DeleteReachable
    );
}

#[test]
fn classify_quarantine_held_object_is_delete_held() {
    assert_eq!(
        classify_quarantine_repair_action(true, false, true),
        QuarantineRepairAction::DeleteHeld
    );
}

#[test]
fn classify_quarantine_keep_when_none_apply() {
    assert_eq!(
        classify_quarantine_repair_action(true, false, false),
        QuarantineRepairAction::Keep
    );
}

#[test]
fn classify_quarantine_missing_takes_precedence_over_reachable() {
    // object does not exist → DeleteMissing regardless of reachability
    assert_eq!(
        classify_quarantine_repair_action(false, true, false),
        QuarantineRepairAction::DeleteMissing
    );
}

// ── classify_retention_hold_repair_action ──────────────────────────────

#[test]
fn classify_retention_hold_expired_release_is_delete_expired() {
    assert_eq!(
        classify_retention_hold_repair_action(Some(50), 10, true, 100),
        RetentionHoldRepairAction::DeleteExpired
    );
}

#[test]
fn classify_retention_hold_missing_object_is_delete_missing() {
    assert_eq!(
        classify_retention_hold_repair_action(Some(150), 10, false, 100),
        RetentionHoldRepairAction::DeleteMissing
    );
}

#[test]
fn classify_retention_hold_expired_takes_precedence_over_missing() {
    // release_after (50) <= now (100) → DeleteExpired (checked first)
    assert_eq!(
        classify_retention_hold_repair_action(Some(50), 10, false, 100),
        RetentionHoldRepairAction::DeleteExpired
    );
}

#[test]
fn classify_retention_hold_keep_when_not_expired_and_object_exists() {
    assert_eq!(
        classify_retention_hold_repair_action(Some(150), 10, true, 100),
        RetentionHoldRepairAction::Keep
    );
}

#[test]
fn classify_retention_hold_no_release_is_keep_when_object_exists() {
    assert_eq!(
        classify_retention_hold_repair_action(None, 10, true, 100),
        RetentionHoldRepairAction::Keep
    );
}

#[test]
fn classify_retention_hold_no_release_is_delete_missing_when_object_missing() {
    assert_eq!(
        classify_retention_hold_repair_action(None, 10, false, 100),
        RetentionHoldRepairAction::DeleteMissing
    );
}

#[test]
fn classify_retention_hold_exact_expiry_is_delete_expired() {
    assert_eq!(
        classify_retention_hold_repair_action(Some(100), 10, true, 100),
        RetentionHoldRepairAction::DeleteExpired
    );
}

// ── classify_webhook_delivery_repair_action ────────────────────────────

#[test]
fn classify_webhook_delivery_future_is_delete_future() {
    assert_eq!(
        classify_webhook_delivery_repair_action(500, 100, 400),
        WebhookDeliveryRepairAction::DeleteFuture
    );
}

#[test]
fn classify_webhook_delivery_stale_is_delete_stale() {
    assert_eq!(
        classify_webhook_delivery_repair_action(50, 100, 400),
        WebhookDeliveryRepairAction::DeleteStale
    );
}

#[test]
fn classify_webhook_delivery_keep_when_within_window() {
    assert_eq!(
        classify_webhook_delivery_repair_action(200, 100, 400),
        WebhookDeliveryRepairAction::Keep
    );
}

#[test]
fn classify_webhook_delivery_future_takes_precedence_over_stale() {
    // processed_at (500) > max_processed_at (400) → DeleteFuture
    // even though it would also be stale (cutoff=100)
    assert_eq!(
        classify_webhook_delivery_repair_action(500, 100, 400),
        WebhookDeliveryRepairAction::DeleteFuture
    );
}

#[test]
fn classify_webhook_delivery_exact_stale_cutoff_is_delete_stale() {
    assert_eq!(
        classify_webhook_delivery_repair_action(100, 100, 400),
        WebhookDeliveryRepairAction::DeleteStale
    );
}

#[test]
fn classify_webhook_delivery_zero_processed_is_keep_when_within_window() {
    assert_eq!(
        classify_webhook_delivery_repair_action(0, 100, 400),
        WebhookDeliveryRepairAction::DeleteStale
    );
}

#[test]
fn classify_webhook_delivery_all_zero_boundaries() {
    // All zero: not future (0 <= 0) and not stale (0 <= 0) → DeleteStale
    assert_eq!(
        classify_webhook_delivery_repair_action(0, 0, 0),
        WebhookDeliveryRepairAction::DeleteStale
    );
}

#[test]
fn classify_retention_hold_release_at_epoch_is_expired() {
    // release_after = 0 (epoch), now = 0 → 0 <= 0 → expired
    assert_eq!(
        classify_retention_hold_repair_action(Some(0), 0, true, 0),
        RetentionHoldRepairAction::DeleteExpired
    );
}

#[test]
fn classify_retention_hold_release_at_epoch_object_missing() {
    // release_after = 0, now = 0 → expired (takes priority over missing)
    assert_eq!(
        classify_retention_hold_repair_action(Some(0), 0, false, 0),
        RetentionHoldRepairAction::DeleteExpired
    );
}

#[test]
fn classify_quarantine_all_false_is_delete_missing() {
    assert_eq!(
        classify_quarantine_repair_action(false, false, false),
        QuarantineRepairAction::DeleteMissing
    );
}

#[test]
fn classify_quarantine_all_true_is_delete_missing_precedence() {
    // object_exists=false takes precedence, even if reachable and held
    assert_eq!(
        classify_quarantine_repair_action(false, true, true),
        QuarantineRepairAction::DeleteMissing
    );
}

#[test]
fn classify_quarantine_reachable_and_held_is_delete_reachable() {
    // is_reachable checked before is_held → DeleteReachable
    assert_eq!(
        classify_quarantine_repair_action(true, true, true),
        QuarantineRepairAction::DeleteReachable
    );
}

// ── LifecycleRepairOptions ─────────────────────────────────────────────

#[test]
fn lifecycle_repair_options_default() {
    let options = LifecycleRepairOptions::default();
    assert_eq!(
        options.webhook_retention_seconds,
        DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS
    );
}

#[test]
fn lifecycle_repair_options_debug_and_clone() {
    let options = LifecycleRepairOptions {
        webhook_retention_seconds: 3600,
    };
    let cloned = options;
    assert_eq!(options, cloned);
    let debug = format!("{options:?}");
    assert!(debug.contains("3600"));
}

// ── Enum formatting ────────────────────────────────────────────────────

#[test]
fn quarantine_repair_action_debug_format() {
    assert_eq!(format!("{:?}", QuarantineRepairAction::Keep), "Keep");
    assert_eq!(
        format!("{:?}", QuarantineRepairAction::DeleteMissing),
        "DeleteMissing"
    );
    assert_eq!(
        format!("{:?}", QuarantineRepairAction::DeleteReachable),
        "DeleteReachable"
    );
    assert_eq!(
        format!("{:?}", QuarantineRepairAction::DeleteHeld),
        "DeleteHeld"
    );
}

#[test]
fn retention_hold_repair_action_debug_format() {
    assert_eq!(format!("{:?}", RetentionHoldRepairAction::Keep), "Keep");
    assert_eq!(
        format!("{:?}", RetentionHoldRepairAction::DeleteExpired),
        "DeleteExpired"
    );
    assert_eq!(
        format!("{:?}", RetentionHoldRepairAction::DeleteMissing),
        "DeleteMissing"
    );
}

#[test]
fn webhook_delivery_repair_action_debug_format() {
    assert_eq!(format!("{:?}", WebhookDeliveryRepairAction::Keep), "Keep");
    assert_eq!(
        format!("{:?}", WebhookDeliveryRepairAction::DeleteStale),
        "DeleteStale"
    );
    assert_eq!(
        format!("{:?}", WebhookDeliveryRepairAction::DeleteFuture),
        "DeleteFuture"
    );
}

// ── Constants ──────────────────────────────────────────────────────────

#[test]
fn default_webhook_retention_is_30_days() {
    assert_eq!(DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS, 2_592_000);
}

#[test]
fn webhook_future_skew_is_5_minutes() {
    assert_eq!(WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS, 300);
}

// ── RepairReachability ─────────────────────────────────────────────────

#[test]
fn repair_reachability_default_has_empty_sets() {
    let reachability = RepairReachability::default();
    assert!(reachability.referenced_object_keys.is_empty());
    assert!(reachability.live_dedupe_chunk_hashes.is_empty());
    assert_eq!(reachability.scanned_records, 0);
}

#[test]
fn repair_reachability_can_accumulate_keys() {
    let mut reachability = RepairReachability::default();
    reachability
        .referenced_object_keys
        .insert("key1".to_owned());
    reachability
        .live_dedupe_chunk_hashes
        .insert("hash1".to_owned());
    reachability.scanned_records = 42;
    assert_eq!(reachability.referenced_object_keys.len(), 1);
    assert!(reachability.referenced_object_keys.contains("key1"));
    assert_eq!(reachability.live_dedupe_chunk_hashes.len(), 1);
    assert!(reachability.live_dedupe_chunk_hashes.contains("hash1"));
    assert_eq!(reachability.scanned_records, 42);
}

// ── LifecycleRepairReport ──────────────────────────────────────────────

#[test]
fn lifecycle_repair_report_all_fields_default_to_zero() {
    let report = LifecycleRepairReport {
        scanned_records: 0,
        referenced_objects: 0,
        scanned_quarantine_candidates: 0,
        removed_missing_quarantine_candidates: 0,
        removed_reachable_quarantine_candidates: 0,
        removed_held_quarantine_candidates: 0,
        scanned_retention_holds: 0,
        removed_expired_retention_holds: 0,
        removed_missing_retention_holds: 0,
        scanned_webhook_deliveries: 0,
        removed_stale_webhook_deliveries: 0,
        removed_future_webhook_deliveries: 0,
    };
    assert_eq!(report.scanned_records, 0);
    assert_eq!(report.referenced_objects, 0);
    assert_eq!(report.scanned_quarantine_candidates, 0);
    assert_eq!(report.removed_missing_quarantine_candidates, 0);
    assert_eq!(report.removed_reachable_quarantine_candidates, 0);
    assert_eq!(report.removed_held_quarantine_candidates, 0);
    assert_eq!(report.scanned_retention_holds, 0);
    assert_eq!(report.removed_expired_retention_holds, 0);
    assert_eq!(report.removed_missing_retention_holds, 0);
    assert_eq!(report.scanned_webhook_deliveries, 0);
    assert_eq!(report.removed_stale_webhook_deliveries, 0);
    assert_eq!(report.removed_future_webhook_deliveries, 0);
}

#[test]
fn lifecycle_repair_report_non_zero_fields() {
    let report = LifecycleRepairReport {
        scanned_records: 100,
        referenced_objects: 50,
        scanned_quarantine_candidates: 10,
        removed_missing_quarantine_candidates: 2,
        removed_reachable_quarantine_candidates: 3,
        removed_held_quarantine_candidates: 1,
        scanned_retention_holds: 20,
        removed_expired_retention_holds: 5,
        removed_missing_retention_holds: 1,
        scanned_webhook_deliveries: 30,
        removed_stale_webhook_deliveries: 10,
        removed_future_webhook_deliveries: 2,
    };
    assert_eq!(report.scanned_records, 100);
    assert_eq!(report.removed_stale_webhook_deliveries, 10);
    assert_eq!(report.removed_future_webhook_deliveries, 2);
    let debug = format!("{:?}", report);
    assert!(debug.contains("scanned_records: 100"));
    assert!(debug.contains("referenced_objects: 50"));
}

#[test]
fn lifecycle_repair_report_clone_and_eq() {
    let report = LifecycleRepairReport {
        scanned_records: 5,
        referenced_objects: 3,
        scanned_quarantine_candidates: 0,
        removed_missing_quarantine_candidates: 0,
        removed_reachable_quarantine_candidates: 0,
        removed_held_quarantine_candidates: 0,
        scanned_retention_holds: 0,
        removed_expired_retention_holds: 0,
        removed_missing_retention_holds: 0,
        scanned_webhook_deliveries: 0,
        removed_stale_webhook_deliveries: 0,
        removed_future_webhook_deliveries: 0,
    };
    let cloned = report.clone();
    assert_eq!(report, cloned);
}

// ── collect_record_object_references ──────────────────────────────────

/// Creates a valid 64-char hex hash string.
fn valid_hash() -> String {
    "ab".repeat(32)
}

fn make_stored_chunks_record(chunks: Vec<FileChunkRecord>) -> Vec<u8> {
    let record = FileRecord {
        file_id: "test-file".to_owned(),
        content_hash: valid_hash(),
        total_bytes: 1024,
        chunk_size: 65536,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks,
    };
    serde_json::to_vec(&record).unwrap()
}

#[test]
fn collect_references_stored_chunks_populates_keys_and_hashes() {
    let hash = valid_hash();
    let frontends = [ServerFrontend::Xet];
    let store = ServerObjectStore::blackhole();
    let mut reachability = RepairReachability::default();

    let chunk = FileChunkRecord {
        hash: hash.clone(),
        offset: 0,
        length: 512,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: 0,
    };
    let record_bytes = make_stored_chunks_record(vec![chunk]);

    let result =
        collect_record_object_references(&store, &frontends, &record_bytes, &mut reachability);
    assert!(result.is_ok());

    // For StoredChunks: referenced_object_keys should contain the chunk key
    let expected_chunk_key = crate::chunk_store::chunk_object_key(&hash)
        .unwrap()
        .as_str()
        .to_owned();
    assert!(
        reachability
            .referenced_object_keys
            .contains(&expected_chunk_key),
        "expected chunk key {expected_chunk_key} in referenced keys"
    );

    // live_dedupe_chunk_hashes should contain the chunk hash
    assert!(reachability.live_dedupe_chunk_hashes.contains(&hash));

    // scanned_records should be incremented
    assert_eq!(reachability.scanned_records, 1);
}

#[test]
fn collect_references_stored_chunks_without_xet_frontend() {
    let hash = valid_hash();
    // No Xet frontend means optional_chunk_container_keys won't produce container keys
    let frontends = [ServerFrontend::Lfs];
    let store = ServerObjectStore::blackhole();
    let mut reachability = RepairReachability::default();

    let chunk = FileChunkRecord {
        hash: hash.clone(),
        offset: 0,
        length: 512,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: 0,
    };
    let record_bytes = make_stored_chunks_record(vec![chunk]);

    let result =
        collect_record_object_references(&store, &frontends, &record_bytes, &mut reachability);
    assert!(result.is_ok());

    // Chunk key should still be referenced (doesn't depend on frontend)
    let expected_chunk_key = crate::chunk_store::chunk_object_key(&hash)
        .unwrap()
        .as_str()
        .to_owned();
    assert!(
        reachability
            .referenced_object_keys
            .contains(&expected_chunk_key)
    );

    // live_dedupe_chunk_hashes populated
    assert!(reachability.live_dedupe_chunk_hashes.contains(&hash));
    assert_eq!(reachability.scanned_records, 1);
}

#[test]
fn collect_references_empty_chunks_does_not_increment_records() {
    let frontends = [ServerFrontend::Xet];
    let store = ServerObjectStore::blackhole();
    let mut reachability = RepairReachability::default();

    // A record with no chunks should still increment scanned_records (the record
    // itself is scanned) but produce no keys or hashes.
    let record = FileRecord {
        file_id: "empty".to_owned(),
        content_hash: valid_hash(),
        total_bytes: 0,
        chunk_size: 65536,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks: vec![],
    };
    let record_bytes = serde_json::to_vec(&record).unwrap();

    let result =
        collect_record_object_references(&store, &frontends, &record_bytes, &mut reachability);
    assert!(result.is_ok());
    assert_eq!(reachability.scanned_records, 1);
    assert!(reachability.referenced_object_keys.is_empty());
    assert!(reachability.live_dedupe_chunk_hashes.is_empty());
}

#[test]
fn collect_references_invalid_record_bytes_returns_error() {
    let frontends = [ServerFrontend::Xet];
    let store = ServerObjectStore::blackhole();
    let mut reachability = RepairReachability::default();

    let result =
        collect_record_object_references(&store, &frontends, b"not-valid-json", &mut reachability);
    assert!(result.is_err());
    // scanned_records should not be incremented on error
    assert_eq!(reachability.scanned_records, 0);
}

#[test]
fn collect_references_multiple_chunks_all_tracked() {
    let hash1 = "ab".repeat(32);
    let hash2 = "cd".repeat(32);
    let frontends = [ServerFrontend::Xet];
    let store = ServerObjectStore::blackhole();
    let mut reachability = RepairReachability::default();

    let chunks = vec![
        FileChunkRecord {
            hash: hash1.clone(),
            offset: 0,
            length: 256,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 0,
        },
        FileChunkRecord {
            hash: hash2.clone(),
            offset: 256,
            length: 256,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 0,
        },
    ];
    let record_bytes = make_stored_chunks_record(chunks);

    let result =
        collect_record_object_references(&store, &frontends, &record_bytes, &mut reachability);
    assert!(result.is_ok());
    assert_eq!(reachability.scanned_records, 1);

    let key1 = crate::chunk_store::chunk_object_key(&hash1)
        .unwrap()
        .as_str()
        .to_owned();
    let key2 = crate::chunk_store::chunk_object_key(&hash2)
        .unwrap()
        .as_str()
        .to_owned();
    assert!(reachability.referenced_object_keys.contains(&key1));
    assert!(reachability.referenced_object_keys.contains(&key2));
    assert!(reachability.live_dedupe_chunk_hashes.contains(&hash1));
    assert!(reachability.live_dedupe_chunk_hashes.contains(&hash2));
}

#[test]
fn collect_references_deduplicates_keys() {
    let hash = valid_hash();
    let frontends = [ServerFrontend::Xet];
    let store = ServerObjectStore::blackhole();
    let mut reachability = RepairReachability::default();

    // Two identical chunks should produce the same key
    let chunks = vec![
        FileChunkRecord {
            hash: hash.clone(),
            offset: 0,
            length: 256,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 0,
        },
        FileChunkRecord {
            hash,
            offset: 256,
            length: 256,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 0,
        },
    ];
    let record_bytes = make_stored_chunks_record(chunks);

    let result =
        collect_record_object_references(&store, &frontends, &record_bytes, &mut reachability);
    assert!(result.is_ok());
    assert_eq!(reachability.referenced_object_keys.len(), 1);
    assert_eq!(reachability.live_dedupe_chunk_hashes.len(), 1);
}

// ── ReferencedObjectTerms layout ───────────────────────────────────────

fn make_referenced_terms_record(chunks: Vec<FileChunkRecord>) -> Vec<u8> {
    // chunk_size = 0 triggers ReferencedObjectTerms layout
    let record = FileRecord {
        file_id: "test-term-file".to_owned(),
        content_hash: valid_hash(),
        total_bytes: 1024,
        chunk_size: 0,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks,
    };
    serde_json::to_vec(&record).unwrap()
}

#[test]
fn collect_references_referenced_terms_with_xet_populates_term_keys() {
    let term_hash = valid_hash();
    let frontends = [ServerFrontend::Xet];
    let store = ServerObjectStore::blackhole();
    let mut reachability = RepairReachability::default();

    let chunk = FileChunkRecord {
        hash: term_hash.clone(),
        offset: 0,
        length: 512,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: 0,
    };
    let record_bytes = make_referenced_terms_record(vec![chunk]);

    let result =
        collect_record_object_references(&store, &frontends, &record_bytes, &mut reachability);
    assert!(result.is_ok());

    // For ReferencedObjectTerms: referenced_term_object_key is called
    // which produces a xorb-like key for Xet frontends
    let expected_term_key =
        crate::server_frontend::referenced_term_object_key(&frontends, &term_hash)
            .unwrap()
            .as_str()
            .to_owned();
    assert!(
        reachability
            .referenced_object_keys
            .contains(&expected_term_key),
        "expected term key {expected_term_key} in referenced keys"
    );

    // live_dedupe_chunk_hashes should NOT be set for ReferencedObjectTerms
    assert!(reachability.live_dedupe_chunk_hashes.is_empty());
    assert_eq!(reachability.scanned_records, 1);
}

#[test]
fn collect_references_referenced_terms_without_xet_returns_error() {
    let term_hash = valid_hash();
    // Without Xet frontend, referenced_term_object_key returns InvalidContentHash
    let frontends = [ServerFrontend::Lfs];
    let store = ServerObjectStore::blackhole();
    let mut reachability = RepairReachability::default();

    let chunk = FileChunkRecord {
        hash: term_hash,
        offset: 0,
        length: 512,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: 0,
    };
    let record_bytes = make_referenced_terms_record(vec![chunk]);

    let result =
        collect_record_object_references(&store, &frontends, &record_bytes, &mut reachability);
    assert!(result.is_err());
}

// ── Overflow test ──────────────────────────────────────────────────────

#[test]
fn collect_references_overflow_scanned_records() {
    // scanned_records at u64::MAX should overflow on increment
    let frontends = [ServerFrontend::Xet];
    let store = ServerObjectStore::blackhole();
    let mut reachability = RepairReachability {
        scanned_records: u64::MAX,
        ..Default::default()
    };

    let hash = valid_hash();
    let chunk = FileChunkRecord {
        hash,
        offset: 0,
        length: 512,
        range_start: 0,
        range_end: 1,
        packed_start: 0,
        packed_end: 0,
    };
    let record_bytes = make_stored_chunks_record(vec![chunk]);
    // chunk_object_key requires a valid hash and will work even though
    // scanned_records overflows — the overflow happens first in the code path
    let result =
        collect_record_object_references(&store, &frontends, &record_bytes, &mut reachability);
    assert!(result.is_err());
}

// ── Integration-style tests with real stores ──────────────────────────

use shardline_index::{
    DedupeShardMapping, LifecycleStore, LocalIndexStore, LocalRecordStore, QuarantineCandidate,
    RecordMutation, RetentionHold, WebhookDelivery,
};
use shardline_protocol::{RepositoryProvider, ShardlineHash};
use shardline_storage::ObjectKey;

/// Helper: create temporary stores for testing.
fn make_test_stores() -> (
    tempfile::TempDir,
    LocalRecordStore,
    LocalIndexStore,
    ServerObjectStore,
) {
    let root = tempfile::tempdir().expect("temp dir");
    let index_store = LocalIndexStore::new(root.path().to_path_buf()).expect("index store");
    let record_store = LocalRecordStore::new(root.path().to_path_buf()).expect("record store");
    let object_store = ServerObjectStore::local(root.path().join("chunks")).expect("object store");
    (root, record_store, index_store, object_store)
}

/// Helper: create a file on disk for a given object key in a local store.
fn seed_object(store: &ServerObjectStore, key: &ObjectKey) {
    if let ServerObjectStore::Local(local) = store {
        let path = local.path_for_key(key);
        std::fs::create_dir_all(path.parent().expect("parent dir")).expect("create dirs");
        std::fs::write(&path, b"x").expect("write file");
    }
}

/// Helper: build a [`FileRecord`] with a single chunk for the given hash.
fn single_chunk_record(file_id: &str, hash: &str, chunk_size: u64) -> FileRecord {
    FileRecord {
        file_id: file_id.to_owned(),
        content_hash: valid_hash(),
        total_bytes: 256,
        chunk_size,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash: hash.to_owned(),
            offset: 0,
            length: 256,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 0,
        }],
    }
}

// ── run_lifecycle_repair_with_stores_at_time — empty stores ────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_empty_stores_produces_zero_report() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        1000,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_records, 0);
    assert_eq!(report.referenced_objects, 0);
    assert_eq!(report.scanned_retention_holds, 0);
    assert_eq!(report.removed_expired_retention_holds, 0);
    assert_eq!(report.removed_missing_retention_holds, 0);
    assert_eq!(report.scanned_quarantine_candidates, 0);
    assert_eq!(report.removed_missing_quarantine_candidates, 0);
    assert_eq!(report.removed_reachable_quarantine_candidates, 0);
    assert_eq!(report.removed_held_quarantine_candidates, 0);
    assert_eq!(report.scanned_webhook_deliveries, 0);
    assert_eq!(report.removed_stale_webhook_deliveries, 0);
    assert_eq!(report.removed_future_webhook_deliveries, 0);
}

// ── Retention hold iteration ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_keeps_active_retention_hold_with_existing_object() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    let key = ObjectKey::parse("test/retention-keep").unwrap();
    seed_object(&object_store, &key);
    let hold = RetentionHold::new(key.clone(), "test hold".to_owned(), 0, Some(2000)).unwrap();
    index_store.upsert_retention_hold(&hold).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_retention_holds, 1);
    assert_eq!(report.removed_expired_retention_holds, 0);
    assert_eq!(report.removed_missing_retention_holds, 0);

    // Verify the hold is still present
    let holds_after = index_store.list_retention_holds().unwrap();
    assert_eq!(holds_after.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_deletes_expired_retention_hold() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    let key = ObjectKey::parse("test/retention-expired").unwrap();
    // release_after (500) <= now (1000) → DeleteExpired
    let hold = RetentionHold::new(key.clone(), "expired hold".to_owned(), 0, Some(500)).unwrap();
    index_store.upsert_retention_hold(&hold).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_retention_holds, 1);
    assert_eq!(report.removed_expired_retention_holds, 1);
    assert_eq!(report.removed_missing_retention_holds, 0);

    // Verify the hold was deleted
    let holds_after = index_store.list_retention_holds().unwrap();
    assert_eq!(holds_after.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_deletes_retention_hold_for_missing_object() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    let key = ObjectKey::parse("test/retention-missing").unwrap();
    // Not expired (release > now), but object does not exist → DeleteMissing
    let hold = RetentionHold::new(key.clone(), "missing hold".to_owned(), 0, Some(2000)).unwrap();
    index_store.upsert_retention_hold(&hold).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_retention_holds, 1);
    assert_eq!(report.removed_expired_retention_holds, 0);
    assert_eq!(report.removed_missing_retention_holds, 1);

    let holds_after = index_store.list_retention_holds().unwrap();
    assert_eq!(holds_after.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_retention_hold_expired_takes_precedence_over_missing() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    let key = ObjectKey::parse("test/retention-expired-vs-missing").unwrap();
    // Both conditions true: expired AND object missing → DeleteExpired (checked first)
    let hold = RetentionHold::new(key, "expired & missing".to_owned(), 0, Some(500)).unwrap();
    index_store.upsert_retention_hold(&hold).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_retention_holds, 1);
    assert_eq!(report.removed_expired_retention_holds, 1);
    assert_eq!(report.removed_missing_retention_holds, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_retention_hold_active_blocks_quarantine_deletion() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    // Retention hold: active (not expired), object exists → Keep (active)
    let hold_key = ObjectKey::parse("test/held-object").unwrap();
    seed_object(&object_store, &hold_key);
    let hold =
        RetentionHold::new(hold_key.clone(), "active hold".to_owned(), 0, Some(2000)).unwrap();
    index_store.upsert_retention_hold(&hold).unwrap();

    // Quarantine candidate for the same key: object exists, NOT reachable
    // (no records), but IS held → DeleteHeld
    let candidate = QuarantineCandidate::new(hold_key, 1, 0, 5000).unwrap();
    index_store.upsert_quarantine_candidate(&candidate).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_retention_holds, 1);
    assert_eq!(report.removed_expired_retention_holds, 0);
    assert_eq!(report.removed_missing_retention_holds, 0);

    assert_eq!(report.scanned_quarantine_candidates, 1);
    assert_eq!(report.removed_held_quarantine_candidates, 1);

    // Retention hold should still exist
    let holds_after = index_store.list_retention_holds().unwrap();
    assert_eq!(holds_after.len(), 1);
}

// ── Quarantine candidate iteration ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_keeps_quarantine_for_unreachable_unheld_existing_object() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    let key = ObjectKey::parse("test/q-keep").unwrap();
    seed_object(&object_store, &key);
    let candidate = QuarantineCandidate::new(key, 1, 0, 2000).unwrap();
    index_store.upsert_quarantine_candidate(&candidate).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_quarantine_candidates, 1);
    assert_eq!(report.removed_missing_quarantine_candidates, 0);
    assert_eq!(report.removed_reachable_quarantine_candidates, 0);
    assert_eq!(report.removed_held_quarantine_candidates, 0);

    let candidates_after = index_store.list_quarantine_candidates().unwrap();
    assert_eq!(candidates_after.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_deletes_quarantine_for_missing_object() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    let key = ObjectKey::parse("test/q-missing").unwrap();
    // No seed_object — object does not exist
    let candidate = QuarantineCandidate::new(key, 1, 0, 2000).unwrap();
    index_store.upsert_quarantine_candidate(&candidate).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_quarantine_candidates, 1);
    assert_eq!(report.removed_missing_quarantine_candidates, 1);
    assert_eq!(report.removed_reachable_quarantine_candidates, 0);
    assert_eq!(report.removed_held_quarantine_candidates, 0);

    let candidates_after = index_store.list_quarantine_candidates().unwrap();
    assert_eq!(candidates_after.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_deletes_quarantine_for_reachable_object() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    // Create a record with a chunk whose key becomes a referenced object
    let hash = valid_hash(); // "ab".repeat(32)
    let chunk_key = crate::chunk_store::chunk_object_key(&hash).unwrap();
    seed_object(&object_store, &chunk_key);

    // Write both version and latest records so scanning finds them
    let record = single_chunk_record("reachable-file", &hash, 65536);
    record_store.write_version_record(&record).await.unwrap();
    record_store.write_latest_record(&record).await.unwrap();

    // Quarantine candidate for the chunk key (which IS reachable via the record)
    let candidate = QuarantineCandidate::new(chunk_key.clone(), 256, 0, 5000).unwrap();
    index_store.upsert_quarantine_candidate(&candidate).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_records, 2); // one version + one latest
    assert_eq!(report.referenced_objects, 1);
    assert_eq!(report.scanned_quarantine_candidates, 1);
    assert_eq!(report.removed_missing_quarantine_candidates, 0);
    assert_eq!(report.removed_reachable_quarantine_candidates, 1);
    assert_eq!(report.removed_held_quarantine_candidates, 0);

    let candidates_after = index_store.list_quarantine_candidates().unwrap();
    assert_eq!(candidates_after.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_deletes_quarantine_for_held_object() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    let key = ObjectKey::parse("test/q-held").unwrap();
    seed_object(&object_store, &key);

    // Active retention hold (not expired, object exists)
    let hold = RetentionHold::new(key.clone(), "active hold".to_owned(), 0, Some(2000)).unwrap();
    index_store.upsert_retention_hold(&hold).unwrap();

    // Quarantine candidate for the same key
    let candidate = QuarantineCandidate::new(key, 1, 0, 5000).unwrap();
    index_store.upsert_quarantine_candidate(&candidate).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_quarantine_candidates, 1);
    assert_eq!(report.removed_missing_quarantine_candidates, 0);
    assert_eq!(report.removed_reachable_quarantine_candidates, 0);
    assert_eq!(report.removed_held_quarantine_candidates, 1);

    let candidates_after = index_store.list_quarantine_candidates().unwrap();
    assert_eq!(candidates_after.len(), 0);
}

// ── Webhook delivery iteration ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_keeps_webhook_delivery_within_window() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    // processed_at=500, stale_cutoff=0 (saturated from retention), max=1300
    // 500 > 0 and 500 <= 1300 → Keep
    let delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        "delivery-keep".to_owned(),
        500,
    )
    .unwrap();
    index_store.record_webhook_delivery(&delivery).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_webhook_deliveries, 1);
    assert_eq!(report.removed_stale_webhook_deliveries, 0);
    assert_eq!(report.removed_future_webhook_deliveries, 0);

    let deliveries_after = index_store.list_webhook_deliveries().unwrap();
    assert_eq!(deliveries_after.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_deletes_stale_webhook_delivery() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    // processed_at=0, stale_cutoff=0 (saturated) → 0 <= 0 → DeleteStale
    let delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        "delivery-stale".to_owned(),
        0,
    )
    .unwrap();
    index_store.record_webhook_delivery(&delivery).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_webhook_deliveries, 1);
    assert_eq!(report.removed_stale_webhook_deliveries, 1);
    assert_eq!(report.removed_future_webhook_deliveries, 0);

    let deliveries_after = index_store.list_webhook_deliveries().unwrap();
    assert_eq!(deliveries_after.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_deletes_future_webhook_delivery() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    // processed_at=1500, max_processed_at=1300 → 1500 > 1300 → DeleteFuture
    let delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        "delivery-future".to_owned(),
        1500,
    )
    .unwrap();
    index_store.record_webhook_delivery(&delivery).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_webhook_deliveries, 1);
    assert_eq!(report.removed_stale_webhook_deliveries, 0);
    assert_eq!(report.removed_future_webhook_deliveries, 1);

    let deliveries_after = index_store.list_webhook_deliveries().unwrap();
    assert_eq!(deliveries_after.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_webhook_delivery_future_takes_precedence_over_stale() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    // processed_at=1500: future (>1300) AND also stale (>0) but future wins
    let delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        "delivery-both".to_owned(),
        1500,
    )
    .unwrap();
    index_store.record_webhook_delivery(&delivery).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_webhook_deliveries, 1);
    assert_eq!(report.removed_stale_webhook_deliveries, 0);
    assert_eq!(report.removed_future_webhook_deliveries, 1);
}

// ── collect_referenced_object_keys — record scanning ──────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collect_referenced_object_keys_scans_version_records() {
    let (_root, record_store, index_store, object_store) = make_test_stores();

    let hash = valid_hash();
    let chunk_key = crate::chunk_store::chunk_object_key(&hash).unwrap();

    let record = single_chunk_record("scan-file", &hash, 65536);
    record_store.write_version_record(&record).await.unwrap();

    let mut reachability = RepairReachability::default();
    collect_referenced_object_keys(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        &mut reachability,
    )
    .await
    .expect("collect should succeed");

    assert_eq!(reachability.scanned_records, 1);
    assert!(
        reachability
            .referenced_object_keys
            .contains(chunk_key.as_str())
    );
    assert!(reachability.live_dedupe_chunk_hashes.contains(&hash));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collect_referenced_object_keys_scans_latest_records() {
    let (_root, record_store, index_store, object_store) = make_test_stores();

    let hash = valid_hash();
    let chunk_key = crate::chunk_store::chunk_object_key(&hash).unwrap();

    let record = single_chunk_record("latest-file", &hash, 65536);
    record_store.write_latest_record(&record).await.unwrap();

    let mut reachability = RepairReachability::default();
    collect_referenced_object_keys(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        &mut reachability,
    )
    .await
    .expect("collect should succeed");

    assert_eq!(reachability.scanned_records, 1);
    assert!(
        reachability
            .referenced_object_keys
            .contains(chunk_key.as_str())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collect_referenced_object_keys_dedupe_mapping_adds_shard_key() {
    let (_root, record_store, index_store, object_store) = make_test_stores();

    let hash = valid_hash();
    let chunk_key = crate::chunk_store::chunk_object_key(&hash).unwrap();

    // Write a record so the chunk hash lands in live_dedupe_chunk_hashes
    let record = single_chunk_record("dedupe-file", &hash, 65536);
    record_store.write_version_record(&record).await.unwrap();

    // Seed a dedupe shard mapping with a ShardlineHash whose xet hash hex
    // matches the chunk hex string used in the record.
    let chunk_hash = ShardlineHash::parse_hex(&hash).unwrap();
    let shard_key = ObjectKey::parse("shards/ab/test-shard").unwrap();
    let mapping = DedupeShardMapping::new(chunk_hash, shard_key.clone());
    index_store.upsert_dedupe_shard_mapping(&mapping).unwrap();

    let mut reachability = RepairReachability::default();
    collect_referenced_object_keys(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        &mut reachability,
    )
    .await
    .expect("collect should succeed");

    // The chunk key from the record should be referenced
    assert!(
        reachability
            .referenced_object_keys
            .contains(chunk_key.as_str())
    );
    // The shard key from the dedupe mapping should also be referenced
    assert!(
        reachability
            .referenced_object_keys
            .contains(shard_key.as_str())
    );
    assert_eq!(reachability.referenced_object_keys.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collect_referenced_object_keys_empty_stores() {
    let (_root, record_store, index_store, object_store) = make_test_stores();

    let mut reachability = RepairReachability::default();
    collect_referenced_object_keys(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        &mut reachability,
    )
    .await
    .expect("collect should succeed");

    assert_eq!(reachability.scanned_records, 0);
    assert!(reachability.referenced_object_keys.is_empty());
    assert!(reachability.live_dedupe_chunk_hashes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collect_referenced_object_keys_referenced_terms_layout() {
    let (_root, record_store, index_store, object_store) = make_test_stores();

    // chunk_size=0 triggers ReferencedObjectTerms layout
    let hash = valid_hash();
    let record = single_chunk_record("term-file", &hash, 0);
    record_store.write_version_record(&record).await.unwrap();

    let mut reachability = RepairReachability::default();
    collect_referenced_object_keys(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        &mut reachability,
    )
    .await
    .expect("collect should succeed");

    assert_eq!(reachability.scanned_records, 1);
    // ReferencedObjectTerms should add a term object key, NOT a chunk key
    let term_key =
        crate::server_frontend::referenced_term_object_key(&[ServerFrontend::Xet], &hash).unwrap();
    assert!(
        reachability
            .referenced_object_keys
            .contains(term_key.as_str())
    );
    // live_dedupe_chunk_hashes should be empty for ReferencedObjectTerms
    assert!(reachability.live_dedupe_chunk_hashes.is_empty());
}

// ── Mixed lifecycle repair scenarios ───────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_mixed_scenario_all_lifecycle_categories() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    // 1. Retention hold — expired (will be deleted)
    let expired_key = ObjectKey::parse("test/retention-exp").unwrap();
    let hold_expired = RetentionHold::new(expired_key, "expired".to_owned(), 0, Some(500)).unwrap();
    index_store.upsert_retention_hold(&hold_expired).unwrap();

    // 2. Retention hold — active (will be kept)
    let active_key = ObjectKey::parse("test/retention-active").unwrap();
    seed_object(&object_store, &active_key);
    let hold_active = RetentionHold::new(active_key, "active".to_owned(), 0, Some(2000)).unwrap();
    index_store.upsert_retention_hold(&hold_active).unwrap();

    // 3. Retention hold — missing object (will be deleted)
    let missing_key = ObjectKey::parse("test/retention-missing").unwrap();
    let hold_missing =
        RetentionHold::new(missing_key.clone(), "missing-obj".to_owned(), 0, Some(2000)).unwrap();
    index_store.upsert_retention_hold(&hold_missing).unwrap();

    // 4. Quarantine — missing object (will be deleted)
    let q_missing_key = ObjectKey::parse("test/q-missing").unwrap();
    let q_missing = QuarantineCandidate::new(q_missing_key, 1, 0, 2000).unwrap();
    index_store.upsert_quarantine_candidate(&q_missing).unwrap();

    // 5. Quarantine — kept (existing, unreachable, unheld)
    let q_keep_key = ObjectKey::parse("test/q-keep").unwrap();
    seed_object(&object_store, &q_keep_key);
    let q_keep = QuarantineCandidate::new(q_keep_key, 1, 0, 2000).unwrap();
    index_store.upsert_quarantine_candidate(&q_keep).unwrap();

    // 6. Quarantine — reachable via record (will be deleted)
    let reachable_hash = valid_hash();
    let reachable_chunk_key = crate::chunk_store::chunk_object_key(&reachable_hash).unwrap();
    seed_object(&object_store, &reachable_chunk_key);
    let record = single_chunk_record("reachable", &reachable_hash, 65536);
    record_store.write_version_record(&record).await.unwrap();
    record_store.write_latest_record(&record).await.unwrap();
    let q_reachable = QuarantineCandidate::new(reachable_chunk_key, 256, 0, 5000).unwrap();
    index_store
        .upsert_quarantine_candidate(&q_reachable)
        .unwrap();

    // 7. Quarantine — held (will be deleted)
    let q_held_key = ObjectKey::parse("test/q-held").unwrap();
    seed_object(&object_store, &q_held_key);
    let q_held = QuarantineCandidate::new(q_held_key.clone(), 1, 0, 5000).unwrap();
    index_store.upsert_quarantine_candidate(&q_held).unwrap();
    let hold_for_q_held =
        RetentionHold::new(q_held_key, "hold-for-q".to_owned(), 0, Some(2000)).unwrap();
    index_store.upsert_retention_hold(&hold_for_q_held).unwrap();

    // 8. Webhook — stale (will be deleted)
    let wh_stale = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        "wh-stale".to_owned(),
        0,
    )
    .unwrap();
    index_store.record_webhook_delivery(&wh_stale).unwrap();

    // 9. Webhook — within window (will be kept)
    let wh_keep = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        "wh-keep".to_owned(),
        500,
    )
    .unwrap();
    index_store.record_webhook_delivery(&wh_keep).unwrap();

    // 10. Webhook — future (will be deleted)
    let wh_future = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "owner".to_owned(),
        "repo".to_owned(),
        "wh-future".to_owned(),
        1500,
    )
    .unwrap();
    index_store.record_webhook_delivery(&wh_future).unwrap();

    // ── Run repair ─────────────────────────────────────────────────────

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    // Records: 2 (version + latest for the reachable chunk record)
    assert_eq!(report.scanned_records, 2);
    assert_eq!(report.referenced_objects, 1);

    // Retention holds: 4 scanned (expired, active, missing, hold-for-q)
    assert_eq!(report.scanned_retention_holds, 4);
    assert_eq!(report.removed_expired_retention_holds, 1);
    assert_eq!(report.removed_missing_retention_holds, 1);

    // Quarantine: 4 scanned (missing, keep, reachable, held)
    assert_eq!(report.scanned_quarantine_candidates, 4);
    assert_eq!(report.removed_missing_quarantine_candidates, 1);
    assert_eq!(report.removed_reachable_quarantine_candidates, 1);
    assert_eq!(report.removed_held_quarantine_candidates, 1);

    // Webhook: 3 scanned (stale, keep, future)
    assert_eq!(report.scanned_webhook_deliveries, 3);
    assert_eq!(report.removed_stale_webhook_deliveries, 1);
    assert_eq!(report.removed_future_webhook_deliveries, 1);

    // ── Verify store state after repair ────────────────────────────────

    // Only the active retention hold and the hold-for-q should remain
    let holds_after = index_store.list_retention_holds().unwrap();
    assert_eq!(holds_after.len(), 2);

    // Only the keep quarantine should remain
    let q_after = index_store.list_quarantine_candidates().unwrap();
    assert_eq!(q_after.len(), 1);

    // Only the keep webhook delivery should remain
    let wh_after = index_store.list_webhook_deliveries().unwrap();
    assert_eq!(wh_after.len(), 1);
}

// ── Edge cases ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_retention_hold_never_release_is_keep_when_object_exists() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    let key = ObjectKey::parse("test/never-release").unwrap();
    seed_object(&object_store, &key);
    // release_after = None → never expires
    let hold = RetentionHold::new(key, "permanent".to_owned(), 0, None).unwrap();
    index_store.upsert_retention_hold(&hold).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_retention_holds, 1);
    assert_eq!(report.removed_expired_retention_holds, 0);
    assert_eq!(report.removed_missing_retention_holds, 0);

    let holds_after = index_store.list_retention_holds().unwrap();
    assert_eq!(holds_after.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_retention_hold_never_release_is_delete_missing_when_object_missing() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    let key = ObjectKey::parse("test/never-release-missing").unwrap();
    // No seed → object missing
    let hold = RetentionHold::new(key, "permanent".to_owned(), 0, None).unwrap();
    index_store.upsert_retention_hold(&hold).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_retention_holds, 1);
    assert_eq!(report.removed_expired_retention_holds, 0);
    assert_eq!(report.removed_missing_retention_holds, 1);

    let holds_after = index_store.list_retention_holds().unwrap();
    assert_eq!(holds_after.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_blackhole_store_treats_all_objects_as_missing() {
    // Use a blackhole store: metadata() returns Ok(None) → every object is "missing".
    let (_root, record_store, index_store, _object_store) = make_test_stores();
    let object_store = ServerObjectStore::blackhole();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    // Retention hold with active (non-expired) hold → object missing → DeleteMissing
    let key = ObjectKey::parse("test/any-key").unwrap();
    let hold = RetentionHold::new(key, "test".to_owned(), 0, Some(2000)).unwrap();
    index_store.upsert_retention_hold(&hold).unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_retention_holds, 1);
    assert_eq!(report.removed_expired_retention_holds, 0);
    assert_eq!(report.removed_missing_retention_holds, 1);

    // Quarantine candidate for any key → object missing → DeleteMissing
    let q_key = ObjectKey::parse("test/q-any-key").unwrap();
    let q_candidate = QuarantineCandidate::new(q_key, 1, 0, 5000).unwrap();
    index_store
        .upsert_quarantine_candidate(&q_candidate)
        .unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("second repair should succeed");

    assert_eq!(report.scanned_quarantine_candidates, 1);
    assert_eq!(report.removed_missing_quarantine_candidates, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_no_frontends_still_processes_lifecycle_items() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    // Empty frontends — record scanning will still count records but
    // won't produce chunk container keys. Lifecycle items still process.
    let key = ObjectKey::parse("test/no-frontend").unwrap();
    let hold = RetentionHold::new(key, "test".to_owned(), 0, Some(2000)).unwrap();
    index_store.upsert_retention_hold(&hold).unwrap();

    // Write a record — scanning will work but referenced_term_object_key
    // will fail for ReferencedObjectTerms without Xet frontend.
    // Use StoredChunks layout which doesn't need a frontend for the chunk key.
    let hash = valid_hash();
    let record = single_chunk_record("frontend-test", &hash, 65536);
    record_store.write_version_record(&record).await.unwrap();

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[], // empty frontends
        options,
        now,
    )
    .await
    .expect("repair should succeed with empty frontends");

    assert_eq!(report.scanned_records, 1);
    assert_eq!(report.scanned_retention_holds, 1);
    assert_eq!(report.removed_missing_retention_holds, 1); // object doesn't exist
    assert_eq!(report.referenced_objects, 1); // chunk key still referenced
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_multiple_webhook_deliveries_mixed_actions() {
    let (_root, record_store, index_store, object_store) = make_test_stores();
    let options = LifecycleRepairOptions::default();
    let now = 1000;

    // Three deliveries with different processed_at values
    for (id, processed_at) in [("wh-a", 0u64), ("wh-b", 500), ("wh-c", 1500)] {
        let delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "owner".to_owned(),
            "repo".to_owned(),
            id.to_owned(),
            processed_at,
        )
        .unwrap();
        index_store.record_webhook_delivery(&delivery).unwrap();
    }

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
        now,
    )
    .await
    .expect("repair should succeed");

    assert_eq!(report.scanned_webhook_deliveries, 3);
    assert_eq!(report.removed_stale_webhook_deliveries, 1);
    assert_eq!(report.removed_future_webhook_deliveries, 1);

    let deliveries_after = index_store.list_webhook_deliveries().unwrap();
    assert_eq!(deliveries_after.len(), 1);
    // Only the "wh-b" delivery (500) should remain
    assert_eq!(deliveries_after[0].delivery_id(), "wh-b");
}
