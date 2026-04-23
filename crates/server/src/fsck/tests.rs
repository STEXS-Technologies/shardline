use std::num::NonZeroUsize;

use axum::body::Bytes;
use shardline_index::{
    FileId, FileReconstruction, IndexStore, LocalIndexStore, LocalRecordStore,
    ProviderRepositoryState, QuarantineCandidate, ReconstructionTerm, RecordStore, RetentionHold,
    WebhookDelivery, XorbId,
};
use shardline_protocol::{ChunkRange, RepositoryProvider, ShardlineHash};
use shardline_storage::ObjectKey;
use tokio::fs;
use xet_core_structures::merklehash::compute_data_hash;

use super::{LocalFsckIssueKind, WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS, run_local_fsck};
use crate::{
    LocalBackend, ShardMetadataLimits,
    chunk_store::chunk_object_key,
    clock::unix_now_seconds_checked,
    local_backend::chunk_hash,
    shard_store::shard_object_key,
    test_fixtures::{shard_hash_hex, single_chunk_xorb, single_file_shard, xet_hash_hex},
    upload_ingest::RequestBodyReader,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_reports_clean_storage_without_issues() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let uploaded = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await;
    assert!(uploaded.is_ok());

    let report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert_eq!(report.latest_records, 1);
    assert_eq!(report.version_records, 1);
    assert_eq!(report.inspected_chunk_references, 6);
    assert!(report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_reports_missing_chunk() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let uploaded = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await;
    assert!(uploaded.is_ok());
    let Ok(uploaded) = uploaded else {
        return;
    };

    let first_chunk = uploaded.chunks.first();
    assert!(first_chunk.is_some());
    let Some(first_chunk) = first_chunk else {
        return;
    };
    let hash_prefix = first_chunk.hash.get(..2);
    assert!(hash_prefix.is_some());
    let Some(hash_prefix) = hash_prefix else {
        return;
    };
    let chunk_path = storage
        .path()
        .join("chunks")
        .join(hash_prefix)
        .join(&first_chunk.hash);
    let removed = fs::remove_file(chunk_path).await;
    assert!(removed.is_ok());

    let report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == LocalFsckIssueKind::MissingChunk)
    );
    assert!(!report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_reports_missing_version_record_for_visible_latest() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let uploaded = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await;
    assert!(uploaded.is_ok());
    let Ok(uploaded) = uploaded else {
        return;
    };

    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let record = backend
        .file_record("asset.bin", Some(&uploaded.content_hash), None)
        .await;
    assert!(record.is_ok());
    let Ok(record) = record else {
        return;
    };
    let version_locator = RecordStore::version_record_locator(&record_store, &record);
    let removed = RecordStore::delete_record_locator(&record_store, &version_locator).await;
    assert!(removed.is_ok());

    let report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == LocalFsckIssueKind::MissingVersionRecord)
    );
    assert!(!report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_reports_are_stable_across_repeated_runs() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let uploaded = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await;
    assert!(uploaded.is_ok());
    let Ok(uploaded) = uploaded else {
        return;
    };

    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let record = backend
        .file_record("asset.bin", Some(&uploaded.content_hash), None)
        .await;
    assert!(record.is_ok());
    let Ok(record) = record else {
        return;
    };
    let version_locator = RecordStore::version_record_locator(&record_store, &record);
    let removed = RecordStore::delete_record_locator(&record_store, &version_locator).await;
    assert!(removed.is_ok());

    let first_report = run_local_fsck(storage.path().to_path_buf()).await;
    let second_report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(first_report.is_ok());
    assert!(second_report.is_ok());
    let (Ok(first_report), Ok(second_report)) = (first_report, second_report) else {
        return;
    };

    assert_eq!(first_report, second_report);
    assert!(
        first_report
            .issues
            .iter()
            .any(|issue| issue.kind == LocalFsckIssueKind::MissingVersionRecord)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_reports_hash_and_length_mismatch_for_corrupted_chunk_body() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let uploaded = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await;
    assert!(uploaded.is_ok());
    let Ok(uploaded) = uploaded else {
        return;
    };

    let first_chunk = uploaded.chunks.first();
    assert!(first_chunk.is_some());
    let Some(first_chunk) = first_chunk else {
        return;
    };
    let hash_prefix = first_chunk.hash.get(..2);
    assert!(hash_prefix.is_some());
    let Some(hash_prefix) = hash_prefix else {
        return;
    };
    let chunk_path = storage
        .path()
        .join("chunks")
        .join(hash_prefix)
        .join(&first_chunk.hash);
    let rewritten = fs::write(&chunk_path, b"corrupted").await;
    assert!(rewritten.is_ok());
    assert_ne!(chunk_hash(b"corrupted").api_hex_string(), first_chunk.hash);

    let report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == LocalFsckIssueKind::ChunkHashMismatch)
    );
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == LocalFsckIssueKind::ChunkLengthMismatch)
    );
    assert!(!report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_reports_latest_record_that_differs_from_version_record() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let uploaded = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await;
    assert!(uploaded.is_ok());

    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let latest_record = backend.file_record("asset.bin", None, None).await;
    assert!(latest_record.is_ok());
    let Ok(mut latest_record) = latest_record else {
        return;
    };
    let first_chunk = latest_record.chunks.first_mut();
    assert!(first_chunk.is_some());
    let Some(first_chunk) = first_chunk else {
        return;
    };
    assert!(first_chunk.packed_end > first_chunk.packed_start + 1);
    first_chunk.packed_end -= 1;
    let written = RecordStore::write_latest_record(&record_store, &latest_record).await;
    assert!(written.is_ok());

    let report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == LocalFsckIssueKind::MismatchedVersionRecord)
    );
    assert!(!report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_reports_missing_dedupe_shard_object() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let (xorb_body, xorb_hash) = single_chunk_xorb(b"aaaa");
    let (shard_body, _file_hash) = single_file_shard(&[(b"aaaa", &xorb_hash)]);
    let uploaded_xorb = backend.upload_xorb(&xorb_hash, xorb_body).await;
    assert!(uploaded_xorb.is_ok());
    let uploaded_shard = backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(shard_body.clone()),
            None,
            ShardMetadataLimits::default(),
        )
        .await;
    assert!(uploaded_shard.is_ok());

    let shard_hash = shard_hash_hex(shard_body.as_ref());
    let shard_key = shard_object_key(&shard_hash);
    assert!(shard_key.is_ok());
    let Ok(shard_key) = shard_key else {
        return;
    };
    let removed = fs::remove_file(storage.path().join("chunks").join(shard_key.as_str())).await;
    assert!(removed.is_ok());

    let report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert_eq!(report.inspected_dedupe_shard_mappings, 1);
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == LocalFsckIssueKind::MissingDedupeShardObject)
    );
    assert!(!report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_reports_missing_native_xet_backing_chunk() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let (xorb_body, xorb_hash) = single_chunk_xorb(b"aaaa");
    let (shard_body, _file_hash) = single_file_shard(&[(b"aaaa", &xorb_hash)]);
    let uploaded_xorb = backend.upload_xorb(&xorb_hash, xorb_body).await;
    assert!(uploaded_xorb.is_ok());
    let uploaded_shard = backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(shard_body),
            None,
            ShardMetadataLimits::default(),
        )
        .await;
    assert!(uploaded_shard.is_ok());

    let native_chunk_hash = xet_hash_hex(&compute_data_hash(b"aaaa"));
    let chunk_prefix = native_chunk_hash.get(..2);
    assert!(chunk_prefix.is_some());
    let Some(chunk_prefix) = chunk_prefix else {
        return;
    };
    let removed = fs::remove_file(
        storage
            .path()
            .join("chunks")
            .join(chunk_prefix)
            .join(&native_chunk_hash),
    )
    .await;
    assert!(removed.is_ok());

    let report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == LocalFsckIssueKind::MissingChunk)
    );
    assert!(!report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_inspects_retained_shard_without_workspace_directory() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let (xorb_body, xorb_hash) = single_chunk_xorb(b"aaaa");
    let (shard_body, _file_hash) = single_file_shard(&[(b"aaaa", &xorb_hash)]);
    let uploaded_xorb = backend.upload_xorb(&xorb_hash, xorb_body).await;
    assert!(uploaded_xorb.is_ok());
    let uploaded_shard = backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(shard_body),
            None,
            ShardMetadataLimits::default(),
        )
        .await;
    assert!(uploaded_shard.is_ok());

    let report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert_eq!(report.inspected_dedupe_shard_mappings, 1);
    let shard_workspace_exists = storage.path().join("shards").try_exists();
    assert!(matches!(shard_workspace_exists, Ok(false)));
    assert!(!report.issues.iter().any(|issue| matches!(
        issue.kind,
        LocalFsckIssueKind::InvalidRetainedShard
            | LocalFsckIssueKind::InvalidDedupeShardMapping
            | LocalFsckIssueKind::MissingDedupeShardObject
    )));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_reports_reachable_quarantined_object() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let uploaded = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await;
    assert!(uploaded.is_ok());
    let Ok(uploaded) = uploaded else {
        return;
    };

    let first_chunk = uploaded.chunks.first();
    assert!(first_chunk.is_some());
    let Some(first_chunk) = first_chunk else {
        return;
    };
    let object_key = chunk_object_key(&first_chunk.hash);
    assert!(object_key.is_ok());
    let Ok(object_key) = object_key else {
        return;
    };
    let candidate = QuarantineCandidate::new(object_key, first_chunk.length, 20, 40);
    assert!(candidate.is_ok());
    let Ok(candidate) = candidate else {
        return;
    };
    let store = LocalIndexStore::open(storage.path().to_path_buf());
    let candidate_result = store.upsert_quarantine_candidate(&candidate);
    assert!(candidate_result.is_ok());

    let report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == LocalFsckIssueKind::ReachableQuarantinedObject)
    );
    assert!(!report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_reports_missing_active_retention_hold_object() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let store = LocalIndexStore::open(storage.path().to_path_buf());
    let object_key = ObjectKey::parse(&format!("de/{}", "de".repeat(32)));
    assert!(object_key.is_ok());
    let Ok(object_key) = object_key else {
        return;
    };
    let hold = RetentionHold::new(
        object_key,
        "provider deletion grace".to_owned(),
        10,
        Some(i64::MAX as u64),
    );
    assert!(hold.is_ok());
    let Ok(hold) = hold else {
        return;
    };
    let hold_result = store.upsert_retention_hold(&hold);
    assert!(hold_result.is_ok());

    let report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == LocalFsckIssueKind::MissingHeldObject)
    );
    assert!(!report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_reports_active_hold_that_still_has_quarantine_state() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let uploaded = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await;
    assert!(uploaded.is_ok());
    let Ok(uploaded) = uploaded else {
        return;
    };

    let first_chunk = uploaded.chunks.first();
    assert!(first_chunk.is_some());
    let Some(first_chunk) = first_chunk else {
        return;
    };
    let object_key = chunk_object_key(&first_chunk.hash);
    assert!(object_key.is_ok());
    let Ok(object_key) = object_key else {
        return;
    };
    let candidate = QuarantineCandidate::new(object_key.clone(), first_chunk.length, 20, 40);
    assert!(candidate.is_ok());
    let Ok(candidate) = candidate else {
        return;
    };
    let hold = RetentionHold::new(
        object_key,
        "provider deletion grace".to_owned(),
        10,
        Some(i64::MAX as u64),
    );
    assert!(hold.is_ok());
    let Ok(hold) = hold else {
        return;
    };
    let store = LocalIndexStore::open(storage.path().to_path_buf());
    let candidate_result = store.upsert_quarantine_candidate(&candidate);
    assert!(candidate_result.is_ok());
    let hold_result = store.upsert_retention_hold(&hold);
    assert!(hold_result.is_ok());

    let report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == LocalFsckIssueKind::HeldQuarantinedObject)
    );
    assert!(!report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_reports_future_dated_webhook_delivery() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let store = LocalIndexStore::open(storage.path().to_path_buf());
    let delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-future".to_owned(),
        unix_now_seconds_checked()
            .unwrap_or(0)
            .saturating_add(WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS + 3_600),
    );
    assert!(delivery.is_ok());
    let Ok(delivery) = delivery else {
        return;
    };
    let recorded = store.record_webhook_delivery(&delivery);
    assert!(recorded.is_ok());

    let report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert_eq!(report.inspected_webhook_deliveries, 1);
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == LocalFsckIssueKind::InvalidWebhookDeliveryTimestamp)
    );
    assert!(!report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_reports_reconstruction_with_missing_xorb_marker() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let store = LocalIndexStore::open(storage.path().to_path_buf());
    let file_id = FileId::new(ShardlineHash::from_bytes([41; 32]));
    let xorb_id = XorbId::new(ShardlineHash::from_bytes([42; 32]));
    let range = ChunkRange::new(0, 1);
    assert!(range.is_ok());
    let Ok(range) = range else {
        return;
    };
    let reconstruction =
        FileReconstruction::new(vec![ReconstructionTerm::new(xorb_id, range, 512)]);
    let inserted = store.insert_reconstruction(&file_id, &reconstruction);
    assert!(inserted.is_ok());

    let report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert_eq!(report.inspected_reconstructions, 1);
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind == LocalFsckIssueKind::MissingReconstructionXorb)
    );
    assert!(!report.is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fsck_counts_provider_repository_state_and_reports_future_timestamp() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let store = LocalIndexStore::open(storage.path().to_path_buf());
    let state = ProviderRepositoryState::new(
        RepositoryProvider::Gitea,
        "team".to_owned(),
        "assets".to_owned(),
        Some(
            unix_now_seconds_checked()
                .unwrap_or(0)
                .saturating_add(WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS + 3_600),
        ),
        None,
        None,
    );
    let inserted = store.upsert_provider_repository_state(&state);
    assert!(inserted.is_ok());

    let report = run_local_fsck(storage.path().to_path_buf()).await;
    assert!(report.is_ok());
    let Ok(report) = report else {
        return;
    };

    assert_eq!(report.inspected_provider_repository_states, 1);
    assert!(report.issues.iter().any(|issue| {
        issue.kind == LocalFsckIssueKind::InvalidProviderRepositoryStateTimestamp
    }));
    assert!(!report.is_clean());
}
