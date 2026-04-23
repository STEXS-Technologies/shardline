use std::{
    env::var as env_var,
    error::Error,
    num::NonZeroUsize,
    process::id as process_id,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::body::Bytes;
use shardline_index::{
    FileChunkRecord, FileRecord, IndexStore, LocalIndexStore, MemoryIndexStore, MemoryRecordStore,
    PostgresIndexStore, PostgresRecordStore, QuarantineCandidate, RecordStore, RetentionHold,
    WebhookDelivery,
};
use shardline_protocol::{RepositoryProvider, RepositoryScope};
use shardline_storage::ObjectKey;
use sqlx::{PgPool, postgres::PgPoolOptions, query};
use tokio::fs;
use url::Url;

use super::{
    DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS, LifecycleRepairOptions, QuarantineRepairAction,
    RetentionHoldRepairAction, WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS, WebhookDeliveryRepairAction,
    classify_quarantine_repair_action, classify_retention_hold_repair_action,
    classify_webhook_delivery_repair_action, run_lifecycle_repair_with_stores,
    run_lifecycle_repair_with_stores_at_time, run_local_lifecycle_repair,
};
use crate::{
    LocalBackend, ServerFrontend, apply_database_migrations, chunk_store::chunk_object_key,
    object_store::ServerObjectStore, record_store::LocalRecordStore,
    test_invariant_error::ServerTestInvariantError,
};

const LARGE_METADATA_RECORD_COUNT: usize = 128;
const DELIVERY_TIMESTAMP_TEST_SLACK_SECONDS: u64 = 3_600;

fn checked_inventory_value(index: u64, delta: u64) -> Result<u64, Box<dyn Error>> {
    index
        .checked_add(delta)
        .ok_or_else(|| ServerTestInvariantError::new("inventory index overflow").into())
}

#[test]
fn quarantine_repair_policy_is_prioritized_and_mutually_exclusive() {
    assert_eq!(
        classify_quarantine_repair_action(false, true, true),
        QuarantineRepairAction::DeleteMissing
    );
    assert_eq!(
        classify_quarantine_repair_action(true, true, true),
        QuarantineRepairAction::DeleteReachable
    );
    assert_eq!(
        classify_quarantine_repair_action(true, false, true),
        QuarantineRepairAction::DeleteHeld
    );
    assert_eq!(
        classify_quarantine_repair_action(true, false, false),
        QuarantineRepairAction::Keep
    );
}

#[test]
fn retention_hold_repair_policy_is_prioritized_and_mutually_exclusive() {
    assert_eq!(
        classify_retention_hold_repair_action(Some(9), 10, true, 10),
        RetentionHoldRepairAction::DeleteExpired
    );
    assert_eq!(
        classify_retention_hold_repair_action(None, 10, false, 10),
        RetentionHoldRepairAction::DeleteMissing
    );
    assert_eq!(
        classify_retention_hold_repair_action(Some(20), 10, true, 10),
        RetentionHoldRepairAction::Keep
    );
}

#[test]
fn webhook_delivery_repair_policy_is_prioritized_and_mutually_exclusive() {
    assert_eq!(
        classify_webhook_delivery_repair_action(101, 50, 100),
        WebhookDeliveryRepairAction::DeleteFuture
    );
    assert_eq!(
        classify_webhook_delivery_repair_action(50, 50, 100),
        WebhookDeliveryRepairAction::DeleteStale
    );
    assert_eq!(
        classify_webhook_delivery_repair_action(75, 50, 100),
        WebhookDeliveryRepairAction::Keep
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_repair_reconciles_stale_metadata() {
    let result = exercise_lifecycle_repair_reconciles_stale_metadata().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "lifecycle repair stale metadata flow failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_repair_reaches_a_fixed_point_on_second_run() {
    let result = exercise_lifecycle_repair_reaches_a_fixed_point_on_second_run().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "lifecycle repair fixed-point behavior failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_repair_reconciles_memory_index_adapter() {
    let result = exercise_lifecycle_repair_reconciles_memory_index_adapter().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "memory lifecycle repair adapter flow failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_repair_reconciles_postgres_index_adapter_when_live_database_is_available() {
    let result = exercise_lifecycle_repair_reconciles_postgres_index_adapter().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "postgres lifecycle repair adapter flow failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_repair_scans_large_metadata_inventory() {
    let result = exercise_lifecycle_repair_scans_large_metadata_inventory().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "large metadata inventory lifecycle repair failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_repair_webhook_delivery_boundaries_are_deterministic() {
    let result = exercise_lifecycle_repair_webhook_delivery_boundaries_are_deterministic().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "deterministic webhook delivery boundary repair failed: {error:?}"
    );
}

async fn exercise_lifecycle_repair_scans_large_metadata_inventory() -> Result<(), Box<dyn Error>> {
    let storage = tempfile::tempdir()?;
    let record_store = LocalRecordStore::open(storage.path().to_path_buf());
    let index_store = LocalIndexStore::open(storage.path().to_path_buf());

    for index in 0..LARGE_METADATA_RECORD_COUNT {
        let index_u64 = u64::try_from(index)?;
        let chunk_hash = format!("{:064x}", checked_inventory_value(index_u64, 10_000)?);
        let scope = RepositoryScope::new(
            RepositoryProvider::GitHub,
            &format!("team-{}", index % 8),
            &format!("assets-{}", index % 5),
            Some(&format!("rev-{}", index % 13)),
        )?;
        let record = FileRecord {
            file_id: format!("asset-{index:04}.bin"),
            content_hash: format!("{:064x}", checked_inventory_value(index_u64, 20_000)?),
            total_bytes: 32,
            chunk_size: 32,
            repository_scope: Some(scope),
            chunks: vec![FileChunkRecord {
                hash: chunk_hash.clone(),
                offset: 0,
                length: 32,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 32,
            }],
        };
        RecordStore::write_latest_record(&record_store, &record).await?;
        RecordStore::write_version_record(&record_store, &record).await?;

        let object_key = chunk_object_key(&chunk_hash)?;
        let object_path = storage.path().join("chunks").join(object_key.as_str());
        let object_parent = object_path
            .parent()
            .ok_or_else(|| ServerTestInvariantError::new("chunk path missing parent"))?;
        fs::create_dir_all(object_parent).await?;
        fs::write(&object_path, [u8::try_from(index % 251)?; 32]).await?;
        index_store.upsert_quarantine_candidate(&QuarantineCandidate::new(
            object_key,
            32,
            index_u64,
            checked_inventory_value(index_u64, 1)?,
        )?)?;
    }

    let report = run_local_lifecycle_repair(
        storage.path().to_path_buf(),
        LifecycleRepairOptions::default(),
    )
    .await?;

    assert_eq!(
        report.scanned_records,
        u64::try_from(LARGE_METADATA_RECORD_COUNT)?
            .checked_mul(2)
            .ok_or_else(|| ServerTestInvariantError::new("large metadata scan count overflow"))?
    );
    assert_eq!(
        report.referenced_objects,
        u64::try_from(LARGE_METADATA_RECORD_COUNT)?
    );
    assert_eq!(
        report.scanned_quarantine_candidates,
        u64::try_from(LARGE_METADATA_RECORD_COUNT)?
    );
    assert_eq!(
        report.removed_reachable_quarantine_candidates,
        u64::try_from(LARGE_METADATA_RECORD_COUNT)?
    );
    assert!(index_store.list_quarantine_candidates()?.is_empty());

    Ok(())
}

async fn exercise_lifecycle_repair_webhook_delivery_boundaries_are_deterministic()
-> Result<(), Box<dyn Error>> {
    let now_unix_seconds: u64 = 1_700_000_000;
    let storage = tempfile::tempdir()?;
    let object_store = ServerObjectStore::local(storage.path())?;
    let record_store = MemoryRecordStore::new();
    let index_store = MemoryIndexStore::new();

    let stale_at_cutoff = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-stale-at-cutoff".to_owned(),
        now_unix_seconds.saturating_sub(DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS),
    )?;
    let fresh_before_cutoff = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-fresh-before-cutoff".to_owned(),
        now_unix_seconds
            .saturating_sub(DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS.saturating_sub(1)),
    )?;
    let keep_at_future_limit = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-keep-at-future-limit".to_owned(),
        now_unix_seconds.saturating_add(WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS),
    )?;
    let future_beyond_limit = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-future-beyond-limit".to_owned(),
        now_unix_seconds
            .saturating_add(WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS)
            .saturating_add(1),
    )?;

    let _recorded_stale = index_store.record_webhook_delivery(&stale_at_cutoff)?;
    let _recorded_fresh = index_store.record_webhook_delivery(&fresh_before_cutoff)?;
    let _recorded_keep = index_store.record_webhook_delivery(&keep_at_future_limit)?;
    let _recorded_future = index_store.record_webhook_delivery(&future_beyond_limit)?;

    let report = run_lifecycle_repair_with_stores_at_time(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        LifecycleRepairOptions::default(),
        now_unix_seconds,
    )
    .await?;

    assert_eq!(report.scanned_webhook_deliveries, 4);
    assert_eq!(report.removed_stale_webhook_deliveries, 1);
    assert_eq!(report.removed_future_webhook_deliveries, 1);

    let remaining_deliveries = index_store.list_webhook_deliveries()?;
    assert_eq!(
        remaining_deliveries,
        vec![fresh_before_cutoff, keep_at_future_limit]
    );

    Ok(())
}

async fn exercise_lifecycle_repair_reconciles_postgres_index_adapter() -> Result<(), Box<dyn Error>>
{
    use shardline_index::AsyncIndexStore;

    let Some(base_url) = env_var("DATABASE_URL").ok() else {
        return Ok(());
    };

    let database_name = format!("shardline_lifecycle_repair_{}", process_id());
    let admin_url = database_url_for(&base_url, "yugabyte")?;
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await?;
    recreate_database(&admin_pool, &database_name).await?;

    let database_url = database_url_for(&base_url, &database_name)?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    apply_database_migrations(&pool).await?;

    let now_unix_seconds = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let storage = tempfile::tempdir()?;
    let object_store = ServerObjectStore::local(storage.path())?;
    let index_store = PostgresIndexStore::new(pool.clone());
    let record_store = PostgresRecordStore::new(pool);
    let chunk_hash = "b".repeat(64);
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None)?;
    let record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "c".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(scope),
        chunks: vec![FileChunkRecord {
            hash: chunk_hash.clone(),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    RecordStore::write_latest_record(&record_store, &record).await?;
    RecordStore::write_version_record(&record_store, &record).await?;

    let live_object_key = chunk_object_key(&chunk_hash)?;
    let live_path = storage.path().join(live_object_key.as_str());
    let live_parent = live_path
        .parent()
        .ok_or_else(|| ServerTestInvariantError::new("live object path missing parent"))?;
    fs::create_dir_all(live_parent).await?;
    fs::write(&live_path, b"bbbb").await?;
    AsyncIndexStore::upsert_quarantine_candidate(
        &index_store,
        &QuarantineCandidate::new(live_object_key.clone(), 4, 10, 20)?,
    )
    .await?;

    let missing_candidate_key = ObjectKey::parse(&format!("ff/{}", "ff".repeat(32)))?;
    AsyncIndexStore::upsert_quarantine_candidate(
        &index_store,
        &QuarantineCandidate::new(missing_candidate_key.clone(), 8, 11, 21)?,
    )
    .await?;

    let old_delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "postgres-delivery-old".to_owned(),
        now_unix_seconds.saturating_sub(
            DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS + DELIVERY_TIMESTAMP_TEST_SLACK_SECONDS,
        ),
    )?;
    let fresh_delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "postgres-delivery-fresh".to_owned(),
        now_unix_seconds
            .saturating_sub(DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS)
            .saturating_add(DELIVERY_TIMESTAMP_TEST_SLACK_SECONDS),
    )?;
    let _recorded_old =
        AsyncIndexStore::record_webhook_delivery(&index_store, &old_delivery).await?;
    let _recorded_fresh =
        AsyncIndexStore::record_webhook_delivery(&index_store, &fresh_delivery).await?;

    let report = run_lifecycle_repair_with_stores(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        LifecycleRepairOptions::default(),
    )
    .await?;

    assert_eq!(report.scanned_records, 2);
    assert_eq!(report.referenced_objects, 1);
    assert_eq!(report.scanned_quarantine_candidates, 2);
    assert_eq!(report.removed_missing_quarantine_candidates, 1);
    assert_eq!(report.removed_reachable_quarantine_candidates, 1);
    assert_eq!(report.scanned_webhook_deliveries, 2);
    assert_eq!(report.removed_stale_webhook_deliveries, 1);
    assert!(
        AsyncIndexStore::quarantine_candidate(&index_store, &live_object_key)
            .await?
            .is_none()
    );
    assert!(
        AsyncIndexStore::quarantine_candidate(&index_store, &missing_candidate_key)
            .await?
            .is_none()
    );
    assert_eq!(
        AsyncIndexStore::list_webhook_deliveries(&index_store).await?,
        vec![fresh_delivery]
    );

    Ok(())
}

async fn exercise_lifecycle_repair_reconciles_memory_index_adapter() -> Result<(), Box<dyn Error>> {
    let now_unix_seconds = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let storage = tempfile::tempdir()?;
    let object_store = ServerObjectStore::local(storage.path())?;
    let record_store = MemoryRecordStore::new();
    let index_store = MemoryIndexStore::new();
    let chunk_hash = "b".repeat(64);
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", None)?;
    let record = FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "c".repeat(64),
        total_bytes: 4,
        chunk_size: 4,
        repository_scope: Some(scope),
        chunks: vec![FileChunkRecord {
            hash: chunk_hash.clone(),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    };
    RecordStore::write_latest_record(&record_store, &record).await?;
    let live_object_key = chunk_object_key(&chunk_hash)?;
    let live_path = storage.path().join(live_object_key.as_str());
    let live_parent = live_path
        .parent()
        .ok_or_else(|| ServerTestInvariantError::new("live object path missing parent"))?;
    fs::create_dir_all(live_parent).await?;
    fs::write(&live_path, b"bbbb").await?;
    index_store.upsert_quarantine_candidate(&QuarantineCandidate::new(
        live_object_key,
        4,
        10,
        20,
    )?)?;
    let old_delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "memory-delivery-old".to_owned(),
        now_unix_seconds.saturating_sub(
            DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS + DELIVERY_TIMESTAMP_TEST_SLACK_SECONDS,
        ),
    )?;
    let _recorded_old = index_store.record_webhook_delivery(&old_delivery)?;

    let report = run_lifecycle_repair_with_stores(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        LifecycleRepairOptions::default(),
    )
    .await?;

    assert_eq!(report.scanned_records, 1);
    assert_eq!(report.scanned_quarantine_candidates, 1);
    assert_eq!(report.removed_reachable_quarantine_candidates, 1);
    assert_eq!(report.scanned_webhook_deliveries, 1);
    assert_eq!(report.removed_stale_webhook_deliveries, 1);
    assert!(index_store.list_quarantine_candidates()?.is_empty());
    assert!(index_store.list_webhook_deliveries()?.is_empty());

    Ok(())
}

async fn exercise_lifecycle_repair_reconciles_stale_metadata() -> Result<(), Box<dyn Error>> {
    let now_unix_seconds = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let storage = tempfile::tempdir()?;
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await?;
    let uploaded = backend
        .upload_file("asset.bin", Bytes::from_static(b"aaaabbbbcccc"), None)
        .await?;
    let first_chunk = uploaded
        .chunks
        .first()
        .ok_or_else(|| ServerTestInvariantError::new("missing uploaded chunk"))?;
    let live_object_key = chunk_object_key(&first_chunk.hash)?;

    let index_store = LocalIndexStore::open(storage.path().to_path_buf());
    index_store.upsert_quarantine_candidate(&QuarantineCandidate::new(
        live_object_key.clone(),
        first_chunk.length,
        10,
        20,
    )?)?;

    let missing_candidate_key = ObjectKey::parse(&format!("ff/{}", "ff".repeat(32)))?;
    index_store.upsert_quarantine_candidate(&QuarantineCandidate::new(
        missing_candidate_key,
        12,
        11,
        21,
    )?)?;

    let missing_hold_key = ObjectKey::parse(&format!("de/{}", "de".repeat(32)))?;
    index_store.upsert_retention_hold(&RetentionHold::new(
        missing_hold_key.clone(),
        "missing object".to_owned(),
        10,
        Some(20),
    )?)?;
    index_store.upsert_retention_hold(&RetentionHold::new(
        live_object_key.clone(),
        "active hold".to_owned(),
        10,
        Some(i64::MAX as u64),
    )?)?;
    index_store.upsert_quarantine_candidate(&QuarantineCandidate::new(
        live_object_key.clone(),
        first_chunk.length,
        30,
        40,
    )?)?;

    let orphan_key = ObjectKey::parse(&format!("ef/{}", "ef".repeat(32)))?;
    let orphan_path = storage
        .path()
        .join("chunks")
        .join("ef")
        .join("ef".repeat(32));
    let parent = orphan_path
        .parent()
        .ok_or_else(|| ServerTestInvariantError::new("orphan chunk path missing parent"))?;
    fs::create_dir_all(parent).await?;
    fs::write(&orphan_path, b"orphan").await?;
    index_store.upsert_quarantine_candidate(&QuarantineCandidate::new(
        orphan_key.clone(),
        6,
        50,
        60,
    )?)?;

    let held_orphan_key = ObjectKey::parse(&format!("aa/{}", "aa".repeat(32)))?;
    let held_orphan_path = storage
        .path()
        .join("chunks")
        .join("aa")
        .join("aa".repeat(32));
    let held_parent = held_orphan_path
        .parent()
        .ok_or_else(|| ServerTestInvariantError::new("held orphan path missing parent"))?;
    fs::create_dir_all(held_parent).await?;
    fs::write(&held_orphan_path, b"held-orphan").await?;
    index_store.upsert_retention_hold(&RetentionHold::new(
        held_orphan_key.clone(),
        "manual retention".to_owned(),
        10,
        Some(i64::MAX as u64),
    )?)?;
    index_store.upsert_quarantine_candidate(&QuarantineCandidate::new(
        held_orphan_key.clone(),
        11,
        70,
        80,
    )?)?;

    let old_delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-old".to_owned(),
        now_unix_seconds.saturating_sub(
            DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS + DELIVERY_TIMESTAMP_TEST_SLACK_SECONDS,
        ),
    )?;
    let future_delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-future".to_owned(),
        now_unix_seconds.saturating_add(
            WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS + DELIVERY_TIMESTAMP_TEST_SLACK_SECONDS,
        ),
    )?;
    let fresh_delivery = WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-fresh".to_owned(),
        now_unix_seconds,
    )?;
    let _recorded_old = index_store.record_webhook_delivery(&old_delivery)?;
    let _recorded_future = index_store.record_webhook_delivery(&future_delivery)?;
    let _recorded_fresh = index_store.record_webhook_delivery(&fresh_delivery)?;

    let report = run_local_lifecycle_repair(
        storage.path().to_path_buf(),
        LifecycleRepairOptions {
            webhook_retention_seconds: DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS,
        },
    )
    .await?;

    assert_eq!(report.scanned_quarantine_candidates, 4);
    assert_eq!(report.removed_missing_quarantine_candidates, 1);
    assert_eq!(report.removed_reachable_quarantine_candidates, 1);
    assert_eq!(report.removed_held_quarantine_candidates, 1);
    assert_eq!(report.scanned_retention_holds, 3);
    assert_eq!(report.removed_expired_retention_holds, 1);
    assert_eq!(report.removed_stale_webhook_deliveries, 1);
    assert_eq!(report.removed_future_webhook_deliveries, 1);

    let remaining_holds = index_store.list_retention_holds()?;
    assert_eq!(remaining_holds.len(), 2);
    let remaining_quarantine = index_store.list_quarantine_candidates()?;
    assert_eq!(
        remaining_quarantine,
        vec![QuarantineCandidate::new(orphan_key, 6, 50, 60)?]
    );
    let remaining_deliveries = index_store.list_webhook_deliveries()?;
    assert_eq!(remaining_deliveries, vec![fresh_delivery]);

    Ok(())
}

async fn exercise_lifecycle_repair_reaches_a_fixed_point_on_second_run()
-> Result<(), Box<dyn Error>> {
    let now_unix_seconds = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let storage = tempfile::tempdir()?;
    let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;

    let missing_candidate_key = ObjectKey::parse(&format!("ff/{}", "ff".repeat(32)))?;
    index_store.upsert_quarantine_candidate(&QuarantineCandidate::new(
        missing_candidate_key.clone(),
        8,
        11,
        21,
    )?)?;
    let missing_hold_key = ObjectKey::parse(&format!("de/{}", "de".repeat(32)))?;
    index_store.upsert_retention_hold(&RetentionHold::new(
        missing_hold_key.clone(),
        "missing object".to_owned(),
        10,
        Some(20),
    )?)?;
    index_store.record_webhook_delivery(&WebhookDelivery::new(
        RepositoryProvider::GitHub,
        "team".to_owned(),
        "assets".to_owned(),
        "delivery-stale".to_owned(),
        now_unix_seconds.saturating_sub(
            DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS + DELIVERY_TIMESTAMP_TEST_SLACK_SECONDS,
        ),
    )?)?;

    let first_report = run_local_lifecycle_repair(
        storage.path().to_path_buf(),
        LifecycleRepairOptions::default(),
    )
    .await?;
    let second_report = run_local_lifecycle_repair(
        storage.path().to_path_buf(),
        LifecycleRepairOptions::default(),
    )
    .await?;

    assert_eq!(first_report.scanned_quarantine_candidates, 1);
    assert_eq!(first_report.removed_missing_quarantine_candidates, 1);
    assert_eq!(first_report.scanned_retention_holds, 1);
    assert_eq!(first_report.removed_expired_retention_holds, 1);
    assert_eq!(first_report.scanned_webhook_deliveries, 1);
    assert_eq!(first_report.removed_stale_webhook_deliveries, 1);
    assert_eq!(second_report.scanned_quarantine_candidates, 0);
    assert_eq!(second_report.removed_missing_quarantine_candidates, 0);
    assert_eq!(second_report.scanned_retention_holds, 0);
    assert_eq!(second_report.removed_expired_retention_holds, 0);
    assert_eq!(second_report.removed_missing_retention_holds, 0);
    assert_eq!(second_report.scanned_webhook_deliveries, 0);
    assert_eq!(second_report.removed_stale_webhook_deliveries, 0);

    Ok(())
}

async fn recreate_database(pool: &PgPool, database_name: &str) -> Result<(), Box<dyn Error>> {
    query(&format!("DROP DATABASE IF EXISTS {database_name}"))
        .execute(pool)
        .await?;
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(pool)
        .await?;
    Ok(())
}
fn database_url_for(base_url: &str, database_name: &str) -> Result<String, Box<dyn Error>> {
    let mut url = Url::parse(base_url)?;
    url.set_path(database_name);
    Ok(url.to_string())
}
