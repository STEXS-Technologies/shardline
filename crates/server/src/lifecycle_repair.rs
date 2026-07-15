use std::{collections::HashSet, path::PathBuf};

use shardline_index::{
    AsyncIndexStore, FileRecordStorageLayout, LocalIndexStore, PostgresIndexStore,
    PostgresRecordStore, RecordStore, RecordTraversal, xet_hash_hex_string,
};
use shardline_storage::ObjectStore;

use crate::{
    ServerConfig, ServerError, ServerFrontend,
    chunk_store::chunk_object_key,
    clock::unix_now_seconds_checked,
    object_store::{ServerObjectStore, object_store_from_config},
    overflow::checked_increment,
    postgres_backend::connect_postgres_metadata_pool,
    record_store::{LocalRecordStore, parse_stored_file_record_bytes},
    server_frontend::{
        optional_chunk_container_keys, referenced_term_object_key,
        visit_protocol_object_member_chunks,
    },
};

/// Default retention for processed webhook delivery claims before repair prunes them.
pub const DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS: u64 = 2_592_000;

pub(crate) const WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS: u64 = 300;

/// Lifecycle-repair execution options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecycleRepairOptions {
    /// Retention applied to processed webhook-delivery claims before they become repairable.
    pub webhook_retention_seconds: u64,
}

impl Default for LifecycleRepairOptions {
    fn default() -> Self {
        Self {
            webhook_retention_seconds: DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS,
        }
    }
}

/// Lifecycle-repair report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleRepairReport {
    /// Number of file and file-version records scanned to derive reachability.
    pub scanned_records: u64,
    /// Number of distinct object keys found reachable from current live metadata.
    pub referenced_objects: u64,
    /// Number of quarantine candidates inspected.
    pub scanned_quarantine_candidates: u64,
    /// Number of quarantine candidates removed because the object was already missing.
    pub removed_missing_quarantine_candidates: u64,
    /// Number of quarantine candidates removed because the object was reachable again.
    pub removed_reachable_quarantine_candidates: u64,
    /// Number of quarantine candidates removed because an active hold protected the object.
    pub removed_held_quarantine_candidates: u64,
    /// Number of retention holds inspected.
    pub scanned_retention_holds: u64,
    /// Number of expired retention holds removed.
    pub removed_expired_retention_holds: u64,
    /// Number of retention holds removed because the protected object was already missing.
    pub removed_missing_retention_holds: u64,
    /// Number of webhook delivery claims inspected.
    pub scanned_webhook_deliveries: u64,
    /// Number of webhook delivery claims removed because they were older than the configured retention.
    pub removed_stale_webhook_deliveries: u64,
    /// Number of webhook delivery claims removed because they were far in the future.
    pub removed_future_webhook_deliveries: u64,
}

#[derive(Debug, Default)]
struct RepairReachability {
    referenced_object_keys: HashSet<String>,
    live_dedupe_chunk_hashes: HashSet<String>,
    scanned_records: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuarantineRepairAction {
    Keep,
    DeleteMissing,
    DeleteReachable,
    DeleteHeld,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetentionHoldRepairAction {
    Keep,
    DeleteExpired,
    DeleteMissing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WebhookDeliveryRepairAction {
    Keep,
    DeleteStale,
    DeleteFuture,
}

/// Repairs stale lifecycle metadata against the configured metadata backend.
///
/// # Errors
///
/// Returns [`ServerError`] when metadata cannot be scanned or updated.
pub async fn run_lifecycle_repair(
    config: ServerConfig,
    options: LifecycleRepairOptions,
) -> Result<LifecycleRepairReport, ServerError> {
    let object_store = object_store_from_config(&config)?;
    if let Some(index_postgres_url) = config.index_postgres_url() {
        let pool = connect_postgres_metadata_pool(index_postgres_url, 4)?;
        let index_store = PostgresIndexStore::new(pool.clone());
        let record_store = PostgresRecordStore::new(pool);
        return run_lifecycle_repair_with_stores(
            &record_store,
            &index_store,
            &object_store,
            config.server_frontends(),
            options,
        )
        .await;
    }

    let index_store = LocalIndexStore::open(config.root_dir().to_path_buf());
    let record_store = LocalRecordStore::open(config.root_dir().to_path_buf());
    run_lifecycle_repair_with_stores(
        &record_store,
        &index_store,
        &object_store,
        config.server_frontends(),
        options,
    )
    .await
}

/// Repairs stale lifecycle metadata for one local storage root.
///
/// # Errors
///
/// Returns [`ServerError`] when metadata cannot be scanned or updated.
pub async fn run_local_lifecycle_repair(
    root: PathBuf,
    options: LifecycleRepairOptions,
) -> Result<LifecycleRepairReport, ServerError> {
    let object_store = ServerObjectStore::local(root.join("chunks"))?;
    let index_store = LocalIndexStore::open(root.clone());
    let record_store = LocalRecordStore::open(root);
    run_lifecycle_repair_with_stores(
        &record_store,
        &index_store,
        &object_store,
        &[ServerFrontend::Xet],
        options,
    )
    .await
}

async fn run_lifecycle_repair_with_stores<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    options: LifecycleRepairOptions,
) -> Result<LifecycleRepairReport, ServerError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ServerError>,
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<ServerError>,
{
    let now_unix_seconds = unix_now_seconds_checked()?;
    run_lifecycle_repair_with_stores_at_time(
        record_store,
        index_store,
        object_store,
        frontends,
        options,
        now_unix_seconds,
    )
    .await
}

async fn run_lifecycle_repair_with_stores_at_time<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    options: LifecycleRepairOptions,
    now_unix_seconds: u64,
) -> Result<LifecycleRepairReport, ServerError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ServerError>,
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<ServerError>,
{
    let mut reachability = RepairReachability::default();
    collect_referenced_object_keys(
        record_store,
        index_store,
        object_store,
        frontends,
        &mut reachability,
    )
    .await?;

    let max_processed_at_unix_seconds = now_unix_seconds
        .checked_add(WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS)
        .ok_or(ServerError::Overflow)?;
    let stale_webhook_cutoff = now_unix_seconds.saturating_sub(options.webhook_retention_seconds);

    let mut report = LifecycleRepairReport {
        scanned_records: reachability.scanned_records,
        referenced_objects: u64::try_from(reachability.referenced_object_keys.len())?,
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

    let retention_holds = index_store
        .list_retention_holds()
        .await
        .map_err(Into::into)?;
    let mut active_hold_object_keys = HashSet::new();
    for hold in retention_holds {
        report.scanned_retention_holds = checked_increment(report.scanned_retention_holds)?;
        match classify_retention_hold_repair_action(
            hold.release_after_unix_seconds(),
            hold.held_at_unix_seconds(),
            object_store.metadata(hold.object_key())?.is_some(),
            now_unix_seconds,
        ) {
            RetentionHoldRepairAction::Keep => {
                active_hold_object_keys.insert(hold.object_key().as_str().to_owned());
            }
            RetentionHoldRepairAction::DeleteExpired => {
                let _deleted = index_store
                    .delete_retention_hold(hold.object_key())
                    .await
                    .map_err(Into::into)?;
                report.removed_expired_retention_holds =
                    checked_increment(report.removed_expired_retention_holds)?;
            }
            RetentionHoldRepairAction::DeleteMissing => {
                let _deleted = index_store
                    .delete_retention_hold(hold.object_key())
                    .await
                    .map_err(Into::into)?;
                report.removed_missing_retention_holds =
                    checked_increment(report.removed_missing_retention_holds)?;
            }
        }
    }

    let quarantine_candidates = index_store
        .list_quarantine_candidates()
        .await
        .map_err(Into::into)?;
    for candidate in quarantine_candidates {
        report.scanned_quarantine_candidates =
            checked_increment(report.scanned_quarantine_candidates)?;
        let object_key = candidate.object_key();
        let object_exists = object_store.metadata(object_key)?.is_some();
        let action = classify_quarantine_repair_action(
            object_exists,
            reachability
                .referenced_object_keys
                .contains(candidate.object_key().as_str()),
            active_hold_object_keys.contains(candidate.object_key().as_str()),
        );
        match action {
            QuarantineRepairAction::Keep => {}
            QuarantineRepairAction::DeleteMissing => {
                let _deleted = index_store
                    .delete_quarantine_candidate(object_key)
                    .await
                    .map_err(Into::into)?;
                report.removed_missing_quarantine_candidates =
                    checked_increment(report.removed_missing_quarantine_candidates)?;
            }
            QuarantineRepairAction::DeleteReachable => {
                let _deleted = index_store
                    .delete_quarantine_candidate(object_key)
                    .await
                    .map_err(Into::into)?;
                report.removed_reachable_quarantine_candidates =
                    checked_increment(report.removed_reachable_quarantine_candidates)?;
            }
            QuarantineRepairAction::DeleteHeld => {
                let _deleted = index_store
                    .delete_quarantine_candidate(object_key)
                    .await
                    .map_err(Into::into)?;
                report.removed_held_quarantine_candidates =
                    checked_increment(report.removed_held_quarantine_candidates)?;
            }
        }
    }

    let webhook_deliveries = index_store
        .list_webhook_deliveries()
        .await
        .map_err(Into::into)?;
    for delivery in webhook_deliveries {
        report.scanned_webhook_deliveries = checked_increment(report.scanned_webhook_deliveries)?;
        match classify_webhook_delivery_repair_action(
            delivery.processed_at_unix_seconds(),
            stale_webhook_cutoff,
            max_processed_at_unix_seconds,
        ) {
            WebhookDeliveryRepairAction::Keep => {}
            WebhookDeliveryRepairAction::DeleteStale => {
                let _deleted = index_store
                    .delete_webhook_delivery(&delivery)
                    .await
                    .map_err(Into::into)?;
                report.removed_stale_webhook_deliveries =
                    checked_increment(report.removed_stale_webhook_deliveries)?;
            }
            WebhookDeliveryRepairAction::DeleteFuture => {
                let _deleted = index_store
                    .delete_webhook_delivery(&delivery)
                    .await
                    .map_err(Into::into)?;
                report.removed_future_webhook_deliveries =
                    checked_increment(report.removed_future_webhook_deliveries)?;
            }
        }
    }

    Ok(report)
}

async fn collect_referenced_object_keys<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    reachability: &mut RepairReachability,
) -> Result<(), ServerError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ServerError>,
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<ServerError>,
{
    RecordTraversal::visit_latest_records(record_store, |entry| {
        collect_record_object_references(object_store, frontends, &entry.bytes, reachability)
    })
    .await?;

    RecordTraversal::visit_version_records(record_store, |entry| {
        collect_record_object_references(object_store, frontends, &entry.bytes, reachability)
    })
    .await?;

    index_store
        .visit_dedupe_shard_mappings(|mapping| {
            let chunk_hash_hex = xet_hash_hex_string(mapping.chunk_hash());
            if reachability
                .live_dedupe_chunk_hashes
                .contains(&chunk_hash_hex)
            {
                reachability
                    .referenced_object_keys
                    .insert(mapping.shard_object_key().as_str().to_owned());
            }
            Ok::<(), ServerError>(())
        })
        .await?;

    Ok(())
}

fn collect_record_object_references(
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    record_bytes: &[u8],
    reachability: &mut RepairReachability,
) -> Result<(), ServerError> {
    let record = parse_stored_file_record_bytes(record_bytes)?;
    let storage_layout = record.storage_layout();
    reachability.scanned_records = checked_increment(reachability.scanned_records)?;
    for chunk in &record.chunks {
        match storage_layout {
            FileRecordStorageLayout::ReferencedObjectTerms => {
                let protocol_object_key = referenced_term_object_key(frontends, &chunk.hash)?;
                reachability
                    .referenced_object_keys
                    .insert(protocol_object_key.as_str().to_owned());
                visit_protocol_object_member_chunks(
                    frontends,
                    object_store,
                    &protocol_object_key,
                    |chunk_hash_hex| {
                        let chunk_key = chunk_object_key(&chunk_hash_hex)?;
                        reachability
                            .referenced_object_keys
                            .insert(chunk_key.as_str().to_owned());
                        Ok(())
                    },
                )?;
            }
            FileRecordStorageLayout::StoredChunks => {
                let chunk_key = chunk_object_key(&chunk.hash)?;
                reachability
                    .referenced_object_keys
                    .insert(chunk_key.as_str().to_owned());
                reachability
                    .live_dedupe_chunk_hashes
                    .insert(chunk.hash.clone());

                for protocol_object_key in optional_chunk_container_keys(frontends, &chunk.hash)? {
                    if object_store.metadata(&protocol_object_key)?.is_some() {
                        reachability
                            .referenced_object_keys
                            .insert(protocol_object_key.as_str().to_owned());
                    }
                }
            }
        }
    }

    Ok(())
}

pub(crate) const fn classify_quarantine_repair_action(
    object_exists: bool,
    is_reachable: bool,
    is_held: bool,
) -> QuarantineRepairAction {
    if !object_exists {
        return QuarantineRepairAction::DeleteMissing;
    }
    if is_reachable {
        return QuarantineRepairAction::DeleteReachable;
    }
    if is_held {
        return QuarantineRepairAction::DeleteHeld;
    }
    QuarantineRepairAction::Keep
}

pub(crate) const fn classify_retention_hold_repair_action(
    release_after_unix_seconds: Option<u64>,
    _held_at_unix_seconds: u64,
    object_exists: bool,
    now_unix_seconds: u64,
) -> RetentionHoldRepairAction {
    if let Some(release_after_unix_seconds) = release_after_unix_seconds
        && release_after_unix_seconds <= now_unix_seconds
    {
        return RetentionHoldRepairAction::DeleteExpired;
    }
    if !object_exists {
        return RetentionHoldRepairAction::DeleteMissing;
    }
    RetentionHoldRepairAction::Keep
}

pub(crate) const fn classify_webhook_delivery_repair_action(
    processed_at_unix_seconds: u64,
    stale_cutoff_unix_seconds: u64,
    max_processed_at_unix_seconds: u64,
) -> WebhookDeliveryRepairAction {
    if processed_at_unix_seconds > max_processed_at_unix_seconds {
        return WebhookDeliveryRepairAction::DeleteFuture;
    }
    if processed_at_unix_seconds <= stale_cutoff_unix_seconds {
        return WebhookDeliveryRepairAction::DeleteStale;
    }
    WebhookDeliveryRepairAction::Keep
}

#[cfg(test)]
mod tests {
    use shardline_index::{FileChunkRecord, FileRecord};

    use crate::{
        ServerFrontend, object_store::ServerObjectStore,
    };

    use super::{
        LifecycleRepairOptions, LifecycleRepairReport, QuarantineRepairAction, RepairReachability,
        RetentionHoldRepairAction, WebhookDeliveryRepairAction, classify_quarantine_repair_action,
        classify_retention_hold_repair_action, classify_webhook_delivery_repair_action,
        collect_record_object_references, DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS,
        WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS,
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
        assert_eq!(
            format!("{:?}", QuarantineRepairAction::Keep),
            "Keep"
        );
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
        assert_eq!(
            format!("{:?}", RetentionHoldRepairAction::Keep),
            "Keep"
        );
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
        assert_eq!(
            format!("{:?}", WebhookDeliveryRepairAction::Keep),
            "Keep"
        );
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
        let expected_chunk_key =
            crate::chunk_store::chunk_object_key(&hash).unwrap().as_str().to_owned();
        assert!(
            reachability.referenced_object_keys.contains(&expected_chunk_key),
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
        let expected_chunk_key =
            crate::chunk_store::chunk_object_key(&hash).unwrap().as_str().to_owned();
        assert!(reachability.referenced_object_keys.contains(&expected_chunk_key));

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

        let result = collect_record_object_references(
            &store,
            &frontends,
            b"not-valid-json",
            &mut reachability,
        );
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
            reachability.referenced_object_keys.contains(&expected_term_key),
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
        let mut reachability = RepairReachability { scanned_records: u64::MAX, ..Default::default() };

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
}
