use std::collections::HashSet;
use std::path::PathBuf;

use shardline_index::{
    AsyncIndexStore, LocalIndexStore, PostgresIndexStore, PostgresRecordStore, RecordStore,
};
use shardline_storage::ObjectStore;

use crate::{
    ServerConfig, ServerError, ServerFrontend,
    clock::unix_now_seconds_checked,
    object_store::{ServerObjectStore, object_store_from_config},
    overflow::checked_increment,
    postgres_backend::connect_postgres_metadata_pool,
    record_store::LocalRecordStore,
};

use super::WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS;
use super::classification::{
    classify_quarantine_repair_action, classify_retention_hold_repair_action,
    classify_webhook_delivery_repair_action,
};
use super::fault_injection::lifecycle_repair_failpoint;
use super::reachability::collect_referenced_object_keys;
use super::types::{
    LifecycleRepairBoundary, LifecycleRepairOptions, LifecycleRepairReport, QuarantineRepairAction,
    RepairReachability, RetentionHoldRepairAction, WebhookDeliveryRepairAction,
};

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

pub(crate) async fn run_lifecycle_repair_with_stores_at_time<RecordAdapter, IndexAdapter>(
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
                lifecycle_repair_failpoint(LifecycleRepairBoundary::AfterRetentionHoldMutation)?;
            }
            RetentionHoldRepairAction::DeleteMissing => {
                let _deleted = index_store
                    .delete_retention_hold(hold.object_key())
                    .await
                    .map_err(Into::into)?;
                report.removed_missing_retention_holds =
                    checked_increment(report.removed_missing_retention_holds)?;
                lifecycle_repair_failpoint(LifecycleRepairBoundary::AfterRetentionHoldMutation)?;
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
                lifecycle_repair_failpoint(
                    LifecycleRepairBoundary::AfterQuarantineCandidateMutation,
                )?;
            }
            QuarantineRepairAction::DeleteReachable => {
                let _deleted = index_store
                    .delete_quarantine_candidate(object_key)
                    .await
                    .map_err(Into::into)?;
                report.removed_reachable_quarantine_candidates =
                    checked_increment(report.removed_reachable_quarantine_candidates)?;
                lifecycle_repair_failpoint(
                    LifecycleRepairBoundary::AfterQuarantineCandidateMutation,
                )?;
            }
            QuarantineRepairAction::DeleteHeld => {
                let _deleted = index_store
                    .delete_quarantine_candidate(object_key)
                    .await
                    .map_err(Into::into)?;
                report.removed_held_quarantine_candidates =
                    checked_increment(report.removed_held_quarantine_candidates)?;
                lifecycle_repair_failpoint(
                    LifecycleRepairBoundary::AfterQuarantineCandidateMutation,
                )?;
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
                lifecycle_repair_failpoint(LifecycleRepairBoundary::AfterWebhookDeliveryMutation)?;
            }
            WebhookDeliveryRepairAction::DeleteFuture => {
                let _deleted = index_store
                    .delete_webhook_delivery(&delivery)
                    .await
                    .map_err(Into::into)?;
                report.removed_future_webhook_deliveries =
                    checked_increment(report.removed_future_webhook_deliveries)?;
                lifecycle_repair_failpoint(LifecycleRepairBoundary::AfterWebhookDeliveryMutation)?;
            }
        }
    }

    Ok(report)
}
