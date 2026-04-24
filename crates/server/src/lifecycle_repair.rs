use std::{collections::HashSet, path::PathBuf};

use shardline_index::{
    AsyncIndexStore, FileRecordStorageLayout, LocalIndexStore, PostgresIndexStore,
    PostgresRecordStore, RecordStore, xet_hash_hex_string,
};

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
    RecordStore::visit_latest_records(record_store, |entry| {
        collect_record_object_references(object_store, frontends, &entry.bytes, reachability)
    })
    .await?;

    RecordStore::visit_version_records(record_store, |entry| {
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
    held_at_unix_seconds: u64,
    object_exists: bool,
    now_unix_seconds: u64,
) -> RetentionHoldRepairAction {
    if let Some(release_after_unix_seconds) = release_after_unix_seconds
        && (release_after_unix_seconds < held_at_unix_seconds
            || release_after_unix_seconds <= now_unix_seconds)
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
mod tests;
