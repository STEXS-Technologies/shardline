use shardline_index::{AsyncIndexStore, LocalIndexStore, RecordStore};
use shardline_provider_events as provider_events_core;
use shardline_vcs::RepositoryWebhookEvent;

use crate::{
    ServerConfig, ServerError,
    object_store::{ServerObjectStore, object_store_from_config},
    record_store::LocalRecordStore,
};

pub use provider_events_core::{
    ProviderEventsError, ProviderWebhookOutcome, ProviderWebhookOutcomeKind,
};

/// Applies a normalized provider webhook to Shardline lifecycle state using
/// the server's LOCAL record and index stores.
///
/// Postgres-backed callers must use [`apply_provider_webhook_with_stores`]
/// with the backend's own stores (their pool is created once per server) —
/// opening a fresh per-call pool here would reintroduce the per-event pool
/// churn F-34 removed.
///
/// # Errors
///
/// Returns [`ServerError`] when record or index storage cannot be read or updated.
pub async fn apply_provider_webhook(
    config: &ServerConfig,
    event: &RepositoryWebhookEvent,
) -> Result<ProviderWebhookOutcome, ServerError> {
    let object_store = object_store_from_config(config)?;
    let record_store = LocalRecordStore::open(config.root_dir().to_path_buf());
    let index_store = LocalIndexStore::open(config.root_dir().to_path_buf());
    apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, event).await
}

pub(crate) async fn apply_provider_webhook_with_stores<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    event: &RepositoryWebhookEvent,
) -> Result<ProviderWebhookOutcome, ServerError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ServerError> + Into<ProviderEventsError>,
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ServerError> + Into<ProviderEventsError>,
{
    provider_events_core::apply_provider_webhook_with_stores(
        record_store,
        index_store,
        object_store,
        event,
    )
    .await
    .map_err(Into::into)
}

#[cfg(test)]
mod tests;
