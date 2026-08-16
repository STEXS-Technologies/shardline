use shardline_index::{
    AsyncIndexStore, LocalIndexStore, PostgresIndexStore, PostgresRecordStore, RecordStore,
};
use shardline_provider_events as provider_events_core;
use shardline_vcs::RepositoryWebhookEvent;

use crate::{
    ServerConfig, ServerError,
    object_store::{ServerObjectStore, object_store_from_config},
    postgres_backend::connect_postgres_metadata_pool,
    record_store::LocalRecordStore,
};

pub use provider_events_core::{
    ProviderEventsError, ProviderWebhookOutcome, ProviderWebhookOutcomeKind,
};

/// Applies a normalized provider webhook to Shardline lifecycle state.
///
/// # Errors
///
/// Returns [`ServerError`] when record or index storage cannot be read or updated.
pub async fn apply_provider_webhook(
    config: &ServerConfig,
    event: &RepositoryWebhookEvent,
) -> Result<ProviderWebhookOutcome, ServerError> {
    let object_store = object_store_from_config(config)?;
    if let Some(index_postgres_url) = config.index_postgres_url() {
        // Process-wide shared pool: sized like the server's own metadata pool
        // (16 connections) so concurrent webhook applications share one
        // adequately-sized pool instead of exhausting a tiny one.
        let pool = connect_postgres_metadata_pool(index_postgres_url, 4)?;
        let record_store = PostgresRecordStore::new(pool.clone());
        let index_store = PostgresIndexStore::new(pool);
        return apply_provider_webhook_with_stores(
            &record_store,
            &index_store,
            &object_store,
            event,
        )
        .await;
    }

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
