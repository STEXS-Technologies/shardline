mod outcome;
mod records;
mod repository;
mod state;

use shardline_index::{
    AsyncIndexStore, LocalIndexStore, PostgresIndexStore, PostgresRecordStore, RecordStore,
    WebhookDelivery,
};
use shardline_protocol::unix_now_seconds_lossy;
use shardline_vcs::{ProviderKind, RepositoryWebhookEvent, RepositoryWebhookEventKind};

use crate::{
    ServerConfig, ServerError,
    object_store::{ServerObjectStore, object_store_from_config},
    postgres_backend::connect_postgres_metadata_pool,
    record_store::LocalRecordStore,
};
use outcome::duplicate_webhook_outcome;
use repository::{apply_repository_deleted, apply_repository_renamed};
use state::{apply_access_changed, apply_revision_pushed};

/// Summary of one handled provider webhook.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderWebhookOutcome {
    /// Affected provider family.
    pub provider: ProviderKind,
    /// Affected repository owner or namespace.
    pub owner: String,
    /// Affected repository name.
    pub repo: String,
    /// Provider delivery identifier.
    pub delivery_id: String,
    /// Normalized webhook event kind.
    pub event_kind: ProviderWebhookOutcomeKind,
    /// Number of file-version records scanned for the repository.
    pub affected_file_versions: u64,
    /// Number of distinct chunk objects referenced by affected file versions.
    pub affected_chunks: u64,
    /// Number of retention holds inserted or refreshed by the event.
    pub applied_holds: u64,
    /// Retention applied to newly created holds, if any.
    pub retention_seconds: Option<u64>,
}

/// Publicly reportable webhook outcome kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderWebhookOutcomeKind {
    /// Repository deletion created durable retention holds.
    RepositoryDeleted,
    /// Repository rename migrated durable metadata into the new repository scope.
    RepositoryRenamed {
        /// New repository owner or namespace.
        new_owner: String,
        /// New repository name.
        new_repo: String,
    },
    /// Repository access changed without durable state mutation.
    AccessChanged,
    /// A revision moved without durable state mutation.
    RevisionPushed {
        /// Updated revision reference.
        revision: String,
    },
}

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

async fn apply_provider_webhook_with_stores<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    event: &RepositoryWebhookEvent,
) -> Result<ProviderWebhookOutcome, ServerError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ServerError>,
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ServerError>,
{
    let recorded_delivery = WebhookDelivery::new(
        event.repository().provider().repository_provider(),
        event.repository().owner().to_owned(),
        event.repository().name().to_owned(),
        event.delivery_id().as_str().to_owned(),
        unix_now_seconds_lossy(),
    )?;
    if !index_store
        .record_webhook_delivery(&recorded_delivery)
        .await
        .map_err(Into::into)?
    {
        return Ok(duplicate_webhook_outcome(event));
    }

    let outcome = match event.kind() {
        RepositoryWebhookEventKind::RepositoryDeleted => {
            apply_repository_deleted(record_store, index_store, object_store, event).await
        }
        RepositoryWebhookEventKind::RepositoryRenamed { new_repository } => {
            apply_repository_renamed(record_store, index_store, event, new_repository).await
        }
        RepositoryWebhookEventKind::AccessChanged => {
            apply_access_changed(
                index_store,
                event,
                recorded_delivery.processed_at_unix_seconds(),
            )
            .await
        }
        RepositoryWebhookEventKind::RevisionPushed { revision } => {
            apply_revision_pushed(
                index_store,
                event,
                revision.as_str(),
                recorded_delivery.processed_at_unix_seconds(),
            )
            .await
        }
    };

    match outcome {
        Ok(outcome) => Ok(outcome),
        Err(error) => {
            let _deleted_delivery = index_store
                .delete_webhook_delivery(&recorded_delivery)
                .await
                .map_err(Into::into)?;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests;
