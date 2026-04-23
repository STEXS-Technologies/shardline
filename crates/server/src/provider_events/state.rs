use shardline_index::{AsyncIndexStore, ProviderRepositoryState};
use shardline_vcs::{RepositoryRef, RepositoryWebhookEvent};

use crate::ServerError;

use super::{ProviderWebhookOutcome, ProviderWebhookOutcomeKind};

pub(super) async fn apply_access_changed<IndexAdapter>(
    index_store: &IndexAdapter,
    event: &RepositoryWebhookEvent,
    processed_at_unix_seconds: u64,
) -> Result<ProviderWebhookOutcome, ServerError>
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ServerError>,
{
    upsert_provider_repository_state(
        index_store,
        event.repository(),
        Some(processed_at_unix_seconds),
        None,
        None,
    )
    .await?;

    Ok(ProviderWebhookOutcome {
        provider: event.repository().provider(),
        owner: event.repository().owner().to_owned(),
        repo: event.repository().name().to_owned(),
        delivery_id: event.delivery_id().as_str().to_owned(),
        event_kind: ProviderWebhookOutcomeKind::AccessChanged,
        affected_file_versions: 0,
        affected_chunks: 0,
        applied_holds: 0,
        retention_seconds: None,
    })
}

pub(super) async fn apply_revision_pushed<IndexAdapter>(
    index_store: &IndexAdapter,
    event: &RepositoryWebhookEvent,
    revision: &str,
    processed_at_unix_seconds: u64,
) -> Result<ProviderWebhookOutcome, ServerError>
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ServerError>,
{
    upsert_provider_repository_state(
        index_store,
        event.repository(),
        None,
        Some(processed_at_unix_seconds),
        Some(revision),
    )
    .await?;

    Ok(ProviderWebhookOutcome {
        provider: event.repository().provider(),
        owner: event.repository().owner().to_owned(),
        repo: event.repository().name().to_owned(),
        delivery_id: event.delivery_id().as_str().to_owned(),
        event_kind: ProviderWebhookOutcomeKind::RevisionPushed {
            revision: revision.to_owned(),
        },
        affected_file_versions: 0,
        affected_chunks: 0,
        applied_holds: 0,
        retention_seconds: None,
    })
}

pub(super) async fn migrate_provider_repository_state<IndexAdapter>(
    index_store: &IndexAdapter,
    old_repository: &RepositoryRef,
    new_repository: &RepositoryRef,
) -> Result<(), ServerError>
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ServerError>,
{
    let old_provider = old_repository.provider().repository_provider();
    let Some(state) = index_store
        .provider_repository_state(old_provider, old_repository.owner(), old_repository.name())
        .await
        .map_err(Into::into)?
    else {
        return Ok(());
    };
    let migrated = ProviderRepositoryState::new(
        new_repository.provider().repository_provider(),
        new_repository.owner().to_owned(),
        new_repository.name().to_owned(),
        state.last_access_changed_at_unix_seconds(),
        state.last_revision_pushed_at_unix_seconds(),
        state.last_pushed_revision().map(ToOwned::to_owned),
    )
    .with_reconciliation(
        state.last_cache_invalidated_at_unix_seconds(),
        state.last_authorization_rechecked_at_unix_seconds(),
        state.last_drift_checked_at_unix_seconds(),
    );
    index_store
        .upsert_provider_repository_state(&migrated)
        .await
        .map_err(Into::into)?;
    let _deleted = index_store
        .delete_provider_repository_state(
            old_provider,
            old_repository.owner(),
            old_repository.name(),
        )
        .await
        .map_err(Into::into)?;
    Ok(())
}

async fn upsert_provider_repository_state<IndexAdapter>(
    index_store: &IndexAdapter,
    repository: &RepositoryRef,
    access_changed_at_unix_seconds: Option<u64>,
    revision_pushed_at_unix_seconds: Option<u64>,
    revision: Option<&str>,
) -> Result<(), ServerError>
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ServerError>,
{
    let provider = repository.provider().repository_provider();
    let existing = index_store
        .provider_repository_state(provider, repository.owner(), repository.name())
        .await
        .map_err(Into::into)?;
    let state = ProviderRepositoryState::new(
        provider,
        repository.owner().to_owned(),
        repository.name().to_owned(),
        access_changed_at_unix_seconds.or_else(|| {
            existing
                .as_ref()
                .and_then(ProviderRepositoryState::last_access_changed_at_unix_seconds)
        }),
        revision_pushed_at_unix_seconds.or_else(|| {
            existing
                .as_ref()
                .and_then(ProviderRepositoryState::last_revision_pushed_at_unix_seconds)
        }),
        revision.map(ToOwned::to_owned).or_else(|| {
            existing
                .as_ref()
                .and_then(ProviderRepositoryState::last_pushed_revision)
                .map(ToOwned::to_owned)
        }),
    )
    .with_reconciliation(
        existing
            .as_ref()
            .and_then(ProviderRepositoryState::last_cache_invalidated_at_unix_seconds),
        existing
            .as_ref()
            .and_then(ProviderRepositoryState::last_authorization_rechecked_at_unix_seconds),
        existing
            .as_ref()
            .and_then(ProviderRepositoryState::last_drift_checked_at_unix_seconds),
    );
    index_store
        .upsert_provider_repository_state(&state)
        .await
        .map_err(Into::into)?;
    Ok(())
}
