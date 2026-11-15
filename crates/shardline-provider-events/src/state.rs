use shardline_index::{AsyncIndexStore, ProviderRepositoryState};
use shardline_vcs::{RepositoryRef, RepositoryWebhookEvent};

use crate::ProviderEventsError;

use super::{ProviderWebhookOutcome, ProviderWebhookOutcomeKind};

pub(super) async fn apply_access_changed<IndexAdapter>(
    index_store: &IndexAdapter,
    event: &RepositoryWebhookEvent,
    processed_at_unix_seconds: u64,
) -> Result<ProviderWebhookOutcome, ProviderEventsError>
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ProviderEventsError>,
{
    upsert_provider_repository_state(
        index_store,
        event.repository(),
        Some(processed_at_unix_seconds),
        None,
        None,
    )
    .await?;

    Ok(ProviderWebhookOutcome::for_event(
        event,
        ProviderWebhookOutcomeKind::AccessChanged,
    ))
}

pub(super) async fn apply_revision_pushed<IndexAdapter>(
    index_store: &IndexAdapter,
    event: &RepositoryWebhookEvent,
    revision: &str,
    processed_at_unix_seconds: u64,
) -> Result<ProviderWebhookOutcome, ProviderEventsError>
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ProviderEventsError>,
{
    upsert_provider_repository_state(
        index_store,
        event.repository(),
        None,
        Some(processed_at_unix_seconds),
        Some(revision),
    )
    .await?;

    Ok(ProviderWebhookOutcome::for_event(
        event,
        ProviderWebhookOutcomeKind::RevisionPushed {
            revision: revision.to_owned(),
        },
    ))
}

pub(super) async fn migrate_provider_repository_state<IndexAdapter>(
    index_store: &IndexAdapter,
    old_repository: &RepositoryRef,
    new_repository: &RepositoryRef,
) -> Result<(), ProviderEventsError>
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ProviderEventsError>,
{
    let Some(migrated) =
        planned_provider_repository_state_migration(index_store, old_repository, new_repository)
            .await?
    else {
        return Ok(());
    };
    let old_provider = old_repository.provider().repository_provider();
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

pub(super) async fn planned_provider_repository_state_migration<IndexAdapter>(
    index_store: &IndexAdapter,
    old_repository: &RepositoryRef,
    new_repository: &RepositoryRef,
) -> Result<Option<ProviderRepositoryState>, ProviderEventsError>
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ProviderEventsError>,
{
    let old_provider = old_repository.provider().repository_provider();
    let Some(state) = index_store
        .provider_repository_state(old_provider, old_repository.owner(), old_repository.name())
        .await
        .map_err(Into::into)?
    else {
        return Ok(None);
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
    Ok(Some(migrated))
}

async fn upsert_provider_repository_state<IndexAdapter>(
    index_store: &IndexAdapter,
    repository: &RepositoryRef,
    access_changed_at_unix_seconds: Option<u64>,
    revision_pushed_at_unix_seconds: Option<u64>,
    revision: Option<&str>,
) -> Result<(), ProviderEventsError>
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ProviderEventsError>,
{
    let provider = repository.provider().repository_provider();
    let state = ProviderRepositoryState::new(
        provider,
        repository.owner().to_owned(),
        repository.name().to_owned(),
        access_changed_at_unix_seconds,
        revision_pushed_at_unix_seconds,
        revision.map(ToOwned::to_owned),
    );
    // The index adapters merge partial observations atomically while they hold
    // the repository row lock. Passing only this event's fields avoids a
    // separate read of the same row and cannot erase independent observations.
    index_store
        .upsert_provider_repository_state(&state)
        .await
        .map_err(Into::into)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use shardline_index::{
        AsyncIndexStore, LifecycleStore, LocalIndexStore, MemoryIndexStore, ProviderRepositoryState,
    };
    use shardline_protocol::RepositoryProvider;
    use shardline_vcs::{
        ProviderKind, RepositoryRef, RepositoryWebhookEvent, RepositoryWebhookEventKind,
        RevisionRef, WebhookDeliveryId,
    };

    use super::*;

    fn make_event(kind: RepositoryWebhookEventKind, delivery_id: &str) -> RepositoryWebhookEvent {
        RepositoryWebhookEvent::new(
            RepositoryRef::new(ProviderKind::GitHub, "team", "repo").unwrap(),
            WebhookDeliveryId::new(delivery_id).unwrap(),
            kind,
        )
    }

    #[tokio::test]
    async fn observations_return_outcomes_and_persist_state() {
        for (kind, expected_kind, at, access_at, revision_at, revision) in [
            (
                RepositoryWebhookEventKind::AccessChanged,
                ProviderWebhookOutcomeKind::AccessChanged,
                2000,
                Some(2000),
                None,
                None,
            ),
            (
                RepositoryWebhookEventKind::RevisionPushed {
                    revision: RevisionRef::new("refs/heads/main").unwrap(),
                },
                ProviderWebhookOutcomeKind::RevisionPushed {
                    revision: "refs/heads/main".into(),
                },
                4000,
                None,
                Some(4000),
                Some("refs/heads/main"),
            ),
        ] {
            let index = MemoryIndexStore::new();
            let event = make_event(kind, "delivery-observation");
            let outcome = if let Some(revision) = revision {
                apply_revision_pushed(&index, &event, revision, at).await
            } else {
                apply_access_changed(&index, &event, at).await
            }
            .unwrap();
            assert_eq!(
                outcome,
                ProviderWebhookOutcome {
                    provider: ProviderKind::GitHub,
                    owner: "team".into(),
                    repo: "repo".into(),
                    delivery_id: "delivery-observation".into(),
                    event_kind: expected_kind,
                    affected_file_versions: 0,
                    affected_chunks: 0,
                    applied_holds: 0,
                    retention_seconds: None,
                },
            );
            let state = LifecycleStore::provider_repository_state(
                &index,
                RepositoryProvider::GitHub,
                "team",
                "repo",
            )
            .unwrap()
            .unwrap();
            assert_eq!(state.last_access_changed_at_unix_seconds(), access_at);
            assert_eq!(state.last_revision_pushed_at_unix_seconds(), revision_at);
            assert_eq!(state.last_pushed_revision(), revision);
        }
    }

    async fn check_partial_upserts<IndexAdapter>(index: &IndexAdapter)
    where
        IndexAdapter: AsyncIndexStore,
        IndexAdapter::Error: Into<ProviderEventsError>,
    {
        let initial = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".into(),
            "repo".into(),
            None,
            Some(100),
            Some("refs/heads/main".into()),
        )
        .with_reconciliation(Some(110), Some(120), Some(130));
        AsyncIndexStore::upsert_provider_repository_state(index, &initial)
            .await
            .map_err(Into::<ProviderEventsError>::into)
            .unwrap();

        let event = make_event(
            RepositoryWebhookEventKind::AccessChanged,
            "delivery-access-preserves-revision",
        );
        apply_access_changed(index, &event, 200).await.unwrap();

        let state = AsyncIndexStore::provider_repository_state(
            index,
            RepositoryProvider::GitHub,
            "team",
            "repo",
        )
        .await
        .map_err(Into::<ProviderEventsError>::into)
        .unwrap()
        .unwrap();
        assert_eq!(state.last_access_changed_at_unix_seconds(), Some(200));
        assert_eq!(state.last_revision_pushed_at_unix_seconds(), Some(100));
        assert_eq!(state.last_pushed_revision(), Some("refs/heads/main"));
        assert_eq!(state.last_cache_invalidated_at_unix_seconds(), Some(110));
        assert_eq!(
            state.last_authorization_rechecked_at_unix_seconds(),
            Some(120)
        );
        assert_eq!(state.last_drift_checked_at_unix_seconds(), Some(130));

        let pushed = make_event(
            RepositoryWebhookEventKind::RevisionPushed {
                revision: RevisionRef::new("refs/heads/release").expect("valid revision"),
            },
            "delivery-push-preserves-access",
        );
        apply_revision_pushed(index, &pushed, "refs/heads/release", 300)
            .await
            .unwrap();
        let state = AsyncIndexStore::provider_repository_state(
            index,
            RepositoryProvider::GitHub,
            "team",
            "repo",
        )
        .await
        .map_err(Into::<ProviderEventsError>::into)
        .unwrap()
        .unwrap();
        assert_eq!(state.last_access_changed_at_unix_seconds(), Some(200));
        assert_eq!(state.last_revision_pushed_at_unix_seconds(), Some(300));
        assert_eq!(state.last_pushed_revision(), Some("refs/heads/release"));
        assert_eq!(state.last_cache_invalidated_at_unix_seconds(), Some(110));
        assert_eq!(
            state.last_authorization_rechecked_at_unix_seconds(),
            Some(120)
        );
        assert_eq!(state.last_drift_checked_at_unix_seconds(), Some(130));
    }

    #[tokio::test]
    async fn provider_event_partial_upserts_preserve_other_observations_on_both_backends() {
        check_partial_upserts(&MemoryIndexStore::new()).await;
        let directory = tempfile::tempdir().unwrap();
        let index = LocalIndexStore::new(directory.path().to_path_buf()).unwrap();
        check_partial_upserts(&index).await;
    }

    #[tokio::test]
    async fn migrate_provider_repository_state_moves_state() {
        let index = MemoryIndexStore::new();
        let old_repo = RepositoryRef::new(ProviderKind::GitHub, "team", "old-repo").unwrap();
        let new_repo = RepositoryRef::new(ProviderKind::GitHub, "team", "new-repo").unwrap();

        // Seed old repo state with reconciliation metadata.
        let old_state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "old-repo".to_owned(),
            Some(100),
            Some(200),
            Some("refs/heads/main".to_owned()),
        )
        .with_reconciliation(Some(300), Some(400), Some(500));
        LifecycleStore::upsert_provider_repository_state(&index, &old_state).unwrap();

        // Migrate.
        migrate_provider_repository_state(&index, &old_repo, &new_repo)
            .await
            .unwrap();

        // Old state is gone.
        let old = LifecycleStore::provider_repository_state(
            &index,
            RepositoryProvider::GitHub,
            "team",
            "old-repo",
        )
        .unwrap();
        assert!(old.is_none());

        // New state exists with migrated fields.
        let new = LifecycleStore::provider_repository_state(
            &index,
            RepositoryProvider::GitHub,
            "team",
            "new-repo",
        )
        .unwrap();
        assert!(new.is_some());
        let new = new.unwrap();
        assert_eq!(new.last_access_changed_at_unix_seconds(), Some(100));
        assert_eq!(new.last_revision_pushed_at_unix_seconds(), Some(200));
        assert_eq!(new.last_pushed_revision(), Some("refs/heads/main"));
        assert_eq!(new.last_cache_invalidated_at_unix_seconds(), Some(300));
        assert_eq!(
            new.last_authorization_rechecked_at_unix_seconds(),
            Some(400)
        );
        assert_eq!(new.last_drift_checked_at_unix_seconds(), Some(500));
    }

    #[tokio::test]
    async fn migrate_provider_repository_state_noop_when_old_state_missing() {
        let index = MemoryIndexStore::new();
        let old_repo = RepositoryRef::new(ProviderKind::GitHub, "team", "missing").unwrap();
        let new_repo = RepositoryRef::new(ProviderKind::GitHub, "team", "target").unwrap();

        let result = migrate_provider_repository_state(&index, &old_repo, &new_repo).await;
        assert!(result.is_ok());

        // New state should not exist either.
        let new = LifecycleStore::provider_repository_state(
            &index,
            RepositoryProvider::GitHub,
            "team",
            "target",
        )
        .unwrap();
        assert!(new.is_none());
    }

    #[tokio::test]
    async fn access_changed_then_revision_push_merges_state() {
        let index = MemoryIndexStore::new();
        let access_event = make_event(
            RepositoryWebhookEventKind::AccessChanged,
            "delivery-access-3",
        );
        let rev_event = make_event(
            RepositoryWebhookEventKind::RevisionPushed {
                revision: RevisionRef::new("refs/heads/dev").unwrap(),
            },
            "delivery-rev-3",
        );

        let _a = apply_access_changed(&index, &access_event, 500)
            .await
            .unwrap();
        let _r = apply_revision_pushed(&index, &rev_event, "refs/heads/dev", 600)
            .await
            .unwrap();

        let state = LifecycleStore::provider_repository_state(
            &index,
            RepositoryProvider::GitHub,
            "team",
            "repo",
        )
        .unwrap()
        .unwrap();
        assert_eq!(state.last_access_changed_at_unix_seconds(), Some(500));
        assert_eq!(state.last_revision_pushed_at_unix_seconds(), Some(600));
        assert_eq!(state.last_pushed_revision(), Some("refs/heads/dev"));
    }

    #[tokio::test]
    async fn revision_pushed_then_access_changed_merges_state() {
        let index = MemoryIndexStore::new();
        let rev_event = make_event(
            RepositoryWebhookEventKind::RevisionPushed {
                revision: RevisionRef::new("refs/heads/main").unwrap(),
            },
            "delivery-rev-first",
        );
        let access_event = make_event(
            RepositoryWebhookEventKind::AccessChanged,
            "delivery-access-second",
        );

        let _r = apply_revision_pushed(&index, &rev_event, "refs/heads/main", 700)
            .await
            .unwrap();
        let _a = apply_access_changed(&index, &access_event, 800)
            .await
            .unwrap();

        let state = LifecycleStore::provider_repository_state(
            &index,
            RepositoryProvider::GitHub,
            "team",
            "repo",
        )
        .unwrap()
        .unwrap();
        // access now has Some(800), revision still has Some(700) retained from existing state
        assert_eq!(state.last_access_changed_at_unix_seconds(), Some(800));
        assert_eq!(state.last_revision_pushed_at_unix_seconds(), Some(700));
        assert_eq!(state.last_pushed_revision(), Some("refs/heads/main"));
    }
}
