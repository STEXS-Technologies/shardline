//! Integration tests for `shardline-provider-events`.
//!
//! These tests exercise the public API — [`apply_provider_webhook_with_stores`] —
//! through in-memory record and index stores so that no filesystem scaffolding
//! is required beyond a temporary object-store root.

use std::error::Error;

use shardline_index::{
    AsyncIndexStore, FileChunkRecord, FileRecord, MemoryIndexStore, MemoryRecordStore,
    PostgresIndexStore, PostgresProviderMutationOutcome, PostgresRecordStore,
    PostgresResourceFence, RecordMutation, RecordTraversal,
};
use shardline_protocol::{RepositoryProvider, RepositoryScope};
use shardline_server_core::ServerObjectStore;
use shardline_vcs::{
    ProviderKind, RepositoryRef, RepositoryWebhookEvent, RepositoryWebhookEventKind, RevisionRef,
    WebhookDeliveryId,
};

use shardline_provider_events::{
    ProviderEventsError, ProviderWebhookOutcomeKind, apply_provider_webhook_with_stores,
    plan_postgres_provider_repository_webhook,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A canned [`FileRecord`] whose chunks use a single 64-char hex hash so that
/// object-key derivation works without touching the object store.
fn test_record(scope: RepositoryScope) -> FileRecord {
    FileRecord {
        file_id: "asset.bin".to_owned(),
        content_hash: "a".repeat(64),
        total_bytes: 8,
        chunk_size: 4,
        storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
        repository_scope: Some(scope),
        chunks: vec![FileChunkRecord {
            hash: "b".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }],
    }
}

/// Short-hand for creating a temporary local object store.
fn local_object_store() -> Result<ServerObjectStore, Box<dyn Error>> {
    Ok(ServerObjectStore::local(
        tempfile::tempdir()?.path().join("chunks"),
    )?)
}

#[allow(clippy::panic_in_result_fn)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_rename_plan_commits_records_state_and_delivery_together()
-> Result<(), Box<dyn Error>> {
    let Ok(database_url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping: no DATABASE_URL");
        return Ok(());
    };
    let pool = sqlx::PgPool::connect(&database_url).await?;
    let records = PostgresRecordStore::new(pool.clone());
    let index = PostgresIndexStore::new(pool.clone());
    let old_owner = "provider-plan-rename";
    let old_repo = "old";
    let new_repo = "new";
    let delivery_id = "provider-plan-rename-delivery";
    sqlx::query(
        "DELETE FROM shardline_webhook_deliveries
         WHERE provider = 'github' AND owner = $1 AND delivery_id = $2",
    )
    .bind(old_owner)
    .bind(delivery_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        "DELETE FROM shardline_provider_repository_states
         WHERE provider = 'github' AND owner = $1 AND repo IN ($2, $3)",
    )
    .bind(old_owner)
    .bind(old_repo)
    .bind(new_repo)
    .execute(&pool)
    .await?;

    let old_scope = RepositoryScope::new(
        RepositoryProvider::GitHub,
        old_owner,
        old_repo,
        Some("main"),
    )?;
    let old_record = test_record(old_scope);
    RecordMutation::write_version_record(&records, &old_record).await?;
    RecordMutation::write_latest_record(&records, &old_record).await?;
    index
        .upsert_provider_repository_state(&shardline_index::ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            old_owner.to_owned(),
            old_repo.to_owned(),
            Some(100),
            Some(200),
            Some("main".to_owned()),
        ))
        .await?;

    let old_repository = RepositoryRef::new(ProviderKind::GitHub, old_owner, old_repo)?;
    let new_repository = RepositoryRef::new(ProviderKind::GitHub, old_owner, new_repo)?;
    let event = RepositoryWebhookEvent::new(
        old_repository,
        WebhookDeliveryId::new(delivery_id)?,
        RepositoryWebhookEventKind::RepositoryRenamed {
            new_repository: new_repository.clone(),
        },
    );
    let object_store = local_object_store()?;
    let plan =
        plan_postgres_provider_repository_webhook(&records, &index, &object_store, &event).await?;
    let (mutation, applied, _duplicate) = plan.into_parts();
    assert!(matches!(
        applied.event_kind,
        ProviderWebhookOutcomeKind::RepositoryRenamed { .. }
    ));

    let resource = format!("github:{old_owner}/{old_repo}");
    sqlx::query(
        "INSERT INTO shardline_resource_fences (domain, resource, epoch)
         VALUES ('provider-repository', $1, 77)
         ON CONFLICT (domain, resource) DO UPDATE SET epoch = EXCLUDED.epoch",
    )
    .bind(&resource)
    .execute(&pool)
    .await?;
    let fences = [PostgresResourceFence::new(
        shardline_index::ResourceLockKey::provider_repository("github", old_owner, old_repo),
        77,
    )];
    let mut connection = pool.acquire().await?;
    let committed = PostgresIndexStore::commit_provider_mutation_on_connection(
        &mut connection,
        &fences,
        &mutation,
    )
    .await?;
    assert_eq!(committed, PostgresProviderMutationOutcome::Applied);

    let old_version = RecordTraversal::version_record_locator(&records, &old_record);
    assert!(!RecordTraversal::record_locator_exists(&records, &old_version).await?);
    let mut renamed_record = old_record.clone();
    renamed_record.repository_scope = Some(RepositoryScope::new(
        RepositoryProvider::GitHub,
        old_owner,
        new_repo,
        Some("main"),
    )?);
    let new_version = RecordTraversal::version_record_locator(&records, &renamed_record);
    assert!(RecordTraversal::record_locator_exists(&records, &new_version).await?);
    assert!(
        index
            .provider_repository_state(RepositoryProvider::GitHub, old_owner, old_repo)
            .await?
            .is_none()
    );
    assert!(
        index
            .provider_repository_state(RepositoryProvider::GitHub, old_owner, new_repo)
            .await?
            .is_some()
    );
    let delivery_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM shardline_webhook_deliveries
         WHERE provider = 'github' AND owner = $1 AND repo = $2 AND delivery_id = $3",
    )
    .bind(old_owner)
    .bind(old_repo)
    .bind(delivery_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(delivery_count, 1);
    Ok(())
}

#[allow(clippy::panic_in_result_fn)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_delete_plan_commits_holds_records_state_and_delivery_together()
-> Result<(), Box<dyn Error>> {
    let Ok(database_url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping: no DATABASE_URL");
        return Ok(());
    };
    let pool = sqlx::PgPool::connect(&database_url).await?;
    let records = PostgresRecordStore::new(pool.clone());
    let index = PostgresIndexStore::new(pool.clone());
    let owner = "provider-plan-delete";
    let repo = "repository";
    let delivery_id = "provider-plan-delete-delivery";
    sqlx::query(
        "DELETE FROM shardline_webhook_deliveries
         WHERE provider = 'github' AND owner = $1 AND delivery_id = $2",
    )
    .bind(owner)
    .bind(delivery_id)
    .execute(&pool)
    .await?;

    let scope = RepositoryScope::new(RepositoryProvider::GitHub, owner, repo, Some("main"))?;
    let record = test_record(scope);
    RecordMutation::write_version_record(&records, &record).await?;
    RecordMutation::write_latest_record(&records, &record).await?;
    index
        .upsert_provider_repository_state(&shardline_index::ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            owner.to_owned(),
            repo.to_owned(),
            Some(100),
            None,
            None,
        ))
        .await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, owner, repo)?,
        WebhookDeliveryId::new(delivery_id)?,
        RepositoryWebhookEventKind::RepositoryDeleted,
    );
    let object_store = local_object_store()?;
    let plan =
        plan_postgres_provider_repository_webhook(&records, &index, &object_store, &event).await?;
    let (mutation, applied, _duplicate) = plan.into_parts();
    assert_eq!(
        applied.event_kind,
        ProviderWebhookOutcomeKind::RepositoryDeleted
    );
    assert_eq!(applied.applied_holds, 2);

    let resource = format!("github:{owner}/{repo}");
    sqlx::query(
        "INSERT INTO shardline_resource_fences (domain, resource, epoch)
         VALUES ('provider-repository', $1, 88)
         ON CONFLICT (domain, resource) DO UPDATE SET epoch = EXCLUDED.epoch",
    )
    .bind(&resource)
    .execute(&pool)
    .await?;
    let fences = [PostgresResourceFence::new(
        shardline_index::ResourceLockKey::provider_repository("github", owner, repo),
        88,
    )];
    let mut connection = pool.acquire().await?;
    let committed = PostgresIndexStore::commit_provider_mutation_on_connection(
        &mut connection,
        &fences,
        &mutation,
    )
    .await?;
    assert_eq!(committed, PostgresProviderMutationOutcome::Applied);

    let version = RecordTraversal::version_record_locator(&records, &record);
    let latest = RecordTraversal::latest_record_locator(&records, &record);
    assert!(!RecordTraversal::record_locator_exists(&records, &version).await?);
    assert!(!RecordTraversal::record_locator_exists(&records, &latest).await?);
    assert!(
        index
            .provider_repository_state(RepositoryProvider::GitHub, owner, repo)
            .await?
            .is_none()
    );
    let hold_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM shardline_retention_holds
         WHERE reason = $1",
    )
    .bind(format!(
        "provider repository deletion: github/{owner}/{repo}"
    ))
    .fetch_one(&pool)
    .await?;
    assert_eq!(hold_count, 2);
    let delivery_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM shardline_webhook_deliveries
         WHERE provider = 'github' AND owner = $1 AND repo = $2 AND delivery_id = $3",
    )
    .bind(owner)
    .bind(repo)
    .bind(delivery_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(delivery_count, 1);
    Ok(())
}

// ---------------------------------------------------------------------------
// All four event kinds
// ---------------------------------------------------------------------------

#[allow(clippy::panic_in_result_fn)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revision_pushed_produces_expected_outcome() -> Result<(), Box<dyn Error>> {
    let record_store = MemoryRecordStore::new();
    let index_store = MemoryIndexStore::new();
    let object_store = local_object_store()?;

    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", Some("main"))?;
    let record = test_record(scope);
    RecordMutation::write_version_record(&record_store, &record).await?;
    RecordMutation::write_latest_record(&record_store, &record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "repo")?,
        WebhookDeliveryId::new("delivery-rev-1")?,
        RepositoryWebhookEventKind::RevisionPushed {
            revision: RevisionRef::new("refs/heads/main")?,
        },
    );

    let outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;

    assert_eq!(
        outcome.event_kind,
        ProviderWebhookOutcomeKind::RevisionPushed {
            revision: "refs/heads/main".to_owned(),
        },
    );
    assert_eq!(outcome.affected_file_versions, 0);
    assert_eq!(outcome.affected_chunks, 0);
    assert_eq!(outcome.applied_holds, 0);
    assert!(outcome.retention_seconds.is_none());
    assert_eq!(outcome.provider, ProviderKind::GitHub);
    assert_eq!(outcome.owner.as_str(), "team");
    assert_eq!(outcome.repo.as_str(), "repo");

    // Provider repository state should have been updated.
    let state = shardline_index::LifecycleStore::provider_repository_state(
        &index_store,
        RepositoryProvider::GitHub,
        "team",
        "repo",
    )?;
    match state {
        Some(s) => {
            assert_eq!(s.last_pushed_revision(), Some("refs/heads/main"));
        }
        None => {
            return Err("provider repository state should exist after revision push".into());
        }
    }

    Ok(())
}

#[allow(clippy::panic_in_result_fn)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn access_changed_produces_expected_outcome() -> Result<(), Box<dyn Error>> {
    let record_store = MemoryRecordStore::new();
    let index_store = MemoryIndexStore::new();
    let object_store = local_object_store()?;

    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", Some("main"))?;
    let record = test_record(scope);
    RecordMutation::write_version_record(&record_store, &record).await?;
    RecordMutation::write_latest_record(&record_store, &record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "repo")?,
        WebhookDeliveryId::new("delivery-access-1")?,
        RepositoryWebhookEventKind::AccessChanged,
    );

    let outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;

    assert_eq!(
        outcome.event_kind,
        ProviderWebhookOutcomeKind::AccessChanged,
    );
    assert_eq!(outcome.affected_file_versions, 0);
    assert_eq!(outcome.affected_chunks, 0);
    assert_eq!(outcome.applied_holds, 0);
    assert!(outcome.retention_seconds.is_none());

    Ok(())
}

#[allow(clippy::panic_in_result_fn)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repository_deleted_with_records_creates_holds() -> Result<(), Box<dyn Error>> {
    let record_store = MemoryRecordStore::new();
    let index_store = MemoryIndexStore::new();
    let object_store = local_object_store()?;

    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))?;
    let record = test_record(scope);
    RecordMutation::write_version_record(&record_store, &record).await?;
    RecordMutation::write_latest_record(&record_store, &record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "assets")?,
        WebhookDeliveryId::new("delivery-del-1")?,
        RepositoryWebhookEventKind::RepositoryDeleted,
    );

    let outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;

    assert_eq!(
        outcome.event_kind,
        ProviderWebhookOutcomeKind::RepositoryDeleted,
    );
    assert_eq!(outcome.affected_file_versions, 1);
    assert_eq!(outcome.affected_chunks, 1);
    // One chunk + one xorb = two retention holds.
    assert_eq!(outcome.applied_holds, 2);
    assert!(
        outcome.retention_seconds.is_some(),
        "deleted repository should have a retention window",
    );

    // Verify records were removed.
    let latest_exists = RecordTraversal::record_locator_exists(
        &record_store,
        &RecordTraversal::latest_record_locator(&record_store, &record),
    )
    .await?;
    assert!(!latest_exists, "latest record should have been removed");

    let version_exists = RecordTraversal::record_locator_exists(
        &record_store,
        &RecordTraversal::version_record_locator(&record_store, &record),
    )
    .await?;
    assert!(!version_exists, "version record should have been removed");

    Ok(())
}

#[allow(clippy::panic_in_result_fn)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repository_renamed_with_records_migrates_scope() -> Result<(), Box<dyn Error>> {
    let record_store = MemoryRecordStore::new();
    let index_store = MemoryIndexStore::new();
    let object_store = local_object_store()?;

    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "old-name", Some("main"))?;
    let record = test_record(scope);
    RecordMutation::write_version_record(&record_store, &record).await?;
    RecordMutation::write_latest_record(&record_store, &record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "old-name")?,
        WebhookDeliveryId::new("delivery-rename-1")?,
        RepositoryWebhookEventKind::RepositoryRenamed {
            new_repository: RepositoryRef::new(ProviderKind::GitHub, "team", "new-name")?,
        },
    );

    let outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;

    assert_eq!(
        outcome.event_kind,
        ProviderWebhookOutcomeKind::RepositoryRenamed {
            new_owner: "team".to_owned(),
            new_repo: "new-name".to_owned(),
        },
    );
    assert_eq!(outcome.affected_file_versions, 1);
    assert_eq!(outcome.affected_chunks, 1);
    assert_eq!(outcome.applied_holds, 0);

    // Old-scope records should be gone.
    let old_latest_locator = RecordTraversal::latest_record_locator(&record_store, &record);
    let old_exists =
        RecordTraversal::record_locator_exists(&record_store, &old_latest_locator).await?;
    assert!(!old_exists, "old latest record should have been removed");

    // New-scope records should exist.
    let new_scope =
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "new-name", Some("main"))?;
    let renamed_record = FileRecord {
        repository_scope: Some(new_scope),
        ..record
    };

    let renamed_latest_exists = RecordTraversal::record_locator_exists(
        &record_store,
        &RecordTraversal::latest_record_locator(&record_store, &renamed_record),
    )
    .await?;
    assert!(renamed_latest_exists, "renamed latest record should exist",);

    let renamed_version_exists = RecordTraversal::record_locator_exists(
        &record_store,
        &RecordTraversal::version_record_locator(&record_store, &renamed_record),
    )
    .await?;
    assert!(
        renamed_version_exists,
        "renamed version record should exist",
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Duplicate webhook delivery
// ---------------------------------------------------------------------------

#[allow(clippy::panic_in_result_fn)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_webhook_delivery_is_noop() -> Result<(), Box<dyn Error>> {
    let record_store = MemoryRecordStore::new();
    let index_store = MemoryIndexStore::new();
    let object_store = local_object_store()?;

    let scope = RepositoryScope::new(
        RepositoryProvider::GitHub,
        "team",
        "dedup-repo",
        Some("main"),
    )?;
    let record = test_record(scope);
    RecordMutation::write_version_record(&record_store, &record).await?;
    RecordMutation::write_latest_record(&record_store, &record).await?;

    let event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "dedup-repo")?,
        WebhookDeliveryId::new("delivery-same")?,
        RepositoryWebhookEventKind::RepositoryDeleted,
    );

    // First application: mutate state.
    let first =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;
    assert_eq!(first.affected_file_versions, 1);
    assert_eq!(first.applied_holds, 2);

    // Second application with identical delivery: no-op (duplicate).
    let second =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
            .await?;
    assert_eq!(
        second.event_kind,
        ProviderWebhookOutcomeKind::RepositoryDeleted,
    );
    assert_eq!(second.affected_file_versions, 0);
    assert_eq!(second.affected_chunks, 0);
    assert_eq!(second.applied_holds, 0);
    assert!(second.retention_seconds.is_none());

    Ok(())
}

// ---------------------------------------------------------------------------
// Empty / unknown repository
// ---------------------------------------------------------------------------

#[allow(clippy::panic_in_result_fn)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn webhook_for_repository_without_records_returns_zero_counts() -> Result<(), Box<dyn Error>>
{
    let record_store = MemoryRecordStore::new();
    let index_store = MemoryIndexStore::new();
    let object_store = local_object_store()?;

    // No records at all — the repository does not exist in our store.
    for kind in [
        RepositoryWebhookEventKind::RepositoryDeleted,
        RepositoryWebhookEventKind::AccessChanged,
        RepositoryWebhookEventKind::RevisionPushed {
            revision: RevisionRef::new("refs/heads/feature")?,
        },
    ] {
        let event = RepositoryWebhookEvent::new(
            RepositoryRef::new(ProviderKind::GitHub, "team", "ghost")?,
            WebhookDeliveryId::new("delivery-ghost")?,
            kind,
        );

        let outcome =
            apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &event)
                .await?;

        assert_eq!(outcome.owner.as_str(), "team");
        assert_eq!(outcome.repo.as_str(), "ghost");
        assert_eq!(outcome.affected_file_versions, 0);
        assert_eq!(outcome.affected_chunks, 0);
        assert_eq!(outcome.applied_holds, 0);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Webhook delivery with an empty delivery identifier is rejected
// ---------------------------------------------------------------------------

#[allow(clippy::unwrap_used)]
#[test]
fn invalid_delivery_id_is_rejected() {
    let err = WebhookDeliveryId::new("").unwrap_err();
    assert_eq!(
        err,
        shardline_vcs::ProviderBoundaryError::Empty,
        "empty delivery id should be rejected",
    );
}

// ---------------------------------------------------------------------------
// ProviderEventsError display coverage
// ---------------------------------------------------------------------------

#[test]
fn provider_events_error_variants_implement_display() {
    // Verify a representative sample of error variants display correctly.
    let overflow = ProviderEventsError::Overflow;
    assert_eq!(overflow.to_string(), "arithmetic overflow");

    let invalid_hash = ProviderEventsError::InvalidContentHash;
    assert_eq!(
        invalid_hash.to_string(),
        "content hash must be 64 hexadecimal characters",
    );

    let invalid_payload = ProviderEventsError::InvalidProviderWebhookPayload;
    assert_eq!(
        invalid_payload.to_string(),
        "provider webhook payload was invalid",
    );

    let conflicting = ProviderEventsError::ConflictingRenameTargetRecord;
    assert_eq!(
        conflicting.to_string(),
        "repository rename target already contains conflicting metadata",
    );
}

// ---------------------------------------------------------------------------
// Cross-event state merging
// ---------------------------------------------------------------------------

#[allow(clippy::panic_in_result_fn)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn access_then_revision_merges_state() -> Result<(), Box<dyn Error>> {
    let record_store = MemoryRecordStore::new();
    let index_store = MemoryIndexStore::new();
    let object_store = local_object_store()?;

    let access_event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "stateful")?,
        WebhookDeliveryId::new("delivery-access-first")?,
        RepositoryWebhookEventKind::AccessChanged,
    );
    let rev_event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "stateful")?,
        WebhookDeliveryId::new("delivery-rev-second")?,
        RepositoryWebhookEventKind::RevisionPushed {
            revision: RevisionRef::new("refs/heads/main")?,
        },
    );

    let access_outcome = apply_provider_webhook_with_stores(
        &record_store,
        &index_store,
        &object_store,
        &access_event,
    )
    .await?;
    assert_eq!(
        access_outcome.event_kind,
        ProviderWebhookOutcomeKind::AccessChanged,
    );

    let rev_outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &rev_event)
            .await?;
    assert_eq!(
        rev_outcome.event_kind,
        ProviderWebhookOutcomeKind::RevisionPushed {
            revision: "refs/heads/main".to_owned(),
        },
    );

    // Both timestamps should be set in the merged state.
    let state = shardline_index::LifecycleStore::provider_repository_state(
        &index_store,
        RepositoryProvider::GitHub,
        "team",
        "stateful",
    )?;
    match state {
        Some(s) => {
            assert!(
                s.last_access_changed_at_unix_seconds().is_some(),
                "access-changed timestamp should be set",
            );
            assert!(
                s.last_revision_pushed_at_unix_seconds().is_some(),
                "revision-pushed timestamp should be set",
            );
            assert_eq!(s.last_pushed_revision(), Some("refs/heads/main"));
        }
        None => {
            return Err("provider repository state should exist".into());
        }
    }

    Ok(())
}

#[allow(clippy::panic_in_result_fn)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revision_then_access_merges_state() -> Result<(), Box<dyn Error>> {
    let record_store = MemoryRecordStore::new();
    let index_store = MemoryIndexStore::new();
    let object_store = local_object_store()?;

    let rev_event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "stateful")?,
        WebhookDeliveryId::new("delivery-rev-first")?,
        RepositoryWebhookEventKind::RevisionPushed {
            revision: RevisionRef::new("refs/heads/develop")?,
        },
    );
    let access_event = RepositoryWebhookEvent::new(
        RepositoryRef::new(ProviderKind::GitHub, "team", "stateful")?,
        WebhookDeliveryId::new("delivery-access-second")?,
        RepositoryWebhookEventKind::AccessChanged,
    );

    let _rev_outcome =
        apply_provider_webhook_with_stores(&record_store, &index_store, &object_store, &rev_event)
            .await?;
    let _access_outcome = apply_provider_webhook_with_stores(
        &record_store,
        &index_store,
        &object_store,
        &access_event,
    )
    .await?;

    let state = shardline_index::LifecycleStore::provider_repository_state(
        &index_store,
        RepositoryProvider::GitHub,
        "team",
        "stateful",
    )?;
    match state {
        Some(s) => {
            assert!(
                s.last_access_changed_at_unix_seconds().is_some(),
                "access-changed timestamp should be set",
            );
            assert!(
                s.last_revision_pushed_at_unix_seconds().is_some(),
                "revision-pushed timestamp should be set (retained from earlier event)",
            );
            assert_eq!(s.last_pushed_revision(), Some("refs/heads/develop"));
        }
        None => {
            return Err("provider repository state should exist".into());
        }
    }

    Ok(())
}
