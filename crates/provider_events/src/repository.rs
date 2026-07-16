use std::collections::HashSet;

use shardline_index::{
    AsyncIndexStore, FileRecord, RecordMutation, RecordStore, RecordTraversal, RetentionHold,
};
use shardline_protocol::unix_now_seconds_lossy;
use shardline_storage::ObjectKey;
use shardline_vcs::{RepositoryRef, RepositoryWebhookEvent};

use super::{
    DEFAULT_LOCAL_GC_RETENTION_SECONDS, ProviderWebhookOutcome, ProviderWebhookOutcomeKind,
    records::{
        collect_deleted_repository_record_references, ensure_absent_or_matching_record,
        parse_record_entry, record_belongs_to_repository, renamed_file_record,
        repository_record_scope,
    },
    state::migrate_provider_repository_state,
};
use crate::ProviderEventsError;
use shardline_server_core::ServerObjectStore;

pub async fn apply_repository_renamed<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    event: &RepositoryWebhookEvent,
    new_repository: &RepositoryRef,
) -> Result<ProviderWebhookOutcome, ProviderEventsError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ProviderEventsError>,
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ProviderEventsError>,
{
    let RepositoryRecords {
        latest_locators: old_latest_locators,
        version_locators: old_version_locators,
        latest_records: _,
        version_records,
    } = collect_repository_records(record_store, event.repository()).await?;
    let mut file_versions = 0_u64;
    let mut chunk_hashes = HashSet::new();
    let renamed_records = version_records
        .iter()
        .map(|record| renamed_file_record(record, new_repository))
        .collect::<Result<Vec<_>, _>>()?;

    for renamed_record in &renamed_records {
        ensure_absent_or_matching_record(
            record_store,
            &RecordTraversal::version_record_locator(record_store, renamed_record),
            renamed_record,
        )
        .await?;
        ensure_absent_or_matching_record(
            record_store,
            &RecordTraversal::latest_record_locator(record_store, renamed_record),
            renamed_record,
        )
        .await?;
    }

    for renamed_record in renamed_records {
        RecordMutation::write_version_record(record_store, &renamed_record)
            .await
            .map_err(Into::into)?;
        RecordMutation::write_latest_record(record_store, &renamed_record)
            .await
            .map_err(Into::into)?;

        file_versions = file_versions
            .checked_add(1)
            .ok_or(ProviderEventsError::Overflow)?;
        for chunk in &renamed_record.chunks {
            chunk_hashes.insert(chunk.hash.clone());
        }
    }

    delete_repository_records(record_store, old_latest_locators, old_version_locators).await?;
    migrate_provider_repository_state(index_store, event.repository(), new_repository).await?;

    Ok(ProviderWebhookOutcome {
        provider: event.repository().provider(),
        owner: event.repository().owner().to_owned(),
        repo: event.repository().name().to_owned(),
        delivery_id: event.delivery_id().as_str().to_owned(),
        event_kind: ProviderWebhookOutcomeKind::RepositoryRenamed {
            new_owner: new_repository.owner().to_owned(),
            new_repo: new_repository.name().to_owned(),
        },
        affected_file_versions: file_versions,
        affected_chunks: u64::try_from(chunk_hashes.len())?,
        applied_holds: 0,
        retention_seconds: None,
    })
}

pub async fn apply_repository_deleted<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    event: &RepositoryWebhookEvent,
) -> Result<ProviderWebhookOutcome, ProviderEventsError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ProviderEventsError>,
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ProviderEventsError>,
{
    let now_unix_seconds = unix_now_seconds_lossy();
    let delete_after_unix_seconds = now_unix_seconds
        .checked_add(DEFAULT_LOCAL_GC_RETENTION_SECONDS)
        .ok_or(ProviderEventsError::Overflow)?;
    let reason = format!(
        "provider repository deletion: {}/{}/{}",
        event.repository().provider().as_str(),
        event.repository().owner(),
        event.repository().name(),
    );
    let RepositoryRecords {
        latest_locators: old_latest_locators,
        version_locators: old_version_locators,
        latest_records,
        version_records,
    } = collect_repository_records(record_store, event.repository()).await?;

    let mut file_versions = 0_u64;
    let mut chunk_hashes = HashSet::new();
    let mut held_object_keys = HashSet::new();
    let mut seen_record_identities = HashSet::new();
    for record in latest_records.iter().chain(&version_records) {
        collect_deleted_repository_record_references(
            object_store,
            record,
            &mut seen_record_identities,
            &mut file_versions,
            &mut chunk_hashes,
            &mut held_object_keys,
        )?;
    }

    let mut applied_holds = 0_u64;
    for object_key in &held_object_keys {
        let hold = RetentionHold::new(
            ObjectKey::parse(object_key)
                .map_err(|_error| ProviderEventsError::InvalidContentHash)?,
            reason.clone(),
            now_unix_seconds,
            Some(delete_after_unix_seconds),
        )?;
        index_store
            .upsert_retention_hold(&hold)
            .await
            .map_err(Into::into)?;
        applied_holds = applied_holds
            .checked_add(1)
            .ok_or(ProviderEventsError::Overflow)?;
    }

    delete_repository_records(record_store, old_latest_locators, old_version_locators).await?;
    let _deleted_state = index_store
        .delete_provider_repository_state(
            event.repository().provider().repository_provider(),
            event.repository().owner(),
            event.repository().name(),
        )
        .await
        .map_err(Into::into)?;

    Ok(ProviderWebhookOutcome {
        provider: event.repository().provider(),
        owner: event.repository().owner().to_owned(),
        repo: event.repository().name().to_owned(),
        delivery_id: event.delivery_id().as_str().to_owned(),
        event_kind: ProviderWebhookOutcomeKind::RepositoryDeleted,
        affected_file_versions: file_versions,
        affected_chunks: u64::try_from(chunk_hashes.len())?,
        applied_holds,
        retention_seconds: Some(DEFAULT_LOCAL_GC_RETENTION_SECONDS),
    })
}

struct RepositoryRecords<Locator> {
    latest_locators: Vec<Locator>,
    version_locators: Vec<Locator>,
    latest_records: Vec<FileRecord>,
    version_records: Vec<FileRecord>,
}

async fn collect_repository_records<RecordAdapter>(
    record_store: &RecordAdapter,
    repository: &RepositoryRef,
) -> Result<RepositoryRecords<RecordAdapter::Locator>, ProviderEventsError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ProviderEventsError>,
{
    let repository_scope = repository_record_scope(repository);
    let mut records = RepositoryRecords {
        latest_locators: Vec::new(),
        version_locators: Vec::new(),
        latest_records: Vec::new(),
        version_records: Vec::new(),
    };

    RecordTraversal::visit_repository_latest_records(record_store, &repository_scope, |entry| {
        let record = parse_record_entry(&entry.bytes)?;
        if !record_belongs_to_repository(&record, repository) {
            return Ok::<(), ProviderEventsError>(());
        }

        records.latest_locators.push(entry.locator);
        records.latest_records.push(record);
        Ok(())
    })
    .await?;

    RecordTraversal::visit_repository_version_records(record_store, &repository_scope, |entry| {
        let record = parse_record_entry(&entry.bytes)?;
        if !record_belongs_to_repository(&record, repository) {
            return Ok::<(), ProviderEventsError>(());
        }

        records.version_locators.push(entry.locator);
        records.version_records.push(record);
        Ok(())
    })
    .await?;

    Ok(records)
}

async fn delete_repository_records<RecordAdapter>(
    record_store: &RecordAdapter,
    old_latest_locators: Vec<RecordAdapter::Locator>,
    old_version_locators: Vec<RecordAdapter::Locator>,
) -> Result<(), ProviderEventsError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ProviderEventsError>,
{
    for locator in old_latest_locators {
        RecordMutation::delete_record_locator(record_store, &locator)
            .await
            .map_err(Into::into)?;
    }
    for locator in old_version_locators {
        RecordMutation::delete_record_locator(record_store, &locator)
            .await
            .map_err(Into::into)?;
    }
    RecordMutation::prune_empty_latest_records(record_store)
        .await
        .map_err(Into::into)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use shardline_index::{
        FileChunkRecord, MemoryIndexStore, MemoryRecordStore, RecordMutation, RecordTraversal,
    };
    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    use shardline_vcs::{
        ProviderKind, RepositoryRef, RepositoryWebhookEvent, RepositoryWebhookEventKind,
        WebhookDeliveryId,
    };

    use super::{
        apply_repository_deleted, apply_repository_renamed, collect_repository_records,
        delete_repository_records,
    };

    fn test_record() -> FileChunkRecord {
        FileChunkRecord {
            hash: "a".repeat(64),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 4,
        }
    }

    #[tokio::test]
    async fn collect_repository_records_empty() {
        let store = MemoryRecordStore::new();
        let repo = RepositoryRef::new(ProviderKind::GitHub, "org", "empty-repo").unwrap();
        let records = collect_repository_records(&store, &repo).await.unwrap();
        assert!(records.latest_locators.is_empty());
        assert!(records.version_locators.is_empty());
        assert!(records.latest_records.is_empty());
        assert!(records.version_records.is_empty());
    }

    #[tokio::test]
    async fn collect_repository_records_filters_non_matching() {
        let store = MemoryRecordStore::new();
        let repo = RepositoryRef::new(ProviderKind::GitHub, "org", "repo").unwrap();

        let matching_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "org", "repo", Some("main")).unwrap();
        let non_matching_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "other", "repo", Some("main"))
                .unwrap();

        let matching = shardline_index::FileRecord {
            file_id: "matching.bin".to_owned(),
            content_hash: "b".repeat(64),
            total_bytes: 4,
            chunk_size: 4,
            repository_scope: Some(matching_scope),
            chunks: vec![test_record()],
        };
        let non_matching = shardline_index::FileRecord {
            file_id: "non_matching.bin".to_owned(),
            content_hash: "c".repeat(64),
            total_bytes: 4,
            chunk_size: 4,
            repository_scope: Some(non_matching_scope),
            chunks: vec![test_record()],
        };

        RecordMutation::write_version_record(&store, &matching)
            .await
            .unwrap();
        RecordMutation::write_version_record(&store, &non_matching)
            .await
            .unwrap();
        RecordMutation::write_latest_record(&store, &matching)
            .await
            .unwrap();
        RecordMutation::write_latest_record(&store, &non_matching)
            .await
            .unwrap();

        let records = collect_repository_records(&store, &repo).await.unwrap();
        assert_eq!(records.version_records.len(), 1);
        assert_eq!(records.latest_records.len(), 1);
        assert_eq!(records.version_records[0].file_id, "matching.bin");
        assert_eq!(records.latest_records[0].file_id, "matching.bin");
    }

    #[tokio::test]
    async fn delete_repository_records_empty_vecs_is_ok() {
        let store = MemoryRecordStore::new();
        let result = delete_repository_records::<MemoryRecordStore>(&store, vec![], vec![]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn delete_repository_records_removes_specified_locators() {
        let store = MemoryRecordStore::new();
        let record = shardline_index::FileRecord {
            file_id: "delete-me.bin".to_owned(),
            content_hash: "d".repeat(64),
            total_bytes: 4,
            chunk_size: 4,
            repository_scope: Some(
                RepositoryScope::new(RepositoryProvider::GitHub, "org", "repo", Some("main"))
                    .unwrap(),
            ),
            chunks: vec![test_record()],
        };

        RecordMutation::write_version_record(&store, &record)
            .await
            .unwrap();
        RecordMutation::write_latest_record(&store, &record)
            .await
            .unwrap();

        let latest_loc = RecordTraversal::latest_record_locator(&store, &record);
        let version_loc = RecordTraversal::version_record_locator(&store, &record);

        let result = delete_repository_records::<MemoryRecordStore>(
            &store,
            vec![latest_loc],
            vec![version_loc],
        )
        .await;
        assert!(result.is_ok());

        // Verify locators are gone
        let latest_loc2 = RecordTraversal::latest_record_locator(&store, &record);
        let version_loc2 = RecordTraversal::version_record_locator(&store, &record);
        assert!(
            !RecordTraversal::record_locator_exists(&store, &latest_loc2)
                .await
                .unwrap()
        );
        assert!(
            !RecordTraversal::record_locator_exists(&store, &version_loc2)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn delete_repository_records_only_latest() {
        let store = MemoryRecordStore::new();
        let record = shardline_index::FileRecord {
            file_id: "only-latest.bin".to_owned(),
            content_hash: "e".repeat(64),
            total_bytes: 4,
            chunk_size: 4,
            repository_scope: Some(
                RepositoryScope::new(RepositoryProvider::GitHub, "org", "repo", Some("main"))
                    .unwrap(),
            ),
            chunks: vec![test_record()],
        };

        RecordMutation::write_latest_record(&store, &record)
            .await
            .unwrap();

        let latest_loc = RecordTraversal::latest_record_locator(&store, &record);
        let result =
            delete_repository_records::<MemoryRecordStore>(&store, vec![latest_loc], vec![]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn delete_repository_records_only_version() {
        let store = MemoryRecordStore::new();
        let record = shardline_index::FileRecord {
            file_id: "only-version.bin".to_owned(),
            content_hash: "f".repeat(64),
            total_bytes: 4,
            chunk_size: 4,
            repository_scope: Some(
                RepositoryScope::new(RepositoryProvider::GitHub, "org", "repo", Some("main"))
                    .unwrap(),
            ),
            chunks: vec![test_record()],
        };

        RecordMutation::write_version_record(&store, &record)
            .await
            .unwrap();

        let version_loc = RecordTraversal::version_record_locator(&store, &record);
        let result =
            delete_repository_records::<MemoryRecordStore>(&store, vec![], vec![version_loc]).await;
        assert!(result.is_ok());
    }

    // ── apply_repository_deleted with empty records ───────────────────────

    #[tokio::test]
    async fn apply_repository_deleted_empty_repo_returns_zero_counts() {
        let record_store = MemoryRecordStore::new();
        let index_store = MemoryIndexStore::new();
        let object_store = shardline_server_core::ServerObjectStore::local(
            tempfile::tempdir().unwrap().path().join("chunks"),
        )
        .unwrap();
        let event = RepositoryWebhookEvent::new(
            RepositoryRef::new(ProviderKind::GitHub, "team", "empty-repo").unwrap(),
            WebhookDeliveryId::new("delivery-empty-1").unwrap(),
            RepositoryWebhookEventKind::RepositoryDeleted,
        );

        let outcome = apply_repository_deleted(&record_store, &index_store, &object_store, &event)
            .await
            .unwrap();

        assert_eq!(outcome.affected_file_versions, 0);
        assert_eq!(outcome.affected_chunks, 0);
        assert_eq!(outcome.applied_holds, 0);
        assert_eq!(outcome.provider, ProviderKind::GitHub);
        assert_eq!(outcome.owner, "team");
        assert_eq!(outcome.repo, "empty-repo");
    }

    // ── apply_repository_renamed with empty records ───────────────────────

    #[tokio::test]
    async fn apply_repository_renamed_empty_repo_returns_zero_counts() {
        let record_store = MemoryRecordStore::new();
        let index_store = MemoryIndexStore::new();
        let new_repository =
            RepositoryRef::new(ProviderKind::GitHub, "team", "renamed-repo").unwrap();
        let event = RepositoryWebhookEvent::new(
            RepositoryRef::new(ProviderKind::GitHub, "team", "empty-repo").unwrap(),
            WebhookDeliveryId::new("delivery-rename-empty-1").unwrap(),
            RepositoryWebhookEventKind::RepositoryRenamed {
                new_repository: new_repository.clone(),
            },
        );

        let outcome =
            apply_repository_renamed(&record_store, &index_store, &event, &new_repository)
                .await
                .unwrap();

        assert_eq!(outcome.affected_file_versions, 0);
        assert_eq!(outcome.affected_chunks, 0);
        assert_eq!(outcome.applied_holds, 0);
        assert_eq!(outcome.provider, ProviderKind::GitHub);
        assert_eq!(outcome.owner, "team");
        assert_eq!(outcome.repo, "empty-repo");
    }
}
