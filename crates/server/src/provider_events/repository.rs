use std::collections::HashSet;

use shardline_index::{AsyncIndexStore, FileRecord, RecordStore, RetentionHold};
use shardline_protocol::unix_now_seconds_lossy;
use shardline_storage::ObjectKey;
use shardline_vcs::{RepositoryRef, RepositoryWebhookEvent};

use super::{
    ProviderWebhookOutcome, ProviderWebhookOutcomeKind,
    records::{
        collect_deleted_repository_record_references, ensure_absent_or_matching_record,
        parse_record_entry, record_belongs_to_repository, renamed_file_record,
        repository_record_scope,
    },
    state::migrate_provider_repository_state,
};
use crate::{ServerError, gc::DEFAULT_LOCAL_GC_RETENTION_SECONDS, object_store::ServerObjectStore};

pub(super) async fn apply_repository_renamed<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    event: &RepositoryWebhookEvent,
    new_repository: &RepositoryRef,
) -> Result<ProviderWebhookOutcome, ServerError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ServerError>,
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ServerError>,
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
            &RecordStore::version_record_locator(record_store, renamed_record),
            renamed_record,
        )
        .await?;
        ensure_absent_or_matching_record(
            record_store,
            &RecordStore::latest_record_locator(record_store, renamed_record),
            renamed_record,
        )
        .await?;
    }

    for renamed_record in renamed_records {
        RecordStore::write_version_record(record_store, &renamed_record)
            .await
            .map_err(Into::into)?;
        RecordStore::write_latest_record(record_store, &renamed_record)
            .await
            .map_err(Into::into)?;

        file_versions = file_versions.checked_add(1).ok_or(ServerError::Overflow)?;
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

pub(super) async fn apply_repository_deleted<RecordAdapter, IndexAdapter>(
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
    let now_unix_seconds = unix_now_seconds_lossy();
    let delete_after_unix_seconds = now_unix_seconds
        .checked_add(DEFAULT_LOCAL_GC_RETENTION_SECONDS)
        .ok_or(ServerError::Overflow)?;
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
            ObjectKey::parse(object_key).map_err(|_error| ServerError::InvalidContentHash)?,
            reason.clone(),
            now_unix_seconds,
            Some(delete_after_unix_seconds),
        )?;
        index_store
            .upsert_retention_hold(&hold)
            .await
            .map_err(Into::into)?;
        applied_holds = applied_holds.checked_add(1).ok_or(ServerError::Overflow)?;
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
) -> Result<RepositoryRecords<RecordAdapter::Locator>, ServerError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ServerError>,
{
    let repository_scope = repository_record_scope(repository);
    let mut records = RepositoryRecords {
        latest_locators: Vec::new(),
        version_locators: Vec::new(),
        latest_records: Vec::new(),
        version_records: Vec::new(),
    };

    RecordStore::visit_repository_latest_records(record_store, &repository_scope, |entry| {
        let record = parse_record_entry(&entry.bytes)?;
        if !record_belongs_to_repository(&record, repository) {
            return Ok::<(), ServerError>(());
        }

        records.latest_locators.push(entry.locator);
        records.latest_records.push(record);
        Ok(())
    })
    .await?;

    RecordStore::visit_repository_version_records(record_store, &repository_scope, |entry| {
        let record = parse_record_entry(&entry.bytes)?;
        if !record_belongs_to_repository(&record, repository) {
            return Ok::<(), ServerError>(());
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
) -> Result<(), ServerError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ServerError>,
{
    for locator in old_latest_locators {
        RecordStore::delete_record_locator(record_store, &locator)
            .await
            .map_err(Into::into)?;
    }
    for locator in old_version_locators {
        RecordStore::delete_record_locator(record_store, &locator)
            .await
            .map_err(Into::into)?;
    }
    RecordStore::prune_empty_latest_records(record_store)
        .await
        .map_err(Into::into)?;
    Ok(())
}
