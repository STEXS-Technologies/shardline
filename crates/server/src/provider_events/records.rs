use std::collections::HashSet;

use serde_json::to_vec;
use shardline_index::{FileRecord, RecordStore, RepositoryRecordScope};
use shardline_protocol::RepositoryScope;
use shardline_vcs::RepositoryRef;

use crate::{
    ServerError,
    chunk_store::chunk_object_key,
    object_store::ServerObjectStore,
    record_store::parse_stored_file_record_bytes,
    repository_scope_path::provider_directory,
    xet_adapter::{visit_stored_xorb_chunk_hashes, xorb_object_key},
};

pub(super) async fn ensure_absent_or_matching_record<RecordAdapter>(
    record_store: &RecordAdapter,
    locator: &RecordAdapter::Locator,
    record: &FileRecord,
) -> Result<(), ServerError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ServerError>,
{
    if !RecordStore::record_locator_exists(record_store, locator)
        .await
        .map_err(Into::into)?
    {
        return Ok(());
    }

    let expected_bytes = to_vec(record)?;
    let existing_bytes = RecordStore::read_record_bytes(record_store, locator)
        .await
        .map_err(Into::into)?;
    if existing_bytes == expected_bytes {
        return Ok(());
    }

    Err(ServerError::ConflictingRenameTargetRecord)
}

pub(super) fn collect_deleted_repository_record_references(
    object_store: &ServerObjectStore,
    record: &FileRecord,
    seen_record_identities: &mut HashSet<String>,
    file_versions: &mut u64,
    chunk_hashes: &mut HashSet<String>,
    held_object_keys: &mut HashSet<String>,
) -> Result<(), ServerError> {
    if !seen_record_identities.insert(record_identity_key(record)) {
        return Ok(());
    }

    *file_versions = file_versions.checked_add(1).ok_or(ServerError::Overflow)?;
    for chunk in &record.chunks {
        if record.chunk_size == 0 {
            let xorb_object_key = xorb_object_key(&chunk.hash)?;
            held_object_keys.insert(xorb_object_key.as_str().to_owned());
            visit_stored_xorb_chunk_hashes(object_store, &xorb_object_key, |chunk_hash_hex| {
                let chunk_object_key = chunk_object_key(&chunk_hash_hex)?;
                held_object_keys.insert(chunk_object_key.as_str().to_owned());
                chunk_hashes.insert(chunk_hash_hex);
                Ok(())
            })?;
            continue;
        }

        chunk_hashes.insert(chunk.hash.clone());
        let chunk_object_key = chunk_object_key(&chunk.hash)?;
        held_object_keys.insert(chunk_object_key.as_str().to_owned());
        let xorb_object_key = xorb_object_key(&chunk.hash)?;
        held_object_keys.insert(xorb_object_key.as_str().to_owned());
    }

    Ok(())
}

pub(super) fn parse_record_entry(bytes: &[u8]) -> Result<FileRecord, ServerError> {
    parse_stored_file_record_bytes(bytes)
}

pub(super) fn record_belongs_to_repository(
    record: &FileRecord,
    repository: &RepositoryRef,
) -> bool {
    let Some(scope) = record.repository_scope.as_ref() else {
        return false;
    };

    scope.provider() == repository.provider().repository_provider()
        && scope.owner() == repository.owner()
        && scope.name() == repository.name()
}

pub(super) fn repository_record_scope(repository: &RepositoryRef) -> RepositoryRecordScope {
    RepositoryRecordScope::new(
        repository.provider().repository_provider(),
        repository.owner(),
        repository.name(),
    )
}

pub(super) fn renamed_file_record(
    record: &FileRecord,
    new_repository: &RepositoryRef,
) -> Result<FileRecord, ServerError> {
    let Some(existing_scope) = record.repository_scope.as_ref() else {
        return Ok(record.clone());
    };
    let renamed_scope = RepositoryScope::new(
        new_repository.provider().repository_provider(),
        new_repository.owner(),
        new_repository.name(),
        existing_scope.revision(),
    )
    .map_err(|_error| ServerError::InvalidProviderWebhookPayload)?;
    let mut renamed_record = record.clone();
    renamed_record.repository_scope = Some(renamed_scope);
    Ok(renamed_record)
}

fn record_identity_key(record: &FileRecord) -> String {
    let repository_scope = record.repository_scope.as_ref().map_or_else(
        || "unscoped".to_owned(),
        |scope| {
            format!(
                "{}:{}/{}@{}",
                provider_directory(scope.provider()),
                scope.owner(),
                scope.name(),
                scope.revision().unwrap_or("latest"),
            )
        },
    );
    format!(
        "{repository_scope}:{}:{}",
        record.file_id, record.content_hash
    )
}
