use std::io::ErrorKind;

use shardline_index::{
    FileRecord, LocalIndexStoreError, LocalRecordStore, RecordStore, RepositoryRecordScope,
};
use shardline_protocol::RepositoryScope;

use crate::{
    ServerError,
    record_store::parse_stored_file_record_bytes,
    validation::{validate_content_hash, validate_identifier},
};

pub(super) async fn read_record(
    record_store: &LocalRecordStore,
    file_id: &str,
    content_hash: Option<&str>,
    repository_scope: Option<&RepositoryScope>,
) -> Result<FileRecord, ServerError> {
    validate_identifier(file_id)?;
    let bytes = if let Some(hash) = content_hash {
        validate_content_hash(hash)?;
        let record = FileRecord {
            file_id: file_id.to_owned(),
            content_hash: hash.to_owned(),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: repository_scope.cloned(),
            chunks: Vec::new(),
        };
        let locator = RecordStore::version_record_locator(record_store, &record);
        read_record_bytes(record_store, &locator).await?
    } else {
        let record = FileRecord {
            file_id: file_id.to_owned(),
            content_hash: String::new(),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: repository_scope.cloned(),
            chunks: Vec::new(),
        };
        let locator = RecordStore::latest_record_locator(record_store, &record);
        read_record_bytes(record_store, &locator).await?
    };
    parse_stored_file_record_bytes(&bytes)
}

async fn read_record_bytes(
    record_store: &LocalRecordStore,
    locator: &<LocalRecordStore as RecordStore>::Locator,
) -> Result<Vec<u8>, ServerError> {
    match RecordStore::read_record_bytes(record_store, locator).await {
        Ok(bytes) => Ok(bytes),
        Err(LocalIndexStoreError::Io(error)) if error.kind() == ErrorKind::NotFound => {
            Err(ServerError::NotFound)
        }
        Err(error) => Err(ServerError::IndexStore(error)),
    }
}

pub(super) async fn repository_references_xorb(
    record_store: &LocalRecordStore,
    hash_hex: &str,
    repository_scope: &RepositoryScope,
) -> Result<bool, ServerError> {
    let repository = RepositoryRecordScope::from_repository_scope(repository_scope);
    let mut found = false;
    record_store
        .visit_repository_latest_records(&repository, |entry| {
            inspect_repository_record_for_xorb(&mut found, &entry.bytes, hash_hex, repository_scope)
        })
        .await?;
    if found {
        return Ok(true);
    }
    record_store
        .visit_repository_version_records(&repository, |entry| {
            inspect_repository_record_for_xorb(&mut found, &entry.bytes, hash_hex, repository_scope)
        })
        .await?;

    Ok(found)
}

fn inspect_repository_record_for_xorb(
    found: &mut bool,
    bytes: &[u8],
    hash_hex: &str,
    repository_scope: &RepositoryScope,
) -> Result<(), ServerError> {
    if *found {
        return Ok(());
    }
    let record = parse_stored_file_record_bytes(bytes)?;
    if record.repository_scope.as_ref() == Some(repository_scope)
        && record.chunks.iter().any(|chunk| chunk.hash == hash_hex)
    {
        *found = true;
    }

    Ok(())
}
