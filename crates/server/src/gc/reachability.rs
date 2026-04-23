use std::collections::{HashMap, HashSet};

use shardline_index::{AsyncIndexStore, FileRecordStorageLayout, RecordStore, StoredRecord};
use shardline_storage::{ObjectKey, ObjectPrefix, ObjectStore};

use crate::{
    ServerError,
    chunk_store::{chunk_hash_from_chunk_object_key_if_present, chunk_object_key},
    object_store::ServerObjectStore,
    overflow::checked_increment,
    record_store::parse_stored_file_record_bytes,
    server_frontend::{
        ServerFrontend, managed_protocol_object_identity, optional_chunk_container_keys,
        referenced_term_object_key, visit_protocol_object_member_chunks,
    },
};

#[derive(Debug, Clone)]
pub(super) struct OrphanObject {
    pub(super) hash: String,
    pub(super) object_key: ObjectKey,
    pub(super) bytes: u64,
}

#[derive(Debug, Default)]
pub(super) struct ReachabilityAccumulator {
    pub(super) referenced_object_keys: HashSet<String>,
    live_dedupe_chunk_hashes: HashSet<String>,
    missing_optional_object_keys: HashSet<String>,
    inspected_protocol_objects: HashSet<String>,
    pub(super) scanned_records: u64,
}

pub(super) async fn collect_referenced_object_keys<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    reachability: &mut ReachabilityAccumulator,
) -> Result<(), ServerError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ServerError>,
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<ServerError>,
{
    RecordStore::visit_latest_records(record_store, |entry| {
        collect_record_object_references(object_store, frontends, &entry, reachability)
    })
    .await?;

    RecordStore::visit_version_records(record_store, |entry| {
        collect_record_object_references(object_store, frontends, &entry, reachability)
    })
    .await?;

    index_store
        .visit_dedupe_shard_mappings(|mapping| {
            let chunk_hash_hex = mapping.chunk_hash().api_hex_string();
            if reachability
                .live_dedupe_chunk_hashes
                .contains(&chunk_hash_hex)
            {
                reachability
                    .referenced_object_keys
                    .insert(mapping.shard_object_key().as_str().to_owned());
            }
            Ok::<(), ServerError>(())
        })
        .await?;

    Ok(())
}

fn collect_record_object_references<Locator>(
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    entry: &StoredRecord<Locator>,
    reachability: &mut ReachabilityAccumulator,
) -> Result<(), ServerError> {
    let record = parse_stored_file_record_bytes(&entry.bytes)?;
    let storage_layout = record.storage_layout();
    reachability.scanned_records = checked_increment(reachability.scanned_records)?;
    for chunk in &record.chunks {
        match storage_layout {
            FileRecordStorageLayout::ReferencedObjectTerms => {
                let object_key = referenced_term_object_key(frontends, &chunk.hash)?;
                reachability
                    .referenced_object_keys
                    .insert(object_key.as_str().to_owned());
                collect_live_chunk_references_from_protocol_object(
                    object_store,
                    frontends,
                    &object_key,
                    reachability,
                )?;
            }
            FileRecordStorageLayout::StoredChunks => {
                let chunk_object_key = chunk_object_key(&chunk.hash)?;
                reachability
                    .referenced_object_keys
                    .insert(chunk_object_key.as_str().to_owned());
                reachability
                    .live_dedupe_chunk_hashes
                    .insert(chunk.hash.clone());

                for object_key in optional_chunk_container_keys(frontends, &chunk.hash)? {
                    mark_optional_object_reference(
                        object_store,
                        &object_key,
                        &mut reachability.referenced_object_keys,
                        &mut reachability.missing_optional_object_keys,
                    )?;
                }
            }
        }
    }

    Ok(())
}

fn mark_optional_object_reference<ObjectAdapter>(
    object_store: &ObjectAdapter,
    object_key: &ObjectKey,
    referenced_object_keys: &mut HashSet<String>,
    missing_optional_object_keys: &mut HashSet<String>,
) -> Result<(), ServerError>
where
    ObjectAdapter: ObjectStore,
    ObjectAdapter::Error: Into<ServerError>,
{
    let object_key_string = object_key.as_str().to_owned();
    if referenced_object_keys.contains(&object_key_string)
        || missing_optional_object_keys.contains(&object_key_string)
    {
        return Ok(());
    }

    if object_store
        .metadata(object_key)
        .map_err(Into::into)?
        .is_some()
    {
        referenced_object_keys.insert(object_key_string);
    } else {
        missing_optional_object_keys.insert(object_key_string);
    }

    Ok(())
}

fn collect_live_chunk_references_from_protocol_object(
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    object_key: &ObjectKey,
    reachability: &mut ReachabilityAccumulator,
) -> Result<(), ServerError> {
    let object_key_string = object_key.as_str().to_owned();
    if reachability
        .inspected_protocol_objects
        .contains(&object_key_string)
    {
        return Ok(());
    }
    reachability
        .inspected_protocol_objects
        .insert(object_key_string);

    visit_protocol_object_member_chunks(frontends, object_store, object_key, |chunk_hash_hex| {
        let chunk_object_key = chunk_object_key(&chunk_hash_hex)?;
        reachability
            .referenced_object_keys
            .insert(chunk_object_key.as_str().to_owned());
        reachability.live_dedupe_chunk_hashes.insert(chunk_hash_hex);
        Ok::<(), ServerError>(())
    })
}

pub(super) fn scan_orphan_objects(
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    referenced_object_keys: &HashSet<String>,
) -> Result<HashMap<String, OrphanObject>, ServerError> {
    let mut orphans = HashMap::new();
    let prefix = ObjectPrefix::parse("").map_err(|_error| ServerError::InvalidContentHash)?;
    object_store.visit_prefix(&prefix, |metadata| {
        let Some(hash) = managed_object_hash(metadata.key(), frontends)? else {
            return Ok(());
        };
        if referenced_object_keys.contains(metadata.key().as_str()) {
            return Ok(());
        }

        orphans.insert(
            metadata.key().as_str().to_owned(),
            OrphanObject {
                hash,
                object_key: metadata.key().clone(),
                bytes: metadata.length(),
            },
        );
        Ok(())
    })?;

    Ok(orphans)
}

fn managed_object_hash(
    key: &ObjectKey,
    frontends: &[ServerFrontend],
) -> Result<Option<String>, ServerError> {
    if let Some(hash) = chunk_hash_from_chunk_object_key_if_present(key)? {
        return Ok(Some(hash.to_owned()));
    }

    managed_protocol_object_identity(frontends, key)
}

pub(super) fn managed_object_hash_or_object_key(
    key: &ObjectKey,
    frontends: &[ServerFrontend],
) -> String {
    match managed_object_hash(key, frontends) {
        Ok(Some(hash)) => hash,
        Ok(None) | Err(_) => key.as_str().to_owned(),
    }
}
