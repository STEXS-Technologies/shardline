use shardline_index::{
    AsyncIndexStore, FileRecordStorageLayout, RecordStore, RecordTraversal, xet_hash_hex_string,
};
use shardline_storage::ObjectStore;

use crate::{
    ServerError, ServerFrontend,
    chunk_store::chunk_object_key,
    object_store::ServerObjectStore,
    overflow::checked_increment,
    record_store::parse_stored_file_record_bytes,
    server_frontend::{
        optional_chunk_container_keys, referenced_term_object_key,
        visit_protocol_object_member_chunks,
    },
};

use super::types::RepairReachability;

pub(crate) async fn collect_referenced_object_keys<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    reachability: &mut RepairReachability,
) -> Result<(), ServerError>
where
    RecordAdapter: RecordStore + Sync,
    RecordAdapter::Error: Into<ServerError>,
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<ServerError>,
{
    RecordTraversal::visit_latest_records(record_store, |entry| {
        collect_record_object_references(object_store, frontends, &entry.bytes, reachability)
    })
    .await?;

    RecordTraversal::visit_version_records(record_store, |entry| {
        collect_record_object_references(object_store, frontends, &entry.bytes, reachability)
    })
    .await?;

    index_store
        .visit_dedupe_shard_mappings(|mapping| {
            let chunk_hash_hex = xet_hash_hex_string(mapping.chunk_hash());
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

pub(crate) fn collect_record_object_references(
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    record_bytes: &[u8],
    reachability: &mut RepairReachability,
) -> Result<(), ServerError> {
    let record = parse_stored_file_record_bytes(record_bytes)?;
    let storage_layout = record.storage_layout();
    reachability.scanned_records = checked_increment(reachability.scanned_records)?;
    for chunk in &record.chunks {
        match storage_layout {
            FileRecordStorageLayout::ReferencedObjectTerms => {
                let protocol_object_key = referenced_term_object_key(frontends, &chunk.hash)?;
                reachability
                    .referenced_object_keys
                    .insert(protocol_object_key.as_str().to_owned());
                visit_protocol_object_member_chunks(
                    frontends,
                    object_store,
                    &protocol_object_key,
                    |chunk_hash_hex| {
                        let chunk_key = chunk_object_key(&chunk_hash_hex)?;
                        reachability
                            .referenced_object_keys
                            .insert(chunk_key.as_str().to_owned());
                        Ok(())
                    },
                )?;
            }
            FileRecordStorageLayout::StoredChunks => {
                let chunk_key = chunk_object_key(&chunk.hash)?;
                reachability
                    .referenced_object_keys
                    .insert(chunk_key.as_str().to_owned());
                reachability
                    .live_dedupe_chunk_hashes
                    .insert(chunk.hash.clone());

                for protocol_object_key in optional_chunk_container_keys(frontends, &chunk.hash)? {
                    if object_store.metadata(&protocol_object_key)?.is_some() {
                        reachability
                            .referenced_object_keys
                            .insert(protocol_object_key.as_str().to_owned());
                    }
                }
            }
        }
    }

    Ok(())
}
