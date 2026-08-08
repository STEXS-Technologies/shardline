use std::io::Cursor;

use shardline_index::{
    AsyncIndexStore, FileRecordStorageLayout, RecordStore, RecordTraversal, StorageRepresentation,
    parse_xet_hash_hex, xet_hash_hex_string,
};
use shardline_storage::ObjectStore;

use crate::{
    ServerError, ServerFrontend,
    chunk_store::chunk_object_key,
    local_backend::chunk_hash,
    object_store::{ServerObjectStore, read_full_object},
    overflow::checked_increment,
    record_store::parse_stored_file_record_bytes,
    server_frontend::{
        optional_chunk_container_keys, referenced_term_object_key,
        visit_protocol_object_member_chunks,
    },
    xet_adapter::{
        XorbVisitError, try_for_each_serialized_xorb_chunk, validate_serialized_xorb,
        xorb_hash_from_object_key_if_present,
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
    // Resolve each distinct xorb container once; all chunks of a repointed
    // record share the same xorb hash.
    let mut resolved_xorbs = std::collections::HashSet::new();
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

                // XorbCdcV1 records may reference a xorb container (the ingestor
                // repoints the record hash to the xorb hash). Resolve the member
                // chunks so the individually-stored chunk objects (written for
                // dedup) stay protected. The shardline ingestor keys those raw
                // chunk objects by `chunk_hash` (blake3) of the member data, so we
                // re-derive them from the xorb payload rather than the xorb's own
                // (merkle) descriptor hashes. The metadata guard is deliberate:
                // legacy pre-packing records carry the raw chunk hash with no xorb
                // under that key, and walking a missing container must not error
                // the repair run.
                if record.storage_repr == StorageRepresentation::XorbCdcV1
                    && resolved_xorbs.insert(chunk.hash.clone())
                {
                    collect_xorb_member_chunk_references(
                        object_store,
                        frontends,
                        &chunk.hash,
                        reachability,
                    )?;
                }
            }
        }
    }

    Ok(())
}

/// Resolves the raw member chunk objects of a xorb container so the repair
/// reachability protects the individually-stored dedup chunks.
///
/// `chunk_hash_hex` is the repointed record hash (the xorb hash). Member chunk
/// object keys are derived by hashing each decoded member payload with
/// [`chunk_hash`] (blake3), matching how the ingestor stores raw chunks.
fn collect_xorb_member_chunk_references(
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    chunk_hash_hex: &str,
    reachability: &mut RepairReachability,
) -> Result<(), ServerError> {
    let Some(xorb_key) = optional_chunk_container_keys(frontends, chunk_hash_hex)?
        .into_iter()
        .next()
    else {
        return Ok(());
    };
    let Some(metadata) = object_store.metadata(&xorb_key)? else {
        return Ok(());
    };
    let xorb_hash_hex =
        xorb_hash_from_object_key_if_present(&xorb_key)?.ok_or(ServerError::InvalidContentHash)?;
    let xorb_bytes = read_full_object(object_store, &xorb_key, metadata.length())?;
    let expected_hash = parse_xet_hash_hex(xorb_hash_hex)?;
    let mut reader = Cursor::new(xorb_bytes);
    let validated = validate_serialized_xorb(&mut reader, expected_hash)?;
    match try_for_each_serialized_xorb_chunk(&mut reader, &validated, |decoded| {
        let member_hash_hex = xet_hash_hex_string(chunk_hash(decoded.data()));
        let member_key = chunk_object_key(&member_hash_hex)?;
        reachability
            .referenced_object_keys
            .insert(member_key.as_str().to_owned());
        reachability
            .live_dedupe_chunk_hashes
            .insert(member_hash_hex);
        Ok::<(), ServerError>(())
    }) {
        Ok(()) => Ok(()),
        Err(XorbVisitError::Parse(error)) => Err(ServerError::from(error)),
        Err(XorbVisitError::Visitor(error)) => Err(error),
    }
}
