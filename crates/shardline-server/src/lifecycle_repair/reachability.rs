use shardline_index::{
    AsyncIndexStore, FileRecordStorageLayout, RecordStore, RecordTraversal, StorageRepresentation,
    parse_xet_hash_hex, xet_hash_hex_string,
};
use shardline_storage::ObjectStore;

use crate::{
    ServerError, ServerFrontend,
    chunk_store::chunk_object_key,
    local_backend::chunk_hash,
    object_store::ServerObjectStore,
    overflow::checked_increment,
    record_store::parse_stored_file_record_bytes,
    server_frontend::{
        optional_chunk_container_keys, referenced_term_object_key,
        visit_protocol_object_member_chunks,
    },
    xet_adapter::{
        XorbVisitError, try_for_each_serialized_xorb_chunk_trusted, validate_serialized_xorb,
        xorb_hash_from_object_key_if_present,
    },
};

use super::types::RepairReachability;

// Cache identities only, never container payloads. Traversals beyond this
// working set safely reread sources instead of accumulating another unbounded
// set alongside the reachability graph.
const MAX_RESOLVED_XORBS: usize = 4096;

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
    // This observation belongs only to this traversal. A later repair must
    // verify the stored source again, even if its metadata has not changed.
    let mut resolved_xorbs = std::collections::HashSet::new();
    RecordTraversal::visit_latest_records(record_store, |entry| {
        collect_record_object_references_cached(
            object_store,
            frontends,
            &entry.bytes,
            reachability,
            &mut resolved_xorbs,
        )
    })
    .await?;

    RecordTraversal::visit_version_records(record_store, |entry| {
        collect_record_object_references_cached(
            object_store,
            frontends,
            &entry.bytes,
            reachability,
            &mut resolved_xorbs,
        )
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

#[cfg(test)]
pub(crate) fn collect_record_object_references(
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    record_bytes: &[u8],
    reachability: &mut RepairReachability,
) -> Result<(), ServerError> {
    collect_record_object_references_cached(
        object_store,
        frontends,
        record_bytes,
        reachability,
        &mut std::collections::HashSet::new(),
    )
}

fn collect_record_object_references_cached(
    object_store: &ServerObjectStore,
    frontends: &[ServerFrontend],
    record_bytes: &[u8],
    reachability: &mut RepairReachability,
    resolved_xorbs: &mut std::collections::HashSet<String>,
) -> Result<(), ServerError> {
    let record = parse_stored_file_record_bytes(record_bytes)?;
    let storage_layout = record.storage_layout();
    reachability.scanned_records = checked_increment(reachability.scanned_records)?;
    // Preserve the legacy per-record probe bound for raw-chunk records whose
    // optional container is absent. Only verified sources cross record bounds.
    let mut attempted_xorbs = std::collections::HashSet::new();
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

                if record.storage_repr == StorageRepresentation::XorbCdcV1
                    && resolved_xorbs.contains(&chunk.hash)
                {
                    continue;
                }

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
                    && attempted_xorbs.insert(chunk.hash.clone())
                    && collect_xorb_member_chunk_references(
                        object_store,
                        frontends,
                        &chunk.hash,
                        reachability,
                    )?
                {
                    // Never cache a missing or failed source observation.
                    if resolved_xorbs.len() >= MAX_RESOLVED_XORBS {
                        resolved_xorbs.clear();
                    }
                    resolved_xorbs.insert(chunk.hash.clone());
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
) -> Result<bool, ServerError> {
    let Some(xorb_key) = optional_chunk_container_keys(frontends, chunk_hash_hex)?
        .into_iter()
        .next()
    else {
        return Ok(false);
    };
    let Some((mut reader, _length)) =
        object_store.materialize_bounded_object_if_present(&xorb_key, 0)?
    else {
        return Ok(false);
    };
    let xorb_hash_hex =
        xorb_hash_from_object_key_if_present(&xorb_key)?.ok_or(ServerError::InvalidContentHash)?;
    let expected_hash = parse_xet_hash_hex(xorb_hash_hex)?;
    let validated = validate_serialized_xorb(&mut reader, expected_hash)?;
    match try_for_each_serialized_xorb_chunk_trusted(&mut reader, &validated, |decoded| {
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
        Ok(()) => Ok(true),
        Err(XorbVisitError::Parse(error)) => Err(ServerError::from(error)),
        Err(XorbVisitError::Visitor(error)) => Err(error),
    }
}
