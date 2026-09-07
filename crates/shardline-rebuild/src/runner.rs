use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use shardline_index::{
    AsyncIndexStore, DedupeShardMapping, FileId, RecordMutation, RecordTraversal,
    parse_xet_hash_hex, xet_hash_hex_string,
};
use shardline_server_core::{
    OpsRecordStore, ServerObjectStore, ShardMetadataLimits, checked_increment, read_full_object,
};
use shardline_storage::ObjectPrefix;
use shardline_xet_adapter::{XetAdapterError, retained_shard_chunk_hashes};

use super::{
    IndexRebuildIssueDetail, IndexRebuildIssueKind, IndexRebuildReport, RebuildError,
    VersionCandidate, collect_candidate, push_issue,
};

/// Rebuilds latest-record state from immutable version records.
///
/// # Errors
///
/// Returns [`RebuildError`] when version records cannot be scanned or latest records
/// cannot be written or removed.
pub async fn run_index_rebuild_with_stores<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    shard_metadata_limits: ShardMetadataLimits,
) -> Result<IndexRebuildReport, RebuildError>
where
    RecordAdapter: OpsRecordStore + Sync,
    RecordAdapter::Error: Into<RebuildError>,
    RecordAdapter::Locator: Hash,
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<RebuildError>,
{
    let mut report = IndexRebuildReport {
        scanned_version_records: 0,
        scanned_retained_shards: 0,
        rebuilt_latest_records: 0,
        unchanged_latest_records: 0,
        removed_stale_latest_records: 0,
        scanned_reconstructions: 0,
        unchanged_reconstructions: 0,
        removed_stale_reconstructions: 0,
        rebuilt_dedupe_shard_mappings: 0,
        unchanged_dedupe_shard_mappings: 0,
        removed_stale_dedupe_shard_mappings: 0,
        preserved_latest_records_unreadable_version: Vec::new(),
        issues: Vec::new(),
    };
    let mut candidates = HashMap::new();
    RecordTraversal::visit_version_records(record_store, |entry| {
        report.scanned_version_records = checked_increment(report.scanned_version_records)?;
        collect_candidate(record_store, entry, &mut candidates, &mut report)
    })
    .await?;

    let mut desired_latest_paths = HashSet::new();
    for candidate in candidates.values() {
        let latest_path = RecordTraversal::latest_record_locator(record_store, &candidate.record);
        desired_latest_paths.insert(latest_path.clone());

        let record_bytes = serde_json::to_vec(&candidate.record)?;
        let existing_bytes =
            RecordTraversal::read_latest_record_bytes(record_store, &candidate.record)
                .await
                .map_err(Into::into)?;

        if existing_bytes.as_deref() == Some(record_bytes.as_slice()) {
            report.unchanged_latest_records = checked_increment(report.unchanged_latest_records)?;
            continue;
        }

        RecordMutation::write_latest_record(record_store, &candidate.record)
            .await
            .map_err(Into::into)?;
        report.rebuilt_latest_records = checked_increment(report.rebuilt_latest_records)?;
    }

    // Remove stale latest records — those with no corresponding version record.
    //
    // Gated on run cleanliness (like `prune_stale_reconstructions`): when any
    // version record failed to parse or validate, the candidate set is
    // incomplete, so a latest record excluded from `desired_latest_paths` may
    // belong to a file whose version record is unreadable rather than to a
    // deleted file. Deleting it then would destroy a valid, authoritative
    // latest record (index loss + GC orphans for a fully intact file). A dirty
    // run therefore keeps every existing latest record; the removal is
    // deferred to a clean run. The per-file "kept because version unreadable"
    // notes are surfaced in
    // `report.preserved_latest_records_unreadable_version`.
    if report.is_clean() {
        let mut stale_latest_paths = Vec::new();
        RecordTraversal::visit_latest_record_locators(record_store, |path| {
            if !desired_latest_paths.contains(&path) {
                stale_latest_paths.push(path);
            }

            Ok::<(), RebuildError>(())
        })
        .await?;
        for path in stale_latest_paths {
            RecordMutation::delete_record_locator(record_store, &path)
                .await
                .map_err(Into::into)?;
            report.removed_stale_latest_records =
                checked_increment(report.removed_stale_latest_records)?;
        }

        RecordMutation::prune_empty_latest_records(record_store)
            .await
            .map_err(Into::into)?;
    }

    let desired_reconstructions = desired_reconstruction_file_ids(candidates.values());
    prune_stale_reconstructions(index_store, &desired_reconstructions, &mut report).await?;

    rebuild_dedupe_shard_mappings(
        index_store,
        object_store,
        shard_metadata_limits,
        &mut report,
    )
    .await?;

    Ok(report)
}

pub(super) fn desired_reconstruction_file_ids<'record, Locator, Records>(
    records: Records,
) -> HashSet<String>
where
    Records: IntoIterator<Item = &'record VersionCandidate<Locator>>,
    Locator: 'record,
{
    records
        .into_iter()
        .filter_map(|candidate| {
            parse_xet_hash_hex(&candidate.record.file_id)
                .ok()
                .map(xet_hash_hex_string)
        })
        .collect::<HashSet<_>>()
}

pub(super) async fn prune_stale_reconstructions<IndexAdapter>(
    index_store: &IndexAdapter,
    desired_reconstructions: &HashSet<String>,
    report: &mut IndexRebuildReport,
) -> Result<(), RebuildError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<RebuildError>,
{
    if !report.is_clean() {
        return Ok(());
    }

    let existing_file_ids = index_store
        .list_reconstruction_file_ids()
        .await
        .map_err(Into::into)?;
    for file_id in existing_file_ids {
        report.scanned_reconstructions = checked_increment(report.scanned_reconstructions)?;
        let file_id_hex = xet_hash_hex_string(file_id.hash());
        if desired_reconstructions.contains(&file_id_hex) {
            report.unchanged_reconstructions = checked_increment(report.unchanged_reconstructions)?;
            continue;
        }

        delete_reconstruction(index_store, &file_id).await?;
        report.removed_stale_reconstructions =
            checked_increment(report.removed_stale_reconstructions)?;
    }

    Ok(())
}

async fn delete_reconstruction<IndexAdapter>(
    index_store: &IndexAdapter,
    file_id: &FileId,
) -> Result<(), RebuildError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<RebuildError>,
{
    let _deleted = index_store
        .delete_reconstruction(file_id)
        .await
        .map_err(Into::into)?;
    Ok(())
}

pub(super) async fn rebuild_dedupe_shard_mappings<IndexAdapter>(
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    shard_metadata_limits: ShardMetadataLimits,
    report: &mut IndexRebuildReport,
) -> Result<(), RebuildError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<RebuildError>,
{
    let prefix =
        ObjectPrefix::parse("shards/").map_err(|_error| RebuildError::InvalidContentHash)?;
    let mut desired = HashMap::<String, DedupeShardMapping>::new();
    let issue_count_before_scan = report.issue_count();

    object_store.visit_prefix(&prefix, |metadata| -> Result<(), RebuildError> {
        report.scanned_retained_shards = checked_increment(report.scanned_retained_shards)?;
        let shard_key = metadata.key().clone();
        let shard_location = shard_key.as_str().to_owned();
        let shard_bytes = read_full_object(object_store, &shard_key, metadata.length())
            .map_err(RebuildError::from)?;
        let chunk_hashes = match retained_shard_chunk_hashes(&shard_bytes, shard_metadata_limits) {
            Ok(chunk_hashes) => chunk_hashes,
            Err(XetAdapterError::InvalidSerializedShard(detail)) => {
                push_issue(
                    report,
                    IndexRebuildIssueKind::InvalidRetainedShard,
                    shard_location,
                    IndexRebuildIssueDetail::InvalidRetainedShard(detail),
                )?;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };

        for chunk_hash_hex in chunk_hashes {
            let mapping =
                DedupeShardMapping::new(parse_xet_hash_hex(&chunk_hash_hex)?, shard_key.clone());
            match desired.get(&chunk_hash_hex) {
                Some(existing)
                    if existing.shard_object_key().as_str()
                        <= mapping.shard_object_key().as_str() => {}
                _ => {
                    desired.insert(chunk_hash_hex, mapping);
                }
            }
        }
        Ok(())
    })?;

    if report.issue_count() != issue_count_before_scan {
        return Ok(());
    }

    let mut existing = HashMap::new();
    index_store
        .visit_dedupe_shard_mappings(|mapping| {
            existing.insert(xet_hash_hex_string(mapping.chunk_hash()), mapping);
            Ok::<(), RebuildError>(())
        })
        .await?;

    for (chunk_hash_hex, mapping) in &desired {
        match existing.get(chunk_hash_hex) {
            Some(existing_mapping)
                if existing_mapping.shard_object_key() == mapping.shard_object_key() =>
            {
                report.unchanged_dedupe_shard_mappings =
                    checked_increment(report.unchanged_dedupe_shard_mappings)?;
            }
            _ => {
                index_store
                    .upsert_dedupe_shard_mapping(mapping)
                    .await
                    .map_err(Into::into)?;
                report.rebuilt_dedupe_shard_mappings =
                    checked_increment(report.rebuilt_dedupe_shard_mappings)?;
            }
        }
    }

    // SECURITY NOTE: This deletion runs without a GC/write barrier. A shard write
    // in progress (temp file not yet hardlinked) will appear in `desired` from the
    // scan above, so its mapping survives. However, a shard appearing BETWEEN scan
    // and delete could lose its mapping temporarily. The next rebuild pass self-
    // heals. Data loss is prevented because individual chunk objects within the
    // shard are still protected by record references.
    for (chunk_hash_hex, _mapping) in existing {
        if desired.contains_key(&chunk_hash_hex) {
            continue;
        }

        let chunk_hash = parse_xet_hash_hex(&chunk_hash_hex)?;
        let _deleted = index_store
            .delete_dedupe_shard_mapping(&chunk_hash)
            .await
            .map_err(Into::into)?;
        report.removed_stale_dedupe_shard_mappings =
            checked_increment(report.removed_stale_dedupe_shard_mappings)?;
    }

    Ok(())
}
