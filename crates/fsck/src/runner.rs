use std::path::{Path, PathBuf};

use shardline_index::{AsyncIndexStore, FileRecord, FileRecordInvariantError, xet_hash_hex_string};
use shardline_server_core::{
    OpsRecordStore, ServerObjectStore, ShardMetadataLimits, checked_increment, read_full_object,
};
use shardline_storage::{ObjectKey, ObjectStore};
use shardline_xet_adapter::{XetAdapterError, retained_shard_chunk_hashes};

use crate::lifecycle_checks::inspect_lifecycle_metadata;
use crate::record_checks::scan_record_tree;
use crate::{
    FsckError, FsckIssue, FsckIssueDetail, FsckIssueKind, FsckReachability,
    FsckReconstructionPlanDetail, FsckReport, RecordKind,
};

/// Runs local filesystem integrity checks over Shardline metadata and chunk storage.
///
/// # Errors
///
/// Returns [`FsckError`] when the storage root cannot be traversed or chunk/record
/// bytes cannot be read due to an operational failure.
pub async fn run_local_fsck(root: PathBuf) -> Result<crate::LocalFsckReport, FsckError> {
    let object_root = root.join("chunks");
    let object_store = ServerObjectStore::local(object_root.clone())?;
    let index_store = shardline_index::LocalIndexStore::open(root.clone());
    let record_store = shardline_index::LocalRecordStore::open(root);
    run_fsck_with_stores(
        &record_store,
        &index_store,
        &object_root,
        &object_store,
        shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
    )
    .await
}

/// Runs local filesystem integrity checks over Shardline metadata and chunk storage
/// using explicit store parameters.
///
/// # Errors
///
/// Returns [`FsckError`] when the storage root cannot be traversed or chunk/record
/// bytes cannot be read due to an operational failure.
pub async fn run_fsck_with_stores<RecordAdapter, IndexAdapter>(
    record_store: &RecordAdapter,
    index_store: &IndexAdapter,
    object_root: &Path,
    object_store: &ServerObjectStore,
    shard_metadata_limits: ShardMetadataLimits,
) -> Result<FsckReport, FsckError>
where
    RecordAdapter: OpsRecordStore + Sync,
    RecordAdapter::Error: Into<FsckError>,
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<FsckError>,
{
    let start = std::time::Instant::now();
    let mut report = FsckReport {
        latest_records: 0,
        version_records: 0,
        inspected_chunk_references: 0,
        inspected_dedupe_shard_mappings: 0,
        inspected_reconstructions: 0,
        inspected_webhook_deliveries: 0,
        inspected_provider_repository_states: 0,
        issues: Vec::new(),
    };
    let mut reachability = FsckReachability::default();

    scan_record_tree(
        record_store,
        RecordKind::Latest,
        object_root,
        object_store,
        &mut reachability,
        &mut report,
    )
    .await?;
    scan_record_tree(
        record_store,
        RecordKind::Version,
        object_root,
        object_store,
        &mut reachability,
        &mut report,
    )
    .await?;
    inspect_dedupe_shard_mappings(
        index_store,
        object_root,
        object_store,
        shard_metadata_limits,
        &mut reachability,
        &mut report,
    )
    .await?;
    inspect_reconstruction_index(index_store, &mut report).await?;
    inspect_lifecycle_metadata(
        index_store,
        object_root,
        object_store,
        &reachability,
        &mut report,
    )
    .await?;

    let elapsed = start.elapsed();
    shardline_metrics::record_fsck_run(elapsed, report.issue_count() as u64);

    Ok(report)
}

pub(crate) fn push_issue(
    report: &mut FsckReport,
    kind: FsckIssueKind,
    location: String,
    detail: FsckIssueDetail,
) -> Result<(), FsckError> {
    let _count = u64::try_from(report.issues.len())?;
    report.issues.push(FsckIssue {
        kind,
        location,
        detail,
    });
    Ok(())
}

pub(crate) fn push_reconstruction_plan_issue(
    report: &mut FsckReport,
    location: String,
    error: &FileRecordInvariantError,
) -> Result<(), FsckError> {
    let kind = match error {
        FileRecordInvariantError::ChunkHash(_) => FsckIssueKind::InvalidContentHash,
        FileRecordInvariantError::EmptyChunk => FsckIssueKind::EmptyChunk,
        FileRecordInvariantError::NonContiguousChunkOffsets => FsckIssueKind::NonContiguousChunks,
        FileRecordInvariantError::InvalidChunkRange => FsckIssueKind::InvalidChunkRange,
        FileRecordInvariantError::InvalidPackedRange => FsckIssueKind::InvalidPackedRange,
        FileRecordInvariantError::LengthOverflow | FileRecordInvariantError::TotalBytesMismatch => {
            FsckIssueKind::TotalBytesMismatch
        }
    };
    push_issue(
        report,
        kind,
        location,
        FsckIssueDetail::InvalidReconstructionPlan(reconstruction_plan_error_detail(error)),
    )
}

pub(crate) const fn reconstruction_plan_error_detail(
    error: &FileRecordInvariantError,
) -> FsckReconstructionPlanDetail {
    match error {
        FileRecordInvariantError::ChunkHash(_) => FsckReconstructionPlanDetail::ChunkHashInvalid,
        FileRecordInvariantError::EmptyChunk => FsckReconstructionPlanDetail::EmptyChunk,
        FileRecordInvariantError::NonContiguousChunkOffsets => {
            FsckReconstructionPlanDetail::NonContiguousChunkOffsets
        }
        FileRecordInvariantError::InvalidChunkRange => {
            FsckReconstructionPlanDetail::InvalidChunkRange
        }
        FileRecordInvariantError::InvalidPackedRange => {
            FsckReconstructionPlanDetail::InvalidPackedRange
        }
        FileRecordInvariantError::LengthOverflow => FsckReconstructionPlanDetail::LengthOverflow,
        FileRecordInvariantError::TotalBytesMismatch => {
            FsckReconstructionPlanDetail::TotalBytesMismatch
        }
    }
}

pub(crate) fn record_path<RecordAdapter>(
    record_store: &RecordAdapter,
    record_kind: RecordKind,
    record: &FileRecord,
) -> RecordAdapter::Locator
where
    RecordAdapter: OpsRecordStore,
{
    match record_kind {
        RecordKind::Latest => record_store.latest_record_locator(record),
        RecordKind::Version => record_store.version_record_locator(record),
    }
}

pub(crate) fn object_key_storage_path(object_root: &Path, object_key: &ObjectKey) -> PathBuf {
    object_root.join(object_key.as_str())
}

pub(crate) fn object_location_display(
    object_root: &Path,
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
) -> String {
    object_store
        .local_path_for_key(object_key)
        .unwrap_or_else(|| object_key_storage_path(object_root, object_key))
        .display()
        .to_string()
}

pub(crate) fn unix_now_seconds_checked() -> Result<u64, FsckError> {
    shardline_server_core::unix_now_seconds_checked().map_err(|_e| FsckError::Overflow)
}

async fn inspect_dedupe_shard_mappings<IndexAdapter>(
    index_store: &IndexAdapter,
    object_root: &Path,
    object_store: &ServerObjectStore,
    shard_metadata_limits: ShardMetadataLimits,
    reachability: &mut FsckReachability,
    report: &mut FsckReport,
) -> Result<(), FsckError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<FsckError>,
{
    index_store
        .visit_dedupe_shard_mappings(|mapping| {
            report.inspected_dedupe_shard_mappings =
                checked_increment(report.inspected_dedupe_shard_mappings)?;
            let chunk_hash_hex = xet_hash_hex_string(mapping.chunk_hash());
            let shard_location =
                object_location_display(object_root, object_store, mapping.shard_object_key());
            let metadata = match object_store.metadata(mapping.shard_object_key())? {
                Some(metadata) => metadata,
                None => {
                    push_issue(
                        report,
                        FsckIssueKind::MissingDedupeShardObject,
                        shard_location,
                        FsckIssueDetail::MappedChunkHash {
                            chunk_hash: chunk_hash_hex,
                        },
                    )?;
                    return Ok::<(), FsckError>(());
                }
            };
            let shard_bytes =
                read_full_object(object_store, mapping.shard_object_key(), metadata.length())?;
            let chunk_hashes =
                match retained_shard_chunk_hashes(&shard_bytes, shard_metadata_limits) {
                    Ok(chunk_hashes) => chunk_hashes,
                    Err(XetAdapterError::InvalidSerializedShard(detail)) => {
                        push_issue(
                            report,
                            FsckIssueKind::InvalidRetainedShard,
                            shard_location,
                            FsckIssueDetail::InvalidRetainedShard(detail),
                        )?;
                        return Ok::<(), FsckError>(());
                    }
                    Err(error) => return Err(error.into()),
                };
            if !chunk_hashes
                .iter()
                .any(|candidate| candidate == &chunk_hash_hex)
            {
                push_issue(
                    report,
                    FsckIssueKind::InvalidDedupeShardMapping,
                    shard_location,
                    FsckIssueDetail::MappedChunkHashAbsentFromRetainedShard {
                        chunk_hash: chunk_hash_hex.clone(),
                    },
                )?;
            }
            if reachability
                .live_dedupe_chunk_hashes
                .contains(&chunk_hash_hex)
            {
                reachability
                    .referenced_object_keys
                    .insert(mapping.shard_object_key().as_str().to_owned());
            }
            Ok::<(), FsckError>(())
        })
        .await?;

    Ok(())
}

async fn inspect_reconstruction_index<IndexAdapter>(
    index_store: &IndexAdapter,
    report: &mut FsckReport,
) -> Result<(), FsckError>
where
    IndexAdapter: AsyncIndexStore + Sync,
    IndexAdapter::Error: Into<FsckError>,
{
    let file_ids = index_store
        .list_reconstruction_file_ids()
        .await
        .map_err(Into::into)?;
    for file_id in file_ids {
        report.inspected_reconstructions = checked_increment(report.inspected_reconstructions)?;
        let file_id_hex = xet_hash_hex_string(file_id.hash());
        let Some(reconstruction) = index_store
            .reconstruction(&file_id)
            .await
            .map_err(Into::into)?
        else {
            push_issue(
                report,
                FsckIssueKind::EmptyReconstruction,
                file_id_hex,
                FsckIssueDetail::ReconstructionListedUnreadableRow,
            )?;
            continue;
        };

        if reconstruction.terms().is_empty() {
            push_issue(
                report,
                FsckIssueKind::EmptyReconstruction,
                file_id_hex.clone(),
                FsckIssueDetail::ReconstructionContainedNoTerms,
            )?;
        }

        for term in reconstruction.terms() {
            let object_id = term.object_id();
            if !index_store
                .contains_object(&object_id)
                .await
                .map_err(Into::into)?
            {
                push_issue(
                    report,
                    FsckIssueKind::MissingReconstructionXorb,
                    file_id_hex.clone(),
                    FsckIssueDetail::MissingReconstructionXorb {
                        xorb_hash: xet_hash_hex_string(object_id.hash()),
                    },
                )?;
            }
        }
    }

    Ok(())
}
