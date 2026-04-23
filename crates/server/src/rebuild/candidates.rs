use std::{collections::HashMap, time::Duration};

use shardline_index::{FileRecord, FileRecordInvariantError, RecordStore, StoredRecord};
use shardline_protocol::{RepositoryScope, TokenClaimsError};

use super::{
    IndexRebuildIssueDetail, IndexRebuildIssueKind, IndexRebuildReconstructionPlanDetail,
    IndexRebuildReport, push_issue,
};
use crate::{
    ServerError,
    ops_record_store::OpsRecordStore,
    record_store::parse_stored_file_record_bytes,
    repository_scope_path::provider_directory,
    validation::{validate_content_hash, validate_identifier},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct RebuildKey {
    provider: Option<&'static str>,
    owner: Option<String>,
    name: Option<String>,
    revision: Option<String>,
    file_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct VersionCandidate<Locator> {
    pub(super) record: FileRecord,
    locator: Locator,
    modified_since_epoch: Duration,
}

pub(super) fn collect_candidate<RecordAdapter>(
    record_store: &RecordAdapter,
    entry: StoredRecord<RecordAdapter::Locator>,
    candidates: &mut HashMap<RebuildKey, VersionCandidate<RecordAdapter::Locator>>,
    report: &mut IndexRebuildReport,
) -> Result<(), ServerError>
where
    RecordAdapter: OpsRecordStore,
    RecordAdapter::Error: Into<ServerError>,
{
    let StoredRecord {
        locator: path,
        bytes,
        modified_since_epoch,
    } = entry;
    let location = record_store.locator_display(&path);
    let record = match parse_stored_file_record_bytes(&bytes) {
        Ok(record) => record,
        Err(ServerError::StoredFileMetadataTooLarge { .. }) => {
            push_issue(
                report,
                IndexRebuildIssueKind::OversizedVersionRecordMetadata,
                location,
                IndexRebuildIssueDetail::OversizedVersionRecordMetadata,
            )?;
            return Ok(());
        }
        Err(ServerError::Json(_)) => {
            push_issue(
                report,
                IndexRebuildIssueKind::InvalidVersionRecordJson,
                location,
                IndexRebuildIssueDetail::RecordJsonInvalid,
            )?;
            return Ok(());
        }
        Err(error) => return Err(error),
    };

    if validate_identifier(&record.file_id).is_err() {
        push_issue(
            report,
            IndexRebuildIssueKind::InvalidVersionFileId,
            record_store.locator_display(&path),
            IndexRebuildIssueDetail::InvalidFileId {
                file_id: record.file_id,
            },
        )?;
        return Ok(());
    }

    if validate_content_hash(&record.content_hash).is_err() {
        push_issue(
            report,
            IndexRebuildIssueKind::InvalidVersionContentHash,
            record_store.locator_display(&path),
            IndexRebuildIssueDetail::InvalidContentHash {
                content_hash: record.content_hash,
            },
        )?;
        return Ok(());
    }

    if validate_repository_scope(record.repository_scope.as_ref()).is_err() {
        push_issue(
            report,
            IndexRebuildIssueKind::InvalidVersionRepositoryScope,
            record_store.locator_display(&path),
            IndexRebuildIssueDetail::InvalidRepositoryScope,
        )?;
        return Ok(());
    }

    let expected_path = RecordStore::version_record_locator(record_store, &record);
    if expected_path != path {
        push_issue(
            report,
            IndexRebuildIssueKind::VersionPathMismatch,
            record_store.locator_display(&path),
            IndexRebuildIssueDetail::VersionPathMismatch {
                expected_locator: record_store.locator_display(&expected_path),
            },
        )?;
        return Ok(());
    }

    if let Err(error) = record.validate_reconstruction_plan() {
        push_issue(
            report,
            IndexRebuildIssueKind::InvalidVersionReconstructionPlan,
            record_store.locator_display(&path),
            reconstruction_plan_error_detail(&error),
        )?;
        return Ok(());
    }

    let candidate = VersionCandidate {
        record: record.clone(),
        locator: path,
        modified_since_epoch,
    };
    let key = rebuild_key(&record);
    match candidates.get_mut(&key) {
        Some(existing) if candidate_is_newer(&candidate, existing) => {
            *existing = candidate;
        }
        None => {
            candidates.insert(key, candidate);
        }
        Some(_) => {}
    }

    Ok(())
}

fn rebuild_key(record: &FileRecord) -> RebuildKey {
    record.repository_scope.as_ref().map_or_else(
        || RebuildKey {
            provider: None,
            owner: None,
            name: None,
            revision: None,
            file_id: record.file_id.clone(),
        },
        |repository_scope| RebuildKey {
            provider: Some(provider_directory(repository_scope.provider())),
            owner: Some(repository_scope.owner().to_owned()),
            name: Some(repository_scope.name().to_owned()),
            revision: repository_scope.revision().map(ToOwned::to_owned),
            file_id: record.file_id.clone(),
        },
    )
}

fn candidate_is_newer<Locator>(
    candidate: &VersionCandidate<Locator>,
    existing: &VersionCandidate<Locator>,
) -> bool
where
    Locator: Ord,
{
    candidate.modified_since_epoch > existing.modified_since_epoch
        || (candidate.modified_since_epoch == existing.modified_since_epoch
            && candidate.record.content_hash > existing.record.content_hash)
        || (candidate.modified_since_epoch == existing.modified_since_epoch
            && candidate.record.content_hash == existing.record.content_hash
            && candidate.locator > existing.locator)
}

fn validate_repository_scope(
    repository_scope: Option<&RepositoryScope>,
) -> Result<(), TokenClaimsError> {
    if let Some(repository_scope) = repository_scope {
        let _validated = RepositoryScope::new(
            repository_scope.provider(),
            repository_scope.owner(),
            repository_scope.name(),
            repository_scope.revision(),
        )?;
    }

    Ok(())
}

const fn reconstruction_plan_error_detail(
    error: &FileRecordInvariantError,
) -> IndexRebuildIssueDetail {
    let detail = match error {
        FileRecordInvariantError::ChunkHash(_) => {
            IndexRebuildReconstructionPlanDetail::ChunkHashInvalid
        }
        FileRecordInvariantError::EmptyChunk => IndexRebuildReconstructionPlanDetail::EmptyChunk,
        FileRecordInvariantError::NonContiguousChunkOffsets => {
            IndexRebuildReconstructionPlanDetail::NonContiguousChunkOffsets
        }
        FileRecordInvariantError::InvalidChunkRange => {
            IndexRebuildReconstructionPlanDetail::InvalidChunkRange
        }
        FileRecordInvariantError::InvalidPackedRange => {
            IndexRebuildReconstructionPlanDetail::InvalidPackedRange
        }
        FileRecordInvariantError::LengthOverflow => {
            IndexRebuildReconstructionPlanDetail::LengthOverflow
        }
        FileRecordInvariantError::TotalBytesMismatch => {
            IndexRebuildReconstructionPlanDetail::TotalBytesMismatch
        }
    };
    IndexRebuildIssueDetail::InvalidReconstructionPlan(detail)
}
