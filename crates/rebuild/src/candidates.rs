use std::{collections::HashMap, time::Duration};

use shardline_index::{FileRecord, FileRecordInvariantError, RecordTraversal, StoredRecord};
use shardline_protocol::{RepositoryScope, TokenClaimsError};
use shardline_server_core::{
    OpsRecordStore, provider_directory, validate_content_hash, validate_identifier,
};

use super::{
    IndexRebuildIssueDetail, IndexRebuildIssueKind, IndexRebuildReconstructionPlanDetail,
    IndexRebuildReport, RebuildError, push_issue,
};
use shardline_server_core::parse_stored_file_record_bytes;

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
    pub(super) locator: Locator,
    pub(super) modified_since_epoch: Duration,
}

pub(super) fn collect_candidate<RecordAdapter>(
    record_store: &RecordAdapter,
    entry: StoredRecord<RecordAdapter::Locator>,
    candidates: &mut HashMap<RebuildKey, VersionCandidate<RecordAdapter::Locator>>,
    report: &mut IndexRebuildReport,
) -> Result<(), RebuildError>
where
    RecordAdapter: OpsRecordStore,
    RecordAdapter::Error: Into<RebuildError>,
{
    let StoredRecord {
        locator: path,
        bytes,
        modified_since_epoch,
    } = entry;
    let location = record_store.locator_display(&path);
    let record = match parse_stored_file_record_bytes(&bytes) {
        Ok(record) => record,
        Err(shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
            ..
        }) => {
            push_issue(
                report,
                IndexRebuildIssueKind::OversizedVersionRecordMetadata,
                location,
                IndexRebuildIssueDetail::OversizedVersionRecordMetadata,
            )?;
            return Ok(());
        }
        Err(shardline_server_core::ParseStoredFileRecordError::Json(_)) => {
            push_issue(
                report,
                IndexRebuildIssueKind::InvalidVersionRecordJson,
                location,
                IndexRebuildIssueDetail::RecordJsonInvalid,
            )?;
            return Ok(());
        }
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

    let expected_path = RecordTraversal::version_record_locator(record_store, &record);
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

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_protocol::{HashParseError, RepositoryProvider, RepositoryScope};
    use std::time::Duration;

    fn valid_hex_hash() -> String {
        "a".repeat(64)
    }

    fn make_file_record(file_id: &str, content_hash: &str) -> FileRecord {
        FileRecord {
            file_id: file_id.to_owned(),
            content_hash: content_hash.to_owned(),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: None,
            chunks: Vec::new(),
        }
    }

    fn make_file_record_with_scope(
        file_id: &str,
        content_hash: &str,
        scope: RepositoryScope,
    ) -> FileRecord {
        FileRecord {
            file_id: file_id.to_owned(),
            content_hash: content_hash.to_owned(),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: Some(scope),
            chunks: Vec::new(),
        }
    }

    fn make_scope() -> RepositoryScope {
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "repo", Some("main")).unwrap()
    }

    // ---- rebuild_key tests ----

    #[test]
    fn rebuild_key_with_none_scope_has_no_repository_fields() {
        let record = make_file_record("file.txt", &valid_hex_hash());
        let key = rebuild_key(&record);

        assert_eq!(key.provider, None);
        assert_eq!(key.owner, None);
        assert_eq!(key.name, None);
        assert_eq!(key.revision, None);
        assert_eq!(key.file_id, "file.txt");
    }

    #[test]
    fn rebuild_key_with_scope_populates_all_fields() {
        let scope = make_scope();
        let record = make_file_record_with_scope("data.bin", &valid_hex_hash(), scope);
        let key = rebuild_key(&record);

        assert_eq!(key.provider, Some("github"));
        assert_eq!(key.owner, Some("team".to_owned()));
        assert_eq!(key.name, Some("repo".to_owned()));
        assert_eq!(key.revision, Some("main".to_owned()));
        assert_eq!(key.file_id, "data.bin");
    }

    #[test]
    fn rebuild_key_with_scope_without_revision() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitLab, "org", "project", None).unwrap();
        let record = make_file_record_with_scope("readme.md", &valid_hex_hash(), scope);
        let key = rebuild_key(&record);

        assert_eq!(key.provider, Some("gitlab"));
        assert_eq!(key.owner, Some("org".to_owned()));
        assert_eq!(key.name, Some("project".to_owned()));
        assert_eq!(key.revision, None);
        assert_eq!(key.file_id, "readme.md");
    }

    #[test]
    fn rebuild_key_uses_provider_directory_mapping() {
        for (provider, expected_dir) in [
            (RepositoryProvider::GitHub, "github"),
            (RepositoryProvider::Gitea, "gitea"),
            (RepositoryProvider::GitLab, "gitlab"),
            (RepositoryProvider::Codeberg, "codeberg"),
            (RepositoryProvider::Generic, "generic"),
        ] {
            let scope = RepositoryScope::new(provider, "owner", "name", None).unwrap();
            let record = make_file_record_with_scope("f", &valid_hex_hash(), scope);
            let key = rebuild_key(&record);
            assert_eq!(key.provider, Some(expected_dir));
        }
    }

    // ---- candidate_is_newer tests ----

    #[test]
    fn candidate_is_newer_when_modified_since_epoch_is_higher() {
        let older = VersionCandidate {
            record: make_file_record("f", &valid_hex_hash()),
            locator: "loc-a",
            modified_since_epoch: Duration::from_secs(100),
        };
        let newer = VersionCandidate {
            record: make_file_record("f", &valid_hex_hash()),
            locator: "loc-b",
            modified_since_epoch: Duration::from_secs(200),
        };

        assert!(candidate_is_newer(&newer, &older));
        assert!(!candidate_is_newer(&older, &newer));
    }

    #[test]
    fn candidate_is_newer_when_same_epoch_higher_content_hash() {
        let epoch = Duration::from_secs(100);
        let low_hash = VersionCandidate {
            record: make_file_record("f", &"0".repeat(64)),
            locator: "loc-a",
            modified_since_epoch: epoch,
        };
        let high_hash = VersionCandidate {
            record: make_file_record("f", &"f".repeat(64)),
            locator: "loc-b",
            modified_since_epoch: epoch,
        };

        assert!(candidate_is_newer(&high_hash, &low_hash));
        assert!(!candidate_is_newer(&low_hash, &high_hash));
    }

    #[test]
    fn candidate_is_newer_when_same_epoch_same_hash_higher_locator() {
        let epoch = Duration::from_secs(100);
        let hash = valid_hex_hash();
        let lower_locator = VersionCandidate {
            record: make_file_record("f", &hash),
            locator: "loc-a",
            modified_since_epoch: epoch,
        };
        let higher_locator = VersionCandidate {
            record: make_file_record("f", &hash),
            locator: "loc-b",
            modified_since_epoch: epoch,
        };

        // "loc-b" > "loc-a"
        assert!(candidate_is_newer(&higher_locator, &lower_locator));
        assert!(!candidate_is_newer(&lower_locator, &higher_locator));
    }

    #[test]
    fn candidate_is_not_newer_when_equal() {
        let epoch = Duration::from_secs(100);
        let hash = valid_hex_hash();
        let a = VersionCandidate {
            record: make_file_record("f", &hash),
            locator: "loc-a",
            modified_since_epoch: epoch,
        };
        let b = VersionCandidate {
            record: make_file_record("f", &hash),
            locator: "loc-a",
            modified_since_epoch: epoch,
        };

        assert!(!candidate_is_newer(&a, &b));
    }

    #[test]
    fn candidate_is_newer_epoch_beats_content_hash() {
        let lower_epoch = VersionCandidate {
            record: make_file_record("f", &"f".repeat(64)),
            locator: "loc-b",
            modified_since_epoch: Duration::from_secs(100),
        };
        let higher_epoch = VersionCandidate {
            record: make_file_record("f", &"0".repeat(64)),
            locator: "loc-a",
            modified_since_epoch: Duration::from_secs(200),
        };

        // Higher epoch wins even with lower content hash
        assert!(candidate_is_newer(&higher_epoch, &lower_epoch));
    }

    // ---- validate_repository_scope tests ----

    #[test]
    fn validate_repository_scope_none_returns_ok() {
        assert!(validate_repository_scope(None).is_ok());
    }

    #[test]
    fn validate_repository_scope_valid_scope_returns_ok() {
        let scope = make_scope();
        assert!(validate_repository_scope(Some(&scope)).is_ok());
    }

    #[test]
    fn validate_repository_scope_empty_owner_returns_err() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "", "repo", None);
        assert_eq!(scope, Err(TokenClaimsError::EmptyRepositoryOwner));
    }

    #[test]
    fn validate_repository_scope_empty_name_returns_err() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "owner", "", None);
        assert_eq!(scope, Err(TokenClaimsError::EmptyRepositoryName));
    }

    // ---- reconstruction_plan_error_detail tests ----

    #[test]
    fn reconstruction_plan_error_detail_chunk_hash() {
        let error = FileRecordInvariantError::ChunkHash(HashParseError::InvalidLength);
        let detail = reconstruction_plan_error_detail(&error);
        assert_eq!(
            detail,
            IndexRebuildIssueDetail::InvalidReconstructionPlan(
                IndexRebuildReconstructionPlanDetail::ChunkHashInvalid
            )
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_empty_chunk() {
        let error = FileRecordInvariantError::EmptyChunk;
        let detail = reconstruction_plan_error_detail(&error);
        assert_eq!(
            detail,
            IndexRebuildIssueDetail::InvalidReconstructionPlan(
                IndexRebuildReconstructionPlanDetail::EmptyChunk
            )
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_non_contiguous_offsets() {
        let error = FileRecordInvariantError::NonContiguousChunkOffsets;
        let detail = reconstruction_plan_error_detail(&error);
        assert_eq!(
            detail,
            IndexRebuildIssueDetail::InvalidReconstructionPlan(
                IndexRebuildReconstructionPlanDetail::NonContiguousChunkOffsets
            )
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_invalid_chunk_range() {
        let error = FileRecordInvariantError::InvalidChunkRange;
        let detail = reconstruction_plan_error_detail(&error);
        assert_eq!(
            detail,
            IndexRebuildIssueDetail::InvalidReconstructionPlan(
                IndexRebuildReconstructionPlanDetail::InvalidChunkRange
            )
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_invalid_packed_range() {
        let error = FileRecordInvariantError::InvalidPackedRange;
        let detail = reconstruction_plan_error_detail(&error);
        assert_eq!(
            detail,
            IndexRebuildIssueDetail::InvalidReconstructionPlan(
                IndexRebuildReconstructionPlanDetail::InvalidPackedRange
            )
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_length_overflow() {
        let error = FileRecordInvariantError::LengthOverflow;
        let detail = reconstruction_plan_error_detail(&error);
        assert_eq!(
            detail,
            IndexRebuildIssueDetail::InvalidReconstructionPlan(
                IndexRebuildReconstructionPlanDetail::LengthOverflow
            )
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_total_bytes_mismatch() {
        let error = FileRecordInvariantError::TotalBytesMismatch;
        let detail = reconstruction_plan_error_detail(&error);
        assert_eq!(
            detail,
            IndexRebuildIssueDetail::InvalidReconstructionPlan(
                IndexRebuildReconstructionPlanDetail::TotalBytesMismatch
            )
        );
    }
}
