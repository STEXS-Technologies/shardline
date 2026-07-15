#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
    )
)]

//! Storage integrity checking logic for the Shardline server ecosystem.
//!
//! This crate provides pure fsck functions that operate on explicit
//! store parameters rather than server configuration.

mod lifecycle_checks;
mod record_checks;

use std::{
    collections::HashSet,
    fmt::{Display, Formatter, Result as FmtResult},
    io::Error as IoError,
    num::TryFromIntError,
    path::{Path, PathBuf},
};

use shardline_index::{
    AsyncIndexStore, FileRecord, FileRecordInvariantError, LocalIndexStoreError,
    MemoryIndexStoreError, MemoryRecordStoreError, PostgresMetadataStoreError, xet_hash_hex_string,
};
use shardline_protocol::HashParseError;
use shardline_server_core::{
    InvalidSerializedShardError, OpsRecordKind, OpsRecordStore, ServerObjectStore,
    ServerObjectStoreError, ShardMetadataLimits, checked_increment, read_full_object,
};
use shardline_storage::{ObjectKey, ObjectStore};
use shardline_xet_adapter::{XetAdapterError, XorbParseError, retained_shard_chunk_hashes};
use thiserror::Error;

use lifecycle_checks::inspect_lifecycle_metadata;
use record_checks::scan_record_tree;

/// Fsck operation failure.
#[derive(Debug, Error)]
pub enum FsckError {
    /// A local filesystem I/O error occurred.
    #[error("local storage operation failed")]
    Io(#[from] IoError),
    /// JSON serialization or deserialization failed.
    #[error("json operation failed")]
    Json(#[from] serde_json::Error),
    /// Numeric conversion exceeded supported bounds.
    #[error("numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
    /// Arithmetic overflowed a checked bound.
    #[error("arithmetic overflow")]
    Overflow,
    /// Local storage adapter access failed.
    #[error("local storage adapter operation failed")]
    LocalObjectStore(#[from] shardline_storage::LocalObjectStoreError),
    /// S3-compatible object-storage adapter access failed.
    #[error("s3 object storage adapter operation failed")]
    S3ObjectStore(#[from] shardline_storage::S3ObjectStoreError),
    /// Object-store backend error.
    #[error("object store operation failed")]
    ObjectStore(#[from] ServerObjectStoreError),
    /// Xet adapter access failed.
    #[error("xet adapter operation failed")]
    XetAdapter(#[from] XetAdapterError),
    /// Local index adapter access failed.
    #[error("local index adapter operation failed")]
    LocalIndexStore(#[from] LocalIndexStoreError),
    /// In-memory index adapter access failed.
    #[error("memory index adapter operation failed")]
    MemoryIndexStore(#[from] MemoryIndexStoreError),
    /// In-memory record adapter access failed.
    #[error("memory record adapter operation failed")]
    MemoryRecordStore(#[from] MemoryRecordStoreError),
    /// Postgres metadata adapter access failed.
    #[error("postgres metadata adapter operation failed")]
    PostgresMetadata(#[from] PostgresMetadataStoreError),
    /// Stored file metadata exceeded the bounded parser ceiling.
    #[error("stored file metadata exceeded the bounded parser ceiling")]
    StoredFileMetadataTooLarge {
        /// Observed file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted file length in bytes.
        maximum_bytes: u64,
    },
}

impl From<shardline_server_core::ParseStoredFileRecordError> for FsckError {
    fn from(value: shardline_server_core::ParseStoredFileRecordError) -> Self {
        match value {
            shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            } => Self::StoredFileMetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            },
            shardline_server_core::ParseStoredFileRecordError::Json(e) => Self::Json(e),
        }
    }
}

impl From<shardline_server_core::ValidateIdentifierError> for FsckError {
    fn from(_: shardline_server_core::ValidateIdentifierError) -> Self {
        Self::Overflow
    }
}

impl From<shardline_server_core::ValidateContentHashError> for FsckError {
    fn from(_: shardline_server_core::ValidateContentHashError) -> Self {
        Self::Overflow
    }
}

impl From<shardline_server_core::RebuildOverflowError> for FsckError {
    fn from(_: shardline_server_core::RebuildOverflowError) -> Self {
        Self::Overflow
    }
}

impl From<XorbParseError> for FsckError {
    fn from(_: XorbParseError) -> Self {
        Self::Overflow
    }
}

impl From<HashParseError> for FsckError {
    fn from(_: HashParseError) -> Self {
        Self::Overflow
    }
}

/// Integrity-check report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsckReport {
    /// Number of latest records scanned through the configured record store.
    pub latest_records: u64,
    /// Number of immutable version records scanned through the configured record store.
    pub version_records: u64,
    /// Number of chunk references inspected across all records.
    pub inspected_chunk_references: u64,
    /// Number of dedupe-shard mappings inspected through the index adapter.
    pub inspected_dedupe_shard_mappings: u64,
    /// Number of durable reconstruction rows inspected through the index adapter.
    pub inspected_reconstructions: u64,
    /// Number of processed provider webhook deliveries inspected through the index adapter.
    pub inspected_webhook_deliveries: u64,
    /// Number of provider repository lifecycle states inspected through the index adapter.
    pub inspected_provider_repository_states: u64,
    /// Collected integrity issues.
    pub issues: Vec<FsckIssue>,
}

impl FsckReport {
    /// Returns the total issue count.
    #[must_use]
    pub const fn issue_count(&self) -> usize {
        self.issues.len()
    }

    /// Returns whether the storage root passed every check.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        self.issues.is_empty()
    }
}

/// One integrity issue reported by the checker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsckIssue {
    /// Problem classification.
    pub kind: FsckIssueKind,
    /// Stable object or record location associated with the issue.
    pub location: String,
    /// Structured detail for operators.
    pub detail: FsckIssueDetail,
}

/// Integrity issue detail.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum FsckIssueDetail {
    /// A required version record was missing.
    #[error("missing version record {version_locator}")]
    MissingVersionRecord {
        /// Expected version-record locator.
        version_locator: String,
    },
    /// Record metadata exceeded parser limits.
    #[error("record metadata exceeded the bounded parser ceiling")]
    OversizedRecordMetadata,
    /// Record JSON was invalid.
    #[error("record json was invalid")]
    RecordJsonInvalid,
    /// A record file identifier was invalid.
    #[error("record file_id `{file_id}` is invalid")]
    InvalidFileId {
        /// Invalid file identifier.
        file_id: String,
    },
    /// A record content hash was invalid.
    #[error("record content hash `{content_hash}` is invalid")]
    InvalidContentHash {
        /// Invalid content hash.
        content_hash: String,
    },
    /// A record was stored at an unexpected location.
    #[error("expected record at {expected_locator}")]
    RecordPathMismatch {
        /// Expected record locator.
        expected_locator: String,
    },
    /// A record file identifier did not match its path.
    #[error("record file_id does not match path")]
    RecordFileIdPathMismatch,
    /// A record content hash did not match its path.
    #[error("record content hash does not match path")]
    RecordContentHashPathMismatch,
    /// A reconstruction plan was invalid.
    #[error("{0}")]
    InvalidReconstructionPlan(FsckReconstructionPlanDetail),
    /// A chunk hash was invalid.
    #[error("chunk hash `{chunk_hash}` is invalid")]
    InvalidChunkHash {
        /// Invalid chunk hash.
        chunk_hash: String,
    },
    /// An xorb hash was invalid.
    #[error("xorb hash `{xorb_hash}` is invalid")]
    InvalidXorbHash {
        /// Invalid xorb hash.
        xorb_hash: String,
    },
    /// An object was referenced by a record.
    #[error("referenced by record {record_location}")]
    ReferencedByRecord {
        /// Referencing record location.
        record_location: String,
    },
    /// An object was referenced by a native Xet record.
    #[error("referenced by native xet record {record_location}")]
    ReferencedByNativeXetRecord {
        /// Referencing record location.
        record_location: String,
    },
    /// An object was referenced by a native Xet xorb.
    #[error("referenced by native xet xorb {xorb_location}")]
    ReferencedByNativeXetXorb {
        /// Referencing xorb location.
        xorb_location: String,
    },
    /// A hash comparison failed.
    #[error("expected {expected_hash}, got {observed_hash}")]
    HashMismatch {
        /// Expected hash.
        expected_hash: String,
        /// Observed hash.
        observed_hash: String,
    },
    /// A length comparison failed.
    #[error("expected {expected_length}, got {observed_length}")]
    LengthMismatch {
        /// Expected length.
        expected_length: u64,
        /// Observed length.
        observed_length: u64,
    },
    /// A native Xet range exceeded the xorb chunk count.
    #[error("xorb range {range_start}..{range_end} exceeded {chunk_count} chunks")]
    XorbRangeExceededChunkCount {
        /// Requested range start.
        range_start: u32,
        /// Requested range end.
        range_end: u32,
        /// Available chunk count.
        chunk_count: usize,
    },
    /// A latest record differed from its immutable version.
    #[error("latest record differed from version record {version_locator}")]
    MismatchedVersionRecord {
        /// Version-record locator.
        version_locator: String,
    },
    /// A mapped chunk hash points at a retained shard object.
    #[error("mapped chunk hash {chunk_hash}")]
    MappedChunkHash {
        /// Mapped chunk hash.
        chunk_hash: String,
    },
    /// A mapped chunk hash was absent from its retained shard.
    #[error("mapped chunk hash {chunk_hash} was absent from retained shard")]
    MappedChunkHashAbsentFromRetainedShard {
        /// Mapped chunk hash.
        chunk_hash: String,
    },
    /// A retained shard was invalid.
    #[error("{0}")]
    InvalidRetainedShard(InvalidSerializedShardError),
    /// A reconstruction row was listed but unreadable.
    #[error("reconstruction index listed a file id without a readable row")]
    ReconstructionListedUnreadableRow,
    /// A reconstruction contained no terms.
    #[error("reconstruction contained no terms")]
    ReconstructionContainedNoTerms,
    /// A reconstruction referenced an unregistered xorb.
    #[error("reconstruction referenced unregistered xorb {xorb_hash}")]
    MissingReconstructionXorb {
        /// Missing xorb hash.
        xorb_hash: String,
    },
    /// Quarantine delete-after preceded first-seen.
    #[error(
        "delete-after {delete_after_unix_seconds} preceded first-seen {first_seen_unreachable_at_unix_seconds}"
    )]
    InvalidQuarantineTimeline {
        /// Candidate delete-after timestamp.
        delete_after_unix_seconds: u64,
        /// Candidate first-seen timestamp.
        first_seen_unreachable_at_unix_seconds: u64,
    },
    /// Quarantine metadata referenced a missing object.
    #[error("quarantine metadata referenced a missing object")]
    QuarantineReferencedMissingObject,
    /// Quarantine metadata targeted a reachable object.
    #[error("quarantine metadata still targeted a reachable live object")]
    QuarantineTargetedReachableObject,
    /// Retention hold release-after preceded held-at.
    #[error("release-after {release_after_unix_seconds} preceded held-at {held_at_unix_seconds}")]
    InvalidRetentionTimeline {
        /// Hold release timestamp.
        release_after_unix_seconds: u64,
        /// Hold creation timestamp.
        held_at_unix_seconds: u64,
    },
    /// Active retention hold reason for a missing held object.
    #[error("active retention hold reason: {reason}")]
    ActiveRetentionHoldReason {
        /// Retention reason.
        reason: String,
    },
    /// Active retention hold coexisted with quarantine state.
    #[error("active retention hold still coexisted with quarantine state")]
    ActiveRetentionHoldQuarantined,
    /// Webhook delivery timestamp exceeded the accepted future skew.
    #[error(
        "processed-at {processed_at_unix_seconds} exceeded max allowed {max_allowed_unix_seconds}"
    )]
    WebhookDeliveryTimestampExceeded {
        /// Observed processed-at timestamp.
        processed_at_unix_seconds: u64,
        /// Maximum accepted timestamp.
        max_allowed_unix_seconds: u64,
    },
    /// Provider repository identity failed validation.
    #[error("provider repository state identity failed repository-scope validation")]
    ProviderRepositoryIdentityInvalid,
    /// Provider repository state timestamp exceeded the accepted future skew.
    #[error("{field} {timestamp} exceeded max allowed {max_allowed_unix_seconds}")]
    ProviderRepositoryStateTimestampExceeded {
        /// Timestamp field.
        field: ProviderRepositoryStateTimestampField,
        /// Observed timestamp.
        timestamp: u64,
        /// Maximum accepted timestamp.
        max_allowed_unix_seconds: u64,
    },
}

/// Reconstruction-plan detail for fsck issues.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum FsckReconstructionPlanDetail {
    /// A chunk hash was invalid.
    #[error("record chunk hash is invalid")]
    ChunkHashInvalid,
    /// A chunk was empty.
    #[error("record contains an empty chunk")]
    EmptyChunk,
    /// Chunks were not contiguous.
    #[error("record chunks are not contiguous")]
    NonContiguousChunkOffsets,
    /// A chunk range was invalid.
    #[error("record chunk range is invalid")]
    InvalidChunkRange,
    /// A packed range was invalid.
    #[error("record packed range is invalid")]
    InvalidPackedRange,
    /// Record length overflowed.
    #[error("record length overflowed")]
    LengthOverflow,
    /// Total byte count did not match chunks.
    #[error("record total byte count did not match chunks")]
    TotalBytesMismatch,
}

/// Provider repository state timestamp field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderRepositoryStateTimestampField {
    /// `last_access_changed_at_unix_seconds`.
    LastAccessChangedAtUnixSeconds,
    /// `last_revision_pushed_at_unix_seconds`.
    LastRevisionPushedAtUnixSeconds,
    /// `last_cache_invalidated_at_unix_seconds`.
    LastCacheInvalidatedAtUnixSeconds,
    /// `last_authorization_rechecked_at_unix_seconds`.
    LastAuthorizationRecheckedAtUnixSeconds,
    /// `last_drift_checked_at_unix_seconds`.
    LastDriftCheckedAtUnixSeconds,
}

impl ProviderRepositoryStateTimestampField {
    const fn as_str(self) -> &'static str {
        match self {
            Self::LastAccessChangedAtUnixSeconds => "last_access_changed_at_unix_seconds",
            Self::LastRevisionPushedAtUnixSeconds => "last_revision_pushed_at_unix_seconds",
            Self::LastCacheInvalidatedAtUnixSeconds => "last_cache_invalidated_at_unix_seconds",
            Self::LastAuthorizationRecheckedAtUnixSeconds => {
                "last_authorization_rechecked_at_unix_seconds"
            }
            Self::LastDriftCheckedAtUnixSeconds => "last_drift_checked_at_unix_seconds",
        }
    }
}

impl Display for ProviderRepositoryStateTimestampField {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str(self.as_str())
    }
}

/// Integrity issue kinds for the checker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsckIssueKind {
    /// Record metadata exceeded the bounded parser ceiling.
    OversizedRecordMetadata,
    /// Record bytes were not valid JSON.
    InvalidRecordJson,
    /// Record file identifier failed validation.
    InvalidFileId,
    /// Record content hash failed validation.
    InvalidContentHash,
    /// Record was stored at an unexpected path.
    RecordPathMismatch,
    /// Record chunk offsets were not contiguous.
    NonContiguousChunks,
    /// Record contained an empty chunk term.
    EmptyChunk,
    /// Record total byte count did not match chunk lengths.
    TotalBytesMismatch,
    /// Record contained an empty or inverted xorb chunk range.
    InvalidChunkRange,
    /// Record contained an empty or inverted packed xorb byte range.
    InvalidPackedRange,
    /// Record content hash did not match reconstructed metadata hash.
    RecordHashMismatch,
    /// Referenced chunk bytes were missing.
    MissingChunk,
    /// Referenced chunk body did not hash to the declared chunk hash.
    ChunkHashMismatch,
    /// Referenced chunk byte length did not match the record.
    ChunkLengthMismatch,
    /// Visible latest record did not have a matching immutable version record.
    MissingVersionRecord,
    /// Visible latest record differed from its immutable version record.
    MismatchedVersionRecord,
    /// Indexed retained-shard object was missing from object storage.
    MissingDedupeShardObject,
    /// Indexed retained-shard object could not be parsed as a native Xet shard.
    InvalidRetainedShard,
    /// Indexed retained-shard object did not contain the mapped chunk hash.
    InvalidDedupeShardMapping,
    /// Durable reconstruction metadata did not contain any terms.
    EmptyReconstruction,
    /// Durable reconstruction metadata referenced an unregistered xorb.
    MissingReconstructionXorb,
    /// Quarantine metadata had an invalid retention timeline.
    InvalidQuarantineCandidate,
    /// Quarantine metadata referenced an object that no longer existed.
    MissingQuarantinedObject,
    /// Quarantine metadata length disagreed with current object metadata.
    QuarantineLengthMismatch,
    /// Quarantine metadata still targeted a reachable live object.
    ReachableQuarantinedObject,
    /// Retention-hold metadata had an invalid timeline.
    InvalidRetentionHold,
    /// An active retention hold referenced an object that no longer existed.
    MissingHeldObject,
    /// An active retention hold still coexisted with quarantine state for the same object.
    HeldQuarantinedObject,
    /// A processed webhook delivery had a timestamp too far in the future.
    InvalidWebhookDeliveryTimestamp,
    /// Provider repository lifecycle metadata had an invalid repository identity.
    InvalidProviderRepositoryState,
    /// Provider repository lifecycle metadata had a timestamp too far in the future.
    InvalidProviderRepositoryStateTimestamp,
}

impl FsckIssueKind {
    /// Stable issue label for CLI and logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OversizedRecordMetadata => "oversized_record_metadata",
            Self::InvalidRecordJson => "invalid_record_json",
            Self::InvalidFileId => "invalid_file_id",
            Self::InvalidContentHash => "invalid_content_hash",
            Self::RecordPathMismatch => "record_path_mismatch",
            Self::NonContiguousChunks => "non_contiguous_chunks",
            Self::EmptyChunk => "empty_chunk",
            Self::TotalBytesMismatch => "total_bytes_mismatch",
            Self::InvalidChunkRange => "invalid_chunk_range",
            Self::InvalidPackedRange => "invalid_packed_range",
            Self::RecordHashMismatch => "record_hash_mismatch",
            Self::MissingChunk => "missing_chunk",
            Self::ChunkHashMismatch => "chunk_hash_mismatch",
            Self::ChunkLengthMismatch => "chunk_length_mismatch",
            Self::MissingVersionRecord => "missing_version_record",
            Self::MismatchedVersionRecord => "mismatched_version_record",
            Self::MissingDedupeShardObject => "missing_dedupe_shard_object",
            Self::InvalidRetainedShard => "invalid_retained_shard",
            Self::InvalidDedupeShardMapping => "invalid_dedupe_shard_mapping",
            Self::EmptyReconstruction => "empty_reconstruction",
            Self::MissingReconstructionXorb => "missing_reconstruction_xorb",
            Self::InvalidQuarantineCandidate => "invalid_quarantine_candidate",
            Self::MissingQuarantinedObject => "missing_quarantined_object",
            Self::QuarantineLengthMismatch => "quarantine_length_mismatch",
            Self::ReachableQuarantinedObject => "reachable_quarantined_object",
            Self::InvalidRetentionHold => "invalid_retention_hold",
            Self::MissingHeldObject => "missing_held_object",
            Self::HeldQuarantinedObject => "held_quarantined_object",
            Self::InvalidWebhookDeliveryTimestamp => "invalid_webhook_delivery_timestamp",
            Self::InvalidProviderRepositoryState => "invalid_provider_repository_state",
            Self::InvalidProviderRepositoryStateTimestamp => {
                "invalid_provider_repository_state_timestamp"
            }
        }
    }
}

/// Backward-compatible local fsck report alias.
pub type LocalFsckReport = FsckReport;

/// Backward-compatible local fsck issue alias.
pub type LocalFsckIssue = FsckIssue;

/// Backward-compatible local fsck issue-kind alias.
pub type LocalFsckIssueKind = FsckIssueKind;

pub const WEBHOOK_DELIVERY_FUTURE_SKEW_SECONDS: u64 = 300;

/// Runs local filesystem integrity checks over Shardline metadata and chunk storage.
///
/// # Errors
///
/// Returns [`FsckError`] when the storage root cannot be traversed or chunk/record
/// bytes cannot be read due to an operational failure.
pub async fn run_local_fsck(root: PathBuf) -> Result<LocalFsckReport, FsckError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn clean_report() -> FsckReport {
        FsckReport {
            latest_records: 0,
            version_records: 0,
            inspected_chunk_references: 0,
            inspected_dedupe_shard_mappings: 0,
            inspected_reconstructions: 0,
            inspected_webhook_deliveries: 0,
            inspected_provider_repository_states: 0,
            issues: Vec::new(),
        }
    }

    #[test]
    fn is_clean_returns_true_for_empty_issues() {
        assert!(clean_report().is_clean());
    }

    #[test]
    fn is_clean_returns_false_when_issues_present() {
        let mut report = clean_report();
        report.issues.push(FsckIssue {
            kind: FsckIssueKind::MissingChunk,
            location: "test".to_owned(),
            detail: FsckIssueDetail::HashMismatch {
                expected_hash: "a".repeat(64),
                observed_hash: "b".repeat(64),
            },
        });
        assert!(!report.is_clean());
    }

    #[test]
    fn issue_count_zero_for_clean_report() {
        assert_eq!(clean_report().issue_count(), 0);
    }

    #[test]
    fn issue_count_matches_issues_length() {
        let mut report = clean_report();
        report.issues.push(FsckIssue {
            kind: FsckIssueKind::MissingChunk,
            location: "a".to_owned(),
            detail: FsckIssueDetail::HashMismatch {
                expected_hash: "a".repeat(64),
                observed_hash: "b".repeat(64),
            },
        });
        report.issues.push(FsckIssue {
            kind: FsckIssueKind::ChunkHashMismatch,
            location: "b".to_owned(),
            detail: FsckIssueDetail::InvalidChunkHash {
                chunk_hash: "c".repeat(64),
            },
        });
        assert_eq!(report.issue_count(), 2);
    }

    // ── push_issue ──────────────────────────────────────────────────────

    #[test]
    fn push_issue_appends_to_report() {
        let mut report = clean_report();
        let detail = FsckIssueDetail::HashMismatch {
            expected_hash: "a".repeat(64),
            observed_hash: "b".repeat(64),
        };
        push_issue(
            &mut report,
            FsckIssueKind::MissingChunk,
            "loc1".to_owned(),
            detail.clone(),
        )
        .unwrap();
        assert_eq!(report.issue_count(), 1);
        assert_eq!(report.issues[0].kind, FsckIssueKind::MissingChunk);
        assert_eq!(report.issues[0].location, "loc1");
        assert_eq!(report.issues[0].detail, detail);
    }

    #[test]
    fn push_issue_increments_count_for_multiple_issues() {
        let mut report = clean_report();
        for i in 0..5 {
            push_issue(
                &mut report,
                FsckIssueKind::EmptyChunk,
                format!("loc{i}"),
                FsckIssueDetail::ReconstructionContainedNoTerms,
            )
            .unwrap();
        }
        assert_eq!(report.issue_count(), 5);
    }

    // ── push_reconstruction_plan_issue ──────────────────────────────────

    #[test]
    fn reconstruction_plan_issue_chunk_hash_maps_to_invalid_content_hash() {
        let mut report = clean_report();
        let err =
            FileRecordInvariantError::ChunkHash(shardline_protocol::HashParseError::InvalidLength);
        push_reconstruction_plan_issue(&mut report, "loc".to_owned(), &err).unwrap();
        assert_eq!(report.issue_count(), 1);
        assert_eq!(report.issues[0].kind, FsckIssueKind::InvalidContentHash);
        assert_eq!(
            report.issues[0].detail,
            FsckIssueDetail::InvalidReconstructionPlan(
                FsckReconstructionPlanDetail::ChunkHashInvalid
            )
        );
    }

    #[test]
    fn reconstruction_plan_issue_empty_chunk() {
        let mut report = clean_report();
        let err = FileRecordInvariantError::EmptyChunk;
        push_reconstruction_plan_issue(&mut report, "loc".to_owned(), &err).unwrap();
        assert_eq!(report.issues[0].kind, FsckIssueKind::EmptyChunk);
        assert_eq!(
            report.issues[0].detail,
            FsckIssueDetail::InvalidReconstructionPlan(FsckReconstructionPlanDetail::EmptyChunk)
        );
    }

    #[test]
    fn reconstruction_plan_issue_non_contiguous_chunk_offsets() {
        let mut report = clean_report();
        let err = FileRecordInvariantError::NonContiguousChunkOffsets;
        push_reconstruction_plan_issue(&mut report, "loc".to_owned(), &err).unwrap();
        assert_eq!(report.issues[0].kind, FsckIssueKind::NonContiguousChunks);
        assert_eq!(
            report.issues[0].detail,
            FsckIssueDetail::InvalidReconstructionPlan(
                FsckReconstructionPlanDetail::NonContiguousChunkOffsets
            )
        );
    }

    #[test]
    fn reconstruction_plan_issue_invalid_chunk_range() {
        let mut report = clean_report();
        let err = FileRecordInvariantError::InvalidChunkRange;
        push_reconstruction_plan_issue(&mut report, "loc".to_owned(), &err).unwrap();
        assert_eq!(report.issues[0].kind, FsckIssueKind::InvalidChunkRange);
        assert_eq!(
            report.issues[0].detail,
            FsckIssueDetail::InvalidReconstructionPlan(
                FsckReconstructionPlanDetail::InvalidChunkRange
            )
        );
    }

    #[test]
    fn reconstruction_plan_issue_invalid_packed_range() {
        let mut report = clean_report();
        let err = FileRecordInvariantError::InvalidPackedRange;
        push_reconstruction_plan_issue(&mut report, "loc".to_owned(), &err).unwrap();
        assert_eq!(report.issues[0].kind, FsckIssueKind::InvalidPackedRange);
        assert_eq!(
            report.issues[0].detail,
            FsckIssueDetail::InvalidReconstructionPlan(
                FsckReconstructionPlanDetail::InvalidPackedRange
            )
        );
    }

    #[test]
    fn reconstruction_plan_issue_length_overflow() {
        let mut report = clean_report();
        let err = FileRecordInvariantError::LengthOverflow;
        push_reconstruction_plan_issue(&mut report, "loc".to_owned(), &err).unwrap();
        assert_eq!(report.issues[0].kind, FsckIssueKind::TotalBytesMismatch);
        assert_eq!(
            report.issues[0].detail,
            FsckIssueDetail::InvalidReconstructionPlan(
                FsckReconstructionPlanDetail::LengthOverflow
            )
        );
    }

    #[test]
    fn reconstruction_plan_issue_total_bytes_mismatch() {
        let mut report = clean_report();
        let err = FileRecordInvariantError::TotalBytesMismatch;
        push_reconstruction_plan_issue(&mut report, "loc".to_owned(), &err).unwrap();
        assert_eq!(report.issues[0].kind, FsckIssueKind::TotalBytesMismatch);
        assert_eq!(
            report.issues[0].detail,
            FsckIssueDetail::InvalidReconstructionPlan(
                FsckReconstructionPlanDetail::TotalBytesMismatch
            )
        );
    }

    // ── reconstruction_plan_error_detail ─────────────────────────────────

    #[test]
    fn reconstruction_plan_error_detail_chunk_hash() {
        let err = FileRecordInvariantError::ChunkHash(
            shardline_protocol::HashParseError::InvalidCharacter,
        );
        assert_eq!(
            reconstruction_plan_error_detail(&err),
            FsckReconstructionPlanDetail::ChunkHashInvalid
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_empty_chunk() {
        let err = FileRecordInvariantError::EmptyChunk;
        assert_eq!(
            reconstruction_plan_error_detail(&err),
            FsckReconstructionPlanDetail::EmptyChunk
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_non_contiguous() {
        let err = FileRecordInvariantError::NonContiguousChunkOffsets;
        assert_eq!(
            reconstruction_plan_error_detail(&err),
            FsckReconstructionPlanDetail::NonContiguousChunkOffsets
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_invalid_chunk_range() {
        let err = FileRecordInvariantError::InvalidChunkRange;
        assert_eq!(
            reconstruction_plan_error_detail(&err),
            FsckReconstructionPlanDetail::InvalidChunkRange
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_invalid_packed_range() {
        let err = FileRecordInvariantError::InvalidPackedRange;
        assert_eq!(
            reconstruction_plan_error_detail(&err),
            FsckReconstructionPlanDetail::InvalidPackedRange
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_length_overflow() {
        let err = FileRecordInvariantError::LengthOverflow;
        assert_eq!(
            reconstruction_plan_error_detail(&err),
            FsckReconstructionPlanDetail::LengthOverflow
        );
    }

    #[test]
    fn reconstruction_plan_error_detail_total_bytes_mismatch() {
        let err = FileRecordInvariantError::TotalBytesMismatch;
        assert_eq!(
            reconstruction_plan_error_detail(&err),
            FsckReconstructionPlanDetail::TotalBytesMismatch
        );
    }

    // ── FsckIssueKind::as_str ───────────────────────────────────────────

    #[test]
    fn fsck_issue_kind_as_str_all_variants() {
        let cases: &[(FsckIssueKind, &str)] = &[
            (
                FsckIssueKind::OversizedRecordMetadata,
                "oversized_record_metadata",
            ),
            (FsckIssueKind::InvalidRecordJson, "invalid_record_json"),
            (FsckIssueKind::InvalidFileId, "invalid_file_id"),
            (FsckIssueKind::InvalidContentHash, "invalid_content_hash"),
            (FsckIssueKind::RecordPathMismatch, "record_path_mismatch"),
            (FsckIssueKind::NonContiguousChunks, "non_contiguous_chunks"),
            (FsckIssueKind::EmptyChunk, "empty_chunk"),
            (FsckIssueKind::TotalBytesMismatch, "total_bytes_mismatch"),
            (FsckIssueKind::InvalidChunkRange, "invalid_chunk_range"),
            (FsckIssueKind::InvalidPackedRange, "invalid_packed_range"),
            (FsckIssueKind::RecordHashMismatch, "record_hash_mismatch"),
            (FsckIssueKind::MissingChunk, "missing_chunk"),
            (FsckIssueKind::ChunkHashMismatch, "chunk_hash_mismatch"),
            (FsckIssueKind::ChunkLengthMismatch, "chunk_length_mismatch"),
            (
                FsckIssueKind::MissingVersionRecord,
                "missing_version_record",
            ),
            (
                FsckIssueKind::MismatchedVersionRecord,
                "mismatched_version_record",
            ),
            (
                FsckIssueKind::MissingDedupeShardObject,
                "missing_dedupe_shard_object",
            ),
            (
                FsckIssueKind::InvalidRetainedShard,
                "invalid_retained_shard",
            ),
            (
                FsckIssueKind::InvalidDedupeShardMapping,
                "invalid_dedupe_shard_mapping",
            ),
            (FsckIssueKind::EmptyReconstruction, "empty_reconstruction"),
            (
                FsckIssueKind::MissingReconstructionXorb,
                "missing_reconstruction_xorb",
            ),
            (
                FsckIssueKind::InvalidQuarantineCandidate,
                "invalid_quarantine_candidate",
            ),
            (
                FsckIssueKind::MissingQuarantinedObject,
                "missing_quarantined_object",
            ),
            (
                FsckIssueKind::QuarantineLengthMismatch,
                "quarantine_length_mismatch",
            ),
            (
                FsckIssueKind::ReachableQuarantinedObject,
                "reachable_quarantined_object",
            ),
            (
                FsckIssueKind::InvalidRetentionHold,
                "invalid_retention_hold",
            ),
            (FsckIssueKind::MissingHeldObject, "missing_held_object"),
            (
                FsckIssueKind::HeldQuarantinedObject,
                "held_quarantined_object",
            ),
            (
                FsckIssueKind::InvalidWebhookDeliveryTimestamp,
                "invalid_webhook_delivery_timestamp",
            ),
            (
                FsckIssueKind::InvalidProviderRepositoryState,
                "invalid_provider_repository_state",
            ),
            (
                FsckIssueKind::InvalidProviderRepositoryStateTimestamp,
                "invalid_provider_repository_state_timestamp",
            ),
        ];
        for &(kind, expected) in cases {
            assert_eq!(kind.as_str(), expected, "variant {kind:?}");
        }
    }

    // ── ProviderRepositoryStateTimestampField::as_str ────────────────────

    #[test]
    fn provider_repository_state_timestamp_field_as_str_all_variants() {
        let cases: &[(ProviderRepositoryStateTimestampField, &str)] = &[
            (
                ProviderRepositoryStateTimestampField::LastAccessChangedAtUnixSeconds,
                "last_access_changed_at_unix_seconds",
            ),
            (
                ProviderRepositoryStateTimestampField::LastRevisionPushedAtUnixSeconds,
                "last_revision_pushed_at_unix_seconds",
            ),
            (
                ProviderRepositoryStateTimestampField::LastCacheInvalidatedAtUnixSeconds,
                "last_cache_invalidated_at_unix_seconds",
            ),
            (
                ProviderRepositoryStateTimestampField::LastAuthorizationRecheckedAtUnixSeconds,
                "last_authorization_rechecked_at_unix_seconds",
            ),
            (
                ProviderRepositoryStateTimestampField::LastDriftCheckedAtUnixSeconds,
                "last_drift_checked_at_unix_seconds",
            ),
        ];
        for &(field, expected) in cases {
            assert_eq!(field.as_str(), expected, "variant {field:?}");
        }
    }

    // ── ProviderRepositoryStateTimestampField Display ────────────────────

    #[test]
    fn provider_repository_state_timestamp_field_display_matches_as_str() {
        let all_fields = [
            ProviderRepositoryStateTimestampField::LastAccessChangedAtUnixSeconds,
            ProviderRepositoryStateTimestampField::LastRevisionPushedAtUnixSeconds,
            ProviderRepositoryStateTimestampField::LastCacheInvalidatedAtUnixSeconds,
            ProviderRepositoryStateTimestampField::LastAuthorizationRecheckedAtUnixSeconds,
            ProviderRepositoryStateTimestampField::LastDriftCheckedAtUnixSeconds,
        ];
        for field in all_fields {
            assert_eq!(
                field.to_string(),
                field.as_str(),
                "Display mismatch for {field:?}"
            );
        }
    }

    // ── RecordKind::ops ─────────────────────────────────────────────────

    #[test]
    fn record_kind_latest_ops_returns_ops_record_kind_latest() {
        assert_eq!(RecordKind::Latest.ops(), OpsRecordKind::Latest);
    }

    #[test]
    fn record_kind_version_ops_returns_ops_record_kind_version() {
        assert_eq!(RecordKind::Version.ops(), OpsRecordKind::Version);
    }

    // ── object_key_storage_path ──────────────────────────────────────────

    #[test]
    fn object_key_storage_path_joins_root_and_key() {
        let root = PathBuf::from("/data/chunks");
        let key = ObjectKey::parse("shards/aa/bb/hash.xorb").unwrap();
        let result = object_key_storage_path(&root, &key);
        assert_eq!(result, PathBuf::from("/data/chunks/shards/aa/bb/hash.xorb"));
    }

    #[test]
    fn object_key_storage_path_handles_single_segment_key() {
        let root = PathBuf::from("/storage");
        let key = ObjectKey::parse("file.bin").unwrap();
        let result = object_key_storage_path(&root, &key);
        assert_eq!(result, PathBuf::from("/storage/file.bin"));
    }

    // ── Error conversions ────────────────────────────────────────────────

    #[test]
    fn fsck_error_from_stored_file_metadata_too_large() {
        let err = shardline_server_core::ParseStoredFileRecordError::StoredFileMetadataTooLarge {
            observed_bytes: 999,
            maximum_bytes: 100,
        };
        let fsck_err: FsckError = err.into();
        assert!(matches!(
            fsck_err,
            FsckError::StoredFileMetadataTooLarge {
                observed_bytes: 999,
                maximum_bytes: 100,
            }
        ));
    }

    #[test]
    fn fsck_error_from_stored_file_record_json_error() {
        let serde_err =
            serde_json::from_str::<serde_json::Value>("not valid json {{{").unwrap_err();
        let err = shardline_server_core::ParseStoredFileRecordError::Json(serde_err);
        let fsck_err: FsckError = err.into();
        assert!(matches!(fsck_err, FsckError::Json(_)));
    }

    #[test]
    fn fsck_error_from_validate_identifier_error() {
        let err = shardline_server_core::ValidateIdentifierError;
        let fsck_err: FsckError = err.into();
        assert!(matches!(fsck_err, FsckError::Overflow));
    }

    #[test]
    fn fsck_error_from_validate_content_hash_error() {
        let err = shardline_server_core::ValidateContentHashError;
        let fsck_err: FsckError = err.into();
        assert!(matches!(fsck_err, FsckError::Overflow));
    }

    #[test]
    fn fsck_error_from_rebuild_overflow_error() {
        let err = shardline_server_core::RebuildOverflowError;
        let fsck_err: FsckError = err.into();
        assert!(matches!(fsck_err, FsckError::Overflow));
    }

    #[test]
    fn fsck_error_from_xorb_parse_error() {
        let err = shardline_xet_adapter::XorbParseError::HashMismatch;
        let fsck_err: FsckError = err.into();
        assert!(matches!(fsck_err, FsckError::Overflow));
    }

    #[test]
    fn fsck_error_from_hash_parse_error() {
        let err = shardline_protocol::HashParseError::InvalidLength;
        let fsck_err: FsckError = err.into();
        assert!(matches!(fsck_err, FsckError::Overflow));
    }

    // ── run_fsck_with_stores integration ─────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn run_fsck_with_empty_stores_returns_clean_report() {
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path().to_path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root.clone());
        let index_store = shardline_index::LocalIndexStore::open(root.clone());
        let object_root = root.join("chunks");
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

        let report = run_fsck_with_stores(
            &record_store,
            &index_store,
            &object_root,
            &object_store,
            shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        )
        .await
        .unwrap();

        assert!(report.is_clean(), "empty stores should produce clean report");
        assert_eq!(report.latest_records, 0);
        assert_eq!(report.version_records, 0);
        assert_eq!(report.inspected_dedupe_shard_mappings, 0);
        assert_eq!(report.inspected_reconstructions, 0);
        assert_eq!(report.inspected_webhook_deliveries, 0);
        assert_eq!(report.inspected_provider_repository_states, 0);
    }

    // ── FsckIssueDetail Display ────────────────────────────────────────

    #[test]
    fn fsck_issue_detail_display_all_variants() {
        let cases: &[(FsckIssueDetail, &str)] = &[
            (
                FsckIssueDetail::MissingVersionRecord {
                    version_locator: "loc".to_owned(),
                },
                "version record",
            ),
            (FsckIssueDetail::OversizedRecordMetadata, "ceiling"),
            (FsckIssueDetail::RecordJsonInvalid, "json"),
            (
                FsckIssueDetail::InvalidFileId {
                    file_id: "bad".to_owned(),
                },
                "bad",
            ),
            (
                FsckIssueDetail::InvalidContentHash {
                    content_hash: "badhash".to_owned(),
                },
                "badhash",
            ),
            (
                FsckIssueDetail::RecordPathMismatch {
                    expected_locator: "expected".to_owned(),
                },
                "expected",
            ),
            (FsckIssueDetail::RecordFileIdPathMismatch, "path"),
            (FsckIssueDetail::RecordContentHashPathMismatch, "hash"),
            (
                FsckIssueDetail::InvalidChunkHash {
                    chunk_hash: "chunk-hash".to_owned(),
                },
                "chunk-hash",
            ),
            (
                FsckIssueDetail::InvalidXorbHash {
                    xorb_hash: "xorb-hash".to_owned(),
                },
                "xorb-hash",
            ),
            (
                FsckIssueDetail::ReferencedByRecord {
                    record_location: "loc".to_owned(),
                },
                "loc",
            ),
            (
                FsckIssueDetail::ReferencedByNativeXetRecord {
                    record_location: "native-loc".to_owned(),
                },
                "native-loc",
            ),
            (
                FsckIssueDetail::ReferencedByNativeXetXorb {
                    xorb_location: "xorb-loc".to_owned(),
                },
                "xorb-loc",
            ),
            (
                FsckIssueDetail::HashMismatch {
                    expected_hash: "abc".to_owned(),
                    observed_hash: "def".to_owned(),
                },
                "abc",
            ),
            (
                FsckIssueDetail::LengthMismatch {
                    expected_length: 10,
                    observed_length: 5,
                },
                "10",
            ),
            (
                FsckIssueDetail::XorbRangeExceededChunkCount {
                    range_start: 0,
                    range_end: 5,
                    chunk_count: 3,
                },
                "5",
            ),
            (
                FsckIssueDetail::MismatchedVersionRecord {
                    version_locator: "vloc".to_owned(),
                },
                "vloc",
            ),
            (
                FsckIssueDetail::MappedChunkHash {
                    chunk_hash: "mapped-hash".to_owned(),
                },
                "mapped-hash",
            ),
            (
                FsckIssueDetail::MappedChunkHashAbsentFromRetainedShard {
                    chunk_hash: "absent-hash".to_owned(),
                },
                "absent-hash",
            ),
            (FsckIssueDetail::ReconstructionListedUnreadableRow, "row"),
            (FsckIssueDetail::ReconstructionContainedNoTerms, "terms"),
            (
                FsckIssueDetail::MissingReconstructionXorb {
                    xorb_hash: "missing-xorb".to_owned(),
                },
                "missing-xorb",
            ),
        ];
        for (detail, substring) in cases {
            let msg = detail.to_string();
            assert!(!msg.is_empty(), "empty display for {detail:?}");
            assert!(
                msg.contains(substring),
                "expected '{substring}' in '{msg}' from {detail:?}"
            );
        }
    }

    #[test]
    fn fsck_issue_detail_invalid_retained_shard_display() {
        let detail = FsckIssueDetail::InvalidRetainedShard(
            shardline_server_core::InvalidSerializedShardError::ParserRejectedMetadata,
        );
        let msg = detail.to_string();
        assert!(!msg.is_empty(), "empty display for {detail:?}");
    }

    #[test]
    fn fsck_reconstruction_plan_detail_display_all_variants() {
        let cases: &[(FsckReconstructionPlanDetail, &str)] = &[
            (FsckReconstructionPlanDetail::ChunkHashInvalid, "hash"),
            (FsckReconstructionPlanDetail::EmptyChunk, "empty"),
            (FsckReconstructionPlanDetail::NonContiguousChunkOffsets, "contiguous"),
            (FsckReconstructionPlanDetail::InvalidChunkRange, "range"),
            (FsckReconstructionPlanDetail::InvalidPackedRange, "range"),
            (FsckReconstructionPlanDetail::LengthOverflow, "overflow"),
            (FsckReconstructionPlanDetail::TotalBytesMismatch, "total"),
        ];
        for (detail, substring) in cases {
            let msg = detail.to_string();
            assert!(!msg.is_empty(), "empty display for {detail:?}");
            assert!(
                msg.contains(substring),
                "expected '{substring}' in '{msg}' from {detail:?}"
            );
        }
    }

    // ── FsckReconstructionPlanDetail Display ──────────────────────────

    #[test]
    fn fsck_reconstruction_plan_detail_invalid_reconstruction_plan_display() {
        let detail = FsckIssueDetail::InvalidReconstructionPlan(
            FsckReconstructionPlanDetail::ChunkHashInvalid,
        );
        let msg = detail.to_string();
        assert!(!msg.is_empty());
    }

    // ── FsckError display ──────────────────────────────────────────────

    #[test]
    fn fsck_error_display_all_variants() {
        let cases: &[(FsckError, &str)] = &[
            (FsckError::Io(std::io::Error::other("test")), "storage"),
            (FsckError::Json(serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err()), "json"),
            (FsckError::NumericConversion(u64::try_from(-1i32).unwrap_err()), "bounds"),
            (FsckError::Overflow, "overflow"),
            (FsckError::StoredFileMetadataTooLarge { observed_bytes: 999, maximum_bytes: 100 }, "ceiling"),
        ];
        for (error, substring) in cases {
            let msg = error.to_string();
            assert!(!msg.is_empty(), "empty display for {error:?}");
            assert!(
                msg.contains(substring),
                "expected '{substring}' in '{msg}' from {error:?}"
            );
        }
    }

    #[test]
    fn fsck_error_from_xorb_parse_error_display() {
        let error: FsckError = shardline_xet_adapter::XorbParseError::HashMismatch.into();
        let msg = error.to_string();
        assert!(!msg.is_empty());
    }

    // ── object_location_display: blackhole fallback ────────────────────

    #[test]
    fn object_location_display_with_blackhole_falls_back_to_storage_path() {
        let object_root = PathBuf::from("/tmp/test-root");
        let object_store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("ab/cdef1234").unwrap();
        let display = object_location_display(&object_root, &object_store, &key);
        assert!(
            display.contains("ab/cdef1234"),
            "expected key in display, got: {display}"
        );
        assert!(
            display.contains("/tmp/test-root"),
            "expected root in display, got: {display}"
        );
    }

    // ── run_fsck_with_stores: missing dedupe shard object ────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn run_fsck_with_missing_dedupe_shard_object_detected() {
        use shardline_index::{DedupeShardMapping, MemoryIndexStore};
        use shardline_protocol::ShardlineHash;
        use shardline_storage::ObjectKey;
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path().to_path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root.clone());
        let index_store = MemoryIndexStore::new();
        let object_root = root.join("chunks");
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

        // Insert a dedupe shard mapping pointing to a shard that does not exist in object storage
        let chunk_hash = ShardlineHash::from_bytes([42; 32]);
        let shard_key = ObjectKey::parse("shards/aa/missing.shard").unwrap();
        let mapping = DedupeShardMapping::new(chunk_hash, shard_key);
        index_store.upsert_dedupe_shard_mapping(&mapping).unwrap();

        let report = run_fsck_with_stores(
            &record_store,
            &index_store,
            &object_root,
            &object_store,
            shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        )
        .await
        .unwrap();

        assert!(!report.is_clean(), "expected issues for missing shard object");
        assert_eq!(report.inspected_dedupe_shard_mappings, 1);
        assert!(
            report.issues.iter().any(|i| i.kind == FsckIssueKind::MissingDedupeShardObject),
            "expected MissingDedupeShardObject issue, got: {:#?}",
            report.issues
        );
    }

    // ── run_fsck_with_stores: invalid retained shard ──────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn run_fsck_with_invalid_retained_shard_detected() {
        use shardline_index::{DedupeShardMapping, MemoryIndexStore};
        use shardline_protocol::ShardlineHash;
        use shardline_storage::ObjectKey;
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path().to_path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root.clone());
        let index_store = MemoryIndexStore::new();
        let object_root = root.join("chunks");
        let _object_store = ServerObjectStore::local(object_root.clone()).unwrap();

        // Insert a dedupe shard mapping pointing to a shard key
        let chunk_hash = ShardlineHash::from_bytes([42; 32]);
        let shard_key = ObjectKey::parse("shards/aa/invalid.shard").unwrap();
        let mapping = DedupeShardMapping::new(chunk_hash, shard_key.clone());
        index_store.upsert_dedupe_shard_mapping(&mapping).unwrap();

        // Create an object at the shard key with garbage bytes (not a valid shard).
        // Write directly to the local filesystem.
        let shard_path = object_root.join(shard_key.as_str());
        if let Some(parent) = shard_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&shard_path, b"not a valid shard").unwrap();

        // Re-create the object store so it picks up the written file
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

        let report = run_fsck_with_stores(
            &record_store,
            &index_store,
            &object_root,
            &object_store,
            shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        )
        .await
        .unwrap();

        assert!(!report.is_clean(), "expected issues for invalid retained shard");
        assert_eq!(report.inspected_dedupe_shard_mappings, 1);
        assert!(
            report.issues.iter().any(|i| i.kind == FsckIssueKind::InvalidRetainedShard),
            "expected InvalidRetainedShard issue, got: {:#?}",
            report.issues
        );
    }

    // ── run_fsck_with_stores: empty reconstruction ───────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn run_fsck_with_empty_reconstruction_detected() {
        use shardline_index::{FileId, FileReconstruction, MemoryIndexStore};
        use shardline_protocol::ShardlineHash;
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path().to_path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root.clone());
        let index_store = MemoryIndexStore::new();
        let object_root = root.join("chunks");
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

        // Insert a reconstruction with empty terms
        let hash = ShardlineHash::from_bytes([99; 32]);
        let file_id = FileId::new(hash);
        let reconstruction = FileReconstruction::new(vec![]);
        index_store.insert_reconstruction(&file_id, &reconstruction).unwrap();

        let report = run_fsck_with_stores(
            &record_store,
            &index_store,
            &object_root,
            &object_store,
            shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        )
        .await
        .unwrap();

        assert!(!report.is_clean(), "expected issues for empty reconstruction");
        assert_eq!(report.inspected_reconstructions, 1);
        assert!(
            report.issues.iter().any(|i| i.kind == FsckIssueKind::EmptyReconstruction),
            "expected EmptyReconstruction issue, got: {:#?}",
            report.issues
        );
    }

    // ── FsckError From impls for store errors ───────────────────────────

    #[test]
    fn fsck_error_from_local_object_store_error() {
        use shardline_storage::LocalObjectStoreError;
        let io_err = std::io::Error::other("test");
        let err = FsckError::from(LocalObjectStoreError::Io(io_err));
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn fsck_error_from_s3_object_store_error() {
        use shardline_storage::S3ObjectStoreError;
        let s3_err = S3ObjectStoreError::IncompleteCredentials;
        let err = FsckError::from(s3_err);
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn fsck_error_from_server_object_store_error() {
        use shardline_server_core::ServerObjectStoreError;
        let err = FsckError::from(ServerObjectStoreError::NotFound);
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn fsck_error_from_xet_adapter_error() {
        use shardline_xet_adapter::XetAdapterError;
        let io_err = std::io::Error::other("test");
        let err = FsckError::from(XetAdapterError::Io(io_err));
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn fsck_error_from_local_index_store_error() {
        use shardline_index::LocalIndexStoreError;
        let io_err = std::io::Error::other("test");
        let err = FsckError::from(LocalIndexStoreError::Io(io_err));
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn fsck_error_from_memory_index_store_error() {
        use shardline_index::MemoryIndexStoreError;
        let err = FsckError::from(MemoryIndexStoreError::LockPoisoned);
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn fsck_error_from_memory_record_store_error() {
        use shardline_index::MemoryRecordStoreError;
        let err = FsckError::from(MemoryRecordStoreError::RecordNotFound);
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn fsck_error_from_postgres_metadata_store_error() {
        use shardline_index::PostgresMetadataStoreError;
        let json_err = serde_json::from_str::<()>("invalid").unwrap_err();
        let err = FsckError::from(PostgresMetadataStoreError::Json(json_err));
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    // ── unix_now_seconds_checked ────────────────────────────────────

    #[test]
    fn unix_now_seconds_checked_returns_ok() {
        let result = unix_now_seconds_checked();
        assert!(result.is_ok());
        let seconds = result.unwrap();
        assert!(seconds > 1_700_000_000, "timestamp should be plausible");
    }

    // ── object_location_display ─────────────────────────────────────

    #[test]
    fn object_location_display_with_local_store() {
        let storage = shardline_test_support::TempStorage::new();
        let object_root = storage.path().join("chunks");
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();
        let key = ObjectKey::parse("ab/cdef1234").unwrap();
        let display = object_location_display(&object_root, &object_store, &key);
        assert!(!display.is_empty());
        assert!(
            display.contains("ab"),
            "expected display to contain 'ab', got: {display}"
        );
        assert!(
            display.contains("cdef1234"),
            "expected display to contain 'cdef1234', got: {display}"
        );
    }

    // ── run_local_fsck ──────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn run_local_fsck_with_temp_dir_returns_clean() {
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path().to_path_buf();
        let report = run_local_fsck(root).await.unwrap();
        assert!(report.is_clean());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_local_fsck_with_non_existent_dir_errors() {
        let root = PathBuf::from("/nonexistent/fsck-test-dir-12345");
        let result = run_local_fsck(root).await;
        assert!(result.is_err());
    }

    // ── run_fsck_with_stores with LocalRecordStore ──────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn run_fsck_with_memory_index_store_returns_clean() {
        use shardline_index::MemoryIndexStore;
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path().to_path_buf();
        let object_root = root.join("chunks");
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();
        let record_store = shardline_index::LocalRecordStore::open(root.clone());
        let index_store = MemoryIndexStore::new();

        let report = run_fsck_with_stores(
            &record_store,
            &index_store,
            &object_root,
            &object_store,
            shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        )
        .await
        .unwrap();

        assert!(report.is_clean());
        assert_eq!(report.latest_records, 0);
        assert_eq!(report.version_records, 0);
    }

    // ── record_path ─────────────────────────────────────────────────

    #[test]
    fn record_path_latest_returns_latest_record_locator() {
        use shardline_index::RecordTraversal;
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root);
        let record = FileRecord {
            file_id: "test-file".to_owned(),
            content_hash: "a".repeat(64),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: None,
            chunks: Vec::new(),
        };
        let locator = record_path(&record_store, RecordKind::Latest, &record);
        let expected = record_store.latest_record_locator(&record);
        assert_eq!(locator, expected);
    }

    #[test]
    fn record_path_version_returns_version_record_locator() {
        use shardline_index::RecordTraversal;
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root);
        let record = FileRecord {
            file_id: "test-file".to_owned(),
            content_hash: "b".repeat(64),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: None,
            chunks: Vec::new(),
        };
        let locator = record_path(&record_store, RecordKind::Version, &record);
        let expected = record_store.version_record_locator(&record);
        assert_eq!(locator, expected);
    }

    // ── FsckIssue Debug ─────────────────────────────────────────────

    #[test]
    fn fsck_issue_debug_format() {
        let issue = FsckIssue {
            kind: FsckIssueKind::MissingChunk,
            location: "test/location".to_owned(),
            detail: FsckIssueDetail::RecordJsonInvalid,
        };
        let debug = format!("{issue:?}");
        assert!(debug.contains("MissingChunk"), "debug: {debug}");
        assert!(debug.contains("test/location"), "debug: {debug}");
    }

    #[test]
    fn fsck_issue_debug_with_detailed_variant() {
        let issue = FsckIssue {
            kind: FsckIssueKind::ChunkHashMismatch,
            location: "chunk/loc".to_owned(),
            detail: FsckIssueDetail::HashMismatch {
                expected_hash: "abc".to_owned(),
                observed_hash: "def".to_owned(),
            },
        };
        let debug = format!("{issue:?}");
        assert!(debug.contains("ChunkHashMismatch"), "debug: {debug}");
        assert!(debug.contains("abc"), "debug: {debug}");
    }

    // ── Additional FsckIssueDetail Display variants ─────────────────

    #[test]
    fn fsck_issue_detail_display_invalid_quarantine_timeline() {
        let detail = FsckIssueDetail::InvalidQuarantineTimeline {
            delete_after_unix_seconds: 10,
            first_seen_unreachable_at_unix_seconds: 100,
        };
        let msg = detail.to_string();
        assert!(msg.contains("10"), "msg: {msg}");
        assert!(msg.contains("100"), "msg: {msg}");
    }

    #[test]
    fn fsck_issue_detail_display_invalid_retention_timeline() {
        let detail = FsckIssueDetail::InvalidRetentionTimeline {
            release_after_unix_seconds: 50,
            held_at_unix_seconds: 100,
        };
        let msg = detail.to_string();
        assert!(msg.contains("50"), "msg: {msg}");
        assert!(msg.contains("100"), "msg: {msg}");
    }

    #[test]
    fn fsck_issue_detail_display_active_retention_hold_reason() {
        let detail = FsckIssueDetail::ActiveRetentionHoldReason {
            reason: "legal hold".to_owned(),
        };
        let msg = detail.to_string();
        assert!(msg.contains("legal hold"), "msg: {msg}");
    }

    #[test]
    fn fsck_issue_detail_display_active_retention_hold_quarantined() {
        let detail = FsckIssueDetail::ActiveRetentionHoldQuarantined;
        let msg = detail.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn fsck_issue_detail_display_webhook_delivery_timestamp_exceeded() {
        let detail = FsckIssueDetail::WebhookDeliveryTimestampExceeded {
            processed_at_unix_seconds: 2000,
            max_allowed_unix_seconds: 1000,
        };
        let msg = detail.to_string();
        assert!(msg.contains("2000"), "msg: {msg}");
    }

    #[test]
    fn fsck_issue_detail_display_provider_repository_identity_invalid() {
        let detail = FsckIssueDetail::ProviderRepositoryIdentityInvalid;
        let msg = detail.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn fsck_issue_detail_display_provider_state_timestamp_exceeded() {
        let detail = FsckIssueDetail::ProviderRepositoryStateTimestampExceeded {
            field: ProviderRepositoryStateTimestampField::LastAccessChangedAtUnixSeconds,
            timestamp: 2000,
            max_allowed_unix_seconds: 1000,
        };
        let msg = detail.to_string();
        assert!(msg.contains("2000"), "msg: {msg}");
        assert!(
            msg.contains("last_access_changed_at_unix_seconds"),
            "msg: {msg}"
        );
    }

    #[test]
    fn fsck_issue_detail_display_quarantine_referenced_missing_object() {
        let detail = FsckIssueDetail::QuarantineReferencedMissingObject;
        let msg = detail.to_string();
        assert!(msg.contains("missing object"), "msg: {msg}");
    }

    #[test]
    fn fsck_issue_detail_display_quarantine_targeted_reachable_object() {
        let detail = FsckIssueDetail::QuarantineTargetedReachableObject;
        let msg = detail.to_string();
        assert!(msg.contains("reachable"), "msg: {msg}");
    }

    // ── Extra FsckError Display coverage ─────────────────────────────

    #[test]
    fn fsck_error_display_numeric_conversion() {
        let err = FsckError::NumericConversion(u64::try_from(-1i32).unwrap_err());
        let msg = err.to_string();
        assert!(msg.contains("numeric conversion"), "msg: {msg}");
    }

    #[test]
    fn fsck_error_display_local_object_store() {
        use shardline_storage::LocalObjectStoreError;
        let err = FsckError::LocalObjectStore(LocalObjectStoreError::Io(std::io::Error::other(
            "disk error",
        )));
        let msg = err.to_string();
        assert!(msg.contains("local storage"), "msg: {msg}");
    }

    #[test]
    fn fsck_error_display_s3_object_store() {
        use shardline_storage::S3ObjectStoreError;
        let err =
            FsckError::S3ObjectStore(S3ObjectStoreError::IncompleteCredentials);
        let msg = err.to_string();
        assert!(msg.contains("s3 object"), "msg: {msg}");
    }

    #[test]
    fn fsck_error_display_xet_adapter() {
        use shardline_xet_adapter::XetAdapterError;
        let err = FsckError::XetAdapter(XetAdapterError::NotFound);
        let msg = err.to_string();
        assert!(msg.contains("xet adapter"), "msg: {msg}");
    }

    #[test]
    fn fsck_error_display_memory_index_store() {
        use shardline_index::MemoryIndexStoreError;
        let err = FsckError::MemoryIndexStore(MemoryIndexStoreError::LockPoisoned);
        let msg = err.to_string();
        assert!(msg.contains("memory index"), "msg: {msg}");
    }

    #[test]
    fn fsck_error_display_memory_record_store() {
        use shardline_index::MemoryRecordStoreError;
        let err = FsckError::MemoryRecordStore(MemoryRecordStoreError::RecordNotFound);
        let msg = err.to_string();
        assert!(msg.contains("memory record"), "msg: {msg}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_fsck_with_missing_reconstruction_detected() {
        use shardline_index::{
            FileId, FileReconstruction, ReconstructionTerm, StoredObjectId,
        };
        use shardline_protocol::ChunkRange;
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path().to_path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root.clone());
        let index_store = shardline_index::MemoryIndexStore::new();
        let object_root = root.join("chunks");
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

        // Insert a reconstruction that references a missing xorb
        let hash = shardline_protocol::ShardlineHash::from_bytes([1; 32]);
        let file_id = FileId::new(hash);
        let xorb_hash = shardline_protocol::ShardlineHash::from_bytes([2; 32]);
        let object_id = StoredObjectId::new(xorb_hash);
        let chunk_range = ChunkRange::new(0, 1).unwrap();
        let term = ReconstructionTerm::new(object_id, chunk_range, 100);
        let reconstruction = FileReconstruction::new(vec![term]);
        index_store.insert_reconstruction(&file_id, &reconstruction).unwrap();

        let report = run_fsck_with_stores(
            &record_store,
            &index_store,
            &object_root,
            &object_store,
            shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        )
        .await
        .unwrap();

        assert!(!report.is_clean());
        assert!(report
            .issues
            .iter()
            .any(|i| i.kind == FsckIssueKind::MissingReconstructionXorb));
    }

    // ── run_fsck_with_invalid_dedupe_shard_mapping (hash not in shard) ─

    #[tokio::test(flavor = "multi_thread")]
    async fn run_fsck_with_invalid_dedupe_shard_mapping_detected() {
        use shardline_index::{DedupeShardMapping, MemoryIndexStore};
        use shardline_protocol::ShardlineHash;
        use shardline_storage::ObjectKey;
        use shardline_xet_core::{
            merklehash::{compute_data_hash, file_hash, xorb_hash},
            metadata_shard::{
                file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
                shard_in_memory::MDBInMemoryShard,
                xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
            },
        };

        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path().to_path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root.clone());
        let index_store = MemoryIndexStore::new();
        let object_root = root.join("chunks");

        // Build a valid minimal shard with one global-dedup chunk
        let chunk_data = b"valid shard chunk content";
        let chunk_hash = compute_data_hash(chunk_data);
        let xorb_val = xorb_hash(&[(chunk_hash, chunk_data.len() as u64)]);
        let file_hash_val = file_hash(&[(chunk_hash, chunk_data.len() as u64)]);
        let mut shard = MDBInMemoryShard::default();
        shard
            .add_file_reconstruction_info(MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash_val, 1_usize, false, false),
                segments: vec![FileDataSequenceEntry::new(
                    xorb_val,
                    chunk_data.len() as u32,
                    0_u32,
                    1_u32,
                )],
                verification: Vec::new(),
                metadata_ext: None,
            })
            .unwrap();
        shard
            .add_xorb_block(MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_val, 1_u32, chunk_data.len() as u32),
                chunks: vec![XorbChunkSequenceEntry::new(
                    chunk_hash,
                    chunk_data.len() as u32,
                    0_u32,
                )
                .with_global_dedup_flag(true)],
            })
            .unwrap();
        let shard_bytes = shard.to_bytes().unwrap();
        let shard_chunk_hash_hex = chunk_hash.hex();

        // Store the shard in object storage
        let shard_key_str = format!("shards/{}/{}", &shard_chunk_hash_hex[..2], shard_chunk_hash_hex);
        let shard_key = ObjectKey::parse(&shard_key_str).unwrap();
        let shard_path = object_root.join(shard_key.as_str());
        if let Some(parent) = shard_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&shard_path, &shard_bytes).unwrap();

        // Re-create the object store after writing
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

        // Insert a dedupe mapping for a DIFFERENT chunk hash (not in the shard)
        let missing_chunk_hash = ShardlineHash::from_bytes([99; 32]);
        let mapping = DedupeShardMapping::new(missing_chunk_hash, shard_key);
        index_store.upsert_dedupe_shard_mapping(&mapping).unwrap();

        let report = run_fsck_with_stores(
            &record_store,
            &index_store,
            &object_root,
            &object_store,
            shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        )
        .await
        .unwrap();

        assert!(!report.is_clean(), "expected issues for invalid dedupe shard mapping");
        assert_eq!(report.inspected_dedupe_shard_mappings, 1);
        assert!(
            report.issues.iter().any(|i| i.kind == FsckIssueKind::InvalidDedupeShardMapping),
            "expected InvalidDedupeShardMapping, got: {:#?}",
            report.issues
        );
    }

    // ── run_fsck_with_dedupe_shard_added_to_reachability ─────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn run_fsck_with_dedupe_shard_added_to_reachability() {
        use shardline_index::{DedupeShardMapping, MemoryIndexStore, RecordMutation};
        use shardline_storage::ObjectKey;
        use shardline_xet_core::{
            merklehash::{compute_data_hash, file_hash, xorb_hash},
            metadata_shard::{
                file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
                shard_in_memory::MDBInMemoryShard,
                xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
            },
        };

        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path().to_path_buf();
        let record_store = shardline_index::LocalRecordStore::open(root.clone());
        let index_store = MemoryIndexStore::new();
        let object_root = root.join("chunks");
        let object_store = ServerObjectStore::local(object_root.clone()).unwrap();

        // Build a valid minimal shard
        let chunk_data = b"shard data for reachability test";
        let chunk_hash = compute_data_hash(chunk_data);
        let xorb_val = xorb_hash(&[(chunk_hash, chunk_data.len() as u64)]);
        let file_hash_val = file_hash(&[(chunk_hash, chunk_data.len() as u64)]);
        let mut shard = MDBInMemoryShard::default();
        shard
            .add_file_reconstruction_info(MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash_val, 1_usize, false, false),
                segments: vec![FileDataSequenceEntry::new(
                    xorb_val,
                    chunk_data.len() as u32,
                    0_u32,
                    1_u32,
                )],
                verification: Vec::new(),
                metadata_ext: None,
            })
            .unwrap();
        shard
            .add_xorb_block(MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_val, 1_u32, chunk_data.len() as u32),
                chunks: vec![XorbChunkSequenceEntry::new(
                    chunk_hash,
                    chunk_data.len() as u32,
                    0_u32,
                )
                .with_global_dedup_flag(true)],
            })
            .unwrap();
        let shard_bytes = shard.to_bytes().unwrap();
        let chunk_hash_hex = chunk_hash.hex();

        // Store the shard in object storage
        let shard_key_str = format!("shards/{}/{}", &chunk_hash_hex[..2], chunk_hash_hex);
        let shard_key = ObjectKey::parse(&shard_key_str).unwrap();
        let shard_path = object_root.join(shard_key.as_str());
        if let Some(parent) = shard_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&shard_path, &shard_bytes).unwrap();

        // Write a latest record whose chunk hash matches the shard's chunk hash
        // so live_dedupe_chunk_hashes contains it.
        let chunks = vec![shardline_index::FileChunkRecord {
            hash: chunk_hash_hex.clone(),
            offset: 0,
            length: chunk_data.len() as u64,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: chunk_data.len() as u64,
        }];
        let total_bytes = chunk_data.len() as u64;
        let chunk_size = 4096_u64;
        let content_hash = shardline_server_core::content_hash(total_bytes, chunk_size, &chunks);
        let record = shardline_index::FileRecord {
            file_id: "reachable-file".to_owned(),
            content_hash,
            total_bytes,
            chunk_size,
            repository_scope: None,
            chunks,
        };
        record_store.write_latest_record(&record).await.unwrap();

        // Also need to write the chunk object so the latest record scans cleanly
        let chunk_key = shardline_server_core::chunk_object_key(&chunk_hash_hex).unwrap();
        let chunk_path = object_root.join(chunk_key.as_str());
        if let Some(parent) = chunk_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&chunk_path, chunk_data).unwrap();

        // Insert a dedupe mapping matching the same chunk hash
        let shardline_chunk_hash = shardline_index::parse_xet_hash_hex(&chunk_hash_hex).unwrap();
        let mapping = DedupeShardMapping::new(shardline_chunk_hash, shard_key);
        index_store.upsert_dedupe_shard_mapping(&mapping).unwrap();

        let report = run_fsck_with_stores(
            &record_store,
            &index_store,
            &object_root,
            &object_store,
            shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS,
        )
        .await
        .unwrap();

        // The dedupe shard should be cleanly scanned and the mapping valid
        assert_eq!(report.inspected_dedupe_shard_mappings, 1);
        // No InvalidDedupeShardMapping issues
        assert!(
            !report.issues.iter().any(|i| i.kind == FsckIssueKind::InvalidDedupeShardMapping),
            "unexpected InvalidDedupeShardMapping: {:#?}",
            report.issues
        );
        // Should have MissingVersionRecord since no version was written
        assert!(
            report.issues.iter().any(|i| i.kind == FsckIssueKind::MissingVersionRecord),
            "expected MissingVersionRecord"
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordKind {
    Latest,
    Version,
}

pub(crate) struct FsckObjectContext<'operation> {
    pub(crate) object_root: &'operation Path,
    pub(crate) object_store: &'operation ServerObjectStore,
}

#[derive(Debug, Default)]
pub(crate) struct FsckReachability {
    pub(crate) referenced_object_keys: HashSet<String>,
    pub(crate) live_dedupe_chunk_hashes: HashSet<String>,
}

pub(crate) struct PendingVersionRecordCheck<Locator> {
    pub(crate) latest_locator: Locator,
    pub(crate) version_locator: Locator,
    pub(crate) latest_record: FileRecord,
}

impl RecordKind {
    pub(crate) const fn ops(self) -> OpsRecordKind {
        match self {
            Self::Latest => OpsRecordKind::Latest,
            Self::Version => OpsRecordKind::Version,
        }
    }
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

const fn reconstruction_plan_error_detail(
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

fn object_key_storage_path(object_root: &Path, object_key: &ObjectKey) -> PathBuf {
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
