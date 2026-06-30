#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
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
use shardline_storage::ObjectKey;
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
