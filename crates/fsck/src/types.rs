use std::{
    collections::HashSet,
    fmt::{Display, Formatter, Result as FmtResult},
    path::Path,
};

use shardline_index::FileRecord;
use shardline_server_core::{InvalidSerializedShardError, OpsRecordKind, ServerObjectStore};
use thiserror::Error;

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
    pub(crate) const fn as_str(self) -> &'static str {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordKind {
    Latest,
    Version,
}

impl RecordKind {
    pub(crate) const fn ops(self) -> OpsRecordKind {
        match self {
            Self::Latest => OpsRecordKind::Latest,
            Self::Version => OpsRecordKind::Version,
        }
    }
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
