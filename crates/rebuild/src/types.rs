use shardline_server_core::InvalidSerializedShardError;
use thiserror::Error;

/// One index-rebuild issue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRebuildIssue {
    /// Problem classification.
    pub kind: IndexRebuildIssueKind,
    /// Stable record location associated with the issue.
    pub location: String,
    /// Structured detail for operators.
    pub detail: IndexRebuildIssueDetail,
}

/// Index-rebuild issue detail.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum IndexRebuildIssueDetail {
    /// Version-record metadata exceeded the bounded parser ceiling.
    #[error("record metadata exceeded the bounded parser ceiling")]
    OversizedVersionRecordMetadata,
    /// Version-record JSON was invalid.
    #[error("record json was invalid")]
    RecordJsonInvalid,
    /// The record file identifier was invalid.
    #[error("record file_id `{file_id}` is invalid")]
    InvalidFileId {
        /// Invalid file identifier.
        file_id: String,
    },
    /// The record content hash was invalid.
    #[error("record content hash `{content_hash}` is invalid")]
    InvalidContentHash {
        /// Invalid content hash.
        content_hash: String,
    },
    /// The repository scope failed validation.
    #[error("record repository scope is invalid")]
    InvalidRepositoryScope,
    /// The version record was stored at an unexpected location.
    #[error("expected version record at {expected_locator}")]
    VersionPathMismatch {
        /// Expected version-record locator.
        expected_locator: String,
    },
    /// The record reconstruction plan was invalid.
    #[error("{0}")]
    InvalidReconstructionPlan(IndexRebuildReconstructionPlanDetail),
    /// The retained shard was invalid.
    #[error("{0}")]
    InvalidRetainedShard(InvalidSerializedShardError),
}

/// Index-rebuild reconstruction-plan issue detail.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum IndexRebuildReconstructionPlanDetail {
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

/// Index-rebuild issue kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexRebuildIssueKind {
    /// Version-record metadata exceeded the bounded parser ceiling.
    OversizedVersionRecordMetadata,
    /// Version-record bytes were not valid JSON.
    InvalidVersionRecordJson,
    /// Version-record file identifier was invalid.
    InvalidVersionFileId,
    /// Version-record content hash was invalid.
    InvalidVersionContentHash,
    /// Version-record repository scope was invalid.
    InvalidVersionRepositoryScope,
    /// Version record was stored at an unexpected path.
    VersionPathMismatch,
    /// Version record could not produce a valid reconstruction plan.
    InvalidVersionReconstructionPlan,
    /// Retained shard object could not be parsed as a native Xet shard.
    InvalidRetainedShard,
}

impl IndexRebuildIssueKind {
    /// Stable issue label for CLI and logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OversizedVersionRecordMetadata => "oversized_version_record_metadata",
            Self::InvalidVersionRecordJson => "invalid_version_record_json",
            Self::InvalidVersionFileId => "invalid_version_file_id",
            Self::InvalidVersionContentHash => "invalid_version_content_hash",
            Self::InvalidVersionRepositoryScope => "invalid_version_repository_scope",
            Self::VersionPathMismatch => "version_path_mismatch",
            Self::InvalidVersionReconstructionPlan => "invalid_version_reconstruction_plan",
            Self::InvalidRetainedShard => "invalid_retained_shard",
        }
    }
}

/// Backward-compatible local index-rebuild report alias.
pub type LocalIndexRebuildReport = crate::report::IndexRebuildReport;

/// Backward-compatible local index-rebuild issue alias.
pub type LocalIndexRebuildIssue = IndexRebuildIssue;

/// Backward-compatible local index-rebuild issue-kind alias.
pub type LocalIndexRebuildIssueKind = IndexRebuildIssueKind;
