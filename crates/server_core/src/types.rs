use std::num::NonZeroUsize;

use thiserror::Error;

/// Lifecycle metadata consistency failure.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum InvalidLifecycleMetadataError {
    /// A quarantine candidate cannot be deleted before it was first observed.
    #[error(
        "quarantine candidate for {object_key} had delete-after {delete_after_unix_seconds} before first-seen {first_seen_unreachable_at_unix_seconds}"
    )]
    QuarantineCandidateDeleteBeforeFirstSeen {
        /// Quarantined object key.
        object_key: String,
        /// Candidate deletion timestamp.
        delete_after_unix_seconds: u64,
        /// First observed unreachable timestamp.
        first_seen_unreachable_at_unix_seconds: u64,
    },
    /// A quarantine candidate referenced an object that is no longer present.
    #[error("quarantine candidate referenced missing object {object_key}")]
    QuarantineCandidateMissingObject {
        /// Quarantined object key.
        object_key: String,
    },
    /// A quarantine candidate recorded a length that differs from object-store metadata.
    #[error(
        "quarantine candidate for {object_key} expected length {expected_length}, got {observed_length}"
    )]
    QuarantineCandidateLengthMismatch {
        /// Quarantined object key.
        object_key: String,
        /// Length recorded in quarantine metadata.
        expected_length: u64,
        /// Length observed in object-store metadata.
        observed_length: u64,
    },
    /// A retention hold cannot be released before it was created.
    #[error(
        "retention hold for {object_key} had release-after {release_after_unix_seconds} before held-at {held_at_unix_seconds}"
    )]
    RetentionHoldReleaseBeforeHeld {
        /// Held object key.
        object_key: String,
        /// Hold release timestamp.
        release_after_unix_seconds: u64,
        /// Hold creation timestamp.
        held_at_unix_seconds: u64,
    },
    /// An active retention hold referenced an object that is no longer present.
    #[error("active retention hold referenced missing object {object_key}")]
    ActiveRetentionHoldMissingObject {
        /// Held object key.
        object_key: String,
    },
    /// An active retention hold coexisted with quarantine metadata for the same object.
    #[error("active retention hold for {object_key} coexisted with quarantine state")]
    ActiveRetentionHoldQuarantined {
        /// Held object key.
        object_key: String,
    },
}

/// Serialized shard validation failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum InvalidSerializedShardError {
    /// The external shard parser rejected the bytes.
    #[error("shard parser rejected metadata")]
    ParserRejectedMetadata,
    /// A native Xet term used an empty or inverted chunk range.
    #[error("native xet term had an empty or inverted chunk range")]
    NativeXetTermEmptyOrInvertedChunkRange,
    /// A native Xet term referenced chunks past the end of its xorb.
    #[error("native xet term range exceeded xorb chunk count")]
    NativeXetTermRangeExceededXorbChunkCount,
    /// A shard file term used an empty or inverted chunk range.
    #[error("shard file term had an empty or inverted chunk range")]
    ShardFileTermEmptyOrInvertedChunkRange,
    /// The transient xorb metadata cache could not return a just-inserted entry.
    #[error("xorb metadata cache insertion failed")]
    XorbMetadataCacheInsertionFailed,
    /// A shard term started past the referenced xorb chunk list.
    #[error("shard term chunk range started past the xorb chunk list")]
    ShardTermRangeStartedPastXorbChunkList,
    /// A shard term ended past the referenced xorb chunk list.
    #[error("shard term chunk range ended past the xorb chunk list")]
    ShardTermRangeEndedPastXorbChunkList,
    /// The retained shard chunk hash list was not strictly ordered.
    #[error("retained shard chunk hashes were not strictly ordered")]
    RetainedShardChunkHashesNotStrictlyOrdered,
}

/// Reconstruction response shape failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum InvalidReconstructionResponseError {
    /// A guarded test record store detected a forbidden global latest-record walk.
    #[error("global latest-record walk attempted")]
    RecordStoreGlobalLatestWalkAttempted,
    /// A guarded test record store could not find the requested record.
    #[error("record not found")]
    RecordStoreRecordNotFound,
    /// V1 response emitted more terms than the source record has chunks.
    #[error("response term count exceeded record chunk count")]
    TermCountExceededRecordChunkCount,
    /// A response term had no bytes.
    #[error("response term had zero unpacked length")]
    TermHadZeroUnpackedLength,
    /// A response term contained an empty chunk range.
    #[error("response term had an empty chunk range")]
    TermHadEmptyChunkRange,
    /// A response term did not have matching fetch metadata.
    #[error("response term did not have matching fetch info")]
    TermMissingFetchInfo,
    /// A fetch-info entry had no fetches.
    #[error("response fetch info contained an empty fetch list")]
    EmptyFetchList,
    /// A fetch URL did not point to the xorb hash that owns it.
    #[error("response fetch URL did not match its xorb hash")]
    FetchUrlHashMismatch,
    /// A fetch entry had an empty chunk range.
    #[error("response fetch entry had an empty chunk range")]
    FetchEntryEmptyChunkRange,
    /// A fetch entry had an inverted byte range.
    #[error("response fetch entry had an inverted byte range")]
    FetchEntryInvertedByteRange,
    /// A fetch entry did not correspond to any response term.
    #[error("response fetch entry did not have a matching term")]
    FetchEntryMissingTerm,
    /// V2 conversion changed `offset_into_first_range`.
    #[error("v2 response changed offset_into_first_range")]
    V2ChangedOffsetIntoFirstRange,
    /// V2 conversion changed the reconstruction terms.
    #[error("v2 response changed reconstruction terms")]
    V2ChangedTerms,
    /// V2 conversion changed the xorb fetch-info cardinality.
    #[error("v2 response changed xorb fetch-info cardinality")]
    V2ChangedXorbFetchInfoCardinality,
    /// V2 conversion emitted a hash absent from V1 fetch-info.
    #[error("v2 response emitted a fetch hash absent from v1")]
    V2FetchHashAbsentFromV1,
    /// V2 conversion emitted an empty fetch list.
    #[error("v2 response emitted an empty fetch list")]
    V2EmptyFetchList,
    /// V2 conversion emitted a fetch entry without ranges.
    #[error("v2 response emitted a fetch entry without ranges")]
    V2FetchEntryWithoutRanges,
    /// V2 conversion emitted an empty chunk range.
    #[error("v2 response emitted an empty chunk range")]
    V2EmptyChunkRange,
    /// V2 conversion emitted an inverted byte range.
    #[error("v2 response emitted an inverted byte range")]
    V2InvertedByteRange,
    /// V2 fetch count did not match V1.
    #[error("v2 response fetch count disagreed with v1")]
    V2FetchCountDisagreedWithV1,
    /// V2 range count did not match V1.
    #[error("v2 response range count disagreed with v1")]
    V2RangeCountDisagreedWithV1,
}

/// Default bounded-parser limits for native Xet shard metadata.
pub const DEFAULT_MAX_SHARD_FILES: NonZeroUsize = match NonZeroUsize::new(16_384) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

/// Default maximum shard xorb sections.
pub const DEFAULT_MAX_SHARD_XORBS: NonZeroUsize = match NonZeroUsize::new(16_384) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

/// Default maximum shard reconstruction terms.
pub const DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS: NonZeroUsize = match NonZeroUsize::new(65_536) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

/// Default maximum shard xorb chunk records.
pub const DEFAULT_MAX_SHARD_XORB_CHUNKS: NonZeroUsize = match NonZeroUsize::new(65_536) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

/// Default bounded-parser limits for native Xet shard metadata.
pub const DEFAULT_SHARD_METADATA_LIMITS: ShardMetadataLimits = ShardMetadataLimits::new(
    DEFAULT_MAX_SHARD_FILES,
    DEFAULT_MAX_SHARD_XORBS,
    DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS,
    DEFAULT_MAX_SHARD_XORB_CHUNKS,
);

/// Bounded-parser limits for native Xet shard metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardMetadataLimits {
    max_files: NonZeroUsize,
    max_xorbs: NonZeroUsize,
    max_reconstruction_terms: NonZeroUsize,
    max_xorb_chunks: NonZeroUsize,
}

impl ShardMetadataLimits {
    /// Creates native Xet shard metadata limits.
    #[must_use]
    pub const fn new(
        max_files: NonZeroUsize,
        max_xorbs: NonZeroUsize,
        max_reconstruction_terms: NonZeroUsize,
        max_xorb_chunks: NonZeroUsize,
    ) -> Self {
        Self {
            max_files,
            max_xorbs,
            max_reconstruction_terms,
            max_xorb_chunks,
        }
    }

    /// Returns the maximum file sections accepted in one uploaded shard.
    #[must_use]
    pub const fn max_files(self) -> NonZeroUsize {
        self.max_files
    }

    /// Returns the maximum xorb sections accepted in one uploaded shard.
    #[must_use]
    pub const fn max_xorbs(self) -> NonZeroUsize {
        self.max_xorbs
    }

    /// Returns the maximum file reconstruction terms accepted in one uploaded shard.
    #[must_use]
    pub const fn max_reconstruction_terms(self) -> NonZeroUsize {
        self.max_reconstruction_terms
    }

    /// Returns the maximum xorb chunk records accepted in one uploaded shard.
    #[must_use]
    pub const fn max_xorb_chunks(self) -> NonZeroUsize {
        self.max_xorb_chunks
    }
}

impl Default for ShardMetadataLimits {
    fn default() -> Self {
        DEFAULT_SHARD_METADATA_LIMITS
    }
}

/// Default retention window for new local quarantine candidates.
pub const DEFAULT_LOCAL_GC_RETENTION_SECONDS: u64 = 86_400;
