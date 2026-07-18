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
    /// Maximum number of file sections accepted in one uploaded shard.
    max_files: NonZeroUsize,
    /// Maximum number of xorb sections accepted in one uploaded shard.
    max_xorbs: NonZeroUsize,
    /// Maximum number of file reconstruction terms accepted in one uploaded shard.
    max_reconstruction_terms: NonZeroUsize,
    /// Maximum number of xorb chunk records accepted in one uploaded shard.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_metadata_limits_new_and_accessors() {
        let limits = ShardMetadataLimits::new(
            NonZeroUsize::new(10).unwrap(),
            NonZeroUsize::new(20).unwrap(),
            NonZeroUsize::new(30).unwrap(),
            NonZeroUsize::new(40).unwrap(),
        );
        assert_eq!(limits.max_files().get(), 10);
        assert_eq!(limits.max_xorbs().get(), 20);
        assert_eq!(limits.max_reconstruction_terms().get(), 30);
        assert_eq!(limits.max_xorb_chunks().get(), 40);
    }

    #[test]
    fn shard_metadata_limits_default_matches_const() {
        assert_eq!(
            ShardMetadataLimits::default(),
            DEFAULT_SHARD_METADATA_LIMITS
        );
    }

    #[test]
    fn invalid_lifecycle_metadata_display_delete_before_first_seen() {
        let err = InvalidLifecycleMetadataError::QuarantineCandidateDeleteBeforeFirstSeen {
            object_key: "obj".to_owned(),
            delete_after_unix_seconds: 100,
            first_seen_unreachable_at_unix_seconds: 200,
        };
        let msg = err.to_string();
        assert!(msg.contains("obj"));
        assert!(msg.contains("delete-after"));
    }

    #[test]
    fn invalid_lifecycle_metadata_display_missing_object() {
        let err = InvalidLifecycleMetadataError::QuarantineCandidateMissingObject {
            object_key: "obj".to_owned(),
        };
        let msg = err.to_string();
        assert!(msg.contains("obj"));
    }

    #[test]
    fn invalid_lifecycle_metadata_display_length_mismatch() {
        let err = InvalidLifecycleMetadataError::QuarantineCandidateLengthMismatch {
            object_key: "obj".to_owned(),
            expected_length: 100,
            observed_length: 200,
        };
        let msg = err.to_string();
        assert!(msg.contains("expected length"));
    }

    #[test]
    fn invalid_lifecycle_metadata_display_release_before_held() {
        let err = InvalidLifecycleMetadataError::RetentionHoldReleaseBeforeHeld {
            object_key: "obj".to_owned(),
            release_after_unix_seconds: 50,
            held_at_unix_seconds: 100,
        };
        let msg = err.to_string();
        assert!(msg.contains("release-after"));
    }

    #[test]
    fn invalid_lifecycle_metadata_display_hold_missing_object() {
        let err = InvalidLifecycleMetadataError::ActiveRetentionHoldMissingObject {
            object_key: "obj".to_owned(),
        };
        let msg = err.to_string();
        assert!(msg.contains("missing object"));
    }

    #[test]
    fn invalid_lifecycle_metadata_display_hold_quarantined() {
        let err = InvalidLifecycleMetadataError::ActiveRetentionHoldQuarantined {
            object_key: "obj".to_owned(),
        };
        let msg = err.to_string();
        assert!(msg.contains("coexisted with quarantine"));
    }

    #[test]
    fn invalid_serialized_shard_error_parser_rejected() {
        assert_eq!(
            InvalidSerializedShardError::ParserRejectedMetadata.to_string(),
            "shard parser rejected metadata"
        );
    }

    #[test]
    fn invalid_serialized_shard_error_native_xet_empty_range() {
        assert_eq!(
            InvalidSerializedShardError::NativeXetTermEmptyOrInvertedChunkRange.to_string(),
            "native xet term had an empty or inverted chunk range"
        );
    }

    #[test]
    fn invalid_serialized_shard_error_native_xet_range_exceeded() {
        assert_eq!(
            InvalidSerializedShardError::NativeXetTermRangeExceededXorbChunkCount.to_string(),
            "native xet term range exceeded xorb chunk count"
        );
    }

    #[test]
    fn invalid_serialized_shard_error_shard_file_term_empty_range() {
        assert_eq!(
            InvalidSerializedShardError::ShardFileTermEmptyOrInvertedChunkRange.to_string(),
            "shard file term had an empty or inverted chunk range"
        );
    }

    #[test]
    fn invalid_serialized_shard_error_xorb_cache_insertion_failed() {
        assert_eq!(
            InvalidSerializedShardError::XorbMetadataCacheInsertionFailed.to_string(),
            "xorb metadata cache insertion failed"
        );
    }

    #[test]
    fn invalid_serialized_shard_error_shard_term_started_past() {
        assert_eq!(
            InvalidSerializedShardError::ShardTermRangeStartedPastXorbChunkList.to_string(),
            "shard term chunk range started past the xorb chunk list"
        );
    }

    #[test]
    fn invalid_serialized_shard_error_shard_term_ended_past() {
        assert_eq!(
            InvalidSerializedShardError::ShardTermRangeEndedPastXorbChunkList.to_string(),
            "shard term chunk range ended past the xorb chunk list"
        );
    }

    #[test]
    fn invalid_serialized_shard_error_retained_hashes_not_ordered() {
        assert_eq!(
            InvalidSerializedShardError::RetainedShardChunkHashesNotStrictlyOrdered.to_string(),
            "retained shard chunk hashes were not strictly ordered"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_record_store_global_latest() {
        assert_eq!(
            InvalidReconstructionResponseError::RecordStoreGlobalLatestWalkAttempted.to_string(),
            "global latest-record walk attempted"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_record_not_found() {
        assert_eq!(
            InvalidReconstructionResponseError::RecordStoreRecordNotFound.to_string(),
            "record not found"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_term_count_exceeded() {
        assert_eq!(
            InvalidReconstructionResponseError::TermCountExceededRecordChunkCount.to_string(),
            "response term count exceeded record chunk count"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_term_zero_unpacked() {
        assert_eq!(
            InvalidReconstructionResponseError::TermHadZeroUnpackedLength.to_string(),
            "response term had zero unpacked length"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_term_empty_chunk_range() {
        assert_eq!(
            InvalidReconstructionResponseError::TermHadEmptyChunkRange.to_string(),
            "response term had an empty chunk range"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_term_missing_fetch_info() {
        assert_eq!(
            InvalidReconstructionResponseError::TermMissingFetchInfo.to_string(),
            "response term did not have matching fetch info"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_empty_fetch_list() {
        assert_eq!(
            InvalidReconstructionResponseError::EmptyFetchList.to_string(),
            "response fetch info contained an empty fetch list"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_fetch_url_hash_mismatch() {
        assert_eq!(
            InvalidReconstructionResponseError::FetchUrlHashMismatch.to_string(),
            "response fetch URL did not match its xorb hash"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_fetch_entry_empty_chunk_range() {
        assert_eq!(
            InvalidReconstructionResponseError::FetchEntryEmptyChunkRange.to_string(),
            "response fetch entry had an empty chunk range"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_fetch_entry_inverted_byte_range() {
        assert_eq!(
            InvalidReconstructionResponseError::FetchEntryInvertedByteRange.to_string(),
            "response fetch entry had an inverted byte range"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_fetch_entry_missing_term() {
        assert_eq!(
            InvalidReconstructionResponseError::FetchEntryMissingTerm.to_string(),
            "response fetch entry did not have a matching term"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_changed_offset() {
        assert_eq!(
            InvalidReconstructionResponseError::V2ChangedOffsetIntoFirstRange.to_string(),
            "v2 response changed offset_into_first_range"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_changed_terms() {
        assert_eq!(
            InvalidReconstructionResponseError::V2ChangedTerms.to_string(),
            "v2 response changed reconstruction terms"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_changed_xorb_cardinality() {
        assert_eq!(
            InvalidReconstructionResponseError::V2ChangedXorbFetchInfoCardinality.to_string(),
            "v2 response changed xorb fetch-info cardinality"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_hash_absent_from_v1() {
        assert_eq!(
            InvalidReconstructionResponseError::V2FetchHashAbsentFromV1.to_string(),
            "v2 response emitted a fetch hash absent from v1"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_empty_fetch_list() {
        assert_eq!(
            InvalidReconstructionResponseError::V2EmptyFetchList.to_string(),
            "v2 response emitted an empty fetch list"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_fetch_entry_without_ranges() {
        assert_eq!(
            InvalidReconstructionResponseError::V2FetchEntryWithoutRanges.to_string(),
            "v2 response emitted a fetch entry without ranges"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_empty_chunk_range() {
        assert_eq!(
            InvalidReconstructionResponseError::V2EmptyChunkRange.to_string(),
            "v2 response emitted an empty chunk range"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_inverted_byte_range() {
        assert_eq!(
            InvalidReconstructionResponseError::V2InvertedByteRange.to_string(),
            "v2 response emitted an inverted byte range"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_fetch_count_disagreed() {
        assert_eq!(
            InvalidReconstructionResponseError::V2FetchCountDisagreedWithV1.to_string(),
            "v2 response fetch count disagreed with v1"
        );
    }

    #[test]
    fn invalid_reconstruction_response_error_v2_range_count_disagreed() {
        assert_eq!(
            InvalidReconstructionResponseError::V2RangeCountDisagreedWithV1.to_string(),
            "v2 response range count disagreed with v1"
        );
    }

    #[test]
    fn invalid_lifecycle_metadata_display_active_retention_hold_quarantined() {
        let err = InvalidLifecycleMetadataError::ActiveRetentionHoldQuarantined {
            object_key: "obj".to_owned(),
        };
        let msg = err.to_string();
        assert!(msg.contains("coexisted with quarantine"));
    }
}
