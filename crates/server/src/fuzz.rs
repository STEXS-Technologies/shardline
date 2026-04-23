use std::io::Cursor;

use shardline_index::FileRecord;
use shardline_protocol::{ByteRange, ShardlineHash, validate_serialized_xorb};

use crate::{
    InvalidReconstructionResponseError, InvalidSerializedShardError, ServerError,
    config::ShardMetadataLimits,
    lifecycle_repair::{
        QuarantineRepairAction, RetentionHoldRepairAction, WebhookDeliveryRepairAction,
        classify_quarantine_repair_action, classify_retention_hold_repair_action,
        classify_webhook_delivery_repair_action,
    },
    xet_adapter::{
        build_reconstruction_response, build_xorb_transfer_url, normalize_serialized_xorb,
        reconstruction_v2_from_v1, retained_shard_chunk_hashes,
    },
};

/// Summary of a normalized and validated xorb payload used by fuzz targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzValidatedXorbSummary {
    /// Length of the normalized serialized xorb.
    pub normalized_len: u64,
    /// Total serialized xorb length reported by validation.
    pub total_len: u64,
    /// Packed content section length reported by validation.
    pub packed_content_len: u64,
    /// Total unpacked byte length represented by all chunks.
    pub unpacked_len: u64,
    /// Number of validated chunks.
    pub chunk_count: usize,
}

/// Normalizes a raw uploaded Xorb payload and validates the normalized result.
///
/// # Errors
///
/// Returns [`ServerError`] when footer reconstruction fails, the expected hash does not
/// match the normalized payload, the normalized Xorb fails validation, or numeric
/// conversions overflow supported bounds.
pub fn fuzz_normalize_and_validate_xorb(
    expected_hash: ShardlineHash,
    bytes: &[u8],
) -> Result<FuzzValidatedXorbSummary, ServerError> {
    let normalized = normalize_serialized_xorb(expected_hash, bytes)?;
    let normalized_len = u64::try_from(normalized.len())?;
    let mut cursor = Cursor::new(normalized.as_slice());
    let validated =
        validate_serialized_xorb(&mut cursor, expected_hash).map_err(ServerError::from)?;

    Ok(FuzzValidatedXorbSummary {
        normalized_len,
        total_len: validated.total_length(),
        packed_content_len: validated.packed_content_length(),
        unpacked_len: validated.unpacked_length(),
        chunk_count: validated.chunks().len(),
    })
}

/// Summary of chunk hashes retained by a shard payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzRetainedShardSummary {
    /// Dedupe chunk hashes extracted from the retained shard.
    pub dedupe_chunk_hashes: Vec<String>,
}

/// Summary of lifecycle-repair classifications used by fuzz targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuzzLifecycleRepairSummary {
    /// Quarantine entries kept unchanged.
    pub quarantine_keep: u64,
    /// Quarantine entries deleted because the object is missing.
    pub quarantine_delete_missing: u64,
    /// Quarantine entries deleted because the object is reachable again.
    pub quarantine_delete_reachable: u64,
    /// Quarantine entries deleted because a retention hold protects the object.
    pub quarantine_delete_held: u64,
    /// Retention holds kept unchanged.
    pub retention_keep: u64,
    /// Retention holds deleted because they are expired.
    pub retention_delete_expired: u64,
    /// Retention holds deleted because their object is missing.
    pub retention_delete_missing: u64,
    /// Webhook delivery records kept unchanged.
    pub webhook_keep: u64,
    /// Webhook delivery records deleted because they are stale.
    pub webhook_delete_stale: u64,
    /// Webhook delivery records deleted because their timestamps are in the future.
    pub webhook_delete_future: u64,
}

/// Summary of reconstruction response shape used by fuzz targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzReconstructionResponseSummary {
    /// Number of reconstruction terms in the v1 response.
    pub terms: usize,
    /// Number of xorb entries in v1 fetch metadata.
    pub fetch_xorbs: usize,
    /// Number of fetch range entries in v1 fetch metadata.
    pub fetch_ranges: usize,
    /// Number of xorb entries in the v2 response.
    pub v2_xorbs: usize,
    /// Number of v2 fetch entries.
    pub v2_fetches: usize,
    /// Number of v2 byte-range descriptors.
    pub v2_ranges: usize,
    /// Offset into the first reconstruction term.
    pub offset_into_first_range: u64,
    /// Sum of unpacked lengths across reconstruction terms.
    pub total_unpacked_length: u64,
}

/// Builds a reconstruction response and checks protocol-shape invariants for fuzzing.
///
/// # Errors
///
/// Returns [`ServerError`] when file metadata cannot produce a valid reconstruction
/// plan or when checked arithmetic overflows.
pub fn fuzz_reconstruction_response_summary(
    public_base_url: &str,
    record: &FileRecord,
    requested_range: Option<ByteRange>,
) -> Result<FuzzReconstructionResponseSummary, ServerError> {
    let response = build_reconstruction_response(public_base_url, record, requested_range)?;
    ensure_reconstruction_response_invariant(
        response.terms.len() <= record.chunks.len(),
        InvalidReconstructionResponseError::TermCountExceededRecordChunkCount,
    )?;

    let mut total_unpacked_length = 0_u64;
    for term in &response.terms {
        ShardlineHash::parse_api_hex(&term.hash)?;
        ensure_reconstruction_response_invariant(
            term.unpacked_length > 0,
            InvalidReconstructionResponseError::TermHadZeroUnpackedLength,
        )?;
        ensure_reconstruction_response_invariant(
            term.range.start < term.range.end,
            InvalidReconstructionResponseError::TermHadEmptyChunkRange,
        )?;
        ensure_reconstruction_response_invariant(
            response.fetch_info.contains_key(&term.hash),
            InvalidReconstructionResponseError::TermMissingFetchInfo,
        )?;
        let next_total = total_unpacked_length
            .checked_add(term.unpacked_length)
            .ok_or(ServerError::Overflow)?;
        total_unpacked_length = next_total;
    }

    let mut fetch_ranges = 0_usize;
    for (hash, fetch_entries) in &response.fetch_info {
        ShardlineHash::parse_api_hex(hash)?;
        ensure_reconstruction_response_invariant(
            !fetch_entries.is_empty(),
            InvalidReconstructionResponseError::EmptyFetchList,
        )?;
        for fetch_entry in fetch_entries {
            ensure_reconstruction_response_invariant(
                fetch_entry.url == build_xorb_transfer_url(public_base_url, hash),
                InvalidReconstructionResponseError::FetchUrlHashMismatch,
            )?;
            ensure_reconstruction_response_invariant(
                fetch_entry.range.start < fetch_entry.range.end,
                InvalidReconstructionResponseError::FetchEntryEmptyChunkRange,
            )?;
            ensure_reconstruction_response_invariant(
                fetch_entry.url_range.start <= fetch_entry.url_range.end,
                InvalidReconstructionResponseError::FetchEntryInvertedByteRange,
            )?;
            ensure_reconstruction_response_invariant(
                response.terms.iter().any(|term| {
                    term.hash == *hash
                        && term.range == fetch_entry.range
                        && term.unpacked_length > 0
                }),
                InvalidReconstructionResponseError::FetchEntryMissingTerm,
            )?;
            fetch_ranges = fetch_ranges.checked_add(1).ok_or(ServerError::Overflow)?;
        }
    }

    let v2 = reconstruction_v2_from_v1(response.clone());
    ensure_reconstruction_response_invariant(
        v2.offset_into_first_range == response.offset_into_first_range,
        InvalidReconstructionResponseError::V2ChangedOffsetIntoFirstRange,
    )?;
    ensure_reconstruction_response_invariant(
        v2.terms == response.terms,
        InvalidReconstructionResponseError::V2ChangedTerms,
    )?;
    ensure_reconstruction_response_invariant(
        v2.xorbs.len() == response.fetch_info.len(),
        InvalidReconstructionResponseError::V2ChangedXorbFetchInfoCardinality,
    )?;

    let mut v2_fetches = 0_usize;
    let mut v2_ranges = 0_usize;
    for (hash, entries) in &v2.xorbs {
        ensure_reconstruction_response_invariant(
            response.fetch_info.contains_key(hash),
            InvalidReconstructionResponseError::V2FetchHashAbsentFromV1,
        )?;
        ensure_reconstruction_response_invariant(
            !entries.is_empty(),
            InvalidReconstructionResponseError::V2EmptyFetchList,
        )?;
        v2_fetches = v2_fetches
            .checked_add(entries.len())
            .ok_or(ServerError::Overflow)?;
        for entry in entries {
            ensure_reconstruction_response_invariant(
                entry.url == build_xorb_transfer_url(public_base_url, hash),
                InvalidReconstructionResponseError::FetchUrlHashMismatch,
            )?;
            ensure_reconstruction_response_invariant(
                !entry.ranges.is_empty(),
                InvalidReconstructionResponseError::V2FetchEntryWithoutRanges,
            )?;
            v2_ranges = v2_ranges
                .checked_add(entry.ranges.len())
                .ok_or(ServerError::Overflow)?;
            for range in &entry.ranges {
                ensure_reconstruction_response_invariant(
                    range.chunks.start < range.chunks.end,
                    InvalidReconstructionResponseError::V2EmptyChunkRange,
                )?;
                ensure_reconstruction_response_invariant(
                    range.bytes.start <= range.bytes.end,
                    InvalidReconstructionResponseError::V2InvertedByteRange,
                )?;
            }
        }
    }
    ensure_reconstruction_response_invariant(
        v2_fetches == fetch_ranges,
        InvalidReconstructionResponseError::V2FetchCountDisagreedWithV1,
    )?;
    ensure_reconstruction_response_invariant(
        v2_ranges == fetch_ranges,
        InvalidReconstructionResponseError::V2RangeCountDisagreedWithV1,
    )?;

    Ok(FuzzReconstructionResponseSummary {
        terms: response.terms.len(),
        fetch_xorbs: response.fetch_info.len(),
        fetch_ranges,
        v2_xorbs: v2.xorbs.len(),
        v2_fetches,
        v2_ranges,
        offset_into_first_range: response.offset_into_first_range,
        total_unpacked_length,
    })
}

fn ensure_reconstruction_response_invariant(
    condition: bool,
    error: InvalidReconstructionResponseError,
) -> Result<(), ServerError> {
    if condition { Ok(()) } else { Err(error.into()) }
}

/// Classifies lifecycle-repair decisions for fuzzed metadata states.
///
/// # Errors
///
/// Returns [`ServerError`] when counter arithmetic overflows.
pub fn fuzz_lifecycle_repair_summary(
    now_unix_seconds: u64,
    webhook_retention_seconds: u64,
    quarantine_states: &[(bool, bool, bool)],
    retention_states: &[(Option<u64>, u64, bool)],
    webhook_processed_at_unix_seconds: &[u64],
) -> Result<FuzzLifecycleRepairSummary, ServerError> {
    let max_processed_at_unix_seconds = now_unix_seconds
        .checked_add(300)
        .ok_or(ServerError::Overflow)?;
    let stale_cutoff_unix_seconds = now_unix_seconds.saturating_sub(webhook_retention_seconds);

    let mut summary = FuzzLifecycleRepairSummary {
        quarantine_keep: 0,
        quarantine_delete_missing: 0,
        quarantine_delete_reachable: 0,
        quarantine_delete_held: 0,
        retention_keep: 0,
        retention_delete_expired: 0,
        retention_delete_missing: 0,
        webhook_keep: 0,
        webhook_delete_stale: 0,
        webhook_delete_future: 0,
    };

    for &(object_exists, is_reachable, is_held) in quarantine_states {
        match classify_quarantine_repair_action(object_exists, is_reachable, is_held) {
            QuarantineRepairAction::Keep => {
                summary.quarantine_keep = increment_counter(summary.quarantine_keep)?;
            }
            QuarantineRepairAction::DeleteMissing => {
                summary.quarantine_delete_missing =
                    increment_counter(summary.quarantine_delete_missing)?;
            }
            QuarantineRepairAction::DeleteReachable => {
                summary.quarantine_delete_reachable =
                    increment_counter(summary.quarantine_delete_reachable)?;
            }
            QuarantineRepairAction::DeleteHeld => {
                summary.quarantine_delete_held = increment_counter(summary.quarantine_delete_held)?;
            }
        }
    }

    for &(release_after_unix_seconds, held_at_unix_seconds, object_exists) in retention_states {
        match classify_retention_hold_repair_action(
            release_after_unix_seconds,
            held_at_unix_seconds,
            object_exists,
            now_unix_seconds,
        ) {
            RetentionHoldRepairAction::Keep => {
                summary.retention_keep = increment_counter(summary.retention_keep)?;
            }
            RetentionHoldRepairAction::DeleteExpired => {
                summary.retention_delete_expired =
                    increment_counter(summary.retention_delete_expired)?;
            }
            RetentionHoldRepairAction::DeleteMissing => {
                summary.retention_delete_missing =
                    increment_counter(summary.retention_delete_missing)?;
            }
        }
    }

    for &processed_at_unix_seconds in webhook_processed_at_unix_seconds {
        match classify_webhook_delivery_repair_action(
            processed_at_unix_seconds,
            stale_cutoff_unix_seconds,
            max_processed_at_unix_seconds,
        ) {
            WebhookDeliveryRepairAction::Keep => {
                summary.webhook_keep = increment_counter(summary.webhook_keep)?;
            }
            WebhookDeliveryRepairAction::DeleteStale => {
                summary.webhook_delete_stale = increment_counter(summary.webhook_delete_stale)?;
            }
            WebhookDeliveryRepairAction::DeleteFuture => {
                summary.webhook_delete_future = increment_counter(summary.webhook_delete_future)?;
            }
        }
    }

    Ok(summary)
}

/// Parses a serialized shard with bounded metadata limits and reports the retained
/// dedupe chunk hashes.
///
/// # Errors
///
/// Returns [`ServerError`] when shard parsing fails, metadata limits are exceeded, the
/// retained hash list is not strictly ordered, or a retained hash is not a valid Xet
/// protocol hash.
pub fn fuzz_retained_shard_chunk_hashes(
    shard_bytes: &[u8],
    limits: ShardMetadataLimits,
) -> Result<FuzzRetainedShardSummary, ServerError> {
    let dedupe_chunk_hashes = retained_shard_chunk_hashes(shard_bytes, limits)?;
    for window in dedupe_chunk_hashes.windows(2) {
        let [left, right] = window else {
            continue;
        };
        if left >= right {
            return Err(
                InvalidSerializedShardError::RetainedShardChunkHashesNotStrictlyOrdered.into(),
            );
        }
    }
    for hash in &dedupe_chunk_hashes {
        ShardlineHash::parse_api_hex(hash).map_err(ServerError::from)?;
    }

    Ok(FuzzRetainedShardSummary {
        dedupe_chunk_hashes,
    })
}

fn increment_counter(value: u64) -> Result<u64, ServerError> {
    value.checked_add(1).ok_or(ServerError::Overflow)
}
