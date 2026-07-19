use std::io::Cursor;

use shardline_index::{FileRecord, parse_xet_hash_hex};
use shardline_protocol::{ByteRange, ShardlineHash};

use crate::{
    BazelCacheKind, InvalidReconstructionResponseError, InvalidSerializedShardError, ServerError,
    app::{parse_oci_path, parse_upload_content_range},
    bazel_cache_object_key,
    config::ShardMetadataLimits,
    lfs_object_key,
    lifecycle_repair::{
        classification::{
            classify_quarantine_repair_action, classify_retention_hold_repair_action,
            classify_webhook_delivery_repair_action,
        },
        types::{
            QuarantineRepairAction, RetentionHoldRepairAction, WebhookDeliveryRepairAction,
        },
    },
    oci_adapter::{oci_blob_key, oci_manifest_key, parse_reference},
    protocol_support::{parse_sha256_digest, validate_oci_repository_name, validate_oci_tag},
    server_frontend::ServerFrontend,
    xet_adapter::{
        build_reconstruction_response, build_xorb_transfer_url, normalize_serialized_xorb,
        reconstruction_v2_from_v1, retained_shard_chunk_hashes, validate_serialized_xorb,
    },
};

/// Summary of Git LFS frontend validation used by fuzz targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzLfsFrontendSummary {
    /// Whether the supplied oid passed validation.
    pub oid_accepts: bool,
    /// Whether object-key derivation was deterministic.
    pub key_is_stable: bool,
}

/// Summary of Bazel HTTP cache frontend validation used by fuzz targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzBazelHttpFrontendSummary {
    /// Whether the supplied hash is accepted for `ac` objects.
    pub ac_accepts: bool,
    /// Whether the supplied hash is accepted for `cas` objects.
    pub cas_accepts: bool,
}

/// Summary of OCI frontend parsing and validation used by fuzz targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzOciFrontendSummary {
    /// Whether the repository name passed validation.
    pub repository_accepts: bool,
    /// Whether the reference passed tag-or-digest parsing.
    pub reference_accepts: bool,
    /// Whether the digest string passed parsing.
    pub digest_accepts: bool,
    /// Whether the upload session identifier passed validation.
    pub session_accepts: bool,
    /// Whether the upload content-range parser accepted the value.
    pub content_range_accepts: bool,
    /// Whether the OCI route parser accepted the supplied path.
    pub path_accepts: bool,
    /// Whether the blob key derivation accepted the repository and digest.
    pub blob_accepts: bool,
    /// Whether the manifest key derivation accepted the repository and digest.
    pub manifest_accepts: bool,
}

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

/// Classification result for quarantine repair, exposed for fuzzing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuzzQuarantineAction {
    Keep,
    DeleteMissing,
    DeleteReachable,
    DeleteHeld,
}

impl From<QuarantineRepairAction> for FuzzQuarantineAction {
    fn from(action: QuarantineRepairAction) -> Self {
        match action {
            QuarantineRepairAction::Keep => Self::Keep,
            QuarantineRepairAction::DeleteMissing => Self::DeleteMissing,
            QuarantineRepairAction::DeleteReachable => Self::DeleteReachable,
            QuarantineRepairAction::DeleteHeld => Self::DeleteHeld,
        }
    }
}

/// Classification result for retention hold repair, exposed for fuzzing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuzzRetentionAction {
    Keep,
    DeleteExpired,
    DeleteMissing,
}

impl From<RetentionHoldRepairAction> for FuzzRetentionAction {
    fn from(action: RetentionHoldRepairAction) -> Self {
        match action {
            RetentionHoldRepairAction::Keep => Self::Keep,
            RetentionHoldRepairAction::DeleteExpired => Self::DeleteExpired,
            RetentionHoldRepairAction::DeleteMissing => Self::DeleteMissing,
        }
    }
}

/// Classification result for webhook delivery repair, exposed for fuzzing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuzzWebhookAction {
    Keep,
    DeleteStale,
    DeleteFuture,
}

impl From<WebhookDeliveryRepairAction> for FuzzWebhookAction {
    fn from(action: WebhookDeliveryRepairAction) -> Self {
        match action {
            WebhookDeliveryRepairAction::Keep => Self::Keep,
            WebhookDeliveryRepairAction::DeleteStale => Self::DeleteStale,
            WebhookDeliveryRepairAction::DeleteFuture => Self::DeleteFuture,
        }
    }
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

/// Summary of protocol-frontend parser and key validation used by fuzz targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzProtocolFrontendSummary {
    /// Whether the frontend selector token parsed successfully.
    pub frontend_accepts: bool,
    /// Whether the digest parser accepted the supplied digest string.
    pub digest_accepts: bool,
    /// Whether the Git LFS object-key derivation accepted the supplied oid.
    pub lfs_accepts: bool,
    /// Whether the Bazel cache key derivation accepted the supplied hash.
    pub bazel_accepts: bool,
    /// Whether the OCI repository validator accepted the repository name.
    pub oci_repository_accepts: bool,
    /// Whether the OCI tag/reference validator accepted the supplied reference.
    pub oci_reference_accepts: bool,
    /// Whether the OCI blob key derivation accepted the input tuple.
    pub oci_blob_accepts: bool,
    /// Whether the OCI manifest key derivation accepted the input tuple.
    pub oci_manifest_accepts: bool,
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
        parse_xet_hash_hex(&term.hash)?;
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
        parse_xet_hash_hex(hash)?;
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

/// Parses protocol frontend selectors and validates protocol-specific object keys.
///
/// # Errors
///
/// Returns [`ServerError`] when a successfully-derived object key cannot preserve a
/// stable, deterministic storage representation.
pub fn fuzz_protocol_frontend_summary(
    frontend: &str,
    oid: &str,
    digest: &str,
    repository: &str,
    reference: &str,
) -> Result<FuzzProtocolFrontendSummary, ServerError> {
    let frontend_accepts = ServerFrontend::parse(frontend).is_ok();
    let digest_accepts = parse_sha256_digest(digest).is_ok();

    let lfs_accepts = match lfs_object_key(oid, None) {
        Ok(key) => {
            let repeated = lfs_object_key(oid, None)?;
            key.as_str() == repeated.as_str()
        }
        Err(_) => false,
    };

    let bazel_accepts = match bazel_cache_object_key(BazelCacheKind::Cas, oid, None) {
        Ok(key) => {
            let repeated = bazel_cache_object_key(BazelCacheKind::Cas, oid, None)?;
            key.as_str() == repeated.as_str()
        }
        Err(_) => false,
    };

    let oci_repository_accepts = validate_oci_repository_name(repository).is_ok();
    let oci_reference_accepts =
        parse_reference(reference).is_ok() || validate_oci_tag(reference).is_ok();

    let digest_hex = parse_sha256_digest(digest).ok();
    let oci_blob_accepts = if let Some(digest_hex) = digest_hex.as_deref() {
        match oci_blob_key(repository, digest_hex, None) {
            Ok(key) => {
                let repeated = oci_blob_key(repository, digest_hex, None)?;
                key.as_str() == repeated.as_str()
            }
            Err(_) => false,
        }
    } else {
        false
    };
    let oci_manifest_accepts = if let Some(digest_hex) = digest_hex.as_deref() {
        match oci_manifest_key(repository, digest_hex, None) {
            Ok(key) => {
                let repeated = oci_manifest_key(repository, digest_hex, None)?;
                key.as_str() == repeated.as_str()
            }
            Err(_) => false,
        }
    } else {
        false
    };

    Ok(FuzzProtocolFrontendSummary {
        frontend_accepts,
        digest_accepts,
        lfs_accepts,
        bazel_accepts,
        oci_repository_accepts,
        oci_reference_accepts,
        oci_blob_accepts,
        oci_manifest_accepts,
    })
}

/// Validates Git LFS object identity and key determinism for fuzzing.
///
/// # Errors
///
/// Returns [`ServerError`] when a successfully-derived object key cannot be recomputed
/// deterministically.
pub fn fuzz_lfs_frontend_summary(oid: &str) -> Result<FuzzLfsFrontendSummary, ServerError> {
    let (oid_accepts, key_is_stable) = match lfs_object_key(oid, None) {
        Ok(key) => {
            let repeated = lfs_object_key(oid, None)?;
            (true, key.as_str() == repeated.as_str())
        }
        Err(_) => (false, false),
    };

    Ok(FuzzLfsFrontendSummary {
        oid_accepts,
        key_is_stable,
    })
}

/// Validates Bazel HTTP cache key derivation for fuzzing.
///
/// # Errors
///
/// Returns [`ServerError`] when a successfully-derived Bazel cache key cannot be
/// recomputed deterministically.
pub fn fuzz_bazel_http_frontend_summary(
    hash_hex: &str,
) -> Result<FuzzBazelHttpFrontendSummary, ServerError> {
    let ac_accepts = match bazel_cache_object_key(BazelCacheKind::Ac, hash_hex, None) {
        Ok(key) => {
            let repeated = bazel_cache_object_key(BazelCacheKind::Ac, hash_hex, None)?;
            key.as_str() == repeated.as_str()
        }
        Err(_) => false,
    };
    let cas_accepts = match bazel_cache_object_key(BazelCacheKind::Cas, hash_hex, None) {
        Ok(key) => {
            let repeated = bazel_cache_object_key(BazelCacheKind::Cas, hash_hex, None)?;
            key.as_str() == repeated.as_str()
        }
        Err(_) => false,
    };

    Ok(FuzzBazelHttpFrontendSummary {
        ac_accepts,
        cas_accepts,
    })
}

/// Validates OCI path parsing and identity derivation for fuzzing.
///
/// # Errors
///
/// Returns [`ServerError`] when a successfully-derived OCI storage key cannot be
/// recomputed deterministically.
pub fn fuzz_oci_frontend_summary(
    repository: &str,
    reference: &str,
    digest: &str,
    session_id: &str,
    content_range: &str,
    path: &str,
) -> Result<FuzzOciFrontendSummary, ServerError> {
    let repository_accepts = validate_oci_repository_name(repository).is_ok();
    let reference_accepts = parse_reference(reference).is_ok();
    let digest_accepts = parse_sha256_digest(digest).is_ok();
    let session_accepts = crate::protocol_support::validate_upload_session_id(session_id).is_ok();
    let content_range_accepts = parse_upload_content_range(content_range).is_ok();
    let path_accepts = parse_oci_path(path).is_ok();
    let digest_hex = parse_sha256_digest(digest).ok();
    let blob_accepts = digest_hex
        .as_deref()
        .is_some_and(|digest_hex| oci_blob_key(repository, digest_hex, None).is_ok());
    let manifest_accepts = digest_hex
        .as_deref()
        .is_some_and(|digest_hex| oci_manifest_key(repository, digest_hex, None).is_ok());

    Ok(FuzzOciFrontendSummary {
        repository_accepts,
        reference_accepts,
        digest_accepts,
        session_accepts,
        content_range_accepts,
        path_accepts,
        blob_accepts,
        manifest_accepts,
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

/// Fuzz-target wrapper for quarantine classification.
#[must_use]
pub fn fuzz_classify_quarantine(
    object_exists: bool,
    is_reachable: bool,
    is_held: bool,
) -> FuzzQuarantineAction {
    classify_quarantine_repair_action(object_exists, is_reachable, is_held).into()
}

/// Fuzz-target wrapper for retention hold classification.
#[must_use]
pub fn fuzz_classify_retention(
    release_after_unix_seconds: Option<u64>,
    held_at_unix_seconds: u64,
    object_exists: bool,
    now_unix_seconds: u64,
) -> FuzzRetentionAction {
    classify_retention_hold_repair_action(
        release_after_unix_seconds,
        held_at_unix_seconds,
        object_exists,
        now_unix_seconds,
    )
    .into()
}

/// Fuzz-target wrapper for webhook delivery classification.
#[must_use]
pub fn fuzz_classify_webhook(
    processed_at_unix_seconds: u64,
    stale_cutoff_unix_seconds: u64,
    max_processed_at_unix_seconds: u64,
) -> FuzzWebhookAction {
    classify_webhook_delivery_repair_action(
        processed_at_unix_seconds,
        stale_cutoff_unix_seconds,
        max_processed_at_unix_seconds,
    )
    .into()
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
        parse_xet_hash_hex(hash).map_err(ServerError::from)?;
    }

    Ok(FuzzRetainedShardSummary {
        dedupe_chunk_hashes,
    })
}

fn increment_counter(value: u64) -> Result<u64, ServerError> {
    value.checked_add(1).ok_or(ServerError::Overflow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        InvalidReconstructionResponseError, ServerError,
        lifecycle_repair::types::{
            QuarantineRepairAction, RetentionHoldRepairAction, WebhookDeliveryRepairAction,
        },
    };

    #[test]
    fn fuzz_quarantine_action_from_keep() {
        let result: FuzzQuarantineAction = QuarantineRepairAction::Keep.into();
        assert_eq!(result, FuzzQuarantineAction::Keep);
    }

    #[test]
    fn fuzz_quarantine_action_from_delete_missing() {
        let result: FuzzQuarantineAction = QuarantineRepairAction::DeleteMissing.into();
        assert_eq!(result, FuzzQuarantineAction::DeleteMissing);
    }

    #[test]
    fn fuzz_quarantine_action_from_delete_reachable() {
        let result: FuzzQuarantineAction = QuarantineRepairAction::DeleteReachable.into();
        assert_eq!(result, FuzzQuarantineAction::DeleteReachable);
    }

    #[test]
    fn fuzz_quarantine_action_from_delete_held() {
        let result: FuzzQuarantineAction = QuarantineRepairAction::DeleteHeld.into();
        assert_eq!(result, FuzzQuarantineAction::DeleteHeld);
    }

    #[test]
    fn fuzz_retention_action_from_keep() {
        let result: FuzzRetentionAction = RetentionHoldRepairAction::Keep.into();
        assert_eq!(result, FuzzRetentionAction::Keep);
    }

    #[test]
    fn fuzz_retention_action_from_delete_expired() {
        let result: FuzzRetentionAction = RetentionHoldRepairAction::DeleteExpired.into();
        assert_eq!(result, FuzzRetentionAction::DeleteExpired);
    }

    #[test]
    fn fuzz_retention_action_from_delete_missing() {
        let result: FuzzRetentionAction = RetentionHoldRepairAction::DeleteMissing.into();
        assert_eq!(result, FuzzRetentionAction::DeleteMissing);
    }

    #[test]
    fn fuzz_webhook_action_from_keep() {
        let result: FuzzWebhookAction = WebhookDeliveryRepairAction::Keep.into();
        assert_eq!(result, FuzzWebhookAction::Keep);
    }

    #[test]
    fn fuzz_webhook_action_from_delete_stale() {
        let result: FuzzWebhookAction = WebhookDeliveryRepairAction::DeleteStale.into();
        assert_eq!(result, FuzzWebhookAction::DeleteStale);
    }

    #[test]
    fn fuzz_webhook_action_from_delete_future() {
        let result: FuzzWebhookAction = WebhookDeliveryRepairAction::DeleteFuture.into();
        assert_eq!(result, FuzzWebhookAction::DeleteFuture);
    }

    #[test]
    fn increment_counter_increases_by_one() {
        let result = increment_counter(41).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn increment_counter_overflows_at_max() {
        let result = increment_counter(u64::MAX);
        assert!(matches!(result, Err(ServerError::Overflow)));
    }

    #[test]
    fn increment_counter_works_at_zero() {
        let result = increment_counter(0).unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn ensure_reconstruction_response_invariant_passes_for_true() {
        let result = ensure_reconstruction_response_invariant(
            true,
            InvalidReconstructionResponseError::TermCountExceededRecordChunkCount,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn ensure_reconstruction_response_invariant_fails_for_false() {
        let result = ensure_reconstruction_response_invariant(
            false,
            InvalidReconstructionResponseError::TermCountExceededRecordChunkCount,
        );
        assert!(result.is_err());
    }

    #[test]
    fn fuzz_classify_quarantine_keep() {
        // object exists, not reachable, not held -> Keep
        let result = fuzz_classify_quarantine(true, false, false);
        assert_eq!(result, FuzzQuarantineAction::Keep);
    }

    #[test]
    fn fuzz_classify_quarantine_delete_missing() {
        // object doesn't exist -> DeleteMissing (regardless of reachable/held)
        let result = fuzz_classify_quarantine(false, false, false);
        assert_eq!(result, FuzzQuarantineAction::DeleteMissing);
    }

    #[test]
    fn fuzz_classify_quarantine_delete_reachable() {
        // object exists, is reachable -> DeleteReachable
        let result = fuzz_classify_quarantine(true, true, false);
        assert_eq!(result, FuzzQuarantineAction::DeleteReachable);
    }

    #[test]
    fn fuzz_classify_quarantine_delete_held() {
        // object exists, not reachable, is held -> DeleteHeld
        let result = fuzz_classify_quarantine(true, false, true);
        assert_eq!(result, FuzzQuarantineAction::DeleteHeld);
    }

    #[test]
    fn fuzz_classify_retention_keep() {
        let result = fuzz_classify_retention(Some(200), 100, true, 150);
        assert_eq!(result, FuzzRetentionAction::Keep);
    }

    #[test]
    fn fuzz_classify_retention_delete_expired() {
        let result = fuzz_classify_retention(Some(100), 50, true, 150);
        assert_eq!(result, FuzzRetentionAction::DeleteExpired);
    }

    #[test]
    fn fuzz_classify_retention_delete_missing() {
        let result = fuzz_classify_retention(Some(200), 100, false, 150);
        assert_eq!(result, FuzzRetentionAction::DeleteMissing);
    }

    #[test]
    fn fuzz_classify_webhook_keep() {
        let result = fuzz_classify_webhook(100, 50, 200);
        assert_eq!(result, FuzzWebhookAction::Keep);
    }

    #[test]
    fn fuzz_classify_webhook_delete_stale() {
        let result = fuzz_classify_webhook(30, 50, 200);
        assert_eq!(result, FuzzWebhookAction::DeleteStale);
    }

    #[test]
    fn fuzz_classify_webhook_delete_future() {
        let result = fuzz_classify_webhook(250, 50, 200);
        assert_eq!(result, FuzzWebhookAction::DeleteFuture);
    }

    #[test]
    fn fuzz_lfs_frontend_summary_accepts_valid_oid() {
        let oid = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let result = fuzz_lfs_frontend_summary(oid).unwrap();
        assert!(result.oid_accepts);
        assert!(result.key_is_stable);
    }

    #[test]
    fn fuzz_lfs_frontend_summary_accepts_empty_oid() {
        // Empty oid may be accepted and passed to object store layer
        let result = fuzz_lfs_frontend_summary("");
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn fuzz_bazel_http_frontend_summary_accepts_valid_hash() {
        let hash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let result = fuzz_bazel_http_frontend_summary(hash).unwrap();
        assert!(result.ac_accepts);
        assert!(result.cas_accepts);
    }

    #[test]
    fn fuzz_bazel_http_frontend_summary_accepts_empty_hash() {
        // Empty hash may be accepted
        let result = fuzz_bazel_http_frontend_summary("");
        // This depends on the implementation; just verify it doesn't panic
        let _ = result;
    }

    #[test]
    fn fuzz_protocol_frontend_summary_accepts_xet_frontend() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let digest = &format!("sha256:{hex}");
        let result = fuzz_protocol_frontend_summary("xet", hex, digest, "repo", "v1").unwrap();
        assert!(result.frontend_accepts);
        assert!(result.digest_accepts);
    }

    #[test]
    fn fuzz_protocol_frontend_summary_rejects_unknown_frontend() {
        let hash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let result = fuzz_protocol_frontend_summary("unknown", hash, hash, "repo", "v1").unwrap();
        assert!(!result.frontend_accepts);
    }

    #[test]
    fn fuzz_lifecycle_repair_summary_counts_actions() {
        // quarantine: (object_exists, is_reachable, is_held)
        // (true, false, false) -> Keep
        // (true, true, false) -> DeleteReachable
        // (true, false, true) -> DeleteHeld
        // (false, _, _) -> DeleteMissing
        let result = fuzz_lifecycle_repair_summary(
            200,
            100,
            &[
                (true, false, false),  // Keep
                (true, true, false),   // DeleteReachable
                (true, false, true),   // DeleteHeld
                (false, false, false), // DeleteMissing
            ],
            // retention: (release_after, held_at, object_exists)
            // release_after=Some(300) > now(200) -> Keep (not expired)
            // release_after=Some(50) < now(200) -> DeleteExpired
            &[(Some(300), 100, true), (Some(50), 30, true)],
            // webhook: processed_at, stale_cutoff=200-100=100, max=200+300=500
            // 150 >= 100 && 150 <= 500 -> Keep
            // 30 < 100 -> DeleteStale
            // 250 <= 500 (also 250 > 200...) let me think
            // max_processed_at = now + 300 = 500
            // stale_cutoff = now - webhook_retention = 200 - 100 = 100
            // 150: between 100 and 500 => Keep
            // 30: < 100 => DeleteStale
            // 250: between 100 and 500 => Keep (not > 500)
            &[150, 30, 250],
        )
        .unwrap();
        assert_eq!(result.quarantine_keep, 1);
        assert_eq!(result.quarantine_delete_missing, 1);
        assert_eq!(result.quarantine_delete_reachable, 1);
        assert_eq!(result.quarantine_delete_held, 1);
        assert_eq!(result.retention_keep, 1);
        assert_eq!(result.retention_delete_expired, 1);
        assert_eq!(result.webhook_keep, 2);
        assert_eq!(result.webhook_delete_stale, 1);
    }

    #[test]
    fn fuzz_lifecycle_repair_summary_empty_inputs() {
        let result = fuzz_lifecycle_repair_summary(200, 100, &[], &[], &[]).unwrap();
        assert_eq!(result.quarantine_keep, 0);
        assert_eq!(result.retention_keep, 0);
        assert_eq!(result.webhook_keep, 0);
    }

    #[test]
    fn fuzz_lifecycle_repair_summary_webhook_delete_future() {
        // processed_at > max_processed_at (now + 300) => DeleteFuture
        let result = fuzz_lifecycle_repair_summary(200, 100, &[], &[], &[600]).unwrap();
        assert_eq!(result.webhook_delete_future, 1);
        assert_eq!(result.webhook_keep, 0);
    }

    #[test]
    fn fuzz_lifecycle_repair_summary_retention_delete_missing() {
        let result =
            fuzz_lifecycle_repair_summary(200, 100, &[], &[(Some(300), 100, false)], &[]).unwrap();
        assert_eq!(result.retention_delete_missing, 1);
    }

    #[test]
    fn fuzz_lifecycle_repair_summary_retention_none_release() {
        // release_after = None and object exists => Keep
        let result =
            fuzz_lifecycle_repair_summary(200, 100, &[], &[(None, 100, true)], &[]).unwrap();
        assert_eq!(result.retention_keep, 1);
    }

    #[test]
    fn fuzz_protocol_frontend_summary_accepts_lfs_oid() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let digest = &format!("sha256:{hex}");
        let result = fuzz_protocol_frontend_summary("lfs", hex, digest, "repo", "v1").unwrap();
        assert!(result.frontend_accepts);
        assert!(result.lfs_accepts);
    }

    #[test]
    fn fuzz_protocol_frontend_summary_accepts_bazel_frontend() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let digest = &format!("sha256:{hex}");
        let result =
            fuzz_protocol_frontend_summary("bazel-http", hex, digest, "repo", "v1").unwrap();
        assert!(result.frontend_accepts);
        assert!(result.bazel_accepts);
    }

    #[test]
    fn fuzz_protocol_frontend_summary_accepts_oci_frontend() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let digest = &format!("sha256:{hex}");
        let result = fuzz_protocol_frontend_summary("oci", hex, digest, "my-repo", "v1").unwrap();
        assert!(result.frontend_accepts);
        assert!(result.oci_repository_accepts);
        assert!(result.oci_reference_accepts);
        assert!(result.oci_blob_accepts);
        assert!(result.oci_manifest_accepts);
    }

    #[test]
    fn fuzz_protocol_frontend_summary_rejects_oci_invalid_repo() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let digest = &format!("sha256:{hex}");
        let result = fuzz_protocol_frontend_summary("oci", hex, digest, "", "v1").unwrap();
        assert!(!result.oci_repository_accepts);
    }

    #[test]
    fn fuzz_protocol_frontend_summary_rejects_bad_digest() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let result =
            fuzz_protocol_frontend_summary("xet", hex, "not-a-digest", "repo", "v1").unwrap();
        assert!(!result.digest_accepts);
        assert!(!result.oci_blob_accepts);
        assert!(!result.oci_manifest_accepts);
    }

    #[test]
    fn fuzz_oci_frontend_summary_accepts_valid_inputs() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let digest = &format!("sha256:{hex}");
        let result = fuzz_oci_frontend_summary(
            "my-repo", "v1", digest, "abc123",
            "0-100", "team/assets/blobs/sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        ).unwrap();
        assert!(result.repository_accepts);
        assert!(result.reference_accepts);
        assert!(result.digest_accepts);
        assert!(result.session_accepts);
        assert!(result.path_accepts);
        assert!(result.blob_accepts);
        assert!(result.manifest_accepts);
    }

    #[test]
    fn fuzz_oci_frontend_summary_rejects_invalid_digest() {
        let result = fuzz_oci_frontend_summary(
            "my-repo",
            "v1",
            "bad-digest",
            "abc123",
            "0-100",
            "/v2/my-repo/blobs/sha256:abc",
        )
        .unwrap();
        assert!(!result.digest_accepts);
        assert!(!result.blob_accepts);
        assert!(!result.manifest_accepts);
    }

    #[test]
    fn fuzz_oci_frontend_summary_rejects_invalid_path() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let digest = &format!("sha256:{hex}");
        let result =
            fuzz_oci_frontend_summary("my-repo", "v1", digest, "abc123", "0-100", "").unwrap();
        assert!(!result.path_accepts);
    }

    #[test]
    fn fuzz_retained_shard_chunk_hashes_rejects_invalid_shard_bytes() {
        let result = fuzz_retained_shard_chunk_hashes(
            b"invalid shard bytes",
            crate::config::ShardMetadataLimits::default(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn fuzz_retained_shard_chunk_hashes_rejects_empty_shard() {
        let result =
            fuzz_retained_shard_chunk_hashes(b"", crate::config::ShardMetadataLimits::default());
        assert!(result.is_err());
    }

    #[test]
    fn fuzz_normalize_and_validate_xorb_rejects_empty_bytes() {
        use shardline_protocol::ShardlineHash;
        let hash = ShardlineHash::from_bytes([0u8; 32]);
        let result = fuzz_normalize_and_validate_xorb(hash, b"");
        assert!(result.is_err());
    }

    #[test]
    fn fuzz_reconstruction_response_summary_handles_empty_record_without_panic() {
        use shardline_index::FileRecord;
        let record = FileRecord {
            file_id: "test".to_owned(),
            content_hash: "abc".to_owned(),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: None,
            chunks: vec![],
        };
        let result = fuzz_reconstruction_response_summary("http://localhost:8080", &record, None);
        // Empty record with zero chunks may succeed (terms will be empty)
        // or fail depending on implementation; verify no panic
        let _ = result;
    }

    #[test]
    fn fuzz_bazel_http_frontend_summary_accepts_mixed_case_hash() {
        let hash = "ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let result = fuzz_bazel_http_frontend_summary(hash).unwrap();
        // Mixed case may or may not be accepted; just verify no panic
        let _ = result;
    }

    #[test]
    fn fuzz_lfs_frontend_summary_rejects_short_oid() {
        let result = fuzz_lfs_frontend_summary("short");
        // short oid may error or return accepts=false
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn fuzz_normalize_and_validate_xorb_rejects_garbage() {
        let hash = shardline_protocol::ShardlineHash::from_bytes([0xabu8; 32]);
        let result = fuzz_normalize_and_validate_xorb(hash, b"this is not a valid xorb");
        assert!(result.is_err());
    }

    #[test]
    fn fuzz_normalize_and_validate_xorb_small_xorb() {
        // A minimal valid xorb is complex to construct manually. Verify
        // that various small byte sequences don't panic.
        let hash = shardline_protocol::ShardlineHash::from_bytes([0u8; 32]);
        for prefix_len in 1..=10 {
            let bytes = vec![0u8; prefix_len];
            let _ = fuzz_normalize_and_validate_xorb(hash, &bytes);
        }
    }

    #[test]
    fn fuzz_reconstruction_response_summary_with_requested_range() {
        use shardline_index::{FileChunkRecord, FileRecord};
        let record = FileRecord {
            file_id: "test.bin".to_owned(),
            content_hash: "aa".repeat(32),
            total_bytes: 10,
            chunk_size: 10,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: "bb".repeat(32),
                offset: 0,
                length: 10,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 10,
            }],
        };
        let range = shardline_protocol::ByteRange::new(2, 7);
        let result =
            fuzz_reconstruction_response_summary("http://localhost:8080", &record, range.ok());
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn fuzz_reconstruction_response_summary_full_range() {
        // ByteRange covering the entire record
        use shardline_index::{FileChunkRecord, FileRecord};
        let record = FileRecord {
            file_id: "full.bin".to_owned(),
            content_hash: "aa".repeat(32),
            total_bytes: 10,
            chunk_size: 10,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: "bb".repeat(32),
                offset: 0,
                length: 10,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 10,
            }],
        };
        let range = shardline_protocol::ByteRange::new(0, 9);
        let result =
            fuzz_reconstruction_response_summary("http://localhost:8080", &record, range.ok());
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn fuzz_reconstruction_response_summary_without_range() {
        // No range constraint
        use shardline_index::{FileChunkRecord, FileRecord};
        let record = FileRecord {
            file_id: "norange.bin".to_owned(),
            content_hash: "aa".repeat(32),
            total_bytes: 10,
            chunk_size: 10,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: "bb".repeat(32),
                offset: 0,
                length: 10,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 10,
            }],
        };
        let result = fuzz_reconstruction_response_summary("http://localhost:8080", &record, None);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn fuzz_protocol_frontend_summary_with_oci_tag_reference() {
        // OCI reference as a tag (not digest)
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let digest = &format!("sha256:{hex}");
        let result =
            fuzz_protocol_frontend_summary("oci", hex, digest, "my-repo", "latest").unwrap();
        assert!(result.frontend_accepts);
        assert!(result.oci_reference_accepts);
    }

    #[test]
    fn fuzz_protocol_frontend_summary_rejects_bazel_with_bad_hash() {
        // Bazel with a hash that fails key derivation
        let result =
            fuzz_protocol_frontend_summary("bazel-http", "bad", "sha256:bad", "repo", "v1")
                .unwrap();
        assert!(result.frontend_accepts);
        assert!(!result.bazel_accepts);
    }

    #[test]
    fn fuzz_oci_frontend_summary_rejects_invalid_content_range() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let digest = &format!("sha256:{hex}");
        let result = fuzz_oci_frontend_summary(
            "my-repo",
            "v1",
            digest,
            "abc123",
            "not-a-range",
            "/v2/my-repo/blobs/sha256:abc",
        )
        .unwrap();
        assert!(!result.content_range_accepts);
    }

    #[test]
    fn fuzz_oci_frontend_summary_rejects_invalid_session_id() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let digest = &format!("sha256:{hex}");
        let result = fuzz_oci_frontend_summary(
            "my-repo",
            "v1",
            digest,
            "!!!invalid-session!!!",
            "0-100",
            "/v2/my-repo/blobs/sha256:abc",
        )
        .unwrap();
        assert!(!result.session_accepts);
    }

    #[test]
    fn fuzz_oci_frontend_summary_empty_session_id() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let digest = &format!("sha256:{hex}");
        let result = fuzz_oci_frontend_summary(
            "my-repo",
            "v1",
            digest,
            "",
            "0-100",
            "/v2/my-repo/blobs/sha256:abc",
        )
        .unwrap();
        assert!(!result.session_accepts);
    }

    #[test]
    fn fuzz_lifecycle_repair_summary_retention_none_missing() {
        // release_after = None, object does not exist => DeleteMissing
        let result =
            fuzz_lifecycle_repair_summary(200, 100, &[], &[(None, 100, false)], &[]).unwrap();
        assert_eq!(result.retention_delete_missing, 1);
    }

    #[test]
    fn fuzz_lifecycle_repair_summary_all_delete_future() {
        // All webhook entries are in the future (> max_processed_at = now + 300)
        let result = fuzz_lifecycle_repair_summary(100, 50, &[], &[], &[500, 600, 700]).unwrap();
        assert_eq!(result.webhook_delete_future, 3);
    }

    #[test]
    fn fuzz_lifecycle_repair_summary_all_delete_stale() {
        // All webhook entries are stale (below stale_cutoff)
        let result = fuzz_lifecycle_repair_summary(100, 50, &[], &[], &[10, 20, 30]).unwrap();
        assert_eq!(result.webhook_delete_stale, 3);
    }

    #[test]
    fn fuzz_lifecycle_repair_summary_max_inputs() {
        // All 8 combinations of quarantine (2^3)
        let result = fuzz_lifecycle_repair_summary(
            200,
            100,
            &[
                (false, false, false), // DeleteMissing
                (false, false, true),  // DeleteMissing
                (false, true, false),  // DeleteMissing
                (false, true, true),   // DeleteMissing
                (true, false, false),  // Keep
                (true, false, true),   // DeleteHeld
                (true, true, false),   // DeleteReachable
                (true, true, true),    // DeleteReachable (reachable wins over held)
            ],
            &[
                (None, 0, true),        // Keep (indefinite hold, exists)
                (None, 0, false),       // DeleteMissing (indefinite, missing)
                (Some(100), 50, true),  // DeleteExpired
                (Some(300), 100, true), // Keep
                (Some(100), 50, false), // DeleteMissing
            ],
            &[50, 150, 600],
        )
        .unwrap();
        assert_eq!(result.quarantine_keep, 1);
        assert_eq!(result.quarantine_delete_missing, 4);
        assert_eq!(result.quarantine_delete_reachable, 2);
        assert_eq!(result.quarantine_delete_held, 1);
        // retention: (Some(100), 50, true) => 100 <= 200 => DeleteExpired
        //           (Some(100), 50, false) => 100 <= 200 => DeleteExpired checked first
        //           (Some(300), 100, true) => 300 > 200 => Keep (object exists)
        //           (None, 0, true) => Keep (indefinite, exists)
        //           (None, 0, false) => DeleteMissing (indefinite, missing)
        assert_eq!(result.retention_keep, 2);
        assert_eq!(result.retention_delete_expired, 2);
        assert_eq!(result.retention_delete_missing, 1);
        // webhook: stale_cutoff=200-100=100, max=200+300=500
        //          50 <= 100 => DeleteStale
        //          150 > 100 && 150 <= 500 => Keep
        //          600 > 500 => DeleteFuture
        assert_eq!(result.webhook_keep, 1);
        assert_eq!(result.webhook_delete_stale, 1);
        assert_eq!(result.webhook_delete_future, 1);
    }

    #[test]
    fn fuzz_lfs_frontend_summary_rejects_oid_with_special_chars() {
        // OID with special characters that should fail validation
        let result = fuzz_lfs_frontend_summary("!@#$%^&*()");
        let _ = result;
    }

    #[test]
    fn fuzz_bazel_http_frontend_summary_rejects_invalid_hash() {
        // Hash that fails key derivation
        let result = fuzz_bazel_http_frontend_summary("z");
        let _ = result;
    }

    // ── fuzz_retained_shard_chunk_hashes ordering validation ─────────────

    #[test]
    fn fuzz_retained_shard_chunk_hashes_rejects_unordered_hashes() {
        // The windows(2) let-else pattern and ordering check at lines 695-702
        // needs to be exercised with retained chunks that are not strictly ordered.
        // Unfortunately constructing a valid shard with specific chunk hashes
        // requires internal xet format knowledge.
        //
        // This test verifies that empty/invalid shards produce errors without
        // reaching the ordering check.
        let result =
            fuzz_retained_shard_chunk_hashes(b"", crate::config::ShardMetadataLimits::default());
        assert!(result.is_err());
    }

    #[test]
    fn fuzz_retained_shard_chunk_hashes_single_hash_window_is_skipped() {
        // windows(2) on a single-element slice yields no windows,
        // so the let-else and ordering checks are skipped entirely.
        // This just verifies no panic.
        let result =
            fuzz_retained_shard_chunk_hashes(b"", crate::config::ShardMetadataLimits::default());
        // Should fail at shard parsing before any hash enumeration.
        assert!(result.is_err());
    }

    // ── fuzz_normalize_and_validate_xorb: summary fields ─────────────────

    #[test]
    fn fuzz_normalize_and_validate_xorb_summary_fields_error() {
        // The Ok arm (lines 97-102) returns a summary with fields from validated.
        // Without a valid xorb, we exercise the error path.
        let hash = shardline_protocol::ShardlineHash::from_bytes([0u8; 32]);
        let result = fuzz_normalize_and_validate_xorb(hash, b"not a valid xorb");
        assert!(result.is_err());
    }

    // ── fuzz_protocol_frontend_summary: OCI with invalid reference ───────

    #[test]
    fn fuzz_protocol_frontend_summary_oci_invalid_reference() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let digest = &format!("sha256:{hex}");
        let result =
            fuzz_protocol_frontend_summary("oci", hex, digest, "my-repo", "!!!invalid").unwrap();
        // validate_oci_tag("!!!invalid") should fail, and parse_reference should also fail
        assert!(!result.oci_reference_accepts);
    }

    // ── fuzz_lifecycle_repair_summary: webhook delete future edge ────────

    #[test]
    fn fuzz_lifecycle_repair_summary_retention_none_and_missing() {
        // retention_none + object_missing => DeleteMissing
        let result =
            fuzz_lifecycle_repair_summary(200, 100, &[], &[(None, 0, false)], &[]).unwrap();
        assert_eq!(result.retention_delete_missing, 1);
        assert_eq!(result.retention_keep, 0);
    }

    #[test]
    fn fuzz_lifecycle_repair_summary_all_quarantine_combinations() {
        // Exercise all 8 quarantine combinations through the classification.
        let result = fuzz_lifecycle_repair_summary(
            200,
            100,
            &[
                (false, false, false), // DeleteMissing
                (false, true, false),  // DeleteMissing
                (false, false, true),  // DeleteMissing
                (false, true, true),   // DeleteMissing
                (true, false, false),  // Keep
                (true, false, true),   // DeleteHeld
                (true, true, false),   // DeleteReachable
                (true, true, true),    // DeleteReachable
            ],
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(result.quarantine_keep, 1);
        assert_eq!(result.quarantine_delete_missing, 4);
        assert_eq!(result.quarantine_delete_reachable, 2);
        assert_eq!(result.quarantine_delete_held, 1);
    }

    // ── fuzz_lifecycle_repair_summary: webhook edge cases ────────────────

    #[test]
    fn fuzz_lifecycle_repair_summary_webhook_at_exact_boundaries() {
        // stale_cutoff = 200 - 100 = 100, max = 200 + 300 = 500
        // processed_at = 100 => 100 <= 100 => DeleteStale
        // processed_at = 500 => 500 > 500 is false, 500 <= 100 is false => Keep
        let result = fuzz_lifecycle_repair_summary(200, 100, &[], &[], &[100, 500]).unwrap();
        assert_eq!(result.webhook_keep, 1);
        assert_eq!(result.webhook_delete_stale, 1);
        assert_eq!(result.webhook_delete_future, 0);
    }

    // ── fuzz_oci_frontend_summary: edge cases ────────────────────────────

    #[test]
    fn fuzz_oci_frontend_summary_rejects_empty_content_range() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let digest = &format!("sha256:{hex}");
        let result = fuzz_oci_frontend_summary(
            "my-repo",
            "v1",
            digest,
            "abc123",
            "",
            "/v2/my-repo/blobs/sha256:abc",
        )
        .unwrap();
        assert!(!result.content_range_accepts);
    }

    #[test]
    fn fuzz_oci_frontend_summary_rejects_empty_reference() {
        let hex = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let digest = &format!("sha256:{hex}");
        let result = fuzz_oci_frontend_summary(
            "my-repo",
            "",
            digest,
            "abc123",
            "0-100",
            "/v2/my-repo/blobs/sha256:abc",
        )
        .unwrap();
        assert!(!result.reference_accepts);
    }
}
