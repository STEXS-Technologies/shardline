use std::io::Cursor;

use shardline_index::{FileRecord, parse_xet_hash_hex};
use shardline_protocol::{ByteRange, ShardlineHash};

use crate::{
    InvalidReconstructionResponseError, InvalidSerializedShardError, ServerError,
    app::{parse_oci_path, parse_upload_content_range},
    bazel_http_adapter::{BazelCacheKind, bazel_cache_object_key},
    config::ShardMetadataLimits,
    lfs_adapter::lfs_object_key,
    lifecycle_repair::{
        QuarantineRepairAction, RetentionHoldRepairAction, WebhookDeliveryRepairAction,
        classify_quarantine_repair_action, classify_retention_hold_repair_action,
        classify_webhook_delivery_repair_action,
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
