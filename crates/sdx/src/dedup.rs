//! Global dedup query + eligibility (M3a).
//!
//! [`DedupClient::query_for_global_dedup_shard`] hits **`GET
//! /v1/chunks/default-merkledb/{hash}`** (GET, not POST — matching shardline
//! `app.rs:406` and upstream `remote_client.rs:159`; a POST would 405), with
//! HTTP 404 reported as a cache **miss** and every other non-success status
//! surfaced as a typed [`TransferError`](crate::error::TransferError). 429 is
//! surfaced without retry (retry policies arrive in M4).
//!
//! The eligibility helper ([`is_global_dedup_eligible`]) mirrors
//! `docs/SDX_PLAN.md` §4.4.2 and §7 item 4 plus upstream
//! `file_deduplication.rs`: **global chunk index 0** is always eligible (not
//! the first chunk of every 8 MiB ingest batch); later chunks are eligible
//! when their hash passes the modulus test (`last-8-bytes-LE % 1024 == 0`) and
//! they are spaced at least [`MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES`] (256)
//! chunks (~16 MiB at a 64 KiB target) from the last query.
//!
//! **Delta vs upstream:** upstream 1.5.4 hardcodes
//! `min_spacing_between_global_dedup_queries` to 0 and never wires the config;
//! sdx applies the documented default itself.
//!
//! [`DefragPrevention`] mirrors upstream `defrag_prevention.rs` (min 8 chunks
//! per range, 0.5 hysteresis, 128-range window) as pure logic.

use std::collections::VecDeque;

use bytes::Bytes;
use xet_core_structures::merklehash::MerkleHash;

use crate::error::SdxError;
use crate::transfer::TransferClient;

/// Route prefix for global dedup queries.
pub const GLOBAL_DEDUP_PREFIX: &str = "default-merkledb";
/// Minimum chunk spacing between global dedup queries (~16 MiB at 64 KiB
/// target). Applied by sdx itself (see module docs for the upstream delta).
pub const MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES: u64 = 256;
/// Global dedup chunk modulus: a chunk whose hash's last 8 bytes (LE) are
/// divisible by this value is eligible (`MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS`).
pub const GLOBAL_DEDUP_CHUNK_MODULUS: u64 = 1024;
/// Defrag-prevention rolling window size in ranges.
pub const DEFRAG_NUM_RANGES_WINDOW: usize = 128;
/// Defrag-prevention minimum chunks per range to allow dedup.
pub const DEFRAG_MIN_CHUNKS_PER_RANGE: usize = 8;
/// Defrag-prevention hysteresis factor (< 1 lowers the effective threshold).
pub const DEFRAG_HYSTERESIS_FACTOR: f32 = 0.5;

/// Outcome of a global dedup query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DedupOutcome {
    /// The chunk is already stored globally; the raw dedup-shard body is
    /// returned for import (shard import is M3b).
    Present {
        /// Raw dedup-shard bytes returned by the server.
        shard_body: Bytes,
    },
    /// The chunk is not stored globally (HTTP 404).
    Miss,
}

/// Client for the global dedup query route.
///
/// Cheap to clone: the underlying [`TransferClient`] only holds a
/// [`reqwest::Client`].
#[derive(Debug, Clone)]
pub struct DedupClient {
    transfer: TransferClient,
}

impl DedupClient {
    /// Creates a dedup client over the supplied CAS transfer transport.
    #[must_use]
    pub const fn new(transfer: TransferClient) -> Self {
        Self { transfer }
    }

    /// Queries the global dedup shard for `hash_hex` (64 lowercase hex chars)
    /// via `GET /v1/chunks/default-merkledb/{hash}`.
    ///
    /// HTTP 200 returns [`DedupOutcome::Present`] with the raw shard body;
    /// HTTP 404 returns [`DedupOutcome::Miss`]. Other non-success statuses are
    /// surfaced as typed errors; 429 is **not** retried (M4 adds retry
    /// policies).
    ///
    /// # Errors
    ///
    /// Returns [`SdxError::Hash`] when `hash_hex` is not valid 64-character
    /// lowercase hex, or [`SdxError::Transfer`] when the request fails or the
    /// server returns a non-404 error status.
    pub async fn query_for_global_dedup_shard(
        &self,
        base_url: &str,
        token: &str,
        hash_hex: &str,
    ) -> Result<DedupOutcome, SdxError> {
        // Validate the hash before spending a request.
        let _ = crate::hash::parse_xet_hash_hex(hash_hex)?;
        let path = format!("/v1/chunks/{GLOBAL_DEDUP_PREFIX}/{hash_hex}");
        let body = self
            .transfer
            .get_optional_bytes(base_url, token, &path)
            .await?;
        Ok(body.map_or_else(
            || DedupOutcome::Miss,
            |shard_body| DedupOutcome::Present { shard_body },
        ))
    }
}

/// Returns whether a chunk at `chunk_index` with hash `chunk_hash` is eligible
/// for a global dedup query.
///
/// Eligibility (plan §4.4.2 / §7 item 4):
///
/// 1. Global chunk index 0 is always eligible.
/// 2. Later chunks are eligible only when `chunk_hash % 1024 == 0` (the
///    upstream `hash_is_global_dedup_eligible` modulus test on the hash's
///    last-8-bytes-LE).
/// 3. The chunk must be at least [`MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES`]
///    chunks past `last_global_query_index` (or `None` when nothing has been
///    queried yet).
#[must_use]
#[allow(clippy::arithmetic_side_effects)] // modulus is the constant 1024 (never 0).
pub fn is_global_dedup_eligible(
    chunk_index: u64,
    chunk_hash: &MerkleHash,
    last_global_query_index: Option<u64>,
) -> bool {
    if chunk_index == 0 {
        return true;
    }
    if *chunk_hash % GLOBAL_DEDUP_CHUNK_MODULUS != 0 {
        return false;
    }
    last_global_query_index.is_none_or(|last| {
        chunk_index.saturating_sub(last) >= MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES
    })
}

/// Rolling defrag-prevention estimate with hysteresis (mirror of upstream
/// `xet-data` `defrag_prevention.rs`).
///
/// Tracks the average number of chunks per dedup range over the last
/// [`DEFRAG_NUM_RANGES_WINDOW`] ranges and allows dedup only when the average
/// is above an effective threshold that toggles between
/// `min_chunks_per_range` and `min_chunks_per_range × hysteresis_factor`
/// (hysteresis factor < 1.0).
#[derive(Debug, Clone)]
pub struct DefragPrevention {
    window_size: usize,
    min_chunks_per_range: f32,
    hysteresis_factor: f32,
    defrag_at_low_threshold: bool,
    rolling_last_nranges: VecDeque<usize>,
    rolling_nranges_chunks: usize,
}

impl Default for DefragPrevention {
    fn default() -> Self {
        Self::new(
            DEFRAG_NUM_RANGES_WINDOW,
            DEFRAG_MIN_CHUNKS_PER_RANGE as f32,
            DEFRAG_HYSTERESIS_FACTOR,
        )
    }
}

impl DefragPrevention {
    /// Creates a defrag-prevention tracker.
    ///
    /// `window_size` is the number of ranges in the rolling estimate,
    /// `min_chunks_per_range` the low dedup threshold, and `hysteresis_factor`
    /// (< 1.0) the multiplier for the hysteresis band.
    #[must_use]
    pub fn new(window_size: usize, min_chunks_per_range: f32, hysteresis_factor: f32) -> Self {
        Self {
            window_size: window_size.max(1),
            min_chunks_per_range,
            hysteresis_factor,
            defrag_at_low_threshold: true,
            rolling_last_nranges: VecDeque::with_capacity(window_size.max(1)),
            rolling_nranges_chunks: 0,
        }
    }

    /// Returns the configured rolling window size in ranges.
    #[must_use]
    pub const fn window_size(&self) -> usize {
        self.window_size
    }

    /// Returns the configured minimum chunks per range threshold.
    #[must_use]
    pub const fn min_chunks_per_range(&self) -> f32 {
        self.min_chunks_per_range
    }

    /// Returns the configured hysteresis factor.
    #[must_use]
    pub const fn hysteresis_factor(&self) -> f32 {
        self.hysteresis_factor
    }

    /// Adds `nchunks` to the currently-last range in the estimate.
    pub fn increment_last_range_in_fragmentation_estimate(&mut self, nchunks: usize) {
        if let Some(back) = self.rolling_last_nranges.back_mut() {
            *back = back.saturating_add(nchunks);
            self.rolling_nranges_chunks = self.rolling_nranges_chunks.saturating_add(nchunks);
        }
    }

    /// Adds a new range with `nchunks` chunks to the rolling estimate, evicting
    /// the oldest range once the window is full.
    pub fn add_range_to_fragmentation_estimate(&mut self, nchunks: usize) {
        self.rolling_last_nranges.push_back(nchunks);
        self.rolling_nranges_chunks = self.rolling_nranges_chunks.saturating_add(nchunks);
        if self.rolling_last_nranges.len() > self.window_size
            && let Some(evicted) = self.rolling_last_nranges.pop_front()
        {
            self.rolling_nranges_chunks = self.rolling_nranges_chunks.saturating_sub(evicted);
        }
    }

    /// Returns the rolling average chunks-per-range, or `None` until the window
    /// is full.
    #[must_use]
    #[allow(clippy::float_arithmetic)] // the defrag hysteresis ratio is f32 by design.
    pub fn rolling_chunks_per_range(&self) -> Option<f32> {
        if self.rolling_last_nranges.len() < self.window_size {
            None
        } else {
            Some(self.rolling_nranges_chunks as f32 / self.rolling_last_nranges.len() as f32)
        }
    }

    /// Returns whether dedup should be allowed against the next range of
    /// `dedup_range_size` chunks, updating the hysteresis threshold.
    #[must_use]
    #[allow(clippy::float_arithmetic)] // hysteresis factor math is f32 by design.
    pub fn allow_dedup_on_next_range(&mut self, dedup_range_size: usize) -> bool {
        let Some(chunks_per_range) = self.rolling_chunks_per_range() else {
            return true;
        };

        let target = if self.defrag_at_low_threshold {
            self.min_chunks_per_range * self.hysteresis_factor
        } else {
            self.min_chunks_per_range
        };

        if chunks_per_range < target {
            // Chunks per range is poor: do not dedupe. Unless the next dedupe
            // window is large enough to improve the estimate, in which case we
            // try to raise the effective threshold.
            if (dedup_range_size as f32) < chunks_per_range {
                self.defrag_at_low_threshold = false;
                return false;
            }
        } else {
            // Deduping again: lower the effective threshold so small fragments
            // are allowed.
            self.defrag_at_low_threshold = true;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path},
    };
    use xet_core_structures::merklehash::MerkleHash;

    use super::{
        DEFRAG_HYSTERESIS_FACTOR, DEFRAG_MIN_CHUNKS_PER_RANGE, DEFRAG_NUM_RANGES_WINDOW,
        DedupClient, DedupOutcome, DefragPrevention, GLOBAL_DEDUP_CHUNK_MODULUS,
        GLOBAL_DEDUP_PREFIX, MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES, is_global_dedup_eligible,
    };
    use crate::error::{SdxError, TransferError};
    use crate::transfer::TransferClient;

    const TOKEN: &str = "cas-token";
    const HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn dedup_route(hash: &str) -> String {
        format!("/v1/chunks/{GLOBAL_DEDUP_PREFIX}/{hash}")
    }

    fn client() -> DedupClient {
        DedupClient::new(TransferClient::new(reqwest::Client::new()))
    }

    async fn request_count(server: &MockServer) -> usize {
        server.received_requests().await.unwrap_or_default().len()
    }

    #[tokio::test]
    async fn query_present_on_200_returns_shard_body() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(dedup_route(HASH)))
            .and(header("authorization", format!("Bearer {TOKEN}")))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                [0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03],
                "application/octet-stream",
            ))
            .mount(&server)
            .await;

        let outcome = client()
            .query_for_global_dedup_shard(&server.uri(), TOKEN, HASH)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            DedupOutcome::Present {
                shard_body: Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03])
            }
        );
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn query_miss_on_404() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(dedup_route(HASH)))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({"error": "not found"})))
            .mount(&server)
            .await;

        let outcome = client()
            .query_for_global_dedup_shard(&server.uri(), TOKEN, HASH)
            .await
            .unwrap();
        assert_eq!(outcome, DedupOutcome::Miss);
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn query_429_surfaces_error_without_retry() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(dedup_route(HASH)))
            .respond_with(ResponseTemplate::new(429).set_body_json(json!({"error": "overloaded"})))
            .mount(&server)
            .await;

        let err = client()
            .query_for_global_dedup_shard(&server.uri(), TOKEN, HASH)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SdxError::Transfer(TransferError::TooManyRequests(_))
        ));
        // No retry: exactly one request was issued.
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn query_typed_errors_for_401_403_and_5xx() {
        for (status, expected) in [(401u16, true), (403, true), (500, false), (503, false)] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path(dedup_route(HASH)))
                .respond_with(ResponseTemplate::new(status).set_body_json(json!({"error": "x"})))
                .mount(&server)
                .await;

            let err = client()
                .query_for_global_dedup_shard(&server.uri(), TOKEN, HASH)
                .await
                .unwrap_err();
            assert!(
                matches!(err, SdxError::Transfer(_)),
                "expected Transfer error for {status}"
            );
            let SdxError::Transfer(transfer_err) = err else {
                continue;
            };
            let ok = match status {
                401 => matches!(transfer_err, TransferError::Unauthorized(_)),
                403 => matches!(transfer_err, TransferError::Forbidden(_)),
                _ => matches!(
                    transfer_err,
                    TransferError::HttpStatus { status: got, .. }
                        if got == status && !expected
                ),
            };
            assert!(ok, "status {status} mapped to {transfer_err:?}");
        }
    }

    #[tokio::test]
    async fn query_uses_get_not_post_and_interpolates_hash() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(dedup_route(HASH)))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![1, 2, 3]))
            .mount(&server)
            .await;

        client()
            .query_for_global_dedup_shard(&server.uri(), TOKEN, HASH)
            .await
            .unwrap();

        assert_eq!(request_count(&server).await, 1);
        let requests = server.received_requests().await.unwrap_or_default();
        assert_eq!(requests[0].method.as_str(), "GET");
        assert_eq!(requests[0].url.path(), dedup_route(HASH));
    }

    #[tokio::test]
    async fn query_rejects_malformed_hash_before_request() {
        let server = MockServer::start().await;
        let err = client()
            .query_for_global_dedup_shard(&server.uri(), TOKEN, "not-a-hash")
            .await
            .unwrap_err();
        assert!(matches!(err, SdxError::Hash(_)));
        assert_eq!(request_count(&server).await, 0);
    }

    fn hash_with_last8(value: u64) -> MerkleHash {
        let mut bytes = [0u8; 32];
        bytes[24..32].copy_from_slice(&value.to_le_bytes());
        MerkleHash::from(bytes)
    }

    #[test]
    fn eligibility_index_zero_always_eligible() {
        let hash = hash_with_last8(1); // fails the modulus test.
        assert!(is_global_dedup_eligible(0, &hash, None));
        assert!(is_global_dedup_eligible(0, &hash, Some(0)));
    }

    #[test]
    fn eligibility_modulus_test_on_last_8_bytes_le() {
        // 1024 is divisible by 1024 → eligible when spaced.
        let eligible = hash_with_last8(1024);
        let ineligible = hash_with_last8(1);
        assert!(is_global_dedup_eligible(1, &eligible, None));
        assert!(!is_global_dedup_eligible(1, &ineligible, None));
        // Boundary: 2048, 3072 also eligible; 512 not.
        assert!(is_global_dedup_eligible(1, &hash_with_last8(2048), None));
        assert!(is_global_dedup_eligible(1, &hash_with_last8(0), None));
        assert!(!is_global_dedup_eligible(1, &hash_with_last8(512), None));
        assert_eq!(hash_with_last8(1024) % GLOBAL_DEDUP_CHUNK_MODULUS, 0);
        assert_ne!(hash_with_last8(1) % GLOBAL_DEDUP_CHUNK_MODULUS, 0);
    }

    #[test]
    fn eligibility_enforces_spacing_between_queries() {
        let hash = hash_with_last8(1024);
        // First query at index 1; next eligible at 1 + 256 = 257.
        assert!(is_global_dedup_eligible(1, &hash, None));
        assert!(!is_global_dedup_eligible(256, &hash, Some(1)));
        assert!(!is_global_dedup_eligible(1, &hash, Some(1)));
        assert!(is_global_dedup_eligible(257, &hash, Some(1)));
        assert!(is_global_dedup_eligible(1000, &hash, Some(700)));
        // Spacing is 256 per the documented default.
        assert_eq!(MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES, 256);
    }

    #[test]
    fn defrag_prevention_window_not_full_allows_dedup() {
        let mut tracker = DefragPrevention::default();
        assert_eq!(tracker.window_size(), DEFRAG_NUM_RANGES_WINDOW);
        for _ in 0..(DEFRAG_NUM_RANGES_WINDOW - 1) {
            tracker.add_range_to_fragmentation_estimate(100);
            assert!(tracker.rolling_chunks_per_range().is_none());
            assert!(tracker.allow_dedup_on_next_range(100));
        }
    }

    #[test]
    #[allow(clippy::float_cmp)] // all compared values are exact in f32 (16.0, 12.0, 0.0, 1.0).
    fn defrag_prevention_average_and_eviction() {
        let mut tracker = DefragPrevention::new(4, 8.0, 0.5);
        for _ in 0..4 {
            tracker.add_range_to_fragmentation_estimate(16);
        }
        let average = tracker.rolling_chunks_per_range().unwrap();
        assert_eq!(average, 16.0);
        // Eviction: a new range of 0 chunks pulls the average to 12.
        tracker.add_range_to_fragmentation_estimate(0);
        assert_eq!(tracker.rolling_chunks_per_range().unwrap(), 12.0);
    }

    #[test]
    #[allow(clippy::float_cmp)] // compared values are exact in f32 (0.0, 1.0).
    fn defrag_prevention_hysteresis_toggles_threshold() {
        let mut tracker = DefragPrevention::new(4, 8.0, 0.5);
        // Fill the window with healthy ranges: average 16 ≥ 8 → dedup allowed.
        for _ in 0..4 {
            tracker.add_range_to_fragmentation_estimate(16);
        }
        assert!(tracker.allow_dedup_on_next_range(16));
        // Now the estimate drops below the low threshold (4.0) but the next
        // dedupe window is not large enough to improve it → refuse and raise
        // the effective threshold.
        let mut degraded = tracker.clone();
        for _ in 0..4 {
            degraded.add_range_to_fragmentation_estimate(0);
        }
        assert_eq!(degraded.rolling_chunks_per_range().unwrap(), 0.0);
        assert!(degraded.rolling_chunks_per_range().unwrap() < 4.0);
        // Dedup allowed again once the range is big enough to lift the average.
        let mut recovering = tracker.clone();
        for _ in 0..4 {
            recovering.add_range_to_fragmentation_estimate(1);
        }
        assert_eq!(recovering.rolling_chunks_per_range().unwrap(), 1.0);
        let allow = recovering.allow_dedup_on_next_range(10);
        assert!(allow);
    }

    #[test]
    #[allow(clippy::float_cmp)] // DEFRAG_MIN_CHUNKS_PER_RANGE and the factor are exact in f32.
    fn defrag_prevention_defaults_match_plan_constants() {
        let tracker = DefragPrevention::default();
        assert_eq!(tracker.window_size(), DEFRAG_NUM_RANGES_WINDOW);
        assert_eq!(
            tracker.min_chunks_per_range(),
            DEFRAG_MIN_CHUNKS_PER_RANGE as f32
        );
        assert_eq!(tracker.hysteresis_factor(), DEFRAG_HYSTERESIS_FACTOR);
        assert_eq!(DEFRAG_MIN_CHUNKS_PER_RANGE, 8);
        assert_eq!(DEFRAG_HYSTERESIS_FACTOR, 0.5);
        assert_eq!(DEFRAG_NUM_RANGES_WINDOW, 128);
    }
}
