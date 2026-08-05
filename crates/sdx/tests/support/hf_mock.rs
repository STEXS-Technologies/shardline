//! A HF-style Xet wire-protocol mock frontend (M7 cross-frontend conformance).
//!
//! This is a **test-support module**, not a product server. It implements the
//! upstream Xet wire protocol shapes that the `sdx` client speaks — token
//! exchange, reconstruction, ranged xorb transfer, global dedup, and xorb/shard
//! upload — mirroring the upstream `xet-data`/`hf-xet`/`xet-runtime` shapes (as
//! implemented by `crates/sdx`'s `auth.rs`, `transfer.rs`, `reconstruction.rs`,
//! `dedup.rs`, and `upload.rs`) rather than shardline's own server internals.
//!
//! The mock stores the xorbs and metadata shards the client uploads, parses the
//! fork-format shard/xorb layouts (the same layout the client serializes), and
//! serves reconstruction responses so the client can round-trip files. The
//! client does the chunking on upload; the mock only stores and re-serves the
//! client's chunks.
//!
//! The path namespace (`tree.rs`/`revisions.rs`) is deliberately NOT served:
//! it is shardline-specific and out of scope for cross-frontend conformance
//! (`docs/SDX_PLAN.md` §4.4.1).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use shardline_xet_adapter::{
    FileReconstructionResponse, FileReconstructionV2Response, ReconstructionChunkRange,
    ReconstructionFetchInfo, ReconstructionMultiRangeFetch, ReconstructionRangeDescriptor,
    ReconstructionTerm, ReconstructionUrlRange, XorbUploadResponse,
};
use tokio::sync::Mutex;
use xet_core_structures::merklehash::MerkleHash;

use sdx::shard::{ShardXorb, ShardXorbChunk};
use sdx::{serialize_shard, xet_hash_hex_string};

// ── fork-format xorb footer constants (mirrors `crates/sdx/src/xorb_build.rs`) ──

const XORB_FORMAT_IDENT: [u8; 7] = *b"XETBLOB";
const XORB_HASHES_SECTION_IDENT: [u8; 7] = *b"XBLBHSH";
const XORB_BOUNDARIES_SECTION_IDENT: [u8; 7] = *b"XBLBBND";
const XORB_FORMAT_VERSION: u8 = 2;
const XORB_HASHES_SECTION_VERSION: u8 = 0;
const XORB_BOUNDARIES_SECTION_VERSION: u8 = 1;

// ── shard layout constants (mirrors `crates/sdx/src/shard.rs`) ──

const SHARD_HEADER_TAG: [u8; 32] = [
    b'H', b'F', b'R', b'e', b'p', b'o', b'M', b'e', b't', b'a', b'D', b'a', b't', b'a', 0, 85, 105,
    103, 69, 106, 123, 129, 87, 131, 165, 189, 217, 92, 205, 209, 74, 169,
];
const SHARD_HEADER_VERSION: u64 = 3;
const SHARD_ENTRY_SIZE: usize = 64;

/// One reconstruction term for a file: a contiguous chunk range in one xorb.
#[derive(Debug, Clone)]
struct Segment {
    xorb_hash: String,
    chunk_start: u64,
    chunk_end: u64,
    unpacked_bytes: u64,
}

/// A stored serialized xorb with its parsed footer metadata.
#[derive(Debug, Clone)]
struct StoredXorb {
    /// Serialized chunk payload (footer excluded).
    payload: Bytes,
    /// Serialized (packed) cumulative boundary offsets per chunk.
    boundaries: Vec<u64>,
    /// Uncompressed cumulative offsets per chunk.
    unpacked_offsets: Vec<u64>,
    /// Chunk hashes, in serialized order.
    chunk_hashes: Vec<MerkleHash>,
}

/// Shared mutable state behind the mock.
#[derive(Debug, Default)]
struct MockState {
    xorbs: HashMap<String, StoredXorb>,
    /// xorb hashes whose payload has been removed (missing-xorb test): metadata
    /// is retained so reconstruction still serves descriptors, but the transfer
    /// route returns 404.
    absent_xorbs: HashSet<String>,
    /// chunk_hash_hex -> serialized shard body served on a dedup hit.
    dedup_shards: HashMap<String, Vec<u8>>,
    /// file_id_hex -> ordered segments.
    file_segments: HashMap<String, Vec<Segment>>,
    /// `X-Xet-Session-Id` values observed on incoming requests.
    session_ids: HashSet<String>,
    // counters (for test assertions)
    read_token_calls: u64,
    write_token_calls: u64,
    xorb_post_count: u64,
    xorb_head_count: u64,
    shard_post_count: u64,
    dedup_queries: u64,
    dedup_hits: u64,
    reconstruction_requests: u64,
}

/// Immutable shared config passed to every handler.
#[derive(Clone)]
struct Shared {
    base_url: String,
    state: Arc<Mutex<MockState>>,
    token_ttl: Arc<std::sync::atomic::AtomicU64>,
    restrict_write: Arc<std::sync::atomic::AtomicBool>,
}

/// The HF-style mock frontend.
pub struct HfMock {
    /// Bound listener port.
    pub port: u16,
    /// Public base URL (`http://127.0.0.1:{port}`).
    pub base_url: String,
    shared: Arc<Shared>,
}

impl HfMock {
    /// Starts the mock on an ephemeral port and returns the handle.
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let base_url = format!("http://127.0.0.1:{port}");

        let shared = Arc::new(Shared {
            base_url: base_url.clone(),
            state: Arc::new(Mutex::new(MockState::default())),
            token_ttl: Arc::new(std::sync::atomic::AtomicU64::new(3600)),
            restrict_write: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });

        let router = Router::new()
            .route(
                "/api/{provider}/{owner}/{repo}/xet-read-token/{rev}",
                get(read_token),
            )
            .route(
                "/api/{provider}/{owner}/{repo}/xet-write-token/{rev}",
                get(write_token),
            )
            .route("/v1/reconstructions/{file_id}", get(reconstruction_v1))
            .route("/v2/reconstructions/{file_id}", get(reconstruction_v2))
            .route("/transfer/xorb/{prefix}/{hash}", get(xorb_get))
            .route("/v1/xorbs/default/{hash}", get(xorb_head).post(xorb_post))
            .route("/v1/shards", post(shard_post))
            .route("/v1/chunks/default-merkledb/{hash}", get(dedup_get))
            .with_state((*shared).clone());

        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        HfMock {
            port,
            base_url,
            shared,
        }
    }

    /// Sets the token TTL in seconds (low values exercise client refresh).
    pub fn set_token_ttl(&self, seconds: u64) {
        self.shared
            .token_ttl
            .store(seconds, std::sync::atomic::Ordering::Relaxed);
    }

    /// Restricts write tokens (issues read-scoped tokens) for the scope test.
    pub fn set_restrict_write(&self, value: bool) {
        self.shared
            .restrict_write
            .store(value, std::sync::atomic::Ordering::Relaxed);
    }

    /// Marks every stored xorb as absent (missing-xorb test).
    pub async fn remove_all_xorbs(&self) {
        let mut state = self.shared.state.lock().await;
        for hash in state.xorbs.keys().cloned().collect::<Vec<_>>() {
            state.absent_xorbs.insert(hash);
        }
    }

    // ── test-assertion accessors ────────────────────────────────────────────

    pub async fn read_token_calls(&self) -> u64 {
        self.shared.state.lock().await.read_token_calls
    }

    pub async fn write_token_calls(&self) -> u64 {
        self.shared.state.lock().await.write_token_calls
    }

    pub async fn xorb_post_count(&self) -> u64 {
        self.shared.state.lock().await.xorb_post_count
    }

    pub async fn xorb_head_count(&self) -> u64 {
        self.shared.state.lock().await.xorb_head_count
    }

    pub async fn shard_post_count(&self) -> u64 {
        self.shared.state.lock().await.shard_post_count
    }

    pub async fn dedup_queries(&self) -> u64 {
        self.shared.state.lock().await.dedup_queries
    }

    pub async fn dedup_hits(&self) -> u64 {
        self.shared.state.lock().await.dedup_hits
    }

    pub async fn reconstruction_requests(&self) -> u64 {
        self.shared.state.lock().await.reconstruction_requests
    }

    pub async fn has_xorb(&self, hash: &str) -> bool {
        self.shared.state.lock().await.xorbs.contains_key(hash)
    }

    pub async fn session_ids(&self) -> Vec<String> {
        let mut ids = self
            .shared
            .state
            .lock()
            .await
            .session_ids
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        ids.sort();
        ids
    }

    pub async fn reset(&self) {
        let mut state = self.shared.state.lock().await;
        state.xorbs.clear();
        state.absent_xorbs.clear();
        state.dedup_shards.clear();
        state.file_segments.clear();
        state.session_ids.clear();
        state.read_token_calls = 0;
        state.write_token_calls = 0;
        state.xorb_post_count = 0;
        state.xorb_head_count = 0;
        state.shard_post_count = 0;
        state.dedup_queries = 0;
        state.dedup_hits = 0;
        state.reconstruction_requests = 0;
    }
}

/// Records the `X-Xet-Session-Id` header if present.
fn record_session(headers: &HeaderMap, state: &mut MockState) {
    static SESSION: HeaderName = HeaderName::from_static("x-xet-session-id");
    if let Some(value) = headers.get(&SESSION).and_then(|value| value.to_str().ok()) {
        state.session_ids.insert(value.to_owned());
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    value.strip_prefix("Bearer ").map(ToOwned::to_owned)
}

fn token_json(base_url: &str, ttl: u64, access: &str) -> serde_json::Value {
    let exp = unix_now().saturating_add(ttl);
    serde_json::json!({
        "casUrl": base_url,
        "exp": exp,
        "accessToken": access,
    })
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

async fn read_token(
    State(shared): State<Shared>,
    headers: HeaderMap,
    Query(_query): Query<serde_json::Value>,
) -> Response {
    let ttl = shared.token_ttl.load(std::sync::atomic::Ordering::Relaxed);
    let mut state = shared.state.lock().await;
    record_session(&headers, &mut state);
    state.read_token_calls = state.read_token_calls.saturating_add(1);
    let access = format!("mock-read-{}", state.read_token_calls);
    (
        StatusCode::OK,
        axum::Json(token_json(&shared.base_url, ttl, &access)),
    )
        .into_response()
}

async fn write_token(
    State(shared): State<Shared>,
    headers: HeaderMap,
    Query(_query): Query<serde_json::Value>,
) -> Response {
    let ttl = shared.token_ttl.load(std::sync::atomic::Ordering::Relaxed);
    let restrict = shared
        .restrict_write
        .load(std::sync::atomic::Ordering::Relaxed);
    let mut state = shared.state.lock().await;
    record_session(&headers, &mut state);
    state.write_token_calls = state.write_token_calls.saturating_add(1);
    let access = if restrict {
        format!("mock-read-{}", state.write_token_calls)
    } else {
        format!("mock-write-{}", state.write_token_calls)
    };
    (
        StatusCode::OK,
        axum::Json(token_json(&shared.base_url, ttl, &access)),
    )
        .into_response()
}

/// Computes the inclusive serialized byte range for chunks `[start, end)`.
fn serialized_byte_range(xorb: &StoredXorb, start: u64, end: u64) -> Option<(u64, u64)> {
    if start >= end || end > u64::try_from(xorb.boundaries.len()).unwrap_or(u64::MAX) {
        return None;
    }
    let first = usize::try_from(start).unwrap_or(usize::MAX);
    let last = usize::try_from(end.saturating_sub(1)).unwrap_or(usize::MAX);
    let packed_start = if start == 0 {
        0
    } else {
        *xorb.boundaries.get(first.saturating_sub(1))?
    };
    let packed_end = *xorb.boundaries.get(last)?;
    Some((packed_start, packed_end))
}

/// A fetch descriptor with its chunk range and serialized byte range.
struct FetchDesc {
    hash: String,
    chunk_start: u64,
    chunk_end: u64,
    packed_start: u64,
    packed_end: u64,
    url: String,
}

/// Builds the reconstruction plan for `file_id`, returning `(offset_into_first,
/// terms, fetches)`. Returns `None` for an unknown/empty file.
fn reconstruction_for(
    state: &MockState,
    base_url: &str,
    file_id: &str,
    range: Option<(u64, u64)>,
) -> Result<Option<(u64, Vec<ReconstructionTerm>, Vec<FetchDesc>)>, StatusCode> {
    let Some(segments) = state.file_segments.get(file_id) else {
        return Ok(None);
    };
    if segments.is_empty() {
        return Ok(None);
    }
    let total = segments
        .iter()
        .fold(0u64, |acc, seg| acc.saturating_add(seg.unpacked_bytes));
    if total == 0 {
        return Ok(None);
    }
    let (start, mut end_incl) = range.unwrap_or_else(|| (0, total.saturating_sub(1)));
    // Clamp the end to the file boundary; a range starting at/after EOF is
    // unsatisfiable (the reference server treats end-past-EOF by returning the
    // available bytes, and only 416s a range that starts past the end).
    if start >= total {
        return Err(StatusCode::RANGE_NOT_SATISFIABLE);
    }
    end_incl = end_incl.min(total.saturating_sub(1));

    let mut offset_into_first_range = 0u64;
    let mut first = true;
    let mut terms = Vec::new();
    let mut fetches = Vec::new();
    let mut cursor = 0u64;
    for seg in segments {
        let seg_end = cursor.saturating_add(seg.unpacked_bytes);
        if seg_end <= start || cursor > end_incl {
            cursor = seg_end;
            continue;
        }
        if first {
            offset_into_first_range = start.saturating_sub(cursor);
            first = false;
        }
        let xorb = state.xorbs.get(&seg.xorb_hash);
        let (packed_start, packed_end) = xorb
            .and_then(|x| serialized_byte_range(x, seg.chunk_start, seg.chunk_end))
            .ok_or(StatusCode::NOT_FOUND)?;
        terms.push(ReconstructionTerm {
            hash: seg.xorb_hash.clone(),
            unpacked_length: seg.unpacked_bytes,
            range: ReconstructionChunkRange {
                start: seg.chunk_start,
                end: seg.chunk_end,
            },
        });
        fetches.push(FetchDesc {
            hash: seg.xorb_hash.clone(),
            chunk_start: seg.chunk_start,
            chunk_end: seg.chunk_end,
            packed_start,
            packed_end,
            url: xorb_transfer_url(base_url, &seg.xorb_hash),
        });
        cursor = seg_end;
    }
    if first {
        return Err(StatusCode::RANGE_NOT_SATISFIABLE);
    }
    Ok(Some((offset_into_first_range, terms, fetches)))
}

fn xorb_transfer_url(base_url: &str, hash: &str) -> String {
    let prefix = hash.get(..4).unwrap_or(hash);
    format!("{base_url}/transfer/xorb/{prefix}/{hash}")
}

fn valid_hash(hash: &str) -> bool {
    hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit())
}

fn parse_range_header(headers: &HeaderMap) -> Option<(u64, u64)> {
    let value = headers.get(header::RANGE)?.to_str().ok()?;
    let value = value.strip_prefix("bytes=")?;
    let (start, end) = value.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

async fn reconstruction_v1(
    State(shared): State<Shared>,
    headers: HeaderMap,
    Path(file_id): Path<String>,
) -> Response {
    serve_reconstruction(&shared, &headers, &file_id, false).await
}

async fn reconstruction_v2(
    State(shared): State<Shared>,
    headers: HeaderMap,
    Path(file_id): Path<String>,
) -> Response {
    serve_reconstruction(&shared, &headers, &file_id, true).await
}

async fn serve_reconstruction(
    shared: &Shared,
    headers: &HeaderMap,
    file_id: &str,
    v2: bool,
) -> Response {
    if !valid_hash(file_id) {
        return (StatusCode::BAD_REQUEST, "invalid file id").into_response();
    }
    if bearer_token(headers).is_none() {
        return (StatusCode::UNAUTHORIZED, "missing token").into_response();
    }
    let range = parse_range_header(headers);
    let mut state = shared.state.lock().await;
    record_session(headers, &mut state);
    state.reconstruction_requests = state.reconstruction_requests.saturating_add(1);
    let base_url = shared.base_url.clone();
    let plan = match reconstruction_for(&state, &base_url, file_id, range) {
        Ok(Some(plan)) => plan,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, "file not found").into_response();
        }
        Err(status) => return (status, "range not satisfiable").into_response(),
    };
    let (offset_into_first_range, terms, fetches) = plan;
    if v2 {
        let mut xorbs: BTreeMap<String, Vec<ReconstructionMultiRangeFetch>> = BTreeMap::new();
        for fetch in fetches {
            xorbs
                .entry(fetch.hash.clone())
                .or_default()
                .push(ReconstructionMultiRangeFetch {
                    url: fetch.url,
                    ranges: vec![ReconstructionRangeDescriptor {
                        chunks: ReconstructionChunkRange {
                            start: fetch.chunk_start,
                            end: fetch.chunk_end,
                        },
                        bytes: ReconstructionUrlRange {
                            start: fetch.packed_start,
                            end: fetch.packed_end,
                        },
                    }],
                });
        }
        let resp = FileReconstructionV2Response {
            offset_into_first_range,
            terms,
            xorbs,
        };
        (StatusCode::OK, axum::Json(resp)).into_response()
    } else {
        let mut fetch_info: BTreeMap<String, Vec<ReconstructionFetchInfo>> = BTreeMap::new();
        for fetch in fetches {
            fetch_info
                .entry(fetch.hash.clone())
                .or_default()
                .push(ReconstructionFetchInfo {
                    range: ReconstructionChunkRange {
                        start: fetch.chunk_start,
                        end: fetch.chunk_end,
                    },
                    url: fetch.url,
                    url_range: ReconstructionUrlRange {
                        start: fetch.packed_start,
                        end: fetch.packed_end,
                    },
                });
        }
        let resp = FileReconstructionResponse {
            offset_into_first_range,
            terms,
            fetch_info,
        };
        (StatusCode::OK, axum::Json(resp)).into_response()
    }
}

/// Serves a ranged xorb payload from the stored serialized bytes.
async fn xorb_get(
    State(shared): State<Shared>,
    headers: HeaderMap,
    Path((_prefix, hash)): Path<(String, String)>,
) -> Response {
    let mut state = shared.state.lock().await;
    record_session(&headers, &mut state);
    if state.absent_xorbs.contains(&hash) {
        return (StatusCode::NOT_FOUND, "xorb not found").into_response();
    }
    let Some(xorb) = state.xorbs.get(&hash) else {
        return (StatusCode::NOT_FOUND, "xorb not found").into_response();
    };
    let payload = &xorb.payload;
    let payload_len = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    if payload_len == 0 {
        return (StatusCode::OK, Bytes::new()).into_response();
    }
    let Some((start, end)) = parse_range_header(&headers) else {
        return (StatusCode::OK, Bytes::copy_from_slice(payload)).into_response();
    };
    let last = payload_len.saturating_sub(1);
    let start = start.min(last);
    let end = end.min(last);
    let slice_start = usize::try_from(start).unwrap_or(usize::MAX);
    let slice_end = usize::try_from(end.saturating_add(1)).unwrap_or(usize::MAX);
    let Some(data) = payload.get(slice_start..slice_end) else {
        return (StatusCode::RANGE_NOT_SATISFIABLE, "range not satisfiable").into_response();
    };
    let content_range = format!("bytes {start}-{end}/{payload_len}");
    (
        StatusCode::PARTIAL_CONTENT,
        [(
            header::CONTENT_RANGE,
            content_range.parse::<HeaderValue>().expect("valid"),
        )],
        Bytes::copy_from_slice(data),
    )
        .into_response()
}

async fn xorb_head(
    State(shared): State<Shared>,
    headers: HeaderMap,
    Path(hash): Path<String>,
) -> Response {
    let mut state = shared.state.lock().await;
    record_session(&headers, &mut state);
    if state.absent_xorbs.contains(&hash) || !state.xorbs.contains_key(&hash) {
        StatusCode::NOT_FOUND.into_response()
    } else {
        StatusCode::OK.into_response()
    }
}

async fn xorb_post(
    State(shared): State<Shared>,
    headers: HeaderMap,
    Path(hash): Path<String>,
    body: axum::body::Bytes,
) -> Response {
    let restrict = shared
        .restrict_write
        .load(std::sync::atomic::Ordering::Relaxed);
    let mut state = shared.state.lock().await;
    record_session(&headers, &mut state);
    if !auth_ok(&headers, restrict) {
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }
    let inserted = !state.xorbs.contains_key(&hash);
    if inserted {
        let Ok(stored) = parse_fork_xorb(&body) else {
            return (StatusCode::BAD_REQUEST, "cannot parse xorb footer").into_response();
        };
        let shard = build_dedup_shard(&stored, &hash);
        for chunk_hash in &stored.chunk_hashes {
            state
                .dedup_shards
                .insert(xet_hash_hex_string(*chunk_hash), shard.clone());
        }
        state.xorbs.insert(hash.clone(), stored);
    }
    state.xorb_post_count = state.xorb_post_count.saturating_add(1);
    (
        StatusCode::OK,
        axum::Json(XorbUploadResponse {
            was_inserted: inserted,
        }),
    )
        .into_response()
}

fn auth_ok(headers: &HeaderMap, restrict_write: bool) -> bool {
    let Some(token) = bearer_token(headers) else {
        return false;
    };
    if restrict_write {
        return false;
    }
    token.starts_with("mock-write-")
}

/// Builds a serialized dedup shard listing a single xorb (the one whose chunks
/// the client just uploaded), so a later dedup hit finds them.
fn build_dedup_shard(stored: &StoredXorb, xorb_hash_hex: &str) -> Vec<u8> {
    let mut chunks = Vec::with_capacity(stored.chunk_hashes.len());
    let mut unpacked = 0u64;
    for (index, hash) in stored.chunk_hashes.iter().enumerate() {
        let prev = stored
            .unpacked_offsets
            .get(index.saturating_sub(1))
            .copied()
            .unwrap_or(0);
        let len = stored
            .unpacked_offsets
            .get(index)
            .copied()
            .unwrap_or(0)
            .saturating_sub(prev);
        chunks.push(ShardXorbChunk {
            chunk_hash: *hash,
            chunk_byte_range_start: unpacked,
            unpacked_segment_bytes: len,
            flags: 0,
        });
        unpacked = unpacked.saturating_add(len);
    }
    let xorb_hash = sdx::parse_xet_hash_hex(xorb_hash_hex).expect("valid xorb hash");
    let xorb = ShardXorb {
        xorb_hash,
        num_bytes_in_xorb: stored.boundaries.last().copied().unwrap_or(0),
        chunks,
    };
    serialize_shard(&[], &[xorb])
}

async fn shard_post(
    State(shared): State<Shared>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let restrict = shared
        .restrict_write
        .load(std::sync::atomic::Ordering::Relaxed);
    let mut state = shared.state.lock().await;
    record_session(&headers, &mut state);
    if !auth_ok(&headers, restrict) {
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }
    let Ok(files) = parse_shard_files(&body) else {
        return (StatusCode::BAD_REQUEST, "cannot parse shard").into_response();
    };
    for (file_id, segments) in files {
        state.file_segments.insert(file_id, segments);
    }
    state.shard_post_count = state.shard_post_count.saturating_add(1);
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({ "result": 1 })),
    )
        .into_response()
}

async fn dedup_get(
    State(shared): State<Shared>,
    headers: HeaderMap,
    Path(hash): Path<String>,
) -> Response {
    let mut state = shared.state.lock().await;
    record_session(&headers, &mut state);
    state.dedup_queries = state.dedup_queries.saturating_add(1);
    let hit = state.dedup_shards.get(&hash).cloned();
    if hit.is_some() {
        state.dedup_hits = state.dedup_hits.saturating_add(1);
    }
    hit.map_or_else(
        || (StatusCode::NOT_FOUND, "chunk not stored").into_response(),
        |shard_body| (StatusCode::OK, Bytes::copy_from_slice(&shard_body)).into_response(),
    )
}

// ── fork-format xorb footer parser ─────────────────────────────────────────

fn parse_fork_xorb(full: &[u8]) -> Result<StoredXorb, ()> {
    let len = full.len();
    if len < 8 {
        return Err(());
    }
    let info_length = read_u64_at(full, len.saturating_sub(8))?;
    // `info_length` counts everything before the trailing 8-byte length field.
    let footer_start = len
        .saturating_sub(usize::try_from(info_length).map_err(|_| ())?)
        .saturating_sub(8);
    if footer_start >= len {
        return Err(());
    }
    let payload = Bytes::copy_from_slice(full.get(..footer_start).ok_or(())?);
    let footer = full.get(footer_start..).ok_or(())?;
    let mut offset = 0usize;

    // "XETBLOB" | version | xorb_hash(32)
    if footer.get(..7) != Some(&XORB_FORMAT_IDENT[..]) {
        return Err(());
    }
    offset = offset
        .saturating_add(7)
        .saturating_add(1)
        .saturating_add(32);

    // "XBLBHSH" | hashes_version | num_chunks(u64) | chunk_hashes(32·n)
    if footer.get(offset..offset.saturating_add(7)) != Some(&XORB_HASHES_SECTION_IDENT[..]) {
        return Err(());
    }
    offset = offset.saturating_add(7).saturating_add(1);
    let num_chunks = read_u64_at(footer, offset)?;
    offset = offset.saturating_add(8);
    let hashes_bytes = usize::try_from(num_chunks)
        .map_err(|_| ())?
        .saturating_mul(32);
    let hashes_region = footer
        .get(offset..offset.saturating_add(hashes_bytes))
        .ok_or(())?;
    let mut chunk_hashes = Vec::with_capacity(hashes_bytes / 32);
    for i in 0..(hashes_bytes / 32) {
        let start = i.saturating_mul(32);
        let h = hashes_region
            .get(start..start.saturating_add(32))
            .ok_or(())?;
        chunk_hashes.push(MerkleHash::from(<[u8; 32]>::try_from(h).map_err(|_| ())?));
    }
    offset = offset.saturating_add(hashes_bytes);

    // "XBLBBND" | boundaries_version | num_chunks(u64) | boundaries(u64·n) | unpacked(u64·n)
    if footer.get(offset..offset.saturating_add(7)) != Some(&XORB_BOUNDARIES_SECTION_IDENT[..]) {
        return Err(());
    }
    offset = offset.saturating_add(7).saturating_add(1);
    let bnum = read_u64_at(footer, offset)?;
    if bnum != num_chunks {
        return Err(());
    }
    offset = offset.saturating_add(8);
    let mut boundaries = Vec::with_capacity(usize::try_from(num_chunks).map_err(|_| ())?);
    for _ in 0..num_chunks {
        boundaries.push(read_u64_at(footer, offset)?);
        offset = offset.saturating_add(8);
    }
    let mut unpacked_offsets = Vec::with_capacity(usize::try_from(num_chunks).map_err(|_| ())?);
    for _ in 0..num_chunks {
        unpacked_offsets.push(read_u64_at(footer, offset)?);
        offset = offset.saturating_add(8);
    }

    Ok(StoredXorb {
        payload,
        boundaries,
        unpacked_offsets,
        chunk_hashes,
    })
}

fn read_u64_at(bytes: &[u8], offset: usize) -> Result<u64, ()> {
    let slice = bytes.get(offset..offset.saturating_add(8)).ok_or(())?;
    let arr: [u8; 8] = slice.try_into().map_err(|_| ())?;
    Ok(u64::from_le_bytes(arr))
}

// ── fork-format shard file-section parser ──────────────────────────────────

fn parse_shard_files(body: &[u8]) -> Result<HashMap<String, Vec<Segment>>, ()> {
    let mut reader = Cursor::new(body);
    let tag = reader.take(32)?;
    if tag != &SHARD_HEADER_TAG[..] {
        return Err(());
    }
    let version = reader.take_u64()?;
    let _footer_size = reader.take_u64()?;
    let entry_size = entry_size(version);

    let mut files: HashMap<String, Vec<Segment>> = HashMap::new();
    loop {
        let header = reader.take(entry_size)?;
        let file_hash = read_hash(header)?;
        if is_bookend(&file_hash) {
            break;
        }
        let num_entries = read_file_num_entries(header, version)?;
        let flags = read_flags(header)?;
        let mut segments = Vec::with_capacity(usize::try_from(num_entries).unwrap_or(0));
        for _ in 0..num_entries {
            let entry = reader.take(entry_size)?;
            segments.push(read_segment(entry)?);
        }
        if flags & (1 << 31) != 0 {
            for _ in 0..num_entries {
                let _ = reader.take(entry_size)?;
            }
        }
        if flags & (1 << 30) != 0 {
            let _ = reader.take(entry_size)?;
        }
        files.insert(xet_hash_hex_string(file_hash), segments);
    }
    Ok(files)
}

fn entry_size(version: u64) -> usize {
    if version == 2 { 48 } else { SHARD_ENTRY_SIZE }
}

fn read_hash(entry: &[u8]) -> Result<MerkleHash, ()> {
    let h = entry.get(..32).ok_or(())?;
    Ok(MerkleHash::from(<[u8; 32]>::try_from(h).map_err(|_| ())?))
}

fn is_bookend(hash: &MerkleHash) -> bool {
    hash == &MerkleHash::from([0xFFu8; 32])
}

fn read_file_num_entries(entry: &[u8], version: u64) -> Result<u64, ()> {
    if version == 2 {
        Ok(u64::from(read_u32_at(entry, 32)?))
    } else {
        read_u64_at(entry, 36)
    }
}

fn read_flags(entry: &[u8]) -> Result<u32, ()> {
    read_u32_at(entry, 32)
}

fn read_segment(entry: &[u8]) -> Result<Segment, ()> {
    let xorb_hash = xet_hash_hex_string(read_hash(entry)?);
    let unpacked_bytes = read_u64_at(entry, 36)?;
    let chunk_start = read_u64_at(entry, 44)?;
    let chunk_end = read_u64_at(entry, 52)?;
    Ok(Segment {
        xorb_hash,
        chunk_start,
        chunk_end,
        unpacked_bytes,
    })
}

fn read_u32_at(entry: &[u8], offset: usize) -> Result<u32, ()> {
    let slice = entry.get(offset..offset.saturating_add(4)).ok_or(())?;
    let arr: [u8; 4] = slice.try_into().map_err(|_| ())?;
    Ok(u32::from_le_bytes(arr))
}

/// Minimal byte cursor for shard parsing.
struct Cursor<'buf> {
    bytes: &'buf [u8],
    pos: usize,
}

impl<'buf> Cursor<'buf> {
    fn new(bytes: &'buf [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'buf [u8], ()> {
        let start = self.pos;
        let end = start.saturating_add(len);
        let out = self.bytes.get(start..end).ok_or(())?;
        self.pos = end;
        Ok(out)
    }

    fn take_u64(&mut self) -> Result<u64, ()> {
        let slice = self.take(8)?;
        Ok(u64::from_le_bytes(slice.try_into().map_err(|_| ())?))
    }
}

/// Deterministic pseudo-random test content (the mock's own synthesis).
pub fn deterministic_content(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as u8
        })
        .collect()
}
