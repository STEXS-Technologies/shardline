//! Reconstruction orchestration for the sdx CAS read path (M2a).
//!
//! Requests a file reconstruction from the shardline server (V2 first, with a
//! V1 fallback on 404/501, mirroring `xet-client-1.5.4`'s
//! `get_reconstruction_with_version_override`), fetches the referenced xorb
//! byte ranges, decodes and validates the chunks, and assembles the requested
//! byte range.
//!
//! The response shape matches the adapter's `FileReconstructionResponse` /
//! `FileReconstructionV2Response` (`crates/shardline-xet-adapter/src/model.rs`)
//! served by `GET /v1/reconstructions/{file_id}` and
//! `GET /v2/reconstructions/{file_id}` (`crates/shardline-server/src/app.rs`).
//! Range semantics follow `docs/PROTOCOL_CONFORMANCE.md`: reconstruction range
//! ends are inclusive, chunk ranges are end-exclusive, xorb URL byte ranges are
//! inclusive, and the first term may carry an offset into its first chunk.

use std::collections::HashMap;

use shardline_xet_adapter::{
    FileReconstructionResponse, FileReconstructionV2Response, ReconstructionTerm,
};
use xet_core_structures::merklehash::MerkleHash;

use crate::{
    error::{SdxError, TransferError},
    hash::compute_term_verification_hash,
    transfer::{ByteRange, TransferClient},
    xorb::{DecodedChunk, XorbReader},
};

/// A fully reconstructed byte range (or full file) with its resolved terms.
#[derive(Debug, Clone)]
pub struct ReconstructedFile {
    /// Assembled bytes of the requested range (or the full file).
    pub data: Vec<u8>,
    /// Ordered terms that produced `data`, in download order.
    pub terms: Vec<ResolvedTerm>,
}

/// One resolved reconstruction term.
#[derive(Debug, Clone)]
pub struct ResolvedTerm {
    /// Xorb hash this term was sourced from.
    pub xorb_hash: String,
    /// Chunk range within the xorb, end-exclusive.
    pub chunk_range: (u64, u64),
    /// Declared decompressed length of the term's chunk range.
    pub unpacked_length: u64,
    /// Verification hash (`VERIFICATION_KEY`) over the term's chunk data
    /// hashes (`compute_term_verification_hash`).
    pub verification_hash: MerkleHash,
    /// Byte offset of this term's data within the assembled output.
    pub output_offset: u64,
}

/// Reconstructs `file_id`, returning the requested byte range (or the whole
/// file when `requested_range` is `None`).
///
/// `api_base` is the API control-plane base URL and `token` is an opaque
/// read-scoped bearer token from [`crate::TokenService`].
///
/// # Errors
///
/// Returns [`SdxError`] when reconstruction fails, a referenced xorb range
/// cannot be fetched or decoded, a term's `unpacked_length` disagrees with the
/// decoded bytes, or the requested range is past the end of the file.
pub async fn reconstruct(
    transfer: &TransferClient,
    api_base: &str,
    token: &str,
    file_id: &str,
    requested_range: Option<ByteRange>,
) -> Result<ReconstructedFile, SdxError> {
    let plan = fetch_reconstruction(transfer, api_base, token, file_id, requested_range).await?;
    assemble(transfer, token, plan, requested_range).await
}

/// Fetches the reconstruction plan, preferring V2 and falling back to V1 on
/// 404/501 (V2 not served).
async fn fetch_reconstruction(
    transfer: &TransferClient,
    api_base: &str,
    token: &str,
    file_id: &str,
    requested_range: Option<ByteRange>,
) -> Result<Reconstruction, SdxError> {
    match transfer
        .reconstruction_v2(api_base, token, file_id, requested_range)
        .await
    {
        Ok(response) => Ok(Reconstruction::from_v2(response)?),
        Err(TransferError::NotFound(_) | TransferError::HttpStatus { status: 501, .. }) => {
            // V2 not available on this frontend: fall back to V1
            // (SDX_PLAN.md §7 item 7, cross-frontend M7).
            let response = transfer
                .reconstruction_v1(api_base, token, file_id, requested_range)
                .await?;
            Ok(Reconstruction::from_v1(response)?)
        }
        Err(error) => Err(error.into()),
    }
}

/// Fetches every unique xorb range, decodes its chunks, resolves each term,
/// validates `unpacked_length`, and assembles the output.
async fn assemble(
    transfer: &TransferClient,
    token: &str,
    plan: Reconstruction,
    requested_range: Option<ByteRange>,
) -> Result<ReconstructedFile, SdxError> {
    let mut decoded_fetches: HashMap<usize, Vec<DecodedChunk>> = HashMap::new();
    for (fetch_index, fetch) in plan.fetches.iter().enumerate() {
        let ranged = transfer
            .fetch_xorb_range(&fetch.url, token, fetch.bytes)
            .await?;
        let chunks = XorbReader::new(ranged.data).decode_chunks()?;
        let expected_chunks = fetch.chunks.1.saturating_sub(fetch.chunks.0);
        let actual_chunks = u64::try_from(chunks.len()).unwrap_or(u64::MAX);
        if actual_chunks != expected_chunks {
            return Err(SdxError::FetchChunkCountMismatch {
                url: fetch.url.clone(),
                expected: expected_chunks,
                actual: actual_chunks,
            });
        }
        decoded_fetches.insert(fetch_index, chunks);
    }

    let mut output = Vec::new();
    let mut resolved_terms = Vec::with_capacity(plan.terms.len());
    for (term_index, term) in plan.terms.iter().enumerate() {
        let fetch =
            plan.fetches
                .get(term.fetch_index)
                .ok_or_else(|| SdxError::MissingFetchInfo {
                    term_index,
                    hash: term.xorb_hash.clone(),
                })?;
        let chunks =
            decoded_fetches
                .get(&term.fetch_index)
                .ok_or_else(|| SdxError::MissingFetchInfo {
                    term_index,
                    hash: term.xorb_hash.clone(),
                })?;
        let local_start = term
            .chunk_range
            .0
            .checked_sub(fetch.chunks.0)
            .ok_or_else(|| SdxError::MissingFetchInfo {
                term_index,
                hash: term.xorb_hash.clone(),
            })?;
        let term_chunk_count = term
            .chunk_range
            .1
            .checked_sub(term.chunk_range.0)
            .ok_or_else(|| SdxError::MissingFetchInfo {
                term_index,
                hash: term.xorb_hash.clone(),
            })?;
        let start = usize::try_from(local_start).unwrap_or(usize::MAX);
        let count = usize::try_from(term_chunk_count).unwrap_or(usize::MAX);
        let term_chunks = chunks
            .get(start..start.saturating_add(count))
            .ok_or_else(|| SdxError::MissingFetchInfo {
                term_index,
                hash: term.xorb_hash.clone(),
            })?;

        let decoded_len: u64 = term_chunks
            .iter()
            .map(|chunk| chunk.data.len() as u64)
            .sum();
        if decoded_len != term.unpacked_length {
            return Err(SdxError::UnpackedLengthMismatch {
                term_index,
                expected: term.unpacked_length,
                actual: decoded_len,
            });
        }

        // Only the first term may start mid-chunk; the offset comes from the
        // reconstruction response.
        let offset = if term_index == 0 {
            plan.offset_into_first_range
        } else {
            0
        };
        if offset > term.unpacked_length {
            return Err(SdxError::UnpackedLengthMismatch {
                term_index,
                expected: term.unpacked_length,
                actual: offset,
            });
        }
        let term_byte_size = term.unpacked_length.saturating_sub(offset);

        let mut term_data = Vec::with_capacity(decoded_len as usize);
        for chunk in term_chunks {
            term_data.extend_from_slice(&chunk.data);
        }
        term_data.drain(..usize::try_from(offset).unwrap_or(usize::MAX));
        term_data.truncate(usize::try_from(term_byte_size).unwrap_or(usize::MAX));

        let chunk_hashes: Vec<MerkleHash> = term_chunks.iter().map(|chunk| chunk.hash).collect();
        let verification_hash = compute_term_verification_hash(&chunk_hashes);

        resolved_terms.push(ResolvedTerm {
            xorb_hash: term.xorb_hash.clone(),
            chunk_range: term.chunk_range,
            unpacked_length: term.unpacked_length,
            verification_hash,
            output_offset: u64::try_from(output.len()).unwrap_or(u64::MAX),
        });
        output.extend_from_slice(&term_data);
    }

    if let Some(range) = requested_range {
        let expected = range.len();
        let actual = u64::try_from(output.len()).unwrap_or(u64::MAX);
        if actual < expected {
            return Err(SdxError::RangePastEnd {
                start: range.start,
                end: range.end,
            });
        }
        output.truncate(usize::try_from(expected).unwrap_or(usize::MAX));
    }

    Ok(ReconstructedFile {
        data: output,
        terms: resolved_terms,
    })
}

/// A normalized reconstruction plan: ordered terms plus the deduplicated set of
/// xorb byte ranges that must be fetched.
#[derive(Debug)]
struct Reconstruction {
    offset_into_first_range: u64,
    terms: Vec<TermPlan>,
    fetches: Vec<FetchEntry>,
}

impl Reconstruction {
    fn from_v1(response: FileReconstructionResponse) -> Result<Self, SdxError> {
        let mut raw_fetches = Vec::new();
        for (hash, entries) in response.fetch_info {
            for entry in entries {
                raw_fetches.push(FetchEntry {
                    xorb_hash: hash.clone(),
                    url: entry.url,
                    bytes: ByteRange::new(entry.url_range.start, entry.url_range.end),
                    chunks: (entry.range.start, entry.range.end),
                });
            }
        }
        Self::build(
            response.offset_into_first_range,
            &response.terms,
            raw_fetches,
        )
    }

    fn from_v2(response: FileReconstructionV2Response) -> Result<Self, SdxError> {
        let mut raw_fetches = Vec::new();
        for (hash, entries) in response.xorbs {
            for entry in entries {
                for descriptor in entry.ranges {
                    raw_fetches.push(FetchEntry {
                        xorb_hash: hash.clone(),
                        url: entry.url.clone(),
                        bytes: ByteRange::new(descriptor.bytes.start, descriptor.bytes.end),
                        chunks: (descriptor.chunks.start, descriptor.chunks.end),
                    });
                }
            }
        }
        Self::build(
            response.offset_into_first_range,
            &response.terms,
            raw_fetches,
        )
    }

    fn build(
        offset_into_first_range: u64,
        terms: &[ReconstructionTerm],
        raw_fetches: Vec<FetchEntry>,
    ) -> Result<Self, SdxError> {
        // Coalesce fetch entries per xorb: shardline's reconstruction emits one
        // fetch entry per chunk, so a large file would otherwise produce one
        // HTTP request per chunk. Chunks of one xorb are serialized
        // contiguously, so merging adjacent/overlapping chunk ranges into a
        // single byte range fetches exactly the same data in one request.
        // (Mirrors the reference client's per-xorb ranged fetch.)
        let mut groups: HashMap<String, Vec<FetchEntry>> = HashMap::new();
        for fetch in raw_fetches {
            groups.entry(fetch.url.clone()).or_default().push(fetch);
        }
        let mut fetches: Vec<FetchEntry> = Vec::new();
        for mut group in groups.into_values() {
            group.sort_by_key(|fetch| (fetch.chunks.0, fetch.chunks.1));
            let mut merged: Vec<FetchEntry> = Vec::new();
            for fetch in group {
                match merged.last_mut() {
                    Some(entry) if entry.chunks.1 >= fetch.chunks.0 => {
                        entry.chunks.1 = entry.chunks.1.max(fetch.chunks.1);
                        entry.bytes.end = entry.bytes.end.max(fetch.bytes.end);
                    }
                    _ => merged.push(fetch),
                }
            }
            fetches.extend(merged);
        }

        let mut planned_terms = Vec::with_capacity(terms.len());
        for (term_index, term) in terms.iter().enumerate() {
            let fetch_index = fetches
                .iter()
                .position(|fetch| {
                    fetch.xorb_hash == term.hash
                        && fetch.chunks.0 <= term.range.start
                        && term.range.end <= fetch.chunks.1
                })
                .ok_or_else(|| SdxError::MissingFetchInfo {
                    term_index,
                    hash: term.hash.clone(),
                })?;
            planned_terms.push(TermPlan {
                xorb_hash: term.hash.clone(),
                chunk_range: (term.range.start, term.range.end),
                unpacked_length: term.unpacked_length,
                fetch_index,
            });
        }

        Ok(Self {
            offset_into_first_range,
            terms: planned_terms,
            fetches,
        })
    }
}

/// A planned term: which deduplicated fetch sources it and how to slice it.
#[derive(Debug)]
struct TermPlan {
    xorb_hash: String,
    chunk_range: (u64, u64),
    unpacked_length: u64,
    fetch_index: usize,
}

/// One deduplicated xorb byte-range fetch.
#[derive(Debug)]
struct FetchEntry {
    xorb_hash: String,
    url: String,
    bytes: ByteRange,
    chunks: (u64, u64),
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path},
    };

    use super::Reconstruction;
    use crate::{error::SdxError, hash::compute_chunk_hash, transfer::TransferClient};

    const FILE_ID: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    const XORB_HASH: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    fn transfer_client() -> TransferClient {
        TransferClient::new(reqwest::Client::new())
    }

    async fn xorb_range_mock(server: &MockServer, hash: &str, start: u64, end: u64, body: Vec<u8>) {
        Mock::given(method("GET"))
            .and(path(format!("/transfer/xorb/default/{hash}")))
            .and(header("authorization", "Bearer read-token"))
            .and(header("range", format!("bytes={start}-{end}")))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header(
                        "Content-Range",
                        format!("bytes {start}-{end}/{}", body.len()),
                    )
                    .set_body_raw(body, "application/octet-stream"),
            )
            .mount(server)
            .await;
    }

    /// Serializes chunk payloads with the pinned upstream serializer.
    fn serialize_payload(chunks: &[&[u8]]) -> Vec<u8> {
        use xet_core_structures::xorb_object::{CompressionScheme, serialize_chunk};
        let mut payload = Vec::new();
        for chunk in chunks {
            serialize_chunk(chunk, &mut payload, CompressionScheme::None).unwrap();
        }
        payload
    }

    fn v2_response_body(
        offset: u64,
        terms: serde_json::Value,
        xorbs: serde_json::Value,
    ) -> serde_json::Value {
        let mut body = serde_json::Map::new();
        body.insert(
            "offset_into_first_range".to_owned(),
            serde_json::Value::from(offset),
        );
        body.insert("terms".to_owned(), terms);
        body.insert("xorbs".to_owned(), xorbs);
        serde_json::Value::Object(body)
    }

    #[test]
    fn plan_from_v2_merges_contiguous_ranges_and_resolves_terms() {
        let xorbs = json!({
            XORB_HASH: [{
                "url": format!("http://cas/transfer/xorb/default/{XORB_HASH}"),
                "ranges": [
                    {"chunks": {"start": 0, "end": 2}, "bytes": {"start": 0, "end": 99}},
                    {"chunks": {"start": 2, "end": 4}, "bytes": {"start": 100, "end": 199}}
                ]
            }]
        });
        let terms = json!([
            {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 2}},
            {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 2, "end": 4}}
        ]);
        let body = v2_response_body(0, terms, xorbs);
        let response: shardline_xet_adapter::FileReconstructionV2Response =
            serde_json::from_value(body).unwrap();
        let plan = Reconstruction::from_v2(response).unwrap();
        // Contiguous chunk ranges in one xorb coalesce into a single fetch.
        assert_eq!(plan.fetches.len(), 1);
        assert_eq!(plan.fetches[0].chunks, (0, 4));
        assert_eq!(plan.fetches[0].bytes, super::ByteRange::new(0, 199));
        assert_eq!(plan.terms.len(), 2);
        assert_eq!(plan.terms[0].fetch_index, 0);
        assert_eq!(plan.terms[1].fetch_index, 0);
    }

    #[test]
    fn plan_keeps_disjoint_chunk_ranges_as_separate_fetches() {
        let xorbs = json!({
            XORB_HASH: [{
                "url": format!("http://cas/transfer/xorb/default/{XORB_HASH}"),
                "ranges": [
                    {"chunks": {"start": 0, "end": 2}, "bytes": {"start": 0, "end": 99}},
                    {"chunks": {"start": 5, "end": 7}, "bytes": {"start": 300, "end": 399}}
                ]
            }]
        });
        let terms = json!([
            {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 2}},
            {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 5, "end": 7}}
        ]);
        let body = v2_response_body(0, terms, xorbs);
        let response: shardline_xet_adapter::FileReconstructionV2Response =
            serde_json::from_value(body).unwrap();
        let plan = Reconstruction::from_v2(response).unwrap();
        // A gap between chunk ranges keeps them as separate fetches.
        assert_eq!(plan.fetches.len(), 2);
        assert_eq!(plan.terms[0].fetch_index, 0);
        assert_eq!(plan.terms[1].fetch_index, 1);
    }

    #[test]
    fn plan_from_v2_deduplicates_identical_fetch_ranges() {
        let xorbs = json!({
            XORB_HASH: [{
                "url": format!("http://cas/transfer/xorb/default/{XORB_HASH}"),
                "ranges": [
                    {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": 9}},
                    {"chunks": {"start": 1, "end": 2}, "bytes": {"start": 0, "end": 9}}
                ]
            }]
        });
        let terms = json!([
            {"hash": XORB_HASH, "unpacked_length": 5, "range": {"start": 0, "end": 1}},
            {"hash": XORB_HASH, "unpacked_length": 5, "range": {"start": 1, "end": 2}}
        ]);
        let body = v2_response_body(0, terms, xorbs);
        let response: shardline_xet_adapter::FileReconstructionV2Response =
            serde_json::from_value(body).unwrap();
        let plan = Reconstruction::from_v2(response).unwrap();
        // Both descriptors point at the same byte range → one fetch whose chunk
        // range is the union covering both terms.
        assert_eq!(plan.fetches.len(), 1);
        assert_eq!(plan.fetches[0].chunks, (0, 2));
        assert_eq!(plan.terms[0].fetch_index, 0);
        assert_eq!(plan.terms[1].fetch_index, 0);
    }

    #[test]
    fn plan_from_v1_uses_fetch_info_shape() {
        let body = json!({
            "offset_into_first_range": 3,
            "terms": [
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 2}}
            ],
            "fetch_info": {
                XORB_HASH: [{
                    "range": {"start": 0, "end": 2},
                    "url": format!("http://cas/transfer/xorb/default/{XORB_HASH}"),
                    "url_range": {"start": 0, "end": 99}
                }]
            }
        });
        let response: shardline_xet_adapter::FileReconstructionResponse =
            serde_json::from_value(body).unwrap();
        let plan = Reconstruction::from_v1(response).unwrap();
        assert_eq!(plan.offset_into_first_range, 3);
        assert_eq!(plan.fetches.len(), 1);
        assert_eq!(plan.fetches[0].bytes, super::ByteRange::new(0, 99));
        assert_eq!(plan.terms[0].fetch_index, 0);
    }

    #[test]
    fn plan_rejects_term_without_fetch_info() {
        let xorbs = json!({});
        let terms = json!([
            {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 2}}
        ]);
        let body = v2_response_body(0, terms, xorbs);
        let response: shardline_xet_adapter::FileReconstructionV2Response =
            serde_json::from_value(body).unwrap();
        let error = Reconstruction::from_v2(response).unwrap_err();
        assert!(matches!(error, SdxError::MissingFetchInfo { .. }));
    }

    #[tokio::test]
    async fn reconstruct_assembles_full_file_from_v2() {
        let server = MockServer::start().await;
        let chunk_a = vec![7u8; 64];
        let chunk_b = vec![9u8; 64];
        let chunk_c = vec![3u8; 64];
        let payload = serialize_payload(&[&chunk_a, &chunk_b, &chunk_c]);

        Mock::given(method("GET"))
            .and(path(format!("/v2/reconstructions/{FILE_ID}")))
            .and(header("authorization", "Bearer read-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(v2_response_body(
                0,
                json!([
                    {"hash": XORB_HASH, "unpacked_length": 128, "range": {"start": 0, "end": 2}},
                    {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 2, "end": 3}}
                ]),
                json!({
                    XORB_HASH: [{
                        "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                        "ranges": [
                            {"chunks": {"start": 0, "end": 3}, "bytes": {"start": 0, "end": 200}}
                        ]
                    }]
                }),
            )))
            .mount(&server)
            .await;
        xorb_range_mock(&server, XORB_HASH, 0, 200, payload).await;

        let transfer = transfer_client();
        let file = super::reconstruct(&transfer, &server.uri(), "read-token", FILE_ID, None)
            .await
            .unwrap();
        let mut expected = chunk_a.clone();
        expected.extend_from_slice(&chunk_b);
        expected.extend_from_slice(&chunk_c);
        assert_eq!(file.data, expected);
        assert_eq!(file.terms.len(), 2);
        assert_eq!(file.terms[0].output_offset, 0);
        assert_eq!(file.terms[1].output_offset, 128);
        let expected_hash = crate::hash::compute_term_verification_hash(&[
            compute_chunk_hash(&chunk_a),
            compute_chunk_hash(&chunk_b),
        ]);
        assert_eq!(file.terms[0].verification_hash, expected_hash);
    }

    #[tokio::test]
    async fn reconstruct_falls_back_to_v1_when_v2_not_served() {
        let server = MockServer::start().await;
        let chunk = vec![5u8; 64];
        let payload = serialize_payload(&[&chunk]);

        Mock::given(method("GET"))
            .and(path(format!("/v2/reconstructions/{FILE_ID}")))
            .respond_with(ResponseTemplate::new(404).set_body_raw("v2 not served", "text/plain"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/v1/reconstructions/{FILE_ID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "offset_into_first_range": 0,
                "terms": [
                    {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}}
                ],
                "fetch_info": {
                    XORB_HASH: [{
                        "range": {"start": 0, "end": 1},
                        "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                        "url_range": {"start": 0, "end": 100}
                    }]
                }
            })))
            .mount(&server)
            .await;
        xorb_range_mock(&server, XORB_HASH, 0, 100, payload).await;

        let transfer = transfer_client();
        let file = super::reconstruct(&transfer, &server.uri(), "read-token", FILE_ID, None)
            .await
            .unwrap();
        assert_eq!(file.data, chunk);
    }

    #[tokio::test]
    async fn reconstruct_honors_offset_into_first_range() {
        let server = MockServer::start().await;
        let chunk = vec![7u8; 64];
        let payload = serialize_payload(&[&chunk]);

        Mock::given(method("GET"))
            .and(path(format!("/v2/reconstructions/{FILE_ID}")))
            .and(header("range", "bytes=16-63"))
            .respond_with(ResponseTemplate::new(200).set_body_json(v2_response_body(
                16,
                json!([
                    {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}}
                ]),
                json!({
                    XORB_HASH: [{
                        "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                        "ranges": [
                            {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": 100}}
                        ]
                    }]
                }),
            )))
            .mount(&server)
            .await;
        xorb_range_mock(&server, XORB_HASH, 0, 100, payload).await;

        let transfer = transfer_client();
        let range = super::ByteRange::new(16, 63);
        let file = super::reconstruct(&transfer, &server.uri(), "read-token", FILE_ID, Some(range))
            .await
            .unwrap();
        assert_eq!(file.data, chunk[16..64]);
    }

    #[tokio::test]
    async fn reconstruct_rejects_unpacked_length_mismatch() {
        let server = MockServer::start().await;
        let chunk = vec![7u8; 64];
        let payload = serialize_payload(&[&chunk]);

        Mock::given(method("GET"))
            .and(path(format!("/v2/reconstructions/{FILE_ID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(v2_response_body(
                0,
                json!([
                    {"hash": XORB_HASH, "unpacked_length": 32, "range": {"start": 0, "end": 1}}
                ]),
                json!({
                    XORB_HASH: [{
                        "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                        "ranges": [
                            {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": 100}}
                        ]
                    }]
                }),
            )))
            .mount(&server)
            .await;
        xorb_range_mock(&server, XORB_HASH, 0, 100, payload).await;

        let transfer = transfer_client();
        let error = super::reconstruct(&transfer, &server.uri(), "read-token", FILE_ID, None)
            .await
            .unwrap_err();
        assert!(matches!(error, SdxError::UnpackedLengthMismatch { .. }));
    }

    #[tokio::test]
    async fn reconstruct_surfaces_range_past_end() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/v2/reconstructions/{FILE_ID}")))
            .respond_with(
                ResponseTemplate::new(416).set_body_json(json!({"error": "range not satisfiable"})),
            )
            .mount(&server)
            .await;
        let transfer = transfer_client();
        let error = super::reconstruct(
            &transfer,
            &server.uri(),
            "read-token",
            FILE_ID,
            Some(super::ByteRange::new(1000, 2000)),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            SdxError::Transfer(crate::error::TransferError::RangeNotSatisfiable(_))
        ));
    }

    #[tokio::test]
    async fn reconstruct_uses_v2_xorbs_and_ignores_unknown_fields() {
        let server = MockServer::start().await;
        let chunk = vec![1u8; 32];
        let payload = serialize_payload(&[&chunk]);
        let mut body = v2_response_body(
            0,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 32, "range": {"start": 0, "end": 1}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": 40}}
                    ]
                }]
            }),
        );
        // Extra unknown JSON fields must be ignored by the deserializer.
        if let Some(obj) = body.as_object_mut() {
            obj.insert("unexpected".to_owned(), json!({"nested": true}));
        }
        Mock::given(method("GET"))
            .and(path(format!("/v2/reconstructions/{FILE_ID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        xorb_range_mock(&server, XORB_HASH, 0, 40, payload).await;

        let transfer = transfer_client();
        let file = super::reconstruct(&transfer, &server.uri(), "read-token", FILE_ID, None)
            .await
            .unwrap();
        assert_eq!(file.data, chunk);
    }

    #[test]
    fn empty_reconstruction_plan_yields_empty_output() {
        let body = json!({
            "offset_into_first_range": 0,
            "terms": [],
            "xorbs": {}
        });
        let response: shardline_xet_adapter::FileReconstructionV2Response =
            serde_json::from_value(body).unwrap();
        let plan = Reconstruction::from_v2(response).unwrap();
        assert!(plan.terms.is_empty());
        assert!(plan.fetches.is_empty());
    }
}
