//! Shared Xet wire contracts, routes and reconstruction version conversion.

pub const XORB_TRANSFER_NAMESPACE: &str = "default";
pub const XORB_TRANSFER_ROUTE: &str = "/transfer/xorb/{prefix}/{hash}";
pub const XET_READ_TOKEN_ROUTE: &str = "/api/{provider}/{owner}/{repo}/xet-read-token/{rev}";
pub const XET_WRITE_TOKEN_ROUTE: &str = "/api/{provider}/{owner}/{repo}/xet-write-token/{rev}";
pub const XET_TREE_ROUTE: &str = "/api/{provider}/{owner}/{repo}/tree/{rev}";
pub const XET_PATH_ROUTE: &str = "/api/{provider}/{owner}/{repo}/path/{rev}/{*path}";
pub const XET_REVISIONS_ROUTE: &str = "/api/{provider}/{owner}/{repo}/revisions";
pub const XET_REVISION_ROUTE: &str = "/api/{provider}/{owner}/{repo}/revisions/{rev}";
/// Accept media type used by native Xet metadata clients to disambiguate
/// metadata routes from overlapping Hub API paths when both frontends run.
pub const XET_METADATA_ACCEPT: &str = "application/vnd.stexs.xet-metadata+json";

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// End-exclusive chunk index range within one xorb.
#[derive(Debug, Clone, Hash, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconstructionChunkRange {
    /// First chunk index included by this range.
    pub start: u64,
    /// End-exclusive chunk index.
    pub end: u64,
}

/// Inclusive byte range for a ranged fetch URL.
#[derive(Debug, Clone, Hash, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconstructionUrlRange {
    /// First byte offset.
    pub start: u64,
    /// Inclusive final byte offset.
    pub end: u64,
}

/// Ordered reconstruction term returned by the Xet reconstruction API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconstructionTerm {
    /// Xorb hash in Xet CAS API hexadecimal ordering.
    pub hash: String,
    /// Expected total decompressed byte length for this term.
    pub unpacked_length: u64,
    /// Chunk range to read from the referenced xorb.
    pub range: ReconstructionChunkRange,
}

/// Fetch information for downloading one xorb range.
#[derive(Debug, Clone, Hash, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconstructionFetchInfo {
    /// Chunk range provided by this fetch URL.
    pub range: ReconstructionChunkRange,
    /// Download URL for the serialized xorb bytes.
    pub url: String,
    /// Inclusive byte range to request from the download URL.
    pub url_range: ReconstructionUrlRange,
}

/// V2 byte-range descriptor for one xorb fetch entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconstructionRangeDescriptor {
    /// Chunk range covered by this fetch descriptor.
    pub chunks: ReconstructionChunkRange,
    /// Inclusive byte range to request from the xorb URL.
    pub bytes: ReconstructionUrlRange,
}

/// V2 fetch information for downloading one xorb via one or more byte ranges.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconstructionMultiRangeFetch {
    /// Download URL for the serialized xorb bytes.
    pub url: String,
    /// Ordered byte ranges covered by this fetch descriptor.
    pub ranges: Vec<ReconstructionRangeDescriptor>,
}

/// File reconstruction response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileReconstructionResponse {
    /// Byte offset to skip from the first returned term when the request used a
    /// reconstruction range.
    pub offset_into_first_range: u64,
    /// Ordered reconstruction terms in Xet download order.
    pub terms: Vec<ReconstructionTerm>,
    /// Download metadata keyed by xorb hash.
    pub fetch_info: BTreeMap<String, Vec<ReconstructionFetchInfo>>,
}

/// V2 file reconstruction response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileReconstructionV2Response {
    /// Byte offset to skip from the first returned term when the request used a
    /// reconstruction range.
    pub offset_into_first_range: u64,
    /// Ordered reconstruction terms in Xet download order.
    pub terms: Vec<ReconstructionTerm>,
    /// Download metadata keyed by xorb hash.
    pub xorbs: BTreeMap<String, Vec<ReconstructionMultiRangeFetch>>,
}

/// Batch reconstruction response for multiple file identifiers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchReconstructionResponse {
    /// Ordered reconstruction terms keyed by file identifier.
    pub files: BTreeMap<String, Vec<ReconstructionTerm>>,
    /// Download metadata keyed by xorb hash.
    pub fetch_info: BTreeMap<String, Vec<ReconstructionFetchInfo>>,
}

/// Upload result for a single xorb.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct XorbUploadResponse {
    /// Whether the xorb bytes were newly inserted.
    pub was_inserted: bool,
}

/// Upload result for a single shard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardUploadResponse {
    /// Shard registration status.
    pub result: u8,
}

/// Converts V1 fetch metadata into the V2 reconstruction wire representation.
#[must_use]
pub fn reconstruction_v2_from_v1(
    response: FileReconstructionResponse,
) -> FileReconstructionV2Response {
    let xorbs = response
        .fetch_info
        .into_iter()
        .map(|(hash, fetch_entries)| {
            let entries = fetch_entries
                .into_iter()
                .map(|entry| ReconstructionMultiRangeFetch {
                    url: entry.url,
                    ranges: vec![ReconstructionRangeDescriptor {
                        chunks: entry.range,
                        bytes: entry.url_range,
                    }],
                })
                .collect();
            (hash, entries)
        })
        .collect();

    FileReconstructionV2Response {
        offset_into_first_range: response.offset_into_first_range,
        terms: response.terms,
        xorbs,
    }
}
