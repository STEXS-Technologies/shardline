use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// End-exclusive chunk index range within one xorb.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconstructionChunkRange {
    /// First chunk index included by this range.
    pub start: u32,
    /// End-exclusive chunk index.
    pub end: u32,
}

/// Inclusive byte range for a ranged fetch URL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
