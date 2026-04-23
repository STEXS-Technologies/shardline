use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use shardline_protocol::{RepositoryProvider, TokenScope};

/// Health response returned by the HTTP server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthResponse {
    /// Service status.
    pub status: String,
}

/// Readiness response returned by the HTTP server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadyResponse {
    /// Service status.
    pub status: String,
    /// Selected runtime role.
    pub server_role: String,
    /// Enabled runtime protocol frontends.
    pub server_frontends: Vec<String>,
    /// Selected metadata backend.
    pub metadata_backend: String,
    /// Selected immutable object-storage backend.
    pub object_backend: String,
    /// Selected reconstruction-cache backend.
    pub cache_backend: String,
}

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

/// Upload result for a single chunk.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadChunkResult {
    /// Chunk hash in Xet CAS API hexadecimal ordering.
    pub hash: String,
    /// Byte offset inside the uploaded file.
    pub offset: u64,
    /// Chunk byte length.
    pub length: u64,
    /// Whether the upload inserted new chunk bytes.
    pub inserted: bool,
}

/// File upload response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadFileResponse {
    /// Uploaded file identifier.
    pub file_id: String,
    /// Immutable content identity for this uploaded file version.
    pub content_hash: String,
    /// Total uploaded byte length.
    pub total_bytes: u64,
    /// Server chunk size used for this upload.
    pub chunk_size: u64,
    /// Number of chunks inserted.
    pub inserted_chunks: u64,
    /// Number of chunks already present.
    pub reused_chunks: u64,
    /// Number of new bytes written to chunk storage.
    pub stored_bytes: u64,
    /// Ordered chunk upload results.
    pub chunks: Vec<UploadChunkResult>,
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

/// Storage stats response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerStatsResponse {
    /// Number of chunk objects stored.
    pub chunks: u64,
    /// Total bytes stored across chunk objects.
    pub chunk_bytes: u64,
    /// Number of file records stored.
    pub files: u64,
}

/// Xorb upload response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct XorbUploadResponse {
    /// Whether the xorb bytes were newly inserted.
    pub was_inserted: bool,
}

/// Shard registration response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardUploadResponse {
    /// Shard registration status.
    pub result: u8,
}

/// Provider-backed CAS token issuance request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderTokenIssueRequest {
    /// Authenticated provider subject to authorize.
    pub subject: String,
    /// Repository owner or namespace.
    pub owner: String,
    /// Repository name.
    pub repo: String,
    /// Optional revision context. When omitted, the provider default revision is used.
    pub revision: Option<String>,
    /// Requested CAS scope.
    pub scope: TokenScope,
}

/// Provider-backed CAS token issuance response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderTokenIssueResponse {
    /// Signed bearer token for subsequent CAS requests.
    pub token: String,
    /// Issuer embedded into the token.
    pub issuer: String,
    /// Subject embedded into the token.
    pub subject: String,
    /// Repository hosting provider.
    pub provider: RepositoryProvider,
    /// Repository owner or namespace.
    pub owner: String,
    /// Repository name.
    pub repo: String,
    /// Scoped revision.
    pub revision: Option<String>,
    /// Granted CAS scope.
    pub scope: TokenScope,
    /// Token expiration timestamp as Unix seconds.
    pub expires_at_unix_seconds: u64,
}

/// Xet CAS access-token response consumed by reference clients.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct XetCasTokenResponse {
    /// CAS server endpoint base URL.
    pub cas_url: String,
    /// Token expiration timestamp as Unix seconds.
    pub exp: u64,
    /// Signed bearer token for CAS requests.
    pub access_token: String,
}

/// Git LFS authenticate response carrying Xet custom-transfer bootstrap headers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitLfsAuthenticateResponse {
    /// CAS endpoint URL.
    pub href: String,
    /// Headers consumed by the Xet custom transfer adapter.
    pub header: BTreeMap<String, String>,
    /// Relative token lifetime in seconds.
    pub expires_in: u64,
}

/// Provider webhook handling response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderWebhookResponse {
    /// Repository hosting provider.
    pub provider: RepositoryProvider,
    /// Repository owner or namespace.
    pub owner: String,
    /// Repository name.
    pub repo: String,
    /// Provider delivery identifier.
    pub delivery_id: String,
    /// Normalized webhook event kind.
    pub event_kind: String,
    /// New repository owner or namespace when the event renamed the repository.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_owner: Option<String>,
    /// New repository name when the event renamed the repository.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_repo: Option<String>,
    /// Updated revision when the event described a push.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// Number of affected immutable file-version records.
    pub affected_file_versions: u64,
    /// Number of distinct affected chunk objects.
    pub affected_chunks: u64,
    /// Number of retention holds inserted or refreshed by the event.
    pub applied_holds: u64,
    /// Retention applied to newly created holds, when the event mutated lifecycle state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_seconds: Option<u64>,
}
