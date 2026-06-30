use serde::{Deserialize, Serialize};

/// Repository type (model, dataset, space).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RepoType {
    Model,
    Dataset,
    Space,
}

impl RepoType {
    /// Parses a repository type string.
    ///
    /// # Errors
    ///
    /// Returns `None` for unrecognized types.
    #[must_use]
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "models" | "model" => Some(Self::Model),
            "datasets" | "dataset" => Some(Self::Dataset),
            "spaces" | "space" => Some(Self::Space),
            _ => None,
        }
    }

    /// Returns the plural API path segment.
    #[must_use]
    pub const fn as_path_str(self) -> &'static str {
        match self {
            Self::Model => "models",
            Self::Dataset => "datasets",
            Self::Space => "spaces",
        }
    }
}

/// Repository creation request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoCreateRequest {
    /// Repository type.
    #[serde(rename = "type")]
    pub repo_type: RepoType,
    /// Repository owner or namespace.
    pub name: String,
    /// Optional repository visibility (private/public).
    #[serde(default)]
    pub private: bool,
}

/// Repository info response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoResponse {
    /// Repository ID (`owner/name`).
    pub id: String,
    /// Repository type.
    #[serde(rename = "type")]
    pub repo_type: RepoType,
    /// Whether the repository is private.
    pub private: bool,
    /// URL to clone the repository.
    pub url: String,
    /// Default branch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_branch: Option<String>,
}

/// Revision info response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevisionResponse {
    /// Revision ref name.
    pub ref_name: String,
    /// Revision SHA.
    pub sha: String,
}

/// Tree entry for file listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeEntry {
    /// Entry type (`file`, `directory`).
    #[serde(rename = "type")]
    pub entry_type: String,
    /// Relative path.
    pub path: String,
    /// File size in bytes (only for files).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// LFS pointer OID (only for LFS files).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lfs: Option<TreeEntryLfs>,
}

/// LFS metadata for a tree entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeEntryLfs {
    /// SHA-256 OID.
    pub oid: String,
    /// File size in bytes.
    pub size: u64,
}

/// Preupload request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreuploadRequest {
    /// Files to upload.
    pub files: Vec<PreuploadFile>,
}

/// File in a preupload request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreuploadFile {
    /// Relative path.
    pub path: String,
    /// Whether the file is an LFS pointer.
    #[serde(default)]
    pub lfs: bool,
}

/// Preupload response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreuploadResponse {
    /// Files classified by the server.
    pub result: Vec<PreuploadResult>,
}

/// Preupload classification result for one file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreuploadResult {
    /// Relative path.
    pub path: String,
    /// Whether this file already exists.
    pub exists: bool,
}

/// Commit request body (NDJSON).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitHeader {
    /// Commit message.
    pub message: String,
    /// Parent revision.
    #[serde(default)]
    pub parent_commit: Option<String>,
}

/// NDJSON commit file line (inline, base64-encoded).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitFile {
    /// Relative path.
    pub path: String,
    /// Base64-encoded content.
    pub content: String,
}

/// NDJSON commit LFS file line.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitLfsFile {
    /// Relative path.
    pub path: String,
    /// LFS pointer OID (SHA-256).
    pub oid: String,
    /// File size.
    pub size: u64,
}

/// NDJSON commit deletion line.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitDeletedEntry {
    /// Relative path.
    pub path: String,
}

/// Commit response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitResponse {
    /// New commit SHA.
    pub commit_id: String,
    /// New revision ref.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_name: Option<String>,
}

/// Whoami response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhoamiResponse {
    /// User name.
    pub name: String,
    /// Whether the user is an admin.
    #[serde(default)]
    pub is_admin: bool,
}

/// Token exchange response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenExchangeResponse {
    /// Access token.
    pub token: String,
}

/// LFS batch request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LfsBatchRequest {
    /// Transfer adapter operations.
    pub operation: LfsBatchOperation,
    /// Objects to operate on.
    #[serde(rename = "ref")]
    pub ref_: LfsBatchRef,
    /// Objects.
    pub objects: Vec<LfsObjectRequest>,
}

/// LFS batch operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LfsBatchOperation {
    /// Download operation.
    Download,
    /// Upload operation.
    Upload,
    /// Verification operation.
    Verify,
}

/// LFS batch ref info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LfsBatchRef {
    /// Branch name.
    pub name: String,
}

/// LFS object request in a batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LfsObjectRequest {
    /// SHA-256 OID.
    pub oid: String,
    /// Object size.
    pub size: u64,
}

/// LFS batch response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LfsBatchResponse {
    /// Transfer adapter to use.
    pub transfer: String,
    /// Objects with actions.
    pub objects: Vec<LfsObjectResponse>,
}

/// LFS object response in a batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LfsObjectResponse {
    /// SHA-256 OID.
    pub oid: String,
    /// Object size.
    pub size: u64,
    /// Available actions.
    pub actions: Option<LfsObjectActions>,
    /// Error, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<LfsObjectError>,
}

/// Available actions for an LFS object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LfsObjectActions {
    /// Download action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download: Option<LfsObjectAction>,
    /// Upload action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload: Option<LfsObjectAction>,
    /// Verify action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verify: Option<LfsObjectAction>,
}

/// LFS object action with URL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LfsObjectAction {
    /// Action URL.
    pub href: String,
    /// Optional headers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header: Option<std::collections::BTreeMap<String, String>>,
    /// Whether this is an SSH action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssh: Option<bool>,
}

/// Repository list response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoListResponse {
    /// List of repositories.
    pub repos: Vec<RepoResponse>,
}

/// Repository search query parameters.
#[derive(Debug, Clone, Deserialize)]
pub struct RepoSearchQuery {
    /// Search query (name prefix match).
    #[serde(default)]
    pub q: String,
    /// Maximum number of results (default 50, max 200).
    #[serde(default = "default_search_limit")]
    pub limit: usize,
}

const fn default_search_limit() -> usize {
    50
}

/// Revision list response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevisionListResponse {
    /// List of revisions.
    pub revisions: Vec<RevisionResponse>,
}

/// Dataset parquet file info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetParquetFile {
    /// File path relative to repo root.
    pub path: String,
    /// File size in bytes.
    pub size: u64,
    /// File SHA hash.
    pub sha: String,
}

/// Dataset parquet listing response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetParquetResponse {
    /// List of parquet files.
    pub files: Vec<DatasetParquetFile>,
}

/// Dataset first-rows query parameters.
#[derive(Debug, Clone, Deserialize)]
pub struct DatasetFirstRowsQuery {
    /// Dataset config name (default: "default").
    #[serde(default = "default_config")]
    pub config: String,
    /// Dataset split name (default: "train").
    #[serde(default = "default_split")]
    pub split: String,
    /// Maximum number of rows to return (default: 100, max: 1000).
    #[serde(default = "default_first_rows_limit")]
    pub limit: usize,
}

fn default_config() -> String {
    "default".to_owned()
}

fn default_split() -> String {
    "train".to_owned()
}

const fn default_first_rows_limit() -> usize {
    100
}

/// Dataset viewer query parameters.
#[derive(Debug, Clone, Deserialize)]
pub struct DatasetViewerQuery {
    /// Dataset config name (default: "default").
    #[serde(default = "default_config")]
    pub config: String,
    /// Row offset (default: 0).
    #[serde(default)]
    pub offset: usize,
    /// Maximum number of rows to return (default: 100, max: 10000).
    #[serde(default = "default_viewer_limit")]
    pub length: usize,
}

const fn default_viewer_limit() -> usize {
    100
}

/// Dataset row (key-value pairs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetRow {
    /// Column values as key-value pairs.
    pub columns: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Dataset viewer response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetViewerResponse {
    /// Column names.
    pub columns: Vec<String>,
    /// Rows of data.
    pub rows: Vec<DatasetRow>,
    /// Total number of rows in the split (if known).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_rows_total: Option<usize>,
}

/// Dataset first-rows response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetFirstRowsResponse {
    /// Column names.
    pub columns: Vec<String>,
    /// First rows of data.
    pub rows: Vec<DatasetRow>,
}

// ---- Webhook models ----

/// Webhook creation request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookCreateRequest {
    /// Webhook URL to receive events.
    pub url: String,
    /// Events to subscribe to (e.g. ["push", "delete"]).
    #[serde(default = "default_webhook_events")]
    pub events: Vec<String>,
    /// Optional secret for HMAC signature verification.
    #[serde(default)]
    pub secret: Option<String>,
}

fn default_webhook_events() -> Vec<String> {
    vec!["push".to_owned()]
}

/// Webhook info response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookResponse {
    /// Webhook ID.
    pub id: String,
    /// Webhook URL.
    pub url: String,
    /// Subscribed events.
    pub events: Vec<String>,
    /// Whether the webhook is active.
    pub active: bool,
    /// Creation timestamp.
    pub created_at: u64,
}

/// Webhook list response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookListResponse {
    /// List of webhooks.
    pub webhooks: Vec<WebhookResponse>,
}

/// Webhook event payload delivered to subscribers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEventPayload {
    /// Event type (e.g. "push", "delete").
    pub event: String,
    /// Repository ID.
    pub repository: String,
    /// Revision SHA.
    pub revision: String,
    /// Event timestamp.
    pub timestamp: u64,
    /// Additional event-specific data.
    #[serde(default)]
    pub data: serde_json::Value,
}

/// LFS object error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LfsObjectError {
    /// Error code.
    pub code: i32,
    /// Error message.
    pub message: String,
}
