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
    /// Parses a repository type string from an API response.
    ///
    /// # Errors
    ///
    /// Returns `None` for unrecognized types.
    #[must_use]
    pub fn from_api_str(value: &str) -> Option<Self> {
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
    /// Repository tags.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Download count.
    #[serde(default)]
    pub downloads: u64,
    /// Like count.
    #[serde(default)]
    pub likes: u64,
    /// Last modification timestamp (ISO 8601).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
    /// ML pipeline tag (e.g. "text-generation", "image-classification").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline_tag: Option<String>,
    /// Model card data extracted from README metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub card_data: Option<serde_json::Value>,
    /// Security status of the repository.
    #[serde(default = "default_security_status")]
    pub security_status: serde_json::Value,
}

fn default_security_status() -> serde_json::Value {
    serde_json::json!({})
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
///
/// Per the HuggingFace Hub spec the request may include `gitAttributes` and
/// `gitIgnore` fields. These are accepted for spec conformity but not
/// currently used by the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreuploadRequest {
    /// Files to upload.
    pub files: Vec<PreuploadFile>,
    /// Optional git attributes (accepted per HF spec, currently unused).
    #[serde(default)]
    #[serde(rename = "gitAttributes")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_attributes: Option<serde_json::Value>,
    /// Optional git ignore rules (accepted per HF spec, currently unused).
    #[serde(default)]
    #[serde(rename = "gitIgnore")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_ignore: Option<serde_json::Value>,
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
    /// User type (always "user").
    #[serde(rename = "type", default = "default_user_type")]
    pub user_type: String,
    /// Authentication details.
    pub auth: WhoamiAuth,
}

fn default_user_type() -> String {
    "user".to_owned()
}

/// Authentication details in a whoami response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhoamiAuth {
    /// Auth type (always "token").
    #[serde(rename = "type", default = "default_auth_type")]
    pub auth_type: String,
    /// Identity details.
    pub identity: WhoamiIdentity,
}

fn default_auth_type() -> String {
    "token".to_owned()
}

/// Identity details in a whoami auth response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhoamiIdentity {
    /// Account information.
    pub account: WhoamiAccount,
}

/// Account information in a whoami identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhoamiAccount {
    /// Account name.
    pub name: String,
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
    /// Filter by author/owner.
    #[serde(default)]
    pub author: Option<String>,
    /// Sort field ("lastModified", "likes", "downloads").
    #[serde(default)]
    pub sort: Option<String>,
    /// Sort direction ("asc" or "desc").
    #[serde(default)]
    pub direction: Option<String>,
    /// Maximum number of results (default 50, max 200).
    #[serde(default = "default_search_limit")]
    pub limit: usize,
}

const fn default_search_limit() -> usize {
    50
}

/// Tree listing query parameters.
#[derive(Debug, Clone, Deserialize)]
pub struct TreeQuery {
    /// Maximum number of entries to return.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Pagination cursor.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Whether to list entries recursively.
    #[serde(default)]
    pub recursive: bool,
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

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // RepoType::from_api_str
    // -----------------------------------------------------------------------

    #[test]
    fn from_str_models() {
        assert_eq!(RepoType::from_api_str("models"), Some(RepoType::Model));
    }

    #[test]
    fn from_str_model() {
        assert_eq!(RepoType::from_api_str("model"), Some(RepoType::Model));
    }

    #[test]
    fn from_str_datasets() {
        assert_eq!(RepoType::from_api_str("datasets"), Some(RepoType::Dataset));
    }

    #[test]
    fn from_str_dataset() {
        assert_eq!(RepoType::from_api_str("dataset"), Some(RepoType::Dataset));
    }

    #[test]
    fn from_str_spaces() {
        assert_eq!(RepoType::from_api_str("spaces"), Some(RepoType::Space));
    }

    #[test]
    fn from_str_space() {
        assert_eq!(RepoType::from_api_str("space"), Some(RepoType::Space));
    }

    #[test]
    fn from_str_unknown() {
        assert_eq!(RepoType::from_api_str("unknown"), None);
    }

    #[test]
    fn from_str_empty() {
        assert_eq!(RepoType::from_api_str(""), None);
    }

    // -----------------------------------------------------------------------
    // RepoType::as_path_str
    // -----------------------------------------------------------------------

    #[test]
    fn as_path_str_model() {
        assert_eq!(RepoType::Model.as_path_str(), "models");
    }

    #[test]
    fn as_path_str_dataset() {
        assert_eq!(RepoType::Dataset.as_path_str(), "datasets");
    }

    #[test]
    fn as_path_str_space() {
        assert_eq!(RepoType::Space.as_path_str(), "spaces");
    }

    // -----------------------------------------------------------------------
    // RepoType serialization round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn repo_type_roundtrip_model() {
        let value = serde_json::to_string(&RepoType::Model).unwrap();
        assert_eq!(value, "\"model\"");
        let deserialized: RepoType = serde_json::from_str(&value).unwrap();
        assert_eq!(deserialized, RepoType::Model);
    }

    #[test]
    fn repo_type_roundtrip_dataset() {
        let value = serde_json::to_string(&RepoType::Dataset).unwrap();
        assert_eq!(value, "\"dataset\"");
        let deserialized: RepoType = serde_json::from_str(&value).unwrap();
        assert_eq!(deserialized, RepoType::Dataset);
    }

    #[test]
    fn repo_type_roundtrip_space() {
        let value = serde_json::to_string(&RepoType::Space).unwrap();
        assert_eq!(value, "\"space\"");
        let deserialized: RepoType = serde_json::from_str(&value).unwrap();
        assert_eq!(deserialized, RepoType::Space);
    }

    // -----------------------------------------------------------------------
    // LfsBatchOperation serialization
    // -----------------------------------------------------------------------

    #[test]
    fn lfs_batch_operation_download_serializes_to_lowercase() {
        let json = serde_json::to_string(&LfsBatchOperation::Download).unwrap();
        assert_eq!(json, "\"download\"");
    }

    #[test]
    fn lfs_batch_operation_upload_serializes_to_lowercase() {
        let json = serde_json::to_string(&LfsBatchOperation::Upload).unwrap();
        assert_eq!(json, "\"upload\"");
    }

    #[test]
    fn lfs_batch_operation_verify_serializes_to_lowercase() {
        let json = serde_json::to_string(&LfsBatchOperation::Verify).unwrap();
        assert_eq!(json, "\"verify\"");
    }

    #[test]
    fn lfs_batch_operation_roundtrip_download() {
        let op: LfsBatchOperation = serde_json::from_str("\"download\"").unwrap();
        assert_eq!(op, LfsBatchOperation::Download);
    }

    #[test]
    fn lfs_batch_operation_roundtrip_upload() {
        let op: LfsBatchOperation = serde_json::from_str("\"upload\"").unwrap();
        assert_eq!(op, LfsBatchOperation::Upload);
    }

    #[test]
    fn lfs_batch_operation_roundtrip_verify() {
        let op: LfsBatchOperation = serde_json::from_str("\"verify\"").unwrap();
        assert_eq!(op, LfsBatchOperation::Verify);
    }

    // -----------------------------------------------------------------------
    // WebhookCreateRequest defaults
    // -----------------------------------------------------------------------

    #[test]
    fn webhook_create_request_default_events_is_push() {
        let req: WebhookCreateRequest =
            serde_json::from_str(r#"{"url": "https://example.com/hook"}"#).unwrap();
        assert_eq!(req.events, vec!["push"]);
    }

    #[test]
    fn webhook_create_request_secret_is_optional() {
        let req: WebhookCreateRequest =
            serde_json::from_str(r#"{"url": "https://example.com/hook", "events": ["push"]}"#)
                .unwrap();
        assert!(req.secret.is_none());
    }

    // -----------------------------------------------------------------------
    // RepoSearchQuery defaults
    // -----------------------------------------------------------------------

    #[test]
    fn repo_search_query_default_limit() {
        let q: RepoSearchQuery = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(q.limit, 50);
        assert_eq!(q.q, "");
    }

    // -----------------------------------------------------------------------
    // TreeQuery defaults
    // -----------------------------------------------------------------------

    #[test]
    fn tree_query_defaults() {
        let q: TreeQuery = serde_json::from_str(r#"{}"#).unwrap();
        assert!(q.limit.is_none());
        assert!(q.cursor.is_none());
        assert!(!q.recursive);
    }

    // -----------------------------------------------------------------------
    // DatasetFirstRowsQuery defaults
    // -----------------------------------------------------------------------

    #[test]
    fn dataset_first_rows_query_defaults() {
        let q: DatasetFirstRowsQuery = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(q.config, "default");
        assert_eq!(q.split, "train");
        assert_eq!(q.limit, 100);
    }

    // -----------------------------------------------------------------------
    // DatasetViewerQuery defaults
    // -----------------------------------------------------------------------

    #[test]
    fn dataset_viewer_query_defaults() {
        let q: DatasetViewerQuery = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(q.config, "default");
        assert_eq!(q.offset, 0);
        assert_eq!(q.length, 100);
    }

    // -----------------------------------------------------------------------
    // RepoCreateRequest serialization
    // -----------------------------------------------------------------------

    #[test]
    fn repo_create_request_roundtrip() {
        let json = r#"{"type":"model","name":"org/repo","private":true}"#;
        let req: RepoCreateRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.repo_type, RepoType::Model);
        assert_eq!(req.name, "org/repo");
        assert!(req.private);

        let serialized = serde_json::to_string(&req).unwrap();
        let deserialized: RepoCreateRequest = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.repo_type, RepoType::Model);
        assert_eq!(deserialized.name, "org/repo");
        assert!(deserialized.private);
    }

    // -----------------------------------------------------------------------
    // RepoResponse serialization
    // -----------------------------------------------------------------------

    #[test]
    fn repo_response_serialization() {
        let resp = RepoResponse {
            id: "org/repo".into(),
            repo_type: RepoType::Model,
            private: false,
            url: "/models/org/repo".into(),
            default_branch: Some("main".into()),
            tags: vec![],
            downloads: 0,
            likes: 0,
            last_modified: Some("2024-01-01T00:00:00+00:00".into()),
            pipeline_tag: None,
            card_data: None,
            security_status: serde_json::json!({}),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["id"], "org/repo");
        assert_eq!(json["type"], "model");
        assert_eq!(json["url"], "/models/org/repo");
        assert_eq!(json["default_branch"], "main");
    }

    // -----------------------------------------------------------------------
    // WebhookEventPayload serialization
    // -----------------------------------------------------------------------

    #[test]
    fn webhook_event_payload_serialization() {
        let payload = WebhookEventPayload {
            event: "push".into(),
            repository: "org/repo".into(),
            revision: "abc123".into(),
            timestamp: 42,
            data: serde_json::json!({"key": "value"}),
        };
        let json = serde_json::to_value(&payload).unwrap();
        assert_eq!(json["event"], "push");
        assert_eq!(json["repository"], "org/repo");
        assert_eq!(json["timestamp"], 42);
        assert_eq!(json["data"]["key"], "value");
    }

    // -----------------------------------------------------------------------
    // WebhookEventPayload default data
    // -----------------------------------------------------------------------

    #[test]
    fn webhook_event_payload_default_data_is_null() {
        let payload: WebhookEventPayload = serde_json::from_str(
            r#"{"event":"push","repository":"org/repo","revision":"abc123","timestamp":1}"#,
        )
        .unwrap();
        // Default for serde_json::Value is Null, not an empty object
        assert_eq!(payload.data, serde_json::Value::Null);
    }

    // -----------------------------------------------------------------------
    // RepoResponse security_status default
    // -----------------------------------------------------------------------

    #[test]
    fn repo_response_security_status_default_is_empty_object() {
        let resp: RepoResponse = serde_json::from_str(r#"{"id":"r","type":"model","private":false,"url":"/models/r"}"#).unwrap();
        assert_eq!(resp.security_status, serde_json::json!({}));
    }

    // -----------------------------------------------------------------------
    // WhoamiResponse defaults
    // -----------------------------------------------------------------------

    #[test]
    fn whoami_response_defaults() {
        let resp: WhoamiResponse = serde_json::from_str(
            r#"{"name":"testuser","auth":{"identity":{"account":{"name":"testuser"}}}}"#,
        )
        .unwrap();
        assert!(!resp.is_admin);
        assert_eq!(resp.user_type, "user");
        assert_eq!(resp.auth.auth_type, "token");
    }

    // -----------------------------------------------------------------------
    // LfsObjectAction serialization
    // -----------------------------------------------------------------------

    #[test]
    fn lfs_object_action_roundtrip() {
        let action = LfsObjectAction {
            href: "/lfs/objects/abc".into(),
            header: None,
            ssh: None,
        };
        let json = serde_json::to_value(&action).unwrap();
        assert_eq!(json["href"], "/lfs/objects/abc");
        assert!(json.get("header").is_none());
        assert!(json.get("ssh").is_none());
    }

    // -----------------------------------------------------------------------
    // RepoType from_api_str case sensitivity
    // -----------------------------------------------------------------------

    #[test]
    fn from_str_case_sensitive() {
        assert_eq!(RepoType::from_api_str("Model"), None);
        assert_eq!(RepoType::from_api_str("MODEL"), None);
    }
}
