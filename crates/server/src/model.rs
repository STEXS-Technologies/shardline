use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use shardline_protocol::{RepositoryProvider, SecretString, TokenScope};

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
    pub token: SecretString,
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
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct XetCasTokenResponse {
    /// CAS server endpoint base URL.
    pub cas_url: String,
    /// Token expiration timestamp as Unix seconds.
    pub exp: u64,
    /// Signed bearer token for CAS requests.
    pub access_token: String,
}

impl std::fmt::Debug for XetCasTokenResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XetCasTokenResponse")
            .field("cas_url", &self.cas_url)
            .field("exp", &self.exp)
            .field("access_token", &"<redacted>")
            .finish()
    }
}

/// Git LFS authenticate response carrying Xet custom-transfer bootstrap headers.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitLfsAuthenticateResponse {
    /// CAS endpoint URL.
    pub href: String,
    /// Headers consumed by the Xet custom transfer adapter.
    pub header: BTreeMap<String, String>,
    /// Relative token lifetime in seconds.
    pub expires_in: u64,
}

impl std::fmt::Debug for GitLfsAuthenticateResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitLfsAuthenticateResponse")
            .field("href", &self.href)
            .field("header", &"<redacted>")
            .field("expires_in", &self.expires_in)
            .finish()
    }
}

/// OCI registry bearer-token exchange response.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OciRegistryTokenResponse {
    /// Bearer token returned by the registry token service.
    pub token: String,
    /// Duplicate bearer token field used by some clients.
    pub access_token: String,
    /// Relative token lifetime in seconds.
    pub expires_in: u64,
}

impl std::fmt::Debug for OciRegistryTokenResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OciRegistryTokenResponse")
            .field("token", &"<redacted>")
            .field("access_token", &"<redacted>")
            .field("expires_in", &self.expires_in)
            .finish()
    }
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use shardline_protocol::{RepositoryProvider, SecretString, TokenScope};

    use super::*;

    fn health_response() -> HealthResponse {
        HealthResponse {
            status: "ok".to_owned(),
        }
    }

    fn ready_response() -> ReadyResponse {
        ReadyResponse {
            status: "ok".to_owned(),
            server_role: "all".to_owned(),
            server_frontends: vec!["xet".to_owned()],
            metadata_backend: "local".to_owned(),
            object_backend: "local".to_owned(),
            cache_backend: "memory".to_owned(),
        }
    }

    #[test]
    fn health_response_serde_round_trip() {
        let original = health_response();
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: HealthResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
        assert!(json.contains("ok"));
    }

    #[test]
    fn health_response_debug() {
        let response = health_response();
        let debug = format!("{response:?}");
        assert!(debug.contains("HealthResponse"));
        assert!(debug.contains("ok"));
    }

    #[test]
    fn ready_response_serde_round_trip() {
        let original = ready_response();
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: ReadyResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn ready_response_debug() {
        let response = ready_response();
        let debug = format!("{response:?}");
        assert!(debug.contains("ReadyResponse"));
    }

    #[test]
    fn upload_chunk_result_serde_round_trip() {
        let original = UploadChunkResult {
            hash: "abcdef123456".to_owned(),
            offset: 0,
            length: 4096,
            inserted: true,
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: UploadChunkResult = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn upload_file_response_serde_round_trip() {
        let original = UploadFileResponse {
            file_id: "test-file".to_owned(),
            content_hash: "content-hash-abc".to_owned(),
            total_bytes: 8192,
            chunk_size: 4096,
            inserted_chunks: 2,
            reused_chunks: 0,
            stored_bytes: 8192,
            chunks: vec![UploadChunkResult {
                hash: "chunk1".to_owned(),
                offset: 0,
                length: 4096,
                inserted: true,
            }],
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: UploadFileResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn server_stats_response_serde_round_trip() {
        let original = ServerStatsResponse {
            chunks: 10,
            chunk_bytes: 40960,
            files: 5,
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: ServerStatsResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn provider_token_issue_request_serde_round_trip() {
        let original = ProviderTokenIssueRequest {
            subject: "user".to_owned(),
            owner: "org".to_owned(),
            repo: "repo".to_owned(),
            revision: Some("main".to_owned()),
            scope: TokenScope::Read,
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: ProviderTokenIssueRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn provider_token_issue_response_serde_round_trip() {
        let original = ProviderTokenIssueResponse {
            token: SecretString::from_secret("bearer-token"),
            issuer: "shardline".to_owned(),
            subject: "user".to_owned(),
            provider: RepositoryProvider::GitHub,
            owner: "org".to_owned(),
            repo: "repo".to_owned(),
            revision: Some("main".to_owned()),
            scope: TokenScope::Write,
            expires_at_unix_seconds: 1_700_000_000,
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: ProviderTokenIssueResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn xet_cas_token_response_serde_round_trip() {
        let original = XetCasTokenResponse {
            cas_url: "http://cas.example.com".to_owned(),
            exp: 1_700_000_000,
            access_token: "access-token-value".to_owned(),
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: XetCasTokenResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
        // camelCase serialization
        assert!(json.contains("casUrl"));
        assert!(json.contains("accessToken"));
    }

    #[test]
    fn git_lfs_authenticate_response_serde_round_trip() {
        let mut header = BTreeMap::new();
        header.insert("X-Custom".to_owned(), "value".to_owned());
        let original = GitLfsAuthenticateResponse {
            href: "http://lfs.example.com".to_owned(),
            header,
            expires_in: 3600,
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: GitLfsAuthenticateResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn oci_registry_token_response_serde_round_trip() {
        let original = OciRegistryTokenResponse {
            token: "bearer-token".to_owned(),
            access_token: "access-token".to_owned(),
            expires_in: 300,
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: OciRegistryTokenResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn provider_webhook_response_serde_round_trip() {
        let original = ProviderWebhookResponse {
            provider: RepositoryProvider::GitHub,
            owner: "org".to_owned(),
            repo: "repo".to_owned(),
            delivery_id: "delivery-123".to_owned(),
            event_kind: "revision_pushed".to_owned(),
            new_owner: None,
            new_repo: None,
            revision: Some("abc123".to_owned()),
            affected_file_versions: 5,
            affected_chunks: 20,
            applied_holds: 3,
            retention_seconds: Some(86_400),
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: ProviderWebhookResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn provider_webhook_response_omits_optional_empty_fields() {
        let original = ProviderWebhookResponse {
            provider: RepositoryProvider::GitHub,
            owner: "org".to_owned(),
            repo: "repo".to_owned(),
            delivery_id: "delivery-123".to_owned(),
            event_kind: "push".to_owned(),
            new_owner: None,
            new_repo: None,
            revision: None,
            affected_file_versions: 0,
            affected_chunks: 0,
            applied_holds: 0,
            retention_seconds: None,
        };
        let json = serde_json::to_string(&original).unwrap();
        // Fields with skip_serializing_if should be absent
        assert!(
            !json.contains("\"new_owner\""),
            "unexpected new_owner: {json}"
        );
        assert!(
            !json.contains("\"new_repo\""),
            "unexpected new_repo: {json}"
        );
        assert!(
            !json.contains("\"revision\""),
            "unexpected revision: {json}"
        );
        assert!(
            !json.contains("\"retention_seconds\""),
            "unexpected retention_seconds: {json}"
        );
    }
}
