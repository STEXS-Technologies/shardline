use serde::{Deserialize, Serialize};
use serde_json::Value;
use shardline_server_core::AuthorizedRepository;
use shardline_storage::ObjectKey;

use crate::{ProtocolError, object_key, scope_namespace, validate_content_hash};

pub const LFS_CONTENT_TYPE: &str = "application/vnd.git-lfs+json";

/// Xet CAS transfer header names used in LFS batch responses and
/// provider token-issuance flows. These tell the git-xet transfer agent
/// where and how to connect to the content-addressed storage layer.
pub mod cas_headers {
    /// Base URL of the CAS endpoint.
    pub const URL: &str = "X-Xet-Cas-Url";
    /// Scoped bearer token for CAS operations.
    pub const ACCESS_TOKEN: &str = "X-Xet-Access-Token";
    /// Unix-seconds timestamp when the access token expires.
    pub const TOKEN_EXPIRATION: &str = "X-Xet-Token-Expiration";
}

/// LFS transfer adapter identifiers negotiated in the batch API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferAdapter {
    /// Standard HTTP object upload/download.
    Basic,
    /// Xet chunk-level deduplicated transfer via CAS.
    Xet,
}

impl TransferAdapter {
    /// Returns the wire-protocol string for this transfer adapter.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_protocol_adapters::TransferAdapter;
    ///
    /// assert_eq!(TransferAdapter::Basic.as_str(), "basic");
    /// assert_eq!(TransferAdapter::Xet.as_str(), "xet");
    /// ```
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Basic => "basic",
            Self::Xet => "xet",
        }
    }
}

impl std::fmt::Display for TransferAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// LFS batch operation identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LfsOperation {
    /// Retrieve objects from the server.
    Download,
    /// Store objects on the server.
    Upload,
}

impl LfsOperation {
    /// Returns the wire-protocol string for this operation.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_protocol_adapters::LfsOperation;
    ///
    /// assert_eq!(LfsOperation::Upload.as_str(), "upload");
    /// ```
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Download => "download",
            Self::Upload => "upload",
        }
    }
}

impl std::str::FromStr for LfsOperation {
    type Err = ();

    /// Parses a wire-protocol operation name.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_protocol_adapters::LfsOperation;
    ///
    /// assert_eq!("download".parse::<LfsOperation>()?, LfsOperation::Download);
    /// assert!("sidecar".parse::<LfsOperation>().is_err());
    /// # Ok::<(), ()>(())
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "download" => Ok(Self::Download),
            "upload" => Ok(Self::Upload),
            _ => Err(()),
        }
    }
}

/// Well-known LFS validation error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LfsValidationError {
    /// Invalid object ID format.
    InvalidOid,
    /// Unsupported batch operation.
    UnsupportedOperation,
    /// Unsupported hash algorithm.
    UnsupportedHashAlgorithm,
    /// Unsupported transfer adapter.
    UnsupportedTransferAdapter,
    /// Too many objects in a single batch request.
    TooManyObjects,
    /// Object does not exist on the server.
    ObjectNotFound,
    /// Object too large for server-side verification.
    ObjectTooLarge,
    /// SHA-256 hash of uploaded content does not match the expected digest.
    HashMismatch,
    /// Generic validation failure.
    Generic(&'static str),
}

impl LfsValidationError {
    /// Returns the stable message string for this error.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::InvalidOid => "invalid oid",
            Self::UnsupportedOperation => "unsupported operation",
            Self::UnsupportedHashAlgorithm => "unsupported hash algorithm",
            Self::UnsupportedTransferAdapter => "unsupported transfer adapter",
            Self::TooManyObjects => "too many objects",
            Self::ObjectNotFound => "Object does not exist",
            Self::ObjectTooLarge => "object too large for server-side verification",
            Self::HashMismatch => "SHA-256 hash mismatch",
            Self::Generic(msg) => msg,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct LfsBatchRequest {
    pub operation: String,
    #[serde(default)]
    pub transfers: Vec<String>,
    pub objects: Vec<LfsObjectRequest>,
    #[serde(default)]
    pub hash_algo: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LfsObjectRequest {
    pub oid: String,
    pub size: u64,
}

#[derive(Debug, Serialize)]
pub struct LfsBatchResponse {
    pub transfer: String,
    pub objects: Vec<LfsObjectResponse>,
    pub hash_algo: &'static str,
}

#[derive(Debug, Serialize)]
pub struct LfsObjectResponse {
    pub oid: String,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authenticated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actions: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<LfsObjectError>,
}

#[derive(Debug, Serialize)]
pub struct LfsObjectError {
    pub code: u16,
    pub message: String,
}

/// Returns the storage object key for an LFS object.
///
/// Maps an LFS object ID to its content-addressed location under the global or
/// repository-scoped namespace. The namespace is derived from the verified
/// [`AuthorizedRepository`] capability: `None` (permissive, anonymous
/// full-access) resolves to the global namespace, a scoped capability to the
/// repository's SHA-256 namespace.
///
/// # Examples
///
/// ```
/// use shardline_protocol_adapters::lfs_object_key;
/// use shardline_server_core::AuthorizedRepository;
///
/// let oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
/// let key = lfs_object_key(oid, &AuthorizedRepository::anonymous_full_access())?;
/// assert!(key.as_str().starts_with("protocols/lfs/global/objects/"));
/// assert!(key.as_str().ends_with(oid));
///
/// assert!(
///     lfs_object_key(
///         "not-a-valid-sha256",
///         &AuthorizedRepository::anonymous_full_access()
///     )
///     .is_err()
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Errors
///
/// Returns [`ProtocolError::InvalidContentHash`] when `oid` is malformed
/// or the constructed key is invalid.
pub fn lfs_object_key(oid: &str, auth: &AuthorizedRepository) -> Result<ObjectKey, ProtocolError> {
    validate_content_hash(oid)?;
    object_key(&format!(
        "protocols/lfs/{}/objects/{}",
        scope_namespace(auth.namespace()),
        oid
    ))
}

#[cfg(test)]
mod tests {
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
    use shardline_server_core::{AuthProvider, AuthorizedRepository, LocalHmacProvider};

    use super::*;

    fn valid_oid() -> String {
        "a".repeat(64)
    }

    fn test_scope() -> RepositoryScope {
        RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo", None).unwrap()
    }

    /// Builds a capability carrying the given repository scope (or a
    /// permissive anonymous capability when `None`), mirroring how the auth
    /// layer mints capabilities: the claims are verified through a real
    /// provider so the type-level seal is satisfied.
    fn test_capability(scope: Option<RepositoryScope>) -> AuthorizedRepository {
        scope.map_or_else(AuthorizedRepository::anonymous_full_access, |repo| {
            let claims =
                TokenClaims::new("local", "test", TokenScope::Write, repo, u64::MAX).unwrap();
            let provider = LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap();
            let token = provider.mint_token(&claims).unwrap();
            let ctx = provider.verify_verified(&token).unwrap();
            AuthorizedRepository::from_verified_context(ctx, TokenScope::Write).unwrap()
        })
    }

    // --- lfs_object_key ---

    #[test]
    fn lfs_object_key_valid_no_scope() {
        let oid = valid_oid();
        let key = lfs_object_key(&oid, &test_capability(None)).unwrap();
        assert!(key.as_str().contains("protocols/lfs/global/objects/"));
        assert!(key.as_str().ends_with(&oid));
    }

    #[test]
    fn lfs_object_key_valid_with_scope() {
        let oid = valid_oid();
        let scope = test_scope();
        let key = lfs_object_key(&oid, &test_capability(Some(scope))).unwrap();
        assert!(!key.as_str().contains("global"));
        assert!(key.as_str().contains("protocols/lfs/"));
        assert!(key.as_str().ends_with(&oid));
    }

    #[test]
    fn lfs_object_key_invalid_too_short() {
        let result = lfs_object_key("abc123", &test_capability(None));
        assert!(matches!(result, Err(ProtocolError::InvalidContentHash)));
    }

    #[test]
    fn lfs_object_key_invalid_uppercase() {
        let uppercase_oid = "A".repeat(64);
        let result = lfs_object_key(&uppercase_oid, &test_capability(None));
        assert!(matches!(result, Err(ProtocolError::InvalidContentHash)));
    }

    // --- LfsBatchRequest deserialization ---

    #[test]
    fn lfs_batch_request_deserialize_valid_json() {
        let json = r#"{
            "operation": "download",
            "transfers": ["basic"],
            "objects": [{"oid": "aabbccdd", "size": 1234}],
            "hash_algo": "sha256"
        }"#;
        let request: LfsBatchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.operation, "download");
        assert_eq!(request.transfers, vec!["basic"]);
        assert_eq!(request.objects.len(), 1);
        assert_eq!(request.objects[0].oid, "aabbccdd");
        assert_eq!(request.objects[0].size, 1234);
        assert_eq!(request.hash_algo.as_deref(), Some("sha256"));
    }

    #[test]
    fn lfs_batch_request_deserialize_missing_optional_fields() {
        let json = r#"{
            "operation": "upload",
            "objects": [{"oid": "aabbccdd", "size": 5678}]
        }"#;
        let request: LfsBatchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.operation, "upload");
        assert!(request.transfers.is_empty());
        assert!(request.hash_algo.is_none());
        assert_eq!(request.objects.len(), 1);
    }

    // --- LfsBatchResponse serialization ---

    #[test]
    fn lfs_batch_response_serialize_valid_json() {
        let response = LfsBatchResponse {
            transfer: "basic".to_owned(),
            objects: vec![LfsObjectResponse {
                oid: "deadbeef".to_owned(),
                size: 42,
                authenticated: Some(true),
                actions: None,
                error: None,
            }],
            hash_algo: "sha256",
        };
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["transfer"], "basic");
        assert_eq!(json["hash_algo"], "sha256");
        assert_eq!(json["objects"][0]["oid"], "deadbeef");
        assert_eq!(json["objects"][0]["size"], 42);
        assert_eq!(json["objects"][0]["authenticated"], true);
    }

    #[test]
    fn lfs_batch_response_hash_algo_is_always_sha256() {
        let response = LfsBatchResponse {
            transfer: "basic".to_owned(),
            objects: vec![],
            hash_algo: "sha256",
        };
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["hash_algo"], "sha256");
    }

    #[test]
    fn lfs_batch_response_omits_none_fields() {
        let response = LfsBatchResponse {
            transfer: "basic".to_owned(),
            objects: vec![LfsObjectResponse {
                oid: "deadbeef".to_owned(),
                size: 42,
                authenticated: None,
                actions: None,
                error: None,
            }],
            hash_algo: "sha256",
        };
        let json = serde_json::to_value(&response).unwrap();
        assert!(json["objects"][0].get("authenticated").is_none());
        assert!(json["objects"][0].get("actions").is_none());
        assert!(json["objects"][0].get("error").is_none());
    }

    // --- LfsObjectError serialization ---

    #[test]
    fn lfs_object_error_serialize() {
        let err = LfsObjectError {
            code: 404,
            message: "Not found".into(),
        };
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(json["code"], 404);
        assert_eq!(json["message"], "Not found");
    }

    // --- LfsBatchResponse with actions ---

    #[test]
    fn lfs_batch_response_with_actions() {
        let response = LfsBatchResponse {
            transfer: "basic".to_owned(),
            objects: vec![LfsObjectResponse {
                oid: "deadbeef".to_owned(),
                size: 42,
                authenticated: None,
                actions: Some(serde_json::json!({"download": {"href": "https://example.com/obj"}})),
                error: None,
            }],
            hash_algo: "sha256",
        };
        let json = serde_json::to_value(&response).unwrap();
        let obj = &json["objects"][0];
        assert_eq!(
            obj["actions"]["download"]["href"],
            "https://example.com/obj"
        );
    }

    // --- LfsBatchResponse with error on object ---

    #[test]
    fn lfs_batch_response_with_object_error() {
        let response = LfsBatchResponse {
            transfer: "basic".to_owned(),
            objects: vec![LfsObjectResponse {
                oid: "deadbeef".to_owned(),
                size: 42,
                authenticated: None,
                actions: None,
                error: Some(LfsObjectError {
                    code: 422,
                    message: "validation error".into(),
                }),
            }],
            hash_algo: "sha256",
        };
        let json = serde_json::to_value(&response).unwrap();
        let obj = &json["objects"][0];
        assert_eq!(obj["error"]["code"], 422);
        assert_eq!(obj["error"]["message"], "validation error");
    }

    // --- LfsBatchResponse with mixed objects ---

    #[test]
    fn lfs_batch_response_mixed_objects() {
        let response = LfsBatchResponse {
            transfer: "basic".to_owned(),
            objects: vec![
                LfsObjectResponse {
                    oid: "aaa".to_owned(),
                    size: 1,
                    authenticated: Some(true),
                    actions: Some(serde_json::json!({"download": {"href": "https://ok"}})),
                    error: None,
                },
                LfsObjectResponse {
                    oid: "bbb".to_owned(),
                    size: 2,
                    authenticated: None,
                    actions: None,
                    error: Some(LfsObjectError {
                        code: 404,
                        message: "gone".into(),
                    }),
                },
                LfsObjectResponse {
                    oid: "ccc".to_owned(),
                    size: 3,
                    authenticated: None,
                    actions: None,
                    error: None,
                },
            ],
            hash_algo: "sha256",
        };
        let json = serde_json::to_value(&response).unwrap();
        let objects = &json["objects"];
        assert_eq!(objects.as_array().unwrap().len(), 3);

        // First object: has actions
        assert!(objects[0].get("actions").is_some());

        // Second object: has error
        assert_eq!(objects[1]["error"]["code"], 404);

        // Third object: no actions, no error (both omitted)
        assert!(objects[2].get("actions").is_none());
        assert!(objects[2].get("error").is_none());
        assert!(objects[2].get("authenticated").is_none());
    }

    // --- LfsObjectRequest deserialization ---

    #[test]
    fn lfs_object_request_deserialize() {
        let json = r#"{"oid": "abc123", "size": 999}"#;
        let req: LfsObjectRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.oid, "abc123");
        assert_eq!(req.size, 999);
    }
}
