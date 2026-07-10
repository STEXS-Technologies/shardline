use serde::{Deserialize, Serialize};
use serde_json::Value;
use shardline_protocol::RepositoryScope;
use shardline_storage::ObjectKey;

use crate::{ProtocolError, object_key, scope_namespace, validate_content_hash};

pub const LFS_CONTENT_TYPE: &str = "application/vnd.git-lfs+json";

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
/// # Errors
///
/// Returns [`ProtocolError::InvalidContentHash`] when `oid` is malformed
/// or the constructed key is invalid.
pub fn lfs_object_key(
    oid: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, ProtocolError> {
    validate_content_hash(oid)?;
    object_key(&format!(
        "protocols/lfs/{}/objects/{}",
        scope_namespace(repository_scope),
        oid
    ))
}

#[cfg(test)]
mod tests {
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    use super::*;

    fn valid_oid() -> String {
        "a".repeat(64)
    }

    fn test_scope() -> RepositoryScope {
        RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo", None).unwrap()
    }

    // --- lfs_object_key ---

    #[test]
    fn lfs_object_key_valid_no_scope() {
        let oid = valid_oid();
        let key = lfs_object_key(&oid, None).unwrap();
        assert!(key.as_str().contains("protocols/lfs/global/objects/"));
        assert!(key.as_str().ends_with(&oid));
    }

    #[test]
    fn lfs_object_key_valid_with_scope() {
        let oid = valid_oid();
        let scope = test_scope();
        let key = lfs_object_key(&oid, Some(&scope)).unwrap();
        assert!(!key.as_str().contains("global"));
        assert!(key.as_str().contains("protocols/lfs/"));
        assert!(key.as_str().ends_with(&oid));
    }

    #[test]
    fn lfs_object_key_invalid_too_short() {
        let result = lfs_object_key("abc123", None);
        assert!(matches!(result, Err(ProtocolError::InvalidContentHash)));
    }

    #[test]
    fn lfs_object_key_invalid_uppercase() {
        let uppercase_oid = "A".repeat(64);
        let result = lfs_object_key(&uppercase_oid, None);
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
}
