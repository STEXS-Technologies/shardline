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
