use serde::{Deserialize, Serialize};
use serde_json::Value;
use shardline_protocol::RepositoryScope;
use shardline_storage::ObjectKey;

use crate::{
    ServerError,
    protocol_support::{object_key, scope_namespace},
    validation::validate_content_hash,
};

pub(crate) const LFS_CONTENT_TYPE: &str = "application/vnd.git-lfs+json";

#[derive(Debug, Deserialize)]
pub(crate) struct LfsBatchRequest {
    pub(crate) operation: String,
    #[serde(default)]
    pub(crate) transfers: Vec<String>,
    pub(crate) objects: Vec<LfsObjectRequest>,
    #[serde(default)]
    pub(crate) hash_algo: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LfsObjectRequest {
    pub(crate) oid: String,
    pub(crate) size: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct LfsBatchResponse {
    pub(crate) transfer: String,
    pub(crate) objects: Vec<LfsObjectResponse>,
    pub(crate) hash_algo: &'static str,
}

#[derive(Debug, Serialize)]
pub(crate) struct LfsObjectResponse {
    pub(crate) oid: String,
    pub(crate) size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) authenticated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) actions: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<LfsObjectError>,
}

#[derive(Debug, Serialize)]
pub(crate) struct LfsObjectError {
    pub(crate) code: u16,
    pub(crate) message: String,
}

pub(crate) fn lfs_object_key(
    oid: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, ServerError> {
    validate_content_hash(oid)?;
    object_key(&format!(
        "protocols/lfs/{}/objects/{}",
        scope_namespace(repository_scope),
        oid
    ))
}
