//! Exact repository coordinates for private upload evidence.
use shardline_protocol::RepositoryScope;

use crate::ServerError;

pub(crate) const SCOPE_PREFIX: &str = "scope-json-v1:";

pub(crate) fn repository(scope: Option<&RepositoryScope>) -> Result<String, ServerError> {
    scope.map_or_else(
        || Ok("default".to_owned()),
        |scope| Ok(format!("{SCOPE_PREFIX}{}", serde_json::to_string(scope)?)),
    )
}

/// Every new publication has its own stable scope identity. Historical IDs
/// remain untouched, including those previously shared by different scopes.
/// Storage keys and domain content hashes remain independent of this private ID.
pub(crate) fn native_shard(
    body_hash: &str,
    scope: Option<&RepositoryScope>,
) -> Result<(String, String), ServerError> {
    let repository = repository(scope)?;
    let id = format!(
        "shard-v2-{body_hash}-{}",
        blake3::hash(repository.as_bytes()).to_hex()
    );
    Ok((id, repository))
}
