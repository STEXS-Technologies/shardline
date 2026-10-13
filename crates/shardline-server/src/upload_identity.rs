//! Exact repository coordinates for private upload evidence.
use shardline_protocol::RepositoryScope;

use crate::ServerError;

pub(crate) const SCOPE_PREFIX: &str = "scope-json-v1:";

/// Distinguish canonical physical representations from historical wire-length
/// identities. Footerless and normalized retries share this private identity.
pub(crate) fn xorb(hash: &str, canonical_length: u64) -> String {
    format!("xorb-v2-{hash}-{canonical_length}")
}

pub(crate) async fn xorb_length(
    hash: &str,
    staged: crate::upload_ingest::StagedRequestBody,
    pool: &crate::admission::BoundedPool,
) -> Result<(crate::upload_ingest::StagedRequestBody, u64), ServerError> {
    let hash = hash.to_owned();
    crate::admission::blocking_work(pool, move || {
        let length = match &staged {
            crate::upload_ingest::StagedRequestBody::Memory { bytes, .. } => {
                shardline_xet_adapter::canonical_uploaded_xorb_length(&hash, bytes)?
            }
            crate::upload_ingest::StagedRequestBody::File { file, .. } => {
                shardline_xet_adapter::canonical_uploaded_xorb_file_length(&hash, file.path())?
            }
        };
        Ok((staged, length))
    })
    .await
}

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
