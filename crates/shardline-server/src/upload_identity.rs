//! Exact repository coordinates for private upload evidence.
use shardline_protocol::RepositoryScope;

use crate::ServerError;

#[cfg(test)]
mod tests;

pub(crate) const SCOPE_PREFIX: &str = "scope-json-v1:";

pub(crate) enum PreparedNativeXorb {
    Memory(axum::body::Bytes),
    File(Box<shardline_xet_adapter::PreparedXorbFile>),
}

/// Track the validated request representation separately from historical raw
/// wire-length identities. Footerless and normalized retries share this identity.
/// An equivalent retained encoding can have a different physical length; recovery
/// must then verify the complete native content identity before advancing it.
pub(crate) fn xorb(hash: &str, canonical_length: u64) -> String {
    format!("xorb-v2-{hash}-{canonical_length}")
}

pub(crate) async fn xorb_length(
    hash: &str,
    staged: crate::upload_ingest::StagedRequestBody,
    pool: &crate::admission::BoundedPool,
) -> Result<(PreparedNativeXorb, u64), ServerError> {
    let hash = hash.to_owned();
    crate::admission::cancellable_work_with_setup(
        pool,
        || staged,
        move |staged, cancel| prepare_staged_xorb(&hash, staged, &|| cancel.check_io()),
    )
    .await
}

fn prepare_staged_xorb(
    hash: &str,
    staged: crate::upload_ingest::StagedRequestBody,
    checkpoint: &dyn Fn() -> std::io::Result<()>,
) -> Result<(PreparedNativeXorb, u64), ServerError> {
    checkpoint()?;
    let (staged, canonical_length) = match staged {
        crate::upload_ingest::StagedRequestBody::Memory { bytes, .. } => {
            let canonical_length =
                shardline_xet_adapter::canonical_uploaded_xorb_length(hash, &bytes)?;
            (PreparedNativeXorb::Memory(bytes), canonical_length)
        }
        crate::upload_ingest::StagedRequestBody::File { file, .. } => {
            let file = shardline_xet_adapter::prepare_owned_xorb_file_with_checkpoint(
                hash, file, checkpoint,
            )?;
            let canonical_length = file.length();
            (PreparedNativeXorb::File(Box::new(file)), canonical_length)
        }
    };
    checkpoint()?;
    Ok((staged, canonical_length))
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
