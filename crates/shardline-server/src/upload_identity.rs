//! Exact repository coordinates for private upload evidence.
use shardline_protocol::RepositoryScope;

use crate::ServerError;

#[cfg(test)]
mod tests;

pub(crate) const SCOPE_PREFIX: &str = "scope-json-v1:";

pub(crate) type PreparedNativeXorb = shardline_xet_adapter::PreparedXorbMemory;
pub(crate) type NativeUploadResources = (
    tokio::sync::OwnedSemaphorePermit,
    tokio::sync::OwnedSemaphorePermit,
);

/// Track the validated request representation separately from historical raw
/// wire-length identities. Footerless and normalized retries share this identity.
/// An equivalent retained encoding can have a different physical length; recovery
/// must then verify the complete native content identity before advancing it.
pub(crate) fn xorb(hash: &str, canonical_length: u64) -> String {
    format!("xorb-v2-{hash}-{canonical_length}")
}

#[cfg(test)]
pub(crate) async fn xorb_length(
    hash: &str,
    staged: crate::upload_ingest::NativeBody,
    pool: &crate::admission::BoundedPool,
) -> Result<(PreparedNativeXorb, u64, NativeUploadResources), ServerError> {
    let hash = hash.to_owned();
    let worker = pool
        .acquire()
        .await
        .map_err(|error| ServerError::Io(std::io::Error::other(error)))?;
    let ((prepared, admission), worker) =
        crate::admission::cancellable_work_with_permit(worker, move |cancel| {
            let crate::upload_ingest::NativeBody { bytes, admission } = staged;
            cancel.check()?;
            let prepared = shardline_xet_adapter::prepare_owned_xorb_memory_with_checkpoint(
                &hash,
                bytes,
                &|| cancel.check_io(),
            )?;
            cancel.check()?;
            Ok((prepared, admission))
        })
        .await?;
    let length = prepared.length();
    Ok((prepared, length, (admission, worker)))
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
