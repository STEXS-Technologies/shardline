//! Exact repository coordinates for private upload evidence.
use shardline_protocol::RepositoryScope;

use crate::ServerError;

#[cfg(test)]
mod tests;

pub(crate) const SCOPE_PREFIX: &str = "scope-json-v1:";

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
) -> Result<(crate::upload_ingest::StagedRequestBody, u64), ServerError> {
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
) -> Result<(crate::upload_ingest::StagedRequestBody, u64), ServerError> {
    use std::io::Read;
    checkpoint()?;
    let (staged, canonical_length) = match staged {
        crate::upload_ingest::StagedRequestBody::Memory {
            bytes,
            length,
            hash: body_hash,
        } => {
            let canonical_length =
                shardline_xet_adapter::canonical_uploaded_xorb_length(hash, &bytes)?;
            (
                crate::upload_ingest::StagedRequestBody::Memory {
                    bytes,
                    length,
                    hash: body_hash,
                },
                canonical_length,
            )
        }
        crate::upload_ingest::StagedRequestBody::File {
            file,
            length,
            hash: mut body_hash,
        } => {
            let file = shardline_xet_adapter::normalize_owned_uploaded_xorb_file_with_checkpoint(
                hash, file, checkpoint,
            )?;
            let canonical_length = file.as_file().metadata()?.len();
            if canonical_length != length {
                let mut source = std::fs::File::open(file.path())?;
                let mut hasher = blake3::Hasher::new();
                let mut buffer = [0_u8; 64 * 1024];
                loop {
                    checkpoint()?;
                    let read = match source.read(&mut buffer) {
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                        result => result?,
                    };
                    if read == 0 {
                        break;
                    }
                    hasher.update(buffer.get(..read).ok_or(ServerError::Overflow)?);
                }
                body_hash =
                    shardline_protocol::ShardlineHash::from_bytes(*hasher.finalize().as_bytes());
            }
            (
                crate::upload_ingest::StagedRequestBody::File {
                    file,
                    length: canonical_length,
                    hash: body_hash,
                },
                canonical_length,
            )
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
