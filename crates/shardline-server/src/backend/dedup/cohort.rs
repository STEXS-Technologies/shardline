//! Bounded related candidates; every returned member is authorized separately.
use super::*;

const MAX_COHORT_XORBS: usize = 64;
const MAX_COHORT_CHUNKS: usize = 32_768;

#[cfg(test)]
mod tests;

pub(super) async fn descriptors(
    backend: &ServerBackend,
    primary: &XetXorbDescriptor,
    scope: Option<&RepositoryScope>,
    limits: crate::ShardMetadataLimits,
) -> Result<Vec<XetXorbDescriptor>, ServerError> {
    let mut result = vec![primary.clone()];
    let Some(scope) = scope else {
        return Ok(result);
    };
    let max_xorbs = MAX_COHORT_XORBS.min(limits.max_xorbs().get());
    let max_chunks = MAX_COHORT_CHUNKS.min(limits.max_xorb_chunks().get());
    let siblings = match backend {
        ServerBackend::Local(backend) => {
            sibling_hashes(backend.record_store(), &primary.hash, scope, max_xorbs).await?
        }
        ServerBackend::Postgres(backend) => {
            sibling_hashes(backend.record_store(), &primary.hash, scope, max_xorbs).await?
        }
    };
    let mut chunks = primary.chunks.len();
    for hash in siblings {
        if result.len() >= max_xorbs || chunks >= max_chunks {
            break;
        }
        let descriptor = match backend {
            ServerBackend::Local(backend) => {
                backend.record_store().xet_xorb_descriptor(&hash).await?
            }
            ServerBackend::Postgres(backend) => {
                backend.record_store().xet_xorb_descriptor(&hash).await?
            }
        };
        let Some(descriptor) = descriptor else {
            continue;
        };
        let Some(total) = chunks.checked_add(descriptor.chunks.len()) else {
            return Err(invalid_descriptor());
        };
        if total <= max_chunks {
            chunks = total;
            result.push(descriptor);
        }
    }
    Ok(result)
}

async fn sibling_hashes<Store: RecordTraversal + Sync>(
    store: &Store,
    hash: &str,
    scope: &RepositoryScope,
    limit: usize,
) -> Result<Vec<String>, ServerError>
where
    ServerError: From<Store::Error>,
    Store::Error: Send,
{
    // One indexed candidate, never a repository scan. Stale or incompatible
    // metadata simply leaves the existing single-Xorb response unchanged.
    let Some(first) = store.first_repository_hash_record(scope, hash).await? else {
        return Ok(Vec::new());
    };
    let record = parse_stored_file_record_bytes(&first.bytes)?;
    if record.repository_scope.as_ref() != Some(scope)
        || !record.chunks.iter().any(|term| term.hash == hash)
    {
        return Ok(Vec::new());
    }
    let mut hashes = Vec::new();
    for term in &record.chunks {
        if hashes.len() >= limit.saturating_sub(1) {
            break;
        }
        if term.hash != hash && !hashes.contains(&term.hash) {
            hashes.push(term.hash.clone());
        }
    }
    Ok(hashes)
}
