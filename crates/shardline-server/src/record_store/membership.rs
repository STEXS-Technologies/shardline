//! Stop a boolean membership lookup once authoritative metadata proves access.

use crate::ServerError;
use shardline_index::RecordTraversal;
use shardline_protocol::RepositoryScope;

#[cfg(test)]
mod tests;

// Traversal's visitor-error channel provides early termination. Keep a verified
// match distinct from both storage failures and invalid authoritative metadata.
pub(super) enum LookupError<StorageError> {
    Matched,
    InvalidRecord(ServerError),
    Storage(StorageError),
}

impl<StorageError> From<StorageError> for LookupError<StorageError> {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

/// Each predicate evaluation must independently establish a complete match.
/// A failed first-candidate probe may be followed by a fresh full traversal.
pub(crate) async fn any_repository_hash_record<Store, Predicate>(
    store: &Store,
    scope: &RepositoryScope,
    hash: &str,
    mut predicate: Predicate,
) -> Result<bool, ServerError>
where
    Store: RecordTraversal + Sync,
    Store::Error: Send,
    ServerError: From<Store::Error>,
    Predicate: FnMut(&[u8]) -> Result<bool, ServerError> + Send,
{
    let Some(first) = store.first_repository_hash_record(scope, hash).await? else {
        return Ok(false);
    };
    if predicate(&first.bytes)? {
        return Ok(true);
    }
    let result = store
        .visit_repository_hash_records(scope, hash, |entry| {
            if predicate(&entry.bytes).map_err(LookupError::InvalidRecord)? {
                return Err(LookupError::Matched);
            }
            Ok(())
        })
        .await;
    match result {
        Ok(()) => Ok(false),
        Err(LookupError::Matched) => Ok(true),
        Err(LookupError::InvalidRecord(error)) => Err(error),
        Err(LookupError::Storage(error)) => Err(ServerError::from(error)),
    }
}

pub(crate) async fn repository_references_hash_in_scope<RecordAdapter>(
    record_store: &RecordAdapter,
    hash_hex: &str,
    repository_scope: &RepositoryScope,
) -> Result<bool, ServerError>
where
    RecordAdapter: RecordTraversal + Sync,
    RecordAdapter::Error: Send,
    ServerError: From<RecordAdapter::Error>,
{
    any_repository_hash_record(record_store, repository_scope, hash_hex, |bytes| {
        stored_record_references_hash(bytes, hash_hex, repository_scope)
    })
    .await
}

pub(crate) fn stored_record_references_hash(
    bytes: &[u8],
    hash_hex: &str,
    repository_scope: &RepositoryScope,
) -> Result<bool, ServerError> {
    let record = super::parse_stored_file_record_bytes(bytes)?;
    if record.repository_scope.as_ref() != Some(repository_scope) {
        return Ok(false);
    }

    Ok(record.chunks.iter().any(|chunk| chunk.hash == hash_hex))
}
