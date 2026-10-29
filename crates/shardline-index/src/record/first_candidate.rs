//! Compatibility implementation for adapters without a bounded first-row query.

use super::{RecordStoreFuture, RecordTraversal, StoredRecord};
use shardline_protocol::RepositoryScope;

enum ReadError<StorageError> {
    Found,
    Storage(StorageError),
}

impl<StorageError> From<StorageError> for ReadError<StorageError> {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

pub(super) fn read<'operation, Store>(
    store: &'operation Store,
    repository: &'operation RepositoryScope,
    hash: &'operation str,
) -> RecordStoreFuture<'operation, Option<StoredRecord<Store::Locator>>, Store::Error>
where
    Store: RecordTraversal + Sync + ?Sized,
    Store::Error: Send + 'operation,
{
    Box::pin(async move {
        let mut first = None;
        let result = store
            .visit_repository_hash_records(repository, hash, |record| {
                first = Some(record);
                Err(ReadError::Found)
            })
            .await;
        match result {
            Ok(()) | Err(ReadError::Found) => Ok(first),
            Err(ReadError::Storage(error)) => Err(error),
        }
    })
}
