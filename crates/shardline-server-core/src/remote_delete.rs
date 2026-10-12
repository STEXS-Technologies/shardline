//! Durable remote-delete boundary shared by maintenance entry points.

use shardline_index::{PostgresIndexStore, PostgresMetadataStoreError};
use shardline_storage::{DeleteOutcome, ObjectKey, ObjectStore};
use thiserror::Error;

use crate::{ServerObjectStore, ServerObjectStoreError};

/// Failure at the durable remote-delete boundary.
#[derive(Debug, Error)]
pub enum GuardedRemoteDeleteError {
    /// Admission or settlement failed; no replacement session is authorized.
    #[error(transparent)]
    Metadata(#[from] PostgresMetadataStoreError),
    /// The remote outcome may be unknown. Its marker must stay pending.
    #[error(transparent)]
    ObjectStore(#[from] ServerObjectStoreError),
}

/// Deletes through the same non-replaceable session that owns GC's barrier.
///
/// S3 admission commits before the request. Only a successful single-attempt
/// response allows exact-epoch settlement on the original ownership session.
/// Errors and cancellation leave the marker pending. This protects writers that
/// transactionally enforce the marker; it does not grant ownership or prove
/// that every publication path enforces it. The caller must hold the exclusive
/// maintenance barrier for the entire operation.
///
/// # Errors
///
/// Returns admission, remote, or settlement failure. Remote errors are uncertain
/// even if a subsequent object inspection finds the object absent.
pub async fn delete_with_postgres_guard(
    index: &PostgresIndexStore,
    objects: &ServerObjectStore,
    key: &ObjectKey,
) -> Result<DeleteOutcome, GuardedRemoteDeleteError> {
    let ServerObjectStore::S3(store) = objects else {
        // Keep local GC's existing synchronous mutation boundary. Spawning a
        // background delete here could outlive cancellation of this future.
        return Ok(ObjectStore::delete_if_present(objects, key)?);
    };
    let permit = index.begin_remote_delete(key).await?;
    let outcome = store
        .delete_if_present_single_attempt(key)
        .await
        .map_err(ServerObjectStoreError::from)?;
    if !index.finish_remote_delete(&permit).await? {
        return Err(PostgresMetadataStoreError::Unsupported(
            "remote delete attempt changed before settlement".to_owned(),
        )
        .into());
    }
    Ok(outcome)
}
