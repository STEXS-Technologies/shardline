//! Durable remote-delete boundary shared by maintenance entry points.

use ed25519_dalek::SigningKey;
use shardline_index::{PostgresIndexStore, PostgresMetadataStoreError};
use shardline_storage::{DeleteOutcome, ObjectKey, ObjectStore};
use thiserror::Error;

use crate::{ServerObjectStore, ServerObjectStoreError};

mod replies;
pub use replies::{RemoteDeleteReplyRecovery, recover_saved_remote_delete_replies};

/// Failure at the durable remote-delete boundary.
#[derive(Debug, Error)]
pub enum GuardedRemoteDeleteError {
    /// Admission or settlement failed; no replacement work session is authorized.
    #[error(transparent)]
    Metadata(#[from] PostgresMetadataStoreError),
    /// The remote outcome may be unknown. Its marker must stay pending.
    #[error(transparent)]
    ObjectStore(#[from] ServerObjectStoreError),
}

/// Deletes through the same non-replaceable session that owns GC's barrier.
///
/// S3 admission commits before the request. Only a successful single-attempt
/// response allows exact-epoch settlement. An optional independent recorder may
/// settle that acknowledged attempt after ownership loss, but the original
/// error still stops GC. Remote errors and cancellation leave the marker pending.
/// This protects writers that
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
    let signing_key = if index.supports_remote_delete_receipts().await? {
        let mut seed = [0_u8; 32];
        getrandom::fill(&mut seed).map_err(|_error| {
            PostgresMetadataStoreError::Unsupported(
                "remote delete signing entropy unavailable".to_owned(),
            )
        })?;
        Some(SigningKey::from_bytes(&seed))
    } else {
        None
    };
    let permit = if let Some(signing_key) = &signing_key {
        index
            .begin_remote_delete_with_receipt(key, *signing_key.verifying_key().as_bytes())
            .await?
    } else {
        index.begin_remote_delete(key).await?
    };
    let outcome = store
        .delete_if_present_single_attempt(key)
        .await
        .map_err(ServerObjectStoreError::from)?;
    // Preserve only an observed reply. Never create evidence on remote failure.
    // Evidence storage is optional: existing GC-only object-store credentials
    // must not gain a write requirement. Even when saving fails, try the same
    // acknowledged settlement as before; ownership loss still stops all work.
    let saved_reply = signing_key.as_ref().and_then(|signing_key| {
        let saved = (|| -> Result<ObjectKey, GuardedRemoteDeleteError> {
            let receipt = permit.sign_acknowledged_receipt(signing_key)?;
            replies::save(objects, &receipt)
        })();
        match saved {
            Ok(receipt_key) => Some(receipt_key),
            Err(error) => {
                tracing::warn!(%error, "could not save remote delete reply; attempting ordinary acknowledged settlement");
                None
            }
        }
    });
    if !index.finish_acknowledged_remote_delete(&permit).await? {
        return Err(PostgresMetadataStoreError::Unsupported(
            "remote delete attempt changed before settlement".to_owned(),
        )
        .into());
    }
    if let Some(receipt_key) = saved_reply
        && let Err(error) = ObjectStore::delete_if_present(objects, &receipt_key)
    {
        tracing::warn!(%error, "remote delete settled but saved reply cleanup failed");
    }
    Ok(outcome)
}
