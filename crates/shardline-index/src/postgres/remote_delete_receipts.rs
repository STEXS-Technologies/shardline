use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use shardline_storage::ObjectKey;

use super::{PostgresIndexStore, PostgresMetadataStoreError, PostgresRemoteDeletePermit};

/// A signed observation of a successful single-attempt remote delete reply.
/// This is historical evidence, not authority to issue another storage request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteDeleteReceipt {
    schema: String,
    namespace: String,
    object_key: String,
    epoch: i64,
    public_key: [u8; 32],
    signature: Vec<u8>,
}

fn invalid() -> PostgresMetadataStoreError {
    PostgresMetadataStoreError::Unsupported("invalid remote delete receipt".to_owned())
}

fn message(
    schema: &str,
    namespace: &str,
    object_key: &str,
    epoch: i64,
) -> Result<Vec<u8>, PostgresMetadataStoreError> {
    serde_json::to_vec(&(
        "shardline.remote-delete-reply.v1",
        schema,
        namespace,
        object_key,
        epoch,
    ))
    .map_err(|_error| invalid())
}

impl PostgresRemoteDeletePermit {
    /// Signs a successful reply using the fresh key pinned during admission.
    /// Call only after a single-attempt request has returned success, or with
    /// equivalent proof that every issued request has settled. A missing reply,
    /// absent object or elapsed time never authorizes creating this receipt.
    ///
    /// # Errors
    /// Returns an unregistered or mismatched key or an encoding failure.
    pub fn sign_acknowledged_receipt(
        &self,
        key: &SigningKey,
    ) -> Result<RemoteDeleteReceipt, PostgresMetadataStoreError> {
        let public_key = *key.verifying_key().as_bytes();
        if self.receipt_key != Some(public_key) {
            return Err(invalid());
        }
        let namespace = self.receipt_namespace.as_ref().ok_or_else(invalid)?;
        Ok(RemoteDeleteReceipt {
            schema: self.schema.clone(),
            namespace: namespace.clone(),
            object_key: self.object_key.as_str().to_owned(),
            epoch: self.epoch,
            public_key,
            signature: key
                .sign(&message(
                    &self.schema,
                    namespace,
                    self.object_key.as_str(),
                    self.epoch,
                )?)
                .to_bytes()
                .to_vec(),
        })
    }
}

impl PostgresIndexStore {
    /// Atomically pins a fresh per-attempt receipt verifier with the pending
    /// marker before any remote request. The caller retains the private key.
    /// Use a new signing key for every admitted attempt, including after restores.
    /// The original ownership and single-attempt request requirements still apply.
    ///
    /// # Errors
    /// Returns invalid verification keys, missing owner initialization, pending
    /// earlier deletes, insufficient existing GC privileges or database failure.
    pub async fn begin_remote_delete_with_receipt(
        &self,
        object_key: &ObjectKey,
        verification_key: [u8; 32],
    ) -> Result<PostgresRemoteDeletePermit, PostgresMetadataStoreError> {
        VerifyingKey::from_bytes(&verification_key).map_err(|_error| invalid())?;
        self.begin_remote_delete_with_key(object_key, Some(verification_key))
            .await
    }

    /// Verifies a saved reply against the originally pinned key and settles only
    /// that exact pending attempt. It sends no remote request and grants no work
    /// ownership. The caller must use the intended metadata target's pool.
    /// Legacy permits without a receipt verifier cannot be settled this way.
    ///
    /// # Errors
    /// Returns malformed receipts, invalid signatures, missing owner functions,
    /// insufficient existing GC privileges or database failures. Returns false
    /// for another schema, a stale attempt or a different pinned key.
    pub async fn settle_remote_delete_receipt(
        &self,
        receipt: &RemoteDeleteReceipt,
    ) -> Result<bool, PostgresMetadataStoreError> {
        ObjectKey::parse(&receipt.object_key).map_err(|_error| invalid())?;
        if receipt.epoch <= 0 {
            return Err(invalid());
        }
        let key = VerifyingKey::from_bytes(&receipt.public_key).map_err(|_error| invalid())?;
        let signature = Signature::from_slice(&receipt.signature).map_err(|_error| invalid())?;
        key.verify_strict(
            &message(
                &receipt.schema,
                &receipt.namespace,
                &receipt.object_key,
                receipt.epoch,
            )?,
            &signature,
        )
        .map_err(|_error| invalid())?;
        let mut transaction = self.pool.begin().await?;
        let schema: String = sqlx::query_scalar("SELECT current_schema()")
            .fetch_one(&mut *transaction)
            .await?;
        if schema != receipt.schema {
            return Ok(false);
        }
        let namespace: String =
            sqlx::query_scalar("SELECT shardline_task_history_scope_v1(current_schema())")
                .fetch_one(&mut *transaction)
                .await?;
        if namespace != receipt.namespace {
            return Ok(false);
        }
        // Compare the registered verifier under the same object locks used by
        // admission and settlement. Rechecking in SQL closes a read/settle race.
        let changed: bool = sqlx::query_scalar(
            "SELECT shardline_remote_delete_receipt_finish_v1(current_schema(), $1, $2, $3)",
        )
        .bind(&receipt.object_key)
        .bind(receipt.epoch)
        .bind(hex::encode(receipt.public_key))
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(changed)
    }
}

#[cfg(test)]
#[path = "remote_delete_receipts_tests.rs"]
mod tests;
