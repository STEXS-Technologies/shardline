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

/// Outcome of reconciling a verified saved reply with current metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteDeleteReceiptSettlement {
    /// The exact registered attempt was settled.
    Settled,
    /// The signed reply matches the target, which has no pending delete for it.
    NoPendingDelete,
    /// A different target, verifier or pending attempt remains unchanged.
    Unmatched,
}

impl RemoteDeleteReceipt {
    /// Checks the signed observation and returns its metadata namespace.
    /// This checks authenticity only; settlement must still check the pinned key.
    /// # Errors
    /// Returns malformed fields or an invalid signature.
    pub fn verified_namespace(&self) -> Result<&str, PostgresMetadataStoreError> {
        ObjectKey::parse(&self.object_key).map_err(|_error| invalid())?;
        if self.epoch <= 0
            || self.namespace.len() != 64
            || !self
                .namespace
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid());
        }
        let key = VerifyingKey::from_bytes(&self.public_key).map_err(|_error| invalid())?;
        let signature = Signature::from_slice(&self.signature).map_err(|_error| invalid())?;
        key.verify_strict(
            &message(&self.schema, &self.namespace, &self.object_key, self.epoch)?,
            &signature,
        )
        .map_err(|_error| invalid())?;
        Ok(&self.namespace)
    }
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
    /// Reads the saved-reply namespace without creating it. Returns `None` on
    /// an older owner installation; callers may retain legacy admission there.
    /// # Errors
    /// Returns unavailable metadata, insufficient GC privileges or corrupt scope.
    pub async fn remote_delete_receipt_namespace(
        &self,
    ) -> Result<Option<String>, PostgresMetadataStoreError> {
        let installed: bool = sqlx::query_scalar(
            "SELECT to_regprocedure(format('%I.shardline_remote_delete_receipt_namespace_v1(text)', current_schema())) IS NOT NULL
             AND to_regprocedure(format('%I.shardline_remote_delete_receipt_begin_v1(text,text,text)', current_schema())) IS NOT NULL",
        ).fetch_one(&self.pool).await?;
        if !installed {
            return Ok(None);
        }
        Ok(Some(
            sqlx::query_scalar(
                "SELECT shardline_remote_delete_receipt_namespace_v1(current_schema())",
            )
            .fetch_one(&self.pool)
            .await?,
        ))
    }
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
        Ok(self.reconcile_remote_delete_receipt(receipt).await?
            == RemoteDeleteReceiptSettlement::Settled)
    }

    /// Reconciles a saved reply and distinguishes safe evidence retirement from
    /// a pending or foreign attempt. It never sends a remote storage request.
    /// # Errors
    /// Returns invalid evidence, unavailable metadata or insufficient GC access.
    pub async fn reconcile_remote_delete_receipt(
        &self,
        receipt: &RemoteDeleteReceipt,
    ) -> Result<RemoteDeleteReceiptSettlement, PostgresMetadataStoreError> {
        receipt.verified_namespace()?;
        let mut transaction = self.pool.begin().await?;
        let schema: String = sqlx::query_scalar("SELECT current_schema()")
            .fetch_one(&mut *transaction)
            .await?;
        if schema != receipt.schema {
            return Ok(RemoteDeleteReceiptSettlement::Unmatched);
        }
        let namespace: String = sqlx::query_scalar(
            "SELECT shardline_remote_delete_receipt_namespace_v1(current_schema())",
        )
        .fetch_one(&mut *transaction)
        .await?;
        if namespace != receipt.namespace {
            return Ok(RemoteDeleteReceiptSettlement::Unmatched);
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
        let result = if changed {
            RemoteDeleteReceiptSettlement::Settled
        } else {
            // The failed finish still holds the object locks. A later admission
            // cannot race the check that allows retiring already-settled evidence.
            let pending: bool = sqlx::query_scalar(
                "SELECT shardline_remote_delete_receipt_pending_v1(current_schema(), $1)",
            )
            .bind(&receipt.object_key)
            .fetch_one(&mut *transaction)
            .await?;
            if pending {
                RemoteDeleteReceiptSettlement::Unmatched
            } else {
                RemoteDeleteReceiptSettlement::NoPendingDelete
            }
        };
        transaction.commit().await?;
        Ok(result)
    }
}

#[cfg(test)]
#[path = "remote_delete_receipts_tests.rs"]
mod tests;
