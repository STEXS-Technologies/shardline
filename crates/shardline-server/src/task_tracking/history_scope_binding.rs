use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use penelope::core::task::Digest;
use serde::{Deserialize, Serialize};

use super::{TaskEvidenceStore, TaskRunIdentity};
use crate::ServerError;

const MAX_BINDING_BYTES: u64 = 2048;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    schema: u32,
    scope: Digest,
    manifest: Digest,
    signature: Vec<u8>,
}

fn invalid() -> ServerError {
    ServerError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "invalid task history scope binding",
    ))
}

fn message(scope: Digest, manifest: Digest) -> Result<Vec<u8>, ServerError> {
    Ok(serde_json::to_vec(&(
        "shardline.task-history-binding.v1",
        scope,
        manifest,
    ))?)
}

impl TaskEvidenceStore {
    pub(super) async fn publish_scope_on_work_connection(
        &self,
        identity: &TaskRunIdentity,
        key: &SigningKey,
        scope: Digest,
        connection: &mut sqlx::PgConnection,
    ) -> Result<bool, ServerError> {
        let Some(metadata) = &self.postgres_metadata else {
            return Ok(false);
        };
        if metadata.scope != scope || key.verifying_key().as_bytes() != &identity.public_key {
            return Err(invalid());
        }
        // Legacy duplicates must agree too. This read uses only the existing
        // object adapter, never a second PostgreSQL connection.
        let legacy = Self::from_objects(self.objects.clone());
        let observed = identity.clone();
        let (legacy, legacy_binding) = tokio::task::spawn_blocking(move || {
            let manifest = legacy.binding_manifest(&observed)?;
            let binding = legacy.read(
                &format!("scope-bindings/{}", observed.id),
                MAX_BINDING_BYTES,
            )?;
            Ok::<_, ServerError>((manifest, binding))
        })
        .await
        .map_err(|error| ServerError::Io(std::io::Error::other(error)))??;
        let manifest_key = Self::key(&format!("manifests/{}", identity.id))?;
        let stored = shardline_index::PostgresIndexStore::read_task_artifact_on_connection(
            connection,
            manifest_key.as_str(),
            65536,
        )
        .await?;
        let stored = stored
            .map(|artifact| artifact.bytes.ok_or(ServerError::RequestBodyTooLarge))
            .transpose()?;
        if let (Some(stored), Some(legacy)) = (&stored, &legacy)
            && stored != legacy
        {
            return Err(invalid());
        }
        let manifest = stored.or(legacy).ok_or_else(invalid)?;
        let parsed: TaskRunIdentity =
            serde_json::from_slice(&manifest).map_err(|_error| invalid())?;
        if serde_json::to_vec(&parsed)? != serde_json::to_vec(identity)? {
            return Err(invalid());
        }
        let binding = Binding {
            schema: 1,
            scope,
            manifest: Digest::of(&manifest),
            signature: key
                .sign(&message(scope, Digest::of(&manifest))?)
                .to_bytes()
                .to_vec(),
        };
        let binding_key = Self::key(&format!("scope-bindings/{}", identity.id))?;
        let bytes = serde_json::to_vec(&binding)?;
        if legacy_binding
            .as_ref()
            .is_some_and(|previous_binding| previous_binding != &bytes)
        {
            return Err(invalid());
        }
        if self.metadata_only_publication {
            let published =
                shardline_index::PostgresIndexStore::publish_task_artifact_bounded_on_connection(
                    connection,
                    binding_key.as_str(),
                    &bytes,
                    super::admission::AUTOMATIC_HISTORY_CAPACITY,
                )
                .await?;
            super::admission::automatic_publication_result(published)?;
        } else {
            shardline_index::PostgresIndexStore::publish_task_artifact_on_connection(
                connection,
                binding_key.as_str(),
                &bytes,
            )
            .await?;
        }
        Ok(true)
    }

    fn binding_manifest(&self, identity: &TaskRunIdentity) -> Result<Option<Vec<u8>>, ServerError> {
        if identity.schema != 1
            || identity.id.is_empty()
            || identity.id.len() > 128
            || identity
                .id
                .chars()
                .any(|character| character.is_control() || character == '/')
        {
            return Err(invalid());
        }
        let Some(bytes) = self.read(&format!("manifests/{}", identity.id), 65536)? else {
            return Ok(None);
        };
        let stored: TaskRunIdentity = serde_json::from_slice(&bytes).map_err(|_error| invalid())?;
        if serde_json::to_vec(&stored)? != serde_json::to_vec(identity)? {
            return Err(invalid());
        }
        Ok(Some(bytes))
    }

    /// Publishes an immutable association signed by this run's existing key.
    /// It binds the exact stored manifest bytes, preserving legacy records and
    /// readers. It proves association, not completion, health or work ownership.
    ///
    /// # Errors
    /// Returns a missing/mismatched manifest or key, conflicting association,
    /// encoding failure, or storage failure. Repeating the same binding is safe.
    pub fn publish_history_scope_binding(
        &self,
        identity: &TaskRunIdentity,
        key: &SigningKey,
        scope: Digest,
    ) -> Result<(), ServerError> {
        if key.verifying_key().as_bytes() != &identity.public_key {
            return Err(invalid());
        }
        if self
            .postgres_metadata
            .as_ref()
            .is_some_and(|metadata| metadata.scope != scope)
        {
            return Err(invalid());
        }
        let manifest = Digest::of(&self.binding_manifest(identity)?.ok_or_else(invalid)?);
        let binding = Binding {
            schema: 1,
            scope,
            manifest,
            signature: key.sign(&message(scope, manifest)?).to_bytes().to_vec(),
        };
        self.publish(
            &format!("scope-bindings/{}", identity.id),
            &serde_json::to_vec(&binding)?,
        )
    }

    /// Checks a signed association against the exact stored manifest and its
    /// pinned author key. A valid association with another scope returns false.
    /// No association or no manifest also returns false.
    ///
    /// # Errors
    /// Returns corrupt or oversized evidence, identity substitution, invalid
    /// signatures, encoding failure, or storage failure. Verify before filtering
    /// by scope so changing an unsigned scope field cannot hide corruption.
    pub fn history_scope_binding_matches(
        &self,
        identity: &TaskRunIdentity,
        scope: Digest,
    ) -> Result<bool, ServerError> {
        Ok(self
            .history_scope_binding_match(identity, scope)?
            .unwrap_or(false))
    }

    pub(crate) fn history_scope_binding_match(
        &self,
        identity: &TaskRunIdentity,
        scope: Digest,
    ) -> Result<Option<bool>, ServerError> {
        let Some(manifest) = self.binding_manifest(identity)? else {
            return Ok(None);
        };
        self.history_scope_binding_match_for_manifest(identity, scope, &manifest)
    }

    /// Verifies a binding against exact manifest bytes retained by an internal
    /// caller, including a retirement marker after the live manifest is gone.
    pub(crate) fn history_scope_binding_match_for_manifest(
        &self,
        identity: &TaskRunIdentity,
        scope: Digest,
        manifest: &[u8],
    ) -> Result<Option<bool>, ServerError> {
        let stored: TaskRunIdentity =
            serde_json::from_slice(manifest).map_err(|_error| invalid())?;
        if serde_json::to_vec(&stored)? != serde_json::to_vec(identity)? {
            return Err(invalid());
        }
        let Some(bytes) = self.read(
            &format!("scope-bindings/{}", identity.id),
            MAX_BINDING_BYTES,
        )?
        else {
            return Ok(None);
        };
        let binding: Binding = serde_json::from_slice(&bytes).map_err(|_error| invalid())?;
        if binding.schema != 1 || binding.manifest != Digest::of(manifest) {
            return Err(invalid());
        }
        let key = VerifyingKey::from_bytes(&identity.public_key).map_err(|_error| invalid())?;
        let signature = Signature::from_slice(&binding.signature).map_err(|_error| invalid())?;
        key.verify_strict(&message(binding.scope, binding.manifest)?, &signature)
            .map_err(|_error| invalid())?;
        Ok(Some(binding.scope == scope))
    }
}
