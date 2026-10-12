use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use penelope_core::task::Digest;
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
        let Some(bytes) = self.read(
            &format!("scope-bindings/{}", identity.id),
            MAX_BINDING_BYTES,
        )?
        else {
            return Ok(None);
        };
        let binding: Binding = serde_json::from_slice(&bytes).map_err(|_error| invalid())?;
        if binding.schema != 1 || binding.manifest != Digest::of(&manifest) {
            return Err(invalid());
        }
        let key = VerifyingKey::from_bytes(&identity.public_key).map_err(|_error| invalid())?;
        let signature = Signature::from_slice(&binding.signature).map_err(|_error| invalid())?;
        key.verify_strict(&message(binding.scope, binding.manifest)?, &signature)
            .map_err(|_error| invalid())?;
        Ok(Some(binding.scope == scope))
    }
}
