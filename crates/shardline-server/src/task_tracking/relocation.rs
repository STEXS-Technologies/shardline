//! Move finished legacy evidence without rewriting its signed bytes.
use super::{TASK_EVIDENCE_PREFIX, TaskEvidenceStore, TaskRunIdentity, TaskRunState};
use crate::{ServerError, object_store::ServerObjectStoreError};
use penelope_core::task::Digest;
use shardline_storage::{ObjectMetadata, ObjectPrefix, ObjectStore};

enum VisitError {
    Budget,
    Storage(ServerError),
}

impl From<ServerObjectStoreError> for VisitError {
    fn from(error: ServerObjectStoreError) -> Self {
        Self::Storage(error.into())
    }
}

impl From<shardline_storage::LocalObjectStoreError> for VisitError {
    fn from(error: shardline_storage::LocalObjectStoreError) -> Self {
        Self::from(ServerObjectStoreError::Local(error))
    }
}

impl From<shardline_storage::S3ObjectStoreError> for VisitError {
    fn from(error: shardline_storage::S3ObjectStoreError) -> Self {
        Self::from(ServerObjectStoreError::S3(error))
    }
}

impl From<ServerError> for VisitError {
    fn from(error: ServerError) -> Self {
        Self::Storage(error)
    }
}

#[derive(Default)]
struct Budget {
    moved: usize,
    bytes: u64,
}

impl TaskEvidenceStore {
    /// Relocates at most 64 legacy artifacts and 16 MiB per invocation.
    /// Only valid finished runs for this deployment or its verified shared
    /// metadata scope are considered. Existing
    /// metadata is compared immutably and read back before an object is removed.
    /// The legacy manifest is removed last, so another invocation can resume.
    /// Uses existing storage permissions; never relocates user payloads or
    /// authorizes work, retries, or a claim about current health.
    ///
    /// Active, corrupt and unrecognized runs remain unchanged. Artifacts above
    /// the existing 4 MiB output-chunk format are retained with a warning.
    /// Stores without metadata support return zero without touching objects.
    ///
    /// # Errors
    /// Returns storage or conflicting-byte failures. Every removed artifact
    /// already has its exact bytes in metadata, including after partial failure.
    pub fn relocate_finished_history(&self, deployment: Digest) -> Result<usize, ServerError> {
        self.check_housekeeping()?;
        if self.local_metadata.is_none() && self.postgres_metadata.is_none() {
            return Ok(0);
        }
        let prefix = ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}manifests/"))?;
        let mut budget = Budget::default();
        let result = self.objects.visit_prefix(&prefix, |manifest| {
            self.check_housekeeping()?;
            if budget.moved == 64 || budget.bytes == 16 * 1024 * 1024 {
                return Err(VisitError::Budget);
            }
            let id = manifest
                .key()
                .as_str()
                .strip_prefix(prefix.as_str())
                .ok_or(ServerError::InvalidPath)?;
            if id.is_empty()
                || id.len() > 128
                || !id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return Ok(());
            }
            let identity = self.read(&format!("manifests/{id}"), 65536);
            let identity = match identity {
                Ok(Some(bytes)) => serde_json::from_slice::<TaskRunIdentity>(&bytes).ok(),
                Ok(None)
                | Err(ServerError::RequestBodyTooLarge | ServerError::InvalidContentHash) => None,
                Err(error) => return Err(error.into()),
            };
            let Some(identity) = identity else {
                return Ok(());
            };
            if identity.schema != 1
                || identity.id != id
                || identity.scope.definition != identity.definition.fingerprint()
                || identity.definition.validate().is_err()
                || identity.definition.budget.max_history_records > 4096
            {
                return Ok(());
            }
            let relocatable = match self.relocatable(&identity, deployment) {
                Ok(relocatable) => relocatable,
                Err(ServerError::InvalidContentHash | ServerError::RequestBodyTooLarge) => {
                    return Ok(());
                }
                Err(ServerError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData => {
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            };
            if !relocatable {
                return Ok(());
            }
            let run_prefix = ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}runs/{id}/"))
                .map_err(ServerError::from)?;
            let mut retained = false;
            self.objects.visit_prefix(&run_prefix, |artifact| {
                retained |= !self.relocate_artifact(&artifact, &mut budget)?;
                Ok::<_, VisitError>(())
            })?;
            let binding = Self::key(&format!("scope-bindings/{id}"))?;
            if let Some(binding) = self.objects.metadata(&binding)? {
                retained |= !self.relocate_artifact(&binding, &mut budget)?;
            }
            if !retained {
                self.relocate_artifact(&manifest, &mut budget)?;
            }
            Ok(())
        });
        match result {
            Ok(()) | Err(VisitError::Budget) => Ok(budget.moved),
            Err(VisitError::Storage(error)) => Err(error),
        }
    }

    fn relocatable(
        &self,
        identity: &TaskRunIdentity,
        deployment: Digest,
    ) -> Result<bool, ServerError> {
        if !matches!(
            self.state(identity)?,
            (TaskRunState::Completed | TaskRunState::Failed, _)
        ) {
            return Ok(false);
        }
        let scope = self
            .postgres_metadata
            .as_ref()
            .map_or(identity.deployment, |metadata| metadata.scope);
        // Verify any association before moving its bytes. PostgreSQL must not
        // archive a run associated with another work database/schema.
        let binding = self.history_scope_binding_match(identity, scope)?;
        Ok((self.postgres_metadata.is_none() || binding != Some(false))
            && (identity.deployment == deployment
                || (self.postgres_metadata.is_some() && binding == Some(true))))
    }

    fn relocate_artifact(
        &self,
        artifact: &ObjectMetadata,
        budget: &mut Budget,
    ) -> Result<bool, VisitError> {
        if artifact.length() > 4 * 1024 * 1024 {
            tracing::warn!(
                key = artifact.key().as_str(),
                "legacy task artifact exceeds the existing output-chunk format; retaining it"
            );
            return Ok(false);
        }
        if budget.moved == 64 || budget.bytes.saturating_add(artifact.length()) > 16 * 1024 * 1024 {
            return Err(VisitError::Budget);
        }
        let suffix = artifact
            .key()
            .as_str()
            .strip_prefix(TASK_EVIDENCE_PREFIX)
            .ok_or(ServerError::InvalidPath)?;
        let Some(bytes) = self.read_objects(suffix, artifact.length())? else {
            return Ok(true);
        };
        let confirmed = if let Some(metadata) = &self.postgres_metadata {
            self.check_housekeeping()?;
            metadata.publish(artifact.key().as_str(), &bytes)?;
            self.check_housekeeping()?;
            metadata
                .read(artifact.key().as_str(), artifact.length())?
                .and_then(|saved| saved.bytes)
        } else if let Some(metadata) = &self.local_metadata {
            self.check_housekeeping()?;
            metadata
                .publish_task_artifact(artifact.key().as_str(), &bytes)
                .map_err(ServerError::from)?;
            self.check_housekeeping()?;
            metadata
                .read_task_artifact(artifact.key().as_str(), artifact.length())
                .map_err(ServerError::from)?
        } else {
            return Ok(false);
        };
        if confirmed.as_deref() != Some(bytes.as_slice())
            || self.read_objects(suffix, artifact.length())?.as_deref() != Some(bytes.as_slice())
        {
            return Err(ServerError::InvalidContentHash.into());
        }
        self.objects.delete_if_present(artifact.key())?;
        budget.moved = budget.moved.saturating_add(1);
        budget.bytes = budget.bytes.saturating_add(artifact.length());
        Ok(true)
    }
}

#[cfg(test)]
mod tests;
