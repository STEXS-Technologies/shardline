//! Verified finished-history retention, independent of execution authority.

use super::{TaskEvidenceStore, TaskRunIdentity, TaskRunState};
use crate::ServerError;
use penelope_core::task::Digest;
use shardline_storage::{ObjectPrefix, ObjectStore};
use std::{
    collections::BTreeMap,
    io::{Error, ErrorKind},
};

impl TaskEvidenceStore {
    /// Removes at most 64 verified finished histories older than 30 days.
    /// Unfinished, unobservable and corrupt histories are retained for inspection.
    /// This is evidence retention only; it never deletes deployment data.
    /// Verified Postgres scope bindings allow metadata-only history from earlier
    /// configurations to retire. Legacy object copies keep their original
    /// deployment restriction.
    /// # Errors
    /// Returns storage failures; callers must preserve serving/maintenance availability.
    pub fn prune_completed(&self, deployment: Digest, now: u64) -> Result<usize, ServerError> {
        self.prune_completed_with_artifact_budget(deployment, now, usize::MAX)
    }

    /// Retires finished history with at most 256 run-artifact keys per call.
    /// A key can have both metadata and legacy object copies to remove.
    /// Unfinished retirement keeps its authorization marker for the next call.
    /// Manifest, scope-binding and retirement-marker cleanup are separate from
    /// this artifact budget. Inventory and active storage calls are not timed.
    ///
    /// # Errors
    /// Returns storage failures without authorizing application work to repeat.
    pub fn prune_completed_incrementally(
        &self,
        deployment: Digest,
        now: u64,
    ) -> Result<usize, ServerError> {
        self.prune_completed_with_artifact_budget(deployment, now, 256)
    }

    fn prune_completed_with_artifact_budget(
        &self,
        deployment: Digest,
        now: u64,
        mut artifacts_remaining: usize,
    ) -> Result<usize, ServerError> {
        self.check_housekeeping()?;
        // A retirement marker authorizes only evidence cleanup. It is committed
        // after native completion verification, before hiding the manifest.
        // Keep it until all deletes succeed so interrupted cleanup is resumable.
        let retiring = self.retention_candidates("retiring", deployment, now, 64)?;
        let mut removed = 0_usize;
        for identity in retiring.into_values() {
            if !self.finish_retirement(&identity, &mut artifacts_remaining)? {
                return Ok(removed);
            }
            removed = removed.saturating_add(1);
            if artifacts_remaining == 0 {
                return Ok(removed);
            }
        }
        if removed == 64 {
            return Ok(removed);
        }
        let candidates = self.retention_candidates(
            "manifests",
            deployment,
            now,
            64_usize.saturating_sub(removed),
        )?;
        for identity in candidates.into_values() {
            if self.begin_retirement(&identity)? {
                if !self.finish_retirement(&identity, &mut artifacts_remaining)? {
                    return Ok(removed);
                }
                removed = removed.saturating_add(1);
                if artifacts_remaining == 0 {
                    return Ok(removed);
                }
            }
        }
        Ok(removed)
    }

    fn retention_candidates(
        &self,
        category: &str,
        deployment: Digest,
        now: u64,
        limit: usize,
    ) -> Result<BTreeMap<(u64, String), TaskRunIdentity>, ServerError> {
        let prefix = ObjectPrefix::parse(&format!("{}{category}/", super::TASK_EVIDENCE_PREFIX))?;
        let mut candidates: BTreeMap<(u64, String), TaskRunIdentity> = BTreeMap::new();
        let mut reader = super::evidence_reader::EvidencePageReader::new(self, &prefix, None);
        self.visit_prefix(&prefix, |metadata| {
            let id = metadata
                .key()
                .as_str()
                .rsplit('/')
                .next()
                .ok_or(ServerError::InvalidPath)?;
            let suffix = format!("{category}/{id}");
            let read = reader.read(metadata.key().as_str(), &suffix);
            let bytes = match read {
                Ok(Some(bytes)) => bytes,
                Ok(None) => return Ok::<(), ServerError>(()),
                Err(ServerError::RequestBodyTooLarge | ServerError::InvalidContentHash) => {
                    tracing::warn!(
                        run = id,
                        "invalid task retention identity; retaining evidence"
                    );
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            let Ok(identity) = serde_json::from_slice::<TaskRunIdentity>(&bytes) else {
                tracing::warn!(
                    run = id,
                    "invalid task retention identity; retaining evidence"
                );
                return Ok(());
            };
            if identity.schema == 1
                && identity.id == id
                && !id.is_empty()
                && id.len() <= 128
                && identity.scope.definition == identity.definition.fingerprint()
                && identity.definition.validate().is_ok()
                && identity.definition.budget.max_history_records <= 4096
                && identity.created_at.saturating_add(30 * 24 * 60 * 60) < now
            {
                // Only identities older than the newest selected candidate can
                // enter this bounded page. Avoid verifying an entire finished
                // backlog after the page is full. Earlier unfinished runs still
                // get checked and cannot prevent later finished runs entering.
                if candidates.len() == limit
                    && candidates
                        .last_key_value()
                        .is_some_and(|((created_at, selected_id), _)| {
                            (identity.created_at, identity.id.as_str())
                                > (*created_at, selected_id.as_str())
                        })
                {
                    return Ok(());
                }
                if !self.retention_scope_matches(&identity, &bytes, deployment)? {
                    return Ok(());
                }
                // Retained corrupt/unfinished runs must not monopolize the
                // bounded candidate page and prevent all later cleanup.
                if category == "manifests"
                    && !matches!(
                        self.state(&identity),
                        Ok((TaskRunState::Completed | TaskRunState::Failed, _))
                    )
                {
                    return Ok(());
                }
                candidates.insert((identity.created_at, identity.id.clone()), identity);
                if candidates.len() > limit {
                    candidates.pop_last();
                }
            }
            Ok(())
        })?;
        Ok(candidates)
    }

    fn retention_scope_matches(
        &self,
        identity: &TaskRunIdentity,
        manifest: &[u8],
        deployment: Digest,
    ) -> Result<bool, ServerError> {
        let legacy_matches = identity.deployment == deployment;
        let Some(postgres) = &self.postgres_metadata else {
            return Ok(legacy_matches);
        };
        match self.history_scope_binding_match_for_manifest(identity, postgres.scope, manifest) {
            Ok(Some(false)) => return Ok(false),
            Ok(None) => return Ok(legacy_matches),
            Ok(Some(true)) if legacy_matches => return Ok(true),
            Ok(Some(true)) => {}
            Err(ServerError::RequestBodyTooLarge | ServerError::InvalidContentHash) => {
                return Ok(false);
            }
            Err(ServerError::Io(error)) if error.kind() == ErrorKind::InvalidData => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        }
        // Cross-login cleanup is limited to metadata-only evidence. Legacy
        // object copies cannot atomically retire their authorization marker.
        for category in ["manifests", "retiring", "scope-bindings"] {
            self.check_housekeeping()?;
            if self
                .objects
                .metadata(&Self::key(&format!("{category}/{}", identity.id))?)?
                .is_some()
            {
                return Ok(false);
            }
        }
        let prefix = ObjectPrefix::parse(&format!(
            "{}runs/{}/",
            super::TASK_EVIDENCE_PREFIX,
            identity.id
        ))?;
        let mut legacy_artifacts = false;
        self.objects.visit_prefix(&prefix, |_| {
            self.check_housekeeping()?;
            legacy_artifacts = true;
            Ok::<(), ServerError>(())
        })?;
        Ok(!legacy_artifacts)
    }

    pub(super) fn begin_retirement(&self, identity: &TaskRunIdentity) -> Result<bool, ServerError> {
        if !matches!(
            self.state(identity),
            Ok((TaskRunState::Completed | TaskRunState::Failed, _))
        ) {
            return Ok(false);
        }
        let Some(manifest) = self.read(&format!("manifests/{}", identity.id), 65536)? else {
            return Ok(false);
        };
        let stored: TaskRunIdentity = serde_json::from_slice(&manifest)
            .map_err(|_error| invalid("invalid retirement identity"))?;
        if serde_json::to_vec(&stored)? != serde_json::to_vec(identity)? {
            return Err(invalid("retirement identity changed"));
        }
        self.publish(&format!("retiring/{}", identity.id), &manifest)?;
        Ok(true)
    }

    fn finish_retirement(
        &self,
        identity: &TaskRunIdentity,
        artifacts_remaining: &mut usize,
    ) -> Result<bool, ServerError> {
        // Hide the expired history first. Status must not advertise a partly
        // removed native proof; the cleanup marker remains outside status.
        self.delete_artifact(&Self::key(&format!("manifests/{}", identity.id))?)?;
        let run_prefix = ObjectPrefix::parse(&format!(
            "{}runs/{}/",
            super::TASK_EVIDENCE_PREFIX,
            identity.id
        ))?;
        let mut deferred = false;
        if self.postgres_metadata.is_some() || self.local_metadata.is_some() {
            let delete_metadata_batch = |keys: &[String]| {
                self.check_housekeeping()?;
                if let Some(postgres) = &self.postgres_metadata {
                    postgres.delete_batch(keys)?;
                }
                if let Some(local) = &self.local_metadata {
                    local.delete_task_artifacts(keys)?;
                }
                Ok::<(), ServerError>(())
            };
            let mut keys = Vec::with_capacity(64);
            self.visit_retirement_prefix_until(&run_prefix, |metadata| {
                if *artifacts_remaining == 0 {
                    deferred = true;
                    return Ok(std::ops::ControlFlow::Break(()));
                }
                // Remove legacy copies before committing metadata deletion.
                // On any failure the retirement marker remains resumable.
                self.objects.delete_if_present(metadata.key())?;
                keys.push(metadata.key().as_str().to_owned());
                *artifacts_remaining = artifacts_remaining.saturating_sub(1);
                if keys.len() == 64 {
                    delete_metadata_batch(&keys)?;
                    keys.clear();
                }
                Ok::<_, ServerError>(std::ops::ControlFlow::Continue(()))
            })?;
            delete_metadata_batch(&keys)?;
        } else {
            self.visit_retirement_prefix_until(&run_prefix, |metadata| {
                if *artifacts_remaining == 0 {
                    deferred = true;
                    return Ok(std::ops::ControlFlow::Break(()));
                }
                self.delete_artifact(metadata.key())?;
                *artifacts_remaining = artifacts_remaining.saturating_sub(1);
                Ok::<_, ServerError>(std::ops::ControlFlow::Continue(()))
            })?;
        }
        if deferred {
            return Ok(false);
        }
        if let Some(postgres) = &self.postgres_metadata {
            // Keep the signed binding until the marker disappears atomically;
            self.check_housekeeping()?;
            // a replacement login can still authorize interrupted cleanup.
            postgres.finish_retirement(&identity.id)?;
            for category in ["scope-bindings", "retiring"] {
                self.objects
                    .delete_if_present(&Self::key(&format!("{category}/{}", identity.id))?)?;
            }
        } else {
            self.delete_artifact(&Self::key(&format!("scope-bindings/{}", identity.id))?)?;
            self.delete_artifact(&Self::key(&format!("retiring/{}", identity.id))?)?;
        }
        Ok(true)
    }
}

fn invalid(message: &'static str) -> ServerError {
    ServerError::Io(Error::new(ErrorKind::InvalidData, message))
}
