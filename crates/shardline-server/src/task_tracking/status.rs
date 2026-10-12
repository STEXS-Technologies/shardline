use super::{TaskEvidenceStore, TaskExecutionMode, TaskRunIdentity};
use crate::ServerError;
use penelope_core::task::{
    BlobRef, Digest, TaskExecutionEvent, TaskProjection, TaskRecord, TaskStatus,
};
use serde::{Deserialize, Serialize};
use shardline_reliability::{MaintenanceArtifactProof, verify_maintenance_artifact};
use shardline_storage::ObjectPrefix;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Error, ErrorKind},
};

/// Operator-visible evidence state. Remote ownership is never inferred from a PID.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskRunState {
    /// No execution has been committed.
    Pending,
    /// The local owner is observable and still alive.
    Running,
    /// The owner is remote or cannot be observed; completion is unconfirmed.
    RunningOrInterrupted,
    /// Native proof and materialized result have been verified.
    Completed,
    /// Execution finished with a verified nonzero command outcome.
    Failed,
    /// Effects need inspection; no retry authorization is implied.
    NeedsReconciliation,
    /// Persisted identity, history or result failed verification.
    Corrupt,
}

/// A deployment task record suitable for an administrative status surface.
#[derive(Debug, Serialize)]
pub struct TaskRunSummary {
    /// Opaque incident reference allocated by Shardline.
    pub id: String,
    /// Human-readable operation.
    pub operation: String,
    /// What the available evidence establishes.
    pub state: TaskRunState,
    /// Whether work runs in an isolated worker.
    pub execution: TaskExecutionMode,
    /// Creation time; not a scheduling deadline.
    pub created_at: u64,
    /// Recorded owner, not a distributed lease.
    pub owner: super::TaskOwner,
    /// Full command output was checked. Large streams require explicit inspection;
    /// their signed outcome is still verified. None for jobs without command streams.
    pub output_verified: Option<bool>,
}

/// Admin inspection can expose a damaged manifest without inventing its owner,
/// operation or deployment. Library summaries retain their strict contract.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum TaskEvidenceEntry {
    Verified(TaskRunSummary),
    Unreadable {
        id: String,
        state: TaskRunState,
        identity_verified: bool,
    },
}

impl TaskEvidenceEntry {
    pub(crate) fn id(&self) -> &str {
        match self {
            Self::Verified(summary) => &summary.id,
            Self::Unreadable { id, .. } => id,
        }
    }
}

struct VerifiedTaskState {
    state: TaskRunState,
    output_verified: Option<bool>,
    report: Option<Vec<u8>>,
}

impl TaskEvidenceStore {
    /// Removes at most 64 verified finished histories older than 30 days.
    /// Unfinished, unobservable and corrupt histories are retained for inspection.
    /// This is evidence retention only; it never deletes deployment data.
    /// # Errors
    /// Returns storage failures; callers must preserve serving/maintenance availability.
    pub fn prune_completed(&self, deployment: Digest, now: u64) -> Result<usize, ServerError> {
        // A retirement marker authorizes only evidence cleanup. It is committed
        // after native completion verification, before hiding the manifest.
        // Keep it until all deletes succeed so interrupted cleanup is resumable.
        let retiring = self.retention_candidates("retiring", deployment, now, 64)?;
        let mut removed = 0_usize;
        for identity in retiring.into_values() {
            self.finish_retirement(&identity)?;
            removed = removed.saturating_add(1);
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
                self.finish_retirement(&identity)?;
                removed = removed.saturating_add(1);
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
        let mut candidates = BTreeMap::new();
        self.visit_prefix(&prefix, |metadata| {
            let id = metadata
                .key()
                .as_str()
                .rsplit('/')
                .next()
                .ok_or(ServerError::InvalidPath)?;
            let bytes = match self.read(&format!("{category}/{id}"), 65536) {
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
                && identity.deployment == deployment
                && identity.created_at.saturating_add(30 * 24 * 60 * 60) < now
            {
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

    pub(super) fn begin_retirement(&self, identity: &TaskRunIdentity) -> Result<bool, ServerError> {
        if !matches!(
            self.state(identity),
            Ok((TaskRunState::Completed | TaskRunState::Failed, _))
        ) {
            return Ok(false);
        }
        self.publish(
            &format!("retiring/{}", identity.id),
            &serde_json::to_vec(identity)?,
        )?;
        Ok(true)
    }

    fn finish_retirement(&self, identity: &TaskRunIdentity) -> Result<(), ServerError> {
        // Hide the expired history first. Status must not advertise a partly
        // removed native proof; the cleanup marker remains outside status.
        self.delete_artifact(&Self::key(&format!("manifests/{}", identity.id))?)?;
        self.delete_artifact(&Self::key(&format!("scope-bindings/{}", identity.id))?)?;
        let run_prefix = ObjectPrefix::parse(&format!(
            "{}runs/{}/",
            super::TASK_EVIDENCE_PREFIX,
            identity.id
        ))?;
        self.visit_prefix(&run_prefix, |metadata| {
            self.delete_artifact(metadata.key())?;
            Ok::<(), ServerError>(())
        })?;
        self.delete_artifact(&Self::key(&format!("retiring/{}", identity.id))?)?;
        Ok(())
    }

    /// Lists a bounded page of verified summaries. The store inventory walk is
    /// streaming but may be O(N); hosts must admission-gate this synchronous call.
    /// # Errors
    /// Returns storage failures or invalid page bounds.
    pub fn summaries(
        &self,
        deployment: Digest,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TaskRunSummary>, ServerError> {
        self.summaries_with_scope(deployment, None, after, limit)
    }

    /// Lists verified history using the scope resolved by [`Self::from_backend`].
    /// A signed matching association permits history across database logins;
    /// unbound legacy records still require their original deployment identity.
    /// A scope is a history filter, not execution authority or a health claim.
    /// Run on a blocking thread and admission-gate inspection. Inventory can be O(N).
    ///
    /// # Errors
    /// Returns storage failures, invalid page bounds or a corrupt task identity.
    pub fn summaries_with_scope(
        &self,
        deployment: Digest,
        history_scope: Option<Digest>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TaskRunSummary>, ServerError> {
        self.inspect_summaries_with_scope(deployment, history_scope, after, limit)?
            .into_iter()
            .map(|entry| match entry {
                TaskEvidenceEntry::Verified(summary) => Ok(summary),
                TaskEvidenceEntry::Unreadable { .. } => Err(invalid("invalid task identity")),
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn inspect_summaries(
        &self,
        deployment: Digest,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TaskEvidenceEntry>, ServerError> {
        self.inspect_summaries_with_scope(deployment, None, after, limit)
    }

    pub(crate) fn inspect_summaries_with_scope(
        &self,
        deployment: Digest,
        history_scope: Option<Digest>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TaskEvidenceEntry>, ServerError> {
        if limit == 0 || limit > 101 || after.is_some_and(|id| id.len() > 128) {
            return Err(ServerError::InvalidPath);
        }
        let prefix = ObjectPrefix::parse(&format!("{}manifests/", super::TASK_EVIDENCE_PREFIX))?;
        let mut identities = BTreeMap::new();
        self.visit_prefix(&prefix, |metadata| {
            let id = metadata
                .key()
                .as_str()
                .rsplit('/')
                .next()
                .ok_or(ServerError::InvalidPath)?;
            if after.is_some_and(|after| id <= after) {
                return Ok::<(), ServerError>(());
            }
            if id.is_empty() || id.len() > 128 || id.chars().any(char::is_control) {
                return Err(invalid("invalid task identity key"));
            }
            let mut identity = match self.read(&format!("manifests/{id}"), 65536) {
                // Retention can remove an expired manifest during enumeration.
                Ok(None) => return Ok(()),
                Ok(Some(bytes)) => serde_json::from_slice::<TaskRunIdentity>(&bytes).ok(),
                Err(ServerError::RequestBodyTooLarge | ServerError::InvalidContentHash) => None,
                Err(error) => return Err(error),
            };
            if let Some(candidate) = identity.as_ref() {
                let legacy_matches = candidate.deployment == deployment;
                // Verify before admitting a foreign login's identity. Corrupt
                // associations expose only an incident reference, never its
                // untrusted operation, owner or completion claim.
                let association = history_scope
                    .map(|scope| self.history_scope_binding_match(candidate, scope))
                    .transpose()
                    .map(Option::flatten);
                match association {
                    Ok(Some(true)) => {}
                    Ok(Some(false)) => return Ok(()),
                    Ok(None) if !legacy_matches => return Ok(()),
                    Ok(None) => {}
                    Err(ServerError::RequestBodyTooLarge | ServerError::InvalidContentHash) => {
                        identity = None;
                    }
                    Err(ServerError::Io(error)) if error.kind() == ErrorKind::InvalidData => {
                        identity = None;
                    }
                    Err(error) => return Err(error),
                }
            }
            let identity = identity.filter(|identity| {
                identity.schema == 1
                    && identity.id == id
                    && identity.scope.definition == identity.definition.fingerprint()
                    && identity.definition.budget.max_history_records <= 4096
                    && identity.definition.validate().is_ok()
            });
            identities.insert(id.to_owned(), identity);
            if identities.len() > limit {
                identities.pop_last();
            }
            Ok(())
        })?;
        identities
            .into_iter()
            .map(|(id, identity)| {
                let Some(identity) = identity else {
                    return Ok(TaskEvidenceEntry::Unreadable {
                        id,
                        state: TaskRunState::Corrupt,
                        identity_verified: false,
                    });
                };
                let (state, output_verified) = match self.state(&identity) {
                    Ok(result) => result,
                    Err(ServerError::RequestBodyTooLarge | ServerError::InvalidContentHash) => {
                        (TaskRunState::Corrupt, None)
                    }
                    Err(ServerError::Io(error)) if error.kind() == ErrorKind::InvalidData => {
                        (TaskRunState::Corrupt, None)
                    }
                    Err(error) => return Err(error),
                };
                Ok(TaskEvidenceEntry::Verified(TaskRunSummary {
                    id: identity.id,
                    operation: identity.operation,
                    state,
                    execution: identity.execution,
                    created_at: identity.created_at,
                    owner: identity.owner,
                    output_verified,
                }))
            })
            .collect()
    }

    fn state(
        &self,
        identity: &TaskRunIdentity,
    ) -> Result<(TaskRunState, Option<bool>), ServerError> {
        self.state_with_report(identity)
            .map(|verified| (verified.state, verified.output_verified))
    }

    /// Reads one embedded report without a deployment-wide inventory walk.
    /// Association and every referenced artifact are verified before disclosure.
    pub(crate) fn embedded_report(
        &self,
        deployment: Digest,
        history_scope: Option<Digest>,
        id: &str,
    ) -> Result<Option<serde_json::Value>, ServerError> {
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(ServerError::InvalidAdminQuery);
        }
        let Some(bytes) = self.read(&format!("manifests/{id}"), 65536)? else {
            return Ok(None);
        };
        let identity: TaskRunIdentity =
            serde_json::from_slice(&bytes).map_err(|_error| invalid("invalid task identity"))?;
        if identity.schema != 1
            || identity.id != id
            || identity.scope.definition != identity.definition.fingerprint()
            || identity.definition.budget.max_history_records > 4096
            || identity.definition.validate().is_err()
        {
            return Err(invalid("invalid task identity"));
        }
        let association = history_scope
            .map(|scope| self.history_scope_binding_match(&identity, scope))
            .transpose()?
            .flatten();
        if association == Some(false)
            || (association.is_none() && identity.deployment != deployment)
            || identity.execution != TaskExecutionMode::Embedded
        {
            return Ok(None);
        }
        let verified = self.state_with_report(&identity)?;
        let report = verified
            .report
            .map(String::from_utf8)
            .transpose()
            .map_err(|_error| invalid("embedded report is not text"))?;
        Ok(Some(serde_json::json!({
            "id": identity.id,
            "operation": identity.operation,
            "state": verified.state,
            "report": report,
            "report_available": report.is_some(),
        })))
    }

    fn state_with_report(
        &self,
        identity: &TaskRunIdentity,
    ) -> Result<VerifiedTaskState, ServerError> {
        let mut command_success = true;
        let mut output_verified = None;
        let mut report = None;
        let mut projection = TaskProjection::new(identity.scope, identity.definition)
            .map_err(|_error| invalid("invalid task contract"))?;
        let prefix = format!("runs/{}/records/", identity.id);
        let mut sequences = BTreeSet::new();
        self.visit_prefix(
            &ObjectPrefix::parse(&format!("{}{prefix}", super::TASK_EVIDENCE_PREFIX))?,
            |metadata| {
                let sequence = metadata
                    .key()
                    .as_str()
                    .rsplit('/')
                    .next()
                    .and_then(|value| value.parse::<u32>().ok())
                    .ok_or_else(|| invalid("invalid task record key"))?;
                if sequence >= identity.definition.budget.max_history_records {
                    return Err(invalid("task history quota violated"));
                }
                sequences.insert(sequence);
                Ok::<(), ServerError>(())
            },
        )?;
        for sequence in sequences {
            if sequence != projection.next_sequence().get() {
                return Err(invalid("task history contains a gap"));
            }
            let bytes = self
                .read(&format!("{prefix}{sequence:010}"), 16384)?
                .ok_or_else(|| invalid("missing task record"))?;
            let record: TaskRecord =
                serde_json::from_slice(&bytes).map_err(|_error| invalid("invalid task record"))?;
            projection
                .apply(&record)
                .map_err(|_error| invalid("task history failed semantic verification"))?;
            let reference = match record.event {
                TaskExecutionEvent::IntentCommitted { input, .. } => Some(input),
                TaskExecutionEvent::CheckpointCommitted { checkpoint }
                | TaskExecutionEvent::CheckpointMigrated { checkpoint, .. } => Some(checkpoint),
                TaskExecutionEvent::Completed { output }
                | TaskExecutionEvent::Reconciled {
                    result: penelope_core::task::Reconciliation::Completed { output, .. },
                } => Some(output),
                TaskExecutionEvent::Started
                | TaskExecutionEvent::FaultObserved { .. }
                | TaskExecutionEvent::ReconciliationRequired
                | TaskExecutionEvent::Reconciled { .. }
                | TaskExecutionEvent::RecoveryPlanned { .. }
                | TaskExecutionEvent::Restarted
                | TaskExecutionEvent::RecoveryExhausted => None,
            };
            if let Some(reference) = reference {
                let body = self.blob(identity, reference)?;
                if !matches!(record.event, TaskExecutionEvent::IntentCommitted { .. }) {
                    let proof = MaintenanceArtifactProof::from_json(&body)
                        .map_err(|_error| invalid("invalid native task proof"))?;
                    let key = ed25519_dalek::VerifyingKey::from_bytes(&identity.public_key)
                        .map_err(|_error| invalid("invalid task verification key"))?;
                    let artifact = verify_maintenance_artifact(
                        &proof,
                        identity.scope,
                        record.generation,
                        &key,
                        &statechronicle_domain::intent::KeyId(format!(
                            "shardline-maintenance:{}",
                            hex::encode(identity.public_key)
                        )),
                    )
                    .map_err(|_error| invalid("native task proof failed verification"))?;
                    let checkpoint = matches!(
                        record.event,
                        TaskExecutionEvent::CheckpointCommitted { .. }
                            | TaskExecutionEvent::CheckpointMigrated { .. }
                    );
                    if checkpoint
                        != (artifact.kind
                            == shardline_reliability::MaintenanceArtifactKind::Checkpoint)
                    {
                        return Err(invalid(
                            "native artifact role does not match its history record",
                        ));
                    }
                    let materialized = self
                        .read(
                            &format!(
                                "runs/{}/artifacts/{}",
                                identity.id,
                                hex::encode(artifact.digest.0)
                            ),
                            65536,
                        )?
                        .ok_or_else(|| invalid("missing materialized task report"))?;
                    artifact
                        .verify_materialized(materialized.as_slice())
                        .map_err(|_error| invalid("task report failed verification"))?;
                    if identity.execution == TaskExecutionMode::Isolated
                        && matches!(
                            record.event,
                            TaskExecutionEvent::Completed { .. }
                                | TaskExecutionEvent::Reconciled {
                                    result: penelope_core::task::Reconciliation::Completed { .. }
                                }
                        )
                    {
                        let (success, verified) = self.verify_streams(identity, &materialized)?;
                        command_success = success;
                        output_verified = Some(verified);
                    }
                    if identity.execution == TaskExecutionMode::Inline
                        && matches!(
                            record.event,
                            TaskExecutionEvent::Completed { .. }
                                | TaskExecutionEvent::Reconciled {
                                    result: penelope_core::task::Reconciliation::Completed { .. }
                                }
                        )
                    {
                        command_success = inline_outcome(&materialized)? == 0;
                    }
                    if identity.execution == TaskExecutionMode::Embedded {
                        report = Some(materialized);
                    }
                }
            }
        }
        Ok(VerifiedTaskState {
            state: match projection.status() {
                TaskStatus::Completed => {
                    if command_success {
                        TaskRunState::Completed
                    } else {
                        TaskRunState::Failed
                    }
                }
                TaskStatus::Empty | TaskStatus::Pending | TaskStatus::Ready => {
                    TaskRunState::Pending
                }
                TaskStatus::Running => match identity.owner.alive_here() {
                    Some(true) => TaskRunState::Running,
                    Some(false) => TaskRunState::NeedsReconciliation,
                    None => TaskRunState::RunningOrInterrupted,
                },
                TaskStatus::Faulted
                | TaskStatus::Reconciling
                | TaskStatus::Reconciled
                | TaskStatus::Exhausted => TaskRunState::NeedsReconciliation,
            },
            output_verified,
            report,
        })
    }

    fn blob(&self, identity: &TaskRunIdentity, reference: BlobRef) -> Result<Vec<u8>, ServerError> {
        if reference.bytes > 65536 || reference.schema != 1 {
            return Err(invalid("task blob quota violated"));
        }
        let body = self
            .read(
                &format!(
                    "runs/{}/blobs/{}",
                    identity.id,
                    hex::encode(reference.digest.0)
                ),
                65536,
            )?
            .ok_or_else(|| invalid("missing task blob"))?;
        reference
            .verify(&body, reference.schema)
            .map_err(|_error| invalid("task blob failed verification"))?;
        Ok(body)
    }

    fn verify_streams(
        &self,
        identity: &TaskRunIdentity,
        materialized: &[u8],
    ) -> Result<(bool, bool), ServerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Manifest {
            schema: u32,
            exit_code: u8,
            stdout: Stream,
            stderr: Stream,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Stream {
            digest: Digest,
            bytes: u64,
        }
        use sha2::{Digest as _, Sha256};
        let split = materialized
            .iter()
            .position(|byte| *byte == b'\n')
            .ok_or_else(|| invalid("invalid command outcome"))?;
        let body = materialized
            .get(split.saturating_add(1)..)
            .ok_or_else(|| invalid("invalid command outcome"))?;
        let manifest: Manifest =
            serde_json::from_slice(body).map_err(|_error| invalid("invalid command outcome"))?;
        if manifest.schema != 1 || manifest.exit_code > 2 {
            return Err(invalid("invalid command outcome"));
        }
        if materialized.get(..split)
            != Some(format!("maintenance.success: {}", manifest.exit_code == 0).as_bytes())
        {
            return Err(invalid(
                "command outcome header does not match its signed manifest",
            ));
        }
        // Status is a bounded inspection, not an unbounded report download.
        // The signed outcome must still be consistent when streams are too
        // large to inspect here.
        if manifest.stdout.bytes.saturating_add(manifest.stderr.bytes) > 8 * 1024 * 1024 {
            return Ok((manifest.exit_code == 0, false));
        }
        for (name, stream) in [("stdout", manifest.stdout), ("stderr", manifest.stderr)] {
            let mut hash = Sha256::new();
            let mut remaining = stream.bytes;
            let mut index = 0_u64;
            while remaining > 0 {
                let expected = remaining.min(4 * 1024 * 1024);
                let bytes = self
                    .read(
                        &format!("runs/{}/streams/{name}/{index:010}", identity.id),
                        expected,
                    )?
                    .ok_or_else(|| invalid("missing command output chunk"))?;
                if u64::try_from(bytes.len())? != expected {
                    return Err(invalid("invalid command output chunk length"));
                }
                hash.update(&bytes);
                remaining = remaining
                    .checked_sub(expected)
                    .ok_or_else(|| invalid("invalid command output length"))?;
                index = index
                    .checked_add(1)
                    .ok_or_else(|| invalid("invalid command output sequence"))?;
            }
            if Digest(hash.finalize().into()) != stream.digest {
                return Err(invalid("command output failed verification"));
            }
        }
        Ok((manifest.exit_code == 0, true))
    }
}

fn invalid(message: &'static str) -> ServerError {
    ServerError::Io(Error::new(ErrorKind::InvalidData, message))
}

fn inline_outcome(materialized: &[u8]) -> Result<u8, ServerError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Outcome {
        schema: u32,
        exit_code: u8,
    }
    let split = materialized
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or_else(|| invalid("invalid command outcome"))?;
    let body = materialized
        .get(split.saturating_add(1)..)
        .ok_or_else(|| invalid("invalid command outcome"))?;
    let outcome: Outcome =
        serde_json::from_slice(body).map_err(|_error| invalid("invalid command outcome"))?;
    if outcome.schema != 1
        || outcome.exit_code > 2
        || materialized.get(..split)
            != Some(format!("maintenance.success: {}", outcome.exit_code == 0).as_bytes())
    {
        return Err(invalid("invalid signed command outcome"));
    }
    Ok(outcome.exit_code)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{ServerConfig, task_tracking::EmbeddedTaskTracker};
    use std::num::NonZeroUsize;

    #[test]
    fn large_output_still_requires_a_consistent_signed_outcome() {
        let root = tempfile::tempdir().unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:8080".parse().unwrap(),
            "http://localhost:8080".to_owned(),
            root.path().to_path_buf(),
            NonZeroUsize::new(4096).unwrap(),
        );
        let deployment = super::super::deployment_fingerprint(&config).unwrap();
        let store = TaskEvidenceStore::from_config(&config).unwrap();
        let tracker = EmbeddedTaskTracker::new(
            super::super::object_store_from_config(&config).unwrap(),
            deployment,
        );
        let receipt = tracker
            .begin(
                "large-output",
                b"input",
                shardline_reliability::MaintenanceArtifactKind::Fsck,
            )
            .unwrap();
        let id = store.summaries(deployment, None, 1).unwrap()[0].id.clone();
        let identity: TaskRunIdentity = serde_json::from_slice(
            &store
                .read(&format!("manifests/{id}"), 65536)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        // No stream objects exist: large output inspection must remain bounded,
        // while contradictory outcome metadata must fail before that shortcut.
        for exit_code in [0_u8, 1, 2] {
            let manifest = serde_json::json!({
                "schema": 1,
                "exit_code": exit_code,
                "stdout": {"digest": Digest::of(b"stdout"), "bytes": 8 * 1024 * 1024 + 1},
                "stderr": {"digest": Digest::of(b"stderr"), "bytes": 0},
            });
            let report = format!("maintenance.success: {}\n{manifest}", exit_code == 0);
            assert_eq!(
                store.verify_streams(&identity, report.as_bytes()).unwrap(),
                (exit_code == 0, false)
            );
            let contradictory = format!("maintenance.success: {}\n{manifest}", exit_code != 0);
            assert!(matches!(
                store.verify_streams(&identity, contradictory.as_bytes()),
                Err(ServerError::Io(error)) if error.kind() == ErrorKind::InvalidData
            ));
        }
        drop(receipt);
    }
}
