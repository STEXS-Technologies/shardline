use super::{TaskEvidenceStore, TaskExecutionMode, TaskRunIdentity};
#[cfg(test)]
use crate::ServerObjectStore;
use ed25519_dalek::SigningKey;
use penelope_core::task::{
    BlobRef, Digest, EffectContract, ExecutionBudget, RecoveryMode, TaskDefinition,
    TaskExecutionEvent, TaskFault, TaskGeneration, TaskId, TaskInputDigest, TaskProjection,
    TaskRecoveryPolicy, TaskScope, TaskTenantId, TaskTick,
};
use shardline_reliability::{
    MaintenanceArtifact, MaintenanceArtifactKind, sign_maintenance_artifact,
};
use std::{io::Error, time::Instant};

/// In-process owner of background-job evidence. This observer never authorizes
/// automatic re-execution or claims OS fencing of work running in its process.
/// Isolated administrative work uses the protected process runtime instead.
#[derive(Debug, Clone)]
pub struct EmbeddedTaskTracker {
    store: TaskEvidenceStore,
    deployment: Digest,
    history_scope: Option<Digest>,
}

pub(crate) struct TaskReceipt {
    store: TaskEvidenceStore,
    identity: TaskRunIdentity,
    projection: TaskProjection,
    key: SigningKey,
    born: Instant,
    kind: MaintenanceArtifactKind,
}

type TrackingResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

impl EmbeddedTaskTracker {
    pub(crate) async fn for_work_pool(
        &self,
        config: &crate::ServerConfig,
        pool: &sqlx::PgPool,
    ) -> Result<Self, crate::ServerError> {
        let (mut store, history_scope) = TaskEvidenceStore::from_config(config)?
            .require_metadata_publication()
            .with_postgres_pool(config, pool, false)
            .await?;
        if store.postgres_metadata.is_some() {
            let publication = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                let mut connection = pool.acquire().await?;
                sqlx::query("SELECT shardline_task_artifact_access_v1(current_schema(), 'publish')")
                    .execute(&mut *connection)
                    .await
            })
            .await
            .map_err(|error| crate::ServerError::Io(std::io::Error::other(error)))?;
            if let Err(error) = publication {
                if error
                    .as_database_error()
                    .is_some_and(|database| database.code().as_deref() == Some("42501"))
                {
                    // Read access alone does not authorize metadata publication.
                    // Keep the sweep available without adding payload history.
                    store.postgres_metadata = None;
                } else {
                    return Err(shardline_index::PostgresMetadataStoreError::from(error).into());
                }
            }
        }
        Ok(Self {
            store,
            deployment: self.deployment,
            history_scope,
        })
    }

    #[cfg(test)]
    pub(crate) const fn new(objects: ServerObjectStore, deployment: Digest) -> Self {
        Self::from_store(TaskEvidenceStore::from_objects(objects), deployment)
    }

    pub(crate) const fn from_store(store: TaskEvidenceStore, deployment: Digest) -> Self {
        Self {
            store,
            deployment,
            history_scope: None,
        }
    }

    /// Associates new runs with a resolved history scope without changing their
    /// legacy identity. Resolve it on the intended metadata connection first.
    /// This association never grants execution ownership or authorizes retries.
    #[must_use]
    pub const fn with_history_scope(mut self, scope: Digest) -> Self {
        self.history_scope = Some(scope);
        self
    }

    pub(crate) const fn without_history_scope(mut self) -> Self {
        self.history_scope = None;
        self
    }

    pub(crate) async fn begin_async(
        &self,
        operation: &'static str,
        input: Vec<u8>,
        kind: MaintenanceArtifactKind,
    ) -> TrackingResult<TaskReceipt> {
        let tracker = self.clone();
        tokio::task::spawn_blocking(move || tracker.begin(operation, &input, kind)).await?
    }

    pub(crate) fn begin(
        &self,
        operation: &str,
        input: &[u8],
        kind: MaintenanceArtifactKind,
    ) -> TrackingResult<TaskReceipt> {
        let input = serde_json::to_vec(&(self.deployment, operation, input))?;
        let mut seed = zeroize::Zeroizing::new([0; 32]);
        getrandom::fill(seed.as_mut()).map_err(|error| Error::other(error.to_string()))?;
        let key = SigningKey::from_bytes(&seed);
        let id = hex::encode(key.verifying_key().as_bytes());
        let definition = TaskDefinition {
            code: Digest::of(
                format!(
                    "shardline.embedded-task.v1:{}:{operation}",
                    env!("CARGO_PKG_VERSION")
                )
                .as_bytes(),
            ),
            checkpoint_schema: 1,
            effects: EffectContract::ReconcileRequired,
            recovery: TaskRecoveryPolicy {
                mode: RecoveryMode::ReconcileBeforeRestart,
                max_attempts: 1,
                timeout_ticks: u64::MAX,
                restart_delay_ticks: 0,
                max_checkpoint_bytes: 65536,
            },
            budget: ExecutionBudget {
                max_input_bytes: 65536,
                max_output_bytes: 65536,
                max_history_records: 16,
                max_checkpoint_commits: 1,
                cpu_ticks: u64::MAX,
                deadline_tick: TaskTick::new(u64::MAX),
            },
        };
        let scope = TaskScope {
            tenant: TaskTenantId::from_digest(Digest::of(b"shardline.embedded-services.v1")),
            task: TaskId::from_digest(Digest::of(id.as_bytes())),
            definition: definition.fingerprint(),
            input: TaskInputDigest::from_digest(Digest::of(&input)),
        };
        let identity = TaskRunIdentity {
            schema: 1,
            id,
            operation: operation.to_owned(),
            created_at: shardline_protocol::unix_now_seconds_lossy(),
            scope,
            definition,
            public_key: *key.verifying_key().as_bytes(),
            deployment: self.deployment,
            execution: TaskExecutionMode::Embedded,
            owner: super::TaskOwner::current(),
        };
        self.store.publish(
            &format!("manifests/{}", identity.id),
            &serde_json::to_vec(&identity)?,
        )?;
        let mut receipt = TaskReceipt {
            store: self.store.clone(),
            identity,
            projection: TaskProjection::new(scope, definition)?,
            key,
            born: Instant::now(),
            kind,
        };
        if let Some(history_scope) = self.history_scope
            && let Err(error) = receipt.bind_history_scope(history_scope)
        {
            tracing::warn!(%error, "shared history binding unavailable; legacy tracking continues");
        }
        let input_ref = blob(&input)?;
        receipt.commit(
            TaskExecutionEvent::IntentCommitted {
                definition,
                input: input_ref,
            },
            TaskGeneration::ZERO,
            Some(&input),
        )?;
        receipt.commit(TaskExecutionEvent::Started, TaskGeneration::new(1), None)?;
        tracing::info!(operation, run = %receipt.identity.id, "background task tracking started");
        Ok(receipt)
    }
}

fn blob(bytes: &[u8]) -> TrackingResult<BlobRef> {
    if bytes.len() > 65536 {
        return Err("background task evidence exceeds 64 KiB".into());
    }
    Ok(BlobRef {
        digest: Digest::of(bytes),
        bytes: u32::try_from(bytes.len())?,
        schema: 1,
    })
}

impl TaskReceipt {
    pub(crate) fn detach_work_history(&mut self) -> TrackingResult<()> {
        self.store.detach_work_history()?;
        Ok(())
    }

    pub(crate) fn bind_history_scope(&self, scope: Digest) -> TrackingResult<()> {
        self.store
            .publish_history_scope_binding(&self.identity, &self.key, scope)?;
        Ok(())
    }
    pub(crate) async fn finish_async(self, success: bool, report: Vec<u8>) {
        match tokio::task::spawn_blocking(move || self.finish(success, &report)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracking_error(error.as_ref()),
            Err(error) => tracking_error(&error),
        }
    }
    fn commit(
        &mut self,
        event: TaskExecutionEvent,
        generation: TaskGeneration,
        bytes: Option<&[u8]>,
    ) -> TrackingResult<()> {
        let tick = TaskTick::new(u64::try_from(self.born.elapsed().as_millis())?.saturating_add(1));
        let record = self.projection.propose(event, generation, tick)?;
        let prefix = format!("runs/{}", self.identity.id);
        if let Some(bytes) = bytes {
            self.store.publish(
                &format!("{prefix}/blobs/{}", hex::encode(Digest::of(bytes).0)),
                bytes,
            )?;
        }
        self.store.publish(
            &format!("{prefix}/records/{:010}", record.sequence.get()),
            &serde_json::to_vec(&record)?,
        )?;
        self.projection.apply(&record)?;
        Ok(())
    }

    pub(crate) fn finish(mut self, success: bool, report: &[u8]) -> TrackingResult<()> {
        // A failed operation can already have changed data. Preserve its report
        // as a checkpoint, never as evidence that the operation completed safely.
        let kind = if success {
            self.kind
        } else {
            MaintenanceArtifactKind::Checkpoint
        };
        let proof = self.publish_report(report, kind)?;
        if !success {
            self.commit(
                TaskExecutionEvent::CheckpointCommitted {
                    checkpoint: blob(&proof)?,
                },
                TaskGeneration::new(1),
                Some(&proof),
            )?;
            self.commit(
                TaskExecutionEvent::FaultObserved {
                    fault: TaskFault::ApplicationFailure,
                },
                TaskGeneration::new(1),
                None,
            )?;
            self.commit(
                TaskExecutionEvent::ReconciliationRequired,
                TaskGeneration::new(1),
                None,
            )?;
            tracing::warn!(operation = %self.identity.operation, run = %self.identity.id, "background task needs reconciliation; no automatic retry authorized");
            return Ok(());
        }
        self.commit(
            TaskExecutionEvent::Completed {
                output: blob(&proof)?,
            },
            TaskGeneration::new(1),
            Some(&proof),
        )?;
        tracing::info!(operation = %self.identity.operation, run = %self.identity.id, "background task completion evidence committed");
        Ok(())
    }

    fn publish_report(
        &self,
        report: &[u8],
        kind: MaintenanceArtifactKind,
    ) -> TrackingResult<Vec<u8>> {
        let digest = Digest::of(report);
        let artifact = MaintenanceArtifact {
            schema_version: 1,
            scope: self.identity.scope,
            generation: TaskGeneration::new(1),
            step: 1,
            kind,
            digest,
            bytes: u64::try_from(report.len())?,
        };
        let proof = sign_maintenance_artifact(
            artifact,
            chrono::Utc::now(),
            &self.key,
            statechronicle_domain::intent::KeyId(format!(
                "shardline-maintenance:{}",
                hex::encode(self.identity.public_key)
            )),
        )?
        .to_json()?;
        self.store.publish(
            &format!(
                "runs/{}/artifacts/{}",
                self.identity.id,
                hex::encode(digest.0)
            ),
            report,
        )?;
        Ok(proof)
    }
}

impl Drop for TaskReceipt {
    fn drop(&mut self) {
        use penelope_core::task::TaskStatus;
        if !matches!(
            self.projection.status(),
            TaskStatus::Running | TaskStatus::Faulted
        ) {
            return;
        }
        let mut projection = self.projection;
        let store = self.store.clone();
        let id = self.identity.id.clone();
        let tick = TaskTick::new(
            u64::try_from(self.born.elapsed().as_millis())
                .unwrap_or(u64::MAX)
                .saturating_add(1),
        );
        let mut interrupted = move || -> TrackingResult<()> {
            if projection.status() == TaskStatus::Running {
                let record = projection.propose(
                    TaskExecutionEvent::FaultObserved {
                        fault: TaskFault::SupervisorInterrupted,
                    },
                    projection.generation(),
                    tick,
                )?;
                store.publish(
                    &format!("runs/{id}/records/{:010}", record.sequence.get()),
                    &serde_json::to_vec(&record)?,
                )?;
                projection.apply(&record)?;
            }
            let record = projection.propose(
                TaskExecutionEvent::ReconciliationRequired,
                projection.generation(),
                tick,
            )?;
            store.publish(
                &format!("runs/{id}/records/{:010}", record.sequence.get()),
                &serde_json::to_vec(&record)?,
            )?;
            tracing::warn!(run = %id, "background task interrupted; inspect effects before retry");
            Ok(())
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn_blocking(move || {
                if let Err(error) = interrupted() {
                    tracking_error(error.as_ref());
                }
            });
        } else if let Err(error) = interrupted() {
            tracking_error(error.as_ref());
        }
    }
}

pub(crate) fn tracking_error(error: &(dyn std::error::Error + Send + Sync)) {
    tracing::warn!(
        "optional background task tracking unavailable: {error}; serving continues, task completion is not certified"
    );
}
