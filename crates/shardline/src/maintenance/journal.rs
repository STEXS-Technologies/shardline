use super::{Result, artifact, capture};
use penelope_core::task::{
    BlobRef, TaskDefinition, TaskExecutionEvent, TaskRecord, TaskScope, TaskSequence,
    runtime::{TaskJournal, TaskPortError},
};
use shardline_reliability::{MaintenanceArtifactProof, SqliteTaskJournal};
use shardline_server::task_tracking::{TaskEvidenceStore, TaskRunIdentity};
use std::{
    io::Read,
    path::{Path, PathBuf},
};

/// Local durable arbitration with a best-effort deployment evidence export.
/// Export failure never weakens local commit-before-dispatch/acknowledgement.
pub(super) struct PublishedJournal {
    local: SqliteTaskJournal,
    publication: Option<Publication>,
    publication_confirmed: bool,
}

struct Publication {
    store: TaskEvidenceStore,
    id: String,
    directory: PathBuf,
    capture: bool,
}

impl PublishedJournal {
    pub fn open(
        path: &Path,
        scope: TaskScope,
        definition: TaskDefinition,
        identity: Option<(TaskEvidenceStore, TaskRunIdentity)>,
    ) -> Result<Self> {
        let local = SqliteTaskJournal::open(path, scope, definition)?;
        let mut publication_confirmed = true;
        let publication = if let Some((store, identity)) = identity {
            let id = identity.id.clone();
            let bytes = serde_json::to_vec(&identity)?;
            crate::write_output_bytes(&path.with_file_name("identity.json"), &bytes, false)?;
            if let Err(error) = store.publish(&format!("manifests/{id}"), &bytes) {
                publication_confirmed = false;
                tracing::warn!(run = %id, workspace = %path.display(), "shared task evidence unavailable; retaining local evidence: {error}");
            }
            Some(Publication {
                capture: identity.execution
                    == shardline_server::task_tracking::TaskExecutionMode::Isolated,
                store,
                id,
                directory: path.parent().ok_or("missing task workspace")?.to_path_buf(),
            })
        } else {
            None
        };
        Ok(Self {
            local,
            publication,
            publication_confirmed,
        })
    }

    pub const fn publication_confirmed(&self) -> bool {
        self.publication_confirmed
    }
}

impl Publication {
    fn publish(&self, record: &TaskRecord, blob: Option<&[u8]>) -> Result<()> {
        let prefix = format!("runs/{}", self.id);
        if let Some(bytes) = blob {
            let digest = penelope_core::task::Digest::of(bytes);
            self.store
                .publish(&format!("{prefix}/blobs/{}", hex::encode(digest.0)), bytes)?;
            if matches!(
                record.event,
                TaskExecutionEvent::CheckpointCommitted { .. }
                    | TaskExecutionEvent::Completed { .. }
                    | TaskExecutionEvent::Reconciled {
                        result: penelope_core::task::Reconciliation::Completed { .. }
                    }
            ) {
                let proof = MaintenanceArtifactProof::from_json(bytes)?;
                let body = artifact::read_private(
                    &self
                        .directory
                        .join("artifacts")
                        .join(hex::encode(proof.artifact.digest.0)),
                    65536,
                )?;
                proof.artifact.verify_materialized(body.as_slice())?;
                self.store.publish(
                    &format!(
                        "{prefix}/artifacts/{}",
                        hex::encode(proof.artifact.digest.0)
                    ),
                    &body,
                )?;
                if self.capture
                    && proof.artifact.kind
                        != shardline_reliability::MaintenanceArtifactKind::Checkpoint
                {
                    let mut output =
                        capture::verify(&self.directory, proof.artifact.digest, record.generation)?;
                    self.stream(&prefix, "stdout", &mut output.stdout)?;
                    self.stream(&prefix, "stderr", &mut output.stderr)?;
                }
            }
        }
        self.store.publish(
            &format!("{prefix}/records/{:010}", record.sequence.get()),
            &serde_json::to_vec(record)?,
        )?;
        Ok(())
    }

    fn stream(&self, prefix: &str, name: &str, reader: &mut impl Read) -> Result<()> {
        const CHUNK: usize = 4 * 1024 * 1024;
        let mut buffer = vec![0; CHUNK];
        let mut index = 0_u64;
        loop {
            let mut count = 0;
            while count < CHUNK {
                let length =
                    reader.read(buffer.get_mut(count..).ok_or("invalid output buffer")?)?;
                if length == 0 {
                    break;
                }
                count = count.checked_add(length).ok_or("output length overflow")?;
            }
            if count == 0 {
                break;
            }
            self.store.publish(
                &format!("{prefix}/streams/{name}/{index:010}"),
                buffer.get(..count).ok_or("invalid output length")?,
            )?;
            index = index
                .checked_add(1)
                .ok_or("output chunk sequence exhausted")?;
        }
        Ok(())
    }
}

/// Retries evidence export only for verified history from a gone local owner. This never
/// starts a worker, changes its pinned request or authorizes an application retry.
pub(super) fn retry_exports(
    state: &Path,
    store: &TaskEvidenceStore,
    deployment: penelope_core::task::Digest,
) -> Result<()> {
    if !state.exists() {
        return Ok(());
    }
    let Some(batch) = super::outbox::Batch::open(state)? else {
        return Ok(());
    };
    for directory in &batch.jobs {
        batch.advance(directory)?;
        if !directory.join("identity.json").exists() {
            continue;
        }
        artifact::private_directory(directory)?;
        let identity: TaskRunIdentity = serde_json::from_slice(&artifact::read_private(
            &directory.join("identity.json"),
            65536,
        )?)?;
        if identity.schema != 1 || identity.deployment != deployment {
            continue;
        }
        if identity.owner.alive_here() != Some(false) {
            // Never remove scratch owned by another live invocation, nor treat
            // a foreign PID namespace/host as proof that its controller is dead.
            continue;
        }
        let key = artifact::signer(directory, false)?.verifying_key();
        if key.as_bytes() != &identity.public_key {
            return Err("retained task identity has a different verification key".into());
        }
        let path = directory.join("tasks.sqlite");
        super::history::verify(&path, directory, identity.scope, identity.definition, &key)?;
        let mut journal = SqliteTaskJournal::open(&path, identity.scope, identity.definition)?;
        let mut projection =
            penelope_core::task::TaskProjection::new(identity.scope, identity.definition)?;
        let mut records = Vec::new();
        while let Some(record) = journal.read(identity.scope, projection.next_sequence())? {
            projection.apply(&record)?;
            records.push(record);
        }
        let completed = projection.status() == penelope_core::task::TaskStatus::Completed;
        if !completed {
            tracing::warn!(run = %identity.id, workspace = %directory.display(), "unfinished maintenance evidence retained; inspect domain effects before any recovery; no automatic retry");
        }
        let publication = Publication {
            capture: identity.execution
                == shardline_server::task_tracking::TaskExecutionMode::Isolated,
            store: store.clone(),
            id: identity.id.clone(),
            directory: directory.clone(),
        };
        let exported = (|| -> Result<()> {
            store.publish(
                &format!("manifests/{}", identity.id),
                &serde_json::to_vec(&identity)?,
            )?;
            for record in records {
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
                let mut buffer = vec![0; 65536];
                let bytes = if let Some(reference) = reference {
                    let length = journal.read_blob(identity.scope, reference, &mut buffer)?;
                    Some(buffer.get(..length).ok_or("invalid retained task blob")?)
                } else {
                    None
                };
                publication.publish(&record, bytes)?;
            }
            Ok(())
        })();
        if let Err(error) = exported {
            tracing::warn!(run = %identity.id, workspace = %directory.display(), "retained task export remains unavailable: {error}");
            return Ok(());
        }
        drop(journal);
        if completed {
            std::fs::remove_dir_all(directory)?;
        }
    }
    batch.finish()?;
    Ok(())
}

#[cfg(test)]
#[path = "journal_tests.rs"]
mod tests;

impl TaskJournal for PublishedJournal {
    fn read(
        &mut self,
        scope: TaskScope,
        sequence: TaskSequence,
    ) -> std::result::Result<Option<TaskRecord>, TaskPortError> {
        self.local.read(scope, sequence)
    }
    fn commit(
        &mut self,
        record: &TaskRecord,
        blob: Option<&[u8]>,
    ) -> std::result::Result<TaskRecord, TaskPortError> {
        let receipt = self.local.commit(record, blob)?;
        if self.publication_confirmed
            && let Some(publication) = &self.publication
            && let Err(error) = publication.publish(record, blob)
        {
            self.publication_confirmed = false;
            tracing::warn!(run = %publication.id, workspace = %publication.directory.display(), "shared task evidence unavailable; retaining local evidence: {error}");
        }
        Ok(receipt)
    }
    fn read_blob(
        &mut self,
        scope: TaskScope,
        blob: BlobRef,
        buffer: &mut [u8],
    ) -> std::result::Result<usize, TaskPortError> {
        self.local.read_blob(scope, blob, buffer)
    }
}
