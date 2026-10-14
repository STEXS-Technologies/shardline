//! Read-only inspection of one owner-selected native workspace.
use super::{Result, artifact, capture};
use ed25519_dalek::VerifyingKey;
use penelope_core::task::{
    Reconciliation, TaskExecutionEvent, TaskProjection, TaskStatus, runtime::TaskJournal,
};
use shardline_reliability::{MaintenanceArtifactKind, SqliteTaskJournal};
use shardline_server::task_tracking::{TaskExecutionMode, TaskRunIdentity, TaskRunState};
use std::{
    fs::OpenOptions,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

pub(crate) fn run(directory: &Path, after: Option<&str>) -> Result<serde_json::Value> {
    artifact::existing_private_directory(directory)?;
    let identity: TaskRunIdentity = serde_json::from_slice(&artifact::read_private(
        &directory.join("identity.json"),
        65536,
    )?)?;
    if identity.schema != 1
        || identity.id.is_empty()
        || identity.id.len() > 128
        || identity.scope.definition != identity.definition.fingerprint()
        || identity.definition.budget.max_history_records > 4096
    {
        return Err("invalid native archive identity; preserve its files and original logs".into());
    }
    let key = VerifyingKey::from_bytes(&identity.public_key)?;
    // Validate owner, permissions and file type before SQLite opens the path.
    let journal_path = directory.join("tasks.sqlite");
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&journal_path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.uid() != rustix::process::geteuid().as_raw() {
        return Err("invalid native archive journal owner or file type".into());
    }
    let mut journal =
        SqliteTaskJournal::open_read_only(&journal_path, identity.scope, identity.definition)?;
    let mut projection = TaskProjection::new(identity.scope, identity.definition)?;
    let mut success = true;
    let mut output_verified = None;
    let mut buffer = [0; 65536];
    while let Some(record) = journal.read(identity.scope, projection.next_sequence())? {
        projection.apply(&record)?;
        let (reference, checkpoint, completed) = match record.event {
            TaskExecutionEvent::IntentCommitted { input, .. } => (Some(input), false, false),
            TaskExecutionEvent::CheckpointCommitted { checkpoint }
            | TaskExecutionEvent::CheckpointMigrated { checkpoint, .. } => {
                (Some(checkpoint), true, false)
            }
            TaskExecutionEvent::Completed { output }
            | TaskExecutionEvent::Reconciled {
                result: Reconciliation::Completed { output, .. },
            } => (Some(output), false, true),
            TaskExecutionEvent::Started
            | TaskExecutionEvent::FaultObserved { .. }
            | TaskExecutionEvent::ReconciliationRequired
            | TaskExecutionEvent::Reconciled { .. }
            | TaskExecutionEvent::RecoveryPlanned { .. }
            | TaskExecutionEvent::Restarted
            | TaskExecutionEvent::RecoveryExhausted => (None, false, false),
        };
        if let Some(reference) = reference {
            if reference.schema != 1 {
                return Err("invalid native archive blob schema".into());
            }
            let length = journal.read_blob(identity.scope, reference, &mut buffer)?;
            let bytes = buffer
                .get(..length)
                .ok_or("invalid native archive blob length")?;
            if checkpoint || completed {
                let proof = shardline_reliability::MaintenanceArtifactProof::from_json(bytes)?;
                if checkpoint != (proof.artifact.kind == MaintenanceArtifactKind::Checkpoint) {
                    return Err("native archive artifact role does not match its record".into());
                }
                let proof = artifact::verify(
                    directory,
                    bytes,
                    identity.scope,
                    record.generation,
                    proof.artifact.kind,
                    &key,
                )?;
                if completed {
                    success = match identity.execution {
                        TaskExecutionMode::Isolated => {
                            let output = capture::verify(
                                directory,
                                proof.artifact.digest,
                                record.generation,
                            )?;
                            output_verified = Some(true);
                            output.exit_code == 0
                        }
                        TaskExecutionMode::Inline => {
                            inline_success(directory, proof.artifact.digest)?
                        }
                        TaskExecutionMode::Embedded => true,
                    };
                }
            }
        }
    }
    let state = match projection.status() {
        TaskStatus::Completed if success => TaskRunState::Completed,
        TaskStatus::Completed => TaskRunState::Failed,
        TaskStatus::Empty | TaskStatus::Pending | TaskStatus::Ready => TaskRunState::Pending,
        TaskStatus::Running => match identity.owner.alive_here() {
            Some(true) => TaskRunState::Running,
            Some(false) => TaskRunState::NeedsReconciliation,
            None => TaskRunState::RunningOrInterrupted,
        },
        TaskStatus::Faulted
        | TaskStatus::Reconciling
        | TaskStatus::Reconciled
        | TaskStatus::Exhausted => TaskRunState::NeedsReconciliation,
    };
    let tasks = if after.is_some_and(|cursor| identity.id.as_str() <= cursor) {
        Vec::new()
    } else {
        vec![serde_json::json!({
            "id": identity.id, "operation": identity.operation, "state": state,
            "execution": identity.execution, "created_at": identity.created_at,
            "owner": identity.owner, "output_verified": output_verified,
            "next_step": state.next_step()
        })]
    };
    Ok(serde_json::json!({
        "tasks": tasks, "next_after": null, "archive": true, "native_journal": true,
        "history_notice": "Owner-selected native journal. Inspection verifies saved evidence without exporting, rewriting or executing work."
    }))
}

fn inline_success(directory: &Path, digest: penelope_core::task::Digest) -> Result<bool> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Outcome {
        schema: u32,
        exit_code: u8,
    }
    let body = artifact::read_private(
        &directory.join("artifacts").join(hex::encode(digest.0)),
        65536,
    )?;
    let split = body
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or("invalid outcome")?;
    let outcome: Outcome = serde_json::from_slice(
        body.get(split.saturating_add(1)..)
            .ok_or("invalid outcome")?,
    )?;
    if outcome.schema != 1
        || outcome.exit_code > 2
        || body.get(..split)
            != Some(format!("maintenance.success: {}", outcome.exit_code == 0).as_bytes())
    {
        return Err("invalid signed command outcome".into());
    }
    Ok(outcome.exit_code == 0)
}
