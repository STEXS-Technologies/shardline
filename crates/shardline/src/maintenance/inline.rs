//! Track the existing CLI process without adding a second process or buffering
//! its output. Domain transactions and fences remain the effect authority.
use super::{Input, MaintenanceOperation, Result, artifact, target};
use penelope_core::task::{
    BlobRef, Digest, EffectContract, ExecutionBudget, RecoveryMode, TaskDefinition,
    TaskExecutionEvent, TaskGeneration, TaskId, TaskInputDigest, TaskProjection,
    TaskRecoveryPolicy, TaskScope, TaskTenantId, TaskTick, runtime::TaskJournal,
};
use shardline_reliability::{BootTaskClock, MaintenanceArtifactKind, ProcessTaskRuntime};
use std::{ffi::OsString, path::Path, process::ExitCode};

pub(super) fn run(
    state: &Path,
    id: &str,
    operation: &MaintenanceOperation,
    command: Vec<Vec<u8>>,
    arguments: Vec<OsString>,
) -> Result<super::CommandOutput> {
    let config = super::operation_config(operation, true)?;
    let input = serde_json::to_vec(&Input {
        schema: 1,
        operation: operation.clone(),
        config: crate::config::selected_config_path(None)
            .map(std::path::absolute)
            .transpose()?,
        target: target(&config, operation)?,
        command_digest: Some(Digest::of(&serde_json::to_vec(&(
            command,
            std::env::current_dir()?.as_os_str().as_encoded_bytes(),
        ))?)),
        command: None,
    })?;
    let definition = TaskDefinition {
        code: ProcessTaskRuntime::code_digest(&std::env::current_exe()?)?,
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
    let task = TaskId::from_digest(Digest::of(id.as_bytes()));
    let scope = TaskScope {
        tenant: TaskTenantId::from_digest(Digest::of(b"shardline.local-maintenance.v1")),
        task,
        definition: definition.fingerprint(),
        input: TaskInputDigest::from_digest(Digest::of(&input)),
    };
    artifact::private_directory(state)?;
    let directory = state.join(hex::encode(task.digest().0));
    artifact::private_directory(&directory)?;
    let key = artifact::signer(&directory, true)?.verifying_key();
    let identity = if operation.read_only() {
        None
    } else {
        let export_config = super::publication_config(operation, &config)?;
        Some((
            shardline_server::task_tracking::TaskEvidenceStore::from_config(&export_config)?,
            shardline_server::task_tracking::TaskRunIdentity {
                schema: 1,
                id: id.to_owned(),
                operation: format!("{:?}", operation.kind()),
                created_at: shardline_protocol::unix_now_seconds_lossy(),
                scope,
                definition,
                public_key: *key.as_bytes(),
                deployment: shardline_server::task_tracking::deployment_fingerprint(
                    &export_config,
                )?,
                execution: shardline_server::task_tracking::TaskExecutionMode::Inline,
                owner: shardline_server::task_tracking::TaskOwner::current(),
            },
        ))
    };
    let mut journal = super::journal::PublishedJournal::open(
        &directory.join("tasks.sqlite"),
        scope,
        definition,
        identity,
    )?;
    let mut projection = TaskProjection::new(scope, definition)?;
    commit(
        &mut journal,
        &mut projection,
        TaskExecutionEvent::IntentCommitted {
            definition,
            input: reference(&input)?,
        },
        TaskGeneration::ZERO,
        Some(&input),
    )?;
    let generation = TaskGeneration::new(1);
    commit(
        &mut journal,
        &mut projection,
        TaskExecutionEvent::Started,
        generation,
        None,
    )?;
    let checkpoint = artifact::publish(
        &directory,
        scope,
        generation,
        MaintenanceArtifactKind::Checkpoint,
        1,
        b"ordinary CLI admitted; no application work completed",
    )?;
    artifact::verify(
        &directory,
        &checkpoint,
        scope,
        generation,
        MaintenanceArtifactKind::Checkpoint,
        &key,
    )?;
    commit(
        &mut journal,
        &mut projection,
        TaskExecutionEvent::CheckpointCommitted {
            checkpoint: reference(&checkpoint)?,
        },
        generation,
        Some(&checkpoint),
    )?;

    // These are the original handlers, in the original CLI process. They retain
    // their live streams, diagnostic destinations and operation-specific barriers.
    let status = tokio::runtime::Handle::current()
        .block_on(Box::pin(crate::entry::run_direct(arguments.into_iter())));
    let exit_code = if status == ExitCode::SUCCESS {
        0
    } else if status == ExitCode::FAILURE {
        1
    } else {
        2
    };
    let report = format!(
        "maintenance.success: {}\n{}",
        exit_code == 0,
        serde_json::to_string(&serde_json::json!({"schema": 1, "exit_code": exit_code}))?
    );
    let output = artifact::publish(
        &directory,
        scope,
        generation,
        operation.kind(),
        2,
        report.as_bytes(),
    )?;
    artifact::verify(
        &directory,
        &output,
        scope,
        generation,
        operation.kind(),
        &key,
    )?;
    commit(
        &mut journal,
        &mut projection,
        TaskExecutionEvent::Completed {
            output: reference(&output)?,
        },
        generation,
        Some(&output),
    )?;
    Ok(super::CommandOutput {
        text: String::new(),
        success: exit_code == 0,
        capture: None,
        evidence_published: journal.publication_confirmed(),
        exit_code: Some(exit_code),
    })
}

fn reference(bytes: &[u8]) -> Result<BlobRef> {
    if bytes.len() > 65536 {
        return Err("task evidence exceeds 64 KiB".into());
    }
    Ok(BlobRef {
        digest: Digest::of(bytes),
        bytes: u32::try_from(bytes.len())?,
        schema: 1,
    })
}

fn commit(
    journal: &mut super::journal::PublishedJournal,
    projection: &mut TaskProjection,
    event: TaskExecutionEvent,
    generation: TaskGeneration,
    bytes: Option<&[u8]>,
) -> Result<()> {
    let record = projection.propose(event, generation, BootTaskClock::current())?;
    let confirmed = journal.commit(&record, bytes)?;
    if record != confirmed {
        return Err("task journal did not confirm the intended record".into());
    }
    projection.apply(&confirmed)?;
    Ok(())
}
