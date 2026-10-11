use super::{Input, MaintenanceOperation, Result, artifact, target, verified::VerifiedRuntime};
use penelope_core::task::{
    Digest, EffectContract, ExecutionBudget, Reconciliation, RecoveryMode, TaskDefinition, TaskId,
    TaskInputDigest, TaskRecoveryPolicy, TaskScope, TaskStatus, TaskTenantId,
    runtime::{TaskClock, TaskJournal},
};
use shardline_reliability::{BootTaskClock, ProcessTaskRuntime, TaskSupervisor};
use std::{
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

fn definition(executable: &Path, automatic: bool) -> Result<TaskDefinition> {
    Ok(TaskDefinition {
        code: ProcessTaskRuntime::code_digest(executable)?,
        checkpoint_schema: 1,
        effects: EffectContract::ReconcileRequired,
        recovery: TaskRecoveryPolicy {
            mode: RecoveryMode::ReconcileBeforeRestart,
            max_attempts: 2,
            timeout_ticks: if automatic { u64::MAX } else { 900_000 },
            restart_delay_ticks: 100,
            max_checkpoint_bytes: 65536,
        },
        budget: ExecutionBudget {
            max_input_bytes: 65536,
            max_output_bytes: 65536,
            max_history_records: 128,
            max_checkpoint_commits: 4,
            cpu_ticks: if automatic { u64::MAX } else { 600 },
            deadline_tick: if automatic {
                penelope_core::task::TaskTick::new(u64::MAX)
            } else {
                BootTaskClock::current().saturating_add(1_800_000)
            },
        },
    })
}

fn directory(state: &Path, root: &Path, task: TaskId, automatic: bool) -> Result<PathBuf> {
    let root = if automatic {
        crate::local_output::resolved_output_path(root)?
    } else {
        root.canonicalize()?
    };
    crate::local_path::ensure_directory_path_components_are_not_symlinked(state)?;
    let state = crate::local_output::resolved_output_path(state)?;
    let automatic_workspace = automatic
        && (state == root.join(".shardline-maintenance")
            || state
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("shardline-maintenance-")));
    if (state.starts_with(&root) || root.starts_with(&state)) && !automatic_workspace {
        return Err("maintenance state must be separate from the deployment root".into());
    }
    artifact::private_directory(&state)?;
    let directory = state.join(hex::encode(task.digest().0));
    artifact::private_directory(&directory)?;
    Ok(directory)
}

pub(crate) fn run(
    state: &Path,
    task_name: &str,
    operation: MaintenanceOperation,
) -> Result<super::CommandOutput> {
    run_inner(state, task_name, operation, None)
}

pub(super) fn run_inner(
    state: &Path,
    task_name: &str,
    mut operation: MaintenanceOperation,
    command: Option<Vec<Vec<u8>>>,
) -> Result<super::CommandOutput> {
    if task_name.is_empty() || task_name.len() > 256 {
        return Err("task ID must contain 1 to 256 bytes".into());
    }
    crate::local_path::ensure_directory_path_components_are_not_symlinked(operation.root())?;
    let canonical_root = if command.is_some() {
        crate::local_output::resolved_output_path(operation.root())?
    } else {
        operation.root().canonicalize()?
    };
    match &mut operation {
        MaintenanceOperation::Fsck { root }
        | MaintenanceOperation::IndexRebuild { root }
        | MaintenanceOperation::Gc { root, .. }
        | MaintenanceOperation::StorageMigrate { root, .. }
        | MaintenanceOperation::LifecycleRepair { root, .. } => *root = canonical_root,
    }
    let deployment_root = operation.root().to_path_buf();
    if let MaintenanceOperation::StorageMigrate {
        from,
        from_root,
        to,
        to_root,
        ..
    } = &mut operation
    {
        if *from == super::MaintenanceStorageAdapter::Local && from_root.is_none() {
            *from_root = Some(deployment_root);
        }
        for root in [&mut *from_root, &mut *to_root].into_iter().flatten() {
            crate::local_path::ensure_directory_path_components_are_not_symlinked(root)?;
            *root = if command.is_some() {
                crate::local_output::resolved_output_path(root)?
            } else {
                root.canonicalize()?
            };
            crate::local_path::ensure_directory_path_components_are_not_symlinked(state)?;
            let state = crate::local_output::resolved_output_path(state)?;
            if (state.starts_with(&*root) || root.starts_with(&state))
                && !(command.is_some() && state == root.join(".shardline-maintenance"))
            {
                return Err("maintenance state overlaps a migration endpoint".into());
            }
        }
        if *to == super::MaintenanceStorageAdapter::Local && to_root.is_none() {
            return Err("--to-root is required for local migration destination".into());
        }
    }
    let config = super::operation_config(&operation, command.is_some())?;
    let automatic = command.is_some();
    let input = serde_json::to_vec(&Input {
        schema: 1,
        operation: operation.clone(),
        config: crate::config::selected_config_path(None)
            .map(std::path::absolute)
            .transpose()?,
        target: target(&config, &operation)?,
        command,
        command_digest: None,
    })?;
    let tenant = TaskTenantId::from_digest(Digest::of(b"shardline.local-maintenance.v1"));
    let task = TaskId::from_digest(Digest::of(task_name.as_bytes()));
    let directory = directory(state, config.root_dir(), task, automatic)?;
    let path = directory.join("tasks.sqlite");
    let executable = std::env::current_exe()?;
    let pin = if path.exists() {
        ProcessTaskRuntime::pinned_task(&path, tenant, task)?
    } else {
        None
    };
    let definition = pin.map_or_else(
        || definition(&executable, automatic),
        |(_, pinned)| Ok(pinned),
    )?;
    let current_code = if pin.is_none() {
        definition.code
    } else {
        ProcessTaskRuntime::code_digest(&executable)?
    };
    let scope = TaskScope {
        tenant,
        task,
        definition: definition.fingerprint(),
        input: TaskInputDigest::from_digest(Digest::of(&input)),
    };
    if pin.is_some_and(|(pinned, _)| pinned != scope) {
        return Err("task ID already binds a different request or deployment; use a new ID".into());
    }
    let mut runtime = ProcessTaskRuntime::open(&path, &executable, scope, definition)?;
    if automatic {
        runtime.capture_output();
    }
    let key = artifact::signer(&directory, pin.is_none())?.verifying_key();
    let clock = BootTaskClock::open(&path, scope, definition.budget.deadline_tick)?;
    let runtime = VerifiedRuntime {
        inner: runtime,
        directory: directory.clone(),
        key,
        kind: operation.kind(),
        capture: automatic,
    };
    let identity = if automatic && !operation.read_only() {
        let publication_config = super::publication_config(&operation, &config)?;
        Some((
            shardline_server::task_tracking::TaskEvidenceStore::from_config(&publication_config)?,
            shardline_server::task_tracking::TaskRunIdentity {
                schema: 1,
                id: task_name.to_owned(),
                operation: format!("{:?}", operation.kind()),
                created_at: shardline_protocol::unix_now_seconds_lossy(),
                scope,
                definition,
                public_key: *key.as_bytes(),
                deployment: shardline_server::task_tracking::deployment_fingerprint(
                    &publication_config,
                )?,
                execution: shardline_server::task_tracking::TaskExecutionMode::Isolated,
                owner: shardline_server::task_tracking::TaskOwner::current(),
            },
        ))
    } else {
        None
    };
    let task_journal = super::journal::PublishedJournal::open(&path, scope, definition, identity)?;
    let mut supervisor = TaskSupervisor::<_, _, 65536>::restore(
        task_journal,
        runtime,
        scope,
        definition,
        clock.now(),
    )?;
    super::history::verify(&path, &directory, scope, definition, &key)?;
    if supervisor.projection().status() == TaskStatus::Empty {
        supervisor.submit(&input, 1, clock.now())?;
    }
    loop {
        let status = supervisor.projection().status();
        let generation = supervisor.projection().generation();
        match status {
            TaskStatus::Completed => {
                let output = supervisor
                    .projection()
                    .output()
                    .ok_or("completed task has no output")?;
                let (mut journal, _runtime) = supervisor.into_parts();
                let mut bytes = vec![0; 65536];
                let length = journal.read_blob(scope, output, &mut bytes)?;
                let body = bytes.get(..length).ok_or("invalid output length")?;
                let proof =
                    artifact::verify(&directory, body, scope, generation, operation.kind(), &key)?;
                let proof_path = directory.join("result-proof.json");
                crate::write_output_bytes(&proof_path, body, false)?;
                let success = artifact::success(&directory, proof.artifact.digest)?;
                return Ok(super::CommandOutput {
                    exit_code: None,
                    evidence_published: journal.publication_confirmed(),
                    capture: if automatic {
                        Some(super::capture::verify(
                            &directory,
                            proof.artifact.digest,
                            generation,
                        )?)
                    } else {
                        None
                    },
                    success,
                    text: format!(
                        "maintenance.task: {task_name}\nmaintenance.status: completed\nmaintenance.proof: {}\nmaintenance.report: {}\nmaintenance.trusted_key: {}\n",
                        proof_path.display(),
                        directory
                            .join("artifacts")
                            .join(hex::encode(proof.artifact.digest.0))
                            .display(),
                        hex::encode(key.as_bytes())
                    ),
                });
            }
            TaskStatus::Reconciling => {
                if let Some((output, bytes)) =
                    ProcessTaskRuntime::proposed_output(&path, scope, generation)?
                {
                    artifact::verify(
                        &directory,
                        &bytes,
                        scope,
                        generation,
                        operation.kind(),
                        &key,
                    )?;
                    supervisor.reconcile(
                        scope,
                        generation,
                        Reconciliation::Completed {
                            output,
                            evidence: Digest::of(&bytes),
                        },
                        Some(&bytes),
                        clock.now(),
                    )?;
                } else if operation.restart_safe() && current_code == definition.code {
                    // The complete fsck scan and legacy-evidence backfill are
                    // repeatable after fencing. Never resume a partial scan cursor.
                    supervisor.reconcile(
                        scope,
                        generation,
                        Reconciliation::RestartAuthorized {
                            evidence: Digest::of(
                                b"shardline.maintenance.complete-fresh-scan-after-fence.v1",
                            ),
                        },
                        None,
                        clock.now(),
                    )?;
                } else {
                    return Err("maintenance effects remain unknown after fencing; inspect deployment state before starting a new task ID".into());
                }
            }
            TaskStatus::Running => {
                supervisor.poll(clock.now())?;
            }
            TaskStatus::Exhausted => {
                return Err("maintenance recovery budget or original deadline exhausted".into());
            }
            TaskStatus::Empty => return Err("missing task intent".into()),
            TaskStatus::Pending | TaskStatus::Ready => {
                if current_code != definition.code {
                    return Err("task code is pinned to an older executable; new code requires a new task ID".into());
                }
                supervisor.advance(clock.now())?;
            }
            TaskStatus::Faulted | TaskStatus::Reconciled => {
                supervisor.advance(clock.now())?;
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
}
