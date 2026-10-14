#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
use super::*;
use penelope_core::task::{
    Digest, EffectContract, ExecutionBudget, RecoveryMode, TaskGeneration, TaskId, TaskInputDigest,
    TaskProjection, TaskRecoveryPolicy, TaskTenantId, TaskTick,
};
use shardline_reliability::MaintenanceArtifactKind;
use shardline_server::{
    ServerConfig,
    task_tracking::{TaskExecutionMode, TaskOwner, TaskRunState},
};
use std::{fs, num::NonZeroUsize};

fn config(root: &Path) -> ServerConfig {
    ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.to_path_buf(),
        NonZeroUsize::new(4096).unwrap(),
    )
}

fn retained(root: &Path, id: &str, completed: bool) -> PathBuf {
    let config = config(root);
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let deployment = shardline_server::task_tracking::deployment_fingerprint(&config).unwrap();
    let definition = TaskDefinition {
        code: Digest::of(b"outbox-test"),
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
    let input = b"no application work: native outbox fixture";
    let scope = TaskScope {
        tenant: TaskTenantId::from_digest(Digest::of(b"outbox-tests")),
        task: TaskId::from_digest(Digest::of(id.as_bytes())),
        definition: definition.fingerprint(),
        input: TaskInputDigest::from_digest(Digest::of(input)),
    };
    let state = root.join(".shardline-maintenance");
    artifact::private_directory(&state).unwrap();
    let directory = state.join(hex::encode(scope.task.digest().0));
    artifact::private_directory(&directory).unwrap();
    let key = artifact::signer(&directory, true).unwrap().verifying_key();
    let mut owner = TaskOwner::current();
    // Linux cannot allocate this PID; identity remains in our observable host,
    // boot and namespace. This fixture never starts an application worker.
    owner.pid = u32::MAX;
    let identity = TaskRunIdentity {
        schema: 1,
        id: id.to_owned(),
        operation: "test-fsck".to_owned(),
        created_at: shardline_protocol::unix_now_seconds_lossy(),
        scope,
        definition,
        public_key: *key.as_bytes(),
        deployment,
        execution: TaskExecutionMode::Inline,
        owner,
    };
    let mut journal = PublishedJournal::open(
        &directory.join("tasks.sqlite"),
        scope,
        definition,
        Some((store, identity)),
    )
    .unwrap();
    let mut projection = TaskProjection::new(scope, definition).unwrap();
    let mut commit = |event, generation, bytes: Option<&[u8]>, tick| {
        let record = projection
            .propose(event, generation, TaskTick::new(tick))
            .unwrap();
        let committed = journal.commit(&record, bytes).unwrap();
        projection.apply(&committed).unwrap();
    };
    let reference = |bytes: &[u8]| BlobRef {
        digest: Digest::of(bytes),
        bytes: u32::try_from(bytes.len()).unwrap(),
        schema: 1,
    };
    commit(
        TaskExecutionEvent::IntentCommitted {
            definition,
            input: reference(input),
        },
        TaskGeneration::ZERO,
        Some(input),
        0,
    );
    let generation = TaskGeneration::new(1);
    commit(TaskExecutionEvent::Started, generation, None, 1);
    let checkpoint = artifact::publish(
        &directory,
        scope,
        generation,
        MaintenanceArtifactKind::Checkpoint,
        1,
        b"application work not started",
    )
    .unwrap();
    commit(
        TaskExecutionEvent::CheckpointCommitted {
            checkpoint: reference(&checkpoint),
        },
        generation,
        Some(&checkpoint),
        2,
    );
    if completed {
        let proof = artifact::publish(
            &directory,
            scope,
            generation,
            MaintenanceArtifactKind::Fsck,
            2,
            b"maintenance.success: true\n{\"schema\":1,\"exit_code\":0}",
        )
        .unwrap();
        commit(
            TaskExecutionEvent::Completed {
                output: reference(&proof),
            },
            generation,
            Some(&proof),
            3,
        );
    }
    directory
}

#[test]
fn unfinished_and_corrupt_backlogs_do_not_starve_completed_export() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let deployment = shardline_server::task_tracking::deployment_fingerprint(&config).unwrap();
    let blocked = root.path().join("chunks/_shardline_tasks/v1/manifests");
    fs::create_dir_all(blocked.parent().unwrap()).unwrap();
    fs::write(&blocked, b"publication denied").unwrap();
    let mut ids = (0..131)
        .map(|index| {
            let id = format!("outbox-{index}");
            (hex::encode(Digest::of(id.as_bytes()).0), id)
        })
        .collect::<Vec<_>>();
    ids.sort();
    let completed = ids.last().unwrap().1.clone();
    for (_, id) in &ids {
        retained(root.path(), id, id == &completed);
    }
    let first = root.path().join(".shardline-maintenance").join(&ids[0].0);
    fs::write(first.join("signing-key"), [42; 32]).unwrap();
    fs::remove_file(blocked).unwrap();
    let state = root.path().join(".shardline-maintenance");
    assert!(retry_exports(&state, &store, deployment).is_err());
    // A corrupt first journal advances the cursor rather than permanently
    // blocking the verified completed outcome at the end of the backlog.
    for _ in 0..4 {
        let _result = retry_exports(&state, &store, deployment);
    }
    let manifests = store.summaries(deployment, None, 101).unwrap();
    // The completed run sorts last by directory digest, not API run ID.
    let bytes = store
        .read(&format!("manifests/{completed}"), 65536)
        .unwrap()
        .unwrap();
    let identity: TaskRunIdentity = serde_json::from_slice(&bytes).unwrap();
    let all = store
        .summaries(
            deployment,
            manifests.last().map(|task| task.id.as_str()),
            101,
        )
        .unwrap();
    assert!(
        manifests
            .iter()
            .chain(&all)
            .any(|task| task.id == completed && task.state == TaskRunState::Completed)
    );
    assert!(
        !state
            .join(hex::encode(identity.scope.task.digest().0))
            .exists()
    );
    assert!(
        manifests
            .iter()
            .any(|task| task.state == TaskRunState::NeedsReconciliation)
    );
    assert!(first.exists(), "corrupt evidence must be retained");
    assert_eq!(
        fs::read_dir(state)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .filter(|entry| entry.file_name() != "retired")
            .count(),
        130
    );
}

#[test]
fn native_archive_inspection_checks_isolated_output_against_signed_result() {
    let root = tempfile::tempdir().unwrap();
    let directory = retained(root.path(), "captured", false);
    let identity_path = directory.join("identity.json");
    let mut identity: TaskRunIdentity =
        serde_json::from_slice(&fs::read(&identity_path).unwrap()).unwrap();
    identity.execution = TaskExecutionMode::Isolated;
    fs::write(&identity_path, serde_json::to_vec(&identity).unwrap()).unwrap();
    for (name, bytes) in [
        ("stdout-1", b"verified output".as_slice()),
        ("stderr-1", b"".as_slice()),
    ] {
        crate::write_output_bytes(&directory.join(name), bytes, false).unwrap();
    }
    let mut report = Vec::new();
    capture::publish(
        &directory,
        TaskGeneration::new(1),
        std::process::ExitCode::SUCCESS,
        &mut report,
    )
    .unwrap();
    let proof = artifact::publish(
        &directory,
        identity.scope,
        TaskGeneration::new(1),
        MaintenanceArtifactKind::Fsck,
        2,
        &report,
    )
    .unwrap();
    let mut journal = SqliteTaskJournal::open(
        &directory.join("tasks.sqlite"),
        identity.scope,
        identity.definition,
    )
    .unwrap();
    let mut projection = TaskProjection::new(identity.scope, identity.definition).unwrap();
    while let Some(record) = journal
        .read(identity.scope, projection.next_sequence())
        .unwrap()
    {
        projection.apply(&record).unwrap();
    }
    let record = projection
        .propose(
            TaskExecutionEvent::Completed {
                output: BlobRef {
                    digest: Digest::of(&proof),
                    bytes: u32::try_from(proof.len()).unwrap(),
                    schema: 1,
                },
            },
            TaskGeneration::new(1),
            TaskTick::new(3),
        )
        .unwrap();
    journal.commit(&record, Some(&proof)).unwrap();
    drop(journal);
    let page = super::super::inspection::run(&directory, None).unwrap();
    assert_eq!(page["tasks"][0]["state"], "completed");
    assert_eq!(page["tasks"][0]["output_verified"], true);
    fs::write(directory.join("stdout-1"), b"damaged output").unwrap();
    assert!(super::super::inspection::run(&directory, None).is_err());
    assert_eq!(
        fs::read(directory.join("stdout-1")).unwrap(),
        b"damaged output"
    );
}

#[test]
fn native_archive_inspection_verifies_unexported_results_and_preserves_files() {
    let root = tempfile::tempdir().unwrap();
    let blocked = root.path().join("chunks/_shardline_tasks/v1/manifests");
    fs::create_dir_all(blocked.parent().unwrap()).unwrap();
    fs::write(&blocked, b"publication denied").unwrap();
    for (id, completed, expected) in [
        ("native-completed", true, "completed"),
        ("native-interrupted", false, "needs_reconciliation"),
    ] {
        let directory = retained(root.path(), id, completed);
        let path = directory.join("tasks.sqlite");
        let before = fs::read(&path).unwrap();
        let page = super::super::inspection::run(&directory, None).unwrap();
        assert_eq!(page["tasks"][0]["id"], id);
        assert_eq!(page["tasks"][0]["state"], expected);
        assert_eq!(page["native_journal"], true);
        assert_eq!(fs::read(&path).unwrap(), before);
        let next = super::super::inspection::run(&directory, Some(id)).unwrap();
        assert_eq!(next["tasks"].as_array().unwrap().len(), 0);
        // Signed proof alone is insufficient: physical result bytes must match.
        let artifact = fs::read_dir(directory.join("artifacts"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        fs::write(&artifact, b"damaged materialized evidence").unwrap();
        assert!(super::super::inspection::run(&directory, None).is_err());
        assert_eq!(
            fs::read(&artifact).unwrap(),
            b"damaged materialized evidence"
        );
        assert_eq!(fs::read(&path).unwrap(), before);
    }
    assert_eq!(fs::read(blocked).unwrap(), b"publication denied");
}

#[test]
fn unfinished_export_reads_native_journal_without_rewriting_or_restarting_it() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let deployment = shardline_server::task_tracking::deployment_fingerprint(&config).unwrap();
    let blocked = root.path().join("chunks/_shardline_tasks/v1/manifests");
    fs::create_dir_all(blocked.parent().unwrap()).unwrap();
    fs::write(&blocked, b"publication denied").unwrap();
    let directory = retained(root.path(), "unexported", false);
    let path = directory.join("tasks.sqlite");
    let before = fs::read(&path).unwrap();
    let identity_before = fs::read(directory.join("identity.json")).unwrap();
    fs::remove_file(blocked).unwrap();
    retry_exports(
        &root.path().join(".shardline-maintenance"),
        &store,
        deployment,
    )
    .unwrap();
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(
        fs::read(directory.join("identity.json")).unwrap(),
        identity_before
    );
    let history = store.summaries(deployment, None, 100).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].id, "unexported");
    assert_eq!(history[0].state, TaskRunState::NeedsReconciliation);
    assert!(directory.exists());
}

#[test]
fn another_exporter_does_not_block_maintenance_or_delete_its_journals() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let deployment = shardline_server::task_tracking::deployment_fingerprint(&config).unwrap();
    let directory = retained(root.path(), "completed", true);
    let state = root.path().join(".shardline-maintenance");
    let guard = std::fs::File::open(&state).unwrap();
    guard.try_lock().unwrap();
    retry_exports(&state, &store, deployment).unwrap();
    assert!(directory.exists());
    assert!(!state.join(".export-cursor").exists());
    drop(guard);
    retry_exports(&state, &store, deployment).unwrap();
    assert!(!directory.exists());
    assert_eq!(fs::read_dir(state.join("retired")).unwrap().count(), 0);
    assert_eq!(fs::read_dir(state).unwrap().count(), 1);
}
