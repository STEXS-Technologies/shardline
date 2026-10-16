#![cfg(all(feature = "process-runtime", target_os = "linux"))]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use penelope::core::task::{
    Digest, EffectContract, ExecutionBudget, RecoveryMode, TaskDefinition, TaskGeneration, TaskId,
    TaskInputDigest, TaskRecoveryPolicy, TaskScope, TaskStatus, TaskTenantId,
    runtime::{TaskClock, TaskJournal, TaskPortError, TaskRuntime, TaskStart},
};
use shardline_reliability::{
    BootTaskClock, LocalTaskSupervisor, ProcessTaskRuntime, SqliteTaskJournal,
    restore_local_task_supervisor,
};
use std::{
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

fn helper() -> PathBuf {
    let executable = std::env::current_exe().unwrap();
    let path = executable
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples/process_test_worker");
    assert!(
        path.is_file(),
        "build the process_test_worker example before this test suite"
    );
    path
}

fn fixture(input: &[u8]) -> (tempfile::TempDir, PathBuf, TaskScope, TaskDefinition) {
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(
        directory.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .unwrap();
    let path = directory.path().join("tasks.sqlite");
    let definition = TaskDefinition {
        code: ProcessTaskRuntime::code_digest(&helper()).unwrap(),
        checkpoint_schema: 1,
        effects: EffectContract::ReconcileRequired,
        recovery: TaskRecoveryPolicy {
            mode: RecoveryMode::ReconcileBeforeRestart,
            max_attempts: 2,
            timeout_ticks: 5000,
            restart_delay_ticks: 1,
            max_checkpoint_bytes: 128,
        },
        budget: ExecutionBudget {
            max_input_bytes: 128,
            max_output_bytes: 128,
            max_history_records: 32,
            max_checkpoint_commits: 4,
            cpu_ticks: 1,
            deadline_tick: BootTaskClock::current().saturating_add(10000),
        },
    };
    let scope = TaskScope {
        tenant: TaskTenantId::from_digest(Digest::of(b"tenant")),
        task: TaskId::from_digest(Digest::of(b"runtime-task")),
        definition: definition.fingerprint(),
        input: TaskInputDigest::from_digest(Digest::of(input)),
    };
    (directory, path, scope, definition)
}

fn open(
    path: &Path,
    scope: TaskScope,
    definition: TaskDefinition,
) -> LocalTaskSupervisor<ProcessTaskRuntime, 128> {
    let runtime = ProcessTaskRuntime::open(path, &helper(), scope, definition).unwrap();
    restore_local_task_supervisor(path, runtime, scope, definition, BootTaskClock::current())
        .unwrap()
}

fn wait_for(mut condition: impl FnMut() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(
            start.elapsed() < Duration::from_secs(8),
            "process fixture timed out"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn genuine_child_completes_only_through_durable_supervisor_output() {
    let (_directory, path, scope, definition) = fixture(b"complete");
    let mut supervisor = open(&path, scope, definition);
    supervisor
        .submit(b"complete", 1, BootTaskClock::current())
        .unwrap();
    supervisor.advance(BootTaskClock::current()).unwrap();
    wait_for(|| {
        supervisor.poll(BootTaskClock::current()).unwrap();
        supervisor.projection().status() == TaskStatus::Completed
    });
    let reference = supervisor.projection().output().unwrap();
    let (mut journal, _runtime) = supervisor.into_parts();
    let mut bytes = [0; 128];
    assert_eq!(journal.read_blob(scope, reference, &mut bytes).unwrap(), 6);
    assert_eq!(bytes.get(..6).unwrap(), b"output");
}

#[test]
fn real_checkpoint_worker_pauses_until_history_commit_and_acknowledgement() {
    let (_directory, path, scope, definition) = fixture(b"checkpoint");
    let mut supervisor = open(&path, scope, definition);
    supervisor
        .submit(b"checkpoint", 1, BootTaskClock::current())
        .unwrap();
    supervisor.advance(BootTaskClock::current()).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    wait_for(|| {
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM penelope_runtime_v1 WHERE kind=1)",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap()
    });
    assert_eq!(supervisor.projection().checkpoint(), None);
    thread::sleep(Duration::from_millis(30));
    assert_eq!(
        connection
            .query_row("SELECT kind FROM penelope_runtime_v1", [], |row| row
                .get::<_, u32>(0))
            .unwrap(),
        1
    );
    supervisor.poll(BootTaskClock::current()).unwrap();
    assert!(supervisor.projection().checkpoint().is_some());
    wait_for(|| {
        supervisor.poll(BootTaskClock::current()).unwrap();
        supervisor.projection().status() == TaskStatus::Completed
    });
}

#[test]
fn controller_bound_runtime_stops_worker_on_drop_without_capturing_output() {
    let (_directory, path, scope, definition) = fixture(b"hold");
    let mut runtime = ProcessTaskRuntime::open(&path, &helper(), scope, definition).unwrap();
    runtime.stop_with_controller();
    let mut supervisor = restore_local_task_supervisor::<_, 128>(
        &path,
        runtime,
        scope,
        definition,
        BootTaskClock::current(),
    )
    .unwrap();
    supervisor
        .submit(b"hold", 1, BootTaskClock::current())
        .unwrap();
    supervisor.advance(BootTaskClock::current()).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    wait_for(|| {
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM penelope_runtime_v1 WHERE pid IS NOT NULL)",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap()
    });
    let encoded: Vec<u8> = connection
        .query_row("SELECT authorization FROM penelope_runtime_v1", [], |row| {
            row.get(0)
        })
        .unwrap();
    let authorization: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(
        authorization.get("controller_pid"),
        Some(&serde_json::json!(std::process::id()))
    );
    drop(supervisor);
    assert!(
        connection
            .query_row("SELECT fenced FROM penelope_runtime_v1", [], |row| row
                .get::<_, bool>(0))
            .unwrap()
    );
    assert!(
        !std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".stdout")
            })
    );
}

#[test]
fn reconstructed_controller_kills_surviving_worker_and_persists_delayed_start_fence() {
    let (_directory, path, scope, definition) = fixture(b"hold");
    let mut supervisor = open(&path, scope, definition);
    supervisor
        .submit(b"hold", 1, BootTaskClock::current())
        .unwrap();
    supervisor.advance(BootTaskClock::current()).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    wait_for(|| {
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM penelope_runtime_v1 WHERE pid IS NOT NULL)",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap()
    });
    let old_pid: u32 = connection
        .query_row("SELECT pid FROM penelope_runtime_v1", [], |row| row.get(0))
        .unwrap();
    let deadline = supervisor.projection().execution_deadline();
    drop(supervisor);
    let restored = open(&path, scope, definition);
    assert_eq!(restored.projection().status(), TaskStatus::Faulted);
    assert!(
        connection
            .query_row("SELECT fenced FROM penelope_runtime_v1", [], |row| row
                .get::<_, bool>(0))
            .unwrap()
    );
    let (_journal, mut runtime) = restored.into_parts();
    assert_eq!(
        runtime.start(TaskStart {
            scope,
            generation: TaskGeneration::new(1),
            definition,
            execution_deadline: deadline,
            input_schema: 1,
            input: b"hold",
            checkpoint: None
        }),
        Err(TaskPortError::Conflict)
    );
    let _waited = rustix::process::waitpid(
        rustix::process::Pid::from_raw(i32::try_from(old_pid).unwrap()),
        rustix::process::WaitOptions::NOHANG,
    );
}

#[test]
fn concurrent_controller_and_changed_definition_cannot_take_ownership() {
    let (_directory, path, scope, definition) = fixture(b"complete");
    let first = ProcessTaskRuntime::open(&path, &helper(), scope, definition).unwrap();
    assert!(matches!(
        ProcessTaskRuntime::open(&path, &helper(), scope, definition),
        Err(TaskPortError::Conflict)
    ));
    drop(first);
    let mut changed = definition;
    changed.recovery.max_attempts = 3;
    let mut changed_scope = scope;
    changed_scope.definition = changed.fingerprint();
    assert!(matches!(
        ProcessTaskRuntime::open(&path, &helper(), changed_scope, changed),
        Err(TaskPortError::Conflict)
    ));
}

#[test]
fn kernel_cpu_limit_is_applied_before_work_and_runaway_worker_is_stopped() {
    let (_directory, path, scope, definition) = fixture(b"limit");
    let mut supervisor = open(&path, scope, definition);
    supervisor
        .submit(b"limit", 1, BootTaskClock::current())
        .unwrap();
    supervisor.advance(BootTaskClock::current()).unwrap();
    wait_for(|| {
        supervisor.poll(BootTaskClock::current()).unwrap();
        supervisor.projection().status() == TaskStatus::Completed
    });
    let output = supervisor.projection().output().unwrap();
    let (mut journal, _runtime) = supervisor.into_parts();
    let mut buffer = [0; 128];
    let count = journal.read_blob(scope, output, &mut buffer).unwrap();
    assert_eq!(
        serde_json::from_slice::<(Option<u64>, Option<u64>)>(buffer.get(..count).unwrap()).unwrap(),
        (Some(1), Some(1))
    );
    let (_cpu_directory, cpu_path, cpu_scope, cpu_definition) = fixture(b"cpu");
    let mut cpu_supervisor = open(&cpu_path, cpu_scope, cpu_definition);
    cpu_supervisor
        .submit(b"cpu", 1, BootTaskClock::current())
        .unwrap();
    cpu_supervisor.advance(BootTaskClock::current()).unwrap();
    wait_for(|| {
        cpu_supervisor.poll(BootTaskClock::current()).unwrap();
        cpu_supervisor.projection().status() == TaskStatus::Faulted
    });
}

#[test]
fn boot_change_conservatively_expires_deadlines_instead_of_resetting_time() {
    let (_directory, path, scope, definition) = fixture(b"complete");
    let runtime = ProcessTaskRuntime::open(&path, &helper(), scope, definition).unwrap();
    let clock = BootTaskClock::open(&path, scope, definition.budget.deadline_tick).unwrap();
    assert!(clock.now() < definition.budget.deadline_tick);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute("UPDATE penelope_clocks_v1 SET boot='previous-boot'", [])
        .unwrap();
    let rebooted = BootTaskClock::open(&path, scope, definition.budget.deadline_tick).unwrap();
    assert!(rebooted.now() >= definition.budget.deadline_tick);
    drop(runtime);
}

#[test]
fn uncommitted_start_cannot_authorize_a_child() {
    let (_directory, path, scope, definition) = fixture(b"hold");
    let mut runtime = ProcessTaskRuntime::open(&path, &helper(), scope, definition).unwrap();
    assert_eq!(
        runtime.start(TaskStart {
            scope,
            generation: TaskGeneration::new(1),
            definition,
            execution_deadline: definition.budget.deadline_tick,
            input_schema: 1,
            input: b"hold",
            checkpoint: None
        }),
        Err(TaskPortError::Conflict)
    );
    let mut journal = SqliteTaskJournal::open(&path, scope, definition).unwrap();
    assert_eq!(
        journal
            .read(scope, penelope::core::task::TaskSequence::ZERO)
            .unwrap(),
        None
    );
}

#[test]
fn a_foreign_pid_namespace_cannot_claim_local_runtime_ownership() {
    let (_directory, path, scope, definition) = fixture(b"complete");
    let runtime = ProcessTaskRuntime::open(&path, &helper(), scope, definition).unwrap();
    drop(runtime);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute("UPDATE penelope_host_v1 SET namespace=0", [])
        .unwrap();
    assert!(matches!(
        ProcessTaskRuntime::open(&path, &helper(), scope, definition),
        Err(TaskPortError::Conflict)
    ));
}

#[test]
fn task_pin_and_output_proposal_preserve_scope_definition_and_bytes() {
    let (_directory, path, scope, definition) = fixture(b"complete");
    let mut supervisor = open(&path, scope, definition);
    assert_eq!(
        ProcessTaskRuntime::pinned_task(&path, scope.tenant, scope.task).unwrap(),
        Some((scope, definition))
    );
    supervisor
        .submit(b"complete", 1, BootTaskClock::current())
        .unwrap();
    supervisor.advance(BootTaskClock::current()).unwrap();
    wait_for(|| {
        ProcessTaskRuntime::proposed_output(&path, scope, TaskGeneration::new(1))
            .unwrap()
            .is_some()
    });
    let (reference, bytes) =
        ProcessTaskRuntime::proposed_output(&path, scope, TaskGeneration::new(1))
            .unwrap()
            .unwrap();
    assert_eq!(bytes, b"output");
    reference.verify(&bytes, 1).unwrap();
    assert_eq!(supervisor.projection().status(), TaskStatus::Running);
    let mut foreign = scope;
    foreign.task = TaskId::from_digest(Digest::of(b"other-task"));
    assert!(
        ProcessTaskRuntime::proposed_output(&path, foreign, TaskGeneration::new(1))
            .unwrap()
            .is_none()
    );
    supervisor.poll(BootTaskClock::current()).unwrap();
    assert_eq!(supervisor.projection().status(), TaskStatus::Completed);
}

#[test]
fn worker_deadline_is_enforced_even_after_controller_disappears() {
    let (_directory, path, mut scope, mut definition) = fixture(b"hold");
    definition.recovery.timeout_ticks = 2000;
    scope.definition = definition.fingerprint();
    let mut supervisor = open(&path, scope, definition);
    supervisor
        .submit(b"hold", 1, BootTaskClock::current())
        .unwrap();
    supervisor.advance(BootTaskClock::current()).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    wait_for(|| {
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM penelope_runtime_v1 WHERE pid IS NOT NULL)",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap()
    });
    let pid: u32 = connection
        .query_row("SELECT pid FROM penelope_runtime_v1", [], |row| row.get(0))
        .unwrap();
    drop(supervisor);
    wait_for(
        || match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(text) => text.rsplit_once(')').unwrap().1.split_whitespace().next() == Some("Z"),
            Err(error) => error.kind() == std::io::ErrorKind::NotFound,
        },
    );
    let _waited = rustix::process::waitpid(
        rustix::process::Pid::from_raw(i32::try_from(pid).unwrap()),
        rustix::process::WaitOptions::NOHANG,
    );
}
