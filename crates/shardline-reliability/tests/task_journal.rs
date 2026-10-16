#![cfg(feature = "task-journal")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
use penelope::core::task::{
    BlobRef, Digest, EffectContract, ExecutionBudget, Reconciliation, RecoveryMode, TaskDefinition,
    TaskExecutionEvent, TaskGeneration, TaskId, TaskInputDigest, TaskProjection, TaskRecord,
    TaskRecoveryPolicy, TaskScope, TaskSequence, TaskStatus, TaskTenantId, TaskTick,
    runtime::{
        RuntimeProgress, TaskJournal, TaskObservation, TaskPortError, TaskRuntime, TaskStart,
    },
};
use penelope::executor::task::TaskSupervisor;
use shardline_reliability::{SqliteTaskJournal, restore_local_task_supervisor};
use std::{
    path::Path,
    sync::{Arc, Barrier, Mutex},
};

fn fixture() -> (TaskScope, TaskDefinition, BlobRef) {
    let definition = TaskDefinition {
        code: Digest::of(b"shardline-test-task-v1"),
        checkpoint_schema: 1,
        effects: EffectContract::ReconcileRequired,
        recovery: TaskRecoveryPolicy {
            mode: RecoveryMode::ReconcileBeforeRestart,
            max_attempts: 3,
            timeout_ticks: 20,
            restart_delay_ticks: 1,
            max_checkpoint_bytes: 128,
        },
        budget: ExecutionBudget {
            max_input_bytes: 128,
            max_output_bytes: 128,
            max_history_records: 32,
            max_checkpoint_commits: 4,
            cpu_ticks: 10,
            deadline_tick: TaskTick::new(100),
        },
    };
    let input = BlobRef {
        digest: Digest::of(b"input"),
        bytes: 5,
        schema: 1,
    };
    let scope = TaskScope {
        tenant: TaskTenantId::from_digest(Digest::of(b"tenant")),
        task: TaskId::from_digest(Digest::of(b"task")),
        definition: definition.fingerprint(),
        input: TaskInputDigest::from_digest(input.digest),
    };
    (scope, definition, input)
}

fn intent(scope: TaskScope, definition: TaskDefinition, input: BlobRef) -> TaskRecord {
    TaskProjection::new(scope, definition)
        .unwrap()
        .propose(
            TaskExecutionEvent::IntentCommitted { definition, input },
            TaskGeneration::ZERO,
            TaskTick::ZERO,
        )
        .unwrap()
}

fn started(path: &Path) -> (SqliteTaskJournal, TaskProjection) {
    let (scope, definition, input) = fixture();
    let mut journal = SqliteTaskJournal::open(path, scope, definition).unwrap();
    let record = intent(scope, definition, input);
    journal.commit(&record, Some(b"input")).unwrap();
    let mut projection = TaskProjection::new(scope, definition).unwrap();
    projection.apply(&record).unwrap();
    let start = projection.next(TaskTick::new(1)).unwrap().unwrap();
    journal.commit(&start, None).unwrap();
    projection.apply(&start).unwrap();
    (journal, projection)
}

#[test]
fn read_only_journal_preserves_evidence_and_rejects_creation_and_commit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.sqlite");
    let (scope, definition, input) = fixture();
    assert!(SqliteTaskJournal::open_read_only(&path, scope, definition).is_err());
    assert!(!path.exists());
    let empty = directory.path().join("empty.sqlite");
    drop(rusqlite::Connection::open(&empty).unwrap());
    let empty_before = std::fs::read(&empty).unwrap();
    let mut empty_reader = SqliteTaskJournal::open_read_only(&empty, scope, definition).unwrap();
    assert!(empty_reader.read(scope, TaskSequence::ZERO).is_err());
    drop(empty_reader);
    assert_eq!(std::fs::read(&empty).unwrap(), empty_before);
    let (writer, _) = started(&path);
    drop(writer);
    #[cfg(unix)]
    {
        let alias = directory.path().join("alias.sqlite");
        std::os::unix::fs::symlink(&path, &alias).unwrap();
        assert!(SqliteTaskJournal::open_read_only(&alias, scope, definition).is_err());
    }
    let before = std::fs::read(&path).unwrap();
    let mut reader = SqliteTaskJournal::open_read_only(&path, scope, definition).unwrap();
    let record = intent(scope, definition, input);
    assert_eq!(
        reader.read(scope, TaskSequence::ZERO).unwrap(),
        Some(record)
    );
    let mut buffer = [0; 128];
    assert_eq!(reader.read_blob(scope, input, &mut buffer).unwrap(), 5);
    assert_eq!(&buffer[..5], b"input");
    assert!(reader.commit(&record, Some(b"input")).is_err());
    drop(reader);
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

#[test]
fn reopen_preserves_records_and_exact_blobs_and_retry_is_idempotent() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.sqlite");
    let (scope, definition, input) = fixture();
    let record = intent(scope, definition, input);
    {
        let mut journal = SqliteTaskJournal::open(&path, scope, definition).unwrap();
        journal.commit(&record, Some(b"input")).unwrap();
        assert_eq!(journal.commit(&record, None).unwrap(), record);
    }
    let mut journal = SqliteTaskJournal::open(&path, scope, definition).unwrap();
    assert_eq!(
        journal.read(scope, TaskSequence::ZERO).unwrap(),
        Some(record)
    );
    assert_eq!(journal.read(scope, TaskSequence::new(1)).unwrap(), None);
    assert_eq!(
        journal.read_blob(scope, input, &mut [0; 4]),
        Err(TaskPortError::Exhausted)
    );
    let mut bytes = [0; 8];
    assert_eq!(journal.read_blob(scope, input, &mut bytes).unwrap(), 5);
    assert_eq!(&bytes[..5], b"input");
}

#[test]
fn invalid_blob_rolls_back_history_and_conflicting_generation_changes_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.sqlite");
    let (scope, definition, input) = fixture();
    let record = intent(scope, definition, input);
    let mut journal = SqliteTaskJournal::open(&path, scope, definition).unwrap();
    assert_eq!(
        journal.commit(&record, Some(b"wrong")),
        Err(TaskPortError::Corrupt)
    );
    assert_eq!(journal.read(scope, TaskSequence::ZERO).unwrap(), None);
    assert_eq!(
        journal.read_blob(scope, input, &mut [0; 128]),
        Err(TaskPortError::Corrupt)
    );
    journal.commit(&record, Some(b"input")).unwrap();
    let mut projection = TaskProjection::new(scope, definition).unwrap();
    projection.apply(&record).unwrap();
    let mut start = projection.next(TaskTick::new(1)).unwrap().unwrap();
    start.generation = TaskGeneration::new(2);
    assert_eq!(journal.commit(&start, None), Err(TaskPortError::Conflict));
    assert_eq!(journal.read(scope, TaskSequence::new(1)).unwrap(), None);
}

#[test]
fn two_connections_arbitrate_concurrent_checkpoint_commits() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.sqlite");
    let (_, projection) = started(&path);
    let barrier = Arc::new(Barrier::new(2));
    let mut threads = Vec::new();
    for bytes in [b"first".as_slice(), b"second".as_slice()] {
        let path = path.clone();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            let mut journal =
                SqliteTaskJournal::open(&path, projection.scope(), projection.definition())
                    .unwrap();
            let checkpoint = BlobRef {
                digest: Digest::of(bytes),
                bytes: u32::try_from(bytes.len()).unwrap(),
                schema: 1,
            };
            let record = projection
                .propose(
                    TaskExecutionEvent::CheckpointCommitted { checkpoint },
                    projection.generation(),
                    TaskTick::new(2),
                )
                .unwrap();
            barrier.wait();
            (journal.commit(&record, Some(bytes)), checkpoint)
        }));
    }
    let results: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(
        results.iter().filter(|(result, _)| result.is_ok()).count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|(result, _)| *result == Err(TaskPortError::Conflict))
            .count(),
        1
    );
    let mut journal =
        SqliteTaskJournal::open(&path, projection.scope(), projection.definition()).unwrap();
    for (result, reference) in results {
        if result.is_err() {
            assert_eq!(
                journal.read_blob(projection.scope(), reference, &mut [0; 128]),
                Err(TaskPortError::Corrupt)
            );
        }
    }
}

#[test]
fn missing_prefix_and_modified_checksum_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.sqlite");
    let (mut journal, projection) = started(&path);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE penelope_records_v1 SET checksum=zeroblob(32) WHERE sequence=1",
            [],
        )
        .unwrap();
    assert_eq!(
        journal.read(projection.scope(), TaskSequence::new(1)),
        Err(TaskPortError::Corrupt)
    );
    connection
        .execute("DELETE FROM penelope_records_v1 WHERE sequence=0", [])
        .unwrap();
    assert_eq!(
        journal.read(projection.scope(), TaskSequence::ZERO),
        Err(TaskPortError::Corrupt)
    );
    let next = projection
        .propose(
            TaskExecutionEvent::CheckpointCommitted {
                checkpoint: BlobRef {
                    digest: Digest::of(b"checkpoint"),
                    bytes: 10,
                    schema: 1,
                },
            },
            projection.generation(),
            TaskTick::new(2),
        )
        .unwrap();
    assert_eq!(
        journal.commit(&next, Some(b"checkpoint")),
        Err(TaskPortError::Corrupt)
    );
}

#[test]
fn scope_and_definition_cannot_be_substituted() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.sqlite");
    let (scope, definition, input) = fixture();
    let mut journal = SqliteTaskJournal::open(&path, scope, definition).unwrap();
    let mut other = scope;
    other.tenant = TaskTenantId::from_digest(Digest::of(b"other"));
    assert_eq!(
        journal.commit(&intent(other, definition, input), Some(b"input")),
        Err(TaskPortError::Conflict)
    );
    assert_eq!(
        journal.read(other, TaskSequence::ZERO),
        Err(TaskPortError::Conflict)
    );
    let mut changed = definition;
    changed.recovery.max_attempts = 2;
    assert!(matches!(
        SqliteTaskJournal::open(&path, scope, changed),
        Err(TaskPortError::Conflict)
    ));
}

#[test]
fn failure_after_blob_insert_rolls_back_both_rows() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.sqlite");
    let (mut journal, projection) = started(&path);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_checkpoint BEFORE INSERT ON penelope_records_v1
        WHEN NEW.sequence=2 BEGIN SELECT RAISE(ABORT, 'injected write failure'); END;",
        )
        .unwrap();
    let checkpoint = BlobRef {
        digest: Digest::of(b"checkpoint"),
        bytes: 10,
        schema: 1,
    };
    let record = projection
        .propose(
            TaskExecutionEvent::CheckpointCommitted { checkpoint },
            projection.generation(),
            TaskTick::new(2),
        )
        .unwrap();
    assert_eq!(
        journal.commit(&record, Some(b"checkpoint")),
        Err(TaskPortError::Unavailable)
    );
    drop(journal);
    let mut reopened =
        SqliteTaskJournal::open(&path, projection.scope(), projection.definition()).unwrap();
    assert_eq!(
        reopened
            .read(projection.scope(), TaskSequence::new(2))
            .unwrap(),
        None
    );
    assert_eq!(
        reopened.read_blob(projection.scope(), checkpoint, &mut [0; 128]),
        Err(TaskPortError::Corrupt)
    );
    connection
        .execute_batch("DROP TRIGGER reject_checkpoint;")
        .unwrap();
    reopened.commit(&record, Some(b"checkpoint")).unwrap();
}

#[test]
fn corrupted_immutable_blob_rejects_reads_and_duplicate_receipts() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.sqlite");
    let (scope, definition, input) = fixture();
    let record = intent(scope, definition, input);
    let mut journal = SqliteTaskJournal::open(&path, scope, definition).unwrap();
    journal.commit(&record, Some(b"input")).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE penelope_blobs_v1 SET bytes=?1",
            [b"wrong".as_slice()],
        )
        .unwrap();
    assert_eq!(
        journal.read_blob(scope, input, &mut [0; 128]),
        Err(TaskPortError::Corrupt)
    );
    assert_eq!(
        journal.commit(&record, Some(b"input")),
        Err(TaskPortError::Corrupt)
    );
}

#[test]
fn excessive_host_quotas_are_rejected_before_opening_database() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.sqlite");
    let (mut scope, mut definition, _) = fixture();
    definition.budget.max_history_records = 4097;
    scope.definition = definition.fingerprint();
    assert!(matches!(
        SqliteTaskJournal::open(&path, scope, definition),
        Err(TaskPortError::Exhausted)
    ));
    assert!(!path.exists());
}

#[derive(Default)]
struct RuntimeState {
    starts: u32,
    acknowledgements: u32,
    fenced: Vec<TaskGeneration>,
    fail_fence: bool,
}
struct PausedRuntime(Arc<Mutex<RuntimeState>>);
impl TaskRuntime for PausedRuntime {
    fn start(&mut self, request: TaskStart<'_>) -> Result<(), TaskPortError> {
        let mut state = self.0.lock().map_err(|_error| TaskPortError::Runtime)?;
        if state.fenced.contains(&request.generation) {
            return Err(TaskPortError::Conflict);
        }
        state.starts = state
            .starts
            .checked_add(1)
            .ok_or(TaskPortError::Exhausted)?;
        Ok(())
    }
    fn poll(
        &mut self,
        scope: TaskScope,
        generation: TaskGeneration,
        _buffer: &mut [u8],
    ) -> Result<TaskObservation, TaskPortError> {
        Ok(TaskObservation {
            scope,
            generation,
            progress: RuntimeProgress::Pending,
        })
    }
    fn acknowledge_checkpoint(
        &mut self,
        _scope: TaskScope,
        _generation: TaskGeneration,
        _checkpoint: BlobRef,
    ) -> Result<(), TaskPortError> {
        let mut state = self.0.lock().map_err(|_error| TaskPortError::Runtime)?;
        state.acknowledgements = state
            .acknowledgements
            .checked_add(1)
            .ok_or(TaskPortError::Exhausted)?;
        Ok(())
    }
    fn fence(
        &mut self,
        _scope: TaskScope,
        generation: TaskGeneration,
    ) -> Result<(), TaskPortError> {
        let mut state = self.0.lock().map_err(|_error| TaskPortError::Runtime)?;
        if state.fail_fence {
            return Err(TaskPortError::Runtime);
        }
        state.fenced.push(generation);
        Ok(())
    }
}

#[test]
fn real_supervisor_reopen_fences_running_generation_and_unknown_effects_do_not_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.sqlite");
    let (scope, definition, _) = fixture();
    let state = Arc::new(Mutex::new(RuntimeState::default()));
    let journal = SqliteTaskJournal::open(&path, scope, definition).unwrap();
    let mut supervisor = TaskSupervisor::<_, _, 128>::restore(
        journal,
        PausedRuntime(state.clone()),
        scope,
        definition,
        TaskTick::ZERO,
    )
    .unwrap();
    supervisor.submit(b"input", 1, TaskTick::ZERO).unwrap();
    supervisor.advance(TaskTick::new(1)).unwrap();
    drop(supervisor);
    let mut restored = restore_local_task_supervisor::<_, 128>(
        &path,
        PausedRuntime(state.clone()),
        scope,
        definition,
        TaskTick::new(2),
    )
    .unwrap();
    assert_eq!(restored.projection().status(), TaskStatus::Faulted);
    assert_eq!(state.lock().unwrap().fenced, vec![TaskGeneration::new(1)]);
    restored.advance(TaskTick::new(3)).unwrap();
    restored
        .reconcile(
            scope,
            TaskGeneration::new(1),
            Reconciliation::Unknown {
                evidence: Digest::of(b"lost-ack"),
            },
            None,
            TaskTick::new(4),
        )
        .unwrap();
    assert!(!restored.advance(TaskTick::new(5)).unwrap());
    assert_eq!(state.lock().unwrap().starts, 1);
    assert_eq!(restored.projection().status(), TaskStatus::Reconciling);
}

#[test]
fn unconfirmed_fence_cannot_record_interruption_or_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.sqlite");
    let (journal, projection) = started(&path);
    let state = Arc::new(Mutex::new(RuntimeState {
        fail_fence: true,
        ..RuntimeState::default()
    }));
    assert!(
        TaskSupervisor::<_, _, 128>::restore(
            journal,
            PausedRuntime(state.clone()),
            projection.scope(),
            projection.definition(),
            TaskTick::new(2)
        )
        .is_err()
    );
    let mut reopened =
        SqliteTaskJournal::open(&path, projection.scope(), projection.definition()).unwrap();
    assert_eq!(
        reopened
            .read(projection.scope(), TaskSequence::new(2))
            .unwrap(),
        None
    );
    assert_eq!(state.lock().unwrap().starts, 0);
}

#[test]
fn checkpoint_acknowledgement_requires_durable_commit_and_failure_poisons_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.sqlite");
    let (scope, definition, _) = fixture();
    let state = Arc::new(Mutex::new(RuntimeState::default()));
    let mut supervisor = restore_local_task_supervisor::<_, 128>(
        &path,
        PausedRuntime(state.clone()),
        scope,
        definition,
        TaskTick::ZERO,
    )
    .unwrap();
    supervisor.submit(b"input", 1, TaskTick::ZERO).unwrap();
    supervisor.advance(TaskTick::new(1)).unwrap();
    let observation = TaskObservation {
        scope,
        generation: TaskGeneration::new(1),
        progress: RuntimeProgress::Checkpoint {
            bytes: 10,
            schema: 1,
        },
    };
    supervisor
        .observe(observation, b"checkpoint", TaskTick::new(2))
        .unwrap();
    assert_eq!(state.lock().unwrap().acknowledgements, 1);
    let mut reader = SqliteTaskJournal::open(&path, scope, definition).unwrap();
    assert!(reader.read(scope, TaskSequence::new(2)).unwrap().is_some());
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_checkpoint BEFORE INSERT ON penelope_records_v1
        WHEN NEW.sequence=3 BEGIN SELECT RAISE(ABORT, 'injected write failure'); END;",
        )
        .unwrap();
    assert!(
        supervisor
            .observe(observation, b"other-data", TaskTick::new(3))
            .is_err()
    );
    assert_eq!(state.lock().unwrap().acknowledgements, 1);
    assert_eq!(reader.read(scope, TaskSequence::new(3)).unwrap(), None);
    assert!(matches!(
        supervisor.advance(TaskTick::new(4)),
        Err(penelope::executor::task::SupervisorError::ReconstructionRequired)
    ));
    assert_eq!(state.lock().unwrap().starts, 1);
}

#[test]
fn regressed_restore_clock_does_not_fence_or_append_history() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tasks.sqlite");
    let (_, projection) = started(&path);
    let state = Arc::new(Mutex::new(RuntimeState::default()));
    assert!(
        restore_local_task_supervisor::<_, 128>(
            &path,
            PausedRuntime(state.clone()),
            projection.scope(),
            projection.definition(),
            TaskTick::ZERO
        )
        .is_err()
    );
    let mut reader =
        SqliteTaskJournal::open(&path, projection.scope(), projection.definition()).unwrap();
    assert_eq!(
        reader
            .read(projection.scope(), TaskSequence::new(2))
            .unwrap(),
        None
    );
    assert!(state.lock().unwrap().fenced.is_empty());
}
