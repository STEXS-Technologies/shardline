use super::{
    clock::{BootTaskClock, current_tick},
    identity::{
        boot_id, executable_digest, gate_name, io_error, process_handle, scope_key, secure_file,
        stop,
    },
    storage::{Authorization, connect, prepare, projection},
};
use crate::task_journal::unavailable;
use penelope::core::task::{
    BlobRef, Digest, TaskDefinition, TaskExecutionEvent, TaskFault, TaskGeneration, TaskScope,
    TaskStatus,
    runtime::{RuntimeProgress, TaskObservation, TaskPortError, TaskRuntime, TaskStart},
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};

type StoredStart = (Vec<u8>, Vec<u8>, Option<Vec<u8>>, Option<u32>);
type StoredProposal = (
    u32,
    Option<Vec<u8>>,
    Option<u32>,
    Option<u32>,
    Option<String>,
);

/// Hosted runtime for a trusted, single-process Linux Shardline worker.
///
/// Each worker claims a generation under a retained OS lock before doing work.
/// Durable fence tombstones reject delayed starts. Fencing uses pidfds and boot/
/// process-birth identity, then proves the worker lock is released. Workers lower
/// their kernel CPU limit before claiming execution; deadlines use boot milliseconds.
/// The trusted worker must not spawn subprocesses or detach work from its process.
/// Issued external effects still require independent application reconciliation.
pub struct ProcessTaskRuntime {
    connection: Connection,
    path: PathBuf,
    executable: PathBuf,
    scope: TaskScope,
    definition: TaskDefinition,
    key: Vec<u8>,
    _controller: File,
    children: BTreeMap<TaskGeneration, Child>,
    capture_output: bool,
    stop_with_controller: bool,
}

impl Drop for ProcessTaskRuntime {
    fn drop(&mut self) {
        if self.capture_output || self.stop_with_controller {
            let generations: Vec<_> = self.children.keys().copied().collect();
            for generation in generations {
                if let Err(error) = self.fence(self.scope, generation) {
                    eprintln!("could not confirm managed maintenance worker termination: {error}");
                }
            }
        }
    }
}

impl ProcessTaskRuntime {
    /// Stops owned workers when this runtime is dropped or its controller dies.
    /// Uses the worker's Linux parent-death signal without capturing its output.
    /// Already issued external effects still require application reconciliation.
    /// Enable before starting work. The default retains reconstructable workers.
    pub const fn stop_with_controller(&mut self) {
        self.stop_with_controller = true;
    }

    /// Captures worker output in private generation-specific files beside the journal.
    /// The caller verifies and streams these files after accepting the signed result.
    pub const fn capture_output(&mut self) {
        self.capture_output = true;
    }
    /// Reads an existing task pin without changing its definition or input.
    /// # Errors
    /// Rejects foreign host/namespace storage and invalid pinned metadata.
    pub fn pinned_task(
        path: &Path,
        tenant: penelope::core::task::TaskTenantId,
        task: penelope::core::task::TaskId,
    ) -> Result<Option<(TaskScope, TaskDefinition)>, TaskPortError> {
        if !path.exists() {
            return Ok(None);
        }
        let connection = connect(path)?;
        let stored: Option<(Vec<u8>, Vec<u8>)> = connection
            .query_row(
                "SELECT scope,definition FROM penelope_pins_v1 WHERE tenant=?1 AND task=?2
                AND length(scope)<=16384 AND length(definition)<=16384",
                params![tenant.digest().0.as_slice(), task.digest().0.as_slice()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(unavailable)?;
        stored
            .map(|(scope, definition)| {
                let scope: TaskScope =
                    serde_json::from_slice(&scope).map_err(|_error| TaskPortError::Corrupt)?;
                let definition: TaskDefinition =
                    serde_json::from_slice(&definition).map_err(|_error| TaskPortError::Corrupt)?;
                penelope::core::task::TaskProjection::new(scope, definition)
                    .map_err(|_error| TaskPortError::Corrupt)?;
                if scope.tenant != tenant || scope.task != task {
                    return Err(TaskPortError::Corrupt);
                }
                Ok((scope, definition))
            })
            .transpose()
    }

    /// Reads a durable output proposal for independent application reconciliation.
    /// This is not a completion fact: authenticate the result and its materialized
    /// artifact before passing it to Penelope as `Reconciliation::Completed`.
    /// # Errors
    /// Returns unavailable, corrupt or foreign-host storage.
    pub fn proposed_output(
        path: &Path,
        scope: TaskScope,
        generation: TaskGeneration,
    ) -> Result<Option<(BlobRef, Vec<u8>)>, TaskPortError> {
        let connection = connect(path)?;
        let stored: Option<(Vec<u8>, u32)> = connection
            .query_row(
                "SELECT proposal,schema FROM penelope_runtime_v1 WHERE scope=?1 AND generation=?2
                AND kind=2 AND length(proposal)<=65536",
                params![scope_key(scope)?, generation.get()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(unavailable)?;
        stored
            .map(|(bytes, schema)| {
                if schema == 0 {
                    return Err(TaskPortError::Corrupt);
                }
                let reference = BlobRef {
                    digest: Digest::of(&bytes),
                    bytes: u32::try_from(bytes.len()).map_err(|_error| TaskPortError::Corrupt)?,
                    schema,
                };
                Ok((reference, bytes))
            })
            .transpose()
    }
    /// Hashes the exact worker executable for a pinned task definition.
    /// # Errors
    /// Returns read failures; code identity cannot be inferred from version text.
    pub fn code_digest(executable: &Path) -> Result<Digest, TaskPortError> {
        executable_digest(executable)
    }

    /// Opens a private local task database and takes exclusive controller ownership.
    /// # Errors
    /// Rejects insecure paths, concurrent controllers, changed task input/definition
    /// or unavailable storage. Fencing remains possible after a code upgrade;
    /// dispatch separately requires the pinned executable digest to match.
    pub fn open(
        path: &Path,
        executable: &Path,
        scope: TaskScope,
        definition: TaskDefinition,
    ) -> Result<Self, TaskPortError> {
        prepare(path, scope, definition)?;
        let parent = path.parent().ok_or(TaskPortError::Conflict)?;
        let name = format!(
            "controller-{}.lock",
            hex::encode(
                Digest::of(
                    &serde_json::to_vec(&(scope.tenant, scope.task))
                        .map_err(|_error| TaskPortError::Corrupt)?
                )
                .0
            )
        );
        let controller = secure_file(&parent.join(name))?;
        controller
            .try_lock()
            .map_err(|_error| TaskPortError::Conflict)?;
        let connection = connect(path)?;
        BootTaskClock::open(path, scope, definition.budget.deadline_tick)?;
        let key = scope_key(scope)?;
        let encoded_definition =
            serde_json::to_vec(&definition).map_err(|_error| TaskPortError::Corrupt)?;
        connection
            .execute(
                "INSERT OR IGNORE INTO penelope_pins_v1 VALUES (?1,?2,?3,?4)",
                params![
                    scope.tenant.digest().0.as_slice(),
                    scope.task.digest().0.as_slice(),
                    &key,
                    &encoded_definition
                ],
            )
            .map_err(unavailable)?;
        let (pinned_scope, pinned_definition): (Vec<u8>, Vec<u8>) = connection
            .query_row(
                "SELECT scope,definition FROM penelope_pins_v1 WHERE tenant=?1 AND task=?2",
                params![
                    scope.tenant.digest().0.as_slice(),
                    scope.task.digest().0.as_slice()
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(unavailable)?;
        if pinned_scope != key || pinned_definition != encoded_definition {
            return Err(TaskPortError::Conflict);
        }
        Ok(Self {
            connection,
            path: path.to_path_buf(),
            executable: executable.to_path_buf(),
            scope,
            definition,
            key,
            _controller: controller,
            children: BTreeMap::new(),
            capture_output: false,
            stop_with_controller: false,
        })
    }

    fn check(&self, scope: TaskScope) -> Result<(), TaskPortError> {
        if scope != self.scope {
            return Err(TaskPortError::Conflict);
        }
        Ok(())
    }

    fn reap(&mut self, generation: TaskGeneration) -> Result<(), TaskPortError> {
        if let Some(child) = self.children.get_mut(&generation)
            && child.try_wait().map_err(io_error)?.is_some()
        {
            self.children.remove(&generation);
        }
        Ok(())
    }
}

impl TaskRuntime for ProcessTaskRuntime {
    fn start(&mut self, request: TaskStart<'_>) -> Result<(), TaskPortError> {
        self.check(request.scope)?;
        if request.definition != self.definition
            || executable_digest(&self.executable)? != self.definition.code
        {
            return Err(TaskPortError::Conflict);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let fenced: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM penelope_fences_v1 WHERE scope=?1 AND generation=?2)",
                params![&self.key, request.generation.get()],
                |row| row.get(0),
            )
            .map_err(unavailable)?;
        if fenced {
            return Err(TaskPortError::Conflict);
        }
        let pinned_boot: String = transaction
            .query_row(
                "SELECT boot FROM penelope_clocks_v1 WHERE scope=?1",
                params![&self.key],
                |row| row.get(0),
            )
            .map_err(unavailable)?;
        if pinned_boot != boot_id()? {
            return Err(TaskPortError::Conflict);
        }
        let (projection, _) = projection(&transaction, &self.key, self.scope, self.definition)?;
        if projection.status() != TaskStatus::Running
            || projection.generation() != request.generation
            || projection.execution_deadline() != request.execution_deadline
            || current_tick() >= request.execution_deadline
        {
            return Err(TaskPortError::Conflict);
        }
        let input = projection.input().ok_or(TaskPortError::Corrupt)?;
        input
            .verify(request.input, request.input_schema)
            .map_err(|_error| TaskPortError::Corrupt)?;
        match (projection.checkpoint(), request.checkpoint) {
            (Some(reference), Some(bytes)) => reference
                .verify(bytes, self.definition.checkpoint_schema)
                .map_err(|_error| TaskPortError::Corrupt)?,
            (None, None) => {}
            _ => return Err(TaskPortError::Corrupt),
        }
        let authorization = Authorization {
            scope: self.scope,
            generation: request.generation,
            definition: self.definition,
            deadline: request.execution_deadline,
            input_schema: request.input_schema,
            controller_pid: (self.capture_output || self.stop_with_controller)
                .then(std::process::id),
        };
        let encoded =
            serde_json::to_vec(&authorization).map_err(|_error| TaskPortError::Corrupt)?;
        let existing: Option<StoredStart> = transaction.query_row(
            "SELECT authorization,input,checkpoint,pid FROM penelope_runtime_v1 WHERE scope=?1 AND generation=?2",
            params![&self.key, request.generation.get()], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)))
            .optional().map_err(unavailable)?;
        if let Some((prior, prior_input, checkpoint, pid)) = existing {
            if prior != encoded
                || prior_input != request.input
                || checkpoint.as_deref() != request.checkpoint
            {
                return Err(TaskPortError::Conflict);
            }
            if pid.is_some() {
                return Ok(());
            }
        } else {
            transaction.execute("INSERT INTO penelope_runtime_v1(scope,generation,authorization,input,checkpoint) VALUES (?1,?2,?3,?4,?5)",
                params![&self.key, request.generation.get(), &encoded, request.input, request.checkpoint]).map_err(unavailable)?;
        }
        transaction.commit().map_err(unavailable)?;
        // The worker rechecks authorization and persistent fences under its
        // generation lock. A crash in this spawn window cannot bypass fencing.
        let parent = self.path.parent().ok_or(TaskPortError::Conflict)?;
        let stdout = if self.capture_output {
            Stdio::from(secure_file(
                &parent.join(format!("stdout-{}", request.generation.get())),
            )?)
        } else {
            Stdio::null()
        };
        let stderr = if self.capture_output {
            Stdio::from(secure_file(
                &parent.join(format!("stderr-{}", request.generation.get())),
            )?)
        } else {
            Stdio::null()
        };
        let child = Command::new(&self.executable)
            .arg("maintenance-worker")
            .arg("--journal")
            .arg(&self.path)
            .arg("--scope")
            .arg(hex::encode(&self.key))
            .arg("--generation")
            .arg(request.generation.get().to_string())
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .map_err(io_error)?;
        if let Some(mut previous) = self.children.insert(request.generation, child) {
            let _status = previous.try_wait().map_err(io_error)?;
        }
        Ok(())
    }

    fn poll(
        &mut self,
        scope: TaskScope,
        generation: TaskGeneration,
        buffer: &mut [u8],
    ) -> Result<TaskObservation, TaskPortError> {
        self.check(scope)?;
        self.reap(generation)?;
        let (kind, bytes, schema, pid, birth): StoredProposal = self
            .connection
            .query_row(
                "SELECT kind,proposal,schema,pid,birth FROM penelope_runtime_v1
                WHERE scope=?1 AND generation=?2 AND (proposal IS NULL OR length(proposal)<=65536)",
                params![&self.key, generation.get()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()
            .map_err(unavailable)?
            .ok_or(TaskPortError::Corrupt)?;
        let progress = match kind {
            0 => match (pid, birth) {
                (Some(pid), Some(birth)) if process_handle(pid, &birth)?.is_none() => {
                    RuntimeProgress::Fault(TaskFault::Crash)
                }
                (None, None) => RuntimeProgress::Pending,
                (Some(_), Some(_)) => RuntimeProgress::Pending,
                _ => return Err(TaskPortError::Corrupt),
            },
            1 | 2 => {
                let bytes = bytes.ok_or(TaskPortError::Corrupt)?;
                let count =
                    u32::try_from(bytes.len()).map_err(|_error| TaskPortError::Exhausted)?;
                buffer
                    .get_mut(..bytes.len())
                    .ok_or(TaskPortError::Exhausted)?
                    .copy_from_slice(&bytes);
                let schema = schema.ok_or(TaskPortError::Corrupt)?;
                if kind == 1 {
                    RuntimeProgress::Checkpoint {
                        bytes: count,
                        schema,
                    }
                } else {
                    RuntimeProgress::Complete {
                        bytes: count,
                        schema,
                    }
                }
            }
            3 => RuntimeProgress::Fault(
                serde_json::from_slice(&bytes.ok_or(TaskPortError::Corrupt)?)
                    .map_err(|_error| TaskPortError::Corrupt)?,
            ),
            _ => return Err(TaskPortError::Corrupt),
        };
        Ok(TaskObservation {
            scope,
            generation,
            progress,
        })
    }

    fn acknowledge_checkpoint(
        &mut self,
        scope: TaskScope,
        generation: TaskGeneration,
        checkpoint: BlobRef,
    ) -> Result<(), TaskPortError> {
        self.check(scope)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let (projection, last) = projection(&transaction, &self.key, self.scope, self.definition)?;
        if projection.status() != TaskStatus::Running
            || projection.generation() != generation
            || last.is_none_or(|record| {
                record.event != TaskExecutionEvent::CheckpointCommitted { checkpoint }
            })
        {
            return Err(TaskPortError::Conflict);
        }
        let bytes: Vec<u8> = transaction.query_row("SELECT proposal FROM penelope_runtime_v1
            WHERE scope=?1 AND generation=?2 AND kind=1 AND fenced=0 AND schema=?3 AND length(proposal)<=65536",
            params![&self.key,generation.get(),checkpoint.schema], |row| row.get(0)).optional().map_err(unavailable)?
            .ok_or(TaskPortError::Conflict)?;
        checkpoint
            .verify(&bytes, self.definition.checkpoint_schema)
            .map_err(|_error| TaskPortError::Corrupt)?;
        transaction.execute("UPDATE penelope_runtime_v1 SET kind=0,proposal=NULL,schema=NULL,acknowledged=?3 WHERE scope=?1 AND generation=?2",
            params![&self.key,generation.get(),checkpoint.digest.0.as_slice()]).map_err(unavailable)?;
        transaction.commit().map_err(unavailable)?;
        Ok(())
    }

    fn fence(&mut self, scope: TaskScope, generation: TaskGeneration) -> Result<(), TaskPortError> {
        self.check(scope)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO penelope_fences_v1 VALUES (?1,?2)",
                params![&self.key, generation.get()],
            )
            .map_err(unavailable)?;
        transaction
            .execute(
                "UPDATE penelope_runtime_v1 SET fenced=1 WHERE scope=?1 AND generation=?2",
                params![&self.key, generation.get()],
            )
            .map_err(unavailable)?;
        let process: Option<(Option<u32>, Option<String>)> = transaction
            .query_row(
                "SELECT pid,birth FROM penelope_runtime_v1 WHERE scope=?1 AND generation=?2",
                params![&self.key, generation.get()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(unavailable)?;
        transaction.commit().map_err(unavailable)?;
        if let Some((Some(pid), Some(birth))) = process {
            stop(pid, &birth)?;
        }
        let gate = secure_file(
            &self
                .path
                .parent()
                .ok_or(TaskPortError::Conflict)?
                .join(gate_name(scope, generation)?),
        )?;
        for _attempt in 0..200 {
            match gate.try_lock() {
                Ok(()) => {
                    self.reap(generation)?;
                    return Ok(());
                }
                Err(std::fs::TryLockError::WouldBlock) => thread::sleep(Duration::from_millis(5)),
                Err(std::fs::TryLockError::Error(error)) => return Err(io_error(error)),
            }
        }
        Err(TaskPortError::Runtime)
    }
}
