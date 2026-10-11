use super::{
    clock::current_tick,
    identity::{birth, boot_id, executable_digest, gate_name, scope_key, secure_file},
    storage::{Authorization, connect, projection},
};
use crate::task_journal::unavailable;
use penelope_core::task::{
    Digest, TaskFault, TaskGeneration, TaskScope, TaskStatus, runtime::TaskPortError,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{fs::File, path::Path, thread, time::Duration};

/// A process-owned generation lease. Acquire it before loading or executing work.
/// Checkpoint calls pause until the exact durable acknowledgement or fencing.
pub struct ProcessWorker {
    connection: Connection,
    key: Vec<u8>,
    authorization: Authorization,
    input: Vec<u8>,
    checkpoint: Option<Vec<u8>>,
    _gate: File,
}

impl ProcessWorker {
    /// Claims a previously committed start under a retained generation lock.
    /// # Errors
    /// Rejects delayed/fenced/duplicate workers, changed executable identity,
    /// missing authorization, uncommitted starts and expired deadlines.
    pub fn claim(
        path: &Path,
        scope: TaskScope,
        generation: TaskGeneration,
    ) -> Result<Self, TaskPortError> {
        let gate = secure_file(
            &path
                .parent()
                .ok_or(TaskPortError::Conflict)?
                .join(gate_name(scope, generation)?),
        )?;
        gate.try_lock().map_err(|_error| TaskPortError::Conflict)?;
        let key = scope_key(scope)?;
        let mut connection = connect(path)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let (encoded, input, checkpoint): (Vec<u8>, Vec<u8>, Option<Vec<u8>>) = transaction.query_row(
            "SELECT authorization,input,checkpoint FROM penelope_runtime_v1
             WHERE scope=?1 AND generation=?2 AND fenced=0 AND pid IS NULL AND length(authorization)<=16384
                AND length(input)<=65536 AND (checkpoint IS NULL OR length(checkpoint)<=65536)
                AND NOT EXISTS(SELECT 1 FROM penelope_fences_v1 WHERE scope=?1 AND generation=?2)",
            params![&key,generation.get()], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)))
            .optional().map_err(unavailable)?.ok_or(TaskPortError::Conflict)?;
        let authorization: Authorization =
            serde_json::from_slice(&encoded).map_err(|_error| TaskPortError::Corrupt)?;
        if let Some(controller_pid) = authorization.controller_pid {
            rustix::process::set_parent_process_death_signal(Some(rustix::process::Signal::KILL))
                .map_err(|_error| TaskPortError::Runtime)?;
            if rustix::process::getppid().and_then(|pid| u32::try_from(pid.as_raw_pid()).ok())
                != Some(controller_pid)
            {
                return Err(TaskPortError::Conflict);
            }
        }
        let pinned_boot: String = transaction
            .query_row(
                "SELECT boot FROM penelope_clocks_v1 WHERE scope=?1",
                params![&key],
                |row| row.get(0),
            )
            .map_err(unavailable)?;
        if pinned_boot != boot_id()? {
            return Err(TaskPortError::Conflict);
        }
        if authorization.scope != scope
            || authorization.generation != generation
            || current_tick() >= authorization.deadline
            || executable_digest(&std::env::current_exe().map_err(super::identity::io_error)?)?
                != authorization.definition.code
        {
            return Err(TaskPortError::Conflict);
        }
        let (projection, _) = projection(&transaction, &key, scope, authorization.definition)?;
        if projection.status() != TaskStatus::Running
            || projection.generation() != generation
            || projection.execution_deadline() != authorization.deadline
        {
            return Err(TaskPortError::Conflict);
        }
        projection
            .input()
            .ok_or(TaskPortError::Corrupt)?
            .verify(&input, authorization.input_schema)
            .map_err(|_error| TaskPortError::Corrupt)?;
        match (projection.checkpoint(), &checkpoint) {
            (Some(reference), Some(bytes)) => reference
                .verify(bytes, authorization.definition.checkpoint_schema)
                .map_err(|_error| TaskPortError::Corrupt)?,
            (None, None) => {}
            _ => return Err(TaskPortError::Corrupt),
        }
        let pid = std::process::id();
        // CPU ticks are process CPU seconds. Lowering the hard limit is
        // irreversible for an unprivileged worker, and covers all its threads.
        let limit = authorization.definition.budget.cpu_ticks;
        if limit != u64::MAX {
            rustix::process::setrlimit(
                rustix::process::Resource::Cpu,
                rustix::process::Rlimit {
                    current: Some(limit),
                    maximum: Some(limit),
                },
            )
            .map_err(|_error| TaskPortError::Runtime)?;
        }
        super::watchdog::arm(authorization.deadline)?;
        let birth = birth(pid)?.ok_or(TaskPortError::Runtime)?;
        transaction
            .execute(
                "UPDATE penelope_runtime_v1 SET pid=?3,birth=?4 WHERE scope=?1 AND generation=?2",
                params![&key, generation.get(), pid, birth],
            )
            .map_err(unavailable)?;
        transaction.commit().map_err(unavailable)?;
        Ok(Self {
            connection,
            key,
            authorization,
            input,
            checkpoint,
            _gate: gate,
        })
    }

    /// Immutable, verified task input.
    #[must_use]
    pub fn input(&self) -> &[u8] {
        &self.input
    }
    /// Last durably committed checkpoint selected by the recovery policy.
    #[must_use]
    pub fn checkpoint_bytes(&self) -> Option<&[u8]> {
        self.checkpoint.as_deref()
    }
    /// Exact execution scope.
    #[must_use]
    pub const fn scope(&self) -> TaskScope {
        self.authorization.scope
    }
    /// Exact execution generation.
    #[must_use]
    pub const fn generation(&self) -> TaskGeneration {
        self.authorization.generation
    }

    fn propose(
        &mut self,
        kind: u32,
        bytes: &[u8],
        schema: u32,
        max: u32,
    ) -> Result<(), TaskPortError> {
        if schema == 0
            || bytes.len() > usize::try_from(max).map_err(|_error| TaskPortError::Exhausted)?
        {
            return Err(TaskPortError::Exhausted);
        }
        if current_tick() >= self.authorization.deadline {
            return Err(TaskPortError::Runtime);
        }
        let affected = self
            .connection
            .execute(
                "UPDATE penelope_runtime_v1 SET kind=?3,proposal=?4,schema=?5
            WHERE scope=?1 AND generation=?2 AND kind=0 AND fenced=0
                AND (acknowledged IS NULL OR acknowledged!=?6)",
                params![
                    &self.key,
                    self.authorization.generation.get(),
                    kind,
                    bytes,
                    schema,
                    Digest::of(bytes).0.as_slice()
                ],
            )
            .map_err(unavailable)?;
        if affected != 1 {
            return Err(TaskPortError::Conflict);
        }
        Ok(())
    }

    /// Proposes a checkpoint and pauses until the supervisor commits and acknowledges it.
    /// # Errors
    /// Returns invalid bytes/schema, storage failure, deadline expiry or fencing.
    pub fn checkpoint(&mut self, bytes: &[u8], schema: u32) -> Result<(), TaskPortError> {
        if schema != self.authorization.definition.checkpoint_schema {
            return Err(TaskPortError::Corrupt);
        }
        self.propose(
            1,
            bytes,
            schema,
            self.authorization.definition.recovery.max_checkpoint_bytes,
        )?;
        loop {
            let (fenced, acknowledged): (bool, Option<Vec<u8>>) = self.connection.query_row(
                "SELECT fenced,acknowledged FROM penelope_runtime_v1 WHERE scope=?1 AND generation=?2",
                params![&self.key,self.authorization.generation.get()], |row| Ok((row.get(0)?,row.get(1)?)))
                .map_err(unavailable)?;
            if fenced || current_tick() >= self.authorization.deadline {
                return Err(TaskPortError::Runtime);
            }
            if acknowledged.as_deref() == Some(Digest::of(bytes).0.as_slice()) {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Persists an output proposal. The supervisor commits it as the completion fact.
    /// # Errors
    /// Returns storage, quota, deadline or fence failures.
    pub fn complete(&mut self, bytes: &[u8], schema: u32) -> Result<(), TaskPortError> {
        self.propose(
            2,
            bytes,
            schema,
            self.authorization.definition.budget.max_output_bytes,
        )
    }

    /// Persists a trusted worker fault; it never proves external-effect failure.
    /// # Errors
    /// Returns storage, quota, deadline or fence failures.
    pub fn fault(&mut self, fault: TaskFault) -> Result<(), TaskPortError> {
        let bytes = serde_json::to_vec(&fault).map_err(|_error| TaskPortError::Corrupt)?;
        self.propose(3, &bytes, 1, 1024)
    }
}
