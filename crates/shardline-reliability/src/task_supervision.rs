use std::path::Path;

use penelope::core::task::{TaskDefinition, TaskScope, TaskTick, runtime::TaskRuntime};
use penelope::executor::task::{SupervisorError, TaskSupervisor};

use crate::SqliteTaskJournal;

/// Bounded Penelope execution backed by Shardline's local durable journal.
pub type LocalTaskSupervisor<R, const N: usize> = TaskSupervisor<SqliteTaskJournal, R, N>;

/// Opens the dedicated task journal and restores its pinned execution history.
/// A running generation must be successfully fenced by the host runtime before
/// the supervisor records interruption. No user code is dispatched during replay.
///
/// The host supplies persistent, idempotent runtime fencing and a monotonic
/// tick at least as large as the last committed tick, including after restart.
/// Run this synchronous API on a dedicated thread or blocking pool. Maintenance
/// callers retain their existing barriers and independently reconcile effects.
///
/// # Errors
/// Rejects invalid scope/definition, storage corruption, quota violations,
/// regressed time or unconfirmed fencing. Never dispatch on an error; reopen
/// from durable history after an ambiguous storage or runtime operation.
pub fn restore_local_task_supervisor<R: TaskRuntime, const N: usize>(
    path: &Path,
    runtime: R,
    scope: TaskScope,
    definition: TaskDefinition,
    tick: TaskTick,
) -> Result<LocalTaskSupervisor<R, N>, SupervisorError> {
    let journal = SqliteTaskJournal::open(path, scope, definition)?;
    TaskSupervisor::restore(journal, runtime, scope, definition, tick)
}
