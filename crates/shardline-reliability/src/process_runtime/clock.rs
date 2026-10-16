use super::{
    identity::{boot_id, scope_key},
    storage::connect,
};
use crate::task_journal::unavailable;
use penelope::core::task::{
    TaskScope, TaskTick,
    runtime::{TaskClock, TaskPortError},
};
use rusqlite::{Connection, params};
use rustix::time::{ClockId, clock_gettime};
use std::path::Path;

/// Milliseconds since this Linux boot, pinned to the task's original boot.
/// Process restarts retain time; a host reboot conservatively expires the task.
pub struct BootTaskClock {
    expired: bool,
    deadline: TaskTick,
}

pub(super) fn current_tick() -> TaskTick {
    let stamp = clock_gettime(ClockId::Boottime);
    let milliseconds = u64::try_from(stamp.tv_sec)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1000))
        .and_then(|seconds| {
            u64::try_from(stamp.tv_nsec)
                .ok()
                .and_then(|nanos| seconds.checked_add(nanos / 1_000_000))
        })
        .unwrap_or(u64::MAX);
    TaskTick::new(milliseconds)
}

impl BootTaskClock {
    /// Returns the current boot-monotonic millisecond tick for a new definition.
    #[must_use]
    pub fn current() -> TaskTick {
        current_tick()
    }

    /// Opens a clock bound to the exact task scope and absolute deadline.
    /// # Errors
    /// Returns unavailable storage or invalid task-clock metadata.
    pub fn open(path: &Path, scope: TaskScope, deadline: TaskTick) -> Result<Self, TaskPortError> {
        let connection: Connection = connect(path)?;
        let key = scope_key(scope)?;
        let current_boot = boot_id()?;
        connection
            .execute(
                "INSERT OR IGNORE INTO penelope_clocks_v1(scope,boot) VALUES (?1,?2)",
                params![&key, &current_boot],
            )
            .map_err(unavailable)?;
        let original: String = connection
            .query_row(
                "SELECT boot FROM penelope_clocks_v1 WHERE scope=?1",
                params![&key],
                |row| row.get(0),
            )
            .map_err(unavailable)?;
        Ok(Self {
            expired: original != current_boot,
            deadline,
        })
    }
}

impl TaskClock for BootTaskClock {
    fn now(&self) -> TaskTick {
        if self.expired {
            self.deadline.max(current_tick())
        } else {
            current_tick()
        }
    }
}
