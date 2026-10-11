use super::identity::{io_error, secure_file};
use crate::{
    SqliteTaskJournal,
    task_journal::{decode, unavailable},
};
use penelope_core::task::{
    TaskDefinition, TaskGeneration, TaskProjection, TaskRecord, TaskScope, runtime::TaskPortError,
};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    os::unix::fs::{DirBuilderExt, MetadataExt},
    path::Path,
    time::Duration,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Authorization {
    pub scope: TaskScope,
    pub generation: TaskGeneration,
    pub definition: TaskDefinition,
    pub deadline: penelope_core::task::TaskTick,
    pub input_schema: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller_pid: Option<u32>,
}

pub(super) fn prepare(
    path: &Path,
    scope: TaskScope,
    definition: TaskDefinition,
) -> Result<(), TaskPortError> {
    let parent = path.parent().ok_or(TaskPortError::Conflict)?;
    if !parent.exists() {
        fs::DirBuilder::new()
            .mode(0o700)
            .create(parent)
            .map_err(io_error)?;
    }
    let metadata = fs::symlink_metadata(parent).map_err(io_error)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o077 != 0
    {
        return Err(TaskPortError::Conflict);
    }
    let file: File = secure_file(path)?;
    file.sync_all().map_err(io_error)?;
    File::open(parent)
        .map_err(io_error)?
        .sync_all()
        .map_err(io_error)?;
    SqliteTaskJournal::open(path, scope, definition)?;
    Ok(())
}

pub(super) fn connect(path: &Path) -> Result<Connection, TaskPortError> {
    // The controller first prepares a private directory. Workers only open it.
    let file = secure_file(path)?;
    drop(file);
    let connection = Connection::open(path).map_err(unavailable)?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(unavailable)?;
    connection
        .execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS penelope_runtime_v1 (
            scope BLOB NOT NULL, generation INTEGER NOT NULL,
            authorization BLOB NOT NULL CHECK(length(authorization)<=16384),
            input BLOB NOT NULL CHECK(length(input)<=65536),
            checkpoint BLOB CHECK(length(checkpoint)<=65536),
            fenced INTEGER NOT NULL DEFAULT 0,
            pid INTEGER, birth TEXT,
            proposal BLOB CHECK(length(proposal)<=65536),
            schema INTEGER, kind INTEGER NOT NULL DEFAULT 0,
            acknowledged BLOB,
            PRIMARY KEY(scope,generation));
        CREATE TABLE IF NOT EXISTS penelope_pins_v1 (
            tenant BLOB NOT NULL, task BLOB NOT NULL, scope BLOB NOT NULL, definition BLOB NOT NULL,
            PRIMARY KEY(tenant,task));
        CREATE TABLE IF NOT EXISTS penelope_clocks_v1 (scope BLOB PRIMARY KEY, boot TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS penelope_host_v1 (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1), machine TEXT NOT NULL,
            boot TEXT NOT NULL, namespace INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS penelope_fences_v1 (
            scope BLOB NOT NULL, generation INTEGER NOT NULL, PRIMARY KEY(scope,generation));",
        )
        .map_err(unavailable)?;
    let boot = super::identity::boot_id()?;
    // Minimal container images may omit machine-id. Such workspaces remain
    // bound to this boot and PID namespace and cannot migrate across reboot.
    let machine = fs::read_to_string("/etc/machine-id")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| value.len() == 32)
        .unwrap_or_else(|| format!("boot:{boot}"));
    let namespace = i64::try_from(fs::metadata("/proc/self/ns/pid").map_err(io_error)?.ino())
        .map_err(|_error| TaskPortError::Runtime)?;
    connection
        .execute(
            "INSERT OR IGNORE INTO penelope_host_v1 VALUES (1,?1,?2,?3)",
            params![&machine, &boot, namespace],
        )
        .map_err(unavailable)?;
    let (prior_machine, prior_boot, prior_namespace): (String, String, i64) = connection
        .query_row(
            "SELECT machine,boot,namespace FROM penelope_host_v1 WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(unavailable)?;
    if prior_machine != machine || (prior_boot == boot && prior_namespace != namespace) {
        return Err(TaskPortError::Conflict);
    }
    if prior_boot != boot {
        connection
            .execute(
                "UPDATE penelope_host_v1 SET boot=?1,namespace=?2 WHERE singleton=1 AND boot=?3",
                params![&boot, namespace, prior_boot],
            )
            .map_err(unavailable)?;
    }
    Ok(connection)
}

pub(super) fn projection(
    connection: &Connection,
    key: &[u8],
    scope: TaskScope,
    definition: TaskDefinition,
) -> Result<(TaskProjection, Option<TaskRecord>), TaskPortError> {
    let mut projection =
        TaskProjection::new(scope, definition).map_err(|_error| TaskPortError::Conflict)?;
    let mut last = None;
    let mut statement = connection
        .prepare(
            "SELECT sequence,record,checksum,length(record) FROM penelope_records_v1
         WHERE scope=?1 ORDER BY sequence LIMIT 4097",
        )
        .map_err(unavailable)?;
    let mut rows = statement.query(params![key]).map_err(unavailable)?;
    while let Some(row) = rows.next().map_err(unavailable)? {
        let sequence: u32 = row.get(0).map_err(|_error| TaskPortError::Corrupt)?;
        let length: u32 = row.get(3).map_err(|_error| TaskPortError::Corrupt)?;
        if length > 16384 || sequence >= definition.budget.max_history_records || sequence >= 4096 {
            return Err(TaskPortError::Corrupt);
        }
        let bytes: Vec<u8> = row.get(1).map_err(unavailable)?;
        let checksum: Vec<u8> = row.get(2).map_err(unavailable)?;
        let record = decode(&bytes, &checksum)?;
        if record.sequence.get() != sequence {
            return Err(TaskPortError::Corrupt);
        }
        projection
            .apply(&record)
            .map_err(|_error| TaskPortError::Corrupt)?;
        last = Some(record);
    }
    Ok((projection, last))
}
