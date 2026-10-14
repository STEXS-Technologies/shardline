//! Opt-in local durable storage for Penelope's synchronous task port.
use std::{path::Path, time::Duration};

use penelope_core::task::{
    BlobRef, Digest, Reconciliation, TaskDefinition, TaskExecutionEvent, TaskProjection,
    TaskRecord, TaskScope, TaskSequence,
    runtime::{TaskJournal, TaskPortError},
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};

const MAX_BLOB_BYTES: u32 = 64 * 1024;
const MAX_RECORD_BYTES: usize = 16 * 1024;
const MAX_HISTORY_RECORDS: u32 = 4096;

/// SQLite-backed journal pinned to an exact scope and complete task definition.
///
/// Use a dedicated, access-controlled database, separate from Shardline metadata.
/// WAL and FULL synchronization make records and immutable blobs one durable
/// transaction. Checksums detect accidental damage; filesystem access control
/// supplies authenticity. No runtime, clock, fencing or effect reconciliation is
/// supplied by this storage adapter. Run this synchronous port on a dedicated
/// thread or blocking pool when composing it with an async service.
pub struct SqliteTaskJournal {
    connection: Connection,
    scope: TaskScope,
    definition: TaskDefinition,
    key: Vec<u8>,
}

pub(crate) fn unavailable(_error: rusqlite::Error) -> TaskPortError {
    TaskPortError::Unavailable
}

pub(crate) fn decode(bytes: &[u8], checksum: &[u8]) -> Result<TaskRecord, TaskPortError> {
    if bytes.len() > MAX_RECORD_BYTES || Digest::of(bytes).0.as_slice() != checksum {
        return Err(TaskPortError::Corrupt);
    }
    serde_json::from_slice(bytes).map_err(|_error| TaskPortError::Corrupt)
}

const fn reference(record: &TaskRecord) -> Option<BlobRef> {
    match record.event {
        TaskExecutionEvent::IntentCommitted { input, .. } => Some(input),
        TaskExecutionEvent::CheckpointCommitted { checkpoint }
        | TaskExecutionEvent::CheckpointMigrated { checkpoint, .. } => Some(checkpoint),
        TaskExecutionEvent::Completed { output }
        | TaskExecutionEvent::Reconciled {
            result: Reconciliation::Completed { output, .. },
        } => Some(output),
        TaskExecutionEvent::Started
        | TaskExecutionEvent::FaultObserved { .. }
        | TaskExecutionEvent::ReconciliationRequired
        | TaskExecutionEvent::Reconciled { .. }
        | TaskExecutionEvent::RecoveryPlanned { .. }
        | TaskExecutionEvent::Restarted
        | TaskExecutionEvent::RecoveryExhausted => None,
    }
}

fn verify_blob(reference: BlobRef, bytes: &[u8]) -> Result<(), TaskPortError> {
    if reference.schema == 0 || reference.bytes > MAX_BLOB_BYTES {
        return Err(TaskPortError::Corrupt);
    }
    reference
        .verify(bytes, reference.schema)
        .map_err(|_error| TaskPortError::Corrupt)
}

impl SqliteTaskJournal {
    /// Opens an opt-in task database without touching existing metadata schemas.
    ///
    /// # Errors
    /// Rejects invalid definitions, bounds above 64 KiB per blob or 4096 history
    /// records, scope mismatches and unavailable storage.
    pub fn open(
        path: &Path,
        scope: TaskScope,
        definition: TaskDefinition,
    ) -> Result<Self, TaskPortError> {
        Self::open_with_mode(path, scope, definition, false)
    }

    /// Opens existing evidence for inspection without creating tables or changing
    /// journal settings. Commits through this handle fail without writing.
    ///
    /// # Errors
    /// Rejects the same invalid definitions and bounds as [`Self::open`], and
    /// missing or unreadable databases. Reads still verify records and blobs.
    pub fn open_read_only(
        path: &Path,
        scope: TaskScope,
        definition: TaskDefinition,
    ) -> Result<Self, TaskPortError> {
        Self::open_with_mode(path, scope, definition, true)
    }

    fn open_with_mode(
        path: &Path,
        scope: TaskScope,
        definition: TaskDefinition,
        read_only: bool,
    ) -> Result<Self, TaskPortError> {
        TaskProjection::new(scope, definition).map_err(|_error| TaskPortError::Conflict)?;
        if definition.budget.max_input_bytes > MAX_BLOB_BYTES
            || definition.budget.max_output_bytes > MAX_BLOB_BYTES
            || definition.recovery.max_checkpoint_bytes > MAX_BLOB_BYTES
            || definition.budget.max_history_records > MAX_HISTORY_RECORDS
        {
            return Err(TaskPortError::Exhausted);
        }
        let connection = if read_only {
            Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        } else {
            Connection::open(path)
        }
        .map_err(unavailable)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(unavailable)?;
        if !read_only {
            connection
                .execute_batch(
                    "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
             CREATE TABLE IF NOT EXISTS penelope_records_v1 (
                scope BLOB NOT NULL, sequence INTEGER NOT NULL,
                record BLOB NOT NULL CHECK(length(record)<=16384),
                checksum BLOB NOT NULL CHECK(length(checksum)=32),
                PRIMARY KEY(scope, sequence));
             CREATE TABLE IF NOT EXISTS penelope_blobs_v1 (
                scope BLOB NOT NULL, digest BLOB NOT NULL CHECK(length(digest)=32),
                bytes BLOB NOT NULL CHECK(length(bytes)<=65536),
                PRIMARY KEY(scope, digest));",
                )
                .map_err(unavailable)?;
        }
        let key = serde_json::to_vec(&scope).map_err(|_error| TaskPortError::Corrupt)?;
        Ok(Self {
            connection,
            scope,
            definition,
            key,
        })
    }

    fn check_scope(&self, scope: TaskScope) -> Result<(), TaskPortError> {
        if scope != self.scope {
            return Err(TaskPortError::Conflict);
        }
        Ok(())
    }
}

impl TaskJournal for SqliteTaskJournal {
    fn read(
        &mut self,
        scope: TaskScope,
        sequence: TaskSequence,
    ) -> Result<Option<TaskRecord>, TaskPortError> {
        self.check_scope(scope)?;
        // One read transaction ensures that a missing row and the gap check
        // observe the same authoritative history, even with concurrent commits.
        let transaction = self.connection.transaction().map_err(unavailable)?;
        let stored: Option<(Vec<u8>, Vec<u8>)> = transaction
            .query_row(
                "SELECT record, checksum FROM penelope_records_v1
             WHERE scope=?1 AND sequence=?2 AND length(record)<=16384",
                params![&self.key, sequence.get()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(unavailable)?;
        if let Some((bytes, checksum)) = stored {
            let record = decode(&bytes, &checksum)?;
            if record.scope != scope || record.sequence != sequence {
                return Err(TaskPortError::Corrupt);
            }
            Ok(Some(record))
        } else {
            let later: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM penelope_records_v1
                 WHERE scope=?1 AND sequence>=?2)",
                    params![&self.key, sequence.get()],
                    |row| row.get(0),
                )
                .map_err(unavailable)?;
            if later {
                Err(TaskPortError::Corrupt)
            } else {
                Ok(None)
            }
        }
    }

    fn commit(
        &mut self,
        record: &TaskRecord,
        blob: Option<&[u8]>,
    ) -> Result<TaskRecord, TaskPortError> {
        self.check_scope(record.scope)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(unavailable)?;
        let mut projection = TaskProjection::new(self.scope, self.definition)
            .map_err(|_error| TaskPortError::Conflict)?;
        let mut existing = None;
        {
            let mut statement = transaction
                .prepare(
                    "SELECT sequence, record, checksum, length(record) FROM penelope_records_v1
                 WHERE scope=?1 ORDER BY sequence LIMIT 4097",
                )
                .map_err(unavailable)?;
            let mut rows = statement.query(params![&self.key]).map_err(unavailable)?;
            while let Some(row) = rows.next().map_err(unavailable)? {
                let sequence: u32 = row.get(0).map_err(|_error| TaskPortError::Corrupt)?;
                let length: u32 = row.get(3).map_err(|_error| TaskPortError::Corrupt)?;
                if u64::from(length) > MAX_RECORD_BYTES as u64
                    || sequence >= self.definition.budget.max_history_records
                {
                    return Err(TaskPortError::Corrupt);
                }
                let bytes: Vec<u8> = row.get(1).map_err(unavailable)?;
                let checksum: Vec<u8> = row.get(2).map_err(unavailable)?;
                let committed = decode(&bytes, &checksum)?;
                if committed.sequence.get() != sequence {
                    return Err(TaskPortError::Corrupt);
                }
                projection
                    .apply(&committed)
                    .map_err(|_error| TaskPortError::Corrupt)?;
                if sequence == record.sequence.get() {
                    existing = Some(committed);
                }
            }
        }
        if let Some(committed) = existing {
            if committed != *record {
                return Err(TaskPortError::Conflict);
            }
        } else {
            projection
                .apply(record)
                .map_err(|_error| TaskPortError::Conflict)?;
        }
        match (reference(record), blob) {
            (Some(reference), supplied) => {
                if let Some(bytes) = supplied {
                    verify_blob(reference, bytes)?;
                    transaction
                        .execute(
                            "INSERT OR IGNORE INTO penelope_blobs_v1 VALUES (?1, ?2, ?3)",
                            params![&self.key, reference.digest.0.as_slice(), bytes],
                        )
                        .map_err(unavailable)?;
                }
                let stored: Vec<u8> = transaction
                    .query_row(
                        "SELECT bytes FROM penelope_blobs_v1
                     WHERE scope=?1 AND digest=?2 AND length(bytes)<=65536",
                        params![&self.key, reference.digest.0.as_slice()],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(unavailable)?
                    .ok_or(TaskPortError::Corrupt)?;
                verify_blob(reference, &stored)?;
            }
            (None, None) => {}
            (None, Some(_)) => return Err(TaskPortError::Corrupt),
        }
        if existing.is_none() {
            let encoded = serde_json::to_vec(record).map_err(|_error| TaskPortError::Corrupt)?;
            if encoded.len() > MAX_RECORD_BYTES {
                return Err(TaskPortError::Exhausted);
            }
            transaction
                .execute(
                    "INSERT INTO penelope_records_v1 VALUES (?1, ?2, ?3, ?4)",
                    params![
                        &self.key,
                        record.sequence.get(),
                        &encoded,
                        Digest::of(&encoded).0.as_slice()
                    ],
                )
                .map_err(unavailable)?;
        }
        transaction.commit().map_err(unavailable)?;
        Ok(*record)
    }

    fn read_blob(
        &mut self,
        scope: TaskScope,
        blob: BlobRef,
        buffer: &mut [u8],
    ) -> Result<usize, TaskPortError> {
        self.check_scope(scope)?;
        if blob.bytes > MAX_BLOB_BYTES || blob.schema == 0 {
            return Err(TaskPortError::Corrupt);
        }
        let length = usize::try_from(blob.bytes).map_err(|_error| TaskPortError::Exhausted)?;
        let destination = buffer.get_mut(..length).ok_or(TaskPortError::Exhausted)?;
        let bytes: Vec<u8> = self
            .connection
            .query_row(
                "SELECT bytes FROM penelope_blobs_v1
             WHERE scope=?1 AND digest=?2 AND length(bytes)=?3",
                params![&self.key, blob.digest.0.as_slice(), blob.bytes],
                |row| row.get(0),
            )
            .optional()
            .map_err(unavailable)?
            .ok_or(TaskPortError::Corrupt)?;
        verify_blob(blob, &bytes)?;
        destination.copy_from_slice(&bytes);
        Ok(length)
    }
}
