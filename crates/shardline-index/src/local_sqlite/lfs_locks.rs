use super::{LocalIndexStoreError, LocalRecordStore};
use crate::{CreateLfsLockOutcome, LfsLockRecord, LfsLockStore, UnlockLfsOutcome};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

fn row_lock(row: &rusqlite::Row<'_>) -> rusqlite::Result<LfsLockRecord> {
    Ok(LfsLockRecord {
        id: row.get("id")?,
        path: row.get("path")?,
        owner_key: row.get("owner_key")?,
        owner_name: row.get("owner_name")?,
        locked_at: row.get("locked_at")?,
    })
}

async fn run<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, LocalIndexStoreError> + Send + 'static,
) -> Result<T, LocalIndexStoreError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| LocalIndexStoreError::BlockingTask(error.to_string()))?
}

#[async_trait::async_trait]
impl LfsLockStore for LocalRecordStore {
    type Error = LocalIndexStoreError;

    async fn create_lfs_lock(
        &self,
        scope: &str,
        lock: &LfsLockRecord,
    ) -> Result<CreateLfsLockOutcome, Self::Error> {
        let store = self.clone();
        let scope = scope.to_owned();
        let lock = lock.clone();
        run(move || super::helpers::retry_sqlite_busy(|| {
            let mut connection = store.open_connection()?;
            let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let existing = transaction.query_row("SELECT id,path,owner_key,owner_name,locked_at FROM shardline_lfs_locks WHERE scope_namespace=?1 AND path=?2", params![scope,lock.path], row_lock).optional()?;
            let outcome = if let Some(existing) = existing { CreateLfsLockOutcome::Conflict(existing) } else {
                transaction.execute("INSERT INTO shardline_lfs_locks (scope_namespace,id,path,owner_key,owner_name,locked_at) VALUES (?1,?2,?3,?4,?5,?6)", params![scope,lock.id,lock.path,lock.owner_key,lock.owner_name,lock.locked_at])?;
                CreateLfsLockOutcome::Created(lock.clone())
            };
            transaction.commit()?;
            Ok(outcome)
        })).await
    }

    async fn list_lfs_locks(
        &self,
        scope: &str,
        path: Option<&str>,
        id: Option<&str>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<LfsLockRecord>, Self::Error> {
        let store = self.clone();
        let scope = scope.to_owned();
        let path = path.map(str::to_owned);
        let id = id.map(str::to_owned);
        let after = after.map(str::to_owned);
        run(move || {
            let connection = store.open_connection()?;
            let mut statement = connection.prepare("SELECT id,path,owner_key,owner_name,locked_at FROM shardline_lfs_locks WHERE scope_namespace=?1 AND (?2 IS NULL OR path=?2) AND (?3 IS NULL OR id=?3) AND (?4 IS NULL OR id>?4) ORDER BY id LIMIT ?5")?;
            Ok(statement.query_map(params![scope,path,id,after,limit], row_lock)?.collect::<Result<Vec<_>,_>>()?)
        }).await
    }

    async fn unlock_lfs_lock(
        &self,
        scope: &str,
        id: &str,
        owner: &str,
        force: bool,
    ) -> Result<UnlockLfsOutcome, Self::Error> {
        let store = self.clone();
        let scope = scope.to_owned();
        let id = id.to_owned();
        let owner = owner.to_owned();
        run(move || super::helpers::retry_sqlite_busy(|| {
            let mut connection = store.open_connection()?;
            let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let existing = transaction.query_row("SELECT id,path,owner_key,owner_name,locked_at FROM shardline_lfs_locks WHERE scope_namespace=?1 AND id=?2", params![scope,id], row_lock).optional()?;
            let outcome = match existing {
                None => UnlockLfsOutcome::Missing,
                Some(lock) if !force && lock.owner_key != owner => UnlockLfsOutcome::Forbidden,
                Some(lock) => { transaction.execute("DELETE FROM shardline_lfs_locks WHERE scope_namespace=?1 AND id=?2", params![scope,id])?; UnlockLfsOutcome::Removed(lock) }
            };
            transaction.commit()?;
            Ok(outcome)
        })).await
    }
}
