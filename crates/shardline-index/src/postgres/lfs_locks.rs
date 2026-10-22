use super::{PostgresMetadataStoreError, PostgresRecordStore};
use crate::{CreateLfsLockOutcome, LfsLockRecord, LfsLockStore, UnlockLfsOutcome};
use sqlx::{Row, postgres::PgRow};

fn row_lock(row: &PgRow) -> Result<LfsLockRecord, sqlx::Error> {
    Ok(LfsLockRecord {
        id: row.try_get("id")?,
        path: row.try_get("path")?,
        owner_key: row.try_get("owner_key")?,
        owner_name: row.try_get("owner_name")?,
        locked_at: row.try_get("locked_at")?,
    })
}

async fn lock_repository(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 7654))")
        .bind(format!("shardline-lfs-locks:{scope}"))
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

#[async_trait::async_trait]
impl LfsLockStore for PostgresRecordStore {
    type Error = PostgresMetadataStoreError;

    async fn create_lfs_lock(
        &self,
        scope: &str,
        lock: &LfsLockRecord,
    ) -> Result<CreateLfsLockOutcome, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        lock_repository(&mut transaction, scope).await?;
        let existing = sqlx::query("SELECT id,path,owner_key,owner_name,locked_at FROM shardline_lfs_locks WHERE scope_namespace=$1 AND path=$2").bind(scope).bind(&lock.path).fetch_optional(&mut *transaction).await?;
        let outcome = if let Some(row) = existing {
            CreateLfsLockOutcome::Conflict(row_lock(&row)?)
        } else {
            sqlx::query("INSERT INTO shardline_lfs_locks (scope_namespace,id,path,owner_key,owner_name,locked_at) VALUES ($1,$2,$3,$4,$5,$6)").bind(scope).bind(&lock.id).bind(&lock.path).bind(&lock.owner_key).bind(&lock.owner_name).bind(&lock.locked_at).execute(&mut *transaction).await?;
            CreateLfsLockOutcome::Created(lock.clone())
        };
        transaction.commit().await?;
        Ok(outcome)
    }

    async fn list_lfs_locks(
        &self,
        scope: &str,
        path: Option<&str>,
        id: Option<&str>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<LfsLockRecord>, Self::Error> {
        let rows = sqlx::query("SELECT id,path,owner_key,owner_name,locked_at FROM shardline_lfs_locks WHERE scope_namespace=$1 AND ($2::TEXT IS NULL OR path=$2) AND ($3::TEXT IS NULL OR id=$3) AND ($4::TEXT IS NULL OR id>$4) ORDER BY id LIMIT $5").bind(scope).bind(path).bind(id).bind(after).bind(i64::from(limit)).fetch_all(&self.pool).await?;
        rows.iter()
            .map(row_lock)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Self::Error::from)
    }

    async fn unlock_lfs_lock(
        &self,
        scope: &str,
        id: &str,
        owner: &str,
        force: bool,
    ) -> Result<UnlockLfsOutcome, Self::Error> {
        let mut transaction = self.pool.begin().await?;
        lock_repository(&mut transaction, scope).await?;
        let existing = sqlx::query("SELECT id,path,owner_key,owner_name,locked_at FROM shardline_lfs_locks WHERE scope_namespace=$1 AND id=$2").bind(scope).bind(id).fetch_optional(&mut *transaction).await?;
        let outcome = match existing.map(|row| row_lock(&row)).transpose()? {
            None => UnlockLfsOutcome::Missing,
            Some(lock) if !force && lock.owner_key != owner => UnlockLfsOutcome::Forbidden,
            Some(lock) => {
                sqlx::query("DELETE FROM shardline_lfs_locks WHERE scope_namespace=$1 AND id=$2")
                    .bind(scope)
                    .bind(id)
                    .execute(&mut *transaction)
                    .await?;
                UnlockLfsOutcome::Removed(lock)
            }
        };
        transaction.commit().await?;
        Ok(outcome)
    }
}
