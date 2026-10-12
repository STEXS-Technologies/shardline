//! Opaque evidence primitives, independent of domain execution ownership.
use sqlx::{Postgres, Transaction};

use super::{PostgresIndexStore, PostgresMetadataStoreError, i64_to_u64};

/// One bounded artifact read. `None` bytes with a known length means oversized
/// evidence; no blob above the requested bound was transferred to the client.
#[derive(Debug, PartialEq, Eq)]
pub struct PostgresTaskArtifact {
    /// Stored byte length from the same snapshot as the optional body.
    pub length: u64,
    /// Exact bytes when they fit the caller's bound.
    pub bytes: Option<Vec<u8>>,
}

pub(super) async fn initialize_task_artifacts(
    transaction: &mut Transaction<'_, Postgres>,
    schema: &str,
) -> Result<(), sqlx::Error> {
    let quoted_schema = format!("\"{}\"", schema.replace('"', "\"\""));
    let schema_literal = format!("E'{}'", schema.replace('\\', "\\\\").replace('\'', "\\'"));
    let sql = include_str!("task_artifacts.sql")
        .split("@SCHEMA@")
        .map(|part| part.replace("@SCHEMA_LITERAL@", &schema_literal))
        .collect::<Vec<_>>()
        .join(&quoted_schema);
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(&mut **transaction)
        .await?;
    // Owner defaults can grant direct access to every new table. Keep this
    // optional table private without changing released grants or those defaults.
    let grantees: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT actor.rolname FROM pg_class target
         CROSS JOIN LATERAL aclexplode(target.relacl) AS permission
         JOIN pg_roles actor ON actor.oid=permission.grantee
         WHERE target.oid=format('%I.shardline_task_artifacts_v1',$1)::regclass
           AND permission.grantee <> target.relowner",
    )
    .bind(schema)
    .fetch_all(&mut **transaction)
    .await?;
    for grantee in grantees {
        let quoted_role = format!("\"{}\"", grantee.replace('"', "\"\""));
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "REVOKE ALL ON {quoted_schema}.shardline_task_artifacts_v1 FROM {quoted_role}"
        )))
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

impl PostgresIndexStore {
    // Resolve the actual connection's schema once per private transaction.
    // A five-second statement timeout bounds evidence I/O without altering any
    // other work session or assigning this transaction execution ownership.
    async fn task_artifact_transaction(
        &self,
    ) -> Result<(Transaction<'_, Postgres>, String), PostgresMetadataStoreError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT set_config('statement_timeout', '5s', true)")
            .execute(&mut *transaction)
            .await?;
        let schema = sqlx::query_scalar("SELECT current_schema()")
            .fetch_one(&mut *transaction)
            .await?;
        Ok((transaction, schema))
    }

    /// Publishes exact immutable bytes using the owner-installed evidence function.
    /// Identical retries succeed; conflicting bytes fail without replacing them.
    /// Requires existing reliability-read and maintenance-write permissions,
    /// not direct artifact-table access or runtime DDL. RLS schemas are rejected.
    /// Artifact queries use a five-second transaction-local statement timeout.
    ///
    /// # Errors
    /// Returns missing owner installation, authorization, conflict or database errors.
    pub async fn publish_task_artifact(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<(), PostgresMetadataStoreError> {
        let (mut transaction, schema) = self.task_artifact_transaction().await?;
        let quoted = format!("\"{}\"", schema.replace('"', "\"\""));
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {quoted}.shardline_task_artifact_publish_v1($1,$2,$3)"
        )))
        .bind(&schema)
        .bind(key)
        .bind(bytes)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Reads opaque bytes within the supplied bound on one metadata snapshot.
    /// Absence differs from an oversized body; both avoid unbounded allocation.
    /// Requires existing reliability-read permissions. RLS schemas are rejected.
    /// Artifact queries use a five-second transaction-local statement timeout.
    ///
    /// # Errors
    /// Returns missing installation, authorization or database errors.
    pub async fn read_task_artifact(
        &self,
        key: &str,
        maximum_bytes: u64,
    ) -> Result<Option<PostgresTaskArtifact>, PostgresMetadataStoreError> {
        let (mut transaction, schema) = self.task_artifact_transaction().await?;
        let quoted = format!("\"{}\"", schema.replace('"', "\"\""));
        let row: Option<(i64, Option<Vec<u8>>)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT byte_length,body FROM {quoted}.shardline_task_artifact_read_v1($1,$2,$3)"
        )))
        .bind(&schema)
        .bind(key)
        .bind(i64::try_from(maximum_bytes).unwrap_or(i64::MAX))
        .fetch_optional(&mut *transaction)
        .await?;
        transaction.commit().await?;
        row.map(|(length, bytes)| {
            Ok(PostgresTaskArtifact {
                length: i64_to_u64(length)?,
                bytes,
            })
        })
        .transpose()
    }

    /// Returns at most 64 key/length pairs using a literal prefix and lexical cursor.
    /// Stop at the first key outside the prefix. No bodies are loaded; concurrent
    /// inserts behind the cursor can appear on the next inspection.
    /// Artifact queries use a five-second transaction-local statement timeout.
    ///
    /// # Errors
    /// Returns missing installation, authorization or database errors.
    pub async fn task_artifact_page(
        &self,
        prefix: &str,
        after: Option<&str>,
    ) -> Result<Vec<(String, u64)>, PostgresMetadataStoreError> {
        let (mut transaction, schema) = self.task_artifact_transaction().await?;
        let quoted = format!("\"{}\"", schema.replace('"', "\"\""));
        let rows: Vec<(String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT artifact_key,byte_length FROM {quoted}.shardline_task_artifact_page_v1($1,$2,$3)"
        )))
        .bind(&schema)
        .bind(prefix)
        .bind(after)
        .fetch_all(&mut *transaction)
        .await?;
        transaction.commit().await?;
        rows.into_iter()
            .map(|(key, length)| Ok((key, i64_to_u64(length)?)))
            .collect()
    }

    /// Removes evidence with existing maintenance-deletion permissions.
    /// This cannot mutate domain metadata or acquire execution ownership.
    /// Artifact queries use a five-second transaction-local statement timeout.
    ///
    /// # Errors
    /// Returns missing installation, authorization or database errors.
    pub async fn delete_task_artifact(
        &self,
        key: &str,
    ) -> Result<bool, PostgresMetadataStoreError> {
        let (mut transaction, schema) = self.task_artifact_transaction().await?;
        let quoted = format!("\"{}\"", schema.replace('"', "\"\""));
        let removed = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT {quoted}.shardline_task_artifact_delete_v1($1,$2)"
        )))
        .bind(&schema)
        .bind(key)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(removed)
    }
}

#[cfg(test)]
mod tests;
