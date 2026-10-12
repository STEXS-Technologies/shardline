use sqlx::{Postgres, Transaction, query, query_scalar};

use super::PostgresIndexStore;

impl PostgresIndexStore {
    /// Installs the stable history identity and private artifact functions with
    /// migration-owner permissions. History grouping needs schema usage; artifact
    /// functions validate existing reliability and maintenance permissions.
    /// Reinstallation preserves identity and bytes, and keeps the artifact table
    /// private without changing released table grants or owner defaults.
    /// This never rewrites legacy object-backed task evidence.
    ///
    /// # Errors
    /// Returns missing released tables, insufficient DDL rights, entropy failure,
    /// or database failure.
    pub async fn initialize_task_history_scope(&self) -> Result<(), sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let schema = query_scalar::<_, String>("SELECT current_schema()")
            .fetch_one(&mut *transaction)
            .await?;
        initialize_task_history_scope(&mut transaction, &schema).await?;
        transaction.commit().await
    }
}

pub(super) async fn initialize_task_history_scope(
    transaction: &mut Transaction<'_, Postgres>,
    schema: &str,
) -> Result<(), sqlx::Error> {
    let quoted_schema = format!("\"{}\"", schema.replace('"', "\"\""));
    let schema_literal = format!("E'{}'", schema.replace('\\', "\\\\").replace('\'', "\\'"));
    let sql = include_str!("task_history_scope.sql")
        .split("@SCHEMA@")
        .map(|part| part.replace("@SCHEMA_LITERAL@", &schema_literal))
        .collect::<Vec<_>>()
        .join(&quoted_schema);
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(&mut **transaction)
        .await?;
    let mut identity = [0_u8; 32];
    getrandom::fill(&mut identity)
        .map_err(|error| sqlx::Error::Io(std::io::Error::other(error.to_string())))?;
    query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {quoted_schema}.shardline_resource_fences(domain, resource, epoch)
         VALUES ('maintenance-task-history-scope', $1, 1)
         ON CONFLICT(domain) WHERE domain='maintenance-task-history-scope' DO NOTHING"
    )))
    .bind(hex::encode(identity))
    .execute(&mut **transaction)
    .await?;
    super::task_artifacts::initialize_task_artifacts(transaction, schema).await?;
    Ok(())
}
