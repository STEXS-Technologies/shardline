//! Owner-installed additive frontend state with a separate compatibility ledger.
use sqlx::{PgConnection, PgPool, Row, query, query_scalar, raw_sql};

use super::{
    DatabaseMigrationError, FRONTEND_MIGRATIONS, MIGRATION_HISTORY_TABLE, migration_checksum,
};

const HISTORY: &str = "shardline_frontend_schema_v1";

fn incompatible(name: &str) -> DatabaseMigrationError {
    DatabaseMigrationError::FrontendSchemaIncompatible(name.to_owned())
}

pub(super) async fn install(pool: &PgPool) -> Result<u64, DatabaseMigrationError> {
    let mut transaction = pool.begin().await?;
    let mut installed_count = 0_u64;
    // The caller holds the existing migration lock for the whole owner command.
    raw_sql(
        "CREATE TABLE IF NOT EXISTS shardline_frontend_schema_v1 (
        version TEXT PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    )
    .execute(&mut *transaction)
    .await?;
    reject_unknown_history(&mut transaction).await?;
    for migration in FRONTEND_MIGRATIONS {
        let checksum = migration_checksum(&migration);
        let legacy = query(sqlx::AssertSqlSafe(format!(
            "SELECT name,checksum FROM {MIGRATION_HISTORY_TABLE} WHERE version=$1"
        )))
        .bind(migration.version)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = &legacy
            && (row.try_get::<String, _>("name")? != migration.name
                || row.try_get::<String, _>("checksum")? != checksum)
        {
            return Err(incompatible(migration.name));
        }
        let installed = query(sqlx::AssertSqlSafe(format!(
            "SELECT name,checksum FROM {HISTORY} WHERE version=$1"
        )))
        .bind(migration.version)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = installed {
            if row.try_get::<String, _>("name")? != migration.name
                || row.try_get::<String, _>("checksum")? != checksum
            {
                return Err(incompatible(migration.name));
            }
            if legacy.is_some() {
                let same_time: bool = query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT frontend.applied_at=core.applied_at FROM {HISTORY} frontend
                     JOIN {MIGRATION_HISTORY_TABLE} core USING(version) WHERE version=$1"
                )))
                .bind(migration.version)
                .fetch_one(&mut *transaction)
                .await?;
                if !same_time {
                    return Err(incompatible(migration.name));
                }
            }
        } else {
            raw_sql(migration.up_sql).execute(&mut *transaction).await?;
            query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {HISTORY} (version,name,checksum,applied_at)
                 VALUES ($1,$2,$3,COALESCE((SELECT applied_at FROM {MIGRATION_HISTORY_TABLE} WHERE version=$1),now()))"
            ))).bind(migration.version).bind(migration.name).bind(&checksum)
                .execute(&mut *transaction).await?;
            installed_count = installed_count.saturating_add(1);
        }
        if legacy.is_some() {
            query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {MIGRATION_HISTORY_TABLE} WHERE version=$1"
            )))
            .bind(migration.version)
            .execute(&mut *transaction)
            .await?;
        }
    }
    reject_row_security_on_sources(&mut transaction).await?;
    check_on_connection(&mut transaction).await?;
    // Preserve existing role policy, including PUBLIC and inherited grants.
    // The registry exposes only definitions already readable in the core ledger.
    mirror_grants(
        &mut transaction,
        MIGRATION_HISTORY_TABLE,
        HISTORY,
        &["SELECT"],
    )
    .await?;
    mirror_grants(
        &mut transaction,
        "shardline_file_records",
        "shardline_xet_file_identities",
        &["SELECT", "INSERT"],
    )
    .await?;
    mirror_grants(
        &mut transaction,
        "shardline_file_records",
        "shardline_lfs_locks",
        &["SELECT", "INSERT", "DELETE"],
    )
    .await?;
    transaction.commit().await?;
    Ok(installed_count)
}

pub(super) async fn status_history(
    pool: &PgPool,
) -> Result<Vec<super::AppliedMigration>, DatabaseMigrationError> {
    let exists: bool = query_scalar("SELECT to_regclass(format('%I.shardline_frontend_schema_v1',current_schema())) IS NOT NULL")
        .fetch_one(pool).await?;
    if !exists {
        return Ok(Vec::new());
    }
    let rows = query(sqlx::AssertSqlSafe(format!(
        "SELECT version,checksum,to_char(applied_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS applied_at_utc FROM {HISTORY} ORDER BY version"
    ))).fetch_all(pool).await?;
    let mut history = Vec::new();
    for row in rows {
        let version: String = row.try_get("version")?;
        let Some(migration) = FRONTEND_MIGRATIONS
            .iter()
            .find(|migration| migration.version == version)
        else {
            return Err(incompatible(&version));
        };
        let checksum: String = row.try_get("checksum")?;
        if checksum != migration_checksum(migration) {
            return Err(incompatible(migration.name));
        }
        history.push(super::AppliedMigration {
            version,
            checksum,
            applied_at_utc: row.try_get("applied_at_utc")?,
        });
    }
    Ok(history)
}

async fn mirror_grants(
    connection: &mut PgConnection,
    source: &str,
    target: &str,
    allowed: &[&str],
) -> Result<(), DatabaseMigrationError> {
    let schema: String = query_scalar("SELECT current_schema()")
        .fetch_one(&mut *connection)
        .await?;
    let grants = query("SELECT CASE WHEN acl.grantee=0 THEN NULL ELSE pg_get_userbyid(acl.grantee) END AS grantee, acl.privilege_type
        FROM pg_class relation JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace,
        LATERAL aclexplode(COALESCE(relation.relacl,acldefault('r',relation.relowner))) acl
        WHERE namespace.nspname=$1 AND relation.relname=$2
        AND acl.privilege_type IN ('SELECT','INSERT','UPDATE','DELETE')")
        .bind(&schema).bind(source).fetch_all(&mut *connection).await?;
    for grant in grants {
        let privilege: String = grant.try_get("privilege_type")?;
        if !allowed.contains(&privilege.as_str()) {
            continue;
        }
        let privilege = match privilege.as_str() {
            "SELECT" => "SELECT",
            "INSERT" => "INSERT",
            "DELETE" => "DELETE",
            _ => continue,
        };
        let role = grant
            .try_get::<Option<String>, _>("grantee")?
            .map_or_else(|| "PUBLIC".to_owned(), |name| quote(&name));
        query(sqlx::AssertSqlSafe(format!(
            "GRANT {privilege} ON {}.{} TO {role}",
            quote(&schema),
            quote(target)
        )))
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(super) async fn check(pool: &PgPool) -> Result<(), DatabaseMigrationError> {
    let mut connection = pool.acquire().await?;
    check_on_connection(&mut connection).await
}

async fn check_on_connection(connection: &mut PgConnection) -> Result<(), DatabaseMigrationError> {
    let exists: bool = query_scalar("SELECT to_regclass(format('%I.shardline_frontend_schema_v1',current_schema())) IS NOT NULL")
        .fetch_one(&mut *connection).await?;
    if !exists {
        return Err(incompatible("frontend installation history"));
    }
    reject_unknown_history(connection).await?;
    reject_row_security_on_sources(connection).await?;
    for migration in FRONTEND_MIGRATIONS {
        let row = query(sqlx::AssertSqlSafe(format!(
            "SELECT name,checksum FROM {HISTORY} WHERE version=$1"
        )))
        .bind(migration.version)
        .fetch_optional(&mut *connection)
        .await?;
        let Some(row) = row else {
            return Err(incompatible(migration.name));
        };
        if row.try_get::<String, _>("name")? != migration.name
            || row.try_get::<String, _>("checksum")? != migration_checksum(&migration)
        {
            return Err(incompatible(migration.name));
        }
        let legacy: bool = query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT EXISTS(SELECT 1 FROM {MIGRATION_HISTORY_TABLE} WHERE version=$1)"
        )))
        .bind(migration.version)
        .fetch_one(&mut *connection)
        .await?;
        if legacy {
            return Err(incompatible("frontend entry remains in core history"));
        }
    }
    verify_table(
        connection,
        "shardline_xet_file_identities",
        &[
            ("scope_key", "text"),
            ("sha256", "text"),
            ("file_id", "text"),
            ("total_bytes", "bigint"),
        ],
        &[
            "CHECK (total_bytes >= 0)",
            "PRIMARY KEY (scope_key, sha256)",
        ],
    )
    .await?;
    verify_table(
        connection,
        "shardline_lfs_locks",
        &[
            ("scope_namespace", "text"),
            ("id", "text"),
            ("path", "text"),
            ("owner_key", "text"),
            ("owner_name", "text"),
            ("locked_at", "text"),
        ],
        &[
            "PRIMARY KEY (scope_namespace, id)",
            "UNIQUE (scope_namespace, path)",
        ],
    )
    .await?;
    let index_valid: bool = query_scalar("SELECT COALESCE((SELECT index.indisvalid AND index.indisready
        AND NOT index.indisunique AND index.indnkeyatts=2 AND index.indnatts=2
        AND index.indpred IS NULL AND index.indexprs IS NULL
        AND index.indrelid=to_regclass(format('%I.shardline_xet_file_identities',current_schema()))
        AND pg_get_indexdef(index.indexrelid,1,true)='scope_key'
        AND pg_get_indexdef(index.indexrelid,2,true)='file_id'
        FROM pg_index index WHERE index.indexrelid=to_regclass(format('%I.shardline_xet_file_identities_file_id_idx',current_schema()))),false)")
        .fetch_one(&mut *connection).await?;
    if !index_valid {
        return Err(incompatible("Xet identity index"));
    }
    Ok(())
}

async fn reject_unknown_history(
    connection: &mut PgConnection,
) -> Result<(), DatabaseMigrationError> {
    let known_versions = FRONTEND_MIGRATIONS
        .iter()
        .map(|migration| migration.version.to_owned())
        .collect::<Vec<_>>();
    let unknown: Option<String> = query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT version FROM {HISTORY} WHERE NOT (version = ANY($1::text[])) ORDER BY version LIMIT 1"
    )))
    .bind(known_versions)
    .fetch_optional(&mut *connection)
    .await?;
    if let Some(version) = unknown {
        return Err(incompatible(&version));
    }
    Ok(())
}

async fn reject_row_security_on_sources(
    connection: &mut PgConnection,
) -> Result<(), DatabaseMigrationError> {
    let schema: String = query_scalar("SELECT current_schema()")
        .fetch_one(&mut *connection)
        .await?;
    let relation: Option<String> = query_scalar(
        "SELECT relation.relname FROM pg_class relation
         JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
         WHERE namespace.nspname=$1
           AND relation.relname=ANY($2::text[])
           AND (relation.relrowsecurity OR relation.relforcerowsecurity)
         ORDER BY relation.relname LIMIT 1",
    )
    .bind(schema)
    .bind(["shardline_schema_migrations", "shardline_file_records"])
    .fetch_optional(&mut *connection)
    .await?;
    if let Some(relation) = relation {
        // Grant copying cannot reproduce arbitrary row policies on tables with
        // a different shape. Refuse the upgrade instead of widening access.
        return Err(incompatible(&format!("{relation} row-level security")));
    }
    Ok(())
}

async fn verify_table(
    connection: &mut PgConnection,
    name: &str,
    expected: &[(&str, &str)],
    expected_constraints: &[&str],
) -> Result<(), DatabaseMigrationError> {
    let columns: Vec<(String, String, bool)> = sqlx::query_as("SELECT attname::text,format_type(atttypid,atttypmod),attnotnull
        FROM pg_attribute WHERE attrelid=to_regclass(format('%I.%I',current_schema(),$1)) AND attnum>0 AND NOT attisdropped ORDER BY attnum")
        .bind(name).fetch_all(&mut *connection).await?;
    if columns.len() != expected.len()
        || columns.iter().zip(expected).any(
            |((column, kind, required), (expected_column, expected_kind))| {
                column != expected_column || kind != expected_kind || !required
            },
        )
    {
        return Err(incompatible(name));
    }
    let constraints: Vec<String> = query_scalar("SELECT pg_get_constraintdef(oid,true) FROM pg_constraint WHERE conrelid=to_regclass(format('%I.%I',current_schema(),$1)) ORDER BY pg_get_constraintdef(oid,true)")
        .bind(name).fetch_all(&mut *connection).await?;
    if constraints.iter().map(String::as_str).collect::<Vec<_>>() != expected_constraints {
        return Err(incompatible(name));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
