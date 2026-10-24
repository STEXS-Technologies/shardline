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
    // Released metadata writers take the shared barrier before touching their
    // tables. Take its exclusive side before any DDL to avoid acquiring table
    // locks in the opposite order to an in-flight publication transaction.
    // Ownership and DDL share this transaction, including rollback on failure.
    query("SELECT pg_advisory_xact_lock($1)")
        .bind(crate::maintenance_barrier::GC_WRITE_BARRIER_KEY)
        .execute(&mut *transaction)
        .await?;
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
    mirror_grants(
        &mut transaction,
        "shardline_s3_objects",
        "shardline_s3_multipart_counts",
        &["SELECT", "INSERT", "UPDATE", "DELETE"],
    )
    .await?;
    mirror_grants(
        &mut transaction,
        "shardline_file_records",
        "shardline_record_membership",
        &["SELECT"],
    )
    .await?;
    // The invoker-rights trigger replaces a record's derived candidate set.
    // Grant only the derived-table operations needed by existing writers.
    mirror_grants_as(
        &mut transaction,
        "shardline_file_records",
        "shardline_record_membership",
        &["INSERT", "UPDATE"],
        Some(&["SELECT", "INSERT", "DELETE"]),
    )
    .await?;
    mirror_grants_as(
        &mut transaction,
        "shardline_file_records",
        "shardline_record_membership",
        &["DELETE"],
        Some(&["SELECT", "DELETE"]),
    )
    .await?;
    for table in ["shardline_xet_xorbs", "shardline_xet_chunks"] {
        mirror_grants(
            &mut transaction,
            "shardline_file_records",
            table,
            &["SELECT"],
        )
        .await?;
        mirror_grants_as(
            &mut transaction,
            "shardline_file_records",
            table,
            &["INSERT", "UPDATE"],
            Some(&["SELECT", "INSERT"]),
        )
        .await?;
    }
    mirror_grants(
        &mut transaction,
        "shardline_hub_repos",
        "shardline_hub_object_locations",
        &["SELECT"],
    )
    .await?;
    mirror_grants_as(
        &mut transaction,
        "shardline_hub_repos",
        "shardline_hub_object_locations",
        &["INSERT", "UPDATE"],
        Some(&["SELECT", "INSERT", "UPDATE"]),
    )
    .await?;
    // Copy the final effective identity grants, including those installed above.
    mirror_grants(
        &mut transaction,
        "shardline_xet_file_identities",
        "shardline_xet_file_aliases",
        &["SELECT"],
    )
    .await?;
    mirror_grants_as(
        &mut transaction,
        "shardline_xet_file_identities",
        "shardline_xet_file_aliases",
        &["INSERT", "UPDATE"],
        Some(&["SELECT", "INSERT"]),
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
    mirror_grants_as(connection, source, target, allowed, None).await
}

async fn mirror_grants_as(
    connection: &mut PgConnection,
    source: &str,
    target: &str,
    allowed: &[&str],
    target_privileges: Option<&[&str]>,
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
        let source_privilege: String = grant.try_get("privilege_type")?;
        if !allowed.contains(&source_privilege.as_str()) {
            continue;
        }
        let role = grant
            .try_get::<Option<String>, _>("grantee")?
            .map_or_else(|| "PUBLIC".to_owned(), |name| quote(&name));
        let original = [source_privilege.as_str()];
        for privilege in target_privileges.unwrap_or(&original) {
            let privilege = match *privilege {
                "SELECT" => "SELECT",
                "INSERT" => "INSERT",
                "UPDATE" => "UPDATE",
                "DELETE" => "DELETE",
                _ => continue,
            };
            query(sqlx::AssertSqlSafe(format!(
                "GRANT {privilege} ON {}.{} TO {role}",
                quote(&schema),
                quote(target)
            )))
            .execute(&mut *connection)
            .await?;
        }
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
    verify_table(
        connection,
        "shardline_s3_multipart_counts",
        &[
            ("scope_namespace", "text"),
            ("object_key", "text"),
            ("file_id", "text"),
            ("content_hash", "text"),
            ("part_count", "integer"),
            ("updated_at_unix_seconds", "bigint"),
            ("proof", "text"),
        ],
        &[
            "CHECK (part_count >= 0)",
            "PRIMARY KEY (scope_namespace, object_key)",
        ],
    )
    .await?;
    let invalidation_trigger_exists: bool = query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM pg_trigger trigger_row
            JOIN pg_class source_table ON source_table.oid=trigger_row.tgrelid
            JOIN pg_proc trigger_function ON trigger_function.oid=trigger_row.tgfoid
            WHERE trigger_row.tgname='shardline_s3_multipart_counts_invalidate'
              AND NOT trigger_row.tgisinternal
              AND trigger_row.tgenabled <> 'D'
              AND source_table.oid=to_regclass(format('%I.shardline_s3_objects', current_schema()))
              AND trigger_function.proname='shardline_invalidate_s3_multipart_count'
        )",
    )
    .fetch_one(&mut *connection)
    .await?;
    if !invalidation_trigger_exists {
        return Err(incompatible("S3 multipart count invalidation trigger"));
    }
    verify_table(
        connection,
        "shardline_record_membership",
        &[
            ("record_key", "text"),
            ("scope_key", "text"),
            ("member_hash", "text"),
        ],
        &["PRIMARY KEY (record_key, member_hash)"],
    )
    .await?;
    verify_xet_aliases(connection).await?;
    verify_table(
        connection,
        "shardline_hub_object_locations",
        &[
            ("repository_key", "text"),
            ("object_kind", "text"),
            ("object_id", "text"),
            ("storage_revision", "text"),
        ],
        &[
            "CHECK (object_kind = ANY (ARRAY['file'::text, 'git-archive'::text]))",
            "PRIMARY KEY (repository_key, object_kind, object_id)",
        ],
    )
    .await?;
    verify_table(
        connection,
        "shardline_xet_xorbs",
        &[("xorb_hash", "text"), ("descriptor", "jsonb")],
        &["PRIMARY KEY (xorb_hash)"],
    )
    .await?;
    verify_table(
        connection,
        "shardline_xet_chunks",
        &[
            ("xorb_hash", "text"),
            ("chunk_index", "bigint"),
            ("chunk_hash", "text"),
        ],
        &[
            "CHECK (chunk_index >= 0)",
            "FOREIGN KEY (xorb_hash) REFERENCES shardline_xet_xorbs(xorb_hash) ON DELETE CASCADE",
            "PRIMARY KEY (xorb_hash, chunk_index)",
        ],
    )
    .await?;
    let xet_lookup_valid: bool = query_scalar(
        "SELECT COALESCE((SELECT index.indisvalid AND index.indisready
            AND index.indnkeyatts=3 AND index.indpred IS NULL AND index.indexprs IS NULL
            AND index.indrelid=to_regclass(format('%I.shardline_xet_chunks',current_schema()))
            AND (SELECT array_agg(attribute.attname::text ORDER BY key.ordinality)
                 FROM unnest(index.indkey) WITH ORDINALITY key(attnum,ordinality)
                 JOIN pg_attribute attribute ON attribute.attrelid=index.indrelid AND attribute.attnum=key.attnum)
                 =ARRAY['chunk_hash','xorb_hash','chunk_index']::text[]
            FROM pg_index index WHERE index.indexrelid=to_regclass(format('%I.shardline_xet_chunks_hash_idx',current_schema()))),false)"
    ).fetch_one(&mut *connection).await?;
    if !xet_lookup_valid {
        return Err(incompatible("Xet chunk lookup index"));
    }
    let read_indexes: &[(&str, &str, &[&str])] = &[
        (
            "shardline_file_records_file_scope_idx",
            "shardline_file_records",
            &["file_id", "scope_key", "record_kind"],
        ),
        (
            "shardline_xet_file_identities_file_scope_idx",
            "shardline_xet_file_identities",
            &["file_id", "scope_key"],
        ),
        (
            "shardline_hub_file_entries_hash_idx",
            "shardline_hub_file_entries",
            &["commit_sha", "sha", "is_lfs", "size"],
        ),
    ];
    for (name, table, columns) in read_indexes {
        let valid: bool = query_scalar(
            "SELECT COALESCE((SELECT i.indisvalid AND i.indisready AND NOT i.indisunique
             AND i.indnkeyatts=cardinality($3::text[]) AND i.indnatts=i.indnkeyatts
             AND i.indpred IS NULL AND i.indexprs IS NULL
             AND i.indrelid=to_regclass(format('%I.%I',current_schema(),$2))
             AND (SELECT array_agg(a.attname::text ORDER BY k.ordinality)
                  FROM unnest(i.indkey) WITH ORDINALITY k(attnum,ordinality)
                  JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum)=$3::text[]
             FROM pg_index i WHERE i.indexrelid=to_regclass(format('%I.%I',current_schema(),$1))),false)",
        ).bind(name).bind(table).bind(columns).fetch_one(&mut *connection).await?;
        if !valid {
            return Err(incompatible(name));
        }
    }
    let membership_function = FRONTEND_MIGRATIONS
        .iter()
        .find(|migration| migration.name == "record_membership")
        .and_then(|migration| migration.up_sql.split_once("AS $$"))
        .and_then(|(_, tail)| tail.split_once("$$;"))
        .map(|(body, _)| body)
        .ok_or_else(|| incompatible("bundled record membership function"))?;
    let membership_trigger: bool = query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_trigger trigger_row
         JOIN pg_proc trigger_function ON trigger_function.oid=trigger_row.tgfoid
         WHERE trigger_row.tgname='shardline_record_membership_refresh'
           AND trigger_row.tgrelid=to_regclass(format('%I.shardline_file_records',current_schema()))
           AND trigger_row.tgenabled <> 'D' AND NOT trigger_row.tgisinternal
           AND trigger_row.tgtype=29
           AND trigger_function.proname='shardline_refresh_record_membership'
           AND NOT trigger_function.prosecdef
           AND trigger_function.prosrc=$1
           AND trigger_function.proconfig=ARRAY['search_path=pg_catalog']::text[])",
    )
    .bind(membership_function)
    .fetch_one(&mut *connection)
    .await?;
    if !membership_trigger {
        return Err(incompatible("record membership trigger"));
    }
    let membership_index: bool = query_scalar(
        "SELECT COALESCE((SELECT index.indisvalid AND index.indisready
         AND index.indnkeyatts=3 AND index.indnatts=3
         AND index.indpred IS NULL AND index.indexprs IS NULL
         AND index.indrelid=to_regclass(format('%I.shardline_record_membership',current_schema()))
         AND pg_get_indexdef(index.indexrelid,1,true)='member_hash'
         AND pg_get_indexdef(index.indexrelid,2,true)='scope_key'
         AND pg_get_indexdef(index.indexrelid,3,true)='record_key'
         FROM pg_index index WHERE index.indexrelid=to_regclass(format('%I.shardline_record_membership_lookup_idx',current_schema()))),false)",
    ).fetch_one(&mut *connection).await?;
    if !membership_index {
        return Err(incompatible("record membership index"));
    }
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
    .bind([
        "shardline_schema_migrations",
        "shardline_file_records",
        "shardline_hub_repos",
        "shardline_xet_file_identities",
    ])
    .fetch_optional(&mut *connection)
    .await?;
    if let Some(relation) = relation {
        // Grant copying cannot reproduce arbitrary row policies on tables with
        // a different shape. Refuse the upgrade instead of widening access.
        return Err(incompatible(&format!("{relation} row-level security")));
    }
    Ok(())
}

async fn verify_xet_aliases(connection: &mut PgConnection) -> Result<(), DatabaseMigrationError> {
    verify_table(
        connection,
        "shardline_xet_file_aliases",
        &[
            ("scope_key", "text"),
            ("file_id", "text"),
            ("sha256", "text"),
            ("total_bytes", "bigint"),
        ],
        &[
            "CHECK (total_bytes >= 0)",
            "PRIMARY KEY (scope_key, file_id)",
        ],
    )
    .await?;
    let body = FRONTEND_MIGRATIONS
        .iter()
        .find(|migration| migration.name == "xet_file_aliases")
        .and_then(|migration| migration.up_sql.split_once("AS $$"))
        .and_then(|(_, tail)| tail.split_once("$$;"))
        .map(|(body, _)| body)
        .ok_or_else(|| incompatible("bundled Xet alias function"))?;
    let valid: bool = query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_trigger t JOIN pg_proc f ON f.oid=t.tgfoid
         WHERE t.tgname='shardline_xet_file_aliases_preserve'
           AND t.tgrelid=to_regclass(format('%I.shardline_xet_file_identities',current_schema()))
           AND t.tgenabled IN ('O','A') AND NOT t.tgisinternal AND t.tgtype=21
           AND f.proname='shardline_preserve_xet_file_alias_v1'
           AND NOT f.prosecdef AND f.prosrc=$1
           AND f.proconfig=ARRAY['search_path=pg_catalog']::text[])",
    )
    .bind(body)
    .fetch_one(connection)
    .await?;
    if !valid {
        return Err(incompatible("Xet alias preservation trigger"));
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
