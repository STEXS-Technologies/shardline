#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::test_support::postgres_fixture::PostgresFixture;

async fn move_frontend_history_to_prerelease_ledger(pool: &PgPool) {
    for migration in FRONTEND_MIGRATIONS {
        query(
            "INSERT INTO shardline_schema_migrations(version,name,checksum,applied_at)
             VALUES ($1,$2,$3,now())",
        )
        .bind(migration.version)
        .bind(migration.name)
        .bind(migration_checksum(&migration))
        .execute(pool)
        .await
        .unwrap();
    }
    raw_sql("DROP TABLE shardline_frontend_schema_v1")
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires configured disposable PostgreSQL"]
async fn adoption_preserves_original_history_and_durable_frontend_rows() {
    let fixture = PostgresFixture::new().await.expect("DATABASE_URL required");
    let pool = fixture.index().pool();
    move_frontend_history_to_prerelease_ledger(pool).await;
    raw_sql(
        "INSERT INTO shardline_xet_file_identities VALUES ('scope','sha','file',42);
        INSERT INTO shardline_lfs_locks VALUES ('scope','id','path','owner','name','time')",
    )
    .execute(pool)
    .await
    .unwrap();
    let before: Vec<(String,String,String)> = sqlx::query_as("SELECT version,checksum,applied_at::text FROM shardline_schema_migrations WHERE version IN ('20261021000000','20261022000000') ORDER BY version")
        .fetch_all(pool).await.unwrap();
    assert_eq!(before.len(), 2);
    let report =
        super::super::run_database_migration(&super::super::DatabaseMigrationOptions::new(
            fixture.config.index_postgres_url().unwrap().to_owned(),
            super::super::DatabaseMigrationCommand::Up { steps: None },
        ))
        .await
        .unwrap();
    assert_eq!(report.pending_count, 0);
    assert_eq!(report.applied_count, 2);
    assert_eq!(report.applied_total_count, 37);
    install(pool).await.unwrap();
    check(pool).await.unwrap();
    let after: Vec<(String,String,String)> = sqlx::query_as("SELECT version,checksum,applied_at::text FROM shardline_frontend_schema_v1 ORDER BY version")
        .fetch_all(pool).await.unwrap();
    assert_eq!(after, before);
    let legacy: i64 = query_scalar("SELECT count(*) FROM shardline_schema_migrations WHERE version IN ('20261021000000','20261022000000')").fetch_one(pool).await.unwrap();
    assert_eq!(legacy, 0);
    let identity: String = query_scalar("SELECT file_id FROM shardline_xet_file_identities")
        .fetch_one(pool)
        .await
        .unwrap();
    let lock: String = query_scalar("SELECT id FROM shardline_lfs_locks")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(identity, "file");
    assert_eq!(lock, "id");
    super::super::verify_applied_migrations(pool).await.unwrap();
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires configured disposable PostgreSQL"]
async fn mismatched_prerelease_checksum_rolls_back_without_losing_history() {
    let fixture = PostgresFixture::new().await.expect("DATABASE_URL required");
    let pool = fixture.index().pool();
    move_frontend_history_to_prerelease_ledger(pool).await;
    query(
        "UPDATE shardline_schema_migrations SET checksum='corrupt' WHERE version='20261022000000'",
    )
    .execute(pool)
    .await
    .unwrap();
    assert!(install(pool).await.is_err());
    let count: i64 = query_scalar("SELECT count(*) FROM shardline_schema_migrations WHERE version IN ('20261021000000','20261022000000')").fetch_one(pool).await.unwrap();
    assert_eq!(count, 2);
    let registry: bool = query_scalar("SELECT to_regclass(format('%I.shardline_frontend_schema_v1',current_schema())) IS NOT NULL").fetch_one(pool).await.unwrap();
    assert!(!registry);
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires configured disposable PostgreSQL"]
async fn corrupted_physical_contract_and_registry_fail_closed() {
    let fixture = PostgresFixture::new().await.expect("DATABASE_URL required");
    let pool = fixture.index().pool();
    install(pool).await.unwrap();
    raw_sql("DROP INDEX shardline_xet_file_identities_file_id_idx")
        .execute(pool)
        .await
        .unwrap();
    assert!(check(pool).await.is_err());
    assert!(install(pool).await.is_err());
    raw_sql(FRONTEND_MIGRATIONS.first().unwrap().up_sql)
        .execute(pool)
        .await
        .unwrap();
    check(pool).await.unwrap();
    query("UPDATE shardline_frontend_schema_v1 SET checksum='corrupt'")
        .execute(pool)
        .await
        .unwrap();
    assert!(check(pool).await.is_err());
    assert!(install(pool).await.is_err());
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires configured disposable PostgreSQL"]
async fn unknown_frontend_history_fails_check_status_and_install() {
    let fixture = PostgresFixture::new().await.expect("DATABASE_URL required");
    let pool = fixture.index().pool();
    install(pool).await.unwrap();
    query("INSERT INTO shardline_frontend_schema_v1(version,name,checksum) VALUES ('99999999999999','future','future')")
        .execute(pool).await.unwrap();

    assert!(check(pool).await.is_err());
    assert!(status_history(pool).await.is_err());
    assert!(install(pool).await.is_err());
    let preserved: i64 = query_scalar(
        "SELECT count(*) FROM shardline_frontend_schema_v1 WHERE version='99999999999999'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(preserved, 1);
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires configured disposable PostgreSQL"]
async fn unrelated_core_history_is_never_hidden_by_frontend_installation() {
    let fixture = PostgresFixture::new().await.expect("DATABASE_URL required");
    let pool = fixture.index().pool();
    query("INSERT INTO shardline_schema_migrations(version,name,checksum) VALUES ('99999999999999','unknown','unknown')")
        .execute(pool).await.unwrap();
    install(pool).await.unwrap();
    assert!(
        matches!(super::super::verify_applied_migrations(pool).await, Err(DatabaseMigrationError::UnknownAppliedMigration(version)) if version=="99999999999999")
    );
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires configured disposable PostgreSQL"]
async fn existing_read_only_role_can_check_without_ddl_or_new_write_permissions() {
    let fixture = PostgresFixture::new().await.expect("DATABASE_URL required");
    let pool = fixture.index().pool();
    let role_name = format!("frontend_reader_{}", fixture.schema);
    let role = quote(&role_name);
    let schema = quote(&fixture.schema);
    raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {role}; GRANT USAGE ON SCHEMA {schema} TO {role};
         GRANT SELECT ON {schema}.shardline_schema_migrations,{schema}.shardline_file_records TO {role}"
    ))).execute(pool).await.unwrap();
    install(pool).await.unwrap();
    // The fixture pool has exactly one connection, so SET ROLE applies to
    // every query below, including the ordinary startup compatibility check.
    query(sqlx::AssertSqlSafe(format!("SET ROLE {role}")))
        .execute(pool)
        .await
        .unwrap();
    super::super::check_database_schema_compatibility(pool)
        .await
        .unwrap();
    for table in [
        HISTORY,
        "shardline_xet_file_identities",
        "shardline_lfs_locks",
    ] {
        let permissions: (bool, bool, bool, bool) = sqlx::query_as(
            "SELECT
            has_table_privilege(current_user,format('%I.%I',current_schema(),$1),'SELECT'),
            has_table_privilege(current_user,format('%I.%I',current_schema(),$1),'INSERT'),
            has_table_privilege(current_user,format('%I.%I',current_schema(),$1),'UPDATE'),
            has_table_privilege(current_user,format('%I.%I',current_schema(),$1),'DELETE')",
        )
        .bind(table)
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(permissions, (true, false, false, false));
    }
    let ddl: bool =
        query_scalar("SELECT has_schema_privilege(current_user,current_schema(),'CREATE')")
            .fetch_one(pool)
            .await
            .unwrap();
    assert!(!ddl);
    query("RESET ROLE").execute(pool).await.unwrap();
    // The role owns no objects: remove only its fixture grants before dropping it.
    raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP OWNED BY {role}; DROP ROLE {role}"
    )))
    .execute(pool)
    .await
    .unwrap();
    fixture.finish().await;
}

#[tokio::test]
#[ignore = "requires configured disposable PostgreSQL"]
async fn row_security_on_grant_sources_blocks_privilege_widening_atomically() {
    for table in ["shardline_file_records", "shardline_schema_migrations"] {
        let fixture = PostgresFixture::new().await.expect("DATABASE_URL required");
        let pool = fixture.index().pool();
        raw_sql("DROP TABLE shardline_frontend_schema_v1")
            .execute(pool)
            .await
            .unwrap();
        let schema = quote(&fixture.schema);
        raw_sql(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE {schema}.{table} ENABLE ROW LEVEL SECURITY;
             ALTER TABLE {schema}.{table} FORCE ROW LEVEL SECURITY;
             CREATE POLICY deny_frontend_rows ON {schema}.{table}
                 USING (false) WITH CHECK (false)"
        )))
        .execute(pool)
        .await
        .unwrap();
        assert!(matches!(
            install(pool).await,
            Err(DatabaseMigrationError::FrontendSchemaIncompatible(message))
                if message.contains(table) && message.contains("row-level security")
        ));
        let registry: bool = query_scalar(
            "SELECT to_regclass(format('%I.shardline_frontend_schema_v1',current_schema())) IS NOT NULL",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert!(
            !registry,
            "failed migration must roll back its new registry"
        );
        fixture.finish().await;
    }
}
