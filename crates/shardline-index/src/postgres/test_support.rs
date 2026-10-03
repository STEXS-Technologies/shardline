use std::{
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU64, Ordering},
};

use sqlx::{PgPool, postgres::PgPoolOptions, query, query_scalar};
use tokio::sync::OnceCell;

// Quote catalog identifiers before inserting them into test-only DDL.
fn quoted_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

static NEXT_SCHEMA: AtomicU64 = AtomicU64::new(1);
static CLEANED_STALE_SCHEMAS: OnceCell<()> = OnceCell::const_new();

// Never print connection strings or driver errors: malformed configuration can
// contain credentials. A configured fixture must fail at its named setup stage.
#[allow(clippy::panic)]
fn fixture_stage<T, E>(result: Result<T, E>, stage: &str) -> T {
    result.unwrap_or_else(|_| panic!("configured PostgreSQL test fixture failed: {stage}"))
}

/// Creates a private PostgreSQL schema for one integration-test caller.
///
/// The production code intentionally lists and verifies all rows in a store.
/// A shared test database therefore makes a test that deliberately corrupts
/// one row race with an unrelated listing test.  Each caller gets its own
/// schema and connection search path, so those fault-injection tests can run
/// concurrently without weakening production verification or using a process
/// lock.
pub(crate) async fn connect_isolated_postgres() -> Option<PgPool> {
    let database_url = match std::env::var("DATABASE_URL") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => {
            match std::env::var("SHARDLINE_INDEX_POSTGRES_URL") {
                Ok(value) => value,
                Err(std::env::VarError::NotPresent) => return None,
                Err(error) => fixture_stage(Err(error), "fallback connection configuration"),
            }
        }
        Err(error) => fixture_stage(Err(error), "connection configuration"),
    };
    let thread = std::thread::current();
    let test_name = thread.name().unwrap_or("unnamed-test");
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    test_name.hash(&mut hasher);
    let nonce = NEXT_SCHEMA.fetch_add(1, Ordering::Relaxed);
    let schema = format!(
        "shardline_test_{}_{}_{}",
        std::process::id(),
        hasher.finish(),
        nonce
    );

    let admin_pool = fixture_stage(
        PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await,
        "administrator connection",
    );
    let stale_schema_prefix = format!("shardline_test_{}_%", std::process::id());
    CLEANED_STALE_SCHEMAS
        .get_or_init(|| async {
            let schema_names = fixture_stage(
                query_scalar::<_, String>(
                    "SELECT nspname
                 FROM pg_namespace
                 WHERE nspname LIKE $1",
                )
                .bind(&stale_schema_prefix)
                .fetch_all(&admin_pool)
                .await,
                "stale schema catalog",
            );
            for schema_name in schema_names {
                fixture_stage(
                    query(sqlx::AssertSqlSafe(format!(
                        "DROP SCHEMA {} CASCADE",
                        quoted_identifier(&schema_name)
                    )))
                    .execute(&admin_pool)
                    .await,
                    "stale schema cleanup",
                );
            }
        })
        .await;
    let schema = quoted_identifier(&schema);
    fixture_stage(
        query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin_pool)
            .await,
        "isolated schema creation",
    );
    let table_names = fixture_stage(
        query_scalar::<_, String>(
            "SELECT tablename
         FROM pg_tables
         WHERE schemaname = 'public' AND tablename LIKE 'shardline_%'
         ORDER BY tablename",
        )
        .fetch_all(&admin_pool)
        .await,
        "table catalog",
    );
    for table_name in table_names {
        let table_name = quoted_identifier(&table_name);
        fixture_stage(
            query(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {schema}.{table_name} (LIKE public.{table_name} INCLUDING ALL)"
            )))
            .execute(&admin_pool)
            .await,
            "isolated table copy",
        );
    }
    let trigger_definitions = fixture_stage(
        query_scalar::<_, String>(
            "SELECT pg_get_triggerdef(trigger.oid)
         FROM pg_trigger AS trigger
         JOIN pg_class AS table_entry ON table_entry.oid = trigger.tgrelid
         JOIN pg_namespace AS namespace_entry
           ON namespace_entry.oid = table_entry.relnamespace
         WHERE namespace_entry.nspname = 'public'
           AND table_entry.relname LIKE 'shardline_%'
           AND NOT trigger.tgisinternal",
        )
        .fetch_all(&admin_pool)
        .await,
        "trigger catalog",
    );
    for definition in trigger_definitions {
        let isolated_definition = definition.replace(" ON public.", &format!(" ON {schema}."));
        // This SQL comes from PostgreSQL pg_get_triggerdef, with a quoted schema substitution.
        fixture_stage(
            query(sqlx::AssertSqlSafe(isolated_definition))
                .execute(&admin_pool)
                .await,
            "isolated trigger copy",
        );
    }
    admin_pool.close().await;

    let search_path = format!("SET search_path TO {schema}, public");
    Some(fixture_stage(
        PgPoolOptions::new()
            .max_connections(8)
            .after_connect(move |connection, _metadata| {
                let search_path = search_path.clone();
                Box::pin(async move {
                    query(sqlx::AssertSqlSafe(search_path))
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(&database_url)
            .await,
        "isolated connection pool/search path",
    ))
}
