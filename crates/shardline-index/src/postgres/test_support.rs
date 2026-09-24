use std::{
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU64, Ordering},
};

use sqlx::{PgPool, postgres::PgPoolOptions, query, query_scalar};
use tokio::sync::OnceCell;

static NEXT_SCHEMA: AtomicU64 = AtomicU64::new(1);
static CLEANED_STALE_SCHEMAS: OnceCell<()> = OnceCell::const_new();

/// Creates a private PostgreSQL schema for one integration-test caller.
///
/// The production code intentionally lists and verifies all rows in a store.
/// A shared test database therefore makes a test that deliberately corrupts
/// one row race with an unrelated listing test.  Each caller gets its own
/// schema and connection search path, so those fault-injection tests can run
/// concurrently without weakening production verification or using a process
/// lock.
pub(crate) async fn connect_isolated_postgres() -> Option<PgPool> {
    let database_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("SHARDLINE_INDEX_POSTGRES_URL"))
        .ok()?;
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

    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .ok()?;
    let _ = CLEANED_STALE_SCHEMAS
        .get_or_init(|| async {
            let schema_names = query_scalar::<_, String>(
                "SELECT nspname
                 FROM pg_namespace
                 WHERE nspname LIKE 'shardline_test_%'",
            )
            .fetch_all(&admin_pool)
            .await
            .unwrap_or_default();
            for schema_name in schema_names {
                let _ = query(&format!("DROP SCHEMA {schema_name} CASCADE"))
                    .execute(&admin_pool)
                    .await;
            }
        })
        .await;
    query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin_pool)
        .await
        .ok()?;
    let table_names = query_scalar::<_, String>(
        "SELECT tablename
         FROM pg_tables
         WHERE schemaname = 'public' AND tablename LIKE 'shardline_%'
         ORDER BY tablename",
    )
    .fetch_all(&admin_pool)
    .await
    .ok()?;
    for table_name in table_names {
        query(&format!(
            "CREATE TABLE {schema}.{table_name} (LIKE public.{table_name} INCLUDING ALL)"
        ))
        .execute(&admin_pool)
        .await
        .ok()?;
    }
    let trigger_definitions = query_scalar::<_, String>(
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
    .await
    .ok()?;
    for definition in trigger_definitions {
        let isolated_definition = definition.replace(" ON public.", &format!(" ON {schema}."));
        query(&isolated_definition)
            .execute(&admin_pool)
            .await
            .ok()?;
    }
    admin_pool.close().await;

    let search_path = format!("SET search_path TO {schema}, public");
    PgPoolOptions::new()
        .max_connections(8)
        .after_connect(move |connection, _metadata| {
            let search_path = search_path.clone();
            Box::pin(async move {
                query(&search_path).execute(connection).await?;
                Ok(())
            })
        })
        .connect(&database_url)
        .await
        .ok()
}
