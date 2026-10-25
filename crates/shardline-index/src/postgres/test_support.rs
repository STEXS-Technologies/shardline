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
    let variable = if std::env::var_os("DATABASE_URL").is_none()
        && std::env::var_os("SHARDLINE_INDEX_POSTGRES_URL").is_some()
    {
        "SHARDLINE_INDEX_POSTGRES_URL"
    } else {
        "DATABASE_URL"
    };
    let database_url = shardline_test_support::postgres_fixture_url(variable)?;
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
    let has_creation_policy = table_names
        .iter()
        .any(|name| name == "shardline_hub_creation_policies");
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
    // LIKE INCLUDING ALL omits foreign keys. Preserve descriptor ownership in
    // isolated fixtures so deletion and rollback tests exercise real constraints.
    let has_xet_descriptors = fixture_stage(
        query_scalar::<_, bool>("SELECT to_regclass('public.shardline_xet_chunks') IS NOT NULL")
            .fetch_one(&admin_pool)
            .await,
        "descriptor table catalog",
    );
    if has_xet_descriptors {
        fixture_stage(
            query(sqlx::AssertSqlSafe(format!(
                "ALTER TABLE {schema}.shardline_xet_chunks ADD FOREIGN KEY(xorb_hash) REFERENCES {schema}.shardline_xet_xorbs(xorb_hash) ON DELETE CASCADE"
            )))
            .execute(&admin_pool)
            .await,
            "isolated descriptor foreign key",
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
           AND trigger.tgname NOT LIKE 'shardline_hub_creation_%'
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
    let pool = fixture_stage(
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
    );
    if has_creation_policy {
        fixture_stage(
            query(sqlx::AssertSqlSafe(format!(
                "ALTER TABLE {schema}.shardline_hub_creation_denials ADD FOREIGN KEY(namespace) REFERENCES {schema}.shardline_hub_creation_policies(namespace) ON DELETE CASCADE"
            )))
            .execute(&pool)
            .await,
            "isolated creation policy foreign key",
        );
        let migration =
            include_str!("../../../../migrations/20261024000000_hub_creation_policy.up.sql");
        let (_, functions) = fixture_stage(
            migration
                .split_once("CREATE FUNCTION shardline_hub_creation_policy_lock()")
                .ok_or(()),
            "creation policy function definitions",
        );
        let definitions =
            format!("CREATE FUNCTION shardline_hub_creation_policy_lock(){functions}");
        fixture_stage(
            sqlx::raw_sql(sqlx::AssertSqlSafe(definitions))
                .execute(&pool)
                .await,
            "isolated creation policy functions and triggers",
        );
    }
    fixture_stage(
        super::PostgresIndexStore::new(pool.clone())
            .initialize_remote_delete_guard()
            .await,
        "isolated remote delete guard",
    );
    Some(pool)
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[ignore = "invoked by the prerequisite subprocess regression"]
    async fn isolated_postgres_prerequisite_child() {
        assert!(super::connect_isolated_postgres().await.is_none());
    }

    #[test]
    fn isolated_postgres_respects_required_and_optional_prerequisites()
    -> Result<(), Box<dyn std::error::Error>> {
        let child = concat!(module_path!(), "::isolated_postgres_prerequisite_child")
            .split_once("::")
            .ok_or("missing test crate prefix")?
            .1;
        for (required, empty_variable, succeeds) in [
            (false, None, true),
            (true, None, false),
            (false, Some("DATABASE_URL"), false),
            (false, Some("SHARDLINE_INDEX_POSTGRES_URL"), false),
        ] {
            let mut command = std::process::Command::new(std::env::current_exe()?);
            command
                .args(["--exact", child, "--ignored", "--nocapture"])
                .env_remove("DATABASE_URL")
                .env_remove("SHARDLINE_INDEX_POSTGRES_URL")
                .env_remove("SHARDLINE_REQUIRE_POSTGRES_TESTS");
            if required {
                command.env("SHARDLINE_REQUIRE_POSTGRES_TESTS", "1");
            }
            if let Some(variable) = empty_variable {
                command.env(variable, "  ");
            }
            let output = shardline_test_support::native_client_output(
                &mut command,
                std::time::Duration::from_secs(30),
            )?;
            if output.status.success() != succeeds {
                return Err("unexpected PostgreSQL prerequisite child status".into());
            }
            let diagnostic = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            if !diagnostic.contains(if succeeds {
                "SKIPPED: optional PostgreSQL fixture"
            } else {
                "is missing or invalid"
            }) {
                return Err("unexpected PostgreSQL prerequisite diagnostic".into());
            }
        }
        Ok(())
    }
}
