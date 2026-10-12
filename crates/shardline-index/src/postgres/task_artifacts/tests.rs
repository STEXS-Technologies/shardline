#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
use super::*;

const PREFIX: &str = "_shardline_tasks/v1/";

#[tokio::test]
async fn owner_defaults_cannot_bypass_private_artifact_functions() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool);
    let schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let quoted_schema = format!("\"{}\"", schema.replace('"', "\"\""));
    let mut nonce = [0_u8; 8];
    getrandom::fill(&mut nonce).unwrap();
    let role = format!("artifact_default_{}", hex::encode(nonce));
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE \"{role}\"; ALTER DEFAULT PRIVILEGES IN SCHEMA {quoted_schema}
         GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO \"{role}\";"
    )))
    .execute(store.pool())
    .await
    .unwrap();
    store.initialize_task_history_scope().await.unwrap();
    let direct: bool = sqlx::query_scalar("SELECT has_table_privilege($1,format('%I.shardline_task_artifacts_v1',$2),'SELECT,INSERT,UPDATE,DELETE')")
        .bind(&role).bind(&schema).fetch_one(store.pool()).await.unwrap();
    assert!(!direct);
    // Declared owner defaults still apply to unrelated future tables.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {quoted_schema}.future_metadata(value TEXT)"
    )))
    .execute(store.pool())
    .await
    .unwrap();
    let retained: bool = sqlx::query_scalar("SELECT has_table_privilege($1,format('%I.future_metadata',$2),'SELECT,INSERT,UPDATE,DELETE')")
        .bind(&role).bind(&schema).fetch_one(store.pool()).await.unwrap();
    assert!(retained);
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP OWNED BY \"{role}\"; DROP ROLE \"{role}\";"
    )))
    .execute(store.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn immutable_publication_bounds_and_concurrent_conflicts() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool);
    store.initialize_task_history_scope().await.unwrap();
    let key = format!("{PREFIX}test-record");
    assert!(store.read_task_artifact(&key, 0).await.unwrap().is_none());
    store
        .publish_task_artifact(&key, b"exact signed bytes")
        .await
        .unwrap();
    store
        .publish_task_artifact(&key, b"exact signed bytes")
        .await
        .unwrap();
    assert!(
        store
            .publish_task_artifact(&key, b"replacement")
            .await
            .is_err()
    );
    assert_eq!(
        store.read_task_artifact(&key, 2).await.unwrap(),
        Some(PostgresTaskArtifact {
            length: 18,
            bytes: None
        })
    );
    assert_eq!(
        store
            .read_task_artifact(&key, 18)
            .await
            .unwrap()
            .unwrap()
            .bytes
            .as_deref(),
        Some(b"exact signed bytes".as_slice())
    );
    let racing = format!("{PREFIX}racing");
    let (first, second) = tokio::join!(
        store.publish_task_artifact(&racing, b"first"),
        store.publish_task_artifact(&racing, b"second")
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let winner = if first.is_ok() {
        b"first".as_slice()
    } else {
        b"second".as_slice()
    };
    let reopened = PostgresIndexStore::new(store.pool().clone());
    assert_eq!(
        reopened
            .read_task_artifact(&racing, 100)
            .await
            .unwrap()
            .unwrap()
            .bytes
            .as_deref(),
        Some(winner)
    );
    assert!(
        store
            .publish_task_artifact("payload/forbidden", b"body")
            .await
            .is_err()
    );
    assert!(store.delete_task_artifact(&key).await.unwrap());
    assert!(!store.delete_task_artifact(&key).await.unwrap());
}

#[tokio::test]
async fn literal_prefix_pages_allow_retention_without_loading_bodies() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool);
    store.initialize_task_history_scope().await.unwrap();
    let prefix = format!("{PREFIX}test%/");
    for index in 0..130 {
        store
            .publish_task_artifact(&format!("{prefix}{index:03}"), b"opaque")
            .await
            .unwrap();
    }
    let unrelated = format!("{PREFIX}testX/keep");
    store
        .publish_task_artifact(&unrelated, b"keep")
        .await
        .unwrap();
    let mut after: Option<String> = None;
    let mut count = 0_usize;
    'pages: loop {
        let page = store
            .task_artifact_page(&prefix, after.as_deref())
            .await
            .unwrap();
        assert!(page.len() <= 64);
        if page.is_empty() {
            break;
        }
        for (key, length) in page {
            if !key.starts_with(&prefix) {
                break 'pages;
            }
            assert_eq!(length, 6);
            assert!(store.delete_task_artifact(&key).await.unwrap());
            count = count.saturating_add(1);
            after = Some(key);
        }
    }
    assert_eq!(count, 130);
    assert_eq!(
        store
            .read_task_artifact(&unrelated, 4)
            .await
            .unwrap()
            .unwrap()
            .bytes
            .as_deref(),
        Some(b"keep".as_slice())
    );
}

#[tokio::test]
async fn owner_reinstallation_preserves_released_ledger_and_existing_grants() {
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool);
    let schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let ledger: Vec<String> = sqlx::query_scalar("SELECT row_to_json(ledger)::text FROM public.shardline_schema_migrations AS ledger ORDER BY version").fetch_all(store.pool()).await.unwrap();
    let acl_query = "SELECT target.relname, coalesce(target.relacl::text,'') FROM pg_class target JOIN pg_namespace namespace ON namespace.oid=target.relnamespace WHERE namespace.nspname=$1 AND target.relkind='r' AND target.relname <> 'shardline_task_artifacts_v1' ORDER BY target.relname";
    let grants: Vec<(String, String)> = sqlx::query_as(acl_query)
        .bind(&schema)
        .fetch_all(store.pool())
        .await
        .unwrap();
    store.initialize_task_history_scope().await.unwrap();
    let identity_query = "SELECT shardline_task_history_scope_v1(current_schema())";
    let identity: String = sqlx::query_scalar(identity_query)
        .fetch_one(store.pool())
        .await
        .unwrap();
    let key = format!("{PREFIX}retained");
    store
        .publish_task_artifact(&key, b"preserved")
        .await
        .unwrap();
    store.initialize_task_history_scope().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(identity_query)
            .fetch_one(store.pool())
            .await
            .unwrap(),
        identity
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(acl_query)
            .bind(&schema)
            .fetch_all(store.pool())
            .await
            .unwrap(),
        grants
    );
    assert_eq!(sqlx::query_scalar::<_, String>("SELECT row_to_json(ledger)::text FROM public.shardline_schema_migrations AS ledger ORDER BY version").fetch_all(store.pool()).await.unwrap(), ledger);
    assert_eq!(
        store
            .read_task_artifact(&key, 9)
            .await
            .unwrap()
            .unwrap()
            .bytes
            .as_deref(),
        Some(b"preserved".as_slice())
    );
    let rls = format!(
        "ALTER TABLE \"{}\".shardline_reliability_events ENABLE ROW LEVEL SECURITY",
        schema.replace('"', "\"\"")
    );
    sqlx::query(sqlx::AssertSqlSafe(rls))
        .execute(store.pool())
        .await
        .unwrap();
    let error = store.read_task_artifact(&key, 9).await.unwrap_err();
    assert!(
        matches!(error, PostgresMetadataStoreError::Sqlx(error) if error.as_database_error().and_then(sqlx::error::DatabaseError::code).as_deref() == Some("SLD04"))
    );
}

#[tokio::test]
async fn existing_role_permissions_control_publication_reads_and_retention() {
    use std::fmt::Write as _;
    use std::str::FromStr;
    let Some(pool) = super::super::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool);
    store.initialize_task_history_scope().await.unwrap();
    let schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let quoted_schema = format!("\"{}\"", schema.replace('"', "\"\""));
    let url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("SHARDLINE_INDEX_POSTGRES_URL"))
        .unwrap();
    let mut roles = Vec::new();
    for profile in ["reader", "writer", "gc", "unprivileged"] {
        let mut nonce = [0_u8; 8];
        getrandom::fill(&mut nonce).unwrap();
        let role = format!("artifact_{profile}_{}", hex::encode(nonce));
        let quoted_role = format!("\"{role}\"");
        let mut ddl = format!(
            "CREATE ROLE {quoted_role} LOGIN PASSWORD 'fixture_artifact_role'; GRANT USAGE ON SCHEMA {quoted_schema} TO {quoted_role};"
        );
        if profile != "unprivileged" {
            write!(
                ddl,
                "GRANT SELECT ON {quoted_schema}.shardline_reliability_events TO {quoted_role};"
            )
            .unwrap();
        }
        if profile == "writer" {
            write!(
                ddl,
                "GRANT INSERT ON {quoted_schema}.shardline_reliability_events TO {quoted_role};"
            )
            .unwrap();
        }
        if profile == "gc" {
            write!(ddl, "GRANT SELECT,DELETE ON {quoted_schema}.shardline_quarantine_candidates TO {quoted_role};").unwrap();
        }
        sqlx::raw_sql(sqlx::AssertSqlSafe(ddl))
            .execute(store.pool())
            .await
            .unwrap();
        let options = sqlx::postgres::PgConnectOptions::from_str(&url)
            .unwrap()
            .username(&role)
            .password("fixture_artifact_role")
            .options([("search_path", schema.as_str())]);
        let role_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        let direct: bool = sqlx::query_scalar("SELECT has_table_privilege(current_user, format('%I.shardline_task_artifacts_v1', current_schema()), 'SELECT,INSERT,DELETE')").fetch_one(&role_pool).await.unwrap();
        assert!(!direct);
        roles.push((profile, role, PostgresIndexStore::new(role_pool)));
    }
    let key = format!("{PREFIX}authorized");
    let reader = &roles[0].2;
    let writer = &roles[1].2;
    let gc = &roles[2].2;
    let unprivileged = &roles[3].2;
    writer
        .publish_task_artifact(&key, b"preserved")
        .await
        .unwrap();
    assert_eq!(
        reader
            .read_task_artifact(&key, 9)
            .await
            .unwrap()
            .unwrap()
            .bytes
            .as_deref(),
        Some(b"preserved".as_slice())
    );
    assert!(
        reader
            .publish_task_artifact(&key, b"preserved")
            .await
            .is_err()
    );
    assert!(reader.delete_task_artifact(&key).await.is_err());
    assert!(writer.delete_task_artifact(&key).await.is_err());
    assert!(unprivileged.read_task_artifact(&key, 9).await.is_err());
    assert!(unprivileged.task_artifact_page(PREFIX, None).await.is_err());
    gc.publish_task_artifact(&format!("{PREFIX}gc"), b"gc evidence")
        .await
        .unwrap();
    assert!(gc.delete_task_artifact(&key).await.unwrap());
    let spoof = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {quoted_schema}.shardline_task_artifact_read_v1($1,$2,$3)"
    )))
    .bind("public")
    .bind(&key)
    .bind(9_i64)
    .execute(reader.pool())
    .await;
    assert!(spoof.is_err());
    for (_, role, role_store) in roles {
        role_store.pool().close().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP OWNED BY \"{role}\"; DROP ROLE \"{role}\";"
        )))
        .execute(store.pool())
        .await
        .unwrap();
    }
}
