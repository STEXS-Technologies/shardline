#![allow(clippy::unwrap_used, clippy::indexing_slicing)]
use std::process::Command;

fn binary() -> std::ffi::OsString {
    std::env::var_os("CARGO_BIN_EXE_shardline")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_shardline").into())
}

#[test]
fn unsupported_pending_delete_inspection_is_explicit_and_creates_no_state() {
    let root = tempfile::tempdir().unwrap();
    let output = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .args(["history", "--pending-deletes", "--root"])
        .arg(root.path())
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let page: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(page["supported"], false);
    assert!(
        page["inspection_notice"]
            .as_str()
            .unwrap()
            .contains("does not prove")
    );
    assert_eq!(page["pending_deletes"], serde_json::json!([]));
    assert!(page["next_after"].is_null());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn pending_delete_cursor_errors_are_actionable_before_database_access() {
    let root = tempfile::tempdir().unwrap();
    let output = Command::new(binary())
        .env_clear()
        .current_dir(root.path())
        .env(
            "SHARDLINE_INDEX_POSTGRES_URL",
            "postgres://127.0.0.1:1/unavailable",
        )
        .args([
            "history",
            "--pending-deletes",
            "--after",
            "run-id",
            "--root",
        ])
        .arg(root.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--after must be an attempt number"));
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_pending_delete_cli_paginates_without_changing_attempts() {
    let Ok(database_url) = std::env::var("DATABASE_URL") else {
        eprintln!("SKIPPED: pending-delete CLI fixture requires DATABASE_URL");
        return;
    };
    let admin = sqlx::PgPool::connect(&database_url).await.unwrap();
    let schema = format!(
        "history_deletes_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let tables: Vec<String> = sqlx::query_scalar("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'shardline_%' ORDER BY tablename")
        .fetch_all(&admin).await.unwrap();
    for table in tables {
        let quoted = format!("\"{}\"", table.replace('"', "\"\""));
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {schema}.{quoted} (LIKE public.{quoted} INCLUDING ALL)"
        )))
        .execute(&admin)
        .await
        .unwrap();
    }
    let mut url = reqwest::Url::parse(&database_url).unwrap();
    let options = url
        .query_pairs()
        .find(|(key, _)| key == "options")
        .map_or_else(String::new, |(_, value)| value.into_owned());
    let parameters: Vec<_> = url
        .query_pairs()
        .filter(|(key, _)| key != "options")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    url.set_query(None);
    url.query_pairs_mut()
        .extend_pairs(parameters)
        .append_pair("options", &format!("{options} -csearch_path={schema}"));
    let pool = sqlx::PgPool::connect(url.as_str()).await.unwrap();
    let index = shardline_index::PostgresIndexStore::new(pool.clone());
    index.initialize_remote_delete_guard().await.unwrap();
    let keys = [
        format!("inspection/{}", "a".repeat(3500)),
        "inspection/second".to_owned(),
        "inspection/third".to_owned(),
    ];
    let mut permits = Vec::new();
    for key in &keys {
        permits.push(
            index
                .begin_remote_delete(&shardline_storage::ObjectKey::parse(key).unwrap())
                .await
                .unwrap(),
        );
    }
    let query = format!(
        "SELECT to_jsonb(marker)::text FROM {schema}.shardline_resource_fences marker WHERE domain='maintenance-remote-delete' ORDER BY resource"
    );
    let before: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(query.clone()))
        .fetch_all(&admin)
        .await
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut after: Option<String> = None;
    let mut found = Vec::new();
    loop {
        let mut command = Command::new(binary());
        command
            .env_clear()
            .current_dir(root.path())
            .env("SHARDLINE_INDEX_POSTGRES_URL", url.as_str())
            .args(["history", "--pending-deletes", "--limit", "1", "--root"])
            .arg(root.path());
        if let Some(cursor) = &after {
            command.args(["--after", cursor.as_str()]);
        }
        let output = command.output().unwrap();
        assert!(output.status.success(), "{output:?}");
        let page: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(page["supported"], true);
        assert!(page["inspection_notice"].is_null());
        assert_eq!(page["pending_deletes"].as_array().unwrap().len(), 1);
        let attempt = &page["pending_deletes"][0];
        assert_eq!(attempt["state"], "pending");
        assert!(
            attempt["next_step"]
                .as_str()
                .unwrap()
                .contains("shardline repair")
        );
        found.push(attempt["object_key"].as_str().unwrap().to_owned());
        after = page["next_after"].as_str().map(str::to_owned);
        if after.is_none() {
            break;
        }
        assert!(found.len() < keys.len());
    }
    assert_eq!(found, keys);
    let unchanged: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(query))
        .fetch_all(&admin)
        .await
        .unwrap();
    assert_eq!(unchanged, before);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    drop(permits);
    pool.close().await;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
