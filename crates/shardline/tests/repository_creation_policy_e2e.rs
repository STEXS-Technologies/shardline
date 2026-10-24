#![allow(clippy::unwrap_used)]
use shardline_index::{
    LocalIndexStore,
    hub::{HubCreationOutcome, HubRepoType, HubStore},
};
use std::{
    path::Path,
    process::{Command, Output},
};

fn command(root: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_shardline"));
    command
        .env_clear()
        .current_dir(root)
        .args([
            "admin",
            "repository-policy",
            "--namespace",
            "alice",
            "--root",
        ])
        .arg(root);
    command
}

fn json(output: Output) -> serde_json::Value {
    let Output { status, stdout, .. } = output;
    assert!(status.success(), "owner policy command failed");
    serde_json::from_slice(&stdout).unwrap()
}

#[test]
fn owner_can_inspect_apply_and_clear_policy_without_changing_repositories() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalIndexStore::new(root.path().join("hub")).unwrap();
    assert!(json(command(root.path()).output().unwrap()).is_null());
    let input = root.path().join("policy.json");
    std::fs::write(&input, r#"{"max_repositories":1}"#).unwrap();
    let policy = json(
        command(root.path())
            .arg("--policy-file")
            .arg(&input)
            .output()
            .unwrap(),
    );
    assert_eq!(policy.get("max_repositories"), Some(&serde_json::json!(1)));
    assert!(matches!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/one", true, None)
            .unwrap(),
        HubCreationOutcome::Created(_)
    ));
    assert!(matches!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/two", false, None)
            .unwrap(),
        HubCreationOutcome::QuotaExceeded
    ));
    std::fs::write(&input, r#"{"enabled":false}"#).unwrap();
    assert_eq!(
        json(
            command(root.path())
                .arg("--policy-file")
                .arg(&input)
                .output()
                .unwrap()
        )
        .get("enabled"),
        Some(&serde_json::json!(false))
    );
    assert!(matches!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/one", false, None)
            .unwrap(),
        HubCreationOutcome::Denied
    ));
    assert!(json(command(root.path()).arg("--clear").output().unwrap()).is_null());
    assert!(store.get_repo("alice/one").unwrap().unwrap().private);
    store
        .create_repo(HubRepoType::Model, "alice/two", false)
        .unwrap();
    assert_eq!(store.list_repos().unwrap().len(), 2);
}

#[test]
fn invalid_policy_is_rejected_before_database_access() {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("policy.json");
    for contents in [
        r#"{"max_repositories":-1}"#.to_owned(),
        r#"{"window_seconds":0}"#.to_owned(),
        r#"{"unknown_setting":true}"#.to_owned(),
        " ".repeat(65537),
    ] {
        std::fs::write(&input, contents).unwrap();
        let output = command(root.path())
            .arg("--policy-file")
            .arg(&input)
            .env(
                "SHARDLINE_INDEX_POSTGRES_URL",
                "postgres://127.0.0.1:1/unavailable",
            )
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(!root.path().join("metadata.sqlite3").exists());
    }
    let output = command(root.path())
        .arg("--clear")
        .arg("--policy-file")
        .arg(&input)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
}

#[tokio::test]
#[ignore = "requires configured disposable PostgreSQL"]
async fn owner_policy_cli_uses_the_shared_postgres_deployment() {
    let database_url = std::env::var("DATABASE_URL").unwrap();
    let admin = sqlx::PgPool::connect(&database_url).await.unwrap();
    let schema = format!(
        "policy_cli_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let mut url = url::Url::parse(&database_url).unwrap();
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let root = tempfile::tempdir().unwrap();
    let migrate = Command::new(env!("CARGO_BIN_EXE_shardline"))
        .env_clear()
        .current_dir(root.path())
        .args(["db", "migrate", "up", "--database-url"])
        .arg(url.as_str())
        .output()
        .unwrap();
    assert!(migrate.status.success(), "owner migration must succeed");
    let input = root.path().join("policy.json");
    std::fs::write(
        &input,
        r#"{"max_repositories":2,"max_creations":3,"window_seconds":60}"#,
    )
    .unwrap();
    let applied = json(
        command(root.path())
            .env("SHARDLINE_INDEX_POSTGRES_URL", url.as_str())
            .arg("--policy-file")
            .arg(&input)
            .output()
            .unwrap(),
    );
    assert_eq!(applied.get("max_repositories"), Some(&serde_json::json!(2)));
    let inspected = json(
        command(root.path())
            .env("SHARDLINE_INDEX_POSTGRES_URL", url.as_str())
            .output()
            .unwrap(),
    );
    assert_eq!(inspected, applied);
    let configured: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT configured FROM {schema}.shardline_hub_creation_policies WHERE namespace='alice'"
    )))
    .fetch_one(&admin)
    .await
    .unwrap();
    assert!(configured);
    assert!(
        json(
            command(root.path())
                .env("SHARDLINE_INDEX_POSTGRES_URL", url.as_str())
                .arg("--clear")
                .output()
                .unwrap()
        )
        .is_null()
    );
    assert!(!root.path().join("metadata.sqlite3").exists());
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
