//! Actual Git clone and strict verification after detached history accumulates.
use super::*;

#[tokio::test]
#[ignore = "requires actual Shardline executable and stock Git"]
async fn stock_git_clone_survives_ten_thousand_detached_revisions() {
    exercise(None, &[]).await;
}

#[cfg(feature = "docker")]
#[tokio::test]
#[ignore = "requires actual executable, stock Git and owned PostgreSQL/MinIO"]
async fn remote_stock_git_clone_survives_ten_thousand_detached_revisions() {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()
        .unwrap()
        .expect("Docker is required");
    let database = services.postgres_url().unwrap();
    let prefix = services.unique_s3_key_prefix("git-detached-history");
    let s3 = services.s3_raw_config(Some(&prefix)).unwrap();
    let storage = [
        ("SHARDLINE_OBJECT_STORAGE_ADAPTER", "s3".to_owned()),
        ("SHARDLINE_S3_BUCKET", s3.bucket),
        ("SHARDLINE_S3_REGION", s3.region),
        ("SHARDLINE_S3_ENDPOINT", s3.endpoint.unwrap()),
        ("SHARDLINE_S3_ACCESS_KEY_ID", s3.access_key.unwrap()),
        ("SHARDLINE_S3_SECRET_ACCESS_KEY", s3.secret_key.unwrap()),
        ("SHARDLINE_S3_KEY_PREFIX", prefix),
        ("SHARDLINE_S3_ALLOW_HTTP", "true".to_owned()),
    ];
    exercise(Some(&database), &storage).await;
}

async fn exercise(database: Option<&str>, storage: &[(&str, String)]) {
    let binary = resolve_shardline_binary().expect("actual executable required");
    let root = TempDir::new().unwrap();
    let work = TempDir::new().unwrap();
    let mut storage = storage.to_vec();
    storage.push(("SHARDLINE_HUB_WEBHOOK_SECRET_KEY", "00".repeat(16)));
    migrate_metadata(&binary, root.path(), database).await;
    let mut server =
        LocalServer::spawn_with_storage(&binary, root.path(), database, "hub", &storage);
    server.ready().await;
    let token = mint_token("drill", "drill", TokenScope::Write);
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
        .post(format!("{}/api/repos/create", server.base))
        .bearer_auth(&token)
        .json(&serde_json::json!({"type":"model", "name":"drill/drill"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let url = format!("{}/models/drill/drill", server.base);
    success(work.path(), &token, &["clone", &url, "original"]).await;
    let original = work.path().join("original");
    std::fs::write(
        original.join("payload.txt"),
        b"reachable history remains intact\n",
    )
    .unwrap();
    success(&original, &token, &["add", "payload.txt"]).await;
    success(
        &original,
        &token,
        &["-c", "commit.gpgsign=false", "commit", "-m", "reachable"],
    )
    .await;
    success(&original, &token, &["push", "origin", "HEAD:main"]).await;
    let expected = success(&original, &token, &["rev-parse", "HEAD"]).await;
    drop(server);

    if let Some(database) = database {
        let pool = sqlx::PgPool::connect(database).await.unwrap();
        sqlx::query(
            "INSERT INTO shardline_hub_revisions (repo_id,ref_name,sha,parent_sha,message,created_at_unix_seconds)
             SELECT 'drill/drill','detached',lpad(to_hex(value),64,'0'),NULL,'detached',42
             FROM generate_series(0,10000) AS value",
        ).execute(&pool).await.unwrap();
        pool.close().await;
    } else {
        let mut connection =
            rusqlite::Connection::open(root.path().join("hub/metadata.sqlite3")).unwrap();
        let transaction = connection.transaction().unwrap();
        for number in 0..10_001 {
            transaction.execute(
                "INSERT INTO shardline_hub_revisions (repo_id,ref_name,sha,parent_sha,message,created_at_unix_seconds) VALUES ('drill/drill','detached',?1,NULL,'detached',42)",
                [format!("{number:064x}")],
            ).unwrap();
        }
        transaction.commit().unwrap();
    }
    eprintln!("DETACHED_HISTORY_INSERTED=10001");
    let mut restarted =
        LocalServer::spawn_with_storage(&binary, root.path(), database, "hub", &storage);
    restarted.ready().await;
    let url = format!("{}/models/drill/drill", restarted.base);
    success(work.path(), &token, &["clone", &url, "recovered"]).await;
    let recovered = work.path().join("recovered");
    assert_eq!(
        success(&recovered, &token, &["rev-parse", "HEAD"]).await,
        expected
    );
    success(&recovered, &token, &["fsck", "--strict"]).await;
    assert_eq!(
        std::fs::read(recovered.join("payload.txt")).unwrap(),
        b"reachable history remains intact\n"
    );
}
