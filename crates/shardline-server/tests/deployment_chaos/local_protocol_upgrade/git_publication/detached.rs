//! Actual Git clone and strict verification after detached history accumulates.
use super::*;

#[tokio::test]
#[ignore = "requires actual Shardline executable and stock Git"]
async fn stock_git_clone_survives_ten_thousand_detached_revisions() {
    let binary = resolve_shardline_binary().expect("actual executable required");
    let root = TempDir::new().unwrap();
    let work = TempDir::new().unwrap();
    let storage = [("SHARDLINE_HUB_WEBHOOK_SECRET_KEY", "00".repeat(16))];
    migrate_metadata(&binary, root.path(), None).await;
    let mut server = LocalServer::spawn_with_storage(&binary, root.path(), None, "hub", &storage);
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
    drop(connection);
    eprintln!("DETACHED_HISTORY_INSERTED=10001");
    let mut restarted =
        LocalServer::spawn_with_storage(&binary, root.path(), None, "hub", &storage);
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
