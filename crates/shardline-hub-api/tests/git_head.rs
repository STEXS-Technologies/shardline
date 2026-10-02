#![allow(clippy::unwrap_used, clippy::expect_used)]
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use shardline_hub_api::{hub_routes, routes::HubState};
use shardline_index::{
    LocalIndexStore,
    hub::{BoxedHubStore, EMPTY_HUB_REVISION, HubRepoType},
};
use shardline_server_core::ServerObjectStore;
use tower::ServiceExt;

fn app(root: &std::path::Path, store: BoxedHubStore) -> Router {
    hub_routes(
        HubState {
            store,
            object_store: ServerObjectStore::local(root.join("objects")).unwrap(),
            auth: None,
            http_client: None,
            webhook_secret_cipher: None,
            public_base_url: "http://localhost".to_owned(),
        },
        false,
    )
}
async fn head(app: &Router, repo: &str) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/models/owner/{repo}/HEAD"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    String::from_utf8(
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}
fn advertisement(sha: &str) -> String {
    format!("ref: refs/heads/main\n{sha} refs/heads/main\n")
}

#[tokio::test]
async fn head_tracks_live_main_after_rollback_and_ignores_other_branch_history() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    store
        .create_repo(HubRepoType::Model, "owner/repo", false)
        .unwrap();
    let a = "a".repeat(64);
    let b = "b".repeat(64);
    let c = "c".repeat(64);
    store
        .create_revision("owner/repo", None, &a, "main", "first")
        .unwrap();
    store
        .create_revision("owner/repo", Some(&a), &b, "main", "second")
        .unwrap();
    let app = app(root.path(), store.clone());
    assert_eq!(head(&app, "repo").await, advertisement(&b));
    store
        .create_revision("owner/repo", Some(&b), &a, "main", "rollback")
        .unwrap();
    assert_eq!(head(&app, "repo").await, advertisement(&a));
    store
        .create_revision("owner/repo", None, &c, "dev", "new branch")
        .unwrap();
    assert_eq!(head(&app, "repo").await, advertisement(&a));
    store.delete_ref("owner/repo", "dev", &c).unwrap();
    assert_eq!(head(&app, "repo").await, advertisement(&a));
    assert!(
        store.delete_ref("owner/repo", "main", &a).is_err(),
        "supported mutation forbids deleting default branch"
    );
    // Force deterministic timestamp ties on immutable history. HEAD remains a
    // live-ref lookup independent of SQL's historical row ordering.
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    connection.execute("UPDATE shardline_hub_revisions SET created_at_unix_seconds = 100 WHERE repo_id = 'owner/repo'",[]).unwrap();
    assert_eq!(head(&app, "repo").await, advertisement(&a));
    let history = store.list_revisions("owner/repo").unwrap();
    assert!(history.iter().any(|r| r.sha == b));
    assert!(history.iter().any(|r| r.sha == c));
}

#[tokio::test]
async fn empty_repo_and_missing_repository_keep_existing_head_contract() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    store
        .create_repo(HubRepoType::Model, "owner/empty", false)
        .unwrap();
    let app = app(root.path(), store.clone());
    assert_eq!(head(&app, "empty").await, advertisement(EMPTY_HUB_REVISION));
    assert_eq!(head(&app, "absent").await, advertisement(&"0".repeat(40)));
    store.delete_repo("owner/empty").unwrap();
    assert_eq!(head(&app, "empty").await, advertisement(&"0".repeat(40)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_head_tracks_live_ref_after_rollback_and_timestamp_ties() {
    let Ok(url) = std::env::var("SHARDLINE_HEAD_TEST_DATABASE_URL") else {
        return;
    };
    let root = tempfile::TempDir::new().unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let store = BoxedHubStore::from_store(shardline_index::PostgresIndexStore::new(pool.clone()));
    store
        .create_repo(HubRepoType::Model, "owner/repo", false)
        .unwrap();
    let a = "a".repeat(64);
    let b = "b".repeat(64);
    let c = "c".repeat(64);
    store
        .create_revision("owner/repo", None, &a, "main", "first")
        .unwrap();
    store
        .create_revision("owner/repo", Some(&a), &b, "main", "second")
        .unwrap();
    let app = app(root.path(), store.clone());
    assert_eq!(head(&app, "repo").await, advertisement(&b));
    store
        .create_revision("owner/repo", Some(&b), &a, "main", "rollback")
        .unwrap();
    assert_eq!(head(&app, "repo").await, advertisement(&a));
    store
        .create_revision("owner/repo", None, &c, "dev", "new branch")
        .unwrap();
    assert_eq!(head(&app, "repo").await, advertisement(&a));
    store.delete_ref("owner/repo", "dev", &c).unwrap();
    assert_eq!(head(&app, "repo").await, advertisement(&a));
    assert!(store.delete_ref("owner/repo", "main", &a).is_err());
    sqlx::query(
        "UPDATE shardline_hub_revisions SET created_at_unix_seconds=100 WHERE repo_id='owner/repo'",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(head(&app, "repo").await, advertisement(&a));
    store
        .create_repo(HubRepoType::Model, "owner/empty", false)
        .unwrap();
    assert_eq!(head(&app, "empty").await, advertisement(EMPTY_HUB_REVISION));
    assert_eq!(head(&app, "absent").await, advertisement(&"0".repeat(40)));
    store.delete_repo("owner/empty").unwrap();
    assert_eq!(head(&app, "empty").await, advertisement(&"0".repeat(40)));
}
