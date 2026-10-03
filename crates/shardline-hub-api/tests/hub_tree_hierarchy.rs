#![allow(clippy::unwrap_used, clippy::expect_used)]
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use shardline_hub_api::{hub_routes, routes::HubState};
use shardline_index::{
    LocalIndexStore,
    hub::{BoxedHubStore, HubRepoType},
};
use shardline_server_core::ServerObjectStore;
use tower::ServiceExt;
fn router(root: &std::path::Path, store: BoxedHubStore) -> Router {
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
fn add(path: &str) -> serde_json::Value {
    serde_json::json!({"file":{"path":path,"content":"YQ=="}})
}
fn delete(path: &str) -> serde_json::Value {
    serde_json::json!({"deletedEntry":{"path":path}})
}
async fn commit(app: &Router, repo: &str, instructions: Vec<serde_json::Value>) -> StatusCode {
    let mut lines =
        vec![serde_json::json!({"header":{"message":"hierarchy regression"}}).to_string()];
    lines.extend(instructions.into_iter().map(|r| r.to_string()));
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/models/{repo}/commit/main"))
                .header("content-type", "application/x-ndjson")
                .body(Body::from(lines.join("\n")))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}
fn snapshot(store: &BoxedHubStore, repo: &str) -> serde_json::Value {
    let head = store.resolve_revision(repo, "main").unwrap().unwrap();
    let mut history = store.list_revisions(repo).unwrap();
    history.sort_by(|a, b| a.sha.cmp(&b.sha));
    let history: Vec<_> = history
        .into_iter()
        .map(|r| {
            serde_json::json!([
                r.repo_id,
                r.ref_name,
                r.sha,
                r.parent_sha,
                r.message,
                r.created_at_unix_seconds
            ])
        })
        .collect();
    let files: Vec<_> = store
        .get_files(&head)
        .unwrap()
        .into_iter()
        .map(|f| serde_json::json!([f.path, f.size, f.sha, f.is_lfs]))
        .collect();
    serde_json::json!({"head":head,"history":history,"files":files})
}
async fn contract(app: Router, store: BoxedHubStore) {
    for (index, paths) in [
        vec!["x", "x/a"],
        vec!["x/a", "x"],
        vec!["x", "x-foo", "x/a"],
        vec!["σ", "σ/a"],
    ]
    .into_iter()
    .enumerate()
    {
        let repo = format!("owner/same-{index}");
        store.create_repo(HubRepoType::Model, &repo, false).unwrap();
        let before = snapshot(&store, &repo);
        assert_eq!(
            commit(&app, &repo, paths.into_iter().map(add).collect()).await,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            snapshot(&store, &repo),
            before,
            "rejection must preserve current ref, tree and immutable history"
        );
    }
    for (index, (parent, new)) in [("x", "x/a"), ("x/a", "x")].into_iter().enumerate() {
        let repo = format!("owner/parent-{index}");
        store.create_repo(HubRepoType::Model, &repo, false).unwrap();
        assert_eq!(commit(&app, &repo, vec![add(parent)]).await, StatusCode::OK);
        let before = snapshot(&store, &repo);
        assert_eq!(
            commit(&app, &repo, vec![add("x-foo"), add(new)]).await,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(snapshot(&store, &repo), before);
    }
    let repo = "owner/replacement";
    store.create_repo(HubRepoType::Model, repo, false).unwrap();
    assert_eq!(
        commit(&app, repo, vec![add("x"), add("xy/a")]).await,
        StatusCode::OK,
        "similar prefix is valid"
    );
    let original = store.resolve_revision(repo, "main").unwrap().unwrap();
    // Validate the final tree, permitting an intermediate conflict removed by
    // a later instruction in the same acknowledged commit.
    assert_eq!(
        commit(&app, repo, vec![add("x/a"), add("x/b"), delete("x")]).await,
        StatusCode::OK
    );
    assert_eq!(
        commit(&app, repo, vec![add("x"), delete("x/a"), delete("x/b")]).await,
        StatusCode::OK
    );
    let head = store.resolve_revision(repo, "main").unwrap().unwrap();
    let paths: Vec<_> = store
        .get_files(&head)
        .unwrap()
        .into_iter()
        .map(|f| f.path)
        .collect();
    assert_eq!(paths, vec!["x", "xy/a"]);
    assert_eq!(
        store.get_files(&original).unwrap().len(),
        2,
        "valid replacement must retain earlier immutable tree"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_hub_path_conflicts_are_rejected_before_revision_publication() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    contract(router(root.path(), store.clone()), store).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_hub_path_conflicts_are_rejected_before_revision_publication() {
    let Ok(url) = std::env::var("SHARDLINE_TREE_HIERARCHY_TEST_DATABASE_URL") else {
        return;
    };
    let root = tempfile::TempDir::new().unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let store = BoxedHubStore::from_store(shardline_index::PostgresIndexStore::new(pool));
    contract(router(root.path(), store.clone()), store).await;
}
