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
fn seed(store: &BoxedHubStore, repo: &str, count: usize) -> String {
    store.create_repo(HubRepoType::Model, repo, false).unwrap();
    let sha = format!("{count:064x}");
    let files: Vec<_> = (0..count)
        .map(|i| shardline_index::hub::HubFileEntry {
            path: format!("f{i:06}"),
            size: 1,
            sha: "a".repeat(64),
            is_lfs: false,
        })
        .collect();
    store.store_files(&sha, &files).unwrap();
    store
        .create_revision(repo, None, &sha, "main", "fixture")
        .unwrap();
    sha
}
async fn contract_preupload_preserves_order_duplicate_paths_and_metadata(
    root: &std::path::Path,
    store: BoxedHubStore,
) {
    let repo = "owner/preupload";
    seed(&store, repo, 2);
    let before = snapshot(&store, repo);
    let app = router(root, store.clone());
    let body = serde_json::json!({"files":[{"path":"missing"},{"path":"f000001"},{"path":"f000000","lfs":true},{"path":"missing"},{"path":"f000001"},{"path":"F000001"},{"path":"f000001/child"}]});
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/models/{repo}/preupload/main"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let expected: Vec<_> = [("missing",false),("f000001",true),("f000000",true),("missing",false),("f000001",true),("F000001",false),("f000001/child",false)].into_iter().map(|(path,exists)|serde_json::json!({"path":path,"exists":exists,"uploadMode":"regular","shouldIgnore":false})).collect();
    assert_eq!(
        result,
        serde_json::json!({"files":expected,"result":expected})
    );
    let oversized = serde_json::json!({"files":vec![serde_json::json!({"path":"missing"});10001]});
    let oversized_response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/models/{repo}/preupload/main"))
                .header("content-type", "application/json")
                .body(Body::from(oversized.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oversized_response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(snapshot(&store, repo), before);
}
async fn contract_commit_at_read_ceiling_rejects_overflow_but_accepts_final_replacement(
    root: &std::path::Path,
    store: BoxedHubStore,
) {
    let repo = "owner/ceiling";
    let original = seed(&store, repo, shardline_index::hub::HUB_TREE_READ_CEILING);
    let app = router(root, store.clone());
    let before = snapshot(&store, repo);
    assert_eq!(
        commit(&app, repo, vec![add("extra")]).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(snapshot(&store, repo), before);
    assert_eq!(
        commit(&app, repo, vec![add("extra"), delete("f000000")]).await,
        StatusCode::OK
    );
    let head = store.resolve_revision(repo, "main").unwrap().unwrap();
    let files = store.get_files(&head).unwrap();
    assert_eq!(files.len(), shardline_index::hub::HUB_TREE_READ_CEILING);
    assert!(files.iter().any(|f| f.path == "extra"));
    assert!(!files.iter().any(|f| f.path == "f000000"));
    assert_eq!(
        store.get_files(&original).unwrap().len(),
        shardline_index::hub::HUB_TREE_READ_CEILING
    );
}

async fn contract_commit_updates_keep_last_instruction_and_canonical_tree_identity(
    root: &std::path::Path,
    store: BoxedHubStore,
) {
    let repo = "owner/operations";
    store.create_repo(HubRepoType::Model, repo, false).unwrap();
    let app = router(root, store.clone());
    let parent = store.resolve_revision(repo, "main").unwrap().unwrap();
    let lfs =
        |path: &str| serde_json::json!({"lfsFile":{"path":path,"oid":"b".repeat(64),"size":17}});
    assert_eq!(
        commit(
            &app,
            repo,
            vec![
                add("z"),
                delete("z"),
                lfs("z"),
                add("a"),
                lfs("a"),
                delete("a"),
                add("a"),
                add("gone"),
                delete("gone"),
                delete("absent")
            ]
        )
        .await,
        StatusCode::OK
    );
    let first = store.resolve_revision(repo, "main").unwrap().unwrap();
    let files = store.get_files(&first).unwrap();
    assert_eq!(files.len(), 2);
    let inline_file = files.first().unwrap();
    let lfs_file = files.get(1).unwrap();
    assert_eq!(inline_file.path, "a");
    assert!(!inline_file.is_lfs);
    assert_eq!(inline_file.sha, blake3::hash(b"a").to_hex().to_string());
    assert_eq!(lfs_file.path, "z");
    assert!(lfs_file.is_lfs);
    assert_eq!(lfs_file.sha, "b".repeat(64));
    assert_eq!(lfs_file.size, 17);
    store
        .create_revision(repo, None, &parent, "main", "reset fixture")
        .unwrap();
    assert_eq!(
        commit(
            &app,
            repo,
            vec![lfs("a"), delete("z"), add("a"), lfs("z"), delete("gone")]
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        store.resolve_revision(repo, "main").unwrap().unwrap(),
        first,
        "equivalent final trees retain canonical commit identity"
    );
}

#[tokio::test]
async fn preupload_preserves_order_duplicate_paths_and_metadata() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    contract_preupload_preserves_order_duplicate_paths_and_metadata(root.path(), store).await;
}

#[tokio::test]
async fn commit_at_read_ceiling_rejects_overflow_but_accepts_final_replacement() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    contract_commit_at_read_ceiling_rejects_overflow_but_accepts_final_replacement(
        root.path(),
        store,
    )
    .await;
}

#[tokio::test]
async fn commit_updates_keep_last_instruction_and_canonical_tree_identity() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    contract_commit_updates_keep_last_instruction_and_canonical_tree_identity(root.path(), store)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_commit_bounds_preupload_and_operation_matrix() {
    let Ok(url) = std::env::var("SHARDLINE_COMMIT_BOUNDS_TEST_DATABASE_URL") else {
        return;
    };
    let root = tempfile::TempDir::new().unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let store = BoxedHubStore::from_store(shardline_index::PostgresIndexStore::new(pool));
    contract_preupload_preserves_order_duplicate_paths_and_metadata(root.path(), store.clone())
        .await;
    contract_commit_at_read_ceiling_rejects_overflow_but_accepts_final_replacement(
        root.path(),
        store.clone(),
    )
    .await;
    contract_commit_updates_keep_last_instruction_and_canonical_tree_identity(
        root.path(),
        store.clone(),
    )
    .await;
}
