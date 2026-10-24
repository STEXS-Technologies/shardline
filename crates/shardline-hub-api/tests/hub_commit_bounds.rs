#![allow(clippy::unwrap_used, clippy::expect_used)]
mod batching;
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
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
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
fn seed(root: &std::path::Path, store: &BoxedHubStore, repo: &str, count: usize) -> String {
    store.create_repo(HubRepoType::Model, repo, false).unwrap();
    let content_hash = blake3::hash(b"a");
    let content_id = content_hash.to_hex().to_string();
    // Seed real immutable content: Git identity projection validates parent
    // payloads, so metadata-only fake hashes cannot form a healthy tree.
    ServerObjectStore::local(root.join("objects"))
        .unwrap()
        .put_if_absent(
            &ObjectKey::parse(&format!("protocols/lfs/global/objects/{content_id}")).unwrap(),
            ObjectBody::from_slice(b"a"),
            &ObjectIntegrity::new(
                shardline_protocol::ShardlineHash::from_bytes(*content_hash.as_bytes()),
                1,
            ),
        )
        .unwrap();
    let sha = format!("{count:064x}");
    let files: Vec<_> = (0..count)
        .map(|i| shardline_index::hub::HubFileEntry {
            path: format!("f{i:06}"),
            size: 1,
            sha: content_id.clone(),
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
    seed(root, &store, repo, 2);
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
    let expected: Vec<_> = [("missing",false),("f000001",true),("f000000",true),("missing",false),("f000001",true),("F000001",false),("f000001/child",false)].into_iter().map(|(path,exists)|serde_json::json!({"path":path,"exists":exists,"uploadMode":if path == "f000000" { "lfs" } else { "regular" },"shouldIgnore":false})).collect();
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
    let original = seed(
        root,
        &store,
        repo,
        shardline_index::hub::HUB_TREE_READ_CEILING,
    );
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

async fn contract_incomplete_stream_never_publishes(root: &std::path::Path, store: BoxedHubStore) {
    use axum::body::Bytes;
    use futures_util::StreamExt;
    let repo = "owner/stream-errors";
    seed(root, &store, repo, 1);
    let before = snapshot(&store, repo);
    let app = router(root, store.clone());
    let prefix = format!(
        "{}\n{}\n",
        serde_json::json!({"header":{"message":"must not publish"}}),
        add("new-file")
    );
    for failure in ["malformed", "disconnected", "oversized-line"] {
        let mut frames = vec![Ok(Bytes::from(prefix.clone()))];
        if failure == "disconnected" {
            frames.push(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "injected disconnect",
            )));
        } else if failure == "oversized-line" {
            frames.push(Ok(Bytes::from(vec![b' '; 16 * 1024 * 1024 + 1])));
        } else {
            frames.push(Ok(Bytes::from_static(b"{invalid trailing JSON")));
        }
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/models/{repo}/commit/main"))
                    .header("content-type", "application/x-ndjson")
                    .body(Body::from_stream(futures_util::stream::iter(frames)))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            snapshot(&store, repo),
            before,
            "failed body must not publish a partial tree"
        );
    }
    let frames =
        futures_util::stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"{bad JSON}\n"))])
            .chain(futures_util::stream::pending());
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/models/{repo}/commit/main"))
                .header("content-type", "application/x-ndjson")
                .body(Body::from_stream(frames))
                .unwrap(),
        ),
    )
    .await
    .expect("parser error must stop a stalled body")
    .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(snapshot(&store, repo), before);
}

#[tokio::test]
async fn incomplete_stream_never_publishes() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    contract_incomplete_stream_never_publishes(root.path(), store).await;
}

async fn contract_inline_size_boundary(root: &std::path::Path, store: BoxedHubStore) {
    use base64::Engine;
    let repo = "owner/inline-size-boundary";
    seed(root, &store, repo, 0);
    let app = router(root, store.clone());
    let ceiling = 10 * 1024 * 1024;
    for size in [ceiling, ceiling + 1] {
        let before = snapshot(&store, repo);
        let payload = vec![b'k'; size];
        let instruction = serde_json::json!({"file":{"path":"boundary.bin", "content":base64::engine::general_purpose::STANDARD.encode(&payload)}});
        let status = commit(&app, repo, vec![instruction]).await;
        if size == ceiling {
            assert_eq!(status, StatusCode::OK);
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/{repo}/resolve/main/boundary.bin"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), ceiling + 1)
                .await
                .unwrap();
            assert_eq!(bytes.as_ref(), payload);
        } else {
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(snapshot(&store, repo), before);
        }
    }
}

#[tokio::test]
async fn inline_size_boundary() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    contract_inline_size_boundary(root.path(), store).await;
}

async fn contract_projection_depth_rejection_preserves_ref(
    root: &std::path::Path,
    store: BoxedHubStore,
) {
    let repo = "owner/projection-depth";
    seed(root, &store, repo, 1);
    let app = router(root, store.clone());
    let before = snapshot(&store, repo);
    let invalid = vec!["d"; 129].join("/");
    assert_eq!(
        commit(&app, repo, vec![add(&invalid)]).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(snapshot(&store, repo), before);
    let boundary = vec!["d"; 128].join("/");
    assert_eq!(
        commit(&app, repo, vec![add(&boundary)]).await,
        StatusCode::OK
    );
    let head = store.resolve_revision(repo, "main").unwrap().unwrap();
    assert!(
        store
            .get_files(&head)
            .unwrap()
            .iter()
            .any(|f| f.path == boundary)
    );
}

#[tokio::test]
async fn projection_depth_rejection_preserves_ref() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    contract_projection_depth_rejection_preserves_ref(root.path(), store).await;
}

async fn contract_archive_failure_preserves_ref(root: &std::path::Path, store: BoxedHubStore) {
    let repo = "owner/archive-failure";
    let parent = seed(root, &store, repo, 1);
    let hash = blake3::hash(b"a").to_hex().to_string();
    let tree = [("f000000", 1_u64, &hash, false), ("new", 1, &hash, false)];
    let identity = serde_json::to_vec(&(
        "shardline-hub-ndjson-commit-v2",
        repo,
        &parent,
        "hierarchy regression",
        tree,
    ))
    .unwrap();
    let internal = blake3::hash(&identity).to_hex().to_string();
    // Block this exact new forward identity archive, after parent/tree
    // preparation succeeds. A directory cannot be an immutable object file.
    let blocked = root
        .join("objects/protocols/hub/git/global")
        .join(blake3::hash(repo.as_bytes()).to_hex().as_str())
        .join("identities-v1")
        .join(&internal);
    std::fs::create_dir_all(&blocked).unwrap();
    let app = router(root, store.clone());
    let before = snapshot(&store, repo);
    assert_eq!(
        commit(&app, repo, vec![add("new")]).await,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(snapshot(&store, repo), before);
    assert!(store.get_revision(repo, &internal).unwrap().is_none());
    assert!(store.get_files(&internal).unwrap().is_empty());
    // Removing the injected fault permits an identical safe retry.
    std::fs::remove_dir(&blocked).unwrap();
    assert_eq!(commit(&app, repo, vec![add("new")]).await, StatusCode::OK);
    assert_eq!(
        store.resolve_revision(repo, "main").unwrap().as_deref(),
        Some(internal.as_str())
    );
}

async fn contract_prepared_metadata_conflict_rolls_back(
    root: &std::path::Path,
    store: BoxedHubStore,
) {
    use shardline_index::hub::{HubRefUpdateOutcome, HubRevision};
    let repo = "owner/prepared-conflict";
    let parent = seed(root, &store, repo, 1);
    let revision = HubRevision {
        repo_id: repo.to_owned(),
        ref_name: "main".to_owned(),
        sha: "e".repeat(64),
        parent_sha: Some(parent.clone()),
        message: Some("prepared".to_owned()),
        created_at_unix_seconds: 1_700_000_123,
    };
    assert!(matches!(
        store.publish_prepared_revision(&revision).unwrap(),
        HubRefUpdateOutcome::Updated(_)
    ));
    assert_eq!(
        store
            .get_revision(repo, &revision.sha)
            .unwrap()
            .unwrap()
            .created_at_unix_seconds,
        revision.created_at_unix_seconds
    );
    // Replay from another existing branch must not change immutable metadata.
    store
        .create_revision(repo, Some(&parent), &parent, "other", "fixture")
        .unwrap();
    let mut conflicting = revision.clone();
    conflicting.ref_name = "other".to_owned();
    conflicting.created_at_unix_seconds = 1_700_000_124;
    let before = snapshot(&store, repo);
    assert!(store.publish_prepared_revision(&conflicting).is_err());
    assert_eq!(snapshot(&store, repo), before);
    assert_eq!(
        store.resolve_revision(repo, "other").unwrap().as_deref(),
        Some(parent.as_str())
    );
    conflicting.created_at_unix_seconds = revision.created_at_unix_seconds;
    assert!(matches!(
        store.publish_prepared_revision(&conflicting).unwrap(),
        HubRefUpdateOutcome::Updated(_)
    ));
    let count = store.list_revisions(repo).unwrap().len();
    assert!(matches!(
        store.publish_prepared_revision(&conflicting).unwrap(),
        HubRefUpdateOutcome::Conflict
    ));
    assert_eq!(store.list_revisions(repo).unwrap().len(), count);
}

#[tokio::test]
async fn archive_failure_preserves_ref() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    contract_archive_failure_preserves_ref(root.path(), store).await;
}

async fn contract_unique_object_limit_preserves_ref(root: &std::path::Path, store: BoxedHubStore) {
    use shardline_index::hub::HubFileEntry;
    let repo = "owner/object-limit";
    store.create_repo(HubRepoType::Model, repo, false).unwrap();
    let parent = store.resolve_revision(repo, "main").unwrap().unwrap();
    // 99,998 unique pointer blobs + root tree + commit fit exactly. The
    // file-count ceiling alone cannot protect the distinct Git-object budget.
    let files: Vec<_> = (0..99_998)
        .map(|i| HubFileEntry {
            path: format!("f{i:06}"),
            size: 1,
            sha: format!("{i:064x}"),
            is_lfs: true,
        })
        .collect();
    let sha = "d".repeat(64);
    store.store_files(&sha, &files).unwrap();
    store
        .create_revision(repo, Some(&parent), &sha, "main", "fixture")
        .unwrap();
    let app = router(root, store.clone());
    let before = snapshot(&store, repo);
    let pointer =
        |oid: String| serde_json::json!({"lfsFile":{"path":"extra", "oid":oid, "size":1}});
    assert_eq!(
        commit(&app, repo, vec![pointer("f".repeat(64))]).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(snapshot(&store, repo), before);
    // A further path sharing an existing blob is legal: count actual unique
    // objects, not a conservative file/directory upper bound.
    assert_eq!(
        commit(&app, repo, vec![pointer("0".repeat(64))]).await,
        StatusCode::OK
    );
}

#[tokio::test]
async fn unique_object_limit_preserves_ref() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    contract_unique_object_limit_preserves_ref(root.path(), store).await;
}

#[tokio::test]
async fn oversized_single_git_tree_preserves_ref() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    let repo = "owner/tree-bytes";
    store.create_repo(HubRepoType::Model, repo, false).unwrap();
    let before = snapshot(&store, repo);
    let app = router(root.path(), store.clone());
    // Each individual path is valid and all files share one pointer blob.
    // The resulting root tree nevertheless exceeds the 64 MiB object cap.
    let suffix = "x".repeat(700);
    let instructions = (0..99_999)
        .map(|i| {
            serde_json::json!({
                "lfsFile":{"path":format!("f{i:06}-{suffix}"), "oid":"0".repeat(64), "size":1}
            })
        })
        .collect();
    let status = commit(&app, repo, instructions).await;
    assert_eq!(snapshot(&store, repo), before);
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn prepared_metadata_conflict_rolls_back() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    contract_prepared_metadata_conflict_rolls_back(root.path(), store).await;
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
    let Some(url) =
        shardline_test_support::postgres_fixture_url("SHARDLINE_COMMIT_BOUNDS_TEST_DATABASE_URL")
    else {
        return;
    };
    let root = tempfile::TempDir::new().unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let store = BoxedHubStore::from_store(shardline_index::PostgresIndexStore::new(pool.clone()));
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
    contract_incomplete_stream_never_publishes(root.path(), store.clone()).await;
    contract_inline_size_boundary(root.path(), store.clone()).await;
    contract_projection_depth_rejection_preserves_ref(root.path(), store.clone()).await;
    contract_archive_failure_preserves_ref(root.path(), store.clone()).await;
    contract_prepared_metadata_conflict_rolls_back(root.path(), store.clone()).await;
    contract_unique_object_limit_preserves_ref(root.path(), store.clone()).await;
    batching::contract(&store, &pool).await;
}
