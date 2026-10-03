#![allow(clippy::unwrap_used, clippy::expect_used)]
use axum::{Router, body::Body, http::Request};
use shardline_hub_api::{hub_routes, routes::HubState};
use shardline_index::{
    LocalIndexStore,
    hub::{BoxedHubStore, HubRefCreateOutcome, HubRepoType, canonical_ref_name},
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
async fn request(
    app: &Router,
    method: &str,
    uri: &str,
    content_type: &str,
    body: String,
) -> (u16, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", content_type)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}
fn command(old: &str, new: &str, name: &str) -> String {
    let line = format!("{old} {new} {name}\0report-status\n");
    format!("{:04x}{line}0000", line.len().checked_add(4).unwrap())
}
fn oid(advertisement: &str, name: &str) -> String {
    let end = advertisement.find(&format!(" {name}")).unwrap();
    advertisement
        .get(end.checked_sub(40).unwrap()..end)
        .unwrap()
        .to_owned()
}
fn metadata(store: &BoxedHubStore, repo: &str) -> serde_json::Value {
    let mut history: Vec<_> = store
        .list_revisions(repo)
        .unwrap()
        .into_iter()
        .map(|r| {
            serde_json::json!([
                r.repo_id,
                r.sha,
                r.ref_name,
                r.parent_sha,
                r.message,
                r.created_at_unix_seconds
            ])
        })
        .collect();
    history.sort_by_key(serde_json::Value::to_string);
    let refs: Vec<_> = store
        .list_refs(repo)
        .unwrap()
        .into_iter()
        .map(|r| serde_json::json!([r.ref_name, r.sha]))
        .collect();
    serde_json::json!({"history":history,"refs":refs})
}
fn atomic_contract(store: &BoxedHubStore) {
    let repo = "owner/atomic";
    store.create_repo(HubRepoType::Model, repo, false).unwrap();
    let runtime = tokio::runtime::Handle::current();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let results = std::thread::scope(|scope| {
        let workers = ["a".repeat(64), "b".repeat(64)].map(|sha| {
            let store = store.clone();
            let barrier = barrier.clone();
            let runtime = runtime.clone();
            scope.spawn(move || {
                let _runtime_guard = runtime.enter();
                barrier.wait();
                let result = store
                    .create_revision_if_absent(repo, None, &sha, "feature", "race")
                    .unwrap();
                (sha, result)
            })
        });
        workers
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(
        results
            .iter()
            .filter(|(_, r)| matches!(r, HubRefCreateOutcome::Created(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|(_, r)| matches!(r, HubRefCreateOutcome::AlreadyExists))
            .count(),
        1
    );
    let winner = results
        .iter()
        .find(|(_, r)| matches!(r, HubRefCreateOutcome::Created(_)))
        .unwrap();
    let loser = results
        .iter()
        .find(|(_, r)| matches!(r, HubRefCreateOutcome::AlreadyExists))
        .unwrap();
    assert_eq!(
        store.resolve_revision(repo, "feature").unwrap().as_deref(),
        Some(winner.0.as_str())
    );
    assert!(
        !store
            .list_revisions(repo)
            .unwrap()
            .iter()
            .any(|r| r.sha == loser.0)
    );
    assert_eq!(store.list_revisions(repo).unwrap().len(), 2);
    let before = metadata(store, repo);
    assert!(matches!(
        store
            .create_revision_if_absent(repo, None, &"c".repeat(64), "refs/heads/feature", "loser")
            .unwrap(),
        HubRefCreateOutcome::AlreadyExists
    ));
    assert_eq!(metadata(store, repo), before);
    assert!(store.delete_ref(repo, "feature", &loser.0).is_err());
    assert_eq!(metadata(store, repo), before);
}
async fn http_contract(root: &std::path::Path, store: BoxedHubStore) {
    atomic_contract(&store);
    let repo = "owner/http";
    store.create_repo(HubRepoType::Model, repo, false).unwrap();
    let app = router(root, store.clone());
    for (path, content) in [("a", "YQ=="), ("b", "Yg==")] {
        let body = format!(
            "{{\"header\":{{\"message\":\"{path}\"}}}}\n{{\"file\":{{\"path\":\"{path}\",\"content\":\"{content}\"}}}}"
        );
        let (commit_status, _) = request(
            &app,
            "POST",
            &format!("/api/models/{repo}/commit/main"),
            "application/x-ndjson",
            body,
        )
        .await;
        assert_eq!(commit_status, 200);
        if path == "a" {
            let first = store.resolve_revision(repo, "main").unwrap().unwrap();
            store
                .create_revision(repo, None, &first, "feature", "fixture")
                .unwrap();
            store
                .create_revision(repo, None, &first, "refs/tags/v1", "fixture")
                .unwrap();
        }
    }
    let base = format!("/models/{repo}");
    let discovery = format!("{base}/info/refs?service=git-receive-pack");
    let (discovery_status, discovery_advertisement) =
        request(&app, "GET", &discovery, "", String::new()).await;
    assert_eq!(discovery_status, 200);
    let main = oid(&discovery_advertisement, "refs/heads/main");
    let tag = oid(&discovery_advertisement, "refs/tags/v1");
    let zero = "0".repeat(40);
    let before = metadata(&store, repo);
    let (conflict_status, conflict_report) = request(
        &app,
        "POST",
        &format!("{base}/git-receive-pack"),
        "application/x-git-receive-pack-request",
        command(&zero, &main, "refs/heads/feature"),
    )
    .await;
    assert_eq!(conflict_status, 200);
    assert!(
        conflict_report.contains("ng refs/heads/feature"),
        "{conflict_report}"
    );
    assert_eq!(metadata(&store, repo), before);
    let reserved = "refs/heads/refs/tags/v1";
    let (create_status, create_report) = request(
        &app,
        "POST",
        &format!("{base}/git-receive-pack"),
        "application/x-git-receive-pack-request",
        command(&zero, &main, reserved),
    )
    .await;
    assert_eq!(create_status, 200);
    assert!(
        create_report.contains(&format!("ok {reserved}")),
        "{create_report}"
    );
    for _ in 0..30 {
        let (repeated_discovery_status, repeated_discovery_advertisement) =
            request(&app, "GET", &discovery, "", String::new()).await;
        assert_eq!(
            repeated_discovery_status, 200,
            "{repeated_discovery_advertisement}"
        );
        assert_eq!(oid(&repeated_discovery_advertisement, reserved), main);
        assert_eq!(oid(&repeated_discovery_advertisement, "refs/tags/v1"), tag);
        assert_eq!(
            oid(&repeated_discovery_advertisement, "refs/heads/main"),
            main
        );
    }
    let (tree_status, tree) = request(
        &app,
        "GET",
        &format!("/api/models/{repo}/tree/refs%2Fheads%2Frefs%2Ftags%2Fv1?recursive=true"),
        "",
        String::new(),
    )
    .await;
    assert_eq!(tree_status, 200, "{tree}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&tree)
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let before_stale = metadata(&store, repo);
    let (stale_status, stale_report) = request(
        &app,
        "POST",
        &format!("{base}/git-receive-pack"),
        "application/x-git-receive-pack-request",
        command(&tag, &main, reserved),
    )
    .await;
    assert_eq!(stale_status, 200);
    assert!(stale_report.contains(&format!("ng {reserved}")));
    assert_eq!(metadata(&store, repo), before_stale);
    let (update_status, update_report) = request(
        &app,
        "POST",
        &format!("{base}/git-receive-pack"),
        "application/x-git-receive-pack-request",
        command(&main, &tag, reserved),
    )
    .await;
    assert_eq!(update_status, 200);
    assert!(
        update_report.contains(&format!("ok {reserved}")),
        "{update_report}"
    );
    let (updated_discovery_status, updated_advertisement) =
        request(&app, "GET", &discovery, "", String::new()).await;
    assert_eq!(updated_discovery_status, 200);
    assert_eq!(oid(&updated_advertisement, reserved), tag);
    assert_eq!(oid(&updated_advertisement, "refs/heads/main"), main);
    let tag_binding = store.resolve_revision(repo, "refs/tags/v1").unwrap();
    let (delete_status, delete_report) = request(
        &app,
        "POST",
        &format!("{base}/git-receive-pack"),
        "application/x-git-receive-pack-request",
        command(&tag, &zero, reserved),
    )
    .await;
    assert_eq!(delete_status, 200);
    assert!(
        delete_report.contains(&format!("ok {reserved}")),
        "{delete_report}"
    );
    assert_eq!(store.resolve_revision(repo, reserved).unwrap(), None);
    assert_eq!(
        store.resolve_revision(repo, "refs/tags/v1").unwrap(),
        tag_binding
    );
}
#[test]
fn canonical_reference_keys_preserve_namespaces_and_are_idempotent() {
    for (input, expected) in [
        ("main", "main"),
        ("refs/heads/main", "main"),
        ("refs/heads/feature/deep", "feature/deep"),
        ("refs/tags/v1", "refs/tags/v1"),
        ("refs/heads/refs/tags/v1", "refs/heads/refs/tags/v1"),
        ("refs/heads/refs/heads/x", "refs/heads/refs/heads/x"),
    ] {
        assert_eq!(canonical_ref_name(input), expected);
        assert_eq!(canonical_ref_name(canonical_ref_name(input)), expected);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_git_ref_lifecycle() {
    let root = tempfile::TempDir::new().unwrap();
    let store = BoxedHubStore::from_store(LocalIndexStore::new(root.path().to_path_buf()).unwrap());
    http_contract(root.path(), store).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_git_ref_lifecycle() {
    let Ok(url) = std::env::var("SHARDLINE_GIT_REF_TEST_DATABASE_URL") else {
        return;
    };
    let root = tempfile::TempDir::new().unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let store = BoxedHubStore::from_store(shardline_index::PostgresIndexStore::new(pool));
    http_contract(root.path(), store).await;
}
