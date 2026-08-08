//! In-process HTTP tests for the M5a metadata endpoints (tree, path, revisions).
//!
//! These tests build a minimal [`Router`] wired to the metadata route handlers and
//! drive them with real HTTP requests via [`tower::ServiceExt::oneshot`].

use std::{num::NonZeroUsize, sync::Arc};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

use crate::{
    AppState, ServerConfig, ServerFrontend, ServerRole,
    app::ProtocolMetrics,
    backend::ServerBackend,
    local_backend::LocalBackend,
    object_store::ServerObjectStore,
    reconstruction_cache::ReconstructionCacheService,
    transfer_limiter::TransferLimiter,
    xet_adapter::{XET_PATH_ROUTE, XET_REVISION_ROUTE, XET_REVISIONS_ROUTE, XET_TREE_ROUTE},
};
use shardline_index::{FileRecord, LocalRecordStore, RecordMutation, StorageRepresentation};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};

const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const PROVIDER: &str = "generic";
const OWNER: &str = "owner";
const REPO: &str = "repo";
const REV: &str = "main";

fn file_id(n: u8) -> String {
    format!("{:064x}", n)
}

fn scope(owner: &str, repo: &str, rev: &str) -> RepositoryScope {
    RepositoryScope::new(RepositoryProvider::Generic, owner, repo, Some(rev)).unwrap()
}

fn mint_token(token_scope: TokenScope, owner: &str, repo: &str, rev: &str) -> String {
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let claims = TokenClaims::new(
        "shardline",
        "test",
        token_scope,
        scope(owner, repo, rev),
        u64::MAX,
    )
    .unwrap();
    provider.mint_token(&claims).unwrap()
}

async fn build_app(auth_enabled: bool) -> (Router, TempDir) {
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let object_store = ServerObjectStore::local(tmp.path().join("chunks")).unwrap();
    let backend = LocalBackend::new_with_object_store_and_upload_parallelism_with_frontends(
        tmp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
        NonZeroUsize::new(64).unwrap(),
        object_store,
        &[ServerFrontend::Xet],
    )
    .await
    .unwrap();

    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends(vec![ServerFrontend::Xet])
    .unwrap()
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .unwrap();

    config.validate_runtime_requirements().unwrap();

    let auth = if auth_enabled {
        Some(crate::auth::ServerAuth::new(TEST_SIGNING_KEY).unwrap())
    } else {
        None
    };

    let state = Arc::new(AppState {
        config,
        role: ServerRole::All,
        backend: ServerBackend::Local(backend),
        auth,
        provider_tokens: None,
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(chunk_size, NonZeroUsize::new(64).unwrap()),
        oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(100)),
        admission: crate::admission::WeightedAdmission::new(
            std::num::NonZeroUsize::new(256).unwrap(),
        ),
        pools: crate::admission::ExecutionPools::default_sizes(),
        protocol_metrics: ProtocolMetrics::default(),
    });

    let app = Router::new()
        .route(
            XET_TREE_ROUTE,
            axum::routing::get(crate::app::metadata_routes::tree_lookup),
        )
        .route(
            XET_PATH_ROUTE,
            axum::routing::put(crate::app::metadata_routes::register_path)
                .delete(crate::app::metadata_routes::delete_path),
        )
        .route(
            XET_REVISIONS_ROUTE,
            axum::routing::get(crate::app::metadata_routes::list_revisions),
        )
        .route(
            XET_REVISION_ROUTE,
            axum::routing::post(crate::app::metadata_routes::create_revision)
                .delete(crate::app::metadata_routes::delete_revision),
        )
        .with_state(state);
    (app, tmp)
}

/// Writes a latest file record so registration can validate it.
async fn write_record(root: &std::path::Path, id: &str, size: u64, scope: Option<RepositoryScope>) {
    let store = LocalRecordStore::open(root.to_path_buf());
    let record = FileRecord {
        file_id: id.to_owned(),
        content_hash: String::new(),
        total_bytes: size,
        chunk_size: 65536,
        storage_repr: StorageRepresentation::WholeFileV1,
        repository_scope: scope,
        chunks: vec![],
    };
    RecordMutation::write_latest_record(&store, &record)
        .await
        .unwrap();
}

fn tree_url(query: &str) -> String {
    format!("/api/{PROVIDER}/{OWNER}/{REPO}/tree/{REV}{query}")
}

fn path_url(path: &str) -> String {
    format!("/api/{PROVIDER}/{OWNER}/{REPO}/path/{REV}/{path}")
}

async fn get_body(response: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

// ── Resolve (§1.1) ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolve_path_returns_200() {
    let (app, tmp) = build_app(false).await;
    let id = file_id(1);
    write_record(tmp.path(), &id, 123456, None).await;

    let register = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_url("data/model.pt"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({ "fileId": id }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(register.status(), StatusCode::OK);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url("?path=data%2Fmodel.pt"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = get_body(response).await;
    assert_eq!(body["path"], "data/model.pt");
    assert_eq!(body["fileId"], id);
    assert_eq!(body["size"], 123456);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolve_path_missing_returns_404() {
    let (app, _tmp) = build_app(false).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url("?path=missing.txt"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = get_body(response).await;
    assert_eq!(body["error"], "content not found");
}

// ── Registration (§1.3) ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_upsert_reports_created_flag() {
    let (app, tmp) = build_app(false).await;
    let id = file_id(2);
    write_record(tmp.path(), &id, 10, None).await;

    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_url("a.txt"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({ "fileId": id }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let body = get_body(first).await;
    assert_eq!(body["created"], true);
    assert_eq!(body["fileId"], id);

    let second = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_url("a.txt"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({ "fileId": id }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let body = get_body(second).await;
    assert_eq!(body["created"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_unregistered_file_returns_400() {
    let (app, _tmp) = build_app(false).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_url("missing.txt"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({ "fileId": file_id(9) }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = get_body(response).await;
    assert_eq!(
        body["error"],
        format!("file is not registered in revision {REV}")
    );
}

// ── Listing (§1.2) ─────────────────────────────────────────────────────────

async fn register_paths(app: &Router, tmp: &TempDir, paths: &[(&str, u8)]) {
    for (path, n) in paths {
        let id = file_id(*n);
        write_record(tmp.path(), &id, 100, None).await;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(path_url(path))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({ "fileId": id }).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listing_shapes_include_dir_children() {
    let (app, tmp) = build_app(false).await;
    register_paths(
        &app,
        &tmp,
        &[
            ("data/model.pt", 1),
            ("data/sub/x.txt", 2),
            ("readme.md", 3),
            ("a/b/c.txt", 4),
        ],
    )
    .await;

    // Root listing: derived dirs + top-level files.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url(""))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = get_body(response).await;
    let entries = body["entries"].as_array().unwrap();
    let paths: Vec<&str> = entries
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    let dirs: Vec<&str> = entries
        .iter()
        .filter(|e| e["isDir"].as_bool().unwrap())
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["a/", "data/", "readme.md"]);
    assert_eq!(dirs, vec!["a/", "data/"]);

    // Listing under a directory prefix: one level.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url("?prefix=data/"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = get_body(response).await;
    let entries = body["entries"].as_array().unwrap();
    let paths: Vec<&str> = entries
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["data/model.pt", "data/sub/"]);
    // Files carry metadata; directories are null metadata.
    let model = entries
        .iter()
        .find(|e| e["path"] == "data/model.pt")
        .unwrap();
    assert_eq!(model["isDir"], false);
    assert_eq!(model["size"], 100);
    assert!(model["fileId"].as_str().is_some());
    let sub = entries.iter().find(|e| e["path"] == "data/sub/").unwrap();
    assert_eq!(sub["isDir"], true);
    assert_eq!(sub["fileId"], Value::Null);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listing_paginates_with_keyset_cursor() {
    let (app, tmp) = build_app(false).await;
    let paths: Vec<(String, u8)> = (0..10).map(|i| (format!("f{i:02}.txt"), i + 1)).collect();
    let refs: Vec<(&str, u8)> = paths.iter().map(|(p, n)| (p.as_str(), *n)).collect();
    register_paths(&app, &tmp, &refs).await;

    // First page: limit 3.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url("?limit=3"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = get_body(response).await;
    let entries = body["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0]["path"], "f00.txt");
    let cursor = body["nextCursor"].as_str().unwrap().to_owned();

    // Second page resumes after the cursor.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url(&format!("?limit=3&cursor={cursor}")))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = get_body(response).await;
    let entries = body["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0]["path"], "f03.txt");

    // Exhausted page has a null nextCursor; walk all remaining pages.
    let mut collected = 0usize;
    let mut cursor = body["nextCursor"].as_str().unwrap().to_owned();
    loop {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(tree_url(&format!("?limit=3&cursor={cursor}")))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = get_body(response).await;
        let entries = body["entries"].as_array().unwrap();
        collected = collected.saturating_add(entries.len());
        match body["nextCursor"].as_str() {
            Some(next) => cursor = next.to_owned(),
            None => break,
        }
    }
    assert_eq!(collected, 4);
    // All 10 files eventually listed across pages.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url("?limit=100"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = get_body(response).await;
    assert_eq!(body["entries"].as_array().unwrap().len(), 10);
    assert_eq!(body["nextCursor"], Value::Null);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listing_rejects_out_of_range_limit() {
    let (app, _tmp) = build_app(false).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url("?limit=0"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ── Revisions (§1.4) ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revision_create_conflict_and_delete_idempotent() {
    let (app, _tmp) = build_app(false).await;
    let rev_url = format!("/api/{PROVIDER}/{OWNER}/{REPO}/revisions/{REV}");

    let created = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&rev_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::OK);
    let body = get_body(created).await;
    assert_eq!(body["name"], REV);

    let conflict = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&rev_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    let body = get_body(conflict).await;
    assert_eq!(body["error"], "revision already exists");

    let deleted = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(&rev_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::OK);
    let body = get_body(deleted).await;
    assert_eq!(body["deleted"], true);

    let deleted_again = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(&rev_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deleted_again.status(), StatusCode::OK);
    let body = get_body(deleted_again).await;
    assert_eq!(body["deleted"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_revisions_returns_created() {
    let (app, _tmp) = build_app(false).await;
    let base = format!("/api/{PROVIDER}/{OWNER}/{REPO}/revisions");
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("{base}/one"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("{base}/two"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&base)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = get_body(response).await;
    let revisions = body["revisions"].as_array().unwrap();
    let names: Vec<&str> = revisions
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["one", "two"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_revision_cascades_tree_entries() {
    let (app, tmp) = build_app(false).await;
    let id = file_id(5);
    write_record(tmp.path(), &id, 50, None).await;
    app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_url("gone.txt"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({ "fileId": id }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let rev_url = format!("/api/{PROVIDER}/{OWNER}/{REPO}/revisions/{REV}");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(&rev_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = get_body(response).await;
    assert_eq!(body["deleted"], true);

    let resolve = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url("?path=gone.txt"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resolve.status(), StatusCode::NOT_FOUND);
}

// ── Deregistration (§1.5) ──────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_path_recursive_and_idempotent() {
    let (app, tmp) = build_app(false).await;
    register_paths(
        &app,
        &tmp,
        &[
            ("data/a.txt", 1),
            ("data/sub/b.txt", 2),
            ("data/sub/deep/c.txt", 3),
            ("other.txt", 4),
        ],
    )
    .await;

    // Non-recursive delete removes exactly one row.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(path_url("data/a.txt"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = get_body(response).await;
    assert_eq!(body["deleted"], 1);
    assert_eq!(body["recursive"], false);

    // Recursive delete removes the path and all descendants.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("{}?recursive=true", path_url("data")))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = get_body(response).await;
    assert_eq!(body["deleted"], 2);

    // Idempotent: missing path yields deleted: 0.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("{}?recursive=true", path_url("data")))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = get_body(response).await;
    assert_eq!(body["deleted"], 0);

    // Untouched sibling remains reachable.
    let resolve = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url("?path=other.txt"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resolve.status(), StatusCode::OK);
}

// ── Authentication ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_required_without_token() {
    let (app, _tmp) = build_app(true).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url("?path=missing.txt"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_scope_mismatch_returns_403() {
    let (app, _tmp) = build_app(true).await;
    let token = mint_token(TokenScope::Read, "other-owner", REPO, REV);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url("?path=missing.txt"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_token_required_for_mutations() {
    let (app, tmp) = build_app(true).await;
    let id = file_id(7);
    write_record(tmp.path(), &id, 5, Some(scope(OWNER, REPO, REV))).await;

    // A read-scoped token must be rejected for registration.
    let read_token = mint_token(TokenScope::Read, OWNER, REPO, REV);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_url("auth.txt"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {read_token}"))
                .body(Body::from(json!({ "fileId": id }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // A matching write token succeeds.
    let write_token = mint_token(TokenScope::Write, OWNER, REPO, REV);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_url("auth.txt"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {write_token}"))
                .body(Body::from(json!({ "fileId": id }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

// ── Error / normalization edge cases ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolve_path_empty_or_malformed_returns_400() {
    let (app, _tmp) = build_app(false).await;
    // Empty path is rejected.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url("?path="))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Traversal segments are rejected.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url("?path=../etc/passwd"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Leading slash is rejected.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(tree_url("?path=%2Fabsolute"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_with_invalid_body_or_path_returns_400() {
    let (app, tmp) = build_app(false).await;
    let id = file_id(3);
    write_record(tmp.path(), &id, 10, None).await;

    // Malformed JSON body is rejected.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_url("a.txt"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("not-json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Path with a `..` segment is rejected before any store write.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_url("a/../b.txt"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({ "fileId": id }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Invalid fileId (non-hex) is rejected with 400.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(path_url("b.txt"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({ "fileId": "not-a-hex-id" }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_revision_rejects_invalid_revision() {
    let (app, _tmp) = build_app(false).await;
    // Over-long revision is rejected with 400.
    let long_rev = "r".repeat(513);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/generic/{OWNER}/{REPO}/revisions/{long_rev}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Control characters in a revision are rejected with 400.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/generic/{OWNER}/{REPO}/revisions/bad%0Arev"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
