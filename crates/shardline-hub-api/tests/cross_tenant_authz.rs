#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use,
    clippy::format_push_string
)]

//! Cross-tenant authorization (C1) and LFS namespace isolation (C2) tests.
//!
//! A token issued for repo `alice/own` must NEVER be able to read, write, or
//! delete another tenant's repo (`bob/own`). Same-repo access must still work.
//!
//! C1 coverage: same-repo write/read succeed (control), cross-repo commit,
//! delete, and read (resolve) are denied with 403.
//!
//! C2 is verified implicitly: the commit path writes LFS objects under the
//! token's repository namespace, so the same OID in two repos maps to distinct
//! storage objects (no cross-tenant first-writer-wins substitution).

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use shardline_hub_api::routes::HubState;
use shardline_index::LocalIndexStore;
use shardline_index::hub::{BoxedHubStore, HubFileEntry, HubRepoType};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthError, AuthProvider};
use tempfile::TempDir;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS shardline_hub_repos (
                repo_id TEXT PRIMARY KEY,
                repo_type TEXT NOT NULL CHECK (repo_type IN ('model', 'dataset', 'space')),
                private INTEGER NOT NULL DEFAULT 0 CHECK (private IN (0, 1)),
                default_branch TEXT NOT NULL,
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
                updated_at_unix_seconds INTEGER NOT NULL CHECK (updated_at_unix_seconds >= 0)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_revisions (
                repo_id TEXT NOT NULL,
                ref_name TEXT NOT NULL,
                sha TEXT NOT NULL,
                parent_sha TEXT,
                message TEXT,
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
                PRIMARY KEY (repo_id, sha),
                FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_refs (
                repo_id TEXT NOT NULL,
                ref_name TEXT NOT NULL,
                sha TEXT NOT NULL,
                PRIMARY KEY (repo_id, ref_name)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_file_entries (
                commit_sha TEXT NOT NULL,
                path TEXT NOT NULL,
                size INTEGER NOT NULL CHECK (size >= 0),
                sha TEXT NOT NULL,
                is_lfs INTEGER NOT NULL DEFAULT 0 CHECK (is_lfs IN (0, 1)),
                PRIMARY KEY (commit_sha, path)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_webhooks (
                id TEXT PRIMARY KEY,
                repo_id TEXT NOT NULL,
                url TEXT NOT NULL,
                events TEXT NOT NULL DEFAULT 'push',
                secret TEXT,
                active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
                created_at_unix_seconds INTEGER NOT NULL CHECK (created_at_unix_seconds >= 0),
                FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
            );";

/// Mock provider that round-trips an `owner:name:scope` token string so we can
/// mint two distinct tokens for `alice/own` and `bob/own`.
struct RepoTokenProvider;

impl AuthProvider for RepoTokenProvider {
    fn verify_token(&self, token: &str) -> Result<TokenClaims, AuthError> {
        let parts: Vec<&str> = token.split(':').collect();
        if parts.len() != 3 {
            return Err(AuthError::InvalidToken);
        }
        let (owner, name, scope_str) = (parts[0], parts[1], parts[2]);
        let scope = if scope_str == "write" {
            TokenScope::Write
        } else {
            TokenScope::Read
        };
        let repo = RepositoryScope::new(RepositoryProvider::GitHub, owner, name, None)
            .map_err(|_err| AuthError::InvalidToken)?;
        TokenClaims::new("test-issuer", "test-subject", scope, repo, u64::MAX)
            .map_err(|_err| AuthError::InvalidToken)
    }

    fn mint_token(&self, claims: &TokenClaims) -> Result<String, AuthError> {
        let scope_str = if claims.scope() == TokenScope::Write {
            "write"
        } else {
            "read"
        };
        Ok(format!(
            "{}:{}:{}",
            claims.repository().owner(),
            claims.repository().name(),
            scope_str
        ))
    }
}

fn build_state() -> (TempDir, HubState) {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path().to_path_buf();
    let db_path = root.join("metadata.sqlite3");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(SCHEMA).unwrap();
    drop(conn);
    shardline_index::hub::ensure_hub_tables(&root).unwrap();

    let store = LocalIndexStore::open(root.clone());
    let boxed = BoxedHubStore::from_store(store);
    let object_store = shardline_server_core::ServerObjectStore::local(root.join("lfs"))
        .expect("local object store");

    let state = HubState {
        store: boxed,
        object_store,
        auth: Some(shardline_hub_api::auth::HubAuth::new(Box::new(
            RepoTokenProvider,
        ))),
        http_client: None,
        webhook_secret_cipher: None,
        public_base_url: "http://127.0.0.1:8080".to_owned(),
    };
    (tmp, state)
}

/// Creates a repo with an initial `main` revision containing an LFS file so
/// that resolve returns a redirect (success) and commit has a parent to build on.
fn seed_repo(state: &HubState, repo_id: &str) {
    state
        .store
        .create_repo(HubRepoType::Model, repo_id, false)
        .unwrap();
    let parent = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
    let sha = format!("{repo_id}-initial-sha");
    state
        .store
        .create_revision(repo_id, Some(parent), &sha, "main", "init")
        .unwrap();
    let files = vec![HubFileEntry {
        path: "big.bin".into(),
        size: 2_000_000,
        sha: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".into(),
        is_lfs: true,
    }];
    state.store.store_files(&sha, &files).unwrap();
}

fn auth_header(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );
    headers
}

fn ndjson_headers(token: &str) -> HeaderMap {
    let mut headers = auth_header(token);
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    headers
}

fn json_headers(token: &str) -> HeaderMap {
    let mut headers = auth_header(token);
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers
}

fn request(method: &str, uri: &str, headers: HeaderMap, body: Body) -> Request<Body> {
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .body(body)
        .unwrap();
    *req.headers_mut() = headers;
    req
}

// ---------------------------------------------------------------------------
// C1: repository binding
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_content_commits_in_two_repositories_keep_independent_trees() {
    let (_tmp, state) = build_state();
    for name in ["alice/own", "bob/own"] {
        state
            .store
            .create_repo(HubRepoType::Model, name, true)
            .unwrap();
    }
    let app = shardline_hub_api::hub_routes(state.clone(), true);
    for (owner, path) in [("alice", "private.txt"), ("bob", "attacker.txt")] {
        let body = format!(
            "{{\"header\":{{\"message\":\"same message\"}}}}\n{{\"file\":{{\"path\":\"{path}\",\"content\":\"aGVsbG8=\"}}}}\n"
        );
        let response = app
            .clone()
            .oneshot(request(
                "POST",
                &format!("/api/models/{owner}/own/commit/main"),
                ndjson_headers(&format!("{owner}:own:write")),
                Body::from(body),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let alice_sha = state
        .store
        .resolve_revision("alice/own", "main")
        .unwrap()
        .unwrap();
    let bob_sha = state
        .store
        .resolve_revision("bob/own", "main")
        .unwrap()
        .unwrap();
    let alice_paths: Vec<_> = state
        .store
        .get_files(&alice_sha)
        .unwrap()
        .into_iter()
        .map(|f| f.path)
        .collect();
    let bob_paths: Vec<_> = state
        .store
        .get_files(&bob_sha)
        .unwrap()
        .into_iter()
        .map(|f| f.path)
        .collect();
    assert_eq!(alice_paths, ["private.txt"]);
    assert_eq!(bob_paths, ["attacker.txt"]);
    assert_ne!(alice_sha, bob_sha);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_protocol_v2_discovery_enforces_repository_read_binding() {
    let (_tmp, state) = build_state();
    for repo_id in ["alice/private", "bob/private"] {
        state
            .store
            .create_repo(HubRepoType::Model, repo_id, true)
            .unwrap();
    }
    let app = shardline_hub_api::hub_routes(state, true);
    let uri = "/models/alice/private/info/refs?service=git-upload-pack";

    let mut alice_headers = auth_header("alice:private:read");
    alice_headers.insert("git-protocol", HeaderValue::from_static("version=2"));
    let allowed = app
        .clone()
        .oneshot(request("GET", uri, alice_headers, Body::empty()))
        .await
        .unwrap();
    assert_eq!(allowed.status(), StatusCode::OK);
    let allowed_body = axum::body::to_bytes(allowed.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&allowed_body).contains("version 2"));

    let mut bob_headers = auth_header("bob:private:read");
    bob_headers.insert("git-protocol", HeaderValue::from_static("version=2"));
    let cross_tenant = app
        .clone()
        .oneshot(request("GET", uri, bob_headers, Body::empty()))
        .await
        .unwrap();
    assert_eq!(cross_tenant.status(), StatusCode::FORBIDDEN);

    let mut anonymous_headers = HeaderMap::new();
    anonymous_headers.insert("git-protocol", HeaderValue::from_static("version=2"));
    let anonymous = app
        .oneshot(request("GET", uri, anonymous_headers, Body::empty()))
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_create_cannot_create_outside_the_write_token_repository() {
    let (_tmp, state) = build_state();
    state
        .store
        .create_repo(HubRepoType::Model, "bob/secret", true)
        .unwrap();
    let app = shardline_hub_api::hub_routes(state.clone(), true);

    for (uri, body) in [
        (
            "/api/repos/create",
            r#"{"type":"model","organization":"bob","name":"secret","private":false}"#,
        ),
        (
            "/api/repos/create",
            r#"{"organization":"bob","name":"secret","visibility":"public"}"#,
        ),
        ("/api/models/bob/secret", r#"{"private":false}"#),
        (
            "/api/repos/create",
            r#"{"type":"model","organization":"alice","name":"sibling","private":true}"#,
        ),
        (
            "/api/repos/create",
            r#"{"organization":"alice","name":"sibling","visibility":"private"}"#,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(request(
                "POST",
                uri,
                json_headers("alice:own:write"),
                Body::from(body),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{uri}");
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        assert!(!String::from_utf8_lossy(&body).contains("bob/secret"));
    }

    assert!(state.store.get_repo("bob/secret").unwrap().is_some());
    assert!(state.store.get_repo("alice/sibling").unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repository_visibility_never_silently_downgrades_to_public() {
    for uri in ["/api/repos/create", "/api/models/alice/visibility"] {
        for visibility in ["protected", "PRIVATE", "", "unknown"] {
            let (_tmp, state) = build_state();
            let app = shardline_hub_api::hub_routes(state.clone(), true);
            let body = serde_json::json!({
                "organization":"alice", "name":"visibility", "visibility":visibility,
            });
            let response = app
                .oneshot(request(
                    "POST",
                    uri,
                    json_headers("alice:visibility:write"),
                    Body::from(body.to_string()),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(state.store.get_repo("alice/visibility").unwrap().is_none());
        }
        for (visibility, private, expected_private) in [
            ("private", false, true),
            ("public", false, false),
            ("public", true, true),
        ] {
            let (_tmp, state) = build_state();
            let app = shardline_hub_api::hub_routes(state.clone(), true);
            let body = serde_json::json!({
                "organization":"alice", "name":"visibility", "visibility":visibility,
                "private":private,
            });
            let response = app
                .oneshot(request(
                    "POST",
                    uri,
                    json_headers("alice:visibility:write"),
                    Body::from(body.to_string()),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
            assert_eq!(
                state
                    .store
                    .get_repo("alice/visibility")
                    .unwrap()
                    .unwrap()
                    .private,
                expected_private
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_creation_rejects_malformed_privacy_without_publishing() {
    for body in [
        r#"{"private":"true"}"#,
        r#"{"private":1}"#,
        r#"{"private":null}"#,
        r#"{"visibility":true}"#,
        r#"{"visibility":{}}"#,
        "null",
        "[]",
    ] {
        let (_tmp, state) = build_state();
        let app = shardline_hub_api::hub_routes(state.clone(), true);
        let response = app
            .oneshot(request(
                "POST",
                "/api/models/alice/privacy",
                json_headers("alice:privacy:write"),
                Body::from(body),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(state.store.get_repo("alice/privacy").unwrap().is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_create_allows_exact_scoped_target_and_safe_retry() {
    let (_tmp, state) = build_state();
    let app = shardline_hub_api::hub_routes(state.clone(), true);
    let create = || {
        request(
            "POST",
            "/api/repos/create",
            json_headers("alice:new-repo:write"),
            Body::from(r#"{"organization":"alice","name":"new-repo"}"#),
        )
    };

    let created = app.clone().oneshot(create()).await.unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    assert_eq!(
        state
            .store
            .get_repo("alice/new-repo")
            .unwrap()
            .unwrap()
            .repo_id,
        "alice/new-repo"
    );

    let retry = app.oneshot(create()).await.unwrap();
    assert_eq!(retry.status(), StatusCode::CONFLICT);
    let body = axum::body::to_bytes(retry.into_body(), 4096).await.unwrap();
    assert!(String::from_utf8_lossy(&body).contains("alice/new-repo"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn creation_urls_ignore_untrusted_host_and_forwarding_headers() {
    for (kind, prefix, suffix) in [
        ("model", "", "models"),
        ("dataset", "/datasets", "datasets"),
        ("space", "/spaces", "spaces"),
    ] {
        let (_tmp, mut state) = build_state();
        state.public_base_url = "https://hub.example.com/shardline/".to_owned();
        let app = shardline_hub_api::hub_routes(state, true);
        let mut headers = json_headers("alice:origin:write");
        headers.insert("host", HeaderValue::from_static("attacker.invalid"));
        headers.insert(
            "x-forwarded-host",
            HeaderValue::from_static("other.invalid"),
        );
        headers.insert("x-forwarded-proto", HeaderValue::from_static("http"));
        let body = serde_json::json!({
            "type":kind,"organization":"alice","name":"origin","private":true,
        });
        // Generic creation followed by typed and generic authorized retries.
        // Both conflict responses must use the same trusted public target.
        for (uri, expected) in [
            ("/api/repos/create".to_owned(), StatusCode::CREATED),
            (format!("/api/{suffix}/alice/origin"), StatusCode::CONFLICT),
            ("/api/repos/create".to_owned(), StatusCode::CONFLICT),
        ] {
            let response = app
                .clone()
                .oneshot(request(
                    "POST",
                    &uri,
                    headers.clone(),
                    Body::from(body.to_string()),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            let bytes = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            let response: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                response["url"],
                format!("https://hub.example.com/shardline{prefix}/alice/origin")
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_different_delete_cannot_change_a_published_commit_tree() {
    let (_tmp, state) = build_state();
    state
        .store
        .create_repo(HubRepoType::Model, "alice/own", true)
        .unwrap();
    let app = shardline_hub_api::hub_routes(state.clone(), true);
    let initial = r#"{"header":{"message":"initial"}}
{"file":{"path":"a.txt","content":"YQ=="}}
{"file":{"path":"b.txt","content":"Yg=="}}
"#;
    let response = app
        .clone()
        .oneshot(request(
            "POST",
            "/api/models/alice/own/commit/main",
            ndjson_headers("alice:own:write"),
            Body::from(initial),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let parent = state
        .store
        .resolve_revision("alice/own", "main")
        .unwrap()
        .unwrap();
    let delete_a =
        "{\"header\":{\"message\":\"delete\"}}\n{\"deletedEntry\":{\"path\":\"a.txt\"}}\n";
    let response = app
        .clone()
        .oneshot(request(
            "POST",
            "/api/models/alice/own/commit/main",
            ndjson_headers("alice:own:write"),
            Body::from(delete_a),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let published = state
        .store
        .resolve_revision("alice/own", "main")
        .unwrap()
        .unwrap();
    let delete_b =
        "{\"header\":{\"message\":\"delete\"}}\n{\"deletedEntry\":{\"path\":\"b.txt\"}}\n";
    let response = app
        .oneshot(request(
            "POST",
            &format!("/api/models/alice/own/commit/{parent}"),
            ndjson_headers("alice:own:write"),
            Body::from(delete_b),
        ))
        .await
        .unwrap();
    assert!(
        !response.status().is_success(),
        "a stale parent must be rejected"
    );
    assert_eq!(
        state
            .store
            .resolve_revision("alice/own", "main")
            .unwrap()
            .unwrap(),
        published
    );
    let paths: Vec<_> = state
        .store
        .get_files(&published)
        .unwrap()
        .into_iter()
        .map(|f| f.path)
        .collect();
    assert_eq!(
        paths,
        ["b.txt"],
        "failed stale commit must preserve the published tree"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_repo_commit_succeeds() {
    let (_tmp, state) = build_state();
    seed_repo(&state, "alice/own");
    seed_repo(&state, "bob/own");
    let app = shardline_hub_api::hub_routes(state, true);

    let body = r#"{"header":{"message":"hello"}}
{"file":{"path":"f.txt","content":"aGVsbG8="}}
"#;
    let response = app
        .oneshot(request(
            "POST",
            "/api/models/alice/own/commit/main",
            ndjson_headers("alice:own:write"),
            Body::from(body),
        ))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "same-repo commit must succeed (control)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_repo_commit_is_forbidden() {
    let (_tmp, state) = build_state();
    seed_repo(&state, "alice/own");
    seed_repo(&state, "bob/own");
    let app = shardline_hub_api::hub_routes(state, true);

    let body = r#"{"header":{"message":"pwned"}}
{"file":{"path":"evil.txt","content":"Yg=="}}
"#;
    let response = app
        .oneshot(request(
            "POST",
            "/api/models/bob/own/commit/main",
            ndjson_headers("alice:own:write"),
            Body::from(body),
        ))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "cross-repo commit must be denied with 403"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_repo_delete_is_forbidden() {
    let (_tmp, state) = build_state();
    seed_repo(&state, "alice/own");
    seed_repo(&state, "bob/own");
    let app = shardline_hub_api::hub_routes(state, true);

    let response = app
        .oneshot(request(
            "DELETE",
            "/api/models/bob/own",
            auth_header("alice:own:write"),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "cross-repo delete must be denied with 403"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_repo_delete_succeeds() {
    let (_tmp, state) = build_state();
    seed_repo(&state, "alice/own");
    seed_repo(&state, "bob/own");
    let app = shardline_hub_api::hub_routes(state, true);

    let response = app
        .oneshot(request(
            "DELETE",
            "/api/models/alice/own",
            auth_header("alice:own:write"),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "same-repo delete must succeed (control)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_repo_read_resolve_is_forbidden() {
    let (_tmp, state) = build_state();
    seed_repo(&state, "alice/own");
    seed_repo(&state, "bob/own");
    let app = shardline_hub_api::hub_routes(state, true);

    let response = app
        .oneshot(request(
            "GET",
            "/bob/own/resolve/main/big.bin",
            auth_header("alice:own:read"),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "cross-repo read (resolve) must be denied with 403"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_repo_read_resolve_not_forbidden() {
    let (_tmp, state) = build_state();
    seed_repo(&state, "alice/own");
    seed_repo(&state, "bob/own");
    let app = shardline_hub_api::hub_routes(state, true);

    let response = app
        .oneshot(request(
            "GET",
            "/alice/own/resolve/main/big.bin",
            auth_header("alice:own:read"),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_ne!(
        response.status(),
        StatusCode::FORBIDDEN,
        "same-repo read (resolve) must not be forbidden"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bob_token_can_write_bob_own() {
    let (_tmp, state) = build_state();
    seed_repo(&state, "alice/own");
    seed_repo(&state, "bob/own");
    let app = shardline_hub_api::hub_routes(state, true);

    let body = r#"{"header":{"message":"bob writes"}}
{"file":{"path":"bob.txt","content":"Ym9i"}}
"#;
    let response = app
        .oneshot(request(
            "POST",
            "/api/models/bob/own/commit/main",
            ndjson_headers("bob:own:write"),
            Body::from(body),
        ))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "bob's own token must be able to write bob/own (control)"
    );
}
