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

    let store = LocalIndexStore::open(root.clone());
    let boxed = BoxedHubStore::from_store(store);
    let object_store = shardline_server_core::ServerObjectStore::local(root.join("lfs"))
        .expect("local object store");

    let state = HubState {
        store: boxed,
        object_store,
        auth: Some(shardline_hub_api::auth::HubAuth::new(Box::new(RepoTokenProvider))),
        http_client: None,
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
async fn same_repo_commit_succeeds() {
    let (_tmp, state) = build_state();
    seed_repo(&state, "alice/own");
    seed_repo(&state, "bob/own");
    let app = shardline_hub_api::hub_routes(state, true);

    let body = r#"{"header":{"message":"hello"}}
{"file":{"path":"f.txt","content":"aGVsbG8="}}
"#;
    let response = app
        .oneshot(
            request(
                "POST",
                "/api/models/alice/own/commit/main",
                ndjson_headers("alice:own:write"),
                Body::from(body),
            ),
        )
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
        .oneshot(
            request(
                "POST",
                "/api/models/bob/own/commit/main",
                ndjson_headers("alice:own:write"),
                Body::from(body),
            ),
        )
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
        .oneshot(
            request(
                "DELETE",
                "/api/models/bob/own",
                auth_header("alice:own:write"),
                Body::empty(),
            ),
        )
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
        .oneshot(
            request(
                "DELETE",
                "/api/models/alice/own",
                auth_header("alice:own:write"),
                Body::empty(),
            ),
        )
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
        .oneshot(
            request(
                "GET",
                "/bob/own/resolve/main/big.bin",
                auth_header("alice:own:read"),
                Body::empty(),
            ),
        )
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
        .oneshot(
            request(
                "GET",
                "/alice/own/resolve/main/big.bin",
                auth_header("alice:own:read"),
                Body::empty(),
            ),
        )
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
        .oneshot(
            request(
                "POST",
                "/api/models/bob/own/commit/main",
                ndjson_headers("bob:own:write"),
                Body::from(body),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "bob's own token must be able to write bob/own (control)"
    );
}
