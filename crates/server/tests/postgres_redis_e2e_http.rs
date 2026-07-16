#![cfg(feature = "docker")]
#![allow(
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::let_underscore_must_use,
    clippy::shadow_unrelated,
    clippy::expect_used,
    clippy::panic,
    clippy::needless_borrows_for_generic_args,
    clippy::unnecessary_map_or,
    clippy::or_fun_call
)]

use std::num::{NonZeroU64, NonZeroUsize};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{ServerConfig, ServerFrontend, ServerRole, app};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use shardline_test_support::DockerLocalStack;
use tempfile::TempDir;
use tokio::sync::OnceCell;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Shared Docker Postgres + Redis — one container stack for all tests.
// ---------------------------------------------------------------------------

static PG_REDIS: OnceCell<(DockerLocalStack, String, String)> = OnceCell::const_new();

/// Ensure the global Docker Postgres and Redis are running with migrations
/// applied. Returns the Postgres URL and Redis URL.
async fn ensure_stack() -> (&'static str, &'static str) {
    let (_, pg_url, redis_url) = PG_REDIS
        .get_or_init(|| async {
            let stack = DockerLocalStack::builder()
                .with_postgres()
                .with_redis()
                .start()
                .unwrap()
                .expect("Docker stack: is docker available?");
            let base = stack.postgres_url().unwrap();
            let pg_url = format!("{base}?sslmode=disable");
            // Run migrations with retry (container may not accept connections immediately).
            let mut last_err = None;
            for _ in 0..5 {
                match sqlx::PgPool::connect(&pg_url).await {
                    Ok(pool) => {
                        shardline_server::apply_database_migrations(&pool).await.unwrap();
                        pool.close().await;
                        let redis_url = stack.redis_url().unwrap();
                        return (stack, pg_url, redis_url);
                    }
                    Err(e) => {
                        last_err = Some(e);
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                }
            }
            panic!("Failed to connect to Postgres after 5 retries: {last_err:?}");
        })
        .await;
    (pg_url, redis_url)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const CACHE_TTL: NonZeroU64 = match NonZeroU64::new(3600) {
    Some(v) => v,
    None => panic!("3600 is not zero"),
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_with_redis_cache_healthz() {
    let (pg_url, redis_url) = ensure_stack().await;
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends([ServerFrontend::Xet])
    .unwrap()
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .unwrap()
    .with_index_postgres_url(pg_url.to_owned())
    .unwrap()
    .with_reconstruction_cache_redis(redis_url.to_owned(), CACHE_TTL)
    .unwrap();
    config.validate_runtime_requirements().unwrap();
    let app = app::router(config).await.unwrap();

    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/healthz")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "healthz should respond 200");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_with_redis_cache_xet_request() {
    let (pg_url, redis_url) = ensure_stack().await;
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends([ServerFrontend::Xet])
    .unwrap()
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .unwrap()
    .with_index_postgres_url(pg_url.to_owned())
    .unwrap()
    .with_reconstruction_cache_redis(redis_url.to_owned(), CACHE_TTL)
    .unwrap();
    config.validate_runtime_requirements().unwrap();
    let app = app::router(config).await.unwrap();

    let token = {
        let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
        let repo_s = RepositoryScope::new(
            RepositoryProvider::Generic,
            "test",
            "test",
            Some("main"),
        )
        .unwrap();
        let claims = TokenClaims::new("shardline", "test", TokenScope::Read, repo_s, u64::MAX)
            .unwrap();
        provider.mint_token(&claims).unwrap()
    };

    // Make a Xet read token request — should return 200 or 404 (not a crash).
    let req = axum::http::Request::builder()
        .method("GET")
        .uri("/api/generic/test/test/xet-read-token/main")
        .header("Authorization", format!("Bearer {token}"))
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    // Should not be a 500
    assert!(
        !resp.status().is_server_error(),
        "Xet read should not 500: {}",
        resp.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_with_redis_cache_hub_lfs() {
    let (pg_url, redis_url) = ensure_stack().await;
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends([ServerFrontend::Hub, ServerFrontend::Lfs])
    .unwrap()
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .unwrap()
    .with_index_postgres_url(pg_url.to_owned())
    .unwrap()
    .with_reconstruction_cache_redis(redis_url.to_owned(), CACHE_TTL)
    .unwrap();
    config.validate_runtime_requirements().unwrap();
    let app = app::router(config).await.unwrap();

    let token = {
        let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
        let repo_s = RepositoryScope::new(
            RepositoryProvider::Generic,
            "test",
            "test",
            Some("main"),
        )
        .unwrap();
        let claims =
            TokenClaims::new("shardline", "test", TokenScope::Write, repo_s, u64::MAX).unwrap();
        provider.mint_token(&claims).unwrap()
    };

    // Create repo via Hub
    let hub_create = axum::http::Request::builder()
        .method("POST")
        .uri("/api/repos/create")
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .body(axum::body::Body::from(
            serde_json::to_vec(&serde_json::json!({"name": "test/test", "type": "model"})).unwrap(),
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(hub_create).await.unwrap();
    assert_eq!(
        create_resp.status(),
        201,
        "Hub repo creation should succeed"
    );

    // LFS batch request
    let lfs_batch = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/lfs/objects/batch")
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .body(axum::body::Body::from(
            serde_json::to_vec(&serde_json::json!({
                "operation": "download",
                "objects": [],
                "transfers": ["basic"]
            }))
            .unwrap(),
        ))
        .unwrap();
    let lfs_resp = app.oneshot(lfs_batch).await.unwrap();
    assert_eq!(lfs_resp.status(), 200, "LFS batch should succeed");
}
