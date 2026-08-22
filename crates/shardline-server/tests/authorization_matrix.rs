//! # Authorization permission-matrix property test (bounty harness, Stage B)
//!
//! ## Invariant under test
//!
//! For every request, `authorize(identity, resource, operation)` must precede
//! every externally observable resource operation. Concretely: a request that
//! lacks credentials, presents invalid credentials, is scoped to the wrong
//! repository, or is scoped to the wrong visibility/operation MUST be denied
//! with a 4xx response AND MUST NOT produce any side effect (no object
//! written/read/deleted, no repository content created/mutated, no listing
//! leaked). A request with correct scope MUST succeed (2xx) with the observable
//! effect.
//!
//! This is the invariant guard for the v1.6.0 authorization-hardening effort.
//!
//! ## Matrix dimensions
//!
//! ```text
//! identity        : fixed LocalHmac subject "test" (the LocalHmac provider
//!                   resolves authorization through the token's RepositoryScope,
//!                   which is the effective identity boundary — covered by the
//!                   repository dimension below)
//! repository      : repo A = {tenant-a}/{repo-a}, repo B = {tenant-b}/{repo-b}
//! protocol        : S3, LFS, Bazel, OCI, Hub, Xet (all six frontends exist in
//!                   app::router with HTTP routes; Xet is NOT N/A)
//! operation       : read (GET), write (PUT/POST), delete (DELETE),
//!                   list/count, create (Hub /api/repos/create)
//! visibility      : exercised via Hub repo metadata (private: true/false) and
//!                   per-repo token-scope namespacing on all protocol stores
//! auth provider   : LocalHmac (the only in-process provider that enforces
//!                   claims; Passthrough is deliberately authless and is out of
//!                   scope for a deny-invariant test)
//! ```
//!
//! ## Case legend (a)-(e)
//!
//! ```text
//! (a) no credentials                        -> denied + no side effect
//! (b) invalid credentials (garbage token)   -> denied + no side effect
//! (c) valid token for the WRONG repo scope  -> denied + no side effect
//!     (cross-tenant: claims scoped to repo B, request targets repo A)
//! (d) valid token, wrong visibility/scope   -> denied + no side effect
//!     (Read-scoped claims on a write/delete operation)
//! (e) valid token, correct scope            -> 2xx + observable effect
//! ```
//!
//! "Denied" is a 4xx. The exact code is protocol-specific and documented per
//! cell in the harness report:
//! - S3 maps every authentication failure to `403 AccessDenied`
//!   (AWS-compatible; see `authorize_s3` in protocol_routes/s3/mod.rs), so
//!   (a)/(b)/(c)/(d) all yield 403 on S3.
//! - Every other frontend maps missing/invalid credentials to 401 (a)/(b) and
//!   insufficient scope to 403 (d). Cross-tenant (c) is 403 (S3 AccessDenied,
//!   Hub Forbidden) or 404 (OCI / LFS / Bazel / Xet namespace not found).
//!
//! ## Documented design exceptions (not invariant violations)
//!
//! - Hub `repo_create` / `repo_create_type` are DELIBERATELY GLOBAL (see
//!   routes/repos.rs): a Write-scoped caller may create a repository under any
//!   namespace; a freshly created empty repository grants no access to existing
//!   tenants' content. The cross-tenant boundary is enforced on ACCESS
//!   (read/write/delete/commit/resolve require the token's repository scope to
//!   match the URL repo). Case (c) for Hub is therefore asserted against
//!   commit/read access, not creation.
//! - LFS/Bazel/Xet carry no repository in the HTTP path — the token's own
//!   `RepositoryScope` selects the storage namespace, so a repo-B token on
//!   those routes operates in repo B's namespace by construction. The
//!   cross-tenant concern for them is exercised by
//!   `tenant_isolation_across_repositories` (write with the A token, prove the
//!   B token cannot observe or mutate it, and vice versa).
//!
//! ## Harness
//!
//! In-process on a random 127.0.0.1 port (the repo's `TestServer` pattern),
//! read-only against the real server code, max 20 rps. No Docker, no live
//! targets. OCI wildcard routes are driven via `tower::ServiceExt::oneshot`
//! exactly like the existing `s3_e2e_http` OCI tests (reqwest cannot address
//! the `/v2/{*path}` wildcard in this test setup). Xet write/read fixtures use
//! the public `shardline_server::test_fixtures` builders.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::let_underscore_must_use,
    clippy::shadow_unrelated,
    clippy::needless_borrows_for_generic_args,
    clippy::unnecessary_map_or,
    clippy::arithmetic_side_effects,
    clippy::string_add
)]

use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use axum::{Router, body::Body};
use sha2::{Digest, Sha256};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{ServerConfig, ServerFrontend, ServerRole, app};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tower::ServiceExt;

const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

// Repo A / repo B — the cross-tenant pair.
const OWNER_A: &str = "tenant-a";
const NAME_A: &str = "repo-a";
const OWNER_B: &str = "tenant-b";
const NAME_B: &str = "repo-b";

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

// ---------------------------------------------------------------------------
// Token minting — LocalHmacProvider with the shared test signing key, exactly
// the pattern used by s3_e2e_http.rs / s3/tests.rs. The S3 frontend uses the
// token as a SigV4 access key; every other frontend uses it as a Bearer token.
// ---------------------------------------------------------------------------

fn mint_token(scope: TokenScope, owner: &str, name: &str) -> String {
    mint_token_expiring_at(scope, owner, name, u64::MAX)
}

fn mint_token_expiring_at(
    scope: TokenScope,
    owner: &str,
    name: &str,
    expires_at_unix_seconds: u64,
) -> String {
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::Generic, owner, name, None).unwrap();
    let claims =
        TokenClaims::new("shardline", "test", scope, repo, expires_at_unix_seconds).unwrap();
    provider.mint_token(&claims).unwrap()
}

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

/// SigV4-style Authorization header whose access key IS the bearer token
/// (the S3 frontend's SigV4 -> bearer bridge), matching s3/tests.rs.
fn sigv4_auth(token: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={token}/20260813/us-east-1/s3/aws4_request, \
         SignedHeaders=host;x-amz-date, Signature=deadbeef"
    )
}

const GARBAGE_TOKEN: &str = "garbage.token.here";
const GARBAGE_ACCESS_KEY: &str = "garbage-access-key-not-a-token";

// ---------------------------------------------------------------------------
// TestServer — full shardline HTTP server on a random 127.0.0.1 port with ALL
// six frontends, backed by the local filesystem backend (no Docker).
// ---------------------------------------------------------------------------

struct TestServer {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    base_url: String,
    _tmp: TempDir,
}

impl TestServer {
    async fn start() -> Self {
        let tmp = TempDir::new().unwrap();
        let chunk_size = NonZeroUsize::new(65536).unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:8080".to_owned(),
            tmp.path().to_path_buf(),
            chunk_size,
        )
        .with_server_role(ServerRole::All)
        .with_server_frontends([
            ServerFrontend::S3,
            ServerFrontend::Lfs,
            ServerFrontend::BazelHttp,
            ServerFrontend::Oci,
            ServerFrontend::Hub,
            ServerFrontend::Xet,
        ])
        .unwrap()
        .with_deployment_mode(shardline_server::DeploymentMode::Insecure)
        .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
        .unwrap()
        .with_reconstruction_cache_disabled();

        let app = app::router(config).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    shutdown_rx.await.ok();
                })
                .await
                .ok();
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        Self {
            shutdown: Some(shutdown_tx),
            base_url,
            _tmp: tmp,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

impl TestServer {
    /// Starts a permissive no-provider server (no auth provider configured at
    /// all), used to assert the documented anonymous full-access behavior:
    /// with no auth, Hub routes are a no-op binding check and every request
    /// proceeds as `anonymous_full_access`.
    async fn start_permissive() -> Self {
        let tmp = TempDir::new().unwrap();
        let chunk_size = NonZeroUsize::new(65536).unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:8080".to_owned(),
            tmp.path().to_path_buf(),
            chunk_size,
        )
        .with_server_role(ServerRole::All)
        .with_server_frontends([
            ServerFrontend::S3,
            ServerFrontend::Lfs,
            ServerFrontend::BazelHttp,
            ServerFrontend::Oci,
            ServerFrontend::Hub,
            ServerFrontend::Xet,
        ])
        .unwrap()
        .with_deployment_mode(shardline_server::DeploymentMode::Insecure)
        .with_reconstruction_cache_disabled();

        let app = app::router(config).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    shutdown_rx.await.ok();
                })
                .await
                .ok();
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        Self {
            shutdown: Some(shutdown_tx),
            base_url,
            _tmp: tmp,
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// OCI-only router for oneshot driving of the `/v2/{*path}` wildcard routes
/// (mirrors `oci_oneshot_app` in s3_e2e_http.rs).
async fn oci_oneshot_app() -> (Router, TempDir) {
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends([ServerFrontend::Oci])
    .unwrap()
    .with_deployment_mode(shardline_server::DeploymentMode::Insecure)
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .unwrap()
    .with_reconstruction_cache_disabled();
    let app = app::router(config).await.unwrap();
    (app, tmp)
}

// ---------------------------------------------------------------------------
// Shared assertions
// ---------------------------------------------------------------------------

/// (a)/(b) are 401 on every frontend except S3 (which is AWS-compatible 403).
fn assert_denied_401(status: axum::http::StatusCode, case: &str, ctx: &str) {
    assert!(
        status == axum::http::StatusCode::UNAUTHORIZED,
        "{ctx}: case {case} must be denied with 401, got {status}"
    );
}

/// (d) is a scope conflict: InsufficientScope -> 403 on the server error table
/// and on the Hub error table.
fn assert_denied_403(status: axum::http::StatusCode, case: &str, ctx: &str) {
    assert!(
        status == axum::http::StatusCode::FORBIDDEN,
        "{ctx}: case {case} must be denied with 403, got {status}"
    );
}

/// (c) cross-tenant mismatches map to 403 (S3 AccessDenied, Hub Forbidden) or
/// 404 (namespace not found) depending on the frontend. The invariant only
/// requires a 4xx denial; the exact code is recorded per cell in the report.
fn assert_denied_client_error(status: axum::http::StatusCode, case: &str, ctx: &str) {
    assert!(
        status.is_client_error(),
        "{ctx}: case {case} must be denied with a 4xx, got {status}"
    );
}

#[derive(Debug, PartialEq, Eq)]
struct DeniedSurface {
    status: reqwest::StatusCode,
    content_type: Option<String>,
    body_len: usize,
}

async fn denied_http_sample(
    client: &reqwest::Client,
    url: &str,
    authorization: &str,
) -> (DeniedSurface, Duration) {
    let started = Instant::now();
    let response = client
        .get(url)
        .header("Authorization", authorization)
        .send()
        .await
        .unwrap();
    let elapsed = started.elapsed();
    let status = response.status();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let body_len = response.bytes().await.unwrap().len();
    (
        DeniedSurface {
            status,
            content_type,
            body_len,
        },
        elapsed,
    )
}

fn median_duration(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn assert_timing_is_bounded(
    protocol: &str,
    mut existing_samples: Vec<Duration>,
    mut missing_samples: Vec<Duration>,
) {
    let existing = median_duration(&mut existing_samples);
    let missing = median_duration(&mut missing_samples);
    let faster = existing.min(missing);
    let slower = existing.max(missing);
    let allowance = faster
        .checked_mul(8)
        .unwrap_or(Duration::MAX)
        .saturating_add(Duration::from_millis(5));
    assert!(
        slower <= allowance,
        "{protocol}: denied existing/missing median latency diverged: existing={existing:?}, missing={missing:?}, allowance={allowance:?}"
    );
}

async fn assert_http_existence_probe_is_indistinguishable(
    protocol: &str,
    client: &reqwest::Client,
    existing_url: &str,
    missing_url: &str,
    authorization: &str,
) {
    let mut existing_samples = Vec::with_capacity(32);
    let mut missing_samples = Vec::with_capacity(32);
    for iteration in 0..32 {
        let (first_url, second_url) = if iteration % 2 == 0 {
            (existing_url, missing_url)
        } else {
            (missing_url, existing_url)
        };
        let (first_surface, first_elapsed) =
            denied_http_sample(client, first_url, authorization).await;
        let (second_surface, second_elapsed) =
            denied_http_sample(client, second_url, authorization).await;
        assert_eq!(
            first_surface, second_surface,
            "{protocol}: denied surface disclosed whether the resource exists"
        );
        assert!(
            first_surface.status.is_client_error(),
            "{protocol}: revoked probe must be denied"
        );
        if iteration % 2 == 0 {
            existing_samples.push(first_elapsed);
            missing_samples.push(second_elapsed);
        } else {
            missing_samples.push(first_elapsed);
            existing_samples.push(second_elapsed);
        }
    }
    assert_timing_is_bounded(protocol, existing_samples, missing_samples);
}

async fn denied_oci_sample(
    app: Router,
    uri: &str,
    authorization: &str,
) -> ((axum::http::StatusCode, Option<String>, usize), Duration) {
    let started = Instant::now();
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri(uri)
                .header("Authorization", authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    let status = response.status();
    let content_type = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let body_len = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .len();
    ((status, content_type, body_len), elapsed)
}

async fn assert_oci_existence_probe_is_indistinguishable(
    app: Router,
    existing_uri: &str,
    missing_uri: &str,
    authorization: &str,
) {
    let mut existing_samples = Vec::with_capacity(32);
    let mut missing_samples = Vec::with_capacity(32);
    for iteration in 0..32 {
        let (first_uri, second_uri) = if iteration % 2 == 0 {
            (existing_uri, missing_uri)
        } else {
            (missing_uri, existing_uri)
        };
        let (first_surface, first_elapsed) =
            denied_oci_sample(app.clone(), first_uri, authorization).await;
        let (second_surface, second_elapsed) =
            denied_oci_sample(app.clone(), second_uri, authorization).await;
        assert_eq!(
            first_surface, second_surface,
            "oci: denied surface disclosed whether the blob exists"
        );
        assert!(
            first_surface.0.is_client_error(),
            "oci: revoked probe must be denied"
        );
        if iteration % 2 == 0 {
            existing_samples.push(first_elapsed);
            missing_samples.push(second_elapsed);
        } else {
            missing_samples.push(first_elapsed);
            existing_samples.push(second_elapsed);
        }
    }
    assert_timing_is_bounded("oci", existing_samples, missing_samples);
}

// ===========================================================================
// S3 matrix — SigV4 access key = token, bucket = {owner}.{name}
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matrix_s3_cases_a_through_e() {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    let bucket_a = format!("{OWNER_A}.{NAME_A}");
    let key = "data/model.pt";
    let put_url = server.url(&format!("/{bucket_a}/{key}"));
    let get_url = put_url.clone();
    let content = b"s3-matrix-content".to_vec();
    let token_a_write = mint_token(TokenScope::Write, OWNER_A, NAME_A);
    let token_a_read = mint_token(TokenScope::Read, OWNER_A, NAME_A);
    let token_b_write = mint_token(TokenScope::Write, OWNER_B, NAME_B);

    // S3 maps EVERY authentication failure to 403 AccessDenied (AWS semantics,
    // see authorize_s3): (a), (b), (c), (d) all yield 403 here.
    // (a) no credentials.
    let resp = client
        .put(&put_url)
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_denied_403(resp.status(), "(a)", "s3 PUT");
    // (b) invalid credentials (garbage access key).
    let resp = client
        .put(&put_url)
        .header("Authorization", sigv4_auth(GARBAGE_ACCESS_KEY))
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_denied_403(resp.status(), "(b)", "s3 PUT");
    // (c) valid token scoped to repo B, request targets repo A's bucket.
    let resp = client
        .put(&put_url)
        .header("Authorization", sigv4_auth(&token_b_write))
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_denied_403(resp.status(), "(c)", "s3 PUT cross-tenant");
    // (d) Read-scoped token on a write.
    let resp = client
        .put(&put_url)
        .header("Authorization", sigv4_auth(&token_a_read))
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_denied_403(resp.status(), "(d)", "s3 PUT read-scope");

    // No side effect from any denied write: object must still be absent.
    let resp = client
        .get(&get_url)
        .header("Authorization", sigv4_auth(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::NOT_FOUND,
        "s3: denied PUT must not create the object"
    );

    // (e) valid Write token with correct scope -> 2xx + effect.
    let resp = client
        .put(&put_url)
        .header("Authorization", sigv4_auth(&token_a_write))
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "s3 (e) PUT should succeed, got {}",
        resp.status()
    );
    let resp = client
        .get(&get_url)
        .header("Authorization", sigv4_auth(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), content.as_slice());
}

// ===========================================================================
// LFS matrix — bearer token, content-addressed per-repo namespace
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matrix_lfs_cases_a_through_e() {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    let content = b"lfs-matrix-content".to_vec();
    let oid = sha256_hex(&content);
    let obj_url = server.url(&format!("/v1/lfs/objects/{oid}"));
    let token_a_write = mint_token(TokenScope::Write, OWNER_A, NAME_A);
    let token_a_read = mint_token(TokenScope::Read, OWNER_A, NAME_A);

    // (a) no credentials.
    let resp = client
        .put(&obj_url)
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_denied_401(resp.status(), "(a)", "lfs PUT");
    // (b) invalid credentials.
    let resp = client
        .put(&obj_url)
        .header("Authorization", bearer(GARBAGE_TOKEN))
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_denied_401(resp.status(), "(b)", "lfs PUT");
    // (d) Read-scoped token on a write.
    let resp = client
        .put(&obj_url)
        .header("Authorization", bearer(&token_a_read))
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_denied_403(resp.status(), "(d)", "lfs PUT read-scope");

    // No side effect: object must still be absent.
    let resp = client
        .get(&obj_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::NOT_FOUND,
        "lfs: denied PUT must not create the object"
    );

    // (e) valid Write token -> 2xx + read-back.
    let resp = client
        .put(&obj_url)
        .header("Authorization", bearer(&token_a_write))
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "lfs (e) PUT should succeed, got {}",
        resp.status()
    );
    let resp = client
        .get(&obj_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), content.as_slice());

    // (d) Read-scoped token on a delete is also denied, and the object survives.
    let resp = client
        .delete(&obj_url)
        .header("Authorization", bearer(&token_a_read))
        .send()
        .await
        .unwrap();
    assert_denied_403(resp.status(), "(d)", "lfs DELETE read-scope");
    let resp = client
        .get(&obj_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
}

// ===========================================================================
// Bazel matrix — bearer token, CAS content-addressed per-repo namespace
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matrix_bazel_cases_a_through_e() {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    let content = b"bazel-matrix-content".to_vec();
    let hash = sha256_hex(&content);
    let cas_url = server.url(&format!("/v1/bazel/cache/cas/{hash}"));
    let token_a_write = mint_token(TokenScope::Write, OWNER_A, NAME_A);
    let token_a_read = mint_token(TokenScope::Read, OWNER_A, NAME_A);

    // (a) no credentials.
    let resp = client
        .put(&cas_url)
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_denied_401(resp.status(), "(a)", "bazel PUT cas");
    // (b) invalid credentials.
    let resp = client
        .put(&cas_url)
        .header("Authorization", bearer(GARBAGE_TOKEN))
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_denied_401(resp.status(), "(b)", "bazel PUT cas");
    // (d) Read-scoped token on a write.
    let resp = client
        .put(&cas_url)
        .header("Authorization", bearer(&token_a_read))
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_denied_403(resp.status(), "(d)", "bazel PUT cas read-scope");

    // No side effect: CAS blob must still be absent.
    let resp = client
        .get(&cas_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::NOT_FOUND,
        "bazel: denied PUT must not create the CAS blob"
    );

    // (e) valid Write token -> 2xx + read-back.
    let resp = client
        .put(&cas_url)
        .header("Authorization", bearer(&token_a_write))
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "bazel (e) PUT should succeed, got {}",
        resp.status()
    );
    let resp = client
        .get(&cas_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), content.as_slice());
}

// ===========================================================================
// OCI matrix — bearer token, repository in the /v2/{repo}/... path (oneshot)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matrix_oci_cases_a_through_e() {
    let (app, _tmp) = oci_oneshot_app().await;
    let repo_a = format!("{OWNER_A}/{NAME_A}");
    let content = b"oci-matrix-content".to_vec();
    let digest = sha256_hex(&content);
    let token_a_write = mint_token(TokenScope::Write, OWNER_A, NAME_A);
    let token_a_read = mint_token(TokenScope::Read, OWNER_A, NAME_A);
    let token_b_write = mint_token(TokenScope::Write, OWNER_B, NAME_B);

    let upload_uri = format!("/v2/{repo_a}/blobs/uploads/?digest=sha256:{digest}");
    let blob_uri = format!("/v2/{repo_a}/blobs/sha256:{digest}");

    // (a) no credentials.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(&upload_uri)
                .header("Content-Type", "application/octet-stream")
                .body(Body::from(content.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_denied_401(resp.status(), "(a)", "oci blob upload");
    // (b) invalid credentials.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(&upload_uri)
                .header("Authorization", bearer(GARBAGE_TOKEN))
                .header("Content-Type", "application/octet-stream")
                .body(Body::from(content.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_denied_401(resp.status(), "(b)", "oci blob upload");
    // (c) valid token for repo B, request targets repo A -> scope mismatch.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(&upload_uri)
                .header("Authorization", bearer(&token_b_write))
                .header("Content-Type", "application/octet-stream")
                .body(Body::from(content.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_denied_client_error(resp.status(), "(c)", "oci blob upload cross-tenant");
    // (d) Read-scoped token on a write.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(&upload_uri)
                .header("Authorization", bearer(&token_a_read))
                .header("Content-Type", "application/octet-stream")
                .body(Body::from(content.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_denied_403(resp.status(), "(d)", "oci blob upload read-scope");

    // No side effect: blob must still be absent.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri(&blob_uri)
                .header("Authorization", bearer(&token_a_write))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::NOT_FOUND,
        "oci: denied blob upload must not create the blob"
    );

    // (e) valid Write token -> 2xx + read-back.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(&upload_uri)
                .header("Authorization", bearer(&token_a_write))
                .header("Content-Type", "application/octet-stream")
                .body(Body::from(content.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "oci (e) blob upload should succeed, got {}",
        resp.status()
    );
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri(&blob_uri)
                .header("Authorization", bearer(&token_a_write))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), content.as_slice());

    // --- Blob-session PATCH: init a session, then drive (c)/(d)/(e). ---
    let init_uri = format!("/v2/{repo_a}/blobs/uploads/");
    let init = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(&init_uri)
                .header("Authorization", bearer(&token_a_write))
                .header("Content-Type", "application/octet-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        init.status().is_success(),
        "oci session init should succeed, got {}",
        init.status()
    );
    let session_location = init
        .headers()
        .get("location")
        .expect("session init must return a Location header")
        .to_str()
        .unwrap()
        .to_owned();
    let patch_uri = session_location;

    // (c) cross-tenant PATCH on repo A's session.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("PATCH")
                .uri(&patch_uri)
                .header("Authorization", bearer(&token_b_write))
                .header("Content-Type", "application/octet-stream")
                .body(Body::from(content.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_denied_client_error(resp.status(), "(c)", "oci blob-session PATCH cross-tenant");
    // (d) Read-scoped token on a PATCH.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("PATCH")
                .uri(&patch_uri)
                .header("Authorization", bearer(&token_a_read))
                .header("Content-Type", "application/octet-stream")
                .body(Body::from(content.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_denied_403(resp.status(), "(d)", "oci blob-session PATCH read-scope");
    // (e) valid Write token PATCH succeeds.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("PATCH")
                .uri(&patch_uri)
                .header("Authorization", bearer(&token_a_write))
                .header("Content-Type", "application/octet-stream")
                .body(Body::from(content.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "oci (e) blob-session PATCH should succeed, got {}",
        resp.status()
    );

    // --- Manifest PUT / DELETE: cross-tenant deny + no side effect. ---
    let manifest_doc = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": content.len(),
            "digest": format!("sha256:{digest}")
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "size": content.len(),
            "digest": format!("sha256:{digest}")
        }]
    })
    .to_string();
    let manifest_uri = format!("/v2/{repo_a}/manifests/latest");

    // (c) cross-tenant manifest PUT.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("PUT")
                .uri(&manifest_uri)
                .header("Authorization", bearer(&token_b_write))
                .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(manifest_doc.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_denied_client_error(resp.status(), "(c)", "oci manifest PUT cross-tenant");
    // (d) Read-scoped token on a manifest PUT.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("PUT")
                .uri(&manifest_uri)
                .header("Authorization", bearer(&token_a_read))
                .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(manifest_doc.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_denied_403(resp.status(), "(d)", "oci manifest PUT read-scope");

    // No side effect from the denied manifest writes: the tag must be absent.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri(&manifest_uri)
                .header("Authorization", bearer(&token_a_write))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::NOT_FOUND,
        "oci: denied manifest PUT must not publish the tag"
    );

    // (e) valid Write token publishes the manifest.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("PUT")
                .uri(&manifest_uri)
                .header("Authorization", bearer(&token_a_write))
                .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(manifest_doc))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "oci (e) manifest PUT should succeed, got {}",
        resp.status()
    );
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri(&manifest_uri)
                .header("Authorization", bearer(&token_a_write))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::OK,
        "oci: published manifest must be readable"
    );

    // (c) cross-tenant manifest DELETE is denied and leaves the tag intact.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("DELETE")
                .uri(&manifest_uri)
                .header("Authorization", bearer(&token_b_write))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_denied_client_error(resp.status(), "(c)", "oci manifest DELETE cross-tenant");
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri(&manifest_uri)
                .header("Authorization", bearer(&token_a_write))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::OK,
        "oci: denied manifest DELETE must not remove the tag"
    );

    // (e) valid Write token deletes the manifest.
    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("DELETE")
                .uri(&manifest_uri)
                .header("Authorization", bearer(&token_a_write))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "oci (e) manifest DELETE should succeed, got {}",
        resp.status()
    );
    let resp = app
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri(&manifest_uri)
                .header("Authorization", bearer(&token_a_write))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::NOT_FOUND,
        "oci: deleted manifest tag must be gone"
    );
}

// ===========================================================================
// Hub matrix — bearer token, repo-scoped routes under /api/...
//
// NOTE: repo CREATE is deliberately global (see routes/repos.rs) — a
// Write-scoped caller may create under any namespace. The cross-tenant (c)
// boundary is asserted against ACCESS operations (commit/read) below.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matrix_hub_cases_a_through_e() {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    let repo_a = format!("{OWNER_A}/{NAME_A}");
    let create_url = server.url("/api/repos/create");
    let info_url = server.url(&format!("/api/models/{repo_a}"));
    let revisions_url = server.url(&format!("/api/models/{repo_a}/revisions"));
    let commit_url = server.url(&format!("/api/models/{repo_a}/commit/main"));
    let token_a_write = mint_token(TokenScope::Write, OWNER_A, NAME_A);
    let token_a_read = mint_token(TokenScope::Read, OWNER_A, NAME_A);
    let token_b_write = mint_token(TokenScope::Write, OWNER_B, NAME_B);

    let create_body = || {
        serde_json::json!({"type": "model", "name": repo_a.clone(), "private": false}).to_string()
    };

    // (a) no credentials.
    let resp = client
        .post(&create_url)
        .header("Content-Type", "application/json")
        .body(create_body())
        .send()
        .await
        .unwrap();
    assert_denied_401(resp.status(), "(a)", "hub repo create");
    // (b) invalid credentials.
    let resp = client
        .post(&create_url)
        .header("Authorization", bearer(GARBAGE_TOKEN))
        .header("Content-Type", "application/json")
        .body(create_body())
        .send()
        .await
        .unwrap();
    assert_denied_401(resp.status(), "(b)", "hub repo create");
    // (d) Read-scoped token on a write.
    let resp = client
        .post(&create_url)
        .header("Authorization", bearer(&token_a_read))
        .header("Content-Type", "application/json")
        .body(create_body())
        .send()
        .await
        .unwrap();
    assert_denied_403(resp.status(), "(d)", "hub repo create read-scope");

    // No side effect: the repository must not exist.
    let resp = client
        .get(&info_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::NOT_FOUND,
        "hub: denied create must not create the repository"
    );

    // (e) valid Write token with correct scope -> 2xx + repository exists.
    let resp = client
        .post(&create_url)
        .header("Authorization", bearer(&token_a_write))
        .header("Content-Type", "application/json")
        .body(create_body())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "hub (e) repo create should succeed, got {}",
        resp.status()
    );
    let resp = client
        .get(&info_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["id"], repo_a);

    // (c) cross-tenant on ACCESS: repo B's Write token tries to commit to repo
    // A and to read repo A — both must be denied with no side effect.
    let content_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"hub cross-tenant matrix content",
    );
    let ndjson = format!(
        "{{\"header\":{{\"message\":\"cross-tenant commit\",\"parentCommit\":\"\"}}}}\n\
         {{\"file\":{{\"path\":\"secret.txt\",\"content\":\"{content_b64}\"}}}}"
    );
    let resp = client
        .post(&commit_url)
        .header("Authorization", bearer(&token_b_write))
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .unwrap();
    assert_denied_client_error(resp.status(), "(c)", "hub commit cross-tenant");

    let resp = client
        .get(&info_url)
        .header("Authorization", bearer(&token_b_write))
        .send()
        .await
        .unwrap();
    assert_denied_client_error(resp.status(), "(c)", "hub repo_info cross-tenant");

    // No side effect: repo A still has no commits from the denied attempt.
    let resp = client
        .get(&revisions_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(
        !body.contains("cross-tenant commit") && !body.contains("secret.txt"),
        "hub: denied cross-tenant commit must not create content in repo A"
    );

    // (e) repo A's own Write token commits successfully.
    let ndjson_ok = format!(
        "{{\"header\":{{\"message\":\"own commit\",\"parentCommit\":\"\"}}}}\n\
         {{\"file\":{{\"path\":\"ok.txt\",\"content\":\"{content_b64}\"}}}}"
    );
    let resp = client
        .post(&commit_url)
        .header("Authorization", bearer(&token_a_write))
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson_ok)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
}

// ===========================================================================
// Hub git smart-HTTP receive-pack — deny no-side-effect invariant
//
// POST /{type}/{ns}/{repo}/git-receive-pack requires a Write token whose
// repository scope matches the URL pair. A cross-tenant push must be denied
// (4xx) with zero side effects on the target repository.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matrix_hub_git_smart_http_receive_pack_cross_tenant() {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    let repo_a = format!("{OWNER_A}/receive-repo");
    let create_url = server.url("/api/repos/create");
    let receive_url = server.url(&format!("/models/{repo_a}/git-receive-pack"));
    let revisions_url = server.url(&format!("/api/models/{repo_a}/revisions"));
    let token_a_write = mint_token(TokenScope::Write, OWNER_A, "receive-repo");
    let token_a_read = mint_token(TokenScope::Read, OWNER_A, "receive-repo");
    let token_b_write = mint_token(TokenScope::Write, OWNER_B, NAME_B);

    let create = client
        .post(&create_url)
        .header("Authorization", bearer(&token_a_write))
        .header("Content-Type", "application/json")
        .body(
            serde_json::json!({"type": "model", "name": repo_a.clone(), "private": false})
                .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert!(
        create.status().is_success(),
        "receive-repo create failed: {}",
        create.status()
    );

    // (a) no credentials.
    let resp = client
        .post(&receive_url)
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(Vec::new())
        .send()
        .await
        .unwrap();
    assert_denied_401(resp.status(), "(a)", "hub receive-pack");
    // (b) invalid credentials.
    let resp = client
        .post(&receive_url)
        .header("Authorization", bearer(GARBAGE_TOKEN))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(Vec::new())
        .send()
        .await
        .unwrap();
    assert_denied_401(resp.status(), "(b)", "hub receive-pack");
    // (c) cross-tenant: repo B's Write token pushes to repo A.
    let resp = client
        .post(&receive_url)
        .header("Authorization", bearer(&token_b_write))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(Vec::new())
        .send()
        .await
        .unwrap();
    assert_denied_client_error(resp.status(), "(c)", "hub receive-pack cross-tenant");
    // (d) Read-scoped token on a write.
    let resp = client
        .post(&receive_url)
        .header("Authorization", bearer(&token_a_read))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(Vec::new())
        .send()
        .await
        .unwrap();
    assert_denied_403(resp.status(), "(d)", "hub receive-pack read-scope");

    // No side effect: every denied push left the repository without refs.
    let resp = client
        .get(&revisions_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(
        !body.contains("receive-pack") && !body.contains("refs/heads"),
        "hub: denied receive-pack pushes must not create refs, got {body}"
    );

    // (e) control: repo A's own Write token reaches the handler (200 report;
    // an empty update set is a benign no-op push).
    let resp = client
        .post(&receive_url)
        .header("Authorization", bearer(&token_a_write))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(Vec::new())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "hub (e) receive-pack should reach the handler, got {}",
        resp.status()
    );
}

// ===========================================================================
// Hub verb-collision repo cell — a repository named `commit` must bind to the
// correct (ns, repo) pair so cross-tenant denial stays effective and the
// owning tenant's commits succeed.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matrix_hub_verb_collision_repo_named_commit() {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    let repo_collision = format!("{OWNER_A}/commit");
    let create_url = server.url("/api/repos/create");
    // Repo "commit": /api/models/{ns}/commit/commit/{rev} — the second "commit"
    // is the route verb, the repo itself collides with it.
    let commit_url = server.url(&format!("/api/models/{repo_collision}/commit/main"));
    let revisions_url = server.url(&format!("/api/models/{repo_collision}/revisions"));
    let token_a_write = mint_token(TokenScope::Write, OWNER_A, "commit");
    let token_a_read = mint_token(TokenScope::Read, OWNER_A, "commit");
    let token_b_write = mint_token(TokenScope::Write, OWNER_B, NAME_B);

    let create = client
        .post(&create_url)
        .header("Authorization", bearer(&token_a_write))
        .header("Content-Type", "application/json")
        .body(
            serde_json::json!({"type": "model", "name": repo_collision.clone(), "private": false})
                .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert!(
        create.status().is_success(),
        "verb-collision repo create failed: {}",
        create.status()
    );

    // (c) cross-tenant: repo B's token must be denied on the verb-collision
    // repo — if the (ns, repo) pair were misparsed, the binding would enforce
    // against the wrong pair.
    let content_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"verb-collision cross-tenant content",
    );
    let ndjson = format!(
        "{{\"header\":{{\"message\":\"x-tenant commit\",\"parentCommit\":\"\"}}}}\n\
         {{\"file\":{{\"path\":\"secret.txt\",\"content\":\"{content_b64}\"}}}}"
    );
    let resp = client
        .post(&commit_url)
        .header("Authorization", bearer(&token_b_write))
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .unwrap();
    assert_denied_client_error(
        resp.status(),
        "(c)",
        "hub verb-collision commit cross-tenant",
    );

    // No side effect: the verb-collision repo has no content from the denial.
    let resp = client
        .get(&revisions_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(
        !body.contains("x-tenant commit") && !body.contains("secret.txt"),
        "hub: denied cross-tenant commit must not touch the verb-collision repo"
    );

    // (e) the owning tenant's Write token commits successfully to the
    // verb-collision repo, proving the binding resolves to (tenant-a, commit).
    let ndjson_ok = format!(
        "{{\"header\":{{\"message\":\"own verb-collision commit\",\"parentCommit\":\"\"}}}}\n\
         {{\"file\":{{\"path\":\"ok.txt\",\"content\":\"{content_b64}\"}}}}"
    );
    let resp = client
        .post(&commit_url)
        .header("Authorization", bearer(&token_a_write))
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson_ok.clone())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "hub (e) verb-collision repo commit should succeed, got {}",
        resp.status()
    );

    // (d) Read-scoped token on the same write route is denied.
    let resp = client
        .post(&commit_url)
        .header("Authorization", bearer(&token_a_read))
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson_ok)
        .send()
        .await
        .unwrap();
    assert_denied_403(resp.status(), "(d)", "hub verb-collision commit read-scope");
}

// ===========================================================================
// Permissive no-provider cell — with NO auth provider configured, Hub routes
// are a documented no-op binding check: every request proceeds as anonymous
// full access (development deployments without auth keep working).
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matrix_permissive_no_provider_full_access() {
    let server = TestServer::start_permissive().await;
    let client = reqwest::Client::new();

    let repo_a = format!("{OWNER_A}/{NAME_A}");
    let create_url = server.url("/api/repos/create");
    let info_url = server.url(&format!("/api/models/{repo_a}"));
    let commit_url = server.url(&format!("/api/models/{repo_a}/commit/main"));
    let whoami_url = server.url("/api/whoami-v2");

    // No Authorization header anywhere: permissive mode grants full access.
    let create = client
        .post(&create_url)
        .header("Content-Type", "application/json")
        .body(
            serde_json::json!({"type": "model", "name": repo_a.clone(), "private": false})
                .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert!(
        create.status().is_success(),
        "permissive repo create should succeed without auth, got {}",
        create.status()
    );
    let info = client.get(&info_url).send().await.unwrap();
    assert_eq!(
        info.status(),
        axum::http::StatusCode::OK,
        "permissive repo read should succeed without auth"
    );
    let content_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"permissive-mode content",
    );
    let ndjson = format!(
        "{{\"header\":{{\"message\":\"anon commit\",\"parentCommit\":\"\"}}}}\n\
         {{\"file\":{{\"path\":\"anon.txt\",\"content\":\"{content_b64}\"}}}}"
    );
    let commit = client
        .post(&commit_url)
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .unwrap();
    assert!(
        commit.status().is_success(),
        "permissive commit should succeed without auth, got {}",
        commit.status()
    );
    let whoami = client.get(&whoami_url).send().await.unwrap();
    assert_eq!(whoami.status(), axum::http::StatusCode::OK);
    let json: serde_json::Value = whoami.json().await.unwrap();
    assert_eq!(
        json["name"], "anonymous",
        "permissive whoami must report the anonymous identity"
    );
}

// ===========================================================================
// Xet matrix — bearer token; write = xorb + shard upload, read = reconstruction
// (Xet frontend has HTTP routes; see register_xet_routes in app.rs — NOT N/A)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matrix_xet_cases_a_through_e() {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    let content = b"xet-matrix-content";
    // Valid serialized xorb + shard bytes via the public test_fixtures builders
    // (an arbitrary byte body is rejected by xorb validation with 400).
    let (xorb_bytes, xorb_hash) = shardline_server::test_fixtures::single_chunk_xorb(content);
    let (shard_bytes, file_id) =
        shardline_server::test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);

    let shards_url = server.url("/v1/shards");
    let reconstruction_url = server.url(&format!("/v1/reconstructions/{file_id}"));
    let stats_url = server.url("/v1/stats");
    let token_a_write = mint_token(TokenScope::Write, OWNER_A, NAME_A);
    let token_a_read = mint_token(TokenScope::Read, OWNER_A, NAME_A);

    async fn chunks_count(client: &reqwest::Client, stats_url: &str, token: &str) -> u64 {
        let resp = client
            .get(stats_url)
            .header("Authorization", bearer(token))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let json: serde_json::Value = resp.json().await.unwrap();
        json["chunks"].as_u64().unwrap_or(0)
    }

    let chunks_before = chunks_count(&client, &stats_url, &token_a_write).await;

    // (a) no credentials.
    let resp = client
        .post(&shards_url)
        .body(shard_bytes.clone())
        .send()
        .await
        .unwrap();
    assert_denied_401(resp.status(), "(a)", "xet shard upload");
    // (b) invalid credentials.
    let resp = client
        .post(&shards_url)
        .header("Authorization", bearer(GARBAGE_TOKEN))
        .body(shard_bytes.clone())
        .send()
        .await
        .unwrap();
    assert_denied_401(resp.status(), "(b)", "xet shard upload");
    // (d) Read-scoped token on a write.
    let resp = client
        .post(&shards_url)
        .header("Authorization", bearer(&token_a_read))
        .body(shard_bytes.clone())
        .send()
        .await
        .unwrap();
    assert_denied_403(resp.status(), "(d)", "xet shard upload read-scope");

    // No side effect: the file is not reconstructable and the chunk count did
    // not move.
    let resp = client
        .get(&reconstruction_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::NOT_FOUND,
        "xet: denied shard upload must not register the file"
    );
    let chunks_after_denied = chunks_count(&client, &stats_url, &token_a_write).await;
    assert_eq!(
        chunks_after_denied, chunks_before,
        "xet: denied writes must not change the chunk count"
    );

    // (e) valid Write token -> xorb + shard upload succeed, file reconstructable.
    let xorb_url = server.url(&format!("/v1/xorbs/default/{xorb_hash}"));
    let resp = client
        .post(&xorb_url)
        .header("Authorization", bearer(&token_a_write))
        .body(xorb_bytes)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "xet (e) xorb upload should succeed, got {}",
        resp.status()
    );
    let resp = client
        .post(&shards_url)
        .header("Authorization", bearer(&token_a_write))
        .body(shard_bytes)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "xet (e) shard upload should succeed, got {}",
        resp.status()
    );
    let resp = client
        .get(&reconstruction_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::OK,
        "xet: file uploaded by repo A must be reconstructable by repo A"
    );
}

// ===========================================================================
// Revoked-token existence and timing surface across every frontend.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_revocation_probes_hide_existence_across_all_frontends() {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();
    let valid_token = mint_token(TokenScope::Write, OWNER_A, NAME_A);
    let expired_token = mint_token_expiring_at(TokenScope::Write, OWNER_A, NAME_A, 1);
    let valid_bearer = bearer(&valid_token);
    let expired_bearer = bearer(&expired_token);

    // S3 seed.
    let bucket = format!("{OWNER_A}.{NAME_A}");
    let s3_existing = server.url(&format!("/{bucket}/security/exist"));
    let s3_missing = server.url(&format!("/{bucket}/security/missx"));
    let response = client
        .put(&s3_existing)
        .header("Authorization", sigv4_auth(&valid_token))
        .body("security-s3")
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());

    // LFS seed.
    let lfs_bytes = b"security-lfs";
    let lfs_hash = sha256_hex(lfs_bytes);
    let lfs_existing = server.url(&format!("/v1/lfs/objects/{lfs_hash}"));
    let lfs_missing = server.url(&format!("/v1/lfs/objects/{}", "f".repeat(64)));
    let response = client
        .put(&lfs_existing)
        .header("Authorization", &valid_bearer)
        .body(lfs_bytes.as_slice())
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());

    // Bazel seed.
    let bazel_bytes = b"security-bazel";
    let bazel_hash = sha256_hex(bazel_bytes);
    let bazel_existing = server.url(&format!("/v1/bazel/cache/cas/{bazel_hash}"));
    let bazel_missing = server.url(&format!("/v1/bazel/cache/cas/{}", "e".repeat(64)));
    let response = client
        .put(&bazel_existing)
        .header("Authorization", &valid_bearer)
        .body(bazel_bytes.as_slice())
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());

    // Hub seed.
    let hub_existing = server.url(&format!("/api/models/{OWNER_A}/{NAME_A}"));
    let hub_missing = server.url(&format!("/api/models/{OWNER_A}/{NAME_B}"));
    let response = client
        .post(server.url("/api/repos/create"))
        .header("Authorization", &valid_bearer)
        .header("Content-Type", "application/json")
        .body(
            serde_json::json!({
                "type": "model",
                "name": format!("{OWNER_A}/{NAME_A}"),
                "private": true,
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());

    // Xet seed.
    let xet_content = b"security-xet";
    let (xorb_bytes, xorb_hash) = shardline_server::test_fixtures::single_chunk_xorb(xet_content);
    let (shard_bytes, file_id) =
        shardline_server::test_fixtures::single_file_shard(&[(xet_content, xorb_hash.as_str())]);
    let response = client
        .post(server.url(&format!("/v1/xorbs/default/{xorb_hash}")))
        .header("Authorization", &valid_bearer)
        .body(xorb_bytes)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let response = client
        .post(server.url("/v1/shards"))
        .header("Authorization", &valid_bearer)
        .body(shard_bytes)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let xet_existing = server.url(&format!("/v1/reconstructions/{file_id}"));
    let xet_missing = server.url(&format!("/v1/reconstructions/{}", "d".repeat(64)));

    // OCI seed in its wildcard-capable oneshot router.
    let (oci_app, _oci_tmp) = oci_oneshot_app().await;
    let oci_bytes = b"security-oci";
    let oci_digest = sha256_hex(oci_bytes);
    let oci_existing = format!("/v2/{OWNER_A}/{NAME_A}/blobs/sha256:{oci_digest}");
    let oci_missing = format!("/v2/{OWNER_A}/{NAME_A}/blobs/sha256:{}", "c".repeat(64));
    let response = oci_app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(format!(
                    "/v2/{OWNER_A}/{NAME_A}/blobs/uploads/?digest=sha256:{oci_digest}"
                ))
                .header("Authorization", &valid_bearer)
                .header("Content-Type", "application/octet-stream")
                .body(Body::from(oci_bytes.as_slice()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_success());

    // All protocol probes run concurrently. Within each protocol, existing
    // and missing paths alternate first position to remove order drift. An
    // expired token is the deterministic revocation boundary: authorization
    // must terminate before any existence-dependent storage operation.
    let s3_authorization = sigv4_auth(&expired_token);
    tokio::join!(
        assert_http_existence_probe_is_indistinguishable(
            "s3",
            &client,
            &s3_existing,
            &s3_missing,
            &s3_authorization,
        ),
        assert_http_existence_probe_is_indistinguishable(
            "lfs",
            &client,
            &lfs_existing,
            &lfs_missing,
            &expired_bearer,
        ),
        assert_http_existence_probe_is_indistinguishable(
            "bazel",
            &client,
            &bazel_existing,
            &bazel_missing,
            &expired_bearer,
        ),
        assert_http_existence_probe_is_indistinguishable(
            "hub",
            &client,
            &hub_existing,
            &hub_missing,
            &expired_bearer,
        ),
        assert_http_existence_probe_is_indistinguishable(
            "xet",
            &client,
            &xet_existing,
            &xet_missing,
            &expired_bearer,
        ),
        assert_oci_existence_probe_is_indistinguishable(
            oci_app,
            &oci_existing,
            &oci_missing,
            &expired_bearer,
        ),
    );
}

// ===========================================================================
// Tenant isolation end-to-end — object written to repo A is not visible or
// mutable from repo B, and vice versa.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_isolation_across_repositories() {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();

    let token_a_write = mint_token(TokenScope::Write, OWNER_A, NAME_A);
    let token_b_write = mint_token(TokenScope::Write, OWNER_B, NAME_B);

    // --- LFS: write with A, prove B cannot read or delete. ---
    let content_a = b"tenant-a-secret".to_vec();
    let oid = sha256_hex(&content_a);
    let obj_url = server.url(&format!("/v1/lfs/objects/{oid}"));
    let put = client
        .put(&obj_url)
        .header("Authorization", bearer(&token_a_write))
        .body(content_a.clone())
        .send()
        .await
        .unwrap();
    assert!(
        put.status().is_success(),
        "lfs A write failed: {}",
        put.status()
    );

    let get_b = client
        .get(&obj_url)
        .header("Authorization", bearer(&token_b_write))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_b.status(),
        axum::http::StatusCode::NOT_FOUND,
        "lfs: repo B must not see repo A's object (scoped namespace leak)"
    );
    let del_b = client
        .delete(&obj_url)
        .header("Authorization", bearer(&token_b_write))
        .send()
        .await
        .unwrap();
    assert!(
        del_b.status().is_client_error(),
        "lfs: repo B must not delete repo A's object, got {}",
        del_b.status()
    );
    let get_a = client
        .get(&obj_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(get_a.status(), axum::http::StatusCode::OK);
    assert_eq!(get_a.bytes().await.unwrap().as_ref(), content_a.as_slice());

    // --- Bazel: write with B, prove A cannot read. ---
    let content_b = b"tenant-b-secret".to_vec();
    let hash = sha256_hex(&content_b);
    let cas_url = server.url(&format!("/v1/bazel/cache/cas/{hash}"));
    let put_b = client
        .put(&cas_url)
        .header("Authorization", bearer(&token_b_write))
        .body(content_b.clone())
        .send()
        .await
        .unwrap();
    assert!(
        put_b.status().is_success(),
        "bazel B write failed: {}",
        put_b.status()
    );
    let get_a = client
        .get(&cas_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_a.status(),
        axum::http::StatusCode::NOT_FOUND,
        "bazel: repo A must not see repo B's CAS blob"
    );

    // --- S3: write to A's bucket, prove B cannot read it. ---
    let bucket_a = format!("{OWNER_A}.{NAME_A}");
    let key = "models/weights.pt";
    let put_url = server.url(&format!("/{bucket_a}/{key}"));
    let put_a = client
        .put(&put_url)
        .header("Authorization", sigv4_auth(&token_a_write))
        .body(content_a.clone())
        .send()
        .await
        .unwrap();
    assert!(
        put_a.status().is_success(),
        "s3 A write failed: {}",
        put_a.status()
    );
    let get_b = client
        .get(&put_url)
        .header("Authorization", sigv4_auth(&token_b_write))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_b.status(),
        axum::http::StatusCode::FORBIDDEN,
        "s3: repo B's bucket-bound token must be denied on repo A's bucket, got {}",
        get_b.status()
    );
    // The object still exists for A (B's probe caused no side effect).
    let get_a = client
        .get(&put_url)
        .header("Authorization", sigv4_auth(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(get_a.status(), axum::http::StatusCode::OK);
    assert_eq!(get_a.bytes().await.unwrap().as_ref(), content_a.as_slice());

    // --- Hub: repo A's private model must not be visible to repo B. ---
    let repo_a = format!("{OWNER_A}/{NAME_A}");
    let create_url = server.url("/api/repos/create");
    let create_body = serde_json::json!({
        "type": "model",
        "name": repo_a.clone(),
        "private": true, // visibility dimension: private repo
    })
    .to_string();
    let create = client
        .post(&create_url)
        .header("Authorization", bearer(&token_a_write))
        .header("Content-Type", "application/json")
        .body(create_body)
        .send()
        .await
        .unwrap();
    assert!(
        create.status().is_success(),
        "hub A private repo create failed: {}",
        create.status()
    );
    let info_url = server.url(&format!("/api/models/{repo_a}"));
    let info_b = client
        .get(&info_url)
        .header("Authorization", bearer(&token_b_write))
        .send()
        .await
        .unwrap();
    assert!(
        info_b.status().is_client_error(),
        "hub: repo B must not read repo A's private model, got {}",
        info_b.status()
    );
    let info_a = client
        .get(&info_url)
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(
        info_a.status(),
        axum::http::StatusCode::OK,
        "hub: repo A must still read its own private model"
    );

    // --- OCI: blob written to repo A must be invisible from repo B. ---
    let (oci_app, _tmp) = oci_oneshot_app().await;
    let repo_a_path = format!("{OWNER_A}/{NAME_A}");
    let digest = sha256_hex(&content_a);
    let upload_uri = format!("/v2/{repo_a_path}/blobs/uploads/?digest=sha256:{digest}");
    let blob_uri = format!("/v2/{repo_a_path}/blobs/sha256:{digest}");
    let upload = oci_app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(&upload_uri)
                .header("Authorization", bearer(&token_a_write))
                .header("Content-Type", "application/octet-stream")
                .body(Body::from(content_a.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        upload.status().is_success(),
        "oci A blob upload failed: {}",
        upload.status()
    );
    let get_b = oci_app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri(&blob_uri)
                .header("Authorization", bearer(&token_b_write))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        get_b.status().is_client_error(),
        "oci: repo B must not read repo A's blob, got {}",
        get_b.status()
    );
    let get_a = oci_app
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri(&blob_uri)
                .header("Authorization", bearer(&token_a_write))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_a.status(), axum::http::StatusCode::OK);

    // --- Xet: file written by repo A must not reconstruct for repo B. ---
    let xet_content = b"xet-tenant-a-secret";
    let (xorb_bytes, xorb_hash) = shardline_server::test_fixtures::single_chunk_xorb(xet_content);
    let (shard_bytes, xet_file_id) =
        shardline_server::test_fixtures::single_file_shard(&[(xet_content, xorb_hash.as_str())]);
    let xorb_url = server.url(&format!("/v1/xorbs/default/{xorb_hash}"));
    let xorb_post = client
        .post(&xorb_url)
        .header("Authorization", bearer(&token_a_write))
        .body(xorb_bytes)
        .send()
        .await
        .unwrap();
    assert!(
        xorb_post.status().is_success(),
        "xet A xorb upload failed: {}",
        xorb_post.status()
    );
    let shard_post = client
        .post(server.url("/v1/shards"))
        .header("Authorization", bearer(&token_a_write))
        .body(shard_bytes)
        .send()
        .await
        .unwrap();
    assert!(
        shard_post.status().is_success(),
        "xet A shard upload failed: {}",
        shard_post.status()
    );
    let recon_b = client
        .get(server.url(&format!("/v1/reconstructions/{xet_file_id}")))
        .header("Authorization", bearer(&token_b_write))
        .send()
        .await
        .unwrap();
    assert!(
        recon_b.status().is_client_error(),
        "xet: repo B must not reconstruct repo A's file, got {}",
        recon_b.status()
    );
    let recon_a = client
        .get(server.url(&format!("/v1/reconstructions/{xet_file_id}")))
        .header("Authorization", bearer(&token_a_write))
        .send()
        .await
        .unwrap();
    assert_eq!(
        recon_a.status(),
        axum::http::StatusCode::OK,
        "xet: repo A must still reconstruct its own file"
    );
}
