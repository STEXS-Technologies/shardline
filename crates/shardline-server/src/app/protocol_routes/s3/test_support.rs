//! Shared authenticated S3 fixtures. Routers stay in their suites so middleware
//! and fault-boundary coverage remain explicit at each call site.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::{
    ServerConfig, ServerFrontend, ServerRole,
    app::{AppState, ProtocolMetrics},
    reconstruction_cache::ReconstructionCacheService,
    transfer_limiter::TransferLimiter,
};
use axum::{
    body::Body,
    http::{Request, header},
};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};
use tempfile::TempDir;

pub(super) const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
pub(super) const OWNER: &str = "acme";
pub(super) const NAME: &str = "models";
pub(super) const BUCKET: &str = "acme.models";

pub(super) async fn build_test_state() -> (Arc<AppState>, TempDir) {
    build_test_state_with_options(
        NonZeroU64::new(1).unwrap(),
        NonZeroU64::new(1 << 40).unwrap(),
    )
    .await
}

pub(super) async fn build_test_state_with_options(
    min_part_bytes: NonZeroU64,
    session_max_bytes: NonZeroU64,
) -> (Arc<AppState>, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = test_config(tmp.path())
        .with_s3_max_part_bytes(NonZeroU64::new(1_048_576).unwrap())
        .expect("s3 max part bytes")
        .with_s3_min_part_bytes(min_part_bytes)
        .expect("s3 min part bytes")
        .with_s3_upload_session_max_bytes(session_max_bytes)
        .expect("s3 session max bytes");

    let state = build_state(config).await;
    (state, tmp)
}

pub(super) fn test_config(root: &std::path::Path) -> ServerConfig {
    ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        root.to_path_buf(),
        NonZeroUsize::new(65_536).unwrap(),
    )
    .with_server_frontends([ServerFrontend::S3])
    .expect("server frontends")
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .expect("token signing key")
}

pub(super) fn mint_token(scope: TokenScope, owner: &str, name: &str) -> String {
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::Generic, owner, name, None).unwrap();
    let claims = TokenClaims::new("shardline", "test", scope, repo, u64::MAX).unwrap();
    provider.mint_token(&claims).unwrap()
}

pub(super) fn sigv4_auth(token: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={token}/20260813/us-east-1/s3/aws4_request, \
         SignedHeaders=host;x-amz-date, Signature=deadbeef"
    )
}

pub(super) async fn body_bytes(response: axum::http::Response<Body>) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}

pub(super) fn put_request(uri: String, body: Vec<u8>) -> axum::http::Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(uri)
        .header(
            header::AUTHORIZATION,
            sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
        )
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(body))
        .unwrap()
}

pub(super) async fn build_state(config: ServerConfig) -> Arc<AppState> {
    let backend = crate::ServerBackend::from_config(&config)
        .await
        .expect("backend from config");

    build_state_with_backend(config, backend)
}

pub(super) fn build_state_with_backend(
    config: ServerConfig,
    backend: crate::ServerBackend,
) -> Arc<AppState> {
    let chunk_size = config.chunk_size();
    let auth = crate::auth::ServerAuth::new(TEST_SIGNING_KEY).expect("ServerAuth");

    Arc::new(AppState {
        config,
        role: ServerRole::All,
        backend,
        auth: Some(auth),
        provider_tokens: None,
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(chunk_size, NonZeroUsize::new(64).unwrap()),
        oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(64)),
        admission: crate::admission::WeightedAdmission::new(
            std::num::NonZeroUsize::new(256).unwrap(),
        ),
        pools: crate::admission::ExecutionPools::default_sizes(),
        protocol_metrics: ProtocolMetrics::default(),
    })
}
