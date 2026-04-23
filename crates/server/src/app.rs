mod operational;
mod provider;
mod provider_routes;
mod reconstruction_helpers;
mod reconstruction_routes;

use std::{num::NonZeroUsize, sync::Arc};

use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::HeaderMap,
    routing::{get, head, post},
    serve as serve_http,
};
use shardline_protocol::{RepositoryScope, TokenScope};
use tokio::net::TcpListener;
#[cfg(test)]
use tokio::sync::OwnedSemaphorePermit;

use crate::{
    ServerConfig, ServerError,
    auth::{AuthContext, ServerAuth},
    backend::ServerBackend,
    provider::ProviderTokenService,
    reconstruction_cache::ReconstructionCacheService,
    server_role::ServerRole,
    transfer_limiter::TransferLimiter,
    xet_adapter::{XET_READ_TOKEN_ROUTE, XET_WRITE_TOKEN_ROUTE, XORB_TRANSFER_ROUTE},
};
use operational::{
    head_xorb, health, metrics, read_chunk, read_xorb_transfer, ready, stats, upload_shard,
    upload_xorb,
};
#[cfg(test)]
use provider::{
    extract_provider_subject, latest_lifecycle_signal_at, reconciled_provider_repository_state,
    validate_provider_name_path,
};
use provider_routes::{
    git_lfs_authenticate, handle_provider_webhook, issue_provider_token, issue_xet_read_token,
    issue_xet_write_token,
};
#[cfg(test)]
use reconstruction_helpers::{full_byte_stream_response, parse_batch_reconstruction_query};
use reconstruction_routes::{batch_reconstruction, reconstruction, reconstruction_v2};

const MAX_BATCH_RECONSTRUCTION_FILE_IDS: usize = 1024;
const MAX_BATCH_RECONSTRUCTION_QUERY_BYTES: usize = 131_072;
const MAX_PROVIDER_TOKEN_REQUEST_BODY_BYTES: usize = 16_384;
const MAX_PROVIDER_WEBHOOK_BODY_BYTES: usize = 1_048_576;
const MAX_PROVIDER_NAME_BYTES: usize = 64;
const MAX_PROVIDER_SUBJECT_BYTES: usize = 512;
const MAX_PROVIDER_BASIC_AUTH_HEADER_BYTES: usize = 4096;

#[derive(Debug, Clone)]
struct AppState {
    config: ServerConfig,
    role: ServerRole,
    backend: ServerBackend,
    auth: Option<ServerAuth>,
    provider_tokens: Option<ProviderTokenService>,
    reconstruction_cache: ReconstructionCacheService,
    transfer_limiter: TransferLimiter,
}

/// Builds the Shardline HTTP router.
///
/// # Errors
///
/// Returns [`ServerError`] when the configured backend cannot initialize.
pub async fn router(config: ServerConfig) -> Result<Router, ServerError> {
    config.validate_runtime_requirements()?;
    let role = config.server_role();
    let max_request_body_bytes = config.max_request_body_bytes();
    let provider_token_body_limit = bounded_api_body_limit(
        max_request_body_bytes,
        MAX_PROVIDER_TOKEN_REQUEST_BODY_BYTES,
    );
    let provider_webhook_body_limit =
        bounded_api_body_limit(max_request_body_bytes, MAX_PROVIDER_WEBHOOK_BODY_BYTES);
    let backend = ServerBackend::from_config(&config).await?;
    let auth = config
        .token_signing_key()
        .map(ServerAuth::new)
        .transpose()?;
    let provider_tokens = if role.serves_api() {
        match (
            config.provider_config_path(),
            config.provider_api_key(),
            config.provider_token_issuer(),
            config.provider_token_ttl_seconds(),
            config.token_signing_key(),
        ) {
            (
                Some(config_path),
                Some(api_key),
                Some(issuer),
                Some(ttl_seconds),
                Some(signing_key),
            ) => Some(ProviderTokenService::from_file(
                config_path,
                api_key.to_vec(),
                issuer,
                ttl_seconds,
                signing_key,
            )?),
            _ => None,
        }
    } else {
        None
    };
    let reconstruction_cache = if role.uses_reconstruction_cache() {
        ReconstructionCacheService::from_config(&config)?
    } else {
        ReconstructionCacheService::disabled()
    };
    let transfer_limiter =
        TransferLimiter::new(config.chunk_size(), config.transfer_max_in_flight_chunks());
    let state = Arc::new(AppState {
        config,
        role,
        backend,
        auth,
        provider_tokens,
        reconstruction_cache,
        transfer_limiter,
    });

    let mut app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics));
    if role.serves_api() {
        app = app
            .route(
                "/v1/providers/{provider}/tokens",
                post(issue_provider_token).layer(DefaultBodyLimit::max(provider_token_body_limit)),
            )
            .route(
                "/v1/providers/{provider}/git-lfs-authenticate",
                post(git_lfs_authenticate).layer(DefaultBodyLimit::max(provider_token_body_limit)),
            )
            .route(
                "/v1/providers/{provider}/webhooks",
                post(handle_provider_webhook)
                    .layer(DefaultBodyLimit::max(provider_webhook_body_limit)),
            )
            .route(XET_READ_TOKEN_ROUTE, get(issue_xet_read_token))
            .route(XET_WRITE_TOKEN_ROUTE, get(issue_xet_write_token))
            .route("/reconstructions", get(batch_reconstruction))
            .route("/v1/reconstructions", get(batch_reconstruction))
            .route("/v1/reconstructions/{file_id}", get(reconstruction))
            .route("/v2/reconstructions/{file_id}", get(reconstruction_v2))
            .route("/shards", post(upload_shard))
            .route("/v1/shards", post(upload_shard))
            .route("/v1/stats", get(stats));
    }
    if role.serves_transfer() {
        app = app
            .route("/v1/chunks/default/{hash}", get(read_chunk))
            .route("/v1/chunks/default-merkledb/{hash}", get(read_chunk))
            .route(
                "/v1/xorbs/default/{hash}",
                head(head_xorb).post(upload_xorb),
            )
            .route(XORB_TRANSFER_ROUTE, get(read_xorb_transfer));
    }

    Ok(app
        .layer(DefaultBodyLimit::max(max_request_body_bytes.get()))
        .with_state(state))
}

/// Runs the Shardline HTTP server.
///
/// # Errors
///
/// Returns [`ServerError`] when the listener cannot bind or the server exits with an
/// IO error.
pub async fn serve(config: ServerConfig) -> Result<(), ServerError> {
    let listener = TcpListener::bind(config.bind_addr()).await?;
    serve_with_listener(config, listener).await
}

/// Runs the Shardline HTTP server on an existing listener.
///
/// # Errors
///
/// Returns [`ServerError`] when router initialization fails or the server exits with
/// an IO error.
pub async fn serve_with_listener(
    config: ServerConfig,
    listener: TcpListener,
) -> Result<(), ServerError> {
    let app = router(config).await?;
    serve_http(listener, app).await?;
    Ok(())
}

fn authorize(
    state: &AppState,
    headers: &HeaderMap,
    required_scope: TokenScope,
) -> Result<Option<AuthContext>, ServerError> {
    if let Some(auth) = &state.auth {
        return Ok(Some(auth.authorize(headers, required_scope)?));
    }

    Ok(None)
}

const fn scope_from_auth(auth: &AuthContext) -> &RepositoryScope {
    auth.claims().repository()
}

fn bounded_api_body_limit(configured_limit: NonZeroUsize, endpoint_limit: usize) -> usize {
    configured_limit.get().min(endpoint_limit)
}

fn endpoint_body_limit(
    configured_limit: NonZeroUsize,
    endpoint_limit: usize,
) -> Result<NonZeroUsize, ServerError> {
    NonZeroUsize::new(bounded_api_body_limit(configured_limit, endpoint_limit))
        .ok_or(ServerError::Overflow)
}

#[cfg(test)]
async fn acquire_chunk_transfer_permit(
    state: &AppState,
    hash_hex: &str,
) -> Result<OwnedSemaphorePermit, ServerError> {
    let total_bytes = state.backend.chunk_length(hash_hex).await?;
    state.transfer_limiter.acquire_bytes(total_bytes).await
}

#[cfg(test)]
mod tests;
