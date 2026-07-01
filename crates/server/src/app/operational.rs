use std::sync::Arc;

use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::{
        HeaderMap, StatusCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
};
use serde_json::json;
use shardline_protocol::TokenScope;

use crate::{
    HealthResponse, ServerError, ShardUploadResponse, XorbUploadResponse,
    auth::authorize_static_bearer_token,
    model::ReadyResponse,
    upload_ingest::RequestBodyReader,
    xet_adapter::{validate_hash_path, validate_xorb_transfer_namespace},
};

use super::{
    AppState, authorize,
    reconstruction_helpers::{
        byte_range_stream_response, full_byte_stream_response, parse_required_xorb_transfer_range,
    },
    scope_from_auth,
};

pub(super) async fn health() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_owned(),
    })
}

pub(super) async fn ready(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.backend.ready().await {
        Ok(()) => (
            StatusCode::OK,
            Json(ReadyResponse {
                status: "ok".to_owned(),
                server_role: state.role.as_str().to_owned(),
                server_frontends: state
                    .config
                    .server_frontends()
                    .iter()
                    .map(|frontend| frontend.as_str().to_owned())
                    .collect(),
                metadata_backend: state.backend.backend_name().to_owned(),
                object_backend: state.backend.object_backend_name().to_owned(),
                cache_backend: state.reconstruction_cache.backend_name().to_owned(),
            }),
        )
            .into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": format!("{error}") })),
        )
            .into_response(),
    }
}

#[tracing::instrument(skip(state, headers), fields(hash = %hash))]
pub(super) async fn read_chunk(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    validate_hash_path(&hash)?;
    let _dedupe_shard_length = state.backend.dedupe_shard_length(&hash).await?;
    if let Some(auth) = auth.as_ref() {
        let reachable = state
            .backend
            .repository_references_xorb(&hash, scope_from_auth(auth))
            .await?;
        if !reachable {
            return Err(ServerError::NotFound);
        }
    }
    let (byte_stream, total_length) = state.backend.read_dedupe_shard_stream(&hash).await?;
    Ok(full_byte_stream_response(
        byte_stream,
        state.transfer_limiter.clone(),
        total_length,
    ))
}

#[tracing::instrument(skip(state, headers, body), fields(hash = %hash))]
pub(super) async fn upload_xorb(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<XorbUploadResponse>, ServerError> {
    authorize(&state, &headers, TokenScope::Write)?;
    validate_hash_path(&hash)?;
    let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    Ok(Json(state.backend.upload_xorb_stream(&hash, body).await?))
}

#[tracing::instrument(skip(state, headers), fields(hash = %hash))]
pub(super) async fn head_xorb(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    validate_hash_path(&hash)?;
    let total_length = state.backend.xorb_length(&hash).await?;
    if let Some(auth) = auth.as_ref() {
        let reachable = state
            .backend
            .repository_references_xorb(&hash, scope_from_auth(auth))
            .await?;
        if !reachable {
            return Err(ServerError::NotFound);
        }
    }
    Ok((StatusCode::OK, [(CONTENT_LENGTH, total_length.to_string())]))
}

#[tracing::instrument(skip(state, headers), fields(hash = %hash))]
pub(super) async fn read_xorb_transfer(
    State(state): State<Arc<AppState>>,
    Path((prefix, hash)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    validate_xorb_transfer_namespace(&prefix)?;
    validate_hash_path(&hash)?;
    let total_length = state.backend.xorb_length(&hash).await?;
    if let Some(auth) = auth.as_ref() {
        let reachable = state
            .backend
            .repository_references_xorb(&hash, scope_from_auth(auth))
            .await?;
        if !reachable {
            return Err(ServerError::NotFound);
        }
    }
    let range = parse_required_xorb_transfer_range(&headers, total_length)?;
    let transfer_bytes = range.len().ok_or(ServerError::Overflow)?;
    let byte_stream = state
        .backend
        .read_xorb_range_stream(&hash, total_length, range)
        .await?;
    Ok(byte_range_stream_response(
        byte_stream,
        state.transfer_limiter.clone(),
        range,
        total_length,
        transfer_bytes,
    ))
}

#[tracing::instrument(skip(state, headers, body))]
pub(super) async fn upload_shard(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<ShardUploadResponse>, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Write)?;
    let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    Ok(Json(
        state
            .backend
            .upload_shard_stream(
                body,
                auth.as_ref().map(scope_from_auth),
                state.config.shard_metadata_limits(),
            )
            .await?,
    ))
}

#[tracing::instrument(skip(state, headers))]
pub(super) async fn stats(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    authorize(&state, &headers, TokenScope::Read)?;
    Ok(Json(state.backend.stats().await?))
}

pub(super) async fn metrics(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    if let Some(metrics_token) = state.config.metrics_token() {
        authorize_static_bearer_token(&headers, metrics_token)?;
    }

    let auth_enabled = state.auth.is_some();
    let provider_tokens_enabled = state.provider_tokens.is_some();
    let metrics_auth_enabled = state.config.metrics_token().is_some();
    let frontend_labels = state
        .config
        .server_frontends()
        .iter()
        .map(|frontend| frontend.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let mut body = format!(
        concat!(
            "# HELP shardline_up Whether the Shardline process is serving requests.\n",
            "# TYPE shardline_up gauge\n",
            "shardline_up 1\n",
            "# HELP shardline_server_info Static Shardline runtime information.\n",
            "# TYPE shardline_server_info gauge\n",
            "shardline_server_info{{role=\"{}\",frontends=\"{}\",metadata_backend=\"{}\",object_backend=\"{}\",cache_backend=\"{}\"}} 1\n",
            "# HELP shardline_auth_enabled Whether served routes require bearer authentication.\n",
            "# TYPE shardline_auth_enabled gauge\n",
            "shardline_auth_enabled {}\n",
            "# HELP shardline_provider_tokens_enabled Whether provider token issuance is enabled.\n",
            "# TYPE shardline_provider_tokens_enabled gauge\n",
            "shardline_provider_tokens_enabled {}\n",
            "# HELP shardline_metrics_auth_enabled Whether metrics scraping requires bearer authentication.\n",
            "# TYPE shardline_metrics_auth_enabled gauge\n",
            "shardline_metrics_auth_enabled {}\n",
            "# HELP shardline_chunk_size_bytes Configured chunk size in bytes.\n",
            "# TYPE shardline_chunk_size_bytes gauge\n",
            "shardline_chunk_size_bytes {}\n",
            "# HELP shardline_max_request_body_bytes Configured maximum request body size in bytes.\n",
            "# TYPE shardline_max_request_body_bytes gauge\n",
            "shardline_max_request_body_bytes {}\n",
            "# HELP shardline_upload_max_in_flight_chunks Configured upload chunk parallelism.\n",
            "# TYPE shardline_upload_max_in_flight_chunks gauge\n",
            "shardline_upload_max_in_flight_chunks {}\n",
            "# HELP shardline_transfer_max_in_flight_chunks Configured transfer chunk parallelism.\n",
            "# TYPE shardline_transfer_max_in_flight_chunks gauge\n",
            "shardline_transfer_max_in_flight_chunks {}\n",
            "# HELP shardline_oci_registry_token_ttl_seconds Configured OCI registry token TTL in seconds.\n",
            "# TYPE shardline_oci_registry_token_ttl_seconds gauge\n",
            "shardline_oci_registry_token_ttl_seconds {}\n",
            "# HELP shardline_oci_registry_token_max_in_flight_requests Configured OCI registry token concurrency limit.\n",
            "# TYPE shardline_oci_registry_token_max_in_flight_requests gauge\n",
            "shardline_oci_registry_token_max_in_flight_requests {}\n",
        ),
        state.role.as_str(),
        frontend_labels,
        state.backend.backend_name(),
        state.backend.object_backend_name(),
        state.reconstruction_cache.backend_name(),
        u8::from(auth_enabled),
        u8::from(provider_tokens_enabled),
        u8::from(metrics_auth_enabled),
        state.config.chunk_size().get(),
        state.config.max_request_body_bytes().get(),
        state.config.upload_max_in_flight_chunks().get(),
        state.config.transfer_max_in_flight_chunks().get(),
        state.config.oci_registry_token_ttl_seconds().get(),
        state
            .config
            .oci_registry_token_max_in_flight_requests()
            .get(),
    );
    // Append Prometheus registry metrics (includes OCI registry token counters).
    body.push_str(&shardline_metrics::encode_metrics());

    Ok((
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    ))
}

#[cfg(test)]
mod tests {
    #[test]
    fn metrics_output_is_valid_prometheus_text_format() {
        let body = concat!(
            "# HELP shardline_up Whether the Shardline process is serving requests.\n",
            "# TYPE shardline_up gauge\n",
            "shardline_up 1\n",
            "# HELP shardline_server_info Static Shardline runtime information.\n",
            "# TYPE shardline_server_info gauge\n",
            "shardline_server_info{{role=\"all\",frontends=\"xet\",metadata_backend=\"local\",object_backend=\"local\",cache_backend=\"memory\"}} 1\n",
        );
        assert!(body.starts_with("# HELP"));
        assert!(body.contains("# TYPE"));
        assert!(body.contains("shardline_up 1"));
        for line in body.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = trimmed.splitn(2, ' ').collect();
            assert!(
                parts.len() == 2,
                "each metric line must have a name and value: {trimmed}"
            );
            let value = parts[1];
            assert!(
                value.parse::<f64>().is_ok(),
                "metric value must be a number: {trimmed}"
            );
        }
    }

    #[test]
    fn encode_metrics_returns_prometheus_text() {
        // Ensure the global metrics are initialized.
        let _ = shardline_metrics::metrics();
        let encoded = shardline_metrics::encode_metrics();
        assert!(!encoded.is_empty());
        assert!(encoded.contains("# HELP") || encoded.contains("# TYPE"));
    }
}
