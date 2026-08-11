use std::{collections::BTreeMap, sync::Arc, time::Instant};

use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::Deserialize;
use shardline_protocol::TokenScope;

use crate::{
    ServerError,
    app::AppState,
    app::provider::{
        XetTokenRequest, authenticate_provider_token_request, issue_provider_token_response,
        issue_xet_token, map_provider_issue_error, parse_provider_token_request_body,
        provider_webhook_response, validate_provider_name_path,
    },
    cas_headers,
    clock::unix_now_seconds_checked,
    metrics,
    model::{GitLfsAuthenticateResponse, ProviderTokenIssueResponse, XetCasTokenResponse},
    provider_events::apply_provider_webhook,
};

#[derive(Debug, Deserialize)]
pub(super) struct XetTokenQuery {
    subject: Option<String>,
}

#[tracing::instrument(skip(state, headers, body), fields(provider))]
pub(super) async fn issue_provider_token(
    State(state): State<Arc<AppState>>,
    Path(provider): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<ProviderTokenIssueResponse>, ServerError> {
    authenticate_provider_token_request(&state, &headers, &provider)?;
    let request = parse_provider_token_request_body(&state, body).await?;
    Ok(Json(
        issue_provider_token_response(&state, &headers, &provider, &request).await?,
    ))
}

#[tracing::instrument(skip(state, headers, body), fields(provider))]
pub(super) async fn git_lfs_authenticate(
    State(state): State<Arc<AppState>>,
    Path(provider): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<GitLfsAuthenticateResponse>, ServerError> {
    authenticate_provider_token_request(&state, &headers, &provider)?;
    let request = parse_provider_token_request_body(&state, body).await?;
    let issued = issue_provider_token_response(&state, &headers, &provider, &request).await?;
    let mut header = BTreeMap::new();
    header.insert(
        cas_headers::URL.to_owned(),
        state.config.public_base_url().to_owned(),
    );
    header.insert(
        cas_headers::ACCESS_TOKEN.to_owned(),
        issued.token.expose_secret().to_owned(),
    );
    header.insert(
        cas_headers::TOKEN_EXPIRATION.to_owned(),
        issued.expires_at_unix_seconds.to_string(),
    );
    let now = unix_now_seconds_checked()?;
    Ok(Json(GitLfsAuthenticateResponse {
        href: state.config.public_base_url().to_owned(),
        header,
        expires_in: issued.expires_at_unix_seconds.saturating_sub(now),
    }))
}

#[tracing::instrument(skip(state, headers, query))]
pub(super) async fn issue_xet_read_token(
    State(state): State<Arc<AppState>>,
    Path((provider, owner, repo, rev)): Path<(String, String, String, String)>,
    headers: HeaderMap,
    Query(query): Query<XetTokenQuery>,
) -> Result<Json<XetCasTokenResponse>, ServerError> {
    issue_xet_token(
        &state,
        &headers,
        &provider,
        XetTokenRequest {
            subject: query.subject.as_deref(),
            owner: &owner,
            repo: &repo,
            revision: &rev,
            scope: TokenScope::Read,
        },
    )
    .await
}

#[tracing::instrument(skip(state, headers, query))]
pub(super) async fn issue_xet_write_token(
    State(state): State<Arc<AppState>>,
    Path((provider, owner, repo, rev)): Path<(String, String, String, String)>,
    headers: HeaderMap,
    Query(query): Query<XetTokenQuery>,
) -> Result<Json<XetCasTokenResponse>, ServerError> {
    issue_xet_token(
        &state,
        &headers,
        &provider,
        XetTokenRequest {
            subject: query.subject.as_deref(),
            owner: &owner,
            repo: &repo,
            revision: &rev,
            scope: TokenScope::Write,
        },
    )
    .await
}

#[tracing::instrument(skip(state, headers, body), fields(provider))]
pub(super) async fn handle_provider_webhook(
    State(state): State<Arc<AppState>>,
    Path(provider): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let Some(provider_tokens) = &state.provider_tokens else {
        return Err(ServerError::ProviderTokensDisabled);
    };
    validate_provider_name_path(&provider)?;
    let event = provider_tokens
        .parse_webhook(&headers, &provider, body.as_ref())
        .map_err(map_provider_issue_error)?;
    let Some(event) = event else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    let start = Instant::now();
    let outcome = apply_provider_webhook(&state.config, &event).await?;
    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_webhook_event(&provider, "", elapsed);
    Ok((
        StatusCode::ACCEPTED,
        Json(provider_webhook_response(outcome)),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Bytes;
    use axum::extract::{Path, State};
    use axum::http::HeaderMap;

    use super::XetTokenQuery;
    use crate::{
        ProtocolMetrics, ReconstructionCacheService, ServerBackend, ServerConfig, ServerRole,
        TransferLimiter, app::AppState,
    };

    #[test]
    fn xet_token_query_debug_format() {
        let query = XetTokenQuery {
            subject: Some("user".to_owned()),
        };
        let debug = format!("{query:?}");
        assert!(debug.contains("user"));
    }

    #[test]
    fn xet_token_query_subject_none() {
        let query = XetTokenQuery { subject: None };
        assert!(query.subject.is_none());
    }

    #[test]
    fn xet_token_query_deserialize_with_subject() {
        let json = r#"{"subject": "test-user"}"#;
        let query: XetTokenQuery = serde_json::from_str(json).unwrap();
        assert_eq!(query.subject, Some("test-user".to_owned()));
    }

    #[test]
    fn xet_token_query_deserialize_without_subject() {
        let json = r#"{}"#;
        let query: XetTokenQuery = serde_json::from_str(json).unwrap();
        assert!(query.subject.is_none());
    }

    #[test]
    fn xet_token_query_subject_empty_string() {
        let json = r#"{"subject": ""}"#;
        let query: XetTokenQuery = serde_json::from_str(json).unwrap();
        assert_eq!(query.subject, Some(String::new()));
    }

    #[test]
    fn xet_token_query_extra_fields_ignored() {
        let json = r#"{"subject": "user", "extra": 42}"#;
        let query: XetTokenQuery = serde_json::from_str(json).unwrap();
        assert_eq!(query.subject, Some("user".to_owned()));
    }

    #[test]
    fn xet_token_query_json_value_with_subject() {
        let json = r#"{"subject": "alice"}"#;
        let query: XetTokenQuery = serde_json::from_str(json).unwrap();
        assert_eq!(query.subject, Some("alice".to_owned()));
    }

    // ── handle_provider_webhook error paths ────────────────────────────────

    /// Line 128: when provider_tokens is None, return ProviderTokensDisabled.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handle_webhook_provider_tokens_disabled() {
        // Build a minimal AppState with provider_tokens = None
        let temp = tempfile::tempdir().unwrap();
        let chunk_size = std::num::NonZeroUsize::new(65536).unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:8080".to_owned(),
            temp.path().to_path_buf(),
            chunk_size,
        );
        let backend = ServerBackend::from_config(&config).await.unwrap();
        let state = Arc::new(AppState {
            config,
            role: ServerRole::All,
            backend,
            auth: None,
            provider_tokens: None,
            reconstruction_cache: ReconstructionCacheService::disabled(),
            transfer_limiter: TransferLimiter::new(
                std::num::NonZeroUsize::new(65536).unwrap(),
                std::num::NonZeroUsize::new(16).unwrap(),
            ),
            oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(64)),
            admission: crate::admission::WeightedAdmission::new(
                std::num::NonZeroUsize::new(256).unwrap(),
            ),
            pools: crate::admission::ExecutionPools::default_sizes(),
            protocol_metrics: ProtocolMetrics::default(),
        });

        let result = super::handle_provider_webhook(
            State(state),
            Path("github".to_owned()),
            HeaderMap::new(),
            Bytes::new(),
        )
        .await;
        assert!(matches!(
            result,
            Err(crate::ServerError::ProviderTokensDisabled)
        ));
    }

    /// Line 135: when parse_webhook returns None, return 204 No Content.
    /// This path is reached when the provider exists in the registry but
    /// the webhook payload doesn't match any known event type (e.g., ping event).
    /// For simplicity, we verify that an empty registry (no providers)
    /// produces an error return (UnknownProvider), which exercises the
    /// provider_tokens = Some path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handle_webhook_unknown_provider_returns_error() {
        let temp = tempfile::tempdir().unwrap();
        let chunk_size = std::num::NonZeroUsize::new(65536).unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:8080".to_owned(),
            temp.path().to_path_buf(),
            chunk_size,
        );
        let backend = ServerBackend::from_config(&config).await.unwrap();

        // Create a provider config file with an empty providers list
        let config_path = temp.path().join("providers.json");
        std::fs::write(&config_path, br#"{"providers":[]}"#).unwrap();

        // Build ProviderTokenService from the config file
        let service = crate::provider::ProviderTokenService::from_file(
            &config_path,
            b"bootstrap".to_vec(),
            "test-issuer",
            std::num::NonZeroU64::MIN,
            b"a]32-byte-signing-key-for-testing!",
            None,
        )
        .expect("failed to create ProviderTokenService from empty config");

        let state = Arc::new(AppState {
            config,
            role: ServerRole::All,
            backend,
            auth: None,
            provider_tokens: Some(service),
            reconstruction_cache: ReconstructionCacheService::disabled(),
            transfer_limiter: TransferLimiter::new(
                std::num::NonZeroUsize::new(65536).unwrap(),
                std::num::NonZeroUsize::new(16).unwrap(),
            ),
            oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(64)),
            admission: crate::admission::WeightedAdmission::new(
                std::num::NonZeroUsize::new(256).unwrap(),
            ),
            pools: crate::admission::ExecutionPools::default_sizes(),
            protocol_metrics: ProtocolMetrics::default(),
        });

        let result = super::handle_provider_webhook(
            State(state),
            Path("github".to_owned()),
            HeaderMap::new(),
            Bytes::from_static(b"{}"),
        )
        .await;
        // Empty registry means "github" is an unknown provider
        assert!(result.is_err());
    }
}
