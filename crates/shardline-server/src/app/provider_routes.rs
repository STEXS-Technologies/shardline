use std::{collections::BTreeMap, sync::Arc, time::Instant};

use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::Deserialize;
use shardline_index::{PostgresIndexStore, PostgresProviderMutationOutcome, ResourceLockKey};
use shardline_protocol::TokenScope;
use shardline_vcs::{RepositoryRef, RepositoryWebhookEvent, RepositoryWebhookEventKind};

use crate::{
    ServerError,
    app::AppState,
    app::provider::{
        XetTokenRequest, authenticate_provider_token_request, issue_provider_token_response,
        issue_xet_token, map_provider_issue_error, parse_provider_token_request_body,
        provider_webhook_response, validate_provider_name_path,
    },
    backend::ServerBackend,
    cas_headers,
    clock::unix_now_seconds_checked,
    metrics,
    model::{GitLfsAuthenticateResponse, ProviderTokenIssueResponse, XetCasTokenResponse},
    provider_events::{apply_provider_webhook, apply_provider_webhook_with_stores},
};

#[derive(Debug, Deserialize)]
pub(super) struct XetTokenQuery {
    subject: Option<String>,
}

fn provider_repository_lock_resource(repository: &RepositoryRef) -> ResourceLockKey {
    ResourceLockKey::provider_repository(
        repository.provider().as_str(),
        repository.owner(),
        repository.name(),
    )
}

fn provider_event_lock_resources(event: &RepositoryWebhookEvent) -> Vec<ResourceLockKey> {
    let mut resources = vec![provider_repository_lock_resource(event.repository())];
    if let RepositoryWebhookEventKind::RepositoryRenamed { new_repository } = event.kind() {
        resources.push(provider_repository_lock_resource(new_repository));
    }
    resources.sort();
    resources.dedup();
    resources
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
    // Repository lifecycle changes span record and index stores. Serialize
    // them at the production boundary; rename takes old+new identities in
    // canonical order so pushes to either name cannot interleave with the
    // copy/delete/state-migration sequence and two renames cannot deadlock.
    let mut repository_guards = Vec::new();
    for key in provider_event_lock_resources(&event) {
        repository_guards.push(
            state
                .backend
                .acquire_resource_write_lock(state.config.root_dir(), &key)
                .await?,
        );
    }
    let start = Instant::now();
    let outcome = match &state.backend {
        // Reuse the server's own Postgres record/index stores (their pool is
        // created once per server) instead of opening a fresh pool per webhook
        // event.
        ServerBackend::Postgres(backend)
            if matches!(
                event.kind(),
                RepositoryWebhookEventKind::RepositoryDeleted
                    | RepositoryWebhookEventKind::RepositoryRenamed { .. }
            ) =>
        {
            let object_store = backend.object_store();
            let plan = shardline_provider_events::plan_postgres_provider_repository_webhook(
                backend.record_store(),
                backend.index_store(),
                &object_store,
                &event,
            )
            .await?;
            let (mutation, applied_outcome, duplicate_outcome) = plan.into_parts();
            let expected_fences = repository_guards
                .iter()
                .map(|guard| {
                    guard
                        .postgres_fence()
                        .ok_or(ServerError::StaleResourceFence)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let connection = repository_guards
                .first_mut()
                .and_then(|guard| guard.postgres_connection_mut())
                .ok_or(ServerError::StaleResourceFence)?;
            match PostgresIndexStore::commit_provider_mutation_on_connection(
                connection,
                &expected_fences,
                &mutation,
            )
            .await?
            {
                PostgresProviderMutationOutcome::Applied => applied_outcome,
                PostgresProviderMutationOutcome::Duplicate => duplicate_outcome,
                PostgresProviderMutationOutcome::StaleFence => {
                    return Err(ServerError::StaleResourceFence);
                }
            }
        }
        ServerBackend::Postgres(backend) => {
            let object_store = backend.object_store();
            apply_provider_webhook_with_stores(
                backend.record_store(),
                backend.index_store(),
                &object_store,
                &event,
            )
            .await?
        }
        ServerBackend::Local(_) => apply_provider_webhook(&state.config, &event).await?,
    };
    let elapsed = start.elapsed().as_secs_f64();
    for guard in &mut repository_guards {
        guard.assert_current().await?;
    }
    drop(repository_guards);
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
    use shardline_vcs::{
        ProviderKind, RepositoryRef, RepositoryWebhookEvent, RepositoryWebhookEventKind,
        WebhookDeliveryId,
    };

    use shardline_index::ResourceLockKey;

    use super::{XetTokenQuery, provider_event_lock_resources};
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
    fn rename_locks_old_and_new_repository_in_canonical_order() {
        let old = RepositoryRef::new(ProviderKind::GitHub, "z-team", "old").unwrap();
        let new = RepositoryRef::new(ProviderKind::GitHub, "a-team", "new").unwrap();
        let event = RepositoryWebhookEvent::new(
            old,
            WebhookDeliveryId::new("rename-lock-order").unwrap(),
            RepositoryWebhookEventKind::RepositoryRenamed {
                new_repository: new,
            },
        );

        assert_eq!(
            provider_event_lock_resources(&event),
            vec![
                ResourceLockKey::provider_repository("github", "a-team", "new"),
                ResourceLockKey::provider_repository("github", "z-team", "old"),
            ]
        );
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
            b"bootstrap-key-16bytes".to_vec(),
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
