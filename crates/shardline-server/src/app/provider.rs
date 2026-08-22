use std::str::from_utf8;

use axum::{
    Json,
    body::Body,
    http::{HeaderMap, header::AUTHORIZATION},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde_json::from_slice;
use shardline_index::{AsyncIndexStore, LocalIndexStore, ProviderRepositoryState};
use shardline_metrics::record_provider_token_exchange;
use shardline_protocol::{SecretBytes, TokenScope};
use shardline_vcs::BuiltInProviderError;

use super::{
    AppState, MAX_PROVIDER_BASIC_AUTH_HEADER_BYTES, MAX_PROVIDER_NAME_BYTES,
    MAX_PROVIDER_SUBJECT_BYTES, MAX_PROVIDER_TOKEN_REQUEST_BODY_BYTES, endpoint_body_limit,
};
use crate::{
    ServerError,
    backend::ServerBackend,
    clock::unix_now_seconds_checked,
    model::{
        ProviderTokenIssueRequest, ProviderTokenIssueResponse, ProviderWebhookResponse,
        XetCasTokenResponse,
    },
    provider::ProviderServiceError,
    provider_events::{ProviderWebhookOutcome, ProviderWebhookOutcomeKind},
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

#[derive(Debug, Clone, Copy)]
pub(super) struct XetTokenRequest<'request> {
    pub(super) subject: Option<&'request str>,
    pub(super) owner: &'request str,
    pub(super) repo: &'request str,
    pub(super) revision: &'request str,
    pub(super) scope: TokenScope,
}

pub(super) fn authenticate_provider_token_request(
    state: &AppState,
    headers: &HeaderMap,
    provider: &str,
) -> Result<(), ServerError> {
    let Some(provider_tokens) = &state.provider_tokens else {
        return Err(ServerError::ProviderTokensDisabled);
    };
    provider_tokens
        .authorize_bootstrap_key(headers)
        .map_err(map_provider_issue_error)?;
    validate_provider_name_path(provider)
}

pub(super) async fn parse_provider_token_request_body(
    state: &AppState,
    body: Body,
) -> Result<ProviderTokenIssueRequest, ServerError> {
    let max_bytes = endpoint_body_limit(
        state.config.max_request_body_bytes(),
        MAX_PROVIDER_TOKEN_REQUEST_BODY_BYTES,
    )?;
    let mut reader = RequestBodyReader::from_body(body, max_bytes)?;
    let bytes = read_body_to_bytes(&mut reader).await?;
    from_slice(&bytes).map_err(|_error| ServerError::InvalidProviderTokenRequest)
}

pub(super) async fn issue_provider_token_response(
    state: &AppState,
    headers: &HeaderMap,
    provider: &str,
    request: &ProviderTokenIssueRequest,
) -> Result<ProviderTokenIssueResponse, ServerError> {
    validate_provider_name_path(provider)?;
    let Some(provider_tokens) = &state.provider_tokens else {
        return Err(ServerError::ProviderTokensDisabled);
    };
    let issued = provider_tokens
        .issue_token(headers, provider, request)
        .map_err(map_provider_issue_error)?;
    record_provider_token_exchange();
    reconcile_provider_repository_state(state, &issued).await?;
    Ok(issued)
}

pub(super) async fn issue_xet_token(
    state: &AppState,
    headers: &HeaderMap,
    provider: &str,
    request: XetTokenRequest<'_>,
) -> Result<Json<XetCasTokenResponse>, ServerError> {
    authenticate_provider_token_request(state, headers, provider)?;
    let provider = normalize_provider_name(provider);
    validate_provider_name_path(provider)?;
    let request = ProviderTokenIssueRequest {
        subject: extract_provider_subject(headers, request.subject)?,
        owner: request.owner.to_owned(),
        repo: request.repo.to_owned(),
        revision: Some(request.revision.to_owned()),
        scope: request.scope,
    };
    let issued = issue_provider_token_response(state, headers, provider, &request).await?;
    Ok(Json(XetCasTokenResponse {
        cas_url: state.config.public_base_url().to_owned(),
        exp: issued.expires_at_unix_seconds,
        access_token: issued.token.expose_secret().to_owned(),
    }))
}

async fn reconcile_provider_repository_state(
    state: &AppState,
    issued: &ProviderTokenIssueResponse,
) -> Result<(), ServerError> {
    match &state.backend {
        // Reuse the server's own Postgres metadata store (its pool is created
        // once per server alongside the backend) instead of opening a fresh
        // pool per token issuance — per-call pools exhaust Postgres connections
        // under concurrent load, and a process-global cached pool shared
        // connections across server instances.
        ServerBackend::Postgres(backend) => {
            reconcile_provider_repository_state_with_store(backend.index_store(), issued).await
        }
        ServerBackend::Local(_) => {
            let index_store = LocalIndexStore::open(state.config.root_dir().to_path_buf());
            reconcile_provider_repository_state_with_store(&index_store, issued).await
        }
    }
}

async fn reconcile_provider_repository_state_with_store<IndexAdapter>(
    index_store: &IndexAdapter,
    issued: &ProviderTokenIssueResponse,
) -> Result<(), ServerError>
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ServerError>,
{
    let Some(existing) = index_store
        .provider_repository_state(issued.provider, issued.owner.as_str(), issued.repo.as_str())
        .await
        .map_err(Into::into)?
    else {
        return Ok(());
    };
    let reconciled_at_unix_seconds = unix_now_seconds_checked()?;
    let reconciled = reconciled_provider_repository_state(&existing, reconciled_at_unix_seconds);
    if reconciled == existing {
        return Ok(());
    }

    index_store
        .upsert_provider_repository_state(&reconciled)
        .await
        .map_err(Into::into)?;
    Ok(())
}

#[must_use]
pub fn reconciled_provider_repository_state(
    state: &ProviderRepositoryState,
    reconciled_at_unix_seconds: u64,
) -> ProviderRepositoryState {
    let cache_invalidated_at = reconciled_timestamp(
        state.last_revision_pushed_at_unix_seconds(),
        state.last_cache_invalidated_at_unix_seconds(),
        reconciled_at_unix_seconds,
    );
    let authorization_rechecked_at = reconciled_timestamp(
        state.last_access_changed_at_unix_seconds(),
        state.last_authorization_rechecked_at_unix_seconds(),
        reconciled_at_unix_seconds,
    );
    let drift_checked_at = reconciled_timestamp(
        latest_lifecycle_signal_at(state),
        state.last_drift_checked_at_unix_seconds(),
        reconciled_at_unix_seconds,
    );

    state.clone().with_reconciliation(
        cache_invalidated_at,
        authorization_rechecked_at,
        drift_checked_at,
    )
}

fn reconciled_timestamp(
    signal_at: Option<u64>,
    reconciled_at: Option<u64>,
    now_unix_seconds: u64,
) -> Option<u64> {
    let Some(signal_at) = signal_at else {
        return reconciled_at;
    };
    if reconciled_at.is_some_and(|value| value >= signal_at) {
        return reconciled_at;
    }
    Some(now_unix_seconds)
}

#[must_use]
pub fn latest_lifecycle_signal_at(state: &ProviderRepositoryState) -> Option<u64> {
    match (
        state.last_access_changed_at_unix_seconds(),
        state.last_revision_pushed_at_unix_seconds(),
    ) {
        (Some(access_changed_at), Some(revision_pushed_at)) => {
            Some(access_changed_at.max(revision_pushed_at))
        }
        (Some(access_changed_at), None) => Some(access_changed_at),
        (None, Some(revision_pushed_at)) => Some(revision_pushed_at),
        (None, None) => None,
    }
}

fn normalize_provider_name(provider: &str) -> &str {
    match provider {
        "github" | "githubs" => "github",
        "gitea" | "giteas" => "gitea",
        "gitlab" | "gitlabs" => "gitlab",
        other => other,
    }
}

/// Validates that the provider name segment is present and within bounds.
///
/// # Errors
///
/// Returns [`ServerError::InvalidProviderTokenRequest`] if the name is empty or exceeds the maximum byte length.
pub const fn validate_provider_name_path(provider: &str) -> Result<(), ServerError> {
    if provider.is_empty() || provider.len() > MAX_PROVIDER_NAME_BYTES {
        return Err(ServerError::InvalidProviderTokenRequest);
    }

    Ok(())
}

/// Extracts the provider subject from request headers or query parameters.
///
/// # Errors
///
/// Returns [`ServerError::InvalidProviderTokenRequest`] if the subject is missing or malformed.
pub fn extract_provider_subject(
    headers: &HeaderMap,
    query_subject: Option<&str>,
) -> Result<String, ServerError> {
    if let Some(subject) = bounded_subject(query_subject)? {
        return Ok(subject.to_owned());
    }

    if let Some(subject_header) = headers.get("x-shardline-provider-subject") {
        let subject_header = subject_header
            .to_str()
            .map_err(|_error| ServerError::InvalidProviderTokenRequest)?;
        if let Some(subject) = bounded_subject(Some(subject_header))? {
            return Ok(subject.to_owned());
        }
    }

    let Some(header) = headers.get(AUTHORIZATION) else {
        return Err(ServerError::MissingProviderSubject);
    };
    let header = header
        .to_str()
        .map_err(|_error| ServerError::InvalidAuthorizationHeader)?;
    let Some(encoded) = header.strip_prefix("Basic ") else {
        return Err(ServerError::MissingProviderSubject);
    };
    if encoded.len() > MAX_PROVIDER_BASIC_AUTH_HEADER_BYTES {
        return Err(ServerError::InvalidAuthorizationHeader);
    }
    let decoded = BASE64_STANDARD
        .decode(encoded)
        .map_err(|_error| ServerError::InvalidAuthorizationHeader)?;
    let decoded = SecretBytes::new(decoded);
    let decoded = from_utf8(decoded.expose_secret())
        .map_err(|_error| ServerError::InvalidAuthorizationHeader)?;
    let Some((username, _password)) = decoded.split_once(':') else {
        return Err(ServerError::InvalidAuthorizationHeader);
    };
    let Some(username) = bounded_subject(Some(username))? else {
        return Err(ServerError::MissingProviderSubject);
    };
    Ok(username.to_owned())
}

fn bounded_subject(value: Option<&str>) -> Result<Option<&str>, ServerError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if value.len() > MAX_PROVIDER_SUBJECT_BYTES {
        return Err(ServerError::InvalidProviderTokenRequest);
    }

    Ok(Some(value))
}

pub(super) fn map_provider_issue_error(error: ProviderServiceError) -> ServerError {
    match error {
        ProviderServiceError::MissingApiKey => ServerError::MissingProviderApiKey,
        ProviderServiceError::InvalidApiKey => ServerError::InvalidProviderApiKey,
        ProviderServiceError::UnknownProvider => ServerError::UnknownProvider,
        ProviderServiceError::Denied => ServerError::ProviderDenied,
        ProviderServiceError::BuiltIn(BuiltInProviderError::MissingWebhookAuthentication) => {
            ServerError::MissingProviderWebhookAuthentication
        }
        ProviderServiceError::BuiltIn(BuiltInProviderError::InvalidWebhookAuthentication) => {
            ServerError::InvalidProviderWebhookAuthentication
        }
        ProviderServiceError::BuiltIn(BuiltInProviderError::InvalidWebhookPayload)
        | ProviderServiceError::BuiltIn(BuiltInProviderError::InvalidRepositoryPayload)
        | ProviderServiceError::BuiltIn(BuiltInProviderError::InvalidRevisionPayload) => {
            ServerError::InvalidProviderWebhookPayload
        }
        ProviderServiceError::Reference(_) | ProviderServiceError::Subject(_) => {
            ServerError::InvalidProviderTokenRequest
        }
        other @ ProviderServiceError::EmptyApiKey
        | other @ ProviderServiceError::ApiKeyTooLarge
        | other @ ProviderServiceError::ConfigTooLarge { .. }
        | other @ ProviderServiceError::ConfigLengthMismatch
        | other @ ProviderServiceError::Io(_)
        | other @ ProviderServiceError::Json(_)
        | other @ ProviderServiceError::DuplicateProvider
        | other @ ProviderServiceError::InvalidRepositoryVisibility
        | other @ ProviderServiceError::MissingWebhookSecret
        | other @ ProviderServiceError::EmptyWebhookSecret
        | other @ ProviderServiceError::EncryptedSecretWithoutKey
        | other @ ProviderServiceError::SecretDecrypt(_)
        | other @ ProviderServiceError::Token(_)
        | other @ ProviderServiceError::BuiltIn(_) => ServerError::Provider(other),
    }
}

pub(super) fn provider_webhook_response(
    outcome: ProviderWebhookOutcome,
) -> ProviderWebhookResponse {
    let (event_kind, new_owner, new_repo, revision) = match outcome.event_kind {
        ProviderWebhookOutcomeKind::RepositoryDeleted => {
            ("repository_deleted".to_owned(), None, None, None)
        }
        ProviderWebhookOutcomeKind::RepositoryRenamed {
            new_owner,
            new_repo,
        } => (
            "repository_renamed".to_owned(),
            Some(new_owner),
            Some(new_repo),
            None,
        ),
        ProviderWebhookOutcomeKind::AccessChanged => {
            ("access_changed".to_owned(), None, None, None)
        }
        ProviderWebhookOutcomeKind::RevisionPushed { revision } => {
            ("revision_pushed".to_owned(), None, None, Some(revision))
        }
    };

    ProviderWebhookResponse {
        provider: outcome.provider.repository_provider(),
        owner: outcome.owner,
        repo: outcome.repo,
        delivery_id: outcome.delivery_id,
        event_kind,
        new_owner,
        new_repo,
        revision,
        affected_file_versions: outcome.affected_file_versions,
        affected_chunks: outcome.affected_chunks,
        applied_holds: outcome.applied_holds,
        retention_seconds: outcome.retention_seconds,
    }
}

#[cfg(test)]
mod provider_tests {
    use shardline_index::ProviderRepositoryState;
    use shardline_protocol::RepositoryProvider;
    use shardline_vcs::{BuiltInProviderError, ProviderKind};

    use super::*;
    use crate::app::{
        MAX_PROVIDER_BASIC_AUTH_HEADER_BYTES, MAX_PROVIDER_NAME_BYTES, MAX_PROVIDER_SUBJECT_BYTES,
    };
    use crate::{
        ServerBackend, ServerConfig, ServerError, ServerFrontend, ServerRole,
        app::ProtocolMetrics,
        config::AuthProviderKind,
        model::{ProviderTokenIssueRequest, ProviderTokenIssueResponse},
        provider::ProviderServiceError,
        provider_events::{ProviderWebhookOutcome, ProviderWebhookOutcomeKind},
        reconstruction_cache::ReconstructionCacheService,
        transfer_limiter::TransferLimiter,
    };

    #[test]
    fn normalize_provider_name_github() {
        assert_eq!(normalize_provider_name("github"), "github");
        assert_eq!(normalize_provider_name("githubs"), "github");
    }

    #[test]
    fn normalize_provider_name_gitea() {
        assert_eq!(normalize_provider_name("gitea"), "gitea");
        assert_eq!(normalize_provider_name("giteas"), "gitea");
    }

    #[test]
    fn normalize_provider_name_gitlab() {
        assert_eq!(normalize_provider_name("gitlab"), "gitlab");
        assert_eq!(normalize_provider_name("gitlabs"), "gitlab");
    }

    #[test]
    fn normalize_provider_name_passthrough_unknown() {
        assert_eq!(normalize_provider_name("custom"), "custom");
    }

    #[test]
    fn reconcile_timestamp_with_signal_after_reconciled() {
        // When signal_at is after reconciled_at, returns now
        let result = reconciled_timestamp(Some(100), Some(50), 200);
        assert_eq!(result, Some(200));
    }

    #[test]
    fn reconcile_timestamp_with_reconciled_after_signal() {
        // When reconciled_at is already after signal_at, keep reconciled_at
        let result = reconciled_timestamp(Some(50), Some(100), 200);
        assert_eq!(result, Some(100));
    }

    #[test]
    fn reconcile_timestamp_with_equal_values() {
        let result = reconciled_timestamp(Some(100), Some(100), 200);
        assert_eq!(result, Some(100));
    }

    #[test]
    fn reconcile_timestamp_without_signal() {
        // When signal_at is None, return reconciled_at unchanged
        let result = reconciled_timestamp(None, Some(50), 200);
        assert_eq!(result, Some(50));
    }

    #[test]
    fn reconcile_timestamp_without_either() {
        let result = reconciled_timestamp(None, None, 200);
        assert_eq!(result, None);
    }

    #[test]
    fn reconcile_timestamp_without_reconciled() {
        let result = reconciled_timestamp(Some(100), None, 200);
        assert_eq!(result, Some(200));
    }

    #[test]
    fn latest_lifecycle_signal_at_with_both() {
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "org".to_owned(),
            "repo".to_owned(),
            Some(10), // access_changed_at
            Some(20), // revision_pushed_at
            None,
        );
        assert_eq!(latest_lifecycle_signal_at(&state), Some(20));
    }

    #[test]
    fn latest_lifecycle_signal_at_with_only_access_changed() {
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "org".to_owned(),
            "repo".to_owned(),
            Some(10),
            None,
            None,
        );
        assert_eq!(latest_lifecycle_signal_at(&state), Some(10));
    }

    #[test]
    fn latest_lifecycle_signal_at_with_only_revision_pushed() {
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "org".to_owned(),
            "repo".to_owned(),
            None,
            Some(20),
            None,
        );
        assert_eq!(latest_lifecycle_signal_at(&state), Some(20));
    }

    #[test]
    fn latest_lifecycle_signal_at_with_neither() {
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "org".to_owned(),
            "repo".to_owned(),
            None,
            None,
            None,
        );
        assert_eq!(latest_lifecycle_signal_at(&state), None);
    }

    #[test]
    fn bounded_subject_accepts_valid_value() {
        let result = bounded_subject(Some("valid-subject")).unwrap();
        assert_eq!(result, Some("valid-subject"));
    }

    #[test]
    fn bounded_subject_rejects_empty() {
        let result = bounded_subject(Some("")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn bounded_subject_rejects_whitespace_only() {
        let result = bounded_subject(Some("   ")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn bounded_subject_returns_none_for_none_input() {
        let result: Option<&str> = bounded_subject(None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn map_provider_issue_error_missing_api_key() {
        let result = map_provider_issue_error(ProviderServiceError::MissingApiKey);
        assert!(matches!(result, ServerError::MissingProviderApiKey));
    }

    #[test]
    fn map_provider_issue_error_invalid_api_key() {
        let result = map_provider_issue_error(ProviderServiceError::InvalidApiKey);
        assert!(matches!(result, ServerError::InvalidProviderApiKey));
    }

    #[test]
    fn map_provider_issue_error_unknown_provider() {
        let result = map_provider_issue_error(ProviderServiceError::UnknownProvider);
        assert!(matches!(result, ServerError::UnknownProvider));
    }

    #[test]
    fn map_provider_issue_error_denied() {
        let result = map_provider_issue_error(ProviderServiceError::Denied);
        assert!(matches!(result, ServerError::ProviderDenied));
    }

    #[test]
    fn map_provider_issue_error_missing_webhook_auth() {
        let built_in = BuiltInProviderError::MissingWebhookAuthentication;
        let result = map_provider_issue_error(ProviderServiceError::BuiltIn(built_in));
        assert!(matches!(
            result,
            ServerError::MissingProviderWebhookAuthentication
        ));
    }

    #[test]
    fn map_provider_issue_error_invalid_webhook_auth() {
        let built_in = BuiltInProviderError::InvalidWebhookAuthentication;
        let result = map_provider_issue_error(ProviderServiceError::BuiltIn(built_in));
        assert!(matches!(
            result,
            ServerError::InvalidProviderWebhookAuthentication
        ));
    }

    #[test]
    fn map_provider_issue_error_invalid_webhook_payload() {
        let built_in = BuiltInProviderError::InvalidWebhookPayload;
        let result = map_provider_issue_error(ProviderServiceError::BuiltIn(built_in));
        assert!(matches!(result, ServerError::InvalidProviderWebhookPayload));
    }

    #[test]
    fn map_provider_issue_error_generic_fallthrough() {
        let result = map_provider_issue_error(ProviderServiceError::EmptyApiKey);
        assert!(matches!(result, ServerError::Provider(_)));
    }

    #[test]
    fn provider_webhook_response_revision_pushed() {
        let outcome = ProviderWebhookOutcome {
            provider: ProviderKind::GitHub,
            owner: "org".to_owned(),
            repo: "repo".to_owned(),
            delivery_id: "del-123".to_owned(),
            event_kind: ProviderWebhookOutcomeKind::RevisionPushed {
                revision: "abc123".to_owned(),
            },
            affected_file_versions: 5,
            affected_chunks: 20,
            applied_holds: 3,
            retention_seconds: Some(86400),
        };
        let response = provider_webhook_response(outcome);
        assert_eq!(response.event_kind, "revision_pushed");
        assert_eq!(response.revision.as_deref(), Some("abc123"));
        assert_eq!(response.affected_file_versions, 5);
    }

    #[test]
    fn provider_webhook_response_repository_deleted() {
        let outcome = ProviderWebhookOutcome {
            provider: ProviderKind::GitHub,
            owner: "org".to_owned(),
            repo: "repo".to_owned(),
            delivery_id: "del-456".to_owned(),
            event_kind: ProviderWebhookOutcomeKind::RepositoryDeleted,
            affected_file_versions: 0,
            affected_chunks: 0,
            applied_holds: 0,
            retention_seconds: None,
        };
        let response = provider_webhook_response(outcome);
        assert_eq!(response.event_kind, "repository_deleted");
        assert!(response.revision.is_none());
        assert!(response.new_owner.is_none());
    }

    #[test]
    fn provider_webhook_response_repository_renamed() {
        let outcome = ProviderWebhookOutcome {
            provider: ProviderKind::GitHub,
            owner: "old-org".to_owned(),
            repo: "old-repo".to_owned(),
            delivery_id: "del-789".to_owned(),
            event_kind: ProviderWebhookOutcomeKind::RepositoryRenamed {
                new_owner: "new-org".to_owned(),
                new_repo: "new-repo".to_owned(),
            },
            affected_file_versions: 2,
            affected_chunks: 8,
            applied_holds: 1,
            retention_seconds: Some(3600),
        };
        let response = provider_webhook_response(outcome);
        assert_eq!(response.event_kind, "repository_renamed");
        assert_eq!(response.new_owner.as_deref(), Some("new-org"));
        assert_eq!(response.new_repo.as_deref(), Some("new-repo"));
    }

    #[test]
    fn provider_webhook_response_access_changed() {
        let outcome = ProviderWebhookOutcome {
            provider: ProviderKind::GitHub,
            owner: "org".to_owned(),
            repo: "repo".to_owned(),
            delivery_id: "del-012".to_owned(),
            event_kind: ProviderWebhookOutcomeKind::AccessChanged,
            affected_file_versions: 0,
            affected_chunks: 0,
            applied_holds: 0,
            retention_seconds: None,
        };
        let response = provider_webhook_response(outcome);
        assert_eq!(response.event_kind, "access_changed");
    }

    // -----------------------------------------------------------------------
    // validate_provider_name_path
    // -----------------------------------------------------------------------

    #[test]
    fn validate_provider_name_path_accepts_valid_name() {
        assert!(validate_provider_name_path("github").is_ok());
        assert!(validate_provider_name_path("a").is_ok());
        assert!(validate_provider_name_path("custom-provider-123").is_ok());
    }

    #[test]
    fn validate_provider_name_path_rejects_empty() {
        assert!(matches!(
            validate_provider_name_path(""),
            Err(ServerError::InvalidProviderTokenRequest)
        ));
    }

    #[test]
    fn validate_provider_name_path_rejects_too_long() {
        let long_name = "a".repeat(MAX_PROVIDER_NAME_BYTES + 1);
        assert!(matches!(
            validate_provider_name_path(&long_name),
            Err(ServerError::InvalidProviderTokenRequest)
        ));
    }

    #[test]
    fn validate_provider_name_path_accepts_max_length() {
        let max_name = "a".repeat(MAX_PROVIDER_NAME_BYTES);
        assert!(validate_provider_name_path(&max_name).is_ok());
    }

    // -----------------------------------------------------------------------
    // extract_provider_subject
    // -----------------------------------------------------------------------

    #[test]
    fn extract_provider_subject_from_query() {
        let headers = HeaderMap::new();
        let result = extract_provider_subject(&headers, Some("query-subject"));
        assert_eq!(result.unwrap(), "query-subject");
    }

    #[test]
    fn extract_provider_subject_query_takes_priority() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-shardline-provider-subject",
            "header-subject".parse().unwrap(),
        );
        let result = extract_provider_subject(&headers, Some("query-subject"));
        // query wins over header
        assert_eq!(result.unwrap(), "query-subject");
    }

    #[test]
    fn extract_provider_subject_from_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-shardline-provider-subject",
            "header-subject".parse().unwrap(),
        );
        let result = extract_provider_subject(&headers, None);
        assert_eq!(result.unwrap(), "header-subject");
    }

    #[test]
    fn extract_provider_subject_header_whitespace_only_is_skipped() {
        let mut headers = HeaderMap::new();
        headers.insert("x-shardline-provider-subject", "   ".parse().unwrap());
        // No authorization header — should fail with MissingProviderSubject
        let result = extract_provider_subject(&headers, None);
        assert!(matches!(result, Err(ServerError::MissingProviderSubject)));
    }

    #[test]
    fn extract_provider_subject_from_basic_auth() {
        let mut headers = HeaderMap::new();
        // "alice:password" base64
        let encoded = BASE64_STANDARD.encode(b"alice:password");
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Basic {encoded}").parse().unwrap(),
        );
        let result = extract_provider_subject(&headers, None);
        assert_eq!(result.unwrap(), "alice");
    }

    #[test]
    fn extract_provider_subject_basic_auth_no_password_delimiter() {
        let mut headers = HeaderMap::new();
        // No colon in decoded — split_once returns None
        let encoded = BASE64_STANDARD.encode(b"justusername");
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Basic {encoded}").parse().unwrap(),
        );
        let result = extract_provider_subject(&headers, None);
        assert!(matches!(
            result,
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn extract_provider_subject_basic_auth_password_only_means_empty_username() {
        let mut headers = HeaderMap::new();
        let encoded = BASE64_STANDARD.encode(b":password");
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Basic {encoded}").parse().unwrap(),
        );
        // After split_once(':'), username is "" which is empty → MissingProviderSubject
        let result = extract_provider_subject(&headers, None);
        assert!(matches!(result, Err(ServerError::MissingProviderSubject)));
    }

    #[test]
    fn extract_provider_subject_missing_all_sources() {
        let headers = HeaderMap::new();
        let result = extract_provider_subject(&headers, None);
        assert!(matches!(result, Err(ServerError::MissingProviderSubject)));
    }

    #[test]
    fn extract_provider_subject_rejects_invalid_utf8_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-shardline-provider-subject",
            axum::http::HeaderValue::from_bytes(b"\xff\xfe").unwrap(),
        );
        let result = extract_provider_subject(&headers, None);
        assert!(matches!(
            result,
            Err(ServerError::InvalidProviderTokenRequest)
        ));
    }

    #[test]
    fn extract_provider_subject_rejects_basic_auth_without_prefix() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer token".parse().unwrap(),
        );
        let result = extract_provider_subject(&headers, None);
        // No "Basic " prefix → MissingProviderSubject
        assert!(matches!(result, Err(ServerError::MissingProviderSubject)));
    }

    #[test]
    fn extract_provider_subject_rejects_invalid_basic_auth_encoding() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Basic not-valid-base64!!".parse().unwrap(),
        );
        let result = extract_provider_subject(&headers, None);
        assert!(matches!(
            result,
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn extract_provider_subject_rejects_basic_auth_too_large() {
        let mut headers = HeaderMap::new();
        let large = "a".repeat(MAX_PROVIDER_BASIC_AUTH_HEADER_BYTES + 1);
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Basic {large}").parse().unwrap(),
        );
        let result = extract_provider_subject(&headers, None);
        assert!(matches!(
            result,
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn extract_provider_subject_rejects_query_subject_too_long() {
        let headers = HeaderMap::new();
        let long_subject = "a".repeat(MAX_PROVIDER_SUBJECT_BYTES + 1);
        let result = extract_provider_subject(&headers, Some(&long_subject));
        assert!(matches!(
            result,
            Err(ServerError::InvalidProviderTokenRequest)
        ));
    }

    // -----------------------------------------------------------------------
    // bounded_subject — edge case for max length
    // -----------------------------------------------------------------------

    #[test]
    fn bounded_subject_rejects_too_long() {
        let long = "a".repeat(MAX_PROVIDER_SUBJECT_BYTES + 1);
        assert!(matches!(
            bounded_subject(Some(&long)),
            Err(ServerError::InvalidProviderTokenRequest)
        ));
    }

    #[test]
    fn bounded_subject_accepts_max_length() {
        let max = "a".repeat(MAX_PROVIDER_SUBJECT_BYTES);
        let result = bounded_subject(Some(&max)).unwrap();
        assert_eq!(result, Some(max.as_str()));
    }

    // -----------------------------------------------------------------------
    // map_provider_issue_error — all uncovered branches
    // -----------------------------------------------------------------------

    #[test]
    fn map_provider_issue_error_reference() {
        let ref_err = shardline_vcs::VcsReferenceError::Empty;
        let result = map_provider_issue_error(ProviderServiceError::Reference(ref_err));
        assert!(matches!(result, ServerError::InvalidProviderTokenRequest));
    }

    #[test]
    fn map_provider_issue_error_subject() {
        let subj_err = shardline_vcs::ProviderBoundaryError::Empty;
        let result = map_provider_issue_error(ProviderServiceError::Subject(subj_err));
        assert!(matches!(result, ServerError::InvalidProviderTokenRequest));
    }

    #[test]
    fn map_provider_issue_error_api_key_too_large() {
        let result = map_provider_issue_error(ProviderServiceError::ApiKeyTooLarge);
        assert!(matches!(result, ServerError::Provider(_)));
    }

    #[test]
    fn map_provider_issue_error_config_too_large() {
        let result = map_provider_issue_error(ProviderServiceError::ConfigTooLarge {
            observed_bytes: 999,
            maximum_bytes: 100,
        });
        assert!(matches!(result, ServerError::Provider(_)));
    }

    #[test]
    fn map_provider_issue_error_config_length_mismatch() {
        let result = map_provider_issue_error(ProviderServiceError::ConfigLengthMismatch);
        assert!(matches!(result, ServerError::Provider(_)));
    }

    #[test]
    fn map_provider_issue_error_io() {
        let result =
            map_provider_issue_error(ProviderServiceError::Io(std::io::Error::other("io error")));
        assert!(matches!(result, ServerError::Provider(_)));
    }

    #[test]
    fn map_provider_issue_error_json() {
        let json_err = serde_json::from_str::<()>("invalid").unwrap_err();
        let result = map_provider_issue_error(ProviderServiceError::Json(json_err));
        assert!(matches!(result, ServerError::Provider(_)));
    }

    #[test]
    fn map_provider_issue_error_duplicate_provider() {
        let result = map_provider_issue_error(ProviderServiceError::DuplicateProvider);
        assert!(matches!(result, ServerError::Provider(_)));
    }

    #[test]
    fn map_provider_issue_error_missing_webhook_secret() {
        let result = map_provider_issue_error(ProviderServiceError::MissingWebhookSecret);
        assert!(matches!(result, ServerError::Provider(_)));
    }

    #[test]
    fn map_provider_issue_error_empty_webhook_secret() {
        let result = map_provider_issue_error(ProviderServiceError::EmptyWebhookSecret);
        assert!(matches!(result, ServerError::Provider(_)));
    }

    #[test]
    fn map_provider_issue_error_token() {
        let token_err = shardline_vcs::ProviderTokenIssuanceError::LifetimeOverflow;
        let result = map_provider_issue_error(ProviderServiceError::Token(token_err));
        assert!(matches!(result, ServerError::Provider(_)));
    }

    #[test]
    fn map_provider_issue_error_builtin_invalid_repository_payload() {
        let built_in = BuiltInProviderError::InvalidRepositoryPayload;
        let result = map_provider_issue_error(ProviderServiceError::BuiltIn(built_in));
        assert!(matches!(result, ServerError::InvalidProviderWebhookPayload));
    }

    #[test]
    fn map_provider_issue_error_builtin_invalid_revision_payload() {
        let built_in = BuiltInProviderError::InvalidRevisionPayload;
        let result = map_provider_issue_error(ProviderServiceError::BuiltIn(built_in));
        assert!(matches!(result, ServerError::InvalidProviderWebhookPayload));
    }

    // -----------------------------------------------------------------------
    // reconciled_provider_repository_state
    // -----------------------------------------------------------------------

    #[test]
    fn reconciled_provider_repository_state_returns_updated_state() {
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "org".to_owned(),
            "repo".to_owned(),
            None,
            None,
            None,
        );
        let reconciled = reconciled_provider_repository_state(&state, 1000);
        // All three reconciliation timestamps should be None since there are
        // no signals and no prior reconciled values.
        assert_eq!(reconciled, state.with_reconciliation(None, None, None));
    }

    #[test]
    fn reconciled_provider_repository_state_with_signals() {
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "org".to_owned(),
            "repo".to_owned(),
            Some(10), // access_changed_at
            Some(50), // revision_pushed_at
            None,
        );
        // last_access_changed_at=10 > last_authorization_rechecked_at=None → now
        // latest_lifecycle_signal_at=max(10,50)=50 > last_drift_checked_at=None → now
        // last_revision_pushed_at=50 > last_cache_invalidated_at=None → now
        let reconciled = reconciled_provider_repository_state(&state, 1000);
        assert_eq!(
            reconciled.last_cache_invalidated_at_unix_seconds(),
            Some(1000)
        );
        assert_eq!(
            reconciled.last_authorization_rechecked_at_unix_seconds(),
            Some(1000)
        );
        assert_eq!(reconciled.last_drift_checked_at_unix_seconds(), Some(1000));
    }

    #[test]
    fn reconciled_provider_repository_state_preserves_existing_good_timestamps() {
        // If reconciled_at already >= signal_at, keep it
        let state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "org".to_owned(),
            "repo".to_owned(),
            Some(10), // access_changed_at
            Some(5),  // revision_pushed_at
            None,
        );
        // Use with_reconciliation to set prior reconciled values that are
        // already after the signal.
        let state = state.with_reconciliation(Some(20), Some(15), Some(25));
        let reconciled = reconciled_provider_repository_state(&state, 1000);
        assert_eq!(
            reconciled.last_cache_invalidated_at_unix_seconds(),
            Some(20)
        );
        assert_eq!(
            reconciled.last_authorization_rechecked_at_unix_seconds(),
            Some(15)
        );
        assert_eq!(reconciled.last_drift_checked_at_unix_seconds(), Some(25));
    }

    // -----------------------------------------------------------------------
    // authenticate_provider_token_request / issue_provider_token_response
    // -----------------------------------------------------------------------

    /// Builds a minimal AppState with provider_tokens = None for testing
    /// the disabled-token early-return paths.
    async fn state_without_provider_tokens() -> AppState {
        let tmp = tempfile::TempDir::new().unwrap();
        let chunk_size = std::num::NonZeroUsize::new(65536).unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:8080".to_owned(),
            tmp.path().to_path_buf(),
            chunk_size,
        )
        .with_auth_provider(AuthProviderKind::Local)
        .with_server_frontends(vec![ServerFrontend::Xet])
        .unwrap();
        let backend = ServerBackend::from_config(&config).await.unwrap();
        AppState {
            config,
            role: ServerRole::All,
            backend,
            auth: None,
            provider_tokens: None,
            reconstruction_cache: ReconstructionCacheService::disabled(),
            transfer_limiter: TransferLimiter::new(chunk_size, chunk_size),
            oci_registry_token_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(16)),
            admission: crate::admission::WeightedAdmission::new(
                std::num::NonZeroUsize::new(256).unwrap(),
            ),
            pools: crate::admission::ExecutionPools::default_sizes(),
            protocol_metrics: ProtocolMetrics::default(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn authenticate_provider_token_request_disabled_returns_error() {
        let state = state_without_provider_tokens().await;
        let headers = HeaderMap::new();
        let result = authenticate_provider_token_request(&state, &headers, "github");
        assert!(
            matches!(result, Err(ServerError::ProviderTokensDisabled)),
            "with provider_tokens=None, should return ProviderTokensDisabled"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn issue_provider_token_response_disabled_returns_error() {
        let state = state_without_provider_tokens().await;
        let headers = HeaderMap::new();
        let request = ProviderTokenIssueRequest {
            subject: String::new(),
            owner: String::new(),
            repo: String::new(),
            revision: None,
            scope: shardline_protocol::TokenScope::Read,
        };
        let result = issue_provider_token_response(&state, &headers, "github", &request).await;
        assert!(
            matches!(result, Err(ServerError::ProviderTokensDisabled)),
            "with provider_tokens=None, should return ProviderTokensDisabled"
        );
    }

    // -----------------------------------------------------------------------
    // reconcile_provider_repository_state_with_store
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconcile_provider_repository_state_with_store_no_existing() {
        use shardline_protocol::SecretString;
        // When there is no existing state, reconcile is a no-op (returns Ok).
        let tmp = tempfile::TempDir::new().unwrap();
        let store = shardline_index::LocalIndexStore::open(tmp.path().to_path_buf());
        let issued = ProviderTokenIssueResponse {
            token: SecretString::from_secret("tok"),
            issuer: "iss".to_owned(),
            subject: "sub".to_owned(),
            provider: shardline_protocol::RepositoryProvider::GitHub,
            owner: "org".to_owned(),
            repo: "repo".to_owned(),
            revision: None,
            scope: shardline_protocol::TokenScope::Read,
            expires_at_unix_seconds: 99999,
        };
        let result = reconcile_provider_repository_state_with_store(&store, &issued).await;
        assert!(result.is_ok(), "no existing state should succeed (no-op)");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconcile_provider_repository_state_with_store_matching_noop() {
        use shardline_protocol::{RepositoryProvider, SecretString};
        // When existing == reconciled the function returns early (no-op).
        let tmp = tempfile::TempDir::new().unwrap();
        let store = shardline_index::LocalIndexStore::open(tmp.path().to_path_buf());

        // Seed an existing state with reconciliation timestamps that already
        // exceed the signal timestamps — reconcile should see no change needed.
        let seed = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "org".to_owned(),
            "repo".to_owned(),
            Some(100), // access_changed_at
            None,      // revision_pushed_at
            None,
        )
        .with_reconciliation(Some(200), Some(200), Some(200));
        store.upsert_provider_repository_state(&seed).await.unwrap();

        // Call reconcile — since the reconciled values already exceed the
        // signals, they should match existing and return early (no-op).
        let issued = ProviderTokenIssueResponse {
            token: SecretString::from_secret("tok"),
            issuer: "iss".to_owned(),
            subject: "sub".to_owned(),
            provider: RepositoryProvider::GitHub,
            owner: "org".to_owned(),
            repo: "repo".to_owned(),
            revision: None,
            scope: shardline_protocol::TokenScope::Read,
            expires_at_unix_seconds: 99999,
        };
        let result = reconcile_provider_repository_state_with_store(&store, &issued).await;
        assert!(result.is_ok());

        // Verify the stored state was NOT updated
        let after = store
            .provider_repository_state(RepositoryProvider::GitHub, "org", "repo")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.last_cache_invalidated_at_unix_seconds(), Some(200));
        assert_eq!(
            after.last_authorization_rechecked_at_unix_seconds(),
            Some(200)
        );
        assert_eq!(after.last_drift_checked_at_unix_seconds(), Some(200));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconcile_provider_repository_state_with_store_different_upserts() {
        use shardline_protocol::{RepositoryProvider, SecretString};
        // When existing != reconciled, the function upserts the new state.
        let tmp = tempfile::TempDir::new().unwrap();
        let store = shardline_index::LocalIndexStore::open(tmp.path().to_path_buf());

        // Seed an existing state where authorization_rechecked_at is stale.
        // access_changed_at=100 > authorization_rechecked_at=50 → update needed.
        let seed = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "org-upsert".to_owned(),
            "repo".to_owned(),
            Some(100), // access_changed_at
            None,      // revision_pushed_at
            None,
        )
        .with_reconciliation(Some(200), Some(50), Some(200));

        store.upsert_provider_repository_state(&seed).await.unwrap();

        let issued = ProviderTokenIssueResponse {
            token: SecretString::from_secret("tok"),
            issuer: "iss".to_owned(),
            subject: "sub".to_owned(),
            provider: RepositoryProvider::GitHub,
            owner: "org-upsert".to_owned(),
            repo: "repo".to_owned(),
            revision: None,
            scope: shardline_protocol::TokenScope::Read,
            expires_at_unix_seconds: 99999,
        };

        // Reconciliation should detect that authorization_rechecked_at=50
        // is stale (access_changed_at=100) and upsert a new state.
        let result = reconcile_provider_repository_state_with_store(&store, &issued).await;
        assert!(result.is_ok());

        // Verify the state was updated
        let after = store
            .provider_repository_state(RepositoryProvider::GitHub, "org-upsert", "repo")
            .await
            .unwrap()
            .unwrap();
        // authorization_rechecked_at should now be Some(now) instead of Some(50)
        assert!(
            after.last_authorization_rechecked_at_unix_seconds() != Some(50),
            "state should have been updated with new reconciliation timestamp, got: {:?}",
            after.last_authorization_rechecked_at_unix_seconds()
        );
    }
}
