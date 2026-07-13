use std::str::from_utf8;

use axum::{
    Json,
    body::Body,
    http::{HeaderMap, header::AUTHORIZATION},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde_json::from_slice;
use shardline_index::{
    AsyncIndexStore, LocalIndexStore, PostgresIndexStore, ProviderRepositoryState,
};
use shardline_protocol::{SecretBytes, TokenScope};
use shardline_vcs::BuiltInProviderError;

use super::{
    AppState, MAX_PROVIDER_BASIC_AUTH_HEADER_BYTES, MAX_PROVIDER_NAME_BYTES,
    MAX_PROVIDER_SUBJECT_BYTES, MAX_PROVIDER_TOKEN_REQUEST_BODY_BYTES, endpoint_body_limit,
};
use crate::{
    ServerError,
    clock::unix_now_seconds_checked,
    model::{
        ProviderTokenIssueRequest, ProviderTokenIssueResponse, ProviderWebhookResponse,
        XetCasTokenResponse,
    },
    postgres_backend::connect_postgres_metadata_pool,
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
        access_token: issued.token,
    }))
}

async fn reconcile_provider_repository_state(
    state: &AppState,
    issued: &ProviderTokenIssueResponse,
) -> Result<(), ServerError> {
    if let Some(index_postgres_url) = state.config.index_postgres_url() {
        let pool = connect_postgres_metadata_pool(index_postgres_url, 4)?;
        let index_store = PostgresIndexStore::new(pool);
        return reconcile_provider_repository_state_with_store(&index_store, issued).await;
    }

    let index_store = LocalIndexStore::open(state.config.root_dir().to_path_buf());
    reconcile_provider_repository_state_with_store(&index_store, issued).await
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
        | other @ ProviderServiceError::MissingWebhookSecret
        | other @ ProviderServiceError::EmptyWebhookSecret
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
    use crate::{
        ServerError,
        provider::ProviderServiceError,
        provider_events::{ProviderWebhookOutcome, ProviderWebhookOutcomeKind},
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
            Some(10),  // access_changed_at
            Some(20),  // revision_pushed_at
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
        assert!(matches!(
            result,
            ServerError::InvalidProviderWebhookPayload
        ));
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
}
