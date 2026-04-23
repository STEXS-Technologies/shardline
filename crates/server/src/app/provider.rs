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

pub(super) fn reconciled_provider_repository_state(
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

pub(super) fn latest_lifecycle_signal_at(state: &ProviderRepositoryState) -> Option<u64> {
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

pub(super) const fn validate_provider_name_path(provider: &str) -> Result<(), ServerError> {
    if provider.is_empty() || provider.len() > MAX_PROVIDER_NAME_BYTES {
        return Err(ServerError::InvalidProviderTokenRequest);
    }

    Ok(())
}

pub(super) fn extract_provider_subject(
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
