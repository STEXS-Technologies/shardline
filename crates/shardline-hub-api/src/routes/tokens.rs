use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Uri},
    response::{IntoResponse, Response},
};

use super::{HubRepository, HubState};
use crate::{error::HubApiError, models::TokenExchangeResponse};
use shardline_index::hub::HubRepoType;
use shardline_protocol::{RepositoryScope, TokenClaims, TokenScope};

// ---- Token exchange (requires Read scope) ----

pub(crate) async fn xet_read_token(
    State(state): State<HubState>,
    repo: HubRepository,
    Path((ns, repo_name, revision)): Path<(String, String, String)>,
    uri: Uri,
) -> Result<Response, HubApiError> {
    let repo_type = repo_type_from_uri(&uri)?;
    xet_token_response(
        &state,
        &repo,
        repo_type,
        &ns,
        &repo_name,
        &revision,
        "xet_read_token",
    )
    .await
}

pub(crate) async fn xet_write_token(
    State(state): State<HubState>,
    repo: HubRepository<true>,
    Path((ns, repo_name, revision)): Path<(String, String, String)>,
    uri: Uri,
) -> Result<Response, HubApiError> {
    let repo_type = repo_type_from_uri(&uri)?;
    xet_token_response(
        &state,
        &repo,
        repo_type,
        &ns,
        &repo_name,
        &revision,
        "xet_write_token",
    )
    .await
}

fn repo_type_from_uri(uri: &Uri) -> Result<&str, HubApiError> {
    uri.path()
        .split('/')
        .nth(2)
        .filter(|repo_type| matches!(*repo_type, "models" | "datasets" | "spaces"))
        .ok_or_else(|| HubApiError::PathValidation("unsupported repository type".to_owned()))
}

async fn xet_token_response<const WRITE: bool>(
    state: &HubState,
    repo: &HubRepository<WRITE>,
    repo_type: &str,
    namespace: &str,
    repo_name: &str,
    revision: &str,
    metric: &'static str,
) -> Result<Response, HubApiError> {
    let claims = repo
        .capability()
        .claims()
        .ok_or(HubApiError::Unauthorized)?;
    let auth = state.auth.as_ref().ok_or(HubApiError::Unauthorized)?;
    let hub_repo_type = HubRepoType::parse_str(repo_type)
        .ok_or_else(|| HubApiError::PathValidation("unsupported repository type".to_owned()))?;
    let repo_id = format!("{namespace}/{repo_name}");
    let stored_repo = state
        .store
        .get_repo(&repo_id)
        .map_err(|error| HubApiError::CasError(error.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    if stored_repo.repo_type != hub_repo_type {
        return Err(HubApiError::RepoNotFound);
    }
    let commit_sha =
        crate::git::smart_http::resolve_commit_id(state, &repo_id, revision, repo.capability())?
            .ok_or(HubApiError::RevisionNotFound)?;

    let source_repository = claims.repository();
    if let Some(scoped_revision) = source_repository.revision()
        && scoped_revision != revision
    {
        let public = crate::git::smart_http::public_commit_id(
            state,
            &repo_id,
            &commit_sha,
            repo.capability(),
        )
        .await?;
        // Normalize immutable identities only. A ref-limited token must not
        // gain access to another branch or tag merely sharing today's head.
        if ![commit_sha.as_str(), public.as_str()].contains(&scoped_revision)
            || ![commit_sha.as_str(), public.as_str()].contains(&revision)
        {
            return Err(HubApiError::Forbidden);
        }
    }
    // Scope the CAS credential to the exact repository revision requested by
    // the Hub client. A repository-wide Hub token can request a narrower Xet
    // token; a ref-limited Hub token cannot exchange into a different ref.
    let repository = RepositoryScope::new(
        source_repository.provider(),
        namespace,
        repo_name,
        Some(revision),
    )
    .map_err(|error| {
        tracing::debug!("failed to scope Xet repository: {error}");
        HubApiError::InvalidToken
    })?;
    let scope = if WRITE {
        TokenScope::Write
    } else {
        TokenScope::Read
    };
    let cas_claims = TokenClaims::new(
        claims.issuer(),
        claims.subject(),
        scope,
        repository,
        claims.expires_at_unix_seconds(),
    )
    .map_err(|error| {
        tracing::debug!("failed to scope Xet token: {error}");
        HubApiError::InvalidToken
    })?;
    let token = auth.provider().mint_token(&cas_claims).map_err(|error| {
        tracing::debug!("failed to mint token: {error}");
        HubApiError::InvalidToken
    })?;
    // huggingface_hub's hf_xet integration reads these connection fields from
    // response headers (not the JSON body). Preserve Shardline's existing
    // {"token": ...} response body for clients already using this endpoint.
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-Xet-Cas-Url",
        HeaderValue::from_str(state.public_base_url.trim_end_matches('/'))
            .map_err(|_error| HubApiError::InvalidToken)?,
    );
    headers.insert(
        "X-Xet-Access-Token",
        HeaderValue::from_str(&token).map_err(|_error| HubApiError::InvalidToken)?,
    );
    headers.insert(
        "X-Xet-Token-Expiration",
        HeaderValue::from_str(&cas_claims.expires_at_unix_seconds().to_string())
            .map_err(|_error| HubApiError::InvalidToken)?,
    );
    shardline_metrics::record_hub_api_request(metric, "GET", 200);
    Ok((
        headers,
        Json(TokenExchangeResponse {
            token: token.clone(),
            access_token: token,
            exp: cas_claims.expires_at_unix_seconds(),
            cas_url: state.public_base_url.trim_end_matches('/').to_owned(),
        }),
    )
        .into_response())
}
