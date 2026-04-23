use std::{collections::BTreeMap, sync::Arc};

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
    clock::unix_now_seconds_checked,
    model::{GitLfsAuthenticateResponse, ProviderTokenIssueResponse, XetCasTokenResponse},
    provider_events::apply_provider_webhook,
};

use super::{
    AppState,
    provider::{
        XetTokenRequest, authenticate_provider_token_request, issue_provider_token_response,
        issue_xet_token, map_provider_issue_error, parse_provider_token_request_body,
        provider_webhook_response, validate_provider_name_path,
    },
};

#[derive(Debug, Deserialize)]
pub(super) struct XetTokenQuery {
    subject: Option<String>,
}

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
        "X-Xet-Cas-Url".to_owned(),
        state.config.public_base_url().to_owned(),
    );
    header.insert("X-Xet-Access-Token".to_owned(), issued.token.clone());
    header.insert(
        "X-Xet-Token-Expiration".to_owned(),
        issued.expires_at_unix_seconds.to_string(),
    );
    let now = unix_now_seconds_checked()?;
    Ok(Json(GitLfsAuthenticateResponse {
        href: state.config.public_base_url().to_owned(),
        header,
        expires_in: issued.expires_at_unix_seconds.saturating_sub(now),
    }))
}

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
    let outcome = apply_provider_webhook(&state.config, &event).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(provider_webhook_response(outcome)),
    )
        .into_response())
}
