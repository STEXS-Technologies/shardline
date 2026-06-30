use std::fmt::Write;
use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    http::{HeaderMap, Uri},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use shardline_protocol::{TokenClaims, TokenScope, TokenSigner};

use crate::ServerError;

use super::super::{
    AppState,
    authorize,
    parse_query_values,
};

pub(super) const OCI_REGISTRY_SERVICE: &str = "shardline";
const MAX_OCI_TOKEN_BASIC_AUTH_BYTES: usize = 8192;
const MAX_OCI_TOKEN_QUERY_SERVICE_BYTES: usize = 128;
const MAX_OCI_TOKEN_QUERY_SCOPE_BYTES: usize = 1024;
const MAX_OCI_TOKEN_QUERY_ACCOUNT_BYTES: usize = 512;
const MAX_OCI_TOKEN_QUERY_SCOPES: usize = 16;

pub(crate) async fn oci_registry_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Json<crate::model::OciRegistryTokenResponse>, ServerError> {
    state
        .protocol_metrics
        .increment_oci_registry_token_requests();
    shardline_metrics::metrics().protocol.record_oci_registry_token_request();
    let _permit = state
        .oci_registry_token_limiter
        .clone()
        .try_acquire_owned()
        .map_err(|_error| {
            state
                .protocol_metrics
                .increment_oci_registry_token_rate_limited();
            shardline_metrics::metrics().protocol.record_oci_registry_token_rate_limited();
            ServerError::TooManyRegistryTokenRequests
        })?;
    let _active_request = state.protocol_metrics.begin_oci_registry_token_request();
    shardline_metrics::metrics().protocol.begin_oci_registry_token_request();
    let _prom_active = PromActiveRequestGuard;
    let signer = TokenSigner::new(
        state
            .config
            .token_signing_key()
            .ok_or(ServerError::MissingAuthorization)?,
    )?;
    let query = parse_oci_registry_token_query(&uri)?;
    if let Some(service) = query.service.as_deref()
        && service != OCI_REGISTRY_SERVICE
    {
        return Err(ServerError::InvalidManifestReference);
    }

    let bootstrap_claims =
        verify_oci_registry_bootstrap_credentials(&headers, &signer).map_err(|error| {
            if matches!(
                error,
                ServerError::MissingAuthorization
                    | ServerError::InvalidAuthorizationHeader
                    | ServerError::InvalidToken(_)
            ) {
                ServerError::UnauthorizedChallenge(oci_bearer_challenge(
                    state.config.public_base_url(),
                    None,
                    TokenScope::Read,
                ))
            } else {
                error
            }
        })?;
    let (requested_scope, requested_repository) = parse_oci_registry_token_scopes(&query.scopes)?;
    if let Some(repository) = requested_repository.as_deref() {
        crate::protocol_support::validate_oci_repository_scope(repository, Some(bootstrap_claims.repository()))?;
    }
    if !scope_allows_oci_exchange(bootstrap_claims.scope(), requested_scope) {
        return Err(ServerError::InsufficientScope);
    }

    let now = crate::clock::unix_now_seconds_checked()?;
    let expires_at_unix_seconds = bootstrap_claims
        .expires_at_unix_seconds()
        .min(now.saturating_add(state.config.oci_registry_token_ttl_seconds().get()));
    let issued_claims = TokenClaims::new(
        bootstrap_claims.issuer(),
        bootstrap_claims.subject(),
        requested_scope.unwrap_or_else(|| bootstrap_claims.scope()),
        bootstrap_claims.repository().clone(),
        expires_at_unix_seconds,
    )
    .map_err(|_error| ServerError::InvalidProviderTokenRequest)?;
    let token = signer.sign(&issued_claims)?;
    Ok(Json(crate::model::OciRegistryTokenResponse {
        access_token: token.clone(),
        token,
        expires_in: issued_claims
            .expires_at_unix_seconds()
            .saturating_sub(now)
            .min(i32::MAX as u64),
    }))
}

pub(super) fn oci_authorize(
    state: &AppState,
    headers: &HeaderMap,
    repository: Option<&str>,
    required_scope: TokenScope,
) -> Result<Option<crate::auth::AuthContext>, ServerError> {
    match authorize(state, headers, required_scope) {
        Ok(auth) => Ok(auth),
        Err(ServerError::MissingAuthorization)
        | Err(ServerError::InvalidAuthorizationHeader)
        | Err(ServerError::InvalidToken(_)) => Err(ServerError::UnauthorizedChallenge(
            oci_bearer_challenge(state.config.public_base_url(), repository, required_scope),
        )),
        Err(error) => Err(error),
    }
}

fn oci_bearer_challenge(
    public_base_url: &str,
    repository: Option<&str>,
    required_scope: TokenScope,
) -> String {
    let realm = format!("{}/v2/token", public_base_url.trim_end_matches('/'));
    let mut challenge = format!("Bearer realm=\"{realm}\",service=\"{OCI_REGISTRY_SERVICE}\"");
    if let Some(repository) = repository {
        let actions = match required_scope {
            TokenScope::Read => "pull",
            TokenScope::Write => "pull,push",
        };
        let _ignored = write!(challenge, ",scope=\"repository:{repository}:{actions}\"");
    }
    challenge
}

fn verify_oci_registry_bootstrap_credentials(
    headers: &HeaderMap,
    signer: &TokenSigner,
) -> Result<TokenClaims, ServerError> {
    let header = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or(ServerError::MissingAuthorization)?
        .to_str()
        .map_err(|_error| ServerError::InvalidAuthorizationHeader)?;
    if let Some(token) = header.strip_prefix("Bearer ") {
        return signer.verify_now(token).map_err(ServerError::from);
    }
    let Some(encoded) = header.strip_prefix("Basic ") else {
        return Err(ServerError::InvalidAuthorizationHeader);
    };
    if encoded.len() > MAX_OCI_TOKEN_BASIC_AUTH_BYTES {
        return Err(ServerError::InvalidAuthorizationHeader);
    }
    let decoded = BASE64_STANDARD
        .decode(encoded)
        .map_err(|_error| ServerError::InvalidAuthorizationHeader)?;
    let decoded =
        std::str::from_utf8(&decoded).map_err(|_error| ServerError::InvalidAuthorizationHeader)?;
    let Some((_username, password)) = decoded.split_once(':') else {
        return Err(ServerError::InvalidAuthorizationHeader);
    };
    if password.trim().is_empty() {
        return Err(ServerError::InvalidAuthorizationHeader);
    }
    signer.verify_now(password).map_err(ServerError::from)
}

fn parse_oci_registry_token_scope(
    scope: Option<&str>,
) -> Result<(Option<TokenScope>, Option<String>), ServerError> {
    let Some(scope) = scope.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok((None, None));
    };
    let Some((resource_type, repository, actions)) =
        scope
            .split_once(':')
            .and_then(|(resource_type, remainder)| {
                remainder
                    .rsplit_once(':')
                    .map(|(repository, actions)| (resource_type, repository, actions))
            })
    else {
        return Err(ServerError::InvalidManifestReference);
    };
    if resource_type != "repository" {
        return Err(ServerError::InvalidManifestReference);
    }
    crate::oci_adapter::validate_repository(repository)?;
    let requested_scope = parse_oci_registry_actions(actions)?;
    Ok((Some(requested_scope), Some(repository.to_owned())))
}

fn parse_oci_registry_token_scopes(
    scopes: &[String],
) -> Result<(Option<TokenScope>, Option<String>), ServerError> {
    let mut requested_scope = None;
    let mut requested_repository: Option<String> = None;
    for scope in scopes {
        let (scope_value, repository) = parse_oci_registry_token_scope(Some(scope))?;
        let Some(scope_value) = scope_value else {
            continue;
        };
        let Some(repository) = repository else {
            continue;
        };
        if let Some(existing_repository) = requested_repository.as_deref() {
            if existing_repository != repository {
                return Err(ServerError::InvalidManifestReference);
            }
        } else {
            requested_repository = Some(repository);
        }
        requested_scope = Some(match requested_scope {
            Some(TokenScope::Write) => TokenScope::Write,
            Some(TokenScope::Read) if scope_value == TokenScope::Write => TokenScope::Write,
            Some(existing_scope) => existing_scope,
            None => scope_value,
        });
    }
    Ok((requested_scope, requested_repository))
}

fn parse_oci_registry_actions(actions: &str) -> Result<TokenScope, ServerError> {
    let mut saw_pull = false;
    let mut saw_push = false;
    for action in actions
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match action {
            "pull" => saw_pull = true,
            "push" => saw_push = true,
            _ => return Err(ServerError::InvalidManifestReference),
        }
    }
    if saw_push {
        return Ok(TokenScope::Write);
    }
    if saw_pull {
        return Ok(TokenScope::Read);
    }
    Err(ServerError::InvalidManifestReference)
}

struct OciRegistryTokenQuery {
    service: Option<String>,
    scopes: Vec<String>,
    _account: Option<String>,
}

fn parse_oci_registry_token_query(uri: &Uri) -> Result<OciRegistryTokenQuery, ServerError> {
    Ok(OciRegistryTokenQuery {
        service: single_bounded_query_value(uri, "service", MAX_OCI_TOKEN_QUERY_SERVICE_BYTES)?,
        scopes: bounded_query_values(
            uri,
            "scope",
            MAX_OCI_TOKEN_QUERY_SCOPE_BYTES,
            MAX_OCI_TOKEN_QUERY_SCOPES,
        )?,
        _account: single_bounded_query_value(uri, "account", MAX_OCI_TOKEN_QUERY_ACCOUNT_BYTES)?,
    })
}

fn single_bounded_query_value(
    uri: &Uri,
    key: &str,
    max_bytes: usize,
) -> Result<Option<String>, ServerError> {
    let values = parse_query_values(uri, key)?;
    if values.len() > 1 {
        return Err(ServerError::InvalidManifestReference);
    }
    let Some(value) = values.into_iter().next() else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > max_bytes {
        return Err(ServerError::RequestQueryTooLarge);
    }
    Ok(Some(value.to_owned()))
}

fn bounded_query_values(
    uri: &Uri,
    key: &str,
    max_bytes: usize,
    max_values: usize,
) -> Result<Vec<String>, ServerError> {
    let values = parse_query_values(uri, key)?;
    if values.len() > max_values {
        return Err(ServerError::InvalidManifestReference);
    }
    let mut bounded = Vec::with_capacity(values.len());
    for value in values {
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        if value.len() > max_bytes {
            return Err(ServerError::RequestQueryTooLarge);
        }
        bounded.push(value.to_owned());
    }
    Ok(bounded)
}

pub(super) fn scope_allows_oci_exchange(
    actual_scope: TokenScope,
    requested_scope: Option<TokenScope>,
) -> bool {
    match requested_scope.unwrap_or(actual_scope) {
        TokenScope::Read => actual_scope.allows_read(),
        TokenScope::Write => actual_scope.allows_write(),
    }
}

struct PromActiveRequestGuard;

impl Drop for PromActiveRequestGuard {
    fn drop(&mut self) {
        shardline_metrics::metrics().protocol.end_oci_registry_token_request();
    }
}
