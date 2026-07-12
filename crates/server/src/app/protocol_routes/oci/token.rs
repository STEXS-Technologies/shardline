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

use super::super::{AppState, authorize, parse_query_values};

pub(super) const OCI_REGISTRY_SERVICE: &str = "shardline";
const MAX_OCI_TOKEN_BASIC_AUTH_BYTES: usize = 8192;
const MAX_OCI_TOKEN_QUERY_SERVICE_BYTES: usize = 128;
const MAX_OCI_TOKEN_QUERY_SCOPE_BYTES: usize = 1024;
const MAX_OCI_TOKEN_QUERY_ACCOUNT_BYTES: usize = 512;
const MAX_OCI_TOKEN_QUERY_SCOPES: usize = 16;
const MIN_OCI_TOKEN_EXPIRES_IN_SECONDS: u64 = 60;

pub(crate) async fn oci_registry_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Json<crate::model::OciRegistryTokenResponse>, ServerError> {
    state
        .protocol_metrics
        .increment_oci_registry_token_requests();
    shardline_metrics::metrics()
        .protocol
        .record_oci_registry_token_request();
    let _permit = state
        .oci_registry_token_limiter
        .clone()
        .try_acquire_owned()
        .map_err(|_error| {
            state
                .protocol_metrics
                .increment_oci_registry_token_rate_limited();
            shardline_metrics::metrics()
                .protocol
                .record_oci_registry_token_rate_limited();
            ServerError::TooManyRegistryTokenRequests
        })?;
    let _active_request = state.protocol_metrics.begin_oci_registry_token_request();
    shardline_metrics::metrics()
        .protocol
        .begin_oci_registry_token_request();
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
        crate::protocol_support::validate_oci_repository_scope(
            repository,
            Some(bootstrap_claims.repository()),
        )?;
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
            .max(MIN_OCI_TOKEN_EXPIRES_IN_SECONDS)
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
        shardline_metrics::metrics()
            .protocol
            .end_oci_registry_token_request();
    }
}

#[cfg(test)]
mod tests {
    use shardline_protocol::TokenScope;

    use super::{
        MIN_OCI_TOKEN_EXPIRES_IN_SECONDS, OCI_REGISTRY_SERVICE, oci_bearer_challenge,
        parse_oci_registry_actions, parse_oci_registry_token_query, parse_oci_registry_token_scope,
        parse_oci_registry_token_scopes, scope_allows_oci_exchange,
    };

    // ── parse_oci_registry_token_scope ──────────────────────────────────────

    #[test]
    fn parse_scope_none_returns_none() {
        assert_eq!(parse_oci_registry_token_scope(None).unwrap(), (None, None));
    }

    #[test]
    fn parse_scope_empty_string_returns_none() {
        assert_eq!(
            parse_oci_registry_token_scope(Some("")).unwrap(),
            (None, None)
        );
    }

    #[test]
    fn parse_scope_pull() {
        assert_eq!(
            parse_oci_registry_token_scope(Some("repository:team/assets:pull")).unwrap(),
            (Some(TokenScope::Read), Some("team/assets".to_owned()))
        );
    }

    #[test]
    fn parse_scope_push() {
        assert_eq!(
            parse_oci_registry_token_scope(Some("repository:team/assets:push")).unwrap(),
            (Some(TokenScope::Write), Some("team/assets".to_owned()))
        );
    }

    #[test]
    fn parse_scope_pull_and_push() {
        assert_eq!(
            parse_oci_registry_token_scope(Some("repository:team/assets:pull,push")).unwrap(),
            (Some(TokenScope::Write), Some("team/assets".to_owned()))
        );
    }

    #[test]
    fn parse_scope_non_repository_resource_type_errors() {
        assert!(matches!(
            parse_oci_registry_token_scope(Some("notrepository:team/assets:pull")),
            Err(crate::ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn parse_scope_missing_actions_errors() {
        assert!(matches!(
            parse_oci_registry_token_scope(Some("repository:team/assets")),
            Err(crate::ServerError::InvalidManifestReference)
        ));
    }

    // ── parse_oci_registry_actions ──────────────────────────────────────────

    #[test]
    fn parse_actions_pull() {
        assert_eq!(
            parse_oci_registry_actions("pull").unwrap(),
            TokenScope::Read
        );
    }

    #[test]
    fn parse_actions_push() {
        assert_eq!(
            parse_oci_registry_actions("push").unwrap(),
            TokenScope::Write
        );
    }

    #[test]
    fn parse_actions_pull_push() {
        assert_eq!(
            parse_oci_registry_actions("pull,push").unwrap(),
            TokenScope::Write
        );
    }

    #[test]
    fn parse_actions_push_pull() {
        assert_eq!(
            parse_oci_registry_actions("push,pull").unwrap(),
            TokenScope::Write
        );
    }

    #[test]
    fn parse_actions_empty_errors() {
        assert!(matches!(
            parse_oci_registry_actions(""),
            Err(crate::ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn parse_actions_invalid_errors() {
        assert!(matches!(
            parse_oci_registry_actions("invalid"),
            Err(crate::ServerError::InvalidManifestReference)
        ));
    }

    // ── parse_oci_registry_token_scopes ─────────────────────────────────────

    #[test]
    fn parse_scopes_empty_vec() {
        assert_eq!(parse_oci_registry_token_scopes(&[]).unwrap(), (None, None));
    }

    #[test]
    fn parse_scopes_one_pull() {
        let scopes = vec!["repository:repo:pull".to_owned()];
        assert_eq!(
            parse_oci_registry_token_scopes(&scopes).unwrap(),
            (Some(TokenScope::Read), Some("repo".to_owned()))
        );
    }

    #[test]
    fn parse_scopes_one_push() {
        let scopes = vec!["repository:repo:push".to_owned()];
        assert_eq!(
            parse_oci_registry_token_scopes(&scopes).unwrap(),
            (Some(TokenScope::Write), Some("repo".to_owned()))
        );
    }

    #[test]
    fn parse_scopes_two_same_repo_pull_and_push() {
        let scopes = vec![
            "repository:repo:pull".to_owned(),
            "repository:repo:push".to_owned(),
        ];
        let (scope, repo) = parse_oci_registry_token_scopes(&scopes).unwrap();
        assert_eq!(scope, Some(TokenScope::Write));
        assert_eq!(repo, Some("repo".to_owned()));
    }

    #[test]
    fn parse_scopes_two_different_repos_errors() {
        let scopes = vec![
            "repository:repo/a:pull".to_owned(),
            "repository:repo/b:pull".to_owned(),
        ];
        assert!(matches!(
            parse_oci_registry_token_scopes(&scopes),
            Err(crate::ServerError::InvalidManifestReference)
        ));
    }

    // ── parse_oci_registry_token_query ──────────────────────────────────────

    fn uri(path: &str) -> axum::http::Uri {
        path.parse().unwrap()
    }

    #[test]
    fn parse_query_no_params() {
        let query = parse_oci_registry_token_query(&uri("/v2/token")).unwrap();
        assert!(query.service.is_none());
        assert!(query.scopes.is_empty());
        assert!(query._account.is_none());
    }

    #[test]
    fn parse_query_with_service() {
        let query = parse_oci_registry_token_query(&uri("/v2/token?service=shardline")).unwrap();
        assert_eq!(query.service.as_deref(), Some("shardline"));
    }

    #[test]
    fn parse_query_with_scope() {
        let query =
            parse_oci_registry_token_query(&uri("/v2/token?scope=repository:repo:pull")).unwrap();
        assert_eq!(query.scopes.len(), 1);
        assert_eq!(query.scopes[0], "repository:repo:pull");
    }

    #[test]
    fn parse_query_with_multiple_scopes() {
        let query = parse_oci_registry_token_query(&uri("/v2/token?scope=a&scope=b")).unwrap();
        assert_eq!(query.scopes.len(), 2);
    }

    // ── scope_allows_oci_exchange ───────────────────────────────────────────

    #[test]
    fn exchange_read_scope_allows_read() {
        assert!(scope_allows_oci_exchange(
            TokenScope::Read,
            Some(TokenScope::Read)
        ));
    }

    #[test]
    fn exchange_read_scope_denies_write() {
        assert!(!scope_allows_oci_exchange(
            TokenScope::Read,
            Some(TokenScope::Write)
        ));
    }

    #[test]
    fn exchange_write_scope_allows_read() {
        assert!(scope_allows_oci_exchange(
            TokenScope::Write,
            Some(TokenScope::Read)
        ));
    }

    #[test]
    fn exchange_write_scope_allows_write() {
        assert!(scope_allows_oci_exchange(
            TokenScope::Write,
            Some(TokenScope::Write)
        ));
    }

    #[test]
    fn exchange_none_uses_actual_scope() {
        // When requested_scope is None, the function falls back to actual_scope.
        // Read actual → requested is Read → Read.allows_read() = true
        assert!(scope_allows_oci_exchange(TokenScope::Read, None));
        // Write actual → requested is Write → Write.allows_write() = true
        assert!(scope_allows_oci_exchange(TokenScope::Write, None));
    }

    // ── oci_bearer_challenge ────────────────────────────────────────────────

    #[test]
    fn challenge_with_repository_read() {
        let challenge = oci_bearer_challenge("https://example.com", Some("repo"), TokenScope::Read);
        assert!(challenge.contains("realm=\"https://example.com/v2/token\""));
        assert!(challenge.contains(&format!("service=\"{OCI_REGISTRY_SERVICE}\"")));
        assert!(challenge.contains("scope=\"repository:repo:pull\""));
    }

    #[test]
    fn challenge_with_repository_write() {
        let challenge =
            oci_bearer_challenge("https://example.com", Some("repo"), TokenScope::Write);
        assert!(challenge.contains("scope=\"repository:repo:pull,push\""));
    }

    #[test]
    fn challenge_without_repository() {
        let challenge = oci_bearer_challenge("https://example.com", None, TokenScope::Read);
        assert!(challenge.contains("realm=\"https://example.com/v2/token\""));
        assert!(challenge.contains(&format!("service=\"{OCI_REGISTRY_SERVICE}\"")));
        assert!(!challenge.contains("scope="));
    }

    #[test]
    fn challenge_strips_trailing_slash() {
        let challenge =
            oci_bearer_challenge("https://example.com/", Some("repo"), TokenScope::Read);
        assert!(challenge.contains("realm=\"https://example.com/v2/token\""));
    }

    // ── expires_in clamping ─────────────────────────────────────────────────

    #[test]
    fn expires_in_clamps_to_minimum() {
        let now = 1000_u64;
        // Bootstrap token is near-expiry (only 5 seconds remain). The clamp
        // must raise this to at least MIN_OCI_TOKEN_EXPIRES_IN_SECONDS so the
        // client has time to use the issued token.
        assert_eq!(
            (now + 5)
                .saturating_sub(now)
                .max(MIN_OCI_TOKEN_EXPIRES_IN_SECONDS)
                .min(i32::MAX as u64),
            MIN_OCI_TOKEN_EXPIRES_IN_SECONDS,
        );

        // Bootstrap token is already expired (sub gives 0, max raises to 60).
        assert_eq!(
            now.saturating_sub(10)
                .saturating_sub(now)
                .max(MIN_OCI_TOKEN_EXPIRES_IN_SECONDS)
                .min(i32::MAX as u64),
            MIN_OCI_TOKEN_EXPIRES_IN_SECONDS,
        );

        // Ample remaining lifetime is unclamped.
        assert_eq!(
            (now + 3600)
                .saturating_sub(now)
                .max(MIN_OCI_TOKEN_EXPIRES_IN_SECONDS)
                .min(i32::MAX as u64),
            3600,
        );
    }
}
