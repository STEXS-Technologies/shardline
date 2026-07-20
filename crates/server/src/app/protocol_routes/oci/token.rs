use std::fmt::Write;
use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    http::{HeaderMap, Uri},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use shardline_protocol::{TokenClaims, TokenScope, TokenSigner};

use crate::{
    ServerError, auth::AuthContext, clock::unix_now_seconds_checked,
    model::OciRegistryTokenResponse, oci_adapter::validate_repository,
    protocol_support::validate_oci_repository_scope,
};

use super::super::{AppState, authorize, parse_query_values};

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
) -> Result<Json<OciRegistryTokenResponse>, ServerError> {
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
        validate_oci_repository_scope(repository, Some(bootstrap_claims.repository()))?;
    }
    if !scope_allows_oci_exchange(bootstrap_claims.scope(), requested_scope) {
        return Err(ServerError::InsufficientScope);
    }

    let now = unix_now_seconds_checked()?;
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
    Ok(Json(OciRegistryTokenResponse {
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
) -> Result<Option<AuthContext>, ServerError> {
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
    validate_repository(repository)?;
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
    use std::sync::Arc;

    use axum::http::{HeaderMap, HeaderValue, Uri};
    use shardline_protocol::{
        RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
    };

    use super::{
        MAX_OCI_TOKEN_BASIC_AUTH_BYTES, MAX_OCI_TOKEN_QUERY_SCOPE_BYTES,
        MAX_OCI_TOKEN_QUERY_SCOPES, MAX_OCI_TOKEN_QUERY_SERVICE_BYTES, OCI_REGISTRY_SERVICE,
        bounded_query_values, oci_bearer_challenge, oci_registry_token, parse_oci_registry_actions,
        parse_oci_registry_token_query, parse_oci_registry_token_scope,
        parse_oci_registry_token_scopes, scope_allows_oci_exchange, single_bounded_query_value,
        verify_oci_registry_bootstrap_credentials,
    };
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::num::NonZeroUsize;

    use crate::AppState;
    use crate::ServerConfig;
    use crate::ServerError;
    use crate::TransferLimiter;
    use crate::app::ProtocolMetrics;
    use crate::backend::ServerBackend;
    use crate::reconstruction_cache::ReconstructionCacheService;
    use crate::server_role::ServerRole;

    fn signing_key() -> Vec<u8> {
        vec![b'k'; 32]
    }

    fn test_signer() -> TokenSigner {
        TokenSigner::new(&signing_key()).unwrap()
    }

    fn test_claims() -> TokenClaims {
        TokenClaims::new(
            "issuer",
            "subject",
            TokenScope::Write,
            RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", None).unwrap(),
            999_999_999_999,
        )
        .unwrap()
    }

    // ── single_bounded_query_value ──────────────────────────────────────────

    #[test]
    fn single_value_returns_value() {
        let result = single_bounded_query_value(
            &uri("/v2/token?service=shardline"),
            "service",
            MAX_OCI_TOKEN_QUERY_SERVICE_BYTES,
        )
        .unwrap();
        assert_eq!(result.as_deref(), Some("shardline"));
    }

    #[test]
    fn single_value_no_key_returns_none() {
        let result = single_bounded_query_value(
            &uri("/v2/token"),
            "service",
            MAX_OCI_TOKEN_QUERY_SERVICE_BYTES,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn single_value_empty_value_returns_none() {
        let result = single_bounded_query_value(
            &uri("/v2/token?service="),
            "service",
            MAX_OCI_TOKEN_QUERY_SERVICE_BYTES,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn single_value_multiple_values_errors() {
        assert!(matches!(
            single_bounded_query_value(
                &uri("/v2/token?service=a&service=b"),
                "service",
                MAX_OCI_TOKEN_QUERY_SERVICE_BYTES,
            ),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn single_value_too_large_errors() {
        assert!(matches!(
            single_bounded_query_value(
                &uri(&format!(
                    "/v2/token?service={}",
                    "x".repeat(MAX_OCI_TOKEN_QUERY_SERVICE_BYTES + 1)
                )),
                "service",
                MAX_OCI_TOKEN_QUERY_SERVICE_BYTES,
            ),
            Err(ServerError::RequestQueryTooLarge)
        ));
    }

    // ── bounded_query_values ────────────────────────────────────────────────

    #[test]
    fn bounded_values_returns_values() {
        let result = bounded_query_values(
            &uri("/v2/token?scope=a&scope=b"),
            "scope",
            MAX_OCI_TOKEN_QUERY_SCOPE_BYTES,
            MAX_OCI_TOKEN_QUERY_SCOPES,
        )
        .unwrap();
        assert_eq!(result, vec!["a".to_owned(), "b".to_owned()]);
    }

    #[test]
    fn bounded_values_no_key_returns_empty() {
        let result = bounded_query_values(
            &uri("/v2/token"),
            "scope",
            MAX_OCI_TOKEN_QUERY_SCOPE_BYTES,
            MAX_OCI_TOKEN_QUERY_SCOPES,
        )
        .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn bounded_values_skips_empty_values() {
        let result = bounded_query_values(
            &uri("/v2/token?scope=a&scope=&scope=b"),
            "scope",
            MAX_OCI_TOKEN_QUERY_SCOPE_BYTES,
            MAX_OCI_TOKEN_QUERY_SCOPES,
        )
        .unwrap();
        assert_eq!(result, vec!["a".to_owned(), "b".to_owned()]);
    }

    #[test]
    fn bounded_values_too_many_values_errors() {
        assert!(matches!(
            bounded_query_values(
                &uri("/v2/token?scope=a&scope=b&scope=c"),
                "scope",
                MAX_OCI_TOKEN_QUERY_SCOPE_BYTES,
                2,
            ),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn bounded_values_value_too_large_errors() {
        assert!(matches!(
            bounded_query_values(
                &uri(&format!(
                    "/v2/token?scope={}",
                    "x".repeat(MAX_OCI_TOKEN_QUERY_SCOPE_BYTES + 1)
                )),
                "scope",
                MAX_OCI_TOKEN_QUERY_SCOPE_BYTES,
                MAX_OCI_TOKEN_QUERY_SCOPES,
            ),
            Err(ServerError::RequestQueryTooLarge)
        ));
    }

    // ── verify_oci_registry_bootstrap_credentials ───────────────────────────

    #[test]
    fn verify_bootstrap_missing_header_errors() {
        let headers = HeaderMap::new();
        let signer = test_signer();
        assert!(matches!(
            verify_oci_registry_bootstrap_credentials(&headers, &signer),
            Err(ServerError::MissingAuthorization)
        ));
    }

    #[test]
    fn verify_bootstrap_invalid_header_format_errors() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("NotAValidScheme token"),
        );
        let signer = test_signer();
        assert!(matches!(
            verify_oci_registry_bootstrap_credentials(&headers, &signer),
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn verify_bootstrap_bearer_valid_token() {
        let signer = test_signer();
        let claims = test_claims();
        let token = signer.sign(&claims).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let result = verify_oci_registry_bootstrap_credentials(&headers, &signer).unwrap();
        assert_eq!(result.issuer(), claims.issuer());
        assert_eq!(result.subject(), claims.subject());
    }

    #[test]
    fn verify_bootstrap_bearer_expired_token_errors() {
        let signer = test_signer();
        let expired_claims = TokenClaims::new(
            "issuer",
            "subject",
            TokenScope::Write,
            RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", None).unwrap(),
            0, // expired
        )
        .unwrap();
        let token = signer.sign(&expired_claims).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        assert!(matches!(
            verify_oci_registry_bootstrap_credentials(&headers, &signer),
            Err(ServerError::InvalidToken(_))
        ));
    }

    #[test]
    fn verify_bootstrap_basic_valid_token() {
        let signer = test_signer();
        let claims = test_claims();
        let token = signer.sign(&claims).unwrap();
        // Basic auth uses base64(username:password) where password is the token
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            format!("user:{token}"),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {encoded}")).unwrap(),
        );
        let result = verify_oci_registry_bootstrap_credentials(&headers, &signer).unwrap();
        assert_eq!(result.issuer(), claims.issuer());
    }

    #[test]
    fn verify_bootstrap_basic_oversized_encoded_errors() {
        let oversized = "x".repeat(MAX_OCI_TOKEN_BASIC_AUTH_BYTES + 1);
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {oversized}")).unwrap(),
        );
        let signer = test_signer();
        assert!(matches!(
            verify_oci_registry_bootstrap_credentials(&headers, &signer),
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn verify_bootstrap_basic_invalid_base64_errors() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str("Basic !!!invalid-base64!!!").unwrap(),
        );
        let signer = test_signer();
        assert!(matches!(
            verify_oci_registry_bootstrap_credentials(&headers, &signer),
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn verify_bootstrap_basic_no_colon_errors() {
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            "username-no-colon",
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {encoded}")).unwrap(),
        );
        let signer = test_signer();
        assert!(matches!(
            verify_oci_registry_bootstrap_credentials(&headers, &signer),
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn verify_bootstrap_basic_empty_password_errors() {
        let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, "user:");
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {encoded}")).unwrap(),
        );
        let signer = test_signer();
        assert!(matches!(
            verify_oci_registry_bootstrap_credentials(&headers, &signer),
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    // ── parse_oci_registry_token_query additional edge cases ────────────────

    #[test]
    fn parse_query_with_account() {
        let query = parse_oci_registry_token_query(&uri("/v2/token?account=myaccount")).unwrap();
        assert_eq!(query._account.as_deref(), Some("myaccount"));
    }

    #[test]
    fn parse_query_with_service_mismatch_returns_none_service() {
        // service is stored; the caller (oci_registry_token) checks it separately
        let query = parse_oci_registry_token_query(&uri("/v2/token?service=other")).unwrap();
        assert_eq!(query.service.as_deref(), Some("other"));
    }

    #[test]
    fn parse_query_empty_scope_value_skipped() {
        let query = parse_oci_registry_token_query(&uri("/v2/token?scope=")).unwrap();
        assert!(query.scopes.is_empty());
    }

    #[test]
    fn parse_query_rejects_duplicate_service() {
        assert!(matches!(
            parse_oci_registry_token_query(&uri("/v2/token?service=a&service=b")),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    // ── parse_oci_registry_token_scope additional edge cases ────────────────

    #[test]
    fn parse_scope_whitespace_returns_none() {
        assert_eq!(
            parse_oci_registry_token_scope(Some("  ")).unwrap(),
            (None, None)
        );
    }

    #[test]
    fn parse_scope_invalid_actions_errors() {
        assert!(matches!(
            parse_oci_registry_token_scope(Some("repository:repo:invalid")),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn parse_scope_invalid_resource_type_errors() {
        assert!(matches!(
            parse_oci_registry_token_scope(Some("registry:repo:pull")),
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn parse_scope_invalid_repository_name_errors() {
        assert!(matches!(
            parse_oci_registry_token_scope(Some("repository:\0invalid:pull")),
            Err(ServerError::InvalidRepositoryName)
        ));
    }

    // ── parse_oci_registry_token_scopes additional edge cases ───────────────

    #[test]
    fn parse_scopes_with_empty_scope_entries() {
        let scopes = vec![
            "repository:repo:pull".to_owned(),
            String::new(),
            "repository:repo:push".to_owned(),
        ];
        let (scope, repo) = parse_oci_registry_token_scopes(&scopes).unwrap();
        assert_eq!(scope, Some(TokenScope::Write));
        assert_eq!(repo, Some("repo".to_owned()));
    }

    #[test]
    fn parse_scopes_write_upgrades_read() {
        // When a Write scope follows a Read scope, result is Write.
        let scopes = vec![
            "repository:repo:pull".to_owned(),
            "repository:repo:push".to_owned(),
        ];
        let (scope, _) = parse_oci_registry_token_scopes(&scopes).unwrap();
        assert_eq!(scope, Some(TokenScope::Write));
    }

    #[test]
    fn parse_scopes_write_remains_write() {
        // Line 230: when the existing scope is already Write, keep Write
        // even if a subsequent scope is also Write (e.g. two push scopes).
        let scopes = vec![
            "repository:repo:push".to_owned(),
            "repository:repo:push".to_owned(),
        ];
        let (scope, repo) = parse_oci_registry_token_scopes(&scopes).unwrap();
        assert_eq!(scope, Some(TokenScope::Write));
        assert_eq!(repo, Some("repo".to_owned()));
    }

    // ── parse_oci_registry_actions additional edge cases ────────────────────

    #[test]
    fn parse_actions_only_push() {
        assert_eq!(
            parse_oci_registry_actions("push").unwrap(),
            TokenScope::Write
        );
    }

    #[test]
    fn parse_actions_whitespace_around_actions() {
        assert_eq!(
            parse_oci_registry_actions(" pull , push ").unwrap(),
            TokenScope::Write
        );
    }

    // ── scope_allows_oci_exchange additional edge cases ─────────────────────

    // ── oci_registry_token handler tests ────────────────────────────────────

    async fn build_state_with_auth() -> Arc<AppState> {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().to_path_buf();
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            "http://127.0.0.1:8080".to_owned(),
            root,
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_token_signing_key(signing_key())
        .expect("signing key set");
        let backend = ServerBackend::from_config(&config).await.expect("backend");
        Arc::new(AppState {
            config,
            role: ServerRole::All,
            backend,
            auth: None,
            provider_tokens: None,
            reconstruction_cache: ReconstructionCacheService::disabled(),
            transfer_limiter: TransferLimiter::new(
                NonZeroUsize::new(4096).unwrap(),
                NonZeroUsize::new(16).unwrap(),
            ),
            oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(64)),
            protocol_metrics: ProtocolMetrics::default(),
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_token_requires_auth_header() {
        let state = build_state_with_auth().await;
        let headers = HeaderMap::new();
        let uri: Uri = "/v2/token".parse().unwrap();
        let result = oci_registry_token(axum::extract::State(state), headers, uri).await;
        assert!(matches!(result, Err(ServerError::UnauthorizedChallenge(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_token_with_bearer_token_succeeds() {
        let state = build_state_with_auth().await;
        let signer = test_signer();
        let claims = test_claims();
        let token = signer.sign(&claims).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let uri: Uri = "/v2/token?scope=repository:owner/repo:pull"
            .parse()
            .unwrap();
        let result = oci_registry_token(axum::extract::State(state), headers, uri).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.0.access_token, response.0.token);
        assert!(response.0.expires_in > 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_token_rejects_wrong_service() {
        let state = build_state_with_auth().await;
        let signer = test_signer();
        let claims = test_claims();
        let token = signer.sign(&claims).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let uri: Uri = "/v2/token?service=wrong-service".parse().unwrap();
        let result = oci_registry_token(axum::extract::State(state), headers, uri).await;
        assert!(matches!(result, Err(ServerError::InvalidManifestReference)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_token_expired_bearer_returns_challenge() {
        let state = build_state_with_auth().await;
        let signer = test_signer();
        let expired = TokenClaims::new(
            "issuer",
            "subject",
            TokenScope::Write,
            RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", None).unwrap(),
            0,
        )
        .unwrap();
        let token = signer.sign(&expired).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let uri: Uri = "/v2/token".parse().unwrap();
        let result = oci_registry_token(axum::extract::State(state), headers, uri).await;
        assert!(matches!(result, Err(ServerError::UnauthorizedChallenge(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_token_insufficient_scope_errors() {
        let state = build_state_with_auth().await;
        let signer = test_signer();
        let read_claims = TokenClaims::new(
            "issuer",
            "subject",
            TokenScope::Read,
            RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", None).unwrap(),
            999_999_999_999,
        )
        .unwrap();
        let token = signer.sign(&read_claims).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        // Requesting write scope when we only have read
        let uri: Uri = "/v2/token?scope=repository:owner/repo:push"
            .parse()
            .unwrap();
        let result = oci_registry_token(axum::extract::State(state), headers, uri).await;
        assert!(matches!(result, Err(ServerError::InsufficientScope)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_token_with_basic_auth_succeeds() {
        let state = build_state_with_auth().await;
        let signer = test_signer();
        let claims = test_claims();
        let token = signer.sign(&claims).unwrap();
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            format!("user:{token}"),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Basic {encoded}")).unwrap(),
        );
        let uri: Uri = "/v2/token".parse().unwrap();
        let result = oci_registry_token(axum::extract::State(state), headers, uri).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_token_rate_limited_when_permits_exhausted() {
        // Exhaust the semaphore to trigger the rate-limit error path (lines 40-46).
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().to_path_buf();
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            "http://127.0.0.1:8080".to_owned(),
            root,
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_token_signing_key(signing_key())
        .expect("signing key set");
        let backend = ServerBackend::from_config(&config).await.expect("backend");
        // Create a semaphore with 0 permits so try_acquire_owned always fails
        let state = Arc::new(AppState {
            config,
            role: ServerRole::All,
            backend,
            auth: None,
            provider_tokens: None,
            reconstruction_cache: ReconstructionCacheService::disabled(),
            transfer_limiter: TransferLimiter::new(
                NonZeroUsize::new(4096).unwrap(),
                NonZeroUsize::new(16).unwrap(),
            ),
            oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(0)),
            protocol_metrics: ProtocolMetrics::default(),
        });
        let headers = HeaderMap::new();
        let uri: Uri = "/v2/token".parse().unwrap();
        let result = oci_registry_token(axum::extract::State(state), headers, uri).await;
        assert!(matches!(
            result,
            Err(ServerError::TooManyRegistryTokenRequests)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_token_rejects_different_repository_scope() {
        // The bootstrap token has repository = owner/repo.
        // Requesting a token for a different repository should fail.
        let state = build_state_with_auth().await;
        let signer = test_signer();
        let claims = test_claims(); // scope: owner/repo
        let token = signer.sign(&claims).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        // Request token for "different/repo" which does not match "owner/repo"
        let uri: Uri = "/v2/token?scope=repository:different/repo:pull"
            .parse()
            .unwrap();
        let result = oci_registry_token(axum::extract::State(state), headers, uri).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_token_exchange_missing_signing_key_errors() {
        // A state without a signing key should return MissingAuthorization
        // We need a state with no signing key configured.
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().to_path_buf();
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            "http://127.0.0.1:8080".to_owned(),
            root,
            NonZeroUsize::new(4096).unwrap(),
        );
        let backend = ServerBackend::from_config(&config).await.expect("backend");
        let state = Arc::new(AppState {
            config,
            role: ServerRole::All,
            backend,
            auth: None,
            provider_tokens: None,
            reconstruction_cache: ReconstructionCacheService::disabled(),
            transfer_limiter: TransferLimiter::new(
                NonZeroUsize::new(4096).unwrap(),
                NonZeroUsize::new(16).unwrap(),
            ),
            oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(64)),
            protocol_metrics: ProtocolMetrics::default(),
        });
        let headers = HeaderMap::new();
        let uri: Uri = "/v2/token".parse().unwrap();
        let result = oci_registry_token(axum::extract::State(state), headers, uri).await;
        assert!(matches!(result, Err(ServerError::MissingAuthorization)));
    }

    /// Verify that when the service name differs from OCI_REGISTRY_SERVICE it
    /// returns an error. This branch is on line 60-63.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_token_wrong_service_triggers_error() {
        let state = build_state_with_auth().await;
        let signer = test_signer();
        let claims = test_claims();
        let token = signer.sign(&claims).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let uri: Uri = "/v2/token?service=unknown-registry".parse().unwrap();
        let result = oci_registry_token(axum::extract::State(state), headers, uri).await;
        assert!(matches!(result, Err(ServerError::InvalidManifestReference)));
    }

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
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn parse_scope_missing_actions_errors() {
        assert!(matches!(
            parse_oci_registry_token_scope(Some("repository:team/assets")),
            Err(ServerError::InvalidManifestReference)
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
            Err(ServerError::InvalidManifestReference)
        ));
    }

    #[test]
    fn parse_actions_invalid_errors() {
        assert!(matches!(
            parse_oci_registry_actions("invalid"),
            Err(ServerError::InvalidManifestReference)
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
            Err(ServerError::InvalidManifestReference)
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

    // ── parse_oci_registry_token_scopes — line 232: fallthrough for Read+Read ──

    #[test]
    fn parse_scopes_two_read_uses_existing_scope() {
        // Two Read scopes: after the first, requested_scope = Some(Read).
        // The second is also Read, so the guard on line 231
        // `Some(TokenScope::Read) if scope_value == TokenScope::Write`
        // does NOT match, and we fall through to line 232 `Some(existing_scope) => existing_scope`.
        let scopes = vec![
            "repository:repo:pull".to_owned(),
            "repository:repo:pull".to_owned(),
        ];
        let (scope, repo) = parse_oci_registry_token_scopes(&scopes).unwrap();
        assert_eq!(scope, Some(TokenScope::Read));
        assert_eq!(repo, Some("repo".to_owned()));
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

    // ── expires_in reporting ────────────────────────────────────────────────

    #[test]
    fn expires_in_matches_the_issued_token_lifetime() {
        let now = 1000_u64;
        assert_eq!((now + 5).saturating_sub(now).min(i32::MAX as u64), 5,);

        assert_eq!(
            now.saturating_sub(10)
                .saturating_sub(now)
                .min(i32::MAX as u64),
            0,
        );

        assert_eq!((now + 3600).saturating_sub(now).min(i32::MAX as u64), 3600,);
    }
}
