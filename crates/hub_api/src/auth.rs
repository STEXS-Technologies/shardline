use std::sync::Arc;

use axum::http::{HeaderMap, header::AUTHORIZATION};
use shardline_protocol::TokenScope;
use shardline_server_core::{AuthContext, AuthError, AuthProvider};

use crate::error::HubApiError;

const MAX_BEARER_TOKEN_BYTES: usize = 8192;

/// Bearer-token verifier for Hub API routes.
#[derive(Clone)]
pub struct HubAuth {
    provider: Arc<dyn AuthProvider>,
}

impl HubAuth {
    /// Creates a new `HubAuth` from a boxed [`AuthProvider`].
    #[must_use]
    pub fn new(provider: Box<dyn AuthProvider>) -> Self {
        Self {
            provider: Arc::from(provider),
        }
    }

    /// Creates a new `HubAuth` from an [`Arc<dyn AuthProvider>`].
    #[must_use]
    pub fn from_arc(provider: Arc<dyn AuthProvider>) -> Self {
        Self { provider }
    }

    /// Validates the `Authorization` header and returns verified claims.
    ///
    /// # Errors
    ///
    /// Returns [`HubApiError::Unauthorized`] if the token is missing or invalid,
    /// or [`HubApiError::Forbidden`] if the scope is insufficient.
    pub fn authorize(
        &self,
        headers: &HeaderMap,
        required_scope: TokenScope,
    ) -> Result<AuthContext, HubApiError> {
        let header = headers
            .get(AUTHORIZATION)
            .ok_or(HubApiError::Unauthorized)?;
        let header = header.to_str().map_err(|e| {
            tracing::debug!("invalid authorization header encoding: {e}");
            HubApiError::InvalidToken
        })?;
        let token = parse_bearer_token(header)?;
        let claims = self.provider.verify_token(token)?;
        if !scope_allows(claims.scope(), required_scope) {
            return Err(HubApiError::Forbidden);
        }
        Ok(AuthContext::new(claims))
    }

    /// Returns a reference to the underlying provider.
    #[must_use]
    pub fn provider(&self) -> &dyn AuthProvider {
        self.provider.as_ref()
    }
}

impl std::fmt::Debug for HubAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HubAuth")
            .field("provider", &"<dyn AuthProvider>")
            .finish()
    }
}

const fn scope_allows(actual_scope: TokenScope, required_scope: TokenScope) -> bool {
    match required_scope {
        TokenScope::Read => actual_scope.allows_read(),
        TokenScope::Write => actual_scope.allows_write(),
    }
}

fn parse_bearer_token(header: &str) -> Result<&str, HubApiError> {
    let Some(token) = header.strip_prefix("Bearer ") else {
        return Err(HubApiError::InvalidToken);
    };
    if token.trim().is_empty() || token.len() > MAX_BEARER_TOKEN_BYTES {
        return Err(HubApiError::InvalidToken);
    }
    if token.bytes().any(|b| b.is_ascii_whitespace()) {
        return Err(HubApiError::InvalidToken);
    }
    Ok(token)
}

impl From<AuthError> for HubApiError {
    fn from(error: AuthError) -> Self {
        match error {
            AuthError::InvalidToken => Self::InvalidToken,
            AuthError::ExpiredToken => Self::InvalidToken,
            AuthError::InsufficientScope => Self::Forbidden,
            AuthError::ProviderError(msg) => Self::SigningKeyError(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use shardline_protocol::TokenClaims;

    // -----------------------------------------------------------------------
    // parse_bearer_token (private – exercised through authorize)
    // -----------------------------------------------------------------------

    fn make_auth_header(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_str(value).unwrap());
        headers
    }

    fn make_mock_provider() -> impl AuthProvider {
        struct MockProvider;
        impl AuthProvider for MockProvider {
            fn verify_token(&self, _token: &str) -> Result<TokenClaims, AuthError> {
                // Build a valid claim so we can reach the scope-check logic.
            let repo = shardline_protocol::RepositoryScope::new(
                shardline_protocol::RepositoryProvider::GitHub,
                "owner",
                "repo",
                Some("main"),
            )
            .map_err(|_err| AuthError::InvalidToken)?;
            TokenClaims::new("issuer", "subject", TokenScope::Read, repo, u64::MAX)
                .map_err(|_err| AuthError::InvalidToken)
            }
            fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
                Err(AuthError::ProviderError(
                    "mock provider does not mint tokens".to_owned(),
                ))
            }
        }
        MockProvider
    }

    #[test]
    fn parse_bearer_token_valid() {
        let headers = make_auth_header("Bearer validtoken123");
        let auth = HubAuth::new(Box::new(make_mock_provider()));
        let result = auth.authorize(&headers, TokenScope::Read);
        assert!(result.is_ok());
    }

    #[test]
    fn parse_bearer_token_empty_after_prefix() {
        let headers = make_auth_header("Bearer ");
        let auth = HubAuth::new(Box::new(make_mock_provider()));
        let result = auth.authorize(&headers, TokenScope::Read);
        assert!(matches!(result, Err(HubApiError::InvalidToken)));
    }

    #[test]
    fn parse_bearer_token_contains_whitespace() {
        let headers = make_auth_header("Bearer a b");
        let auth = HubAuth::new(Box::new(make_mock_provider()));
        let result = auth.authorize(&headers, TokenScope::Read);
        assert!(matches!(result, Err(HubApiError::InvalidToken)));
    }

    #[test]
    fn parse_bearer_token_not_bearer() {
        let headers = make_auth_header("Basic dXNlcjpwYXNz");
        let auth = HubAuth::new(Box::new(make_mock_provider()));
        let result = auth.authorize(&headers, TokenScope::Read);
        assert!(matches!(result, Err(HubApiError::InvalidToken)));
    }

    #[test]
    fn parse_bearer_token_too_long() {
        let long_token = "a".repeat(8193);
        let header_value = format!("Bearer {long_token}");
        let headers = make_auth_header(&header_value);
        let auth = HubAuth::new(Box::new(make_mock_provider()));
        let result = auth.authorize(&headers, TokenScope::Read);
        assert!(matches!(result, Err(HubApiError::InvalidToken)));
    }

    #[test]
    fn parse_bearer_token_no_authorization_header() {
        let headers = HeaderMap::new();
        let auth = HubAuth::new(Box::new(make_mock_provider()));
        let result = auth.authorize(&headers, TokenScope::Read);
        assert!(matches!(result, Err(HubApiError::Unauthorized)));
    }

    // -----------------------------------------------------------------------
    // scope_allows
    // -----------------------------------------------------------------------

    #[test]
    fn scope_allows_read_for_read() {
        assert!(scope_allows(TokenScope::Read, TokenScope::Read));
    }

    #[test]
    fn scope_allows_write_for_read() {
        assert!(scope_allows(TokenScope::Write, TokenScope::Read));
    }

    #[test]
    fn scope_allows_read_for_write_fails() {
        assert!(!scope_allows(TokenScope::Read, TokenScope::Write));
    }

    #[test]
    fn scope_allows_write_for_write() {
        assert!(scope_allows(TokenScope::Write, TokenScope::Write));
    }

    // -----------------------------------------------------------------------
    // From<AuthError> for HubApiError
    // -----------------------------------------------------------------------

    #[test]
    fn auth_error_invalid_token_maps_to_invalid_token() {
        let err: HubApiError = AuthError::InvalidToken.into();
        assert!(matches!(err, HubApiError::InvalidToken));
    }

    #[test]
    fn auth_error_expired_token_maps_to_invalid_token() {
        let err: HubApiError = AuthError::ExpiredToken.into();
        assert!(matches!(err, HubApiError::InvalidToken));
    }

    #[test]
    fn auth_error_insufficient_scope_maps_to_forbidden() {
        let err: HubApiError = AuthError::InsufficientScope.into();
        assert!(matches!(err, HubApiError::Forbidden));
    }

    #[test]
    fn auth_error_provider_error_maps_to_signing_key_error() {
        let err: HubApiError = AuthError::ProviderError("boom".into()).into();
        assert!(
            matches!(&err, HubApiError::SigningKeyError(msg) if msg == "boom"),
            "expected SigningKeyError(\"boom\"), got {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // authorize with provider errors
    // -----------------------------------------------------------------------

    #[test]
    fn authorize_expired_token_returns_invalid_token() {
        struct ExpiredProvider;
        impl AuthProvider for ExpiredProvider {
            fn verify_token(&self, _token: &str) -> Result<TokenClaims, AuthError> {
                Err(AuthError::ExpiredToken)
            }
            fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
                Err(AuthError::ProviderError("mock does not mint".to_owned()))
            }
        }
        let auth = HubAuth::new(Box::new(ExpiredProvider));
        let headers = make_auth_header("Bearer sometoken");
        let result = auth.authorize(&headers, TokenScope::Read);
        assert!(matches!(result, Err(HubApiError::InvalidToken)));
    }

    #[test]
    fn authorize_invalid_signature_returns_invalid_token() {
        struct BadSigProvider;
        impl AuthProvider for BadSigProvider {
            fn verify_token(&self, _token: &str) -> Result<TokenClaims, AuthError> {
                Err(AuthError::InvalidToken)
            }
            fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
                Err(AuthError::ProviderError("mock does not mint".to_owned()))
            }
        }
        let auth = HubAuth::new(Box::new(BadSigProvider));
        let headers = make_auth_header("Bearer badtoken");
        let result = auth.authorize(&headers, TokenScope::Read);
        assert!(matches!(result, Err(HubApiError::InvalidToken)));
    }

    #[test]
    fn authorize_insufficient_scope_returns_forbidden() {
        struct ReadOnlyProvider;
        impl AuthProvider for ReadOnlyProvider {
            fn verify_token(&self, _token: &str) -> Result<TokenClaims, AuthError> {
                let repo = shardline_protocol::RepositoryScope::new(
                    shardline_protocol::RepositoryProvider::GitHub,
                    "owner",
                    "repo",
                    Some("main"),
                )
                .map_err(|_err| AuthError::InvalidToken)?;
                TokenClaims::new("issuer", "subject", TokenScope::Read, repo, u64::MAX)
                    .map_err(|_err| AuthError::InvalidToken)
            }
            fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
                Err(AuthError::ProviderError("mock does not mint".to_owned()))
            }
        }
        let auth = HubAuth::new(Box::new(ReadOnlyProvider));
        let headers = make_auth_header("Bearer token");
        // Read-only token trying to write → Forbidden
        let result = auth.authorize(&headers, TokenScope::Write);
        assert!(matches!(result, Err(HubApiError::Forbidden)));
    }

    #[test]
    fn authorize_provider_error_returns_signing_key_error() {
        struct ErrorProvider;
        #[allow(clippy::unreachable)]
        impl AuthProvider for ErrorProvider {
            fn verify_token(&self, _token: &str) -> Result<TokenClaims, AuthError> {
                Err(AuthError::ProviderError("downstream is down".to_owned()))
            }
            fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
                unreachable!()
            }
        }
        let auth = HubAuth::new(Box::new(ErrorProvider));
        let headers = make_auth_header("Bearer token");
        let result = auth.authorize(&headers, TokenScope::Read);
        assert!(
            matches!(&result, Err(HubApiError::SigningKeyError(msg)) if msg == "downstream is down")
        );
    }

    // -----------------------------------------------------------------------
    // authorize with no auth configured (permissive path in routes.rs but
    // HubAuth::authorize always requires auth, so this just tests the
    // error path when headers are missing)
    // -----------------------------------------------------------------------

    #[test]
    fn authorize_missing_header_returns_unauthorized() {
        let auth = HubAuth::new(Box::new(make_mock_provider()));
        let headers = HeaderMap::new();
        let result = auth.authorize(&headers, TokenScope::Read);
        assert!(matches!(result, Err(HubApiError::Unauthorized)));
    }
}
