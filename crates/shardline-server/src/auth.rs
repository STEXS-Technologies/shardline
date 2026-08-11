use std::fmt;
use std::sync::Arc;

use axum::http::{HeaderMap, header::AUTHORIZATION};
use shardline_protocol::{MAX_TOKEN_STRING_BYTES, TokenClaims, TokenCodecError, TokenScope};
use shardline_server_core::{AuthError, AuthProvider};
use subtle::ConstantTimeEq;

use crate::ServerError;

/// Verified request authorization context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthContext {
    claims: TokenClaims,
}

impl AuthContext {
    /// Creates an authorization context from verified token claims.
    #[must_use]
    pub const fn new(claims: TokenClaims) -> Self {
        Self { claims }
    }

    /// Returns the verified claims.
    #[must_use]
    pub const fn claims(&self) -> &TokenClaims {
        &self.claims
    }
}

/// Bearer-token verifier backed by a pluggable [`AuthProvider`].
#[derive(Clone)]
pub struct ServerAuth {
    provider: Arc<dyn AuthProvider>,
}

impl ServerAuth {
    /// Creates a bearer-token verifier from a signing key using the local
    /// HMAC-SHA256 provider.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the signing key is invalid.
    pub fn new(signing_key: &[u8]) -> Result<Self, ServerError> {
        let provider = shardline_server_core::auth::LocalHmacProvider::new(signing_key)?;
        Ok(Self {
            provider: Arc::new(provider),
        })
    }

    /// Creates a bearer-token verifier from a boxed [`AuthProvider`].
    #[must_use]
    pub fn from_provider(provider: Box<dyn AuthProvider>) -> Self {
        Self {
            provider: Arc::from(provider),
        }
    }

    /// Returns a reference to the underlying [`AuthProvider`].
    #[must_use]
    pub fn provider(&self) -> &dyn AuthProvider {
        self.provider.as_ref()
    }

    /// Returns a clone of the underlying [`AuthProvider`] as an [`Arc`].
    #[must_use]
    pub fn provider_arc(&self) -> Arc<dyn AuthProvider> {
        self.provider.clone()
    }

    /// Validates the request token and required scope.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the authorization header is missing, malformed, or
    /// insufficient for the requested scope.
    pub fn authorize(
        &self,
        headers: &HeaderMap,
        required_scope: TokenScope,
    ) -> Result<AuthContext, ServerError> {
        let header = headers
            .get(AUTHORIZATION)
            .ok_or(ServerError::MissingAuthorization)?;
        let header = header
            .to_str()
            .map_err(|_error| ServerError::InvalidAuthorizationHeader)?;
        let token = parse_bearer_token(header)?;
        let claims = self.provider.verify_token(token)?;
        if !scope_allows(claims.scope(), required_scope) {
            return Err(ServerError::InsufficientScope);
        }

        Ok(AuthContext::new(claims))
    }
}

impl fmt::Debug for ServerAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerAuth")
            .field("provider", &"<dyn AuthProvider>")
            .finish()
    }
}

fn parse_bearer_token(header: &str) -> Result<&str, ServerError> {
    // Accept any ASCII-case variant of the `Bearer ` prefix (RFC 6750 scheme
    // names are case-insensitive). No allocation; bounds are checked up front.
    const BEARER_PREFIX: [u8; 7] = *b"bearer ";
    let header_bytes = header.as_bytes();
    let has_prefix = header_bytes.len() >= BEARER_PREFIX.len()
        && header_bytes
            .iter()
            .take(BEARER_PREFIX.len())
            .zip(BEARER_PREFIX.iter())
            .all(|(byte, prefix)| byte.to_ascii_lowercase() == *prefix);
    if !has_prefix {
        return Err(ServerError::InvalidAuthorizationHeader);
    }
    let token = header
        .get(BEARER_PREFIX.len()..)
        .ok_or(ServerError::InvalidAuthorizationHeader)?;
    if token.trim().is_empty() {
        return Err(ServerError::InvalidAuthorizationHeader);
    }
    if token.len() > MAX_TOKEN_STRING_BYTES {
        return Err(ServerError::InvalidAuthorizationHeader);
    }
    if token.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(ServerError::InvalidAuthorizationHeader);
    }

    Ok(token)
}

pub(crate) fn authorize_static_bearer_token(
    headers: &HeaderMap,
    expected_token: &[u8],
) -> Result<(), ServerError> {
    let header = headers
        .get(AUTHORIZATION)
        .ok_or(ServerError::MissingAuthorization)?;
    let header = header
        .to_str()
        .map_err(|_error| ServerError::InvalidAuthorizationHeader)?;
    let token = parse_bearer_token(header)?;
    let actual = token.as_bytes();

    use sha2::{Digest, Sha256};
    let actual_hash = Sha256::digest(actual);
    let expected_hash = Sha256::digest(expected_token);
    if bool::from(actual_hash.ct_eq(&expected_hash)) {
        return Ok(());
    }

    Err(ServerError::InvalidAuthorizationHeader)
}

const fn scope_allows(actual_scope: TokenScope, required_scope: TokenScope) -> bool {
    match required_scope {
        TokenScope::Read => actual_scope.allows_read(),
        TokenScope::Write => actual_scope.allows_write(),
    }
}

impl From<TokenCodecError> for ServerError {
    fn from(error: TokenCodecError) -> Self {
        Self::InvalidToken(error)
    }
}

impl From<AuthError> for ServerError {
    fn from(error: AuthError) -> Self {
        match error {
            AuthError::InvalidToken => Self::InvalidToken(TokenCodecError::InvalidFormat),
            AuthError::ExpiredToken => Self::InvalidToken(TokenCodecError::Expired),
            AuthError::InsufficientScope => Self::InsufficientScope,
            AuthError::ProviderError(msg) => Self::SigningKeyError(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{
        HeaderMap,
        header::{AUTHORIZATION, HeaderValue},
    };
    use shardline_protocol::{
        RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
    };

    use super::{MAX_TOKEN_STRING_BYTES, ServerAuth, authorize_static_bearer_token};
    use crate::ServerError;

    #[test]
    fn server_auth_rejects_missing_header() {
        let auth = ServerAuth::new(b"test-signing-key-32-bytes-long!!");
        assert!(auth.is_ok());
        let Ok(auth) = auth else {
            return;
        };

        assert!(matches!(
            auth.authorize(&HeaderMap::new(), TokenScope::Read),
            Err(ServerError::MissingAuthorization)
        ));
    }

    #[test]
    fn server_auth_rejects_insufficient_scope() {
        let auth = ServerAuth::new(b"test-signing-key-32-bytes-long!!");
        assert!(auth.is_ok());
        let Ok(auth) = auth else {
            return;
        };
        let signer = TokenSigner::new(b"test-signing-key-32-bytes-long!!");
        assert!(signer.is_ok());
        let Ok(signer) = signer else {
            return;
        };
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };
        let claims = TokenClaims::new(
            "local",
            "provider-user-1",
            TokenScope::Read,
            repository,
            u64::MAX,
        );
        assert!(claims.is_ok());
        let Ok(claims) = claims else {
            return;
        };
        let token = signer.sign(&claims);
        assert!(token.is_ok());
        let Ok(token) = token else {
            return;
        };
        let mut headers = HeaderMap::new();
        let header_value = HeaderValue::from_str(&format!("Bearer {token}"));
        assert!(header_value.is_ok());
        let Ok(header_value) = header_value else {
            return;
        };
        headers.insert(AUTHORIZATION, header_value);

        assert!(matches!(
            auth.authorize(&headers, TokenScope::Write),
            Err(ServerError::InsufficientScope)
        ));
    }

    #[test]
    fn server_auth_rejects_oversized_bearer_token_before_decoding() {
        let auth = ServerAuth::new(b"test-signing-key-32-bytes-long!!");
        assert!(auth.is_ok());
        let Ok(auth) = auth else {
            return;
        };
        let token = "a".repeat(MAX_TOKEN_STRING_BYTES + 1);
        let mut headers = HeaderMap::new();
        let header_value = HeaderValue::from_str(&format!("Bearer {token}"));
        assert!(header_value.is_ok());
        let Ok(header_value) = header_value else {
            return;
        };
        headers.insert(AUTHORIZATION, header_value);

        assert!(matches!(
            auth.authorize(&headers, TokenScope::Read),
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn server_auth_rejects_bearer_token_with_whitespace() {
        let auth = ServerAuth::new(b"test-signing-key-32-bytes-long!!");
        assert!(auth.is_ok());
        let Ok(auth) = auth else {
            return;
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer abc.def ghi"),
        );

        assert!(matches!(
            auth.authorize(&headers, TokenScope::Read),
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn server_auth_accepts_valid_write_token() {
        let auth = ServerAuth::new(b"test-signing-key-32-bytes-long!!");
        assert!(auth.is_ok());
        let Ok(auth) = auth else {
            return;
        };
        let signer = TokenSigner::new(b"test-signing-key-32-bytes-long!!");
        assert!(signer.is_ok());
        let Ok(signer) = signer else {
            return;
        };
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };
        let claims = TokenClaims::new(
            "local",
            "provider-user-1",
            TokenScope::Write,
            repository,
            u64::MAX,
        );
        assert!(claims.is_ok());
        let Ok(claims) = claims else {
            return;
        };
        let token = signer.sign(&claims);
        assert!(token.is_ok());
        let Ok(token) = token else {
            return;
        };
        let mut headers = HeaderMap::new();
        let header_value = HeaderValue::from_str(&format!("Bearer {token}"));
        assert!(header_value.is_ok());
        let Ok(header_value) = header_value else {
            return;
        };
        headers.insert(AUTHORIZATION, header_value);

        let context = auth.authorize(&headers, TokenScope::Read);

        assert!(context.is_ok());
        let Ok(context) = context else {
            return;
        };
        assert_eq!(context.claims().subject(), "provider-user-1");
        assert_eq!(context.claims().scope(), TokenScope::Write);
    }

    #[test]
    fn static_bearer_token_rejects_missing_header() {
        let result = authorize_static_bearer_token(&HeaderMap::new(), b"metrics-token");

        assert!(matches!(result, Err(ServerError::MissingAuthorization)));
    }

    #[test]
    fn static_bearer_token_rejects_wrong_value() {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer wrong-token"),
        );

        let result = authorize_static_bearer_token(&headers, b"metrics-token");

        assert!(matches!(
            result,
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn static_bearer_token_accepts_matching_value() {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer metrics-token"),
        );

        let result = authorize_static_bearer_token(&headers, b"metrics-token");

        assert!(result.is_ok());
    }

    // ── parse_bearer_token edge cases ──────────────────────────────────────

    #[test]
    fn parse_bearer_token_rejects_missing_bearer_prefix() {
        use super::parse_bearer_token;
        let result = parse_bearer_token("Basic token");
        assert!(matches!(
            result,
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn parse_bearer_token_rejects_empty_token_after_prefix() {
        use super::parse_bearer_token;
        let result = parse_bearer_token("Bearer ");
        assert!(matches!(
            result,
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn parse_bearer_token_rejects_whitespace_only_token() {
        use super::parse_bearer_token;
        let result = parse_bearer_token("Bearer   ");
        assert!(matches!(
            result,
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn parse_bearer_token_rejects_token_with_whitespace() {
        use super::parse_bearer_token;
        let result = parse_bearer_token("Bearer abc def");
        assert!(matches!(
            result,
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn parse_bearer_token_rejects_oversized_token() {
        use super::{MAX_TOKEN_STRING_BYTES, parse_bearer_token};
        let large = "a".repeat(MAX_TOKEN_STRING_BYTES + 1);
        let header = format!("Bearer {large}");
        let result = parse_bearer_token(&header);
        assert!(matches!(
            result,
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    #[test]
    fn parse_bearer_token_accepts_valid_token() {
        use super::parse_bearer_token;
        let result = parse_bearer_token("Bearer valid-token-here");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "valid-token-here");
    }

    #[test]
    fn parse_bearer_token_accepts_case_insensitive_prefix() {
        use super::parse_bearer_token;
        for header in [
            "bearer valid-token",
            "BEARER valid-token",
            "BeArEr valid-token",
            "bEaReR valid-token",
        ] {
            let result = parse_bearer_token(header);
            assert!(result.is_ok(), "expected Ok for {header:?}, got {result:?}");
            assert_eq!(result.unwrap(), "valid-token");
        }
    }

    #[test]
    fn parse_bearer_token_rejects_wrong_scheme_prefix() {
        use super::parse_bearer_token;
        for header in ["BearerX token", "bear token", "bearerr token"] {
            let result = parse_bearer_token(header);
            assert!(
                matches!(result, Err(ServerError::InvalidAuthorizationHeader)),
                "expected Err for {header:?}, got {result:?}"
            );
        }
    }

    // ── scope_allows ───────────────────────────────────────────────────────

    #[test]
    fn scope_allows_read_when_scope_is_read() {
        assert!(super::scope_allows(TokenScope::Read, TokenScope::Read));
    }

    #[test]
    fn scope_allows_write_when_scope_is_write() {
        assert!(super::scope_allows(TokenScope::Write, TokenScope::Write));
    }

    #[test]
    fn scope_allows_read_when_scope_is_write() {
        // Write scope implicitly allows Read
        assert!(super::scope_allows(TokenScope::Write, TokenScope::Read));
    }

    #[test]
    fn scope_allows_rejects_write_when_scope_is_read() {
        assert!(!super::scope_allows(TokenScope::Read, TokenScope::Write));
    }

    // ── AuthError conversion ───────────────────────────────────────────────

    #[test]
    fn from_auth_error_invalid_token() {
        use shardline_server_core::AuthError;
        let err: ServerError = AuthError::InvalidToken.into();
        assert!(matches!(err, ServerError::InvalidToken(_)));
    }

    #[test]
    fn from_auth_error_expired_token() {
        use shardline_server_core::AuthError;
        let err: ServerError = AuthError::ExpiredToken.into();
        assert!(matches!(err, ServerError::InvalidToken(_)));
    }

    #[test]
    fn from_auth_error_insufficient_scope() {
        use shardline_server_core::AuthError;
        let err: ServerError = AuthError::InsufficientScope.into();
        assert!(matches!(err, ServerError::InsufficientScope));
    }

    #[test]
    fn from_auth_error_provider_error() {
        use shardline_server_core::AuthError;
        let err: ServerError = AuthError::ProviderError("msg".to_owned()).into();
        assert!(matches!(err, ServerError::SigningKeyError(_)));
    }

    // ── ServerAuth from_provider ───────────────────────────────────────────

    #[test]
    fn server_auth_from_provider_delegates() {
        use shardline_server_core::auth::PassthroughProvider;
        let provider = Box::new(PassthroughProvider);
        let auth = ServerAuth::from_provider(provider);
        // Verify it can authorize a request with a Bearer token
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer any-token"));
        let result = auth.authorize(&headers, TokenScope::Write);
        assert!(result.is_ok());
        let ctx = result.unwrap();
        // PassthroughProvider uses "anonymous" as the default subject
        assert!(ctx.claims().subject() == "anonymous" || ctx.claims().subject() == "passthrough");
    }

    #[test]
    fn server_auth_debug_redacts_provider() {
        use shardline_server_core::auth::PassthroughProvider;
        let provider = Box::new(PassthroughProvider);
        let auth = ServerAuth::from_provider(provider);
        let debug = format!("{auth:?}");
        assert!(!debug.contains("PassthroughProvider"));
        assert!(debug.contains("<dyn AuthProvider>"));
    }

    #[test]
    fn server_auth_provider_arc_returns_cloneable_arc() {
        use shardline_server_core::auth::PassthroughProvider;
        let provider = Box::new(PassthroughProvider);
        let auth = ServerAuth::from_provider(provider);
        let arc = auth.provider_arc();
        // Verify the arc points to the same provider
        assert!(std::sync::Arc::ptr_eq(&auth.provider, &arc));
    }

    // ── Repeated Authorization header tests ────────────────────────────────

    #[test]
    fn authorize_picks_first_of_two_separate_authorization_headers() {
        // When a client sends two separate Authorization headers, `HeaderMap::get()`
        // returns the first one.  This test validates that behavior.
        let auth = ServerAuth::new(b"test-signing-key-32-bytes-long!!").unwrap();

        // Create two Authorization headers via `append`.
        let mut headers = HeaderMap::new();
        // A valid token is appended first.
        let signer = TokenSigner::new(b"test-signing-key-32-bytes-long!!").unwrap();
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))
                .unwrap();
        let claims = TokenClaims::new(
            "local",
            "provider-user-1",
            TokenScope::Write,
            repository,
            u64::MAX,
        )
        .unwrap();
        let valid_token = signer.sign(&claims).unwrap();

        headers.append(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {valid_token}")).unwrap(),
        );
        // Append a second (invalid) header — should be ignored.
        headers.append(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer invalid-token-here"),
        );

        // Headers.get() returns the first entry — the valid token.
        let result = auth.authorize(&headers, TokenScope::Read);
        assert!(
            result.is_ok(),
            "first Authorization header should be used, got: {result:?}"
        );
    }

    #[test]
    fn authorize_rejects_comma_separated_bearer_in_one_header() {
        // RFC 7230 §3.2.2 allows combining multiple header values into a single
        // comma-separated value.  If a client sends
        // `Authorization: Bearer token1, Bearer token2`, the token after
        // comma + space contains whitespace and MUST be rejected.
        let auth = ServerAuth::new(b"test-signing-key-32-bytes-long!!").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer valid-token, Bearer invalid-token"),
        );

        let result = auth.authorize(&headers, TokenScope::Read);
        assert!(
            matches!(result, Err(ServerError::InvalidAuthorizationHeader)),
            "comma-separated Bearer tokens should be rejected, got: {result:?}"
        );
    }

    #[test]
    fn parse_bearer_token_rejects_comma_space_in_token() {
        // Even without the second "Bearer" prefix, a comma + space triggers the
        // whitespace rejection in parse_bearer_token.
        use super::parse_bearer_token;
        let result = parse_bearer_token("Bearer token1, token2");
        assert!(matches!(
            result,
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    // ── ServerAuth authorize_token method ──────────────────────────────────

    #[test]
    fn server_auth_authorize_token_with_no_matching_token_provider() {
        // When auth is built with Local provider and no token is in the header,
        // it should go through the provider_path.
        use shardline_server_core::auth::PassthroughProvider;
        let provider = Box::new(PassthroughProvider);
        let auth = super::ServerAuth::from_provider(provider);

        // Empty headers → MissingAuthorization
        let result = auth.authorize(&HeaderMap::new(), TokenScope::Read);
        assert!(matches!(result, Err(ServerError::MissingAuthorization)));
    }

    #[test]
    fn server_auth_authorize_token_with_invalid_scheme() {
        use shardline_server_core::auth::PassthroughProvider;
        let provider = Box::new(PassthroughProvider);
        let auth = super::ServerAuth::from_provider(provider);

        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Basic token"));
        let result = auth.authorize(&headers, TokenScope::Read);
        assert!(matches!(
            result,
            Err(ServerError::InvalidAuthorizationHeader)
        ));
    }

    // ── scope_allows edge cases ────────────────────────────────────────────

    #[test]
    fn scope_allows_with_same_scope_read_read() {
        assert!(super::scope_allows(TokenScope::Read, TokenScope::Read));
    }

    #[test]
    fn scope_allows_with_same_scope_write_write() {
        assert!(super::scope_allows(TokenScope::Write, TokenScope::Write));
    }

    #[test]
    fn scope_allows_write_grants_read() {
        assert!(super::scope_allows(TokenScope::Write, TokenScope::Read));
    }

    #[test]
    fn scope_allows_read_denies_write() {
        assert!(!super::scope_allows(TokenScope::Read, TokenScope::Write));
    }
}
