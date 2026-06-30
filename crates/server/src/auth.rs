use axum::http::{HeaderMap, header::AUTHORIZATION};
use shardline_protocol::{TokenClaims, TokenCodecError, TokenScope};
use shardline_server_core::{AuthError, AuthProvider};
use subtle::ConstantTimeEq;

use crate::ServerError;

const MAX_BEARER_TOKEN_BYTES: usize = 8192;

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
    provider: std::sync::Arc<dyn AuthProvider>,
}

impl ServerAuth {
    /// Creates a bearer-token verifier from a signing key using the local
    /// Ed25519 provider.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the signing key is invalid.
    pub fn new(signing_key: &[u8]) -> Result<Self, ServerError> {
        let provider =
            shardline_server_core::auth::LocalEd25519Provider::new(signing_key)?;
        Ok(Self {
            provider: std::sync::Arc::new(provider),
        })
    }

    /// Creates a bearer-token verifier from a boxed [`AuthProvider`].
    #[must_use]
    pub fn from_provider(provider: Box<dyn AuthProvider>) -> Self {
        Self {
            provider: std::sync::Arc::from(provider),
        }
    }

    /// Returns a reference to the underlying [`AuthProvider`].
    #[must_use]
    pub fn provider(&self) -> &dyn AuthProvider {
        self.provider.as_ref()
    }

    /// Returns a clone of the underlying [`AuthProvider`] as an [`Arc`].
    #[must_use]
    pub fn provider_arc(&self) -> std::sync::Arc<dyn AuthProvider> {
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

impl std::fmt::Debug for ServerAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerAuth")
            .field("provider", &"<dyn AuthProvider>")
            .finish()
    }
}

fn parse_bearer_token(header: &str) -> Result<&str, ServerError> {
    let Some(token) = header.strip_prefix("Bearer ") else {
        return Err(ServerError::InvalidAuthorizationHeader);
    };
    if token.trim().is_empty() {
        return Err(ServerError::InvalidAuthorizationHeader);
    }
    if token.len() > MAX_BEARER_TOKEN_BYTES {
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
    if actual.len() != expected_token.len() {
        return Err(ServerError::InvalidAuthorizationHeader);
    }
    if expected_token.ct_eq(actual).into() {
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
            AuthError::ProviderError(_msg) => {
                Self::InvalidToken(TokenCodecError::InvalidFormat)
            }
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

    use super::{MAX_BEARER_TOKEN_BYTES, ServerAuth, authorize_static_bearer_token};
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
        let token = "a".repeat(MAX_BEARER_TOKEN_BYTES + 1);
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
}
