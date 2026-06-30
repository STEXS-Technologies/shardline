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
        let header = header
            .to_str()
            .map_err(|e| {
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
            AuthError::ProviderError(_) => Self::Unauthorized,
        }
    }
}
