use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};

use crate::{AuthError, AuthProvider};

/// Trust-all authentication provider for development mode.
///
/// Any non-empty token is accepted with full write scope. **Do not use in
/// production.**
#[derive(Debug, Clone, Copy)]
pub struct PassthroughProvider;

impl AuthProvider for PassthroughProvider {
    fn verify_token(&self, token: &str) -> Result<TokenClaims, AuthError> {
        if token.trim().is_empty() {
            return Err(AuthError::InvalidToken);
        }

        let repository = RepositoryScope::new(
            RepositoryProvider::Generic,
            "anonymous",
            "anonymous",
            Some("main"),
        )
        .map_err(|e| AuthError::ProviderError(e.to_string()))?;

        TokenClaims::new("passthrough", "anonymous", TokenScope::Write, repository, u64::MAX)
            .map_err(|e| AuthError::ProviderError(e.to_string()))
    }

    fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
        Err(AuthError::ProviderError(
            "passthrough provider does not support token minting".to_owned(),
        ))
    }
}
