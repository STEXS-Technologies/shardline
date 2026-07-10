use std::sync::Once;

use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};

use crate::{AuthError, AuthProvider};

static PASSTHROUGH_WARNING: Once = Once::new();

/// Trust-all authentication provider for development mode.
///
/// Any non-empty token is accepted with full write scope. **Do not use in
/// production.**
#[derive(Debug, Clone, Copy)]
pub struct PassthroughProvider;

impl AuthProvider for PassthroughProvider {
    fn verify_token(&self, token: &str) -> Result<TokenClaims, AuthError> {
        PASSTHROUGH_WARNING.call_once(|| {
            tracing::warn!(
                "SECURITY: PassthroughProvider is active — all tokens are accepted. \
                 Do NOT use in production."
            );
        });
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

        TokenClaims::new(
            "passthrough",
            "anonymous",
            TokenScope::Write,
            repository,
            u64::MAX,
        )
        .map_err(|e| AuthError::ProviderError(e.to_string()))
    }

    fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
        Err(AuthError::ProviderError(
            "passthrough provider does not support token minting".to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AuthProvider;
    use shardline_protocol::RepositoryProvider;

    // ── PassthroughProvider::verify_token ────────────────────────────────

    #[test]
    fn verify_empty_token_errors() {
        let provider = PassthroughProvider;
        let result = provider.verify_token("");
        assert!(matches!(result, Err(AuthError::InvalidToken)));
    }

    #[test]
    fn verify_whitespace_only_token_errors() {
        let provider = PassthroughProvider;
        let result = provider.verify_token("   \t\n  ");
        assert!(matches!(result, Err(AuthError::InvalidToken)));
    }

    #[test]
    fn verify_any_nonempty_token_succeeds() {
        let provider = PassthroughProvider;
        let result = provider.verify_token("any-token-string");
        assert!(result.is_ok());

        let claims = result.unwrap();
        assert_eq!(claims.subject(), "anonymous");
        assert_eq!(claims.scope(), TokenScope::Write);
        assert_eq!(claims.issuer(), "passthrough");
    }

    #[test]
    fn verify_token_has_write_scope() {
        let provider = PassthroughProvider;
        let claims = provider.verify_token("dev-token-123").unwrap();
        assert!(claims.scope().allows_write());
        assert!(claims.scope().allows_read());
    }

    #[test]
    fn verify_token_repository_scope() {
        let provider = PassthroughProvider;
        let claims = provider.verify_token("test").unwrap();
        let repo = claims.repository();
        assert_eq!(repo.provider(), RepositoryProvider::Generic);
        assert_eq!(repo.owner(), "anonymous");
        assert_eq!(repo.name(), "anonymous");
        assert_eq!(repo.revision(), Some("main"));
    }

    // ── PassthroughProvider::mint_token ──────────────────────────────────

    #[test]
    fn mint_token_always_errors() {
        let provider = PassthroughProvider;
        let claims = shardline_protocol::TokenClaims::new(
            "issuer",
            "subject",
            TokenScope::Write,
            shardline_protocol::RepositoryScope::new(
                RepositoryProvider::Generic,
                "owner",
                "repo",
                Some("main"),
            )
            .unwrap(),
            u64::MAX,
        )
        .unwrap();

        let result = provider.mint_token(&claims);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AuthError::ProviderError(_)));
    }
}
