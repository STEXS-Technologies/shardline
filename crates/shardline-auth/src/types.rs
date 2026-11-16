use shardline_protocol::{TokenClaims, TokenCodecError, TokenScope};
use thiserror::Error;

/// Verified request authorization context.
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// The decoded token claims.
    pub claims: TokenClaims,
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

    /// Returns the authenticated subject.
    #[must_use]
    pub fn subject(&self) -> &str {
        self.claims.subject()
    }

    /// Returns the granted scope.
    #[must_use]
    pub const fn scope(&self) -> TokenScope {
        self.claims.scope()
    }
}

/// Verified request authorization context, mintable **only** by the auth layer.
///
/// Unlike [`AuthContext`] — a plain value type whose [`AuthContext::new`] is
/// `pub const` over bare [`TokenClaims`] — a `VerifiedAuthContext` cannot be
/// hand-constructed outside `shardline-auth`: its constructor is `pub(crate)`
/// and there is no public `From`/`Into` conversion from [`AuthContext`] or
/// [`TokenClaims`]. The only way to obtain one is through a provider's
/// verification ([`crate::AuthProvider::verify_verified`]), so its presence
/// proves claims actually passed through the auth layer.
///
/// Capability minting in `shardline-server-core`
/// ([`shardline_server_core::auth_capability::AuthorizedRepository::from_verified_context`])
/// consumes only this type, which makes the capability seal **type-enforced**:
/// a forged `AuthContext` can never reach the seam.
#[derive(Debug, Clone)]
pub struct VerifiedAuthContext {
    ctx: AuthContext,
}

impl VerifiedAuthContext {
    /// Wraps claims that a provider just verified.
    ///
    /// `pub(crate)` on purpose: only code inside `shardline-auth` (the
    /// [`crate::AuthProvider::verify_verified`] trait boundary) may construct
    /// this type. External crates must go through a provider verification.
    pub(crate) const fn from_verified_claims(claims: TokenClaims) -> Self {
        Self {
            ctx: AuthContext::new(claims),
        }
    }

    /// Consumes the context, returning the verified claims.
    #[must_use]
    pub fn into_claims(self) -> TokenClaims {
        self.ctx.claims
    }

    /// Returns the verified claims.
    #[must_use]
    pub const fn claims(&self) -> &TokenClaims {
        self.ctx.claims()
    }

    /// Returns the authenticated subject.
    #[must_use]
    pub fn subject(&self) -> &str {
        self.ctx.subject()
    }

    /// Returns the granted scope.
    #[must_use]
    pub const fn scope(&self) -> TokenScope {
        self.ctx.scope()
    }
}

/// Authentication provider failure.
#[derive(Debug, Error)]
pub enum AuthError {
    /// The token format was invalid.
    #[error("invalid token")]
    InvalidToken,
    /// The token has expired.
    #[error("expired token")]
    ExpiredToken,
    /// The token does not grant the required scope.
    #[error("insufficient scope")]
    InsufficientScope,
    /// The provider encountered an internal error.
    #[error("provider error: {0}")]
    ProviderError(String),
}

impl From<TokenCodecError> for AuthError {
    fn from(error: TokenCodecError) -> Self {
        match error {
            TokenCodecError::Expired => Self::ExpiredToken,
            TokenCodecError::InvalidSignature
            | TokenCodecError::InvalidFormat
            | TokenCodecError::InvalidHex(_)
            | TokenCodecError::Claims(_) => Self::InvalidToken,
            TokenCodecError::EmptySigningKey(_)
            | TokenCodecError::SigningKeyTooShort { .. }
            | TokenCodecError::Json(_) => Self::ProviderError(error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    #[test]
    fn auth_context_new_and_accessors() {
        let repo = RepositoryScope::new(RepositoryProvider::GitHub, "o", "r", None).unwrap();
        let claims = TokenClaims::new("iss", "sub", TokenScope::Read, repo, 100).unwrap();
        let ctx = AuthContext::new(claims.clone());

        assert_eq!(ctx.claims(), &claims);
        assert_eq!(ctx.subject(), "sub");
        assert_eq!(ctx.scope(), TokenScope::Read);
    }

    #[test]
    fn verified_auth_context_accessors_and_into_claims() {
        let repo = RepositoryScope::new(RepositoryProvider::GitHub, "o", "r", None).unwrap();
        let claims = TokenClaims::new("iss", "sub", TokenScope::Read, repo, 100).unwrap();
        let ctx = VerifiedAuthContext::from_verified_claims(claims.clone());

        assert_eq!(ctx.claims(), &claims);
        assert_eq!(ctx.subject(), "sub");
        assert_eq!(ctx.scope(), TokenScope::Read);
        assert_eq!(ctx.into_claims(), claims);
    }

    #[test]
    fn auth_error_display_preserves_public_messages() {
        for (error, expected) in [
            (AuthError::InvalidToken, "invalid token"),
            (AuthError::ExpiredToken, "expired token"),
            (AuthError::InsufficientScope, "insufficient scope"),
            (
                AuthError::ProviderError("oops".to_owned()),
                "provider error: oops",
            ),
        ] {
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn auth_error_from_token_codec_error_expired() {
        let err: AuthError = TokenCodecError::Expired.into();
        assert!(matches!(err, AuthError::ExpiredToken));
    }

    #[test]
    fn malformed_credentials_become_invalid_token_errors() {
        for error in [
            TokenCodecError::InvalidSignature,
            TokenCodecError::InvalidFormat,
            TokenCodecError::InvalidHex(hex::FromHexError::InvalidStringLength),
            TokenCodecError::Claims(shardline_protocol::TokenClaimsError::EmptyIssuer),
        ] {
            let err = AuthError::from(error);
            assert!(matches!(err, AuthError::InvalidToken), "{err:?}");
        }
    }

    #[test]
    fn configuration_and_serialization_failures_remain_provider_errors() {
        for error in [
            TokenCodecError::EmptySigningKey("test".to_owned()),
            TokenCodecError::SigningKeyTooShort { actual_bytes: 4 },
            TokenCodecError::Json(serde_json::from_str::<serde_json::Value>("bad").unwrap_err()),
        ] {
            let err = AuthError::from(error);
            assert!(matches!(err, AuthError::ProviderError(_)), "{err:?}");
        }
    }
}
