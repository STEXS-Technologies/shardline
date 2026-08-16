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
    fn auth_error_display_invalid_token() {
        assert_eq!(AuthError::InvalidToken.to_string(), "invalid token");
    }

    #[test]
    fn auth_error_display_expired_token() {
        assert_eq!(AuthError::ExpiredToken.to_string(), "expired token");
    }

    #[test]
    fn auth_error_display_insufficient_scope() {
        let msg = AuthError::InsufficientScope.to_string();
        assert_eq!(msg, "insufficient scope");
    }

    #[test]
    fn auth_error_display_provider_error() {
        let msg = AuthError::ProviderError("oops".to_owned()).to_string();
        assert_eq!(msg, "provider error: oops");
    }

    #[test]
    fn auth_error_from_token_codec_error_expired() {
        let err: AuthError = TokenCodecError::Expired.into();
        assert!(matches!(err, AuthError::ExpiredToken));
    }

    #[test]
    fn auth_error_from_token_codec_error_invalid_signature() {
        let err: AuthError = TokenCodecError::InvalidSignature.into();
        assert!(matches!(err, AuthError::InvalidToken));
    }

    #[test]
    fn auth_error_from_token_codec_error_invalid_format() {
        let err: AuthError = TokenCodecError::InvalidFormat.into();
        assert!(matches!(err, AuthError::InvalidToken));
    }

    #[test]
    fn auth_error_from_token_codec_error_invalid_hex() {
        let hex_err = hex::FromHexError::InvalidStringLength;
        let err: AuthError = TokenCodecError::InvalidHex(hex_err).into();
        assert!(matches!(err, AuthError::InvalidToken));
    }

    #[test]
    fn auth_error_from_token_codec_error_claims() {
        let err: AuthError =
            TokenCodecError::Claims(shardline_protocol::TokenClaimsError::EmptyIssuer).into();
        assert!(matches!(err, AuthError::InvalidToken));
    }

    #[test]
    fn auth_error_from_token_codec_error_empty_key() {
        let err: AuthError = TokenCodecError::EmptySigningKey("test".to_owned()).into();
        assert!(matches!(err, AuthError::ProviderError(_)));
    }

    #[test]
    fn auth_error_from_token_codec_error_key_too_short() {
        let err: AuthError = TokenCodecError::SigningKeyTooShort { actual_bytes: 4 }.into();
        assert!(matches!(err, AuthError::ProviderError(_)));
    }

    #[test]
    fn auth_error_from_token_codec_error_json() {
        let json_err = serde_json::from_str::<serde_json::Value>("bad").unwrap_err();
        let err: AuthError = TokenCodecError::Json(json_err).into();
        assert!(matches!(err, AuthError::ProviderError(_)));
    }
}
