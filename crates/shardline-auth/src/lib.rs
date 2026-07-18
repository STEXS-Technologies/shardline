#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string,
        clippy::panic
    )
)]

//! Authentication provider trait and implementations for the Shardline
//! ecosystem.  The [`AuthProvider`] trait is the main abstraction; see
//! [`local_hmac::LocalHmacProvider`] and [`passthrough::PassthroughProvider`]
//! for concrete implementations.

use shardline_protocol::{TokenClaims, TokenCodecError, TokenScope};
use thiserror::Error;

pub mod local_hmac;
pub mod passthrough;

pub use local_hmac::LocalHmacProvider;
pub use passthrough::PassthroughProvider;

/// Provider-agnostic authentication trait.
///
/// Implementations verify and mint scoped bearer tokens for the Shardline API.
/// The server selects a concrete provider at startup based on configuration.
pub trait AuthProvider: Send + Sync {
    /// Verifies an opaque bearer token and returns the decoded claims.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when the token is invalid, expired, or otherwise
    /// unverifiable.
    fn verify_token(&self, token: &str) -> Result<TokenClaims, AuthError>;

    /// Mints a signed bearer token from the provided claims.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when the provider does not support token minting
    /// or when signing fails.
    fn mint_token(&self, claims: &TokenClaims) -> Result<String, AuthError>;
}

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

    #[test]
    fn auth_context_new_and_accessors() {
        let repo = shardline_protocol::RepositoryScope::new(
            shardline_protocol::RepositoryProvider::GitHub,
            "o",
            "r",
            None,
        )
        .unwrap();
        let claims = TokenClaims::new("iss", "sub", TokenScope::Read, repo, 100).unwrap();
        let ctx = AuthContext::new(claims.clone());

        assert_eq!(ctx.claims(), &claims);
        assert_eq!(ctx.subject(), "sub");
        assert_eq!(ctx.scope(), TokenScope::Read);
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
