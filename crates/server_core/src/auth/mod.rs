//! Built-in authentication provider implementations.

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
            TokenCodecError::EmptySigningKey
            | TokenCodecError::SigningKeyTooShort { .. }
            | TokenCodecError::Json(_) => Self::ProviderError(error.to_string()),
        }
    }
}
