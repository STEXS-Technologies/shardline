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
//! for concrete implementations.  See [`ed25519::Ed25519AuthProvider`] for
//! asymmetric-key authentication.

use shardline_protocol::TokenClaims;

pub mod ed25519;
pub mod local_hmac;
pub mod passthrough;
mod types;

pub use ed25519::Ed25519AuthProvider;
pub use local_hmac::LocalHmacProvider;
pub use passthrough::PassthroughProvider;
pub use types::{AuthContext, AuthError};

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
