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
pub use types::{AuthContext, AuthError, VerifiedAuthContext};

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

    /// Verifies an opaque bearer token and returns a [`VerifiedAuthContext`].
    ///
    /// This is the only way to obtain a [`VerifiedAuthContext`] outside
    /// `shardline-auth`: the default implementation wraps the result of
    /// [`Self::verify_token`] **inside this crate**, so a `VerifiedAuthContext`
    /// always represents claims a provider actually verified — it can never be
    /// hand-constructed from bare [`TokenClaims`]. Downstream crates (e.g.
    /// `shardline-server-core`'s capability seam) rely on this type-level
    /// guarantee, so providers should not override this method.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when the token is invalid, expired, or otherwise
    /// unverifiable.
    fn verify_verified(&self, token: &str) -> Result<VerifiedAuthContext, AuthError> {
        let claims = self.verify_token(token)?;
        Ok(VerifiedAuthContext::from_verified_claims(claims))
    }

    /// Mints a signed bearer token from the provided claims.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when the provider does not support token minting
    /// or when signing fails.
    fn mint_token(&self, claims: &TokenClaims) -> Result<String, AuthError>;
}
