use crate::{AuthError, VerifiedAuthContext};
use shardline_protocol::TokenClaims;

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
