use shardline_protocol::{TokenClaims, TokenSigner};

use crate::{AuthError, AuthProvider};

/// Ed25519 key-based token provider.
///
/// This is the default adapter. It signs and verifies tokens using a shared
/// HMAC-SHA256 signing key via [`TokenSigner`].
#[derive(Clone)]
pub struct LocalEd25519Provider {
    signer: TokenSigner,
}

impl LocalEd25519Provider {
    /// Creates a local Ed25519 provider from raw signing key bytes.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::ProviderError`] when the signing key is empty.
    pub fn new(signing_key: &[u8]) -> Result<Self, AuthError> {
        let signer =
            TokenSigner::new(signing_key).map_err(|e| AuthError::ProviderError(e.to_string()))?;
        Ok(Self { signer })
    }
}

impl AuthProvider for LocalEd25519Provider {
    fn verify_token(&self, token: &str) -> Result<TokenClaims, AuthError> {
        self.signer
            .verify_now(token)
            .map_err(AuthError::from)
    }

    fn mint_token(&self, claims: &TokenClaims) -> Result<String, AuthError> {
        self.signer
            .sign(claims)
            .map_err(|e| AuthError::ProviderError(e.to_string()))
    }
}

impl std::fmt::Debug for LocalEd25519Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalEd25519Provider")
            .field("signer", &"<redacted>")
            .finish()
    }
}
