use shardline_protocol::{TokenClaims, TokenSigner};

use crate::{AuthError, AuthProvider};

/// Shared-key token provider using HMAC-SHA256.
///
/// This is the default adapter. It signs and verifies tokens using a shared
/// HMAC-SHA256 signing key via [`TokenSigner`].
#[derive(Clone)]
pub struct LocalHmacProvider {
    signer: TokenSigner,
}

impl LocalHmacProvider {
    /// Creates a local HMAC-SHA256 provider from raw signing key bytes.
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

impl AuthProvider for LocalHmacProvider {
    fn verify_token(&self, token: &str) -> Result<TokenClaims, AuthError> {
        self.signer.verify_now(token).map_err(AuthError::from)
    }

    fn mint_token(&self, claims: &TokenClaims) -> Result<String, AuthError> {
        self.signer
            .sign(claims)
            .map_err(|e| AuthError::ProviderError(e.to_string()))
    }
}

impl std::fmt::Debug for LocalHmacProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalHmacProvider")
            .field("signer", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenScope};

    const VALID_KEY: &[u8] = b"test-signing-key-32-bytes-long!!";

    fn test_claims() -> TokenClaims {
        let repo = RepositoryScope::new(
            RepositoryProvider::Generic,
            "test-owner",
            "test-repo",
            Some("main"),
        )
        .unwrap();
        TokenClaims::new(
            "test-issuer",
            "test-subject",
            TokenScope::Write,
            repo,
            u64::MAX,
        )
        .unwrap()
    }

    // ── LocalHmacProvider::new ────────────────────────────────────────

    #[test]
    fn new_with_valid_key_succeeds() {
        assert!(LocalHmacProvider::new(VALID_KEY).is_ok());
    }

    #[test]
    fn new_with_empty_key_errors() {
        let result = LocalHmacProvider::new(b"");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AuthError::ProviderError(_)));
    }

    // ── hmac_mint_token_and_verify ───────────────────────────────────────

    #[test]
    fn hmac_mint_token_and_verify() {
        let provider = LocalHmacProvider::new(VALID_KEY).unwrap();
        let claims = test_claims();

        let token = provider.mint_token(&claims).unwrap();
        assert!(!token.is_empty());

        let verified = provider.verify_token(&token).unwrap();
        assert_eq!(verified, claims);
    }

    #[test]
    fn hmac_verify_expired_token() {
        let provider = LocalHmacProvider::new(VALID_KEY).unwrap();
        let repo = RepositoryScope::new(
            RepositoryProvider::Generic,
            "test-owner",
            "test-repo",
            Some("main"),
        )
        .unwrap();
        let claims = TokenClaims::new("issuer", "subject", TokenScope::Read, repo, 1).unwrap();
        let token = provider.mint_token(&claims).unwrap();

        // Current time is well past 1 second since epoch
        let result = provider.verify_token(&token);
        assert!(result.is_err(), "expired token should fail verification");
        assert!(matches!(result.unwrap_err(), AuthError::ExpiredToken));
    }

    // ── AuthProvider::verify_token ───────────────────────────────────────

    #[test]
    fn verify_token_valid_roundtrip() {
        let provider = LocalHmacProvider::new(VALID_KEY).unwrap();
        let claims = test_claims();
        let token = provider.mint_token(&claims).unwrap();

        let verified = provider.verify_token(&token);
        assert!(verified.is_ok());
        assert_eq!(verified.unwrap(), claims);
    }

    #[test]
    fn verify_token_wrong_key() {
        let key1 = b"test-signing-key-32-bytes-long!!";
        let key2 = b"another-signing-key-32-bytes-ok!";

        let provider1 = LocalHmacProvider::new(key1).unwrap();
        let provider2 = LocalHmacProvider::new(key2).unwrap();

        let claims = test_claims();
        let token = provider1.mint_token(&claims).unwrap();

        // provider2 has a different key; verification must fail
        let result = provider2.verify_token(&token);
        assert!(result.is_err());
    }

    #[test]
    fn verify_token_empty_string() {
        let provider = LocalHmacProvider::new(VALID_KEY).unwrap();
        let result = provider.verify_token("");
        assert!(result.is_err());
    }

    #[test]
    fn verify_token_expired() {
        let provider = LocalHmacProvider::new(VALID_KEY).unwrap();
        let repo = RepositoryScope::new(
            RepositoryProvider::Generic,
            "test-owner",
            "test-repo",
            Some("main"),
        )
        .unwrap();
        let claims = TokenClaims::new("issuer", "subject", TokenScope::Read, repo, 1).unwrap();
        let token = provider.mint_token(&claims).unwrap();

        // Current time is well past 1 second since epoch
        let result = provider.verify_token(&token);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AuthError::ExpiredToken));
    }

    // ── AuthProvider::mint_token ─────────────────────────────────────────

    #[test]
    fn mint_token_produces_verifiable_token() {
        let provider = LocalHmacProvider::new(VALID_KEY).unwrap();
        let claims = test_claims();

        let token = provider.mint_token(&claims).unwrap();
        // Token should be non-empty
        assert!(!token.is_empty());

        // Should roundtrip
        let verified = provider.verify_token(&token).unwrap();
        assert_eq!(verified.subject(), "test-subject");
        assert_eq!(verified.scope(), TokenScope::Write);
        assert_eq!(verified.issuer(), "test-issuer");
    }

    // ── Debug impl ───────────────────────────────────────────────────────

    #[test]
    fn debug_does_not_leak_signing_key() {
        let provider = LocalHmacProvider::new(VALID_KEY).unwrap();
        let debug_str = format!("{provider:?}");
        // The key bytes must never appear in debug output
        assert!(!debug_str.contains("test-signing-key"));
        assert!(debug_str.contains("<redacted>"));
    }
}
