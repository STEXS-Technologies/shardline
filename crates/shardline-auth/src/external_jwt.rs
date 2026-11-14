//! External JWT verification with an independently configured internal issuer.
use std::sync::Arc;

use shardline_protocol::TokenClaims;

use crate::{AuthError, AuthProvider, LocalHmacProvider};

/// Verifies external JWTs and issues Shardline's native scoped credentials.
///
/// JWTs have three dot-separated components; native credentials have two.
/// Each envelope selects exactly one verifier. A failed verification never
/// falls back to the other trust root. Claim contents and expiry are preserved
/// by the existing native signer, including restricted Hub read-token issuers.
pub struct ExternalJwtWithLocalIssuer {
    verifier: Arc<dyn AuthProvider>,
    issuer: LocalHmacProvider,
}

impl ExternalJwtWithLocalIssuer {
    /// Combines a JWT verifier with the deployment's internal signing key.
    ///
    /// # Errors
    /// Returns an error when the internal signing key is invalid.
    pub fn new(verifier: Arc<dyn AuthProvider>, signing_key: &[u8]) -> Result<Self, AuthError> {
        Ok(Self {
            verifier,
            issuer: LocalHmacProvider::new(signing_key)?,
        })
    }
}

impl AuthProvider for ExternalJwtWithLocalIssuer {
    fn verify_token(&self, token: &str) -> Result<TokenClaims, AuthError> {
        if token.split('.').count() == 3 {
            self.verifier.verify_token(token)
        } else {
            self.issuer.verify_token(token)
        }
    }

    fn mint_token(&self, claims: &TokenClaims) -> Result<String, AuthError> {
        self.issuer.mint_token(claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenScope};

    struct JwtVerifier;
    impl AuthProvider for JwtVerifier {
        fn verify_token(&self, token: &str) -> Result<TokenClaims, AuthError> {
            if token != "external.jwt.signature" {
                return Err(AuthError::ProviderError("invalid external JWT".into()));
            }
            TokenClaims::new(
                "external",
                "subject",
                TokenScope::Read,
                RepositoryScope::new(RepositoryProvider::Generic, "owner", "repo", Some("main"))
                    .map_err(|error| AuthError::ProviderError(error.to_string()))?,
                u64::MAX,
            )
            .map_err(|error| AuthError::ProviderError(error.to_string()))
        }
        fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
            Err(AuthError::ProviderError(
                "external JWT verification cannot issue credentials".into(),
            ))
        }
    }

    #[test]
    fn exchange_preserves_claims_and_keeps_verification_roots_separate() {
        let provider = ExternalJwtWithLocalIssuer::new(
            Arc::new(JwtVerifier),
            b"internal-deployment-key-0123456789",
        )
        .unwrap();
        let claims = provider.verify_token("external.jwt.signature").unwrap();
        let token = provider.mint_token(&claims).unwrap();
        assert_eq!(provider.verify_token(&token).unwrap(), claims);
        assert!(JwtVerifier.verify_token(&token).is_err());
        let other = ExternalJwtWithLocalIssuer::new(
            Arc::new(JwtVerifier),
            b"different-deployment-key-0123456789",
        )
        .unwrap();
        assert!(other.verify_token(&token).is_err());
        assert!(provider.verify_token("bad.jwt.signature").is_err());
        assert!(provider.verify_token("bad.signature").is_err());
        for issuer in ["external", shardline_protocol::HUB_XET_READ_TOKEN_ISSUER] {
            let restricted = TokenClaims::new(
                issuer,
                claims.subject(),
                TokenScope::Read,
                claims.repository().clone(),
                1,
            )
            .unwrap();
            let expired = provider.mint_token(&restricted).unwrap();
            assert!(matches!(
                provider.verify_token(&expired),
                Err(AuthError::ExpiredToken)
            ));
        }
        let mut tampered = token.into_bytes();
        let last = tampered.last_mut().unwrap();
        *last = if *last == b'0' { b'1' } else { b'0' };
        assert!(
            provider
                .verify_token(std::str::from_utf8(&tampered).unwrap())
                .is_err()
        );
    }
}
