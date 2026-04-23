use std::{fmt, str::FromStr};

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{Error as JsonError, from_slice, to_vec};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::{SecretBytes, unix_now_seconds_lossy};

type TokenMac = Hmac<sha2::Sha256>;

const MAX_TOKEN_COMPONENT_BYTES: usize = 512;
const MAX_TOKEN_STRING_BYTES: usize = 8192;
const TOKEN_SIGNATURE_HEX_BYTES: usize = 64;
const MAX_TOKEN_PAYLOAD_HEX_BYTES: usize = MAX_TOKEN_STRING_BYTES - TOKEN_SIGNATURE_HEX_BYTES - 1;

/// CAS token scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TokenScope {
    /// Read-only CAS access.
    Read,
    /// Write access, including the read behavior required by upload clients.
    Write,
}

impl TokenScope {
    /// Returns true when this scope can perform read operations.
    #[must_use]
    pub const fn allows_read(self) -> bool {
        matches!(self, Self::Read | Self::Write)
    }

    /// Returns true when this scope can perform write operations.
    #[must_use]
    pub const fn allows_write(self) -> bool {
        matches!(self, Self::Write)
    }
}

/// Provider family encoded into a scoped token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepositoryProvider {
    /// GitHub repository scope.
    GitHub,
    /// Gitea repository scope.
    Gitea,
    /// GitLab repository scope.
    GitLab,
    /// Generic Git forge repository scope.
    Generic,
}

impl RepositoryProvider {
    /// Returns the stable lowercase provider name used in persisted metadata.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GitHub => "github",
            Self::Gitea => "gitea",
            Self::GitLab => "gitlab",
            Self::Generic => "generic",
        }
    }
}

/// Repository provider parse failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error("repository provider was invalid")]
pub struct RepositoryProviderParseError;

impl FromStr for RepositoryProvider {
    type Err = RepositoryProviderParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "github" => Ok(Self::GitHub),
            "gitea" => Ok(Self::Gitea),
            "gitlab" => Ok(Self::GitLab),
            "generic" => Ok(Self::Generic),
            _other => Err(RepositoryProviderParseError),
        }
    }
}

/// Repository and revision scope encoded into a token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryScope {
    provider: RepositoryProvider,
    owner: String,
    name: String,
    revision: Option<String>,
}

impl RepositoryScope {
    /// Creates a repository scope.
    ///
    /// # Errors
    ///
    /// Returns [`TokenClaimsError`] when the owner, name, or revision contain invalid
    /// values.
    pub fn new(
        provider: RepositoryProvider,
        owner: &str,
        name: &str,
        revision: Option<&str>,
    ) -> Result<Self, TokenClaimsError> {
        validate_component(owner, TokenClaimsError::EmptyRepositoryOwner)?;
        validate_component(name, TokenClaimsError::EmptyRepositoryName)?;
        if let Some(value) = revision {
            validate_component(value, TokenClaimsError::EmptyRevision)?;
        }

        Ok(Self {
            provider,
            owner: owner.to_owned(),
            name: name.to_owned(),
            revision: revision.map(ToOwned::to_owned),
        })
    }

    /// Returns the scoped provider family.
    #[must_use]
    pub const fn provider(&self) -> RepositoryProvider {
        self.provider
    }

    /// Returns the scoped repository owner or namespace.
    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Returns the scoped repository name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the scoped revision, when one is required.
    #[must_use]
    pub fn revision(&self) -> Option<&str> {
        self.revision.as_deref()
    }
}

/// Signed token claims used by the Shardline API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenClaims {
    issuer: String,
    subject: String,
    scope: TokenScope,
    repository: RepositoryScope,
    expires_at_unix_seconds: u64,
}

impl TokenClaims {
    /// Creates token claims.
    ///
    /// # Errors
    ///
    /// Returns [`TokenClaimsError`] when the issuer, subject, or repository scope are
    /// invalid.
    pub fn new(
        issuer: &str,
        subject: &str,
        scope: TokenScope,
        repository: RepositoryScope,
        expires_at_unix_seconds: u64,
    ) -> Result<Self, TokenClaimsError> {
        validate_component(issuer, TokenClaimsError::EmptyIssuer)?;
        validate_component(subject, TokenClaimsError::EmptySubject)?;
        Ok(Self {
            issuer: issuer.to_owned(),
            subject: subject.to_owned(),
            scope,
            repository,
            expires_at_unix_seconds,
        })
    }

    /// Returns the token issuer identity.
    #[must_use]
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Returns the authenticated subject.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns the granted scope.
    #[must_use]
    pub const fn scope(&self) -> TokenScope {
        self.scope
    }

    /// Returns the scoped repository identity.
    #[must_use]
    pub const fn repository(&self) -> &RepositoryScope {
        &self.repository
    }

    /// Returns the token expiration timestamp as Unix seconds.
    #[must_use]
    pub const fn expires_at_unix_seconds(&self) -> u64 {
        self.expires_at_unix_seconds
    }
}

/// Token claim validation failure.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum TokenClaimsError {
    /// The issuer was empty.
    #[error("token issuer must not be empty")]
    EmptyIssuer,
    /// The subject was empty.
    #[error("token subject must not be empty")]
    EmptySubject,
    /// The repository owner was empty.
    #[error("token repository owner must not be empty")]
    EmptyRepositoryOwner,
    /// The repository name was empty.
    #[error("token repository name must not be empty")]
    EmptyRepositoryName,
    /// The revision was empty.
    #[error("token revision must not be empty when provided")]
    EmptyRevision,
    /// A token component contained control characters.
    #[error("token components must not contain control characters")]
    ControlCharacter,
    /// A token component exceeded the supported metadata bound.
    #[error("token component exceeded supported length")]
    TooLong,
}

/// Token signing or verification failure.
#[derive(Debug, Error)]
pub enum TokenCodecError {
    /// The signing key was empty.
    #[error("token signing key must not be empty")]
    EmptySigningKey,
    /// The token payload could not be serialized or deserialized.
    #[error("token json operation failed")]
    Json(#[from] JsonError),
    /// The token string did not match the expected format.
    #[error("token format was invalid")]
    InvalidFormat,
    /// A hex-encoded token segment was malformed.
    #[error("token hex segment was invalid")]
    InvalidHex(#[from] hex::FromHexError),
    /// The token signature did not verify.
    #[error("token signature was invalid")]
    InvalidSignature,
    /// The token has expired.
    #[error("token has expired")]
    Expired,
    /// The token payload contained invalid claims.
    #[error("token claims were invalid")]
    Claims(#[from] TokenClaimsError),
}

/// Local token signer and verifier.
#[derive(Clone)]
pub struct TokenSigner {
    signing_key: SecretBytes,
}

impl fmt::Debug for TokenSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenSigner")
            .field("signing_key", &"***")
            .finish()
    }
}

impl TokenSigner {
    /// Creates a token signer from raw key bytes.
    ///
    /// # Errors
    ///
    /// Returns [`TokenCodecError::EmptySigningKey`] when the signing key is empty.
    pub fn new(signing_key: &[u8]) -> Result<Self, TokenCodecError> {
        if signing_key.is_empty() {
            return Err(TokenCodecError::EmptySigningKey);
        }

        Ok(Self {
            signing_key: SecretBytes::from_slice(signing_key),
        })
    }

    /// Signs token claims into an opaque bearer token string.
    ///
    /// # Errors
    ///
    /// Returns [`TokenCodecError`] when the claims cannot be serialized.
    pub fn sign(&self, claims: &TokenClaims) -> Result<String, TokenCodecError> {
        let payload = to_vec(claims)?;
        let signature = self.signature(&payload)?;
        Ok(format!(
            "{}.{}",
            hex::encode(payload),
            hex::encode(signature)
        ))
    }

    /// Verifies a token against the supplied current Unix timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`TokenCodecError`] when the token does not parse, does not verify, or
    /// has expired.
    pub fn verify_at(
        &self,
        token: &str,
        current_unix_seconds: u64,
    ) -> Result<TokenClaims, TokenCodecError> {
        if token.len() > MAX_TOKEN_STRING_BYTES {
            return Err(TokenCodecError::InvalidFormat);
        }
        let Some((payload_hex, signature_hex)) = token.split_once('.') else {
            return Err(TokenCodecError::InvalidFormat);
        };
        if payload_hex.is_empty()
            || payload_hex.len() > MAX_TOKEN_PAYLOAD_HEX_BYTES
            || signature_hex.len() != TOKEN_SIGNATURE_HEX_BYTES
        {
            return Err(TokenCodecError::InvalidFormat);
        }
        let payload = hex::decode(payload_hex)?;
        let signature = hex::decode(signature_hex)?;
        let expected_signature = self.signature(&payload)?;
        if expected_signature.ct_eq(signature.as_slice()).unwrap_u8() != 1 {
            return Err(TokenCodecError::InvalidSignature);
        }

        let claims = from_slice::<TokenClaims>(&payload)?;
        validate_component(claims.issuer(), TokenClaimsError::EmptyIssuer)?;
        validate_component(claims.subject(), TokenClaimsError::EmptySubject)?;
        validate_component(
            claims.repository().owner(),
            TokenClaimsError::EmptyRepositoryOwner,
        )?;
        validate_component(
            claims.repository().name(),
            TokenClaimsError::EmptyRepositoryName,
        )?;
        if let Some(revision) = claims.repository().revision() {
            validate_component(revision, TokenClaimsError::EmptyRevision)?;
        }
        if claims.expires_at_unix_seconds() < current_unix_seconds {
            return Err(TokenCodecError::Expired);
        }

        Ok(claims)
    }

    /// Verifies a token against the current wall clock.
    ///
    /// # Errors
    ///
    /// Returns [`TokenCodecError`] when the token is invalid or expired.
    pub fn verify_now(&self, token: &str) -> Result<TokenClaims, TokenCodecError> {
        self.verify_at(token, unix_now_seconds_lossy())
    }

    fn signature(&self, payload: &[u8]) -> Result<Vec<u8>, TokenCodecError> {
        let mut mac = TokenMac::new_from_slice(self.signing_key.expose_secret())
            .map_err(|_error| TokenCodecError::EmptySigningKey)?;
        mac.update(payload);
        Ok(mac.finalize().into_bytes().to_vec())
    }
}

fn validate_component(value: &str, empty_error: TokenClaimsError) -> Result<(), TokenClaimsError> {
    if value.trim().is_empty() {
        return Err(empty_error);
    }

    if value.len() > MAX_TOKEN_COMPONENT_BYTES {
        return Err(TokenClaimsError::TooLong);
    }

    if value.chars().any(char::is_control) {
        return Err(TokenClaimsError::ControlCharacter);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_TOKEN_COMPONENT_BYTES, MAX_TOKEN_PAYLOAD_HEX_BYTES, RepositoryProvider,
        RepositoryProviderParseError, RepositoryScope, TOKEN_SIGNATURE_HEX_BYTES, TokenClaims,
        TokenClaimsError, TokenCodecError, TokenScope, TokenSigner,
    };

    #[test]
    fn write_token_allows_read_and_write() {
        assert!(TokenScope::Write.allows_read());
        assert!(TokenScope::Write.allows_write());
    }

    #[test]
    fn read_token_does_not_allow_write() {
        assert!(TokenScope::Read.allows_read());
        assert!(!TokenScope::Read.allows_write());
    }

    #[test]
    fn repository_provider_parses_stable_names() {
        assert_eq!("github".parse(), Ok(RepositoryProvider::GitHub));
        assert_eq!("gitea".parse(), Ok(RepositoryProvider::Gitea));
        assert_eq!("gitlab".parse(), Ok(RepositoryProvider::GitLab));
        assert_eq!("generic".parse(), Ok(RepositoryProvider::Generic));
        assert_eq!(
            "bitbucket".parse::<RepositoryProvider>(),
            Err(RepositoryProviderParseError)
        );
    }

    #[test]
    fn token_signer_debug_redacts_signing_key_material() {
        let signer = TokenSigner::new(&[1, 2, 3, 4]);
        assert!(signer.is_ok());
        let Ok(signer) = signer else {
            return;
        };

        let rendered = format!("{signer:?}");

        assert!(!rendered.contains("[1, 2, 3, 4]"));
        assert!(rendered.contains("***"));
    }

    #[test]
    fn token_claims_reject_empty_subject() {
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };
        let claims = TokenClaims::new("issuer", " ", TokenScope::Read, repository, 42);

        assert_eq!(claims, Err(TokenClaimsError::EmptySubject));
    }

    #[test]
    fn repository_scope_rejects_empty_owner() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "", "assets", Some("main"));

        assert_eq!(scope, Err(TokenClaimsError::EmptyRepositoryOwner));
    }

    #[test]
    fn repository_scope_rejects_oversized_components() {
        let oversized = "o".repeat(MAX_TOKEN_COMPONENT_BYTES + 1);
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, &oversized, "assets", None);

        assert_eq!(scope, Err(TokenClaimsError::TooLong));
    }

    #[test]
    fn token_claims_reject_oversized_subject() {
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };
        let oversized = "s".repeat(MAX_TOKEN_COMPONENT_BYTES + 1);
        let claims = TokenClaims::new("issuer", &oversized, TokenScope::Read, repository, 42);

        assert_eq!(claims, Err(TokenClaimsError::TooLong));
    }

    #[test]
    fn token_roundtrips_through_sign_and_verify() {
        let signer = TokenSigner::new(b"signing-key");
        assert!(signer.is_ok());
        let Ok(signer) = signer else {
            return;
        };
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };
        let claims = TokenClaims::new(
            "issuer",
            "provider-user-1",
            TokenScope::Write,
            repository,
            120,
        );
        assert!(claims.is_ok());
        let Ok(claims) = claims else {
            return;
        };

        let token = signer.sign(&claims);
        assert!(token.is_ok());
        let Ok(token) = token else {
            return;
        };
        let verified = signer.verify_at(&token, 119);

        assert!(verified.is_ok());
        let Ok(verified) = verified else {
            return;
        };
        assert_eq!(verified, claims);
    }

    #[test]
    fn token_verify_rejects_tampering() {
        let signer = TokenSigner::new(b"signing-key");
        assert!(signer.is_ok());
        let Ok(signer) = signer else {
            return;
        };
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };
        let claims = TokenClaims::new(
            "issuer",
            "provider-user-1",
            TokenScope::Read,
            repository,
            120,
        );
        assert!(claims.is_ok());
        let Ok(claims) = claims else {
            return;
        };
        let token = signer.sign(&claims);
        assert!(token.is_ok());
        let Ok(token) = token else {
            return;
        };
        let Some((payload, _signature)) = token.split_once('.') else {
            return;
        };
        let tampered = format!("{payload}.{}", "00".repeat(32));

        assert!(matches!(
            signer.verify_at(&tampered, 119),
            Err(TokenCodecError::InvalidSignature)
        ));
    }

    #[test]
    fn token_verify_rejects_oversized_token_before_hex_decoding() {
        let signer = TokenSigner::new(b"signing-key");
        assert!(signer.is_ok());
        let Ok(signer) = signer else {
            return;
        };
        let token = format!(
            "{}.{}",
            "a".repeat(MAX_TOKEN_PAYLOAD_HEX_BYTES + 1),
            "0".repeat(TOKEN_SIGNATURE_HEX_BYTES)
        );

        assert!(matches!(
            signer.verify_at(&token, 119),
            Err(TokenCodecError::InvalidFormat)
        ));
    }

    #[test]
    fn token_verify_rejects_oversized_signature_before_hex_decoding() {
        let signer = TokenSigner::new(b"signing-key");
        assert!(signer.is_ok());
        let Ok(signer) = signer else {
            return;
        };
        let token = format!("{}.{}", "7b7d", "0".repeat(TOKEN_SIGNATURE_HEX_BYTES + 1));

        assert!(matches!(
            signer.verify_at(&token, 119),
            Err(TokenCodecError::InvalidFormat)
        ));
    }

    #[test]
    fn token_verify_rejects_expired_tokens() {
        let signer = TokenSigner::new(b"signing-key");
        assert!(signer.is_ok());
        let Ok(signer) = signer else {
            return;
        };
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };
        let claims = TokenClaims::new(
            "issuer",
            "provider-user-1",
            TokenScope::Read,
            repository,
            120,
        );
        assert!(claims.is_ok());
        let Ok(claims) = claims else {
            return;
        };
        let token = signer.sign(&claims);
        assert!(token.is_ok());
        let Ok(token) = token else {
            return;
        };

        assert!(matches!(
            signer.verify_at(&token, 121),
            Err(TokenCodecError::Expired)
        ));
    }
}
