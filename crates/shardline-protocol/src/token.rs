use std::{fmt, str::FromStr};

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{Error as JsonError, from_slice, to_vec};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::{SecretBytes, unix_now_seconds_lossy};

type TokenMac = Hmac<sha2::Sha256>;

const MAX_TOKEN_COMPONENT_BYTES: usize = 512;
/// Maximum accepted encoded bearer-token length.
pub const MAX_TOKEN_STRING_BYTES: usize = 16_384;
const TOKEN_SIGNATURE_HEX_BYTES: usize = 64;
const MAX_TOKEN_PAYLOAD_HEX_BYTES: usize = MAX_TOKEN_STRING_BYTES - 2;

/// Scope of a bearer token used to authorize access to content-addressed storage.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepositoryProvider {
    /// GitHub repository scope.
    GitHub,
    /// Gitea repository scope.
    Gitea,
    /// GitLab repository scope.
    GitLab,
    /// Codeberg (Gitea-based) repository scope.
    Codeberg,
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
            Self::Codeberg => "codeberg",
            Self::Generic => "generic",
        }
    }
}

impl Serialize for RepositoryProvider {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RepositoryProvider {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        // Accept case-insensitive input for backward compatibility with any
        // data serialized by the default derive (which used the Rust variant name).
        s.to_ascii_lowercase()
            .parse()
            .map_err(serde::de::Error::custom)
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
            "codeberg" => Ok(Self::Codeberg),
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
    /// # Examples
    ///
    /// ```
    /// use shardline_protocol::{RepositoryProvider, RepositoryScope};
    ///
    /// let scope =
    ///     RepositoryScope::new(RepositoryProvider::GitHub, "acme", "assets", Some("main"))?;
    /// assert_eq!(scope.provider().as_str(), "github");
    /// assert_eq!(scope.owner(), "acme");
    /// assert_eq!(scope.name(), "assets");
    /// assert_eq!(scope.revision(), Some("main"));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
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
    /// # Examples
    ///
    /// ```
    /// use shardline_protocol::{
    ///     RepositoryProvider, RepositoryScope, TokenClaims, TokenScope,
    /// };
    ///
    /// let repository =
    ///     RepositoryScope::new(RepositoryProvider::GitHub, "acme", "assets", None)?;
    /// let claims = TokenClaims::new(
    ///     "shardline",
    ///     "alice",
    ///     TokenScope::Read,
    ///     repository,
    ///     1_700_000_600,
    /// )?;
    /// assert_eq!(claims.issuer(), "shardline");
    /// assert_eq!(claims.subject(), "alice");
    /// assert_eq!(claims.scope(), TokenScope::Read);
    /// assert_eq!(claims.expires_at_unix_seconds(), 1_700_000_600);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
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

/// Minimum signing key length in bytes.
const MIN_SIGNING_KEY_BYTES: usize = 32;

/// Token signing or verification failure.
#[derive(Debug, Error)]
pub enum TokenCodecError {
    /// The signing key was empty.
    #[error("token signing key must not be empty: {0}")]
    EmptySigningKey(String),
    /// The signing key is too short.
    #[error("token signing key must be at least {MIN_SIGNING_KEY_BYTES} bytes, got {actual_bytes}")]
    SigningKeyTooShort { actual_bytes: usize },
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
            return Err(TokenCodecError::EmptySigningKey(
                "provided key is empty".to_owned(),
            ));
        }
        if signing_key.len() < MIN_SIGNING_KEY_BYTES {
            return Err(TokenCodecError::SigningKeyTooShort {
                actual_bytes: signing_key.len(),
            });
        }

        Ok(Self {
            signing_key: SecretBytes::from_slice(signing_key),
        })
    }

    /// Signs token claims into an opaque bearer token string.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_protocol::{
    ///     RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
    /// };
    ///
    /// let repository =
    ///     RepositoryScope::new(RepositoryProvider::GitHub, "acme", "assets", None)?;
    /// let claims = TokenClaims::new(
    ///     "shardline",
    ///     "alice",
    ///     TokenScope::Read,
    ///     repository,
    ///     1_700_000_600,
    /// )?;
    /// let signer = TokenSigner::new(b"development-only-signing-key-32bytes")?;
    ///
    /// let token = signer.sign(&claims)?;
    /// assert!(token.contains('.'));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`TokenCodecError`] when the claims cannot be serialized.
    pub fn sign(&self, claims: &TokenClaims) -> Result<String, TokenCodecError> {
        let payload = encode_token_claims(claims)?;
        let signature = self.signature(&payload)?;
        format_signed_token(&payload, &signature)
    }

    /// Verifies a token against the supplied current Unix timestamp.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_protocol::{
    ///     RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
    /// };
    ///
    /// let repository =
    ///     RepositoryScope::new(RepositoryProvider::GitHub, "acme", "assets", None)?;
    /// let claims = TokenClaims::new(
    ///     "shardline",
    ///     "alice",
    ///     TokenScope::Read,
    ///     repository,
    ///     1_700_000_600,
    /// )?;
    /// let signer = TokenSigner::new(b"development-only-signing-key-32bytes")?;
    /// let token = signer.sign(&claims)?;
    ///
    /// // Verifying before the expiry succeeds...
    /// let verified = signer.verify_at(&token, 1_700_000_000)?;
    /// assert_eq!(verified.subject(), "alice");
    ///
    /// // ...and verifying after the expiry fails.
    /// assert!(signer.verify_at(&token, 1_700_000_601).is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
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
        let (payload_hex, signature_hex) = split_token(token)?;
        if signature_hex.len() != TOKEN_SIGNATURE_HEX_BYTES {
            return Err(TokenCodecError::InvalidFormat);
        }
        let payload = hex::decode(payload_hex)?;
        let signature = hex::decode(signature_hex)?;
        let expected_signature = self.signature(&payload)?;
        if expected_signature.ct_eq(signature.as_slice()).unwrap_u8() != 1 {
            return Err(TokenCodecError::InvalidSignature);
        }
        decode_and_validate_claims(&payload, current_unix_seconds)
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
            .map_err(|err| TokenCodecError::EmptySigningKey(err.to_string()))?;
        mac.update(payload);
        Ok(mac.finalize().into_bytes().to_vec())
    }
}

/// Splits a token string `payload_hex.signature_hex` into its two hex segments.
///
/// # Errors
///
/// Returns [`TokenCodecError::InvalidFormat`] when the token exceeds the maximum
/// length, does not contain exactly one separator, or has an empty segment.
pub fn split_token(token: &str) -> Result<(&str, &str), TokenCodecError> {
    if token.len() > MAX_TOKEN_STRING_BYTES {
        return Err(TokenCodecError::InvalidFormat);
    }
    let Some((payload_hex, signature_hex)) = token.split_once('.') else {
        return Err(TokenCodecError::InvalidFormat);
    };
    if payload_hex.is_empty()
        || payload_hex.len() > MAX_TOKEN_PAYLOAD_HEX_BYTES
        || signature_hex.is_empty()
        || signature_hex.contains('.')
    {
        return Err(TokenCodecError::InvalidFormat);
    }
    Ok((payload_hex, signature_hex))
}

/// Serializes validated token claims into the canonical signed payload.
///
/// # Errors
///
/// Returns [`TokenCodecError::Json`] when serialization fails.
pub fn encode_token_claims(claims: &TokenClaims) -> Result<Vec<u8>, TokenCodecError> {
    Ok(to_vec(claims)?)
}

/// Formats a payload and signature using Shardline's canonical hex token envelope.
///
/// # Errors
///
/// Returns [`TokenCodecError::InvalidFormat`] when the encoded token would exceed
/// [`MAX_TOKEN_STRING_BYTES`] or either segment is empty.
pub fn format_signed_token(payload: &[u8], signature: &[u8]) -> Result<String, TokenCodecError> {
    if payload.is_empty() || signature.is_empty() {
        return Err(TokenCodecError::InvalidFormat);
    }
    let encoded_len = payload
        .len()
        .checked_mul(2)
        .and_then(|length| length.checked_add(1))
        .and_then(|length| {
            signature
                .len()
                .checked_mul(2)
                .and_then(|value| length.checked_add(value))
        })
        .ok_or(TokenCodecError::InvalidFormat)?;
    if encoded_len > MAX_TOKEN_STRING_BYTES {
        return Err(TokenCodecError::InvalidFormat);
    }
    Ok(format!(
        "{}.{}",
        hex::encode(payload),
        hex::encode(signature)
    ))
}

/// Decodes and validates token claims from raw JSON payload bytes.
///
/// # Errors
///
/// Returns [`TokenCodecError`] when the payload cannot be deserialized, the
/// claims contain invalid components, or the token has expired.
pub fn decode_and_validate_claims(
    payload: &[u8],
    current_unix_seconds: u64,
) -> Result<TokenClaims, TokenCodecError> {
    let claims = from_slice::<TokenClaims>(payload)?;
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
        MAX_TOKEN_COMPONENT_BYTES, MAX_TOKEN_PAYLOAD_HEX_BYTES, MAX_TOKEN_STRING_BYTES,
        RepositoryProvider, RepositoryProviderParseError, RepositoryScope,
        TOKEN_SIGNATURE_HEX_BYTES, TokenClaims, TokenClaimsError, TokenCodecError, TokenScope,
        TokenSigner, format_signed_token, split_token,
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
        assert_eq!("codeberg".parse(), Ok(RepositoryProvider::Codeberg));
        assert_eq!("generic".parse(), Ok(RepositoryProvider::Generic));
        assert_eq!(
            "bitbucket".parse::<RepositoryProvider>(),
            Err(RepositoryProviderParseError)
        );
    }

    #[test]
    fn token_signer_debug_redacts_signing_key_material() {
        let signer = TokenSigner::new(&[1; 32]);
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
    fn repository_scope_accepts_all_providers() {
        for provider in [
            RepositoryProvider::GitHub,
            RepositoryProvider::Gitea,
            RepositoryProvider::GitLab,
            RepositoryProvider::Codeberg,
            RepositoryProvider::Generic,
        ] {
            let scope = RepositoryScope::new(provider, "owner", "repo", Some("main"));
            assert!(scope.is_ok(), "failed for {provider:?}");
            if let Ok(scope) = scope {
                assert_eq!(scope.provider(), provider);
                assert_eq!(scope.owner(), "owner");
                assert_eq!(scope.name(), "repo");
                assert_eq!(scope.revision(), Some("main"));
            }
        }
    }

    #[test]
    fn repository_scope_accepts_missing_revision() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", None);
        assert!(scope.is_ok());
        if let Ok(scope) = scope {
            assert_eq!(scope.revision(), None);
        }
    }

    #[test]
    fn repository_scope_rejects_empty_revision() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", Some(""));
        assert_eq!(scope, Err(TokenClaimsError::EmptyRevision));
    }

    #[test]
    fn repository_scope_rejects_empty_name() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "owner", "", None);
        assert_eq!(scope, Err(TokenClaimsError::EmptyRepositoryName));
    }

    #[test]
    fn repository_scope_rejects_control_characters_in_owner() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "own\ner", "repo", None);
        assert_eq!(scope, Err(TokenClaimsError::ControlCharacter));
    }

    #[test]
    fn repository_scope_rejects_control_characters_in_name() {
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "owner", "rep\0o", None);
        assert_eq!(scope, Err(TokenClaimsError::ControlCharacter));
    }

    #[test]
    fn repository_scope_rejects_control_characters_in_revision() {
        let scope = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "owner",
            "repo",
            Some("main\x00"),
        );
        assert_eq!(scope, Err(TokenClaimsError::ControlCharacter));
    }

    #[test]
    fn token_claims_serialization_roundtrips() {
        let repository =
            RepositoryScope::new(RepositoryProvider::GitLab, "group", "project", Some("dev"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };
        let claims = TokenClaims::new(
            "shardline-server",
            "ci-user",
            TokenScope::Write,
            repository,
            2_000_000_000,
        );
        assert!(claims.is_ok());
        let Ok(claims) = claims else {
            return;
        };

        let serialized = serde_json::to_vec(&claims);
        assert!(serialized.is_ok());
        let Ok(serialized) = serialized else {
            return;
        };
        let deserialized: Result<TokenClaims, _> = serde_json::from_slice(&serialized);
        assert!(deserialized.is_ok());
        let Ok(deserialized) = deserialized else {
            return;
        };

        assert_eq!(deserialized, claims);
        assert_eq!(deserialized.issuer(), "shardline-server");
        assert_eq!(deserialized.subject(), "ci-user");
        assert_eq!(deserialized.scope(), TokenScope::Write);
        assert_eq!(deserialized.expires_at_unix_seconds(), 2_000_000_000);
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
        let signer = TokenSigner::new(b"test-signing-key-32-bytes-long!!");
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
        let signer = TokenSigner::new(b"test-signing-key-32-bytes-long!!");
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
        let signer = TokenSigner::new(b"test-signing-key-32-bytes-long!!");
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
        let signer = TokenSigner::new(b"test-signing-key-32-bytes-long!!");
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
        let signer = TokenSigner::new(b"test-signing-key-32-bytes-long!!");
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

    #[test]
    fn token_claims_error_display_all_variants() {
        let cases: &[(TokenClaimsError, &str)] = &[
            (TokenClaimsError::EmptyIssuer, "empty"),
            (TokenClaimsError::EmptySubject, "empty"),
            (TokenClaimsError::EmptyRepositoryOwner, "empty"),
            (TokenClaimsError::EmptyRepositoryName, "empty"),
            (TokenClaimsError::EmptyRevision, "empty"),
            (TokenClaimsError::ControlCharacter, "control"),
            (TokenClaimsError::TooLong, "length"),
        ];
        for (error, substring) in cases {
            let msg = error.to_string();
            assert!(!msg.is_empty(), "empty display for {error:?}");
            assert!(
                msg.contains(substring),
                "expected '{substring}' in '{msg}' from {error:?}"
            );
        }
    }

    #[test]
    fn token_codec_error_display_variants() {
        let msg = TokenCodecError::EmptySigningKey("test".to_owned()).to_string();
        assert!(!msg.is_empty());
        assert!(msg.contains("empty"));

        let msg = TokenCodecError::SigningKeyTooShort { actual_bytes: 4 }.to_string();
        assert!(!msg.is_empty());
        assert!(msg.contains("4"));

        let msg = TokenCodecError::InvalidFormat.to_string();
        assert!(!msg.is_empty());
        assert!(msg.contains("format"));

        let msg = TokenCodecError::InvalidSignature.to_string();
        assert!(!msg.is_empty());
        assert!(msg.contains("signature"));

        let msg = TokenCodecError::Expired.to_string();
        assert!(!msg.is_empty());
        assert!(msg.contains("expired"));

        let json_err = serde_json::from_str::<serde_json::Value>("bad").unwrap_err();
        let msg = TokenCodecError::Json(json_err).to_string();
        assert!(!msg.is_empty());
        assert!(msg.contains("json"));

        let msg = TokenCodecError::InvalidHex(hex::FromHexError::InvalidStringLength).to_string();
        assert!(!msg.is_empty());
        assert!(msg.contains("hex") || !msg.is_empty());
    }

    #[test]
    fn token_claims_reject_empty_issuer() {
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(repository.is_ok());
        let Ok(repository) = repository else {
            return;
        };
        let claims = TokenClaims::new("", "subject", TokenScope::Read, repository, 42);

        assert_eq!(claims, Err(TokenClaimsError::EmptyIssuer));
    }

    #[test]
    fn token_signer_rejects_short_key() {
        let short_key = [0u8; 4];
        let signer = TokenSigner::new(&short_key);
        assert!(matches!(
            signer,
            Err(TokenCodecError::SigningKeyTooShort { actual_bytes: 4 })
        ));
    }

    #[test]
    fn token_signer_rejects_empty_key() {
        let signer = TokenSigner::new(&[]);
        assert!(matches!(signer, Err(TokenCodecError::EmptySigningKey(_))));
    }

    #[test]
    fn repository_provider_as_str_returns_expected_values() {
        assert_eq!(RepositoryProvider::GitHub.as_str(), "github");
        assert_eq!(RepositoryProvider::Gitea.as_str(), "gitea");
        assert_eq!(RepositoryProvider::GitLab.as_str(), "gitlab");
        assert_eq!(RepositoryProvider::Codeberg.as_str(), "codeberg");
        assert_eq!(RepositoryProvider::Generic.as_str(), "generic");
    }

    #[test]
    fn repository_provider_parse_error_display() {
        let msg = RepositoryProviderParseError.to_string();
        assert!(!msg.is_empty());
        assert!(msg.contains("provider"));
    }

    // ── TokenClaims field accessors ──────────────────────────────────────

    #[test]
    fn token_claims_field_accessors() {
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))
                .unwrap();
        let claims =
            TokenClaims::new("issuer", "subject", TokenScope::Write, repository, 100).unwrap();
        assert_eq!(claims.issuer(), "issuer");
        assert_eq!(claims.subject(), "subject");
        assert_eq!(claims.scope(), TokenScope::Write);
        assert_eq!(claims.expires_at_unix_seconds(), 100);
        assert_eq!(claims.repository().owner(), "team");
    }

    // ── TokenScope exhaustive ────────────────────────────────────────────

    #[test]
    fn token_scope_read_allows_read_not_write() {
        assert!(TokenScope::Read.allows_read());
        assert!(!TokenScope::Read.allows_write());
    }

    #[test]
    fn token_scope_write_allows_both() {
        assert!(TokenScope::Write.allows_read());
        assert!(TokenScope::Write.allows_write());
    }

    // ── TokenClaims::new validation ──────────────────────────────────────

    #[test]
    fn token_claims_rejects_control_characters_in_issuer() {
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))
                .unwrap();
        let claims = TokenClaims::new("issuer\x00", "subject", TokenScope::Read, repository, 100);
        assert_eq!(claims, Err(TokenClaimsError::ControlCharacter));
    }

    #[test]
    fn token_claims_rejects_control_characters_in_subject() {
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))
                .unwrap();
        let claims = TokenClaims::new("issuer", "sub\nject", TokenScope::Read, repository, 100);
        assert_eq!(claims, Err(TokenClaimsError::ControlCharacter));
    }

    #[test]
    fn token_claims_rejects_oversized_issuer() {
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))
                .unwrap();
        let oversized = "i".repeat(MAX_TOKEN_COMPONENT_BYTES + 1);
        let claims = TokenClaims::new(&oversized, "subject", TokenScope::Read, repository, 100);
        assert_eq!(claims, Err(TokenClaimsError::TooLong));
    }

    // ── TokenSigner verification edge cases ──────────────────────────────

    #[test]
    fn token_signer_verify_rejects_empty_token() {
        let signer = TokenSigner::new(b"test-signing-key-32-bytes-long!!").unwrap();
        let result = signer.verify_at("", 100);
        assert!(matches!(result, Err(TokenCodecError::InvalidFormat)));
    }

    #[test]
    fn token_signer_verify_rejects_token_without_dot() {
        let signer = TokenSigner::new(b"test-signing-key-32-bytes-long!!").unwrap();
        let result = signer.verify_at("justhexwithoutdot", 100);
        assert!(matches!(result, Err(TokenCodecError::InvalidFormat)));
    }

    #[test]
    fn split_token_rejects_empty_signature_and_extra_separator() {
        assert!(matches!(
            split_token("aa."),
            Err(TokenCodecError::InvalidFormat)
        ));
        assert!(matches!(
            split_token("aa.bb.cc"),
            Err(TokenCodecError::InvalidFormat)
        ));
    }

    #[test]
    fn format_signed_token_enforces_shared_length_limit() {
        let maximum_payload_bytes = (MAX_TOKEN_STRING_BYTES - 1 - 2) / 2;
        let token = format_signed_token(&vec![1_u8; maximum_payload_bytes], &[2_u8]);
        assert!(token.is_ok());
        assert!(token.unwrap().len() <= MAX_TOKEN_STRING_BYTES);

        let oversized = format_signed_token(&vec![1_u8; maximum_payload_bytes + 1], &[2_u8]);
        assert!(matches!(oversized, Err(TokenCodecError::InvalidFormat)));
    }

    #[test]
    fn format_signed_token_rejects_empty_segments() {
        assert!(matches!(
            format_signed_token(&[], &[1_u8]),
            Err(TokenCodecError::InvalidFormat)
        ));
        assert!(matches!(
            format_signed_token(&[1_u8], &[]),
            Err(TokenCodecError::InvalidFormat)
        ));
    }

    #[test]
    fn token_signer_verify_rejects_too_long_token() {
        let signer = TokenSigner::new(b"test-signing-key-32-bytes-long!!").unwrap();
        let long_token = format!(
            "{}.{}",
            "a".repeat(MAX_TOKEN_PAYLOAD_HEX_BYTES),
            "b".repeat(TOKEN_SIGNATURE_HEX_BYTES + 1)
        );
        let result = signer.verify_at(&long_token, 100);
        assert!(matches!(result, Err(TokenCodecError::InvalidFormat)));
    }

    #[test]
    fn token_signer_verify_rejects_invalid_hex_payload() {
        let signer = TokenSigner::new(b"test-signing-key-32-bytes-long!!").unwrap();
        let result = signer.verify_at(
            "zzzz.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            100,
        );
        assert!(matches!(result, Err(TokenCodecError::InvalidHex(_))));
    }

    #[test]
    fn token_signer_sign_and_verify_now() {
        // verify_now uses the current wall clock — sign with a future expiry
        let signer = TokenSigner::new(b"test-signing-key-32-bytes-long!!").unwrap();
        let repository =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))
                .unwrap();
        let far_future = 2_000_000_000; // well past 2025
        let claims = TokenClaims::new(
            "issuer",
            "subject",
            TokenScope::Read,
            repository,
            far_future,
        )
        .unwrap();
        // verify_now should succeed since expiration is far in the future
        // (the current unix timestamp is around 1.7-1.8 billion as of 2025)
        let token = signer.sign(&claims).unwrap();
        let verified = signer.verify_now(&token);
        assert!(verified.is_ok(), "verify_now failed: {:?}", verified.err());
    }

    // ── RepositoryProvider as_str exhaustive ─────────────────────────────

    #[test]
    fn repository_provider_as_str_all_variants() {
        assert_eq!(RepositoryProvider::GitHub.as_str(), "github");
        assert_eq!(RepositoryProvider::Gitea.as_str(), "gitea");
        assert_eq!(RepositoryProvider::GitLab.as_str(), "gitlab");
        assert_eq!(RepositoryProvider::Codeberg.as_str(), "codeberg");
        assert_eq!(RepositoryProvider::Generic.as_str(), "generic");
    }

    // ── RepositoryScope accessors ────────────────────────────────────────

    #[test]
    fn repository_scope_accessors() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitLab, "group", "project", Some("dev"))
                .unwrap();
        assert_eq!(scope.provider(), RepositoryProvider::GitLab);
        assert_eq!(scope.owner(), "group");
        assert_eq!(scope.name(), "project");
        assert_eq!(scope.revision(), Some("dev"));
    }

    #[test]
    fn repository_scope_allows_missing_revision() {
        let scope = RepositoryScope::new(RepositoryProvider::Gitea, "owner", "repo", None).unwrap();
        assert_eq!(scope.revision(), None);
    }

    // ── RepositoryProvider parse error ───────────────────────────────────

    #[test]
    fn repository_provider_parse_error_debug_non_empty() {
        let debug = format!("{:?}", RepositoryProviderParseError);
        assert!(!debug.is_empty());
    }

    // ── TokenCodecError derivation ───────────────────────────────────────

    #[test]
    fn token_codec_error_from_json_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let err = TokenCodecError::Json(json_err);
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn token_codec_error_from_hex_error() {
        let hex_err = hex::FromHexError::InvalidStringLength;
        let err = TokenCodecError::InvalidHex(hex_err);
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn token_codec_error_empty_signing_key_display() {
        let msg = TokenCodecError::EmptySigningKey("test".to_owned()).to_string();
        assert!(msg.contains("empty"));
    }

    #[test]
    fn token_codec_error_signing_key_too_short_display() {
        let msg = TokenCodecError::SigningKeyTooShort { actual_bytes: 4 }.to_string();
        assert!(msg.contains("4"));
        assert!(msg.contains("32"));
    }

    #[test]
    fn token_codec_error_claims_display() {
        let claims_err = TokenClaimsError::ControlCharacter;
        let err = TokenCodecError::Claims(claims_err);
        let msg = err.to_string();
        assert!(
            msg.contains("invalid"),
            "expected 'invalid' in Claims display, got: {msg}"
        );
    }
}
