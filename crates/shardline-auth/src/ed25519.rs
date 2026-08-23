use std::fmt;

use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use shardline_protocol::{
    TokenClaims, TokenCodecError, decode_and_validate_claims, encode_token_claims,
    format_signed_token, split_token, unix_now_seconds_lossy,
};
use zeroize::Zeroizing;

use super::{AuthError, AuthProvider};

/// Ed25519 asymmetric-key authentication provider.
///
/// Signs tokens with the configured private key and verifies tokens with
/// the corresponding public key. Supports both signing+verification mode
/// (with a private key) and verification-only mode (with only a public key).
///
/// Accepts private keys as raw 32-byte seeds, raw 64-byte keypairs, hexadecimal
/// encodings of either raw form, or PKCS#8 PEM. Public keys may be raw 32-byte
/// values, hexadecimal encodings, or SubjectPublicKeyInfo PEM.
#[derive(Clone)]
pub struct Ed25519AuthProvider {
    /// Optional signing key for minting tokens. `None` = verification-only.
    signing_key: Option<SigningKey>,
    /// Verifying key for token verification (derived from signing key or
    /// provided separately).
    verifying_keys: Vec<VerifyingKey>,
}

const MAX_VERIFYING_KEYS: usize = 32;

impl fmt::Debug for Ed25519AuthProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Ed25519AuthProvider")
            .field("signing_key", &"<redacted>")
            .field("verifying_keys", &"<redacted>")
            .finish()
    }
}

impl Ed25519AuthProvider {
    /// Creates a provider that can both sign and verify tokens.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::ProviderError`] when the key bytes cannot be parsed
    /// as a valid Ed25519 private key.
    pub fn new(private_key: &[u8]) -> Result<Self, AuthError> {
        let signing_key = parse_signing_key(private_key)?;
        let verifying_key = signing_key.verifying_key();
        Ok(Self {
            signing_key: Some(signing_key),
            verifying_keys: vec![verifying_key],
        })
    }

    /// Creates a signing provider that also accepts tokens signed by an
    /// overlapping public-key ring during rotation.
    ///
    /// The key ring accepts every existing single-key representation plus a
    /// newline-delimited list of hexadecimal public keys. Duplicate keys are
    /// removed and the active signing key is always included.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::ProviderError`] when either key input is invalid
    /// or the bounded verification-key count is exceeded.
    pub fn new_with_public_keyring(
        private_key: &[u8],
        public_keyring: &[u8],
    ) -> Result<Self, AuthError> {
        let signing_key = parse_signing_key(private_key)?;
        let mut verifying_keys = vec![signing_key.verifying_key()];
        append_unique_verifying_keys(&mut verifying_keys, parse_verifying_keys(public_keyring)?);
        enforce_verifying_key_limit(verifying_keys.len())?;
        Ok(Self {
            signing_key: Some(signing_key),
            verifying_keys,
        })
    }

    /// Creates a verification-only provider (cannot mint tokens).
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::ProviderError`] when the key bytes cannot be parsed
    /// as a valid Ed25519 public key.
    pub fn with_public_key(public_key: &[u8]) -> Result<Self, AuthError> {
        let verifying_keys = parse_verifying_keys(public_key)?;
        Ok(Self {
            signing_key: None,
            verifying_keys,
        })
    }

    /// Verifies a token against the supplied current Unix timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when the token is invalid, expired, or otherwise
    /// unverifiable.
    pub fn verify_at(
        &self,
        token: &str,
        current_unix_seconds: u64,
    ) -> Result<TokenClaims, AuthError> {
        let (payload_hex, signature_hex) =
            split_token(token).map_err(|_error| AuthError::InvalidToken)?;
        let payload = hex::decode(payload_hex).map_err(|_error| AuthError::InvalidToken)?;
        let signature_bytes =
            hex::decode(signature_hex).map_err(|_error| AuthError::InvalidToken)?;
        let signature =
            Signature::from_slice(&signature_bytes).map_err(|_error| AuthError::InvalidToken)?;
        if !self
            .verifying_keys
            .iter()
            .any(|key| key.verify_strict(&payload, &signature).is_ok())
        {
            return Err(AuthError::InvalidToken);
        }
        decode_and_validate_claims(&payload, current_unix_seconds).map_err(|e| match e {
            TokenCodecError::Expired => AuthError::ExpiredToken,
            TokenCodecError::EmptySigningKey(_)
            | TokenCodecError::SigningKeyTooShort { .. }
            | TokenCodecError::Json(_)
            | TokenCodecError::InvalidFormat
            | TokenCodecError::InvalidHex(_)
            | TokenCodecError::InvalidSignature
            | TokenCodecError::Claims(_) => AuthError::InvalidToken,
        })
    }
}

fn parse_verifying_keys(bytes: &[u8]) -> Result<Vec<VerifyingKey>, AuthError> {
    let single_error = match parse_verifying_key(bytes) {
        Ok(key) => return Ok(vec![key]),
        Err(error) => error,
    };
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Err(single_error);
    };
    let encoded_keys: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    if encoded_keys.is_empty()
        || encoded_keys
            .iter()
            .any(|line| line.len() != 64 || !line.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return Err(single_error);
    }
    enforce_verifying_key_limit(encoded_keys.len())?;
    let mut keys = Vec::with_capacity(encoded_keys.len());
    for encoded in encoded_keys {
        let decoded = hex::decode(encoded).map_err(|error| {
            AuthError::ProviderError(format!("invalid Ed25519 hexadecimal key: {error}"))
        })?;
        let key = parse_raw_verifying_key(&decoded)?;
        if key.is_weak() {
            return Err(AuthError::ProviderError(
                "Ed25519 public key must not be a weak small-order point".to_owned(),
            ));
        }
        if !keys.iter().any(|existing: &VerifyingKey| existing == &key) {
            keys.push(key);
        }
    }
    Ok(keys)
}

fn append_unique_verifying_keys(
    destination: &mut Vec<VerifyingKey>,
    additional: Vec<VerifyingKey>,
) {
    for key in additional {
        if !destination.iter().any(|existing| existing == &key) {
            destination.push(key);
        }
    }
}

fn enforce_verifying_key_limit(count: usize) -> Result<(), AuthError> {
    if count > MAX_VERIFYING_KEYS {
        return Err(AuthError::ProviderError(format!(
            "Ed25519 public key ring exceeds the {MAX_VERIFYING_KEYS}-key limit"
        )));
    }
    Ok(())
}

impl AuthProvider for Ed25519AuthProvider {
    fn verify_token(&self, token: &str) -> Result<TokenClaims, AuthError> {
        let now = unix_now_seconds_lossy();
        self.verify_at(token, now)
    }

    fn mint_token(&self, claims: &TokenClaims) -> Result<String, AuthError> {
        let Some(ref signing_key) = self.signing_key else {
            return Err(AuthError::ProviderError(
                "Ed25519 provider is in verification-only mode; cannot mint tokens".to_owned(),
            ));
        };
        let payload = encode_token_claims(claims)
            .map_err(|e| AuthError::ProviderError(format!("token serialization failed: {e}")))?;
        let signature = signing_key.sign(&payload);
        format_signed_token(&payload, &signature.to_bytes())
            .map_err(|e| AuthError::ProviderError(format!("token formatting failed: {e}")))
    }
}

/// Parses an Ed25519 signing key from raw bytes or PEM.
fn parse_signing_key(bytes: &[u8]) -> Result<SigningKey, AuthError> {
    if let Some(pem_str) = pem_text(bytes) {
        return SigningKey::from_pkcs8_pem(pem_str).map_err(|error| {
            AuthError::ProviderError(format!("invalid Ed25519 private key PEM: {error}"))
        });
    }
    if let Some(decoded) = decode_hex_key(bytes, &[64, 128])? {
        return parse_raw_signing_key(&decoded);
    }
    parse_raw_signing_key(bytes)
}

fn parse_raw_signing_key(bytes: &[u8]) -> Result<SigningKey, AuthError> {
    match bytes.len() {
        32 => {
            let seed: [u8; 32] = bytes.try_into().map_err(|_error| {
                AuthError::ProviderError("failed to convert 32-byte Ed25519 seed".to_owned())
            })?;
            Ok(SigningKey::from_bytes(&seed))
        }
        64 => {
            let keypair: [u8; 64] = bytes.try_into().map_err(|_error| {
                AuthError::ProviderError("failed to convert 64-byte Ed25519 keypair".to_owned())
            })?;
            SigningKey::from_keypair_bytes(&keypair).map_err(|error| {
                AuthError::ProviderError(format!("invalid Ed25519 keypair bytes: {error}"))
            })
        }
        other => Err(AuthError::ProviderError(format!(
            "Ed25519 private key must be a 32-byte seed, 64-byte keypair, hexadecimal encoding, or PKCS#8 PEM; got {other} bytes",
        ))),
    }
}

/// Parses an Ed25519 verifying key from raw bytes or PEM.
fn parse_verifying_key(bytes: &[u8]) -> Result<VerifyingKey, AuthError> {
    let verifying_key = if let Some(pem_str) = pem_text(bytes) {
        VerifyingKey::from_public_key_pem(pem_str).map_err(|error| {
            AuthError::ProviderError(format!("invalid Ed25519 public key PEM: {error}"))
        })?
    } else if let Some(decoded) = decode_hex_key(bytes, &[64])? {
        parse_raw_verifying_key(&decoded)?
    } else {
        parse_raw_verifying_key(bytes)?
    };
    if verifying_key.is_weak() {
        return Err(AuthError::ProviderError(
            "Ed25519 public key must not be a weak small-order point".to_owned(),
        ));
    }
    Ok(verifying_key)
}

fn parse_raw_verifying_key(bytes: &[u8]) -> Result<VerifyingKey, AuthError> {
    if bytes.len() == 32 {
        let point: [u8; 32] = bytes.try_into().map_err(|_error| {
            AuthError::ProviderError("failed to convert 32-byte Ed25519 public key".to_owned())
        })?;
        VerifyingKey::from_bytes(&point).map_err(|error| {
            AuthError::ProviderError(format!("invalid Ed25519 public key bytes: {error}"))
        })
    } else {
        Err(AuthError::ProviderError(format!(
            "Ed25519 public key must be 32 bytes, a hexadecimal encoding, or SubjectPublicKeyInfo PEM; got {} bytes",
            bytes.len(),
        )))
    }
}

fn pem_text(bytes: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(bytes).ok()?.trim();
    text.starts_with("-----BEGIN").then_some(text)
}

fn decode_hex_key(
    bytes: &[u8],
    encoded_lengths: &[usize],
) -> Result<Option<Zeroizing<Vec<u8>>>, AuthError> {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Ok(None);
    };
    let text = text.trim();
    if !encoded_lengths.contains(&text.len()) || !text.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Ok(None);
    }
    hex::decode(text)
        .map(Zeroizing::new)
        .map(Some)
        .map_err(|error| {
            AuthError::ProviderError(format!("invalid Ed25519 hexadecimal key: {error}"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenScope, TokenSigner};

    fn test_claims() -> TokenClaims {
        let repo = RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", None)
            .expect("valid repo scope");
        TokenClaims::new(
            "test-issuer",
            "test-subject",
            TokenScope::Write,
            repo,
            2_000_000_000,
        )
        .expect("valid claims")
    }

    fn test_seed() -> [u8; 32] {
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ]);
        seed
    }

    #[test]
    fn debug_redacts_key_material() {
        let seed = test_seed();
        let provider = Ed25519AuthProvider::new(&seed).expect("valid provider");
        let rendered = format!("{provider:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(rendered.contains("\"<redacted>\""));
    }

    #[test]
    fn round_trip_sign_and_verify() {
        let seed = test_seed();
        let provider = Ed25519AuthProvider::new(&seed).expect("valid provider");
        let claims = test_claims();
        let token = provider.mint_token(&claims).expect("should mint token");
        let verified = provider.verify_token(&token).expect("should verify");
        assert_eq!(verified, claims);
    }

    #[test]
    fn verification_only_mode_cannot_mint() {
        let seed = test_seed();
        let signing_key = SigningKey::from_bytes(&seed);
        let verifying_key = signing_key.verifying_key();
        let pub_bytes = verifying_key.to_bytes();
        let provider = Ed25519AuthProvider::with_public_key(&pub_bytes).expect("valid provider");
        let claims = test_claims();
        let result = provider.mint_token(&claims);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("verification-only")
        );
    }

    #[test]
    fn verify_rejects_tampered_signature() {
        let seed = test_seed();
        let provider = Ed25519AuthProvider::new(&seed).expect("valid provider");
        let claims = test_claims();
        let token = provider.mint_token(&claims).expect("should mint token");
        // Tamper token: flip a byte in the signature
        let mut tampered = String::with_capacity(token.len());
        let dot_pos = token.find('.').unwrap_or(token.len());
        tampered.push_str(&token[..dot_pos + 1]);
        let sig_hex = &token[dot_pos + 1..];
        let mut sig_bytes = hex::decode(sig_hex).expect("valid hex");
        sig_bytes[0] ^= 0x01;
        tampered.push_str(&hex::encode(&sig_bytes));
        let result = provider.verify_token(&tampered);
        assert!(matches!(result, Err(AuthError::InvalidToken)));
    }

    #[test]
    fn verify_rejects_malformed_token() {
        let seed = test_seed();
        let provider = Ed25519AuthProvider::new(&seed).expect("valid provider");
        assert!(matches!(
            provider.verify_token("not-a-token"),
            Err(AuthError::InvalidToken)
        ));
        assert!(matches!(
            provider.verify_token(""),
            Err(AuthError::InvalidToken)
        ));
        assert!(matches!(
            provider.verify_token("abc.def"),
            Err(AuthError::InvalidToken)
        ));
        assert!(matches!(
            provider.verify_token("aa.bb.cc"),
            Err(AuthError::InvalidToken)
        ));
    }

    #[test]
    fn rejects_invalid_key_bytes() {
        assert!(Ed25519AuthProvider::new(b"").is_err());
        assert!(Ed25519AuthProvider::new(b"too short").is_err());
        assert!(Ed25519AuthProvider::with_public_key(b"").is_err());
        assert!(Ed25519AuthProvider::with_public_key(b"too short").is_err());
    }

    #[test]
    fn verify_rejects_expired_token() {
        let seed = test_seed();
        let provider = Ed25519AuthProvider::new(&seed).expect("valid provider");
        let repo = RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", None)
            .expect("valid repo scope");
        // Token that expired in the past
        let expired_claims = TokenClaims::new("issuer", "subject", TokenScope::Read, repo, 1_000)
            .expect("valid claims");
        let token = provider.mint_token(&expired_claims).expect("should mint");
        // Use verify_at with a current time well past the expiry
        let result = provider.verify_at(&token, 2_000);
        assert!(matches!(result, Err(AuthError::ExpiredToken)));
    }

    #[test]
    fn verify_rejects_token_from_different_key() {
        // Provider A with seed A
        let seed_a = test_seed();
        let provider_a = Ed25519AuthProvider::new(&seed_a).expect("valid provider A");
        let claims = test_claims();
        let token = provider_a.mint_token(&claims).expect("should mint token");

        // Provider B with a different seed
        let mut seed_b = [0u8; 32];
        seed_b.copy_from_slice(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 1,
        ]);
        let provider_b = Ed25519AuthProvider::new(&seed_b).expect("valid provider B");
        let result = provider_b.verify_token(&token);
        assert!(matches!(result, Err(AuthError::InvalidToken)));
    }

    #[test]
    fn reject_malformed_pem_key() {
        assert!(
            Ed25519AuthProvider::new(b"-----BEGIN PRIVATE KEY-----\n\n-----END PRIVATE KEY-----")
                .is_err()
        );
        assert!(
            Ed25519AuthProvider::new(
                b"-----BEGIN RSA PRIVATE KEY-----\nABCD\n-----END RSA PRIVATE KEY-----"
            )
            .is_err()
        );
        assert!(
            Ed25519AuthProvider::with_public_key(
                b"-----BEGIN PUBLIC KEY-----\nABCD\n-----END PUBLIC KEY-----"
            )
            .is_err()
        );
    }

    #[test]
    fn accepts_64_byte_keypair() {
        let seed = test_seed();
        let signing_key = SigningKey::from_bytes(&seed);
        let keypair_bytes = signing_key.to_keypair_bytes();
        let provider =
            Ed25519AuthProvider::new(&keypair_bytes).expect("should accept 64-byte keypair");
        let claims = test_claims();
        let token = provider.mint_token(&claims).expect("should mint");
        let verified = provider.verify_token(&token).expect("should verify");
        assert_eq!(verified, claims);
    }

    #[test]
    fn cross_instance_verify_with_public_key_only() {
        // Provider A (has private key) mints a token.
        let seed = test_seed();
        let provider_a = Ed25519AuthProvider::new(&seed).expect("valid provider A");
        let claims = test_claims();
        let token = provider_a.mint_token(&claims).expect("should mint token");

        // Provider B (public key only) verifies the same token.
        let signing_key = SigningKey::from_bytes(&seed);
        let verifying_key = signing_key.verifying_key();
        let pub_bytes = verifying_key.to_bytes();
        let provider_b =
            Ed25519AuthProvider::with_public_key(&pub_bytes).expect("valid provider B");
        let verified = provider_b
            .verify_token(&token)
            .expect("should verify cross-instance");
        assert_eq!(verified, claims);
    }

    #[test]
    fn accept_pem_format_key() {
        let seed = test_seed();
        let signing_key = SigningKey::from_bytes(&seed);
        let pem = signing_key
            .to_pkcs8_pem(Default::default())
            .expect("PEM serialization should succeed");
        let provider = Ed25519AuthProvider::new(pem.as_bytes()).expect("should accept PEM");
        let claims = test_claims();
        let token = provider.mint_token(&claims).expect("should mint");
        let verified = provider.verify_token(&token).expect("should verify");
        assert_eq!(verified, claims);
    }

    #[test]
    fn accepts_hex_private_and_public_keys_with_trailing_newline() {
        let seed = test_seed();
        let signing_key = SigningKey::from_bytes(&seed);
        let private_hex = format!("{}\n", hex::encode(seed));
        let public_hex = format!("{}\n", hex::encode(signing_key.verifying_key().to_bytes()));
        let signer = Ed25519AuthProvider::new(private_hex.as_bytes()).expect("hex private key");
        let verifier =
            Ed25519AuthProvider::with_public_key(public_hex.as_bytes()).expect("hex public key");

        let claims = test_claims();
        let token = signer.mint_token(&claims).expect("mint");
        assert_eq!(verifier.verify_token(&token).expect("verify"), claims);
    }

    #[test]
    fn public_keyring_verifies_overlapping_signers() {
        let old_seed = test_seed();
        let mut new_seed = test_seed();
        new_seed.reverse();
        let old_signer = Ed25519AuthProvider::new(&old_seed).expect("old signer");
        let new_signer = Ed25519AuthProvider::new(&new_seed).expect("new signer");
        let keyring = format!(
            "{}\n{}\n",
            hex::encode(SigningKey::from_bytes(&old_seed).verifying_key().to_bytes()),
            hex::encode(SigningKey::from_bytes(&new_seed).verifying_key().to_bytes())
        );
        let verifier =
            Ed25519AuthProvider::with_public_key(keyring.as_bytes()).expect("public key ring");
        let claims = test_claims();
        let old_token = old_signer.mint_token(&claims).expect("old token");
        let new_token = new_signer.mint_token(&claims).expect("new token");
        assert_eq!(
            verifier.verify_token(&old_token).expect("old verifies"),
            claims
        );
        assert_eq!(
            verifier.verify_token(&new_token).expect("new verifies"),
            claims
        );
    }

    #[test]
    fn signing_provider_accepts_old_key_during_rotation() {
        let old_seed = test_seed();
        let mut new_seed = test_seed();
        new_seed.reverse();
        let old_signer = Ed25519AuthProvider::new(&old_seed).expect("old signer");
        let old_public = hex::encode(SigningKey::from_bytes(&old_seed).verifying_key().to_bytes());
        let rotating =
            Ed25519AuthProvider::new_with_public_keyring(&new_seed, old_public.as_bytes())
                .expect("rotating provider");
        let claims = test_claims();
        let old_token = old_signer.mint_token(&claims).expect("old token");
        let new_token = rotating.mint_token(&claims).expect("new token");
        assert_eq!(
            rotating.verify_token(&old_token).expect("old verifies"),
            claims
        );
        assert_eq!(
            rotating.verify_token(&new_token).expect("new verifies"),
            claims
        );
    }

    #[test]
    fn public_keyring_is_bounded() {
        let keys = (0..=MAX_VERIFYING_KEYS)
            .map(|offset| {
                let mut seed = test_seed();
                seed[0] = u8::try_from(offset + 1).expect("bounded test offset");
                hex::encode(SigningKey::from_bytes(&seed).verifying_key().to_bytes())
            })
            .collect::<Vec<_>>()
            .join("\n");
        let error = Ed25519AuthProvider::with_public_key(keys.as_bytes()).unwrap_err();
        assert!(error.to_string().contains("32-key limit"));
    }

    #[test]
    fn accepts_subject_public_key_info_pem() {
        let signing_key = SigningKey::from_bytes(&test_seed());
        let pem = signing_key
            .verifying_key()
            .to_public_key_pem(Default::default())
            .expect("public PEM");
        let provider =
            Ed25519AuthProvider::with_public_key(pem.as_bytes()).expect("parse public PEM");
        let signer = Ed25519AuthProvider::new(&test_seed()).expect("signer");
        let claims = test_claims();
        let token = signer.mint_token(&claims).expect("mint");
        assert_eq!(provider.verify_token(&token).expect("verify"), claims);
    }

    #[test]
    fn rejects_weak_public_key() {
        let error = Ed25519AuthProvider::with_public_key(&[0_u8; 32]).unwrap_err();
        assert!(error.to_string().contains("weak"));
    }

    #[test]
    fn rejects_hmac_token_algorithm_confusion() {
        let claims = test_claims();
        let hmac = TokenSigner::new(b"0123456789abcdef0123456789abcdef").expect("HMAC signer");
        let token = hmac.sign(&claims).expect("HMAC token");
        let provider = Ed25519AuthProvider::new(&test_seed()).expect("Ed25519 provider");

        assert!(matches!(
            provider.verify_token(&token),
            Err(AuthError::InvalidToken)
        ));
    }
}
