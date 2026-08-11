//! At-rest encryption for Hub webhook signing secrets.
//!
//! When a `SHARDLINE_HUB_WEBHOOK_SECRET_KEY` is configured, webhook secrets are
//! stored encrypted using AES-256-GCM. Each row carries its own random 12-byte
//! nonce and is bound to its repository via associated authenticated data
//! (AAD) equal to the `repo_id`, so a ciphertext cannot be transplanted to
//! another repository's webhook.
//!
//! The on-disk format is a magic prefix followed by
//! `base64(nonce || ciphertext)`:
//!
//! ```text
//! sse1:<base64>
//! ```
//!
//! Values without the `sse1:` prefix are treated as legacy plaintext and are
//! transparently upgraded (re-encrypted in place) when a key is configured.

use aes_gcm::aead::generic_array::{
    GenericArray,
    typenum::consts::{U12, U32},
};
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit};
use base64::Engine;
use shardline_protocol::{SecretBytes, SecretString};
use thiserror::Error;

/// Magic prefix distinguishing at-rest encrypted webhook secrets.
pub(crate) const MAGIC_PREFIX: &str = "sse1:";
/// AES-GCM nonce length in bytes.
const NONCE_LEN: usize = 12;
/// AES-256 key length in bytes.
const KEY_LEN: usize = 32;

/// Errors arising from webhook secret encryption or decryption.
#[derive(Debug, Error)]
pub enum WebhookSecretCipherError {
    /// The configured key is not a valid AES-256 key length.
    #[error("webhook secret encryption key must be exactly {expected} bytes, got {observed}")]
    InvalidKeyLength {
        /// Required key length in bytes.
        expected: usize,
        /// Observed key length in bytes.
        observed: usize,
    },
    /// Random nonce generation failed.
    #[error("failed to generate nonce for webhook secret encryption")]
    Nonce(String),
    /// AES-GCM encryption failed.
    #[error("failed to encrypt webhook secret")]
    Encrypt(String),
    /// The stored value is not a valid `sse1:`-formatted blob.
    #[error("stored webhook secret is not in the expected encrypted format")]
    BadFormat(#[source] base64::DecodeError),
    /// AES-GCM authentication or decryption failed (wrong key or tampered data).
    #[error("failed to decrypt webhook secret (wrong key or tampered data)")]
    Decrypt(String),
    /// The decrypted secret was not valid UTF-8.
    #[error("decrypted webhook secret is not valid UTF-8")]
    NotUtf8,
    /// Stored ciphertext exists but no encryption key is configured.
    #[error("stored webhook secret is encrypted but no encryption key is configured")]
    NoCipherForCiphertext,
}

/// Encrypts and decrypts Hub webhook signing secrets at rest.
///
/// The wrapped key is treated as a secret and never logged or displayed.
#[derive(Clone)]
pub struct WebhookSecretCipher {
    key: SecretBytes,
}

/// The plaintext result of reading a stored webhook secret.
pub struct DecryptedSecret {
    /// The plaintext signing secret.
    pub secret: SecretString,
    /// Whether the stored value was legacy plaintext (and can be upgraded).
    pub needs_upgrade: bool,
}

impl WebhookSecretCipher {
    /// Creates a cipher from a 32-byte AES-256 key.
    ///
    /// # Errors
    ///
    /// Returns [`WebhookSecretCipherError::InvalidKeyLength`] when `key` is not
    /// exactly 32 bytes.
    pub fn new(key: SecretBytes) -> Result<Self, WebhookSecretCipherError> {
        let observed = key.expose_secret().len();
        if observed != KEY_LEN {
            return Err(WebhookSecretCipherError::InvalidKeyLength {
                expected: KEY_LEN,
                observed,
            });
        }
        Ok(Self { key })
    }

    /// Encrypts `plaintext` bound to `repo_id`, returning the `sse1:`-prefixed
    /// string to store.
    ///
    /// # Errors
    ///
    /// Returns an error when nonce generation or AES-GCM encryption fails.
    pub fn encrypt(
        &self,
        repo_id: &str,
        plaintext: &str,
    ) -> Result<String, WebhookSecretCipherError> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        getrandom::fill(&mut nonce_bytes)
            .map_err(|e| WebhookSecretCipherError::Nonce(e.to_string()))?;
        let cipher = Aes256Gcm::new(GenericArray::<u8, U32>::from_slice(
            self.key.expose_secret(),
        ));
        let payload = Payload {
            msg: plaintext.as_bytes(),
            aad: repo_id.as_bytes(),
        };
        let ciphertext = cipher
            .encrypt(GenericArray::<u8, U12>::from_slice(&nonce_bytes), payload)
            .map_err(|e| WebhookSecretCipherError::Encrypt(e.to_string()))?;
        let mut blob = Vec::with_capacity(NONCE_LEN.saturating_add(ciphertext.len()));
        blob.extend_from_slice(&nonce_bytes);
        blob.extend_from_slice(&ciphertext);
        let encoded = base64::engine::general_purpose::STANDARD.encode(&blob);
        Ok(format!("{MAGIC_PREFIX}{encoded}"))
    }

    /// Decrypts a stored webhook secret bound to `repo_id`.
    ///
    /// Classification of a stored value:
    /// - No `sse1:` prefix -> legacy plaintext (used as-is, `needs_upgrade` set)
    ///   so deployments can enable encryption without a disruptive backfill.
    /// - `sse1:` prefix AND the suffix decodes as base64 with at least the nonce
    ///   length -> real at-rest ciphertext; the AEAD secret is decrypted. A
    ///   decrypt failure (wrong key or tampered data) is a loud error.
    /// - `sse1:` prefix but the suffix is not valid base64 (or is too short) ->
    ///   a legacy plaintext secret that merely begins with the magic prefix; it
    ///   is treated as plaintext (`needs_upgrade` set) so the lazy upgrade can
    ///   re-encrypt those bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the stored value is a structurally valid ciphertext
    /// but fails AEAD authentication (wrong key or tampered ciphertext), or the
    /// decrypted secret is not valid UTF-8.
    pub fn decrypt(
        &self,
        repo_id: &str,
        stored: &str,
    ) -> Result<DecryptedSecret, WebhookSecretCipherError> {
        let Some(encoded) = stored.strip_prefix(MAGIC_PREFIX) else {
            return Ok(DecryptedSecret {
                secret: SecretString::from_secret(stored),
                needs_upgrade: true,
            });
        };
        let Ok(blob) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
            // Not valid base64, so this is legacy plaintext that merely begins
            // with the magic prefix.
            return Ok(DecryptedSecret {
                secret: SecretString::from_secret(stored),
                needs_upgrade: true,
            });
        };
        if blob.len() < NONCE_LEN {
            // Decodes as base64 but is too short to carry a nonce + ciphertext.
            return Ok(DecryptedSecret {
                secret: SecretString::from_secret(stored),
                needs_upgrade: true,
            });
        }
        let (nonce_bytes, ciphertext) = blob.split_at(NONCE_LEN);
        let cipher = Aes256Gcm::new(GenericArray::<u8, U32>::from_slice(
            self.key.expose_secret(),
        ));
        let payload = Payload {
            msg: ciphertext,
            aad: repo_id.as_bytes(),
        };
        let plain = cipher
            .decrypt(GenericArray::<u8, U12>::from_slice(nonce_bytes), payload)
            .map_err(|e| WebhookSecretCipherError::Decrypt(e.to_string()))?;
        // Wrap the raw plaintext in `SecretBytes` so the buffer is zeroized on
        // drop regardless of whether UTF-8 conversion succeeds.
        let secret_bytes = SecretBytes::new(plain);
        String::from_utf8(secret_bytes.expose_secret().to_vec()).map_or_else(
            |_| Err(WebhookSecretCipherError::NotUtf8),
            |secret| {
                Ok(DecryptedSecret {
                    secret: SecretString::new(secret),
                    needs_upgrade: false,
                })
            },
        )
    }
}

/// Returns whether `stored` is a well-formed `sse1:` ciphertext blob — that is,
/// the suffix decodes as base64 with at least the nonce length.
///
/// This distinguishes real at-rest ciphertext from legacy plaintext that merely
/// begins with the magic prefix. A legacy value that starts with `sse1:` but is
/// not structurally valid ciphertext must be re-encrypted by the upgrade sweep,
/// never skipped.
pub(crate) fn is_ciphertext(stored: &str) -> bool {
    let Some(encoded) = stored.strip_prefix(MAGIC_PREFIX) else {
        return false;
    };
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .is_ok_and(|blob| blob.len() >= NONCE_LEN)
}

/// App-level data upgrade: sweeps existing webhook rows and re-encrypts any
/// legacy plaintext signing secrets into at-rest ciphertext.
///
/// Runs when a webhook-secret key is configured (e.g. at Hub store init). SQL
/// cannot perform AES-GCM, so this is a Rust data-upgrade step rather than a
/// pure SQL migration. Failures are logged and do not abort startup.
pub fn upgrade_webhook_secrets(
    store: &shardline_index::hub::BoxedHubStore,
    cipher: &WebhookSecretCipher,
) {
    let Ok(repos) = store.list_repos() else {
        return;
    };
    for repo in repos {
        let Ok(webhooks) = store.list_webhooks(&repo.repo_id) else {
            continue;
        };
        for webhook in webhooks {
            let Some(stored) = webhook.secret.as_ref() else {
                continue;
            };
            // Only skip rows whose full structure validates as ciphertext. A
            // legacy value that merely begins with the magic prefix but is not
            // valid ciphertext must be re-encrypted.
            if is_ciphertext(stored.expose_secret()) {
                continue;
            }
            match cipher.encrypt(&webhook.repo_id, stored.expose_secret()) {
                Ok(encrypted) => {
                    if let Err(e) =
                        store.update_webhook_secret(&webhook.repo_id, &webhook.id, Some(&encrypted))
                    {
                        tracing::warn!("failed to upgrade webhook secret {}: {e}", webhook.id);
                    }
                }
                Err(e) => {
                    tracing::warn!("failed to encrypt webhook secret {}: {e}", webhook.id);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> SecretBytes {
        SecretBytes::new(b"0123456789abcdef0123456789abcdef".to_vec())
    }

    #[test]
    fn rejects_non_32_byte_key() {
        let result = WebhookSecretCipher::new(SecretBytes::new(b"short".to_vec()));
        assert!(matches!(
            result,
            Err(WebhookSecretCipherError::InvalidKeyLength { .. })
        ));
    }

    #[test]
    fn accepts_32_byte_key() {
        assert!(WebhookSecretCipher::new(test_key()).is_ok());
    }

    #[test]
    fn encrypt_round_trips() {
        let cipher = WebhookSecretCipher::new(test_key()).unwrap();
        let enc = cipher.encrypt("org/model", "s3cr3t").unwrap();
        assert!(enc.starts_with(MAGIC_PREFIX));
        let dec = cipher.decrypt("org/model", &enc).unwrap();
        assert!(!dec.needs_upgrade);
        assert_eq!(dec.secret.expose_secret(), "s3cr3t");
    }

    #[test]
    fn ciphertext_differs_from_plaintext_and_is_randomized() {
        let cipher = WebhookSecretCipher::new(test_key()).unwrap();
        let a = cipher.encrypt("org/model", "s3cr3t").unwrap();
        let b = cipher.encrypt("org/model", "s3cr3t").unwrap();
        assert_ne!(a, b, "per-row nonces must randomize ciphertext");
        assert!(!a.contains("s3cr3t"));
    }

    #[test]
    fn aad_binds_secret_to_repo() {
        let cipher = WebhookSecretCipher::new(test_key()).unwrap();
        let enc = cipher.encrypt("org/model", "s3cr3t").unwrap();
        assert!(cipher.decrypt("org/model", &enc).is_ok());
        // A ciphertext bound to one repo must not decrypt for another.
        assert!(matches!(
            cipher.decrypt("other/repo", &enc),
            Err(WebhookSecretCipherError::Decrypt(_))
        ));
    }

    #[test]
    fn wrong_key_fails_to_decrypt() {
        let cipher = WebhookSecretCipher::new(test_key()).unwrap();
        let enc = cipher.encrypt("org/model", "s3cr3t").unwrap();
        let wrong = WebhookSecretCipher::new(SecretBytes::new(
            b"abcdef0123456789abcdef0123456789".to_vec(),
        ))
        .unwrap();
        assert!(matches!(
            wrong.decrypt("org/model", &enc),
            Err(WebhookSecretCipherError::Decrypt(_))
        ));
    }

    #[test]
    fn legacy_plaintext_is_detected_for_upgrade() {
        let cipher = WebhookSecretCipher::new(test_key()).unwrap();
        let dec = cipher.decrypt("org/model", "legacy-plaintext").unwrap();
        assert!(dec.needs_upgrade);
        assert_eq!(dec.secret.expose_secret(), "legacy-plaintext");
    }

    #[test]
    fn sse1_prefixed_bad_base64_is_legacy_and_upgradeable() {
        let cipher = WebhookSecretCipher::new(test_key()).unwrap();
        // A legacy secret that merely begins with the magic prefix but whose
        // suffix is not valid base64 must be treated as plaintext and upgraded,
        // never as malformed ciphertext.
        let dec = cipher.decrypt("org/model", "sse1:my-secret").unwrap();
        assert!(dec.needs_upgrade);
        assert_eq!(dec.secret.expose_secret(), "sse1:my-secret");
    }

    #[test]
    fn sse1_prefixed_short_base64_is_legacy() {
        let cipher = WebhookSecretCipher::new(test_key()).unwrap();
        // base64("short") decodes but is shorter than the 12-byte nonce, so it
        // is not structurally valid ciphertext and must be legacy plaintext.
        let encoded = base64::engine::general_purpose::STANDARD.encode("short");
        let stored = format!("{MAGIC_PREFIX}{encoded}");
        let dec = cipher.decrypt("org/model", &stored).unwrap();
        assert!(dec.needs_upgrade);
        assert_eq!(dec.secret.expose_secret(), stored);
    }

    #[test]
    fn is_ciphertext_classifies_full_structure() {
        let cipher = WebhookSecretCipher::new(test_key()).unwrap();
        // Real ciphertext.
        let real = cipher.encrypt("org/model", "s3cr3t").unwrap();
        assert!(is_ciphertext(&real));
        // Legacy plaintext with or without the prefix, and short base64, are
        // not ciphertext.
        assert!(!is_ciphertext("plain"));
        assert!(!is_ciphertext("sse1:my-secret"));
        let short = format!(
            "{MAGIC_PREFIX}{}",
            base64::engine::general_purpose::STANDARD.encode("short")
        );
        assert!(!is_ciphertext(&short));
    }
}
