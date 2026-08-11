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
const MAGIC_PREFIX: &str = "sse1:";
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
    /// Values with the `sse1:` prefix are decrypted. Values without it are
    /// treated as legacy plaintext (used as-is, with `needs_upgrade` set) so
    /// deployments can enable encryption without a disruptive backfill.
    ///
    /// # Errors
    ///
    /// Returns an error when the stored value is malformed or fails AEAD
    /// authentication (wrong key or tampered ciphertext).
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
        let blob = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(WebhookSecretCipherError::BadFormat)?;
        if blob.len() < NONCE_LEN {
            return Err(WebhookSecretCipherError::BadFormat(
                base64::DecodeError::InvalidLength(NONCE_LEN),
            ));
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
        let secret = String::from_utf8(plain)
            .ok()
            .ok_or(WebhookSecretCipherError::NotUtf8)?;
        Ok(DecryptedSecret {
            secret: SecretString::new(secret),
            needs_upgrade: false,
        })
    }
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
            if stored.expose_secret().starts_with(MAGIC_PREFIX) {
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
    fn malformed_ciphertext_is_rejected() {
        let cipher = WebhookSecretCipher::new(test_key()).unwrap();
        assert!(matches!(
            cipher.decrypt("org/model", "sse1:!!!not-base64!!!"),
            Err(WebhookSecretCipherError::BadFormat(_))
        ));
        assert!(matches!(
            cipher.decrypt("org/model", "sse1:"),
            Err(WebhookSecretCipherError::BadFormat(_))
        ));
    }
}
