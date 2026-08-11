//! At-rest encryption for Hub webhook signing secrets.
//!
//! This module re-exports the shared AES-256-GCM at-rest envelope from
//! `shardline-server-core` and keeps the Hub-specific upgrade sweep local.
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

use shardline_server_core::at_rest::is_ciphertext;

/// Backward-compatible alias: the shared at-rest cipher as the webhook cipher.
pub use shardline_server_core::at_rest::{
    AtRestCipher as WebhookSecretCipher, CipherError as WebhookSecretCipherError, DecryptedSecret,
    MAGIC_PREFIX,
};

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
