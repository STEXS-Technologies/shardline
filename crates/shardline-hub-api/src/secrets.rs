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
    let _report = upgrade_webhook_secrets_with_report(store, cipher);
}

/// Outcome of an upgrade scan. Failures leave lazy per-row upgrade available.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WebhookSecretUpgradeReport {
    /// Rows inspected, including rows that need no upgrade.
    pub inspected: u64,
    /// Plaintext secrets successfully replaced by ciphertext.
    pub upgraded: u64,
    /// Failed reads, encryption attempts or writes; an incomplete scan is not success.
    pub failures: u64,
}

/// Runs the same best-effort sweep and reports whether it completed without errors.
/// Secret values are never included in the report or logs.
pub fn upgrade_webhook_secrets_with_report(
    store: &shardline_index::hub::BoxedHubStore,
    cipher: &WebhookSecretCipher,
) -> WebhookSecretUpgradeReport {
    let mut report = WebhookSecretUpgradeReport::default();
    let repos = match store.list_repos() {
        Ok(repos) => repos,
        Err(error) => {
            tracing::warn!("failed to list repositories for webhook-secret upgrade: {error}");
            report.failures = 1;
            return report;
        }
    };
    for repo in repos {
        let webhooks = match store.list_webhooks(&repo.repo_id) {
            Ok(webhooks) => webhooks,
            Err(error) => {
                report.failures = report.failures.saturating_add(1);
                tracing::warn!("failed to list webhooks for secret upgrade: {error}");
                continue;
            }
        };
        for webhook in webhooks {
            report.inspected = report.inspected.saturating_add(1);
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
                    match store.update_webhook_secret(
                        &webhook.repo_id,
                        &webhook.id,
                        Some(&encrypted),
                    ) {
                        Ok(()) => report.upgraded = report.upgraded.saturating_add(1),
                        Err(e) => {
                            report.failures = report.failures.saturating_add(1);
                            tracing::warn!("failed to upgrade webhook secret {}: {e}", webhook.id);
                        }
                    }
                }
                Err(e) => {
                    report.failures = report.failures.saturating_add(1);
                    tracing::warn!("failed to encrypt webhook secret {}: {e}", webhook.id);
                }
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_index::hub::{BoxedHubStore, HubRepoType};
    use shardline_protocol::SecretBytes;

    const TEST_KEY: [u8; 32] = *b"0123456789abcdef0123456789abcdef";

    fn encrypted_store() -> (tempfile::TempDir, BoxedHubStore, WebhookSecretCipher) {
        let ts = tempfile::tempdir().expect("temp dir");
        shardline_index::hub::ensure_hub_tables(ts.path()).expect("hub tables");
        let store = shardline_index::LocalIndexStore::open(ts.path().to_path_buf());
        let boxed = BoxedHubStore::from_store(store);
        boxed
            .create_repo(HubRepoType::Model, "org/sweep", false)
            .expect("create repo");
        let cipher =
            WebhookSecretCipher::new(SecretBytes::new(TEST_KEY.to_vec())).expect("valid key");
        (ts, boxed, cipher)
    }

    #[test]
    fn upgrade_webhook_secrets_encrypts_legacy_rows() {
        let (_ts, store, cipher) = encrypted_store();
        store
            .create_webhook(
                "org/sweep",
                "https://example.com/hook",
                &["push".to_owned()],
                Some("legacy-plain"),
            )
            .expect("seed legacy webhook");

        let report = upgrade_webhook_secrets_with_report(&store, &cipher);
        assert_eq!(
            report,
            WebhookSecretUpgradeReport {
                inspected: 1,
                upgraded: 1,
                failures: 0
            }
        );
        let repeated = upgrade_webhook_secrets_with_report(&store, &cipher);
        assert_eq!(
            repeated,
            WebhookSecretUpgradeReport {
                inspected: 1,
                upgraded: 0,
                failures: 0
            }
        );

        let webhooks = store.list_webhooks("org/sweep").expect("list webhooks");
        assert_eq!(webhooks.len(), 1);
        let stored = webhooks[0].secret.as_ref().expect("secret present");
        assert!(
            stored.expose_secret().starts_with(MAGIC_PREFIX),
            "legacy plaintext must be re-encrypted at rest"
        );
        let decrypted = cipher
            .decrypt("org/sweep", stored.expose_secret())
            .expect("decrypt upgraded secret");
        assert!(!decrypted.needs_upgrade);
        assert_eq!(decrypted.secret.expose_secret(), "legacy-plain");
    }

    #[test]
    fn upgrade_webhook_secrets_skips_ciphertext_rows() {
        let (_ts, store, cipher) = encrypted_store();
        let encrypted = cipher
            .encrypt("org/sweep", "already-encrypted")
            .expect("encrypt");
        store
            .create_webhook(
                "org/sweep",
                "https://example.com/hook",
                &["push".to_owned()],
                Some(&encrypted),
            )
            .expect("seed ciphertext webhook");

        upgrade_webhook_secrets(&store, &cipher);

        let webhooks = store.list_webhooks("org/sweep").expect("list webhooks");
        assert_eq!(
            webhooks[0]
                .secret
                .as_ref()
                .expect("secret present")
                .expose_secret(),
            &encrypted,
            "already-encrypted rows must be left untouched"
        );
    }

    #[test]
    fn upgrade_webhook_secrets_skips_rows_without_secret() {
        let (_ts, store, cipher) = encrypted_store();
        store
            .create_webhook(
                "org/sweep",
                "https://example.com/hook",
                &["push".to_owned()],
                None,
            )
            .expect("seed secretless webhook");

        upgrade_webhook_secrets(&store, &cipher);

        let webhooks = store.list_webhooks("org/sweep").expect("list webhooks");
        assert!(webhooks[0].secret.is_none());
    }

    #[test]
    fn upgrade_webhook_secrets_empty_store_is_a_noop() {
        let (_ts, store, cipher) = encrypted_store();
        // No webhooks at all: the sweep must complete without error or panic.
        upgrade_webhook_secrets(&store, &cipher);
        assert_eq!(store.list_repos().expect("list repos").len(), 1);
    }
}
