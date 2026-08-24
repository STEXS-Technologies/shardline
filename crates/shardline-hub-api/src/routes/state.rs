use crate::auth::HubAuth;
use crate::secrets::WebhookSecretCipher;
use shardline_index::hub::BoxedHubStore;
use shardline_server_core::ServerObjectStore;

/// Shared Hub API state.
#[derive(Clone)]
pub struct HubState {
    pub store: BoxedHubStore,
    pub object_store: ServerObjectStore,
    pub auth: Option<HubAuth>,
    /// Optional HTTP client for webhook delivery.
    pub http_client: Option<reqwest::Client>,
    /// Optional cipher for at-rest encryption of webhook signing secrets.
    ///
    /// When `None`, webhook secrets are stored as plaintext (and a startup
    /// warning is emitted). When `Some`, secrets are encrypted on write and
    /// decrypted at delivery time.
    pub webhook_secret_cipher: Option<WebhookSecretCipher>,
    /// Public base URL of the shardline server (used for CAS action URLs).
    pub public_base_url: String,
}

impl std::fmt::Debug for HubState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HubState")
            .field("auth", &self.auth.is_some())
            .field("public_base_url", &self.public_base_url)
            .finish()
    }
}
