use crate::auth::HubAuth;
use crate::secrets::WebhookSecretCipher;
use shardline_index::hub::BoxedHubStore;
use shardline_server_core::ServerObjectStore;

/// Explicit SHADOW archive destination. It is kept separate from the ordinary
/// payload store so recovery artifacts inherit the configured archive policy.
#[derive(Clone)]
pub struct HubArchiveContext {
    /// Stable identity for the deployment that owns this archive namespace.
    pub deployment_id: String,
    /// Stable resource/tenant domain used to scope Hub repository identities.
    pub resource_domain: String,
    /// Dedicated immutable archive destination, separate from the payload store.
    pub object_store: ServerObjectStore,
}

/// Shared Hub API state.
#[derive(Clone)]
pub struct HubState {
    pub store: BoxedHubStore,
    pub object_store: ServerObjectStore,
    /// Present only when the persistent Hub recovery mode is explicitly SHADOW.
    pub recovery_archive: Option<HubArchiveContext>,
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
            .field("recovery_archive", &self.recovery_archive.is_some())
            .field("public_base_url", &self.public_base_url)
            .finish()
    }
}
