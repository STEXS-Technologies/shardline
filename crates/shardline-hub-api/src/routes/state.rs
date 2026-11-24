use crate::auth::HubAuth;
use crate::secrets::WebhookSecretCipher;
use shardline_index::{
    HubRefBatchManifestReferenceV1, HubTreeVersionManifestV1,
    hub::{BoxedHubStore, HubFileEntry, HubRevision},
};
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

impl HubArchiveContext {
    /// Stores a canonical Hub version manifest and returns its receipt locator.
    ///
    /// # Errors
    /// Returns an error if manifest validation, encoding, object storage, or
    /// locator validation fails.
    pub(crate) fn store_revision_manifest(
        &self,
        revision: &HubRevision,
        public_commit_id: &str,
        files: Vec<HubFileEntry>,
    ) -> Result<HubRefBatchManifestReferenceV1, String> {
        let manifest = HubTreeVersionManifestV1::new(
            self.deployment_id.clone(),
            self.resource_domain.clone(),
            revision.clone(),
            public_commit_id.to_owned(),
            files,
        )
        .map_err(|error| error.to_string())?;
        let locator = manifest
            .store(&self.object_store)
            .map_err(|error| error.to_string())?;
        HubRefBatchManifestReferenceV1::new(
            self.deployment_id.clone(),
            revision.repo_id.clone(),
            revision.sha.clone(),
            locator.digest,
            locator.key.as_str().to_owned(),
        )
        .map_err(|error| error.to_string())
    }
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
