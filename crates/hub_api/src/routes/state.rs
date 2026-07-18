use crate::auth::HubAuth;
use shardline_index::hub::BoxedHubStore;

/// Shared Hub API state.
#[derive(Clone)]
pub struct HubState {
    pub store: BoxedHubStore,
    pub auth: Option<HubAuth>,
    /// Optional HTTP client for webhook delivery.
    pub http_client: Option<reqwest::Client>,
}

impl std::fmt::Debug for HubState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HubState")
            .field("auth", &self.auth.is_some())
            .finish()
    }
}
