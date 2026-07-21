use axum::http::HeaderMap;

use crate::error::HubApiError;
use shardline_index::hub::HubRepoType;
use shardline_protocol::TokenScope;

use super::HubState;

/// Authorize the request if auth is configured. Returns `Ok(())` when no auth
/// is set (permissive) or when the token satisfies the required scope.
pub(crate) fn authorize(
    state: &HubState,
    headers: &HeaderMap,
    required_scope: TokenScope,
) -> Result<(), HubApiError> {
    if let Some(auth) = &state.auth {
        auth.authorize(headers, required_scope)?;
    }
    Ok(())
}

/// Converts a `HubRepoType` to the API path string.
pub(crate) const fn repo_type_path(rt: HubRepoType) -> &'static str {
    match rt {
        HubRepoType::Model => "models",
        HubRepoType::Dataset => "datasets",
        HubRepoType::Space => "spaces",
    }
}
