use axum::http::HeaderMap;

use crate::error::HubApiError;
use crate::models::RepoType;
use shardline_index::hub::HubRepoType;
use shardline_protocol::TokenScope;
use shardline_server_core::AuthContext;

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

/// Authorize the request and return the verified auth context, when auth is
/// configured.
///
/// Returns `Ok(None)` in permissive mode (no auth configured) and
/// `Ok(Some(AuthContext))` when a valid token was presented. This lets
/// repo-scoped handlers enforce a token→repository binding in addition to the
/// scope check performed by [`authorize`].
///
/// # Errors
///
/// Returns [`HubApiError::Unauthorized`] or [`HubApiError::InvalidToken`] on a
/// missing/invalid token, or [`HubApiError::Forbidden`] on insufficient scope.
pub(crate) fn authorize_with_context(
    state: &HubState,
    headers: &HeaderMap,
    required_scope: TokenScope,
) -> Result<Option<AuthContext>, HubApiError> {
    if let Some(auth) = &state.auth {
        Ok(Some(auth.authorize(headers, required_scope)?))
    } else {
        Ok(None)
    }
}

/// Requires that the authenticated token's repository scope matches the
/// request's `(ns, repo)` URL-path segment.
///
/// This is the hub-api counterpart to the core layer's
/// `validate_oci_repository_scope` / `scope_namespace` binding: a token issued
/// for repo `owner/name` must only ever act on that same repo. Without this
/// binding, any authenticated user could read/write/delete/git-push another
/// tenant's repository using a token scoped only by `scope` (Read/Write).
///
/// In permissive mode (`auth_ctx` is `None`, i.e. no auth is configured) the
/// check is a no-op so development deployments without auth keep working.
///
/// # Errors
///
/// Returns [`HubApiError::Forbidden`] (HTTP 403) when the token's repository
/// does not match the requested `(ns, repo)`.
pub(crate) fn require_repository_binding(
    auth_ctx: Option<&AuthContext>,
    ns: &str,
    repo: &str,
) -> Result<(), HubApiError> {
    let Some(ctx) = auth_ctx else {
        return Ok(());
    };
    let claims_repo = ctx.claims().repository();
    if claims_repo.owner() == ns && claims_repo.name() == repo {
        Ok(())
    } else {
        Err(HubApiError::Forbidden)
    }
}

/// Converts a `HubRepoType` to the API path string.
///
/// Delegates to [`RepoType::as_path_str`] so the plural path segment mapping
/// lives in a single place.
pub(crate) fn repo_type_path(rt: HubRepoType) -> &'static str {
    RepoType::from(rt).as_path_str()
}
