//! Authorize an explicit batch ref before issuing storage capabilities.
use super::HubState;
use crate::{
    error::HubApiError,
    models::{LfsBatchOperation, LfsBatchRequest},
};
use shardline_server_core::AuthorizedRepository;

pub(super) async fn authorize(
    state: &HubState,
    capability: &AuthorizedRepository,
    request: &LfsBatchRequest,
) -> Result<(), HubApiError> {
    let Some(scope) = capability.repository() else {
        return Ok(());
    };
    // An omitted ref uses the credential's existing storage scope. Do not
    // invent a default branch or widen the token for legacy LFS clients.
    if scope.revision().is_none() || request.ref_.name.is_empty() {
        return Ok(());
    }
    let repo = format!("{}/{}", scope.owner(), scope.name());
    if request.operation != LfsBatchOperation::Download {
        return crate::revision_access::authorize_write(&repo, &request.ref_.name, capability);
    }
    // The ordinary read policy also accepts the exact scoped ref immediately.
    // Avoid identity/object lookups for the common same-ref batch.
    if scope.revision() == Some(request.ref_.name.as_str()) {
        return Ok(());
    }
    // Reuse the read policy, including immutable ancestors. A download is not
    // a write and must not reject authorized historical checkout requests.
    let permit = crate::git::smart_http::IDENTITY_ADMISSION
        .acquire()
        .await
        .map_err(|error| HubApiError::CasError(error.to_string()))?;
    let state_for_lookup = state.clone();
    let repo_for_lookup = repo.clone();
    let requested = request.ref_.name.clone();
    let auth = capability.clone();
    let resolved = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        crate::git::smart_http::resolve_commit_id(
            &state_for_lookup,
            &repo_for_lookup,
            &requested,
            &auth,
        )
    })
    .await
    .map_err(|error| HubApiError::CasError(error.to_string()))??;
    let commit = resolved.as_deref().unwrap_or(&request.ref_.name);
    crate::revision_access::authorize_read(state, &repo, &request.ref_.name, commit, capability)
        .await
}
