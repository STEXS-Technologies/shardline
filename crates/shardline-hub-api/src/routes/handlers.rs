use axum::extract::Path;
use axum::http::HeaderMap;
use axum::{Json, extract::State};

use crate::{error::HubApiError, models::*};
use shardline_protocol::TokenScope;

use super::{HubState, authorize_with_context, require_repository_binding};

// ---- Whoami ----

pub(crate) async fn whoami(
    State(state): State<HubState>,
    headers: HeaderMap,
) -> Result<Json<WhoamiResponse>, HubApiError> {
    let name = if let Some(auth) = &state.auth {
        auth.authorize(&headers, TokenScope::Read)?
            .subject()
            .to_owned()
    } else {
        "anonymous".to_owned()
    };
    shardline_metrics::record_hub_api_request("whoami", "GET", 200);
    Ok(Json(WhoamiResponse {
        name: name.clone(),
        is_admin: false,
        user_type: "user".to_owned(),
        auth: WhoamiAuth {
            auth_type: "token".to_owned(),
            identity: WhoamiIdentity {
                account: WhoamiAccount { name },
            },
        },
    }))
}

// ---- Git HEAD reference ----

/// Serves the HEAD reference for a repository.
pub(crate) async fn git_head(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<String, HubApiError> {
    let auth_ctx = authorize_with_context(&state, &headers, TokenScope::Read)?;
    require_repository_binding(auth_ctx.as_ref(), &ns, &repo)?;
    let repo_id = format!("{ns}/{repo}");
    let revisions = state.store.list_revisions(&repo_id).map_err(|e| {
        tracing::debug!("failed to list revisions for {repo_id}: {e}");
        HubApiError::RepoNotFound
    })?;

    // Find HEAD revision — prefer explicit HEAD, then empty, then fall back to latest.
    let head_sha = revisions
        .iter()
        .find(|r| r.ref_name == "HEAD" || r.ref_name.is_empty())
        .or_else(|| revisions.first())
        .map(|r| r.sha.as_str())
        .unwrap_or("0000000000000000000000000000000000000000");

    Ok(format!(
        "ref: refs/heads/main\n{head_sha} refs/heads/main\n"
    ))
}
