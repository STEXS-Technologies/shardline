use axum::extract::Path;
use axum::http::HeaderMap;
use axum::{Json, extract::State};

use crate::{error::HubApiError, models::*};
use shardline_protocol::TokenScope;

use super::{HubRepository, HubState};

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
        orgs: Vec::new(),
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
    capability: HubRepository<false, false>,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<String, HubApiError> {
    let repo = crate::git::smart_http::repository::canonical_name_for_claims(
        &state,
        capability.capability().claims(),
        &ns,
        &repo,
    )
    .await?;
    capability.require_binding(&ns, &repo)?;
    let repo_id = format!("{ns}/{repo}");
    // Immutable history can contain newer commits after an acknowledged ref
    // rollback, or commits on other branches. Resolve the live default ref.
    let head_sha = state
        .store
        .resolve_revision(&repo_id, "main")
        .map_err(|e| {
            tracing::debug!("failed to resolve HEAD for {repo_id}: {e}");
            HubApiError::RepoNotFound
        })?
        .unwrap_or_else(|| "0000000000000000000000000000000000000000".to_owned());

    crate::revision_access::authorize_read(
        &state,
        &repo_id,
        "main",
        &head_sha,
        capability.capability(),
    )
    .await?;

    Ok(format!(
        "ref: refs/heads/main\n{head_sha} refs/heads/main\n"
    ))
}
