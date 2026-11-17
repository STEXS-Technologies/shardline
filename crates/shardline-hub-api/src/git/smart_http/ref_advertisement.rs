//! Ref advertisement for Git Smart HTTP discovery (info/refs).

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use std::fmt::Write;

use super::super::pktline::{self, FLUSH};
use crate::{
    error::HubApiError,
    routes::{HubState, require_repository_binding},
    types::GitSmartHttpService,
};
use shardline_server_core::VerifiedAuthContext;

/// Query parameters for `GET /info/refs`.
#[derive(Debug, Deserialize)]
pub struct InfoRefsQuery {
    pub service: Option<String>,
}

/// Represents a Git reference to advertise.
#[derive(Debug, Clone)]
pub(super) struct GitRef {
    pub(super) name: String,
    pub(super) sha1: String,
}

/// Resolves the repo ID from the URL path components.
pub(super) fn resolve_repo_id(_repo_type: &str, ns: &str, repo: &str) -> String {
    format!("{ns}/{repo}")
}

/// Authorizes reading and returns the verified context for repository binding.
pub(super) fn authorize_read_with_context(
    state: &HubState,
    headers: &HeaderMap,
) -> Result<Option<VerifiedAuthContext>, HubApiError> {
    if let Some(ref auth) = state.auth {
        Ok(Some(auth.authorize(headers, TokenScope::Read)?))
    } else {
        Ok(None)
    }
}

/// Authorizes writing and returns the verified context for repository binding.
pub(super) fn authorize_write_with_context(
    state: &HubState,
    headers: &HeaderMap,
) -> Result<Option<VerifiedAuthContext>, HubApiError> {
    if let Some(ref auth) = state.auth {
        Ok(Some(auth.authorize(headers, TokenScope::Write)?))
    } else {
        Ok(None)
    }
}

use shardline_protocol::TokenScope;

/// Validates a Git refname for receive-pack.
///
/// Requires the `refs/` namespace and rejects Git-reserved spellings, control
/// characters, whitespace, and path traversal components.
pub(crate) fn is_valid_refname(refname: &str) -> bool {
    refname.starts_with("refs/")
        && !refname.ends_with('.')
        && !refname.contains("..")
        && !refname.contains("@{")
        && !refname
            .bytes()
            .any(|byte| byte <= 0x20 || byte == 0x7f || b"~^:?*[\\".contains(&byte))
        && refname.split('/').all(|component| {
            !component.is_empty() && !component.starts_with('.') && !component.ends_with(".lock")
        })
}

/// Collects all refs from the HubStore for a given repo.
pub(super) async fn collect_refs(
    state: &HubState,
    repo_id: &str,
) -> Result<Vec<GitRef>, HubApiError> {
    let owned_state = state.clone();
    let owned_repo_id = repo_id.to_owned();
    tokio::task::spawn_blocking(move || collect_refs_sync(&owned_state, &owned_repo_id))
        .await
        .map_err(|error| HubApiError::CasError(error.to_string()))?
}

pub(super) fn filter_refs(
    state: &HubState,
    repo_id: &str,
    auth: &shardline_server_core::AuthorizedRepository,
    refs: &mut Vec<GitRef>,
) -> Result<(), HubApiError> {
    if let Some(visible) = crate::revision_access::visible_refs(state, repo_id, auth)? {
        // HEAD is an alias for the repository default, not for the caller's branch.
        // HubRepo.default_branch is a legacy commit identity, not a ref name.
        // collect_refs_sync synthesizes HEAD from the live main ref.
        let default = state.store.resolve_revision(repo_id, "main")?;
        refs.retain(|reference| {
            if reference.name == "HEAD" {
                visible.contains("main") && default.as_deref() == Some(reference.sha1.as_str())
            } else {
                visible.contains(shardline_index::hub::canonical_ref_name(&reference.name))
            }
        });
        // An immutable credential has no named-ref rights. A detached HEAD
        // exposes only its selected snapshot so stock Git can fetch that ID.
        if refs.is_empty()
            && let Some(snapshot) = crate::revision_access::snapshot_commit(state, repo_id, auth)?
        {
            refs.push(GitRef {
                name: "HEAD".to_owned(),
                sha1: snapshot,
            });
        }
    }
    Ok(())
}

fn collect_refs_sync(state: &HubState, repo_id: &str) -> Result<Vec<GitRef>, HubApiError> {
    let store_refs = state
        .store
        .list_refs_bounded(repo_id, 10_000)
        .map_err(|e| {
            tracing::debug!("failed to list revisions for {repo_id}: {e}");
            HubApiError::RepoNotFound
        })?;

    let fallback_head = store_refs
        .iter()
        .find(|r| r.ref_name == "main")
        .map(|r| r.sha.clone());
    let mut refs = Vec::new();
    let mut seen_refs = std::collections::HashSet::new();

    for store_ref in store_refs {
        let name = if store_ref.ref_name == "HEAD" || store_ref.ref_name.is_empty() {
            "HEAD".to_owned()
        } else if store_ref.ref_name.starts_with("refs/") {
            store_ref.ref_name
        } else {
            format!("refs/heads/{}", store_ref.ref_name)
        };
        if seen_refs.insert(name.clone()) {
            refs.push(GitRef {
                name,
                sha1: store_ref.sha,
            });
        }
    }

    // If no HEAD was explicitly set, point HEAD at the active default branch.
    if !seen_refs.contains("HEAD")
        && let Some(sha1) = fallback_head
    {
        refs.push(GitRef {
            name: "HEAD".to_owned(),
            sha1,
        });
    }

    // Normalized names are HEAD or refs/*, so this also places HEAD first.
    refs.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(refs)
}

// ---- Discovery: GET /{type}/{ns}/{repo}/info/refs ----

/// Unified Git Smart HTTP discovery handler.
///
/// Dispatches to upload-pack or receive-pack based on the `service` query
/// parameter. Requires Read scope for upload-pack, Write scope for
/// receive-pack.
///
/// # Errors
///
/// Returns [`HubApiError::NotFound`] if the service is unknown or the repo
/// does not exist. Returns [`HubApiError::Unauthorized`] or
/// [`HubApiError::Forbidden`] on auth failure.
pub async fn info_refs(
    State(state): State<HubState>,
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
    Query(query): Query<InfoRefsQuery>,
    headers: HeaderMap,
) -> Result<Response, HubApiError> {
    let service = query
        .service
        .as_deref()
        .unwrap_or(GitSmartHttpService::UploadPack.as_str())
        .parse::<GitSmartHttpService>()
        .map_err(|_err| HubApiError::NotFound)?;

    let auth_ctx = match service {
        GitSmartHttpService::ReceivePack => authorize_write_with_context(&state, &headers)?,
        GitSmartHttpService::UploadPack => authorize_read_with_context(&state, &headers)?,
    };
    let repo = super::repository::canonical_name(&state, auth_ctx.as_ref(), &ns, &repo).await?;
    require_repository_binding(auth_ctx.as_ref(), &ns, &repo)?;

    let repo_id = resolve_repo_id(&repo_type, &ns, &repo);
    let capability = match auth_ctx {
        Some(ctx) => shardline_server_core::AuthorizedRepository::from_verified_context(
            ctx,
            TokenScope::Read,
        )?,
        None => shardline_server_core::AuthorizedRepository::anonymous_full_access(),
    };
    let projection = super::projection::project_history_async(
        state.clone(),
        repo_id.clone(),
        capability.clone(),
    )
    .await?;
    let mut refs = collect_refs(&state, &repo_id).await?;
    filter_refs(&state, &repo_id, &capability, &mut refs)?;
    for git_ref in &mut refs {
        if let Some(sha) = projection.identities.get(&git_ref.sha1) {
            git_ref.sha1 = sha.clone();
        } else {
            return Err(HubApiError::CasError(
                "Git ref has no commit object".to_owned(),
            ));
        }
    }

    // Protocol v2 advertises capabilities without listing refs, but it must
    // still apply the same repository visibility and history checks as the
    // legacy advertisement before returning a successful response.
    // Stock Git's v2 partial-clone path can recursively fetch an unknown
    // server-shallow tip before reading its pack. Negotiate legacy framing
    // for snapshot views; unrestricted history retains protocol v2.
    if super::upload_pack::v2::is_requested(&headers)?
        && crate::revision_access::snapshot_commit(&state, &repo_id, &capability)?.is_none()
    {
        if service != GitSmartHttpService::UploadPack {
            return Err(HubApiError::BadRequest(
                "Git protocol v2 receive-pack is not supported".to_owned(),
            ));
        }
        let response = Response::builder()
            .header(
                "content-type",
                "application/x-git-upload-pack-advertisement",
            )
            .header("cache-control", "no-cache")
            .body(axum::body::Body::from(
                super::upload_pack::v2::advertisement()?,
            ))
            .map_err(|error| HubApiError::CasError(error.to_string()))?;
        return Ok(response);
    }

    let (capabilities, content_type) = match service {
        GitSmartHttpService::ReceivePack if state.store.supports_atomic_ref_updates() => (
            "report-status report-status-v2 delete-refs quiet side-band-64k atomic push-options",
            "application/x-git-receive-pack-advertisement",
        ),
        GitSmartHttpService::ReceivePack => (
            "report-status report-status-v2 delete-refs quiet side-band-64k push-options",
            "application/x-git-receive-pack-advertisement",
        ),
        GitSmartHttpService::UploadPack => (
            "side-band-64k shallow deepen-relative deepen-since deepen-not filter allow-reachable-sha1-in-want",
            "application/x-git-upload-pack-advertisement",
        ),
    };

    let mut body = String::new();
    let mut line_buf = String::with_capacity(128);
    body.push_str(&pktline::encode_line({
        line_buf.clear();
        writeln!(line_buf, "# service={}", service.as_str()).ok();
        &line_buf
    })?);
    body.push_str(FLUSH);
    if let Some(first) = refs.first() {
        body.push_str(&pktline::encode_line({
            line_buf.clear();
            writeln!(line_buf, "{} {}\x00{capabilities}", first.sha1, first.name).ok();
            &line_buf
        })?);
        // SAFETY: refs has at least one element (first is Some), so skip(1)
        // is safe and yields an empty iterator when refs.len() == 1.
        for r in refs.iter().skip(1) {
            body.push_str(&pktline::encode_line({
                line_buf.clear();
                writeln!(line_buf, "{} {}", r.sha1, r.name).ok();
                &line_buf
            })?);
        }
    } else {
        body.push_str(&pktline::encode_line({
            line_buf.clear();
            writeln!(
                line_buf,
                "0000000000000000000000000000000000000000 capabilities^{{}}\x00{capabilities}",
            )
            .ok();
            &line_buf
        })?);
    }
    if service == GitSmartHttpService::UploadPack
        && let Some(snapshot) =
            crate::revision_access::snapshot_commit(&state, &repo_id, &capability)?
    {
        let public = projection.identities.get(&snapshot).unwrap_or(&snapshot);
        body.push_str(&pktline::encode_line(&format!("shallow {public}\n"))?);
    }
    body.push_str(FLUSH);

    let mut resp_headers = axum::http::HeaderMap::new();
    resp_headers.insert("content-type", HeaderValue::from_static(content_type));

    Ok((resp_headers, body).into_response())
}

/// Handles Git Smart HTTP discovery for upload-pack (clone/fetch).
///
/// Returns the refs advertisement in pkt-line format.
///
/// # Errors
///
/// Forwards errors from [`info_refs`].
pub async fn info_refs_upload_pack(
    State(state): State<HubState>,
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
    Query(query): Query<InfoRefsQuery>,
    headers: HeaderMap,
) -> Result<Response, HubApiError> {
    info_refs(
        State(state),
        Path((repo_type, ns, repo)),
        Query(InfoRefsQuery {
            service: Some(
                query
                    .service
                    .unwrap_or_else(|| "git-upload-pack".to_owned()),
            ),
        }),
        headers,
    )
    .await
}

/// Handles Git Smart HTTP discovery for receive-pack (push).
///
/// # Errors
///
/// Forwards errors from [`info_refs`].
pub async fn info_refs_receive_pack(
    State(state): State<HubState>,
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
    Query(query): Query<InfoRefsQuery>,
    headers: HeaderMap,
) -> Result<Response, HubApiError> {
    info_refs(
        State(state),
        Path((repo_type, ns, repo)),
        Query(InfoRefsQuery {
            service: Some(
                query
                    .service
                    .unwrap_or_else(|| "git-receive-pack".to_owned()),
            ),
        }),
        headers,
    )
    .await
}
