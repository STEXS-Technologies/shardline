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
use shardline_server_core::AuthContext;

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

#[cfg(test)]
pub(super) fn authorize_read(state: &HubState, headers: &HeaderMap) -> Result<(), HubApiError> {
    authorize_read_with_context(state, headers).map(|_| ())
}

#[cfg(test)]
pub(super) fn authorize_write(state: &HubState, headers: &HeaderMap) -> Result<(), HubApiError> {
    authorize_write_with_context(state, headers).map(|_| ())
}

/// Like [`authorize_read`] but returns the verified auth context so the caller
/// can enforce a token→repository binding.
pub(super) fn authorize_read_with_context(
    state: &HubState,
    headers: &HeaderMap,
) -> Result<Option<AuthContext>, HubApiError> {
    if let Some(ref auth) = state.auth {
        Ok(Some(auth.authorize(headers, TokenScope::Read)?))
    } else {
        Ok(None)
    }
}

/// Like [`authorize_write`] but returns the verified auth context so the caller
/// can enforce a token→repository binding.
pub(super) fn authorize_write_with_context(
    state: &HubState,
    headers: &HeaderMap,
) -> Result<Option<AuthContext>, HubApiError> {
    if let Some(ref auth) = state.auth {
        Ok(Some(auth.authorize(headers, TokenScope::Write)?))
    } else {
        Ok(None)
    }
}

use shardline_protocol::TokenScope;

/// Validates a Git refname for receive-pack.
///
/// Rejects empty refnames, refnames with ASCII control characters,
/// refnames that do not start with `refs/`, and refnames containing
/// `..` path components (path traversal).
pub(super) fn is_valid_refname(refname: &str) -> bool {
    if refname.is_empty() || refname.contains(' ') {
        return false;
    }
    if refname.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return false;
    }
    if !refname.starts_with("refs/") {
        return false;
    }
    !refname.split('/').any(|c| c == "..")
}

/// Collects all refs from the HubStore for a given repo.
pub(super) async fn collect_refs(
    state: &HubState,
    repo_id: &str,
) -> Result<Vec<GitRef>, HubApiError> {
    let store_refs = state.store.list_refs(repo_id).map_err(|e| {
        tracing::debug!("failed to list revisions for {repo_id}: {e}");
        HubApiError::RepoNotFound
    })?;

    let mut refs = Vec::new();
    let mut seen_refs = std::collections::HashSet::new();

    for store_ref in &store_refs {
        if store_ref.ref_name == "HEAD" || store_ref.ref_name.is_empty() {
            if seen_refs.insert("HEAD".to_owned()) {
                refs.push(GitRef {
                    name: "HEAD".to_owned(),
                    sha1: store_ref.sha.clone(),
                });
            }
        } else if store_ref.ref_name.starts_with("refs/") {
            if seen_refs.insert(store_ref.ref_name.clone()) {
                refs.push(GitRef {
                    name: store_ref.ref_name.clone(),
                    sha1: store_ref.sha.clone(),
                });
            }
        } else {
            let full_ref = format!("refs/heads/{}", store_ref.ref_name);
            if seen_refs.insert(full_ref.clone()) {
                refs.push(GitRef {
                    name: full_ref,
                    sha1: store_ref.sha.clone(),
                });
            }
        }
    }

    // If no HEAD was explicitly set, point HEAD at the active default branch.
    if !seen_refs.contains("HEAD")
        && let Some(main_ref) = store_refs.iter().find(|r| r.ref_name == "main")
    {
        refs.insert(
            0,
            GitRef {
                name: "HEAD".to_owned(),
                sha1: main_ref.sha.clone(),
            },
        );
    }

    refs.sort_by(|a, b| a.name.cmp(&b.name));

    // Ensure HEAD is first.
    if let Some(pos) = refs.iter().position(|r| r.name == "HEAD") {
        let head = refs.remove(pos);
        refs.insert(0, head);
    }

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
    require_repository_binding(auth_ctx.as_ref(), &ns, &repo)?;

    let repo_id = resolve_repo_id(&repo_type, &ns, &repo);
    let refs = collect_refs(&state, &repo_id).await?;

    let (capabilities, content_type) = match service {
        GitSmartHttpService::ReceivePack => (
            "report-status delete-refs side-band-64k quiet",
            "application/x-git-receive-pack-advertisement",
        ),
        GitSmartHttpService::UploadPack => (
            "side-band-64k thin-pack multi_ack_detailed",
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
            writeln!(
                line_buf,
                "{} {} capabilities^{{}}\x00{capabilities}",
                first.sha1, first.name
            )
            .ok();
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
