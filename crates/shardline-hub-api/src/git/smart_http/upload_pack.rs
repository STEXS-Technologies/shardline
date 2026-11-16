//! Upload-pack implementation for clone/fetch.

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
};

use super::super::pack::{GitObject, generate_pack};
use super::super::pktline::{self, FLUSH};
use super::MAX_UPLOAD_PACK_REQUEST_BYTES;
use super::ref_advertisement::{authorize_read_with_context, resolve_repo_id};
use crate::{
    error::HubApiError,
    routes::{HubState, require_repository_binding},
};
mod filter;
mod shallow;
pub(super) mod v2;

// ---- Upload-pack: POST /{type}/{ns}/{repo}/git-upload-pack ----

/// Handles the upload-pack request (clone/fetch).
///
/// # Errors
///
/// Returns [`HubApiError::Unauthorized`] or [`HubApiError::Forbidden`] on
/// auth failure. Returns [`HubApiError::NotFound`] if the repo does not exist.
pub async fn upload_pack(
    State(state): State<HubState>,
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, HubApiError> {
    let auth_ctx = authorize_read_with_context(&state, &headers)?;
    let repo = super::repository::canonical_name(&state, auth_ctx.as_ref(), &ns, &repo).await?;
    require_repository_binding(auth_ctx.as_ref(), &ns, &repo)?;

    let repo_id = resolve_repo_id(&repo_type, &ns, &repo);

    let (body, request_admission) =
        super::admission::read_request(body, MAX_UPLOAD_PACK_REQUEST_BYTES, "upload-pack").await?;
    let v2_request = v2::parse(&body)?;
    let (request_lines, v2_fetch, v2_ls_refs) = match v2_request {
        Some(v2::Request::Fetch(fetch)) => (fetch.lines.clone(), Some(fetch), None),
        Some(v2::Request::LsRefs(request)) => (Vec::new(), None, Some(request)),
        None => (decode_negotiation(&body)?, None, None),
    };
    if v2_fetch.is_none() && v2_ls_refs.is_none() {
        super::capabilities::upload(&request_lines)?;
    }
    let object_filter = filter::Filter::parse(&request_lines)?;
    let mut shallow_request = shallow::Request::parse(&request_lines)?;
    let wants = parse_wants(&request_lines);
    let mut haves = parse_haves(&request_lines);

    if let Some(request) = v2_ls_refs {
        let capability = match auth_ctx {
            Some(ctx) => shardline_server_core::AuthorizedRepository::from_verified_context(
                ctx,
                shardline_protocol::TokenScope::Read,
            )?,
            None => shardline_server_core::AuthorizedRepository::anonymous_full_access(),
        };
        let projection = super::projection::project_history_admitted(
            state.clone(),
            repo_id.clone(),
            capability.clone(),
            request_admission,
        )
        .await?;
        let mut refs = super::ref_advertisement::collect_refs(&state, &repo_id).await?;
        super::ref_advertisement::filter_refs(&state, &repo_id, &capability, &mut refs)?;
        for reference in &mut refs {
            reference.sha1 = projection
                .identities
                .get(&reference.sha1)
                .cloned()
                .ok_or_else(|| HubApiError::CasError("Git ref has no commit object".to_owned()))?;
        }
        let default_symref = refs
            .iter()
            .find(|reference| reference.name == "HEAD")
            .and_then(|head| {
                let mut candidates = refs.iter().filter(|reference| {
                    reference.name.starts_with("refs/heads/") && reference.sha1 == head.sha1
                });
                let candidate = candidates.next()?;
                candidates
                    .next()
                    .is_none()
                    .then_some(candidate.name.as_str())
            });
        let response_body = v2::ls_refs_response(&refs, &request, default_symref)?;
        let response = Response::builder()
            .header("content-type", "application/x-git-upload-pack-result")
            .header("cache-control", "no-cache")
            .body(Body::from(response_body))
            .map_err(|error| HubApiError::CasError(error.to_string()))?;
        return Ok(response);
    }

    let capability = match auth_ctx {
        Some(ctx) => shardline_server_core::AuthorizedRepository::from_verified_context(
            ctx,
            shardline_protocol::TokenScope::Read,
        )?,
        None => shardline_server_core::AuthorizedRepository::anonymous_full_access(),
    };
    let snapshot = crate::revision_access::snapshot_commit(&state, &repo_id, &capability)?;
    let projection = super::projection::project_history_admitted(
        state.clone(),
        repo_id.clone(),
        capability,
        request_admission,
    )
    .await?;
    if let Some(snapshot) = snapshot {
        let public = projection.identities.get(&snapshot).unwrap_or(&snapshot);
        haves.retain(|have| have == public);
        shallow_request.restrict_to_snapshot(public, &wants, v2_fetch.is_some());
    }
    shallow_request
        .resolve_exclusions(&state, &repo_id, &projection.identities)
        .await?;
    let identities: std::collections::HashSet<_> = projection
        .objects
        .iter()
        .map(|object| hex::encode(object.sha1()))
        .collect();
    for want in &wants {
        if !identities.contains(want) {
            return Err(HubApiError::BadRequest(
                "wanted Git object is not reachable in this repository".to_owned(),
            ));
        }
    }
    let sideband = v2_fetch.is_some()
        || request_lines.iter().any(|line| {
            std::str::from_utf8(line)
                .is_ok_and(|line| line.split_whitespace().any(|word| word == "side-band-64k"))
        });
    let done = v2_fetch.as_ref().map_or_else(
        || {
            request_lines
                .iter()
                .any(|line| line.as_slice() == b"done\n" || line.as_slice() == b"done")
        },
        |fetch| fetch.done,
    );
    let common_haves = haves
        .iter()
        .filter(|have| identities.contains(*have))
        .cloned()
        .collect::<Vec<_>>();
    let (response_body, response_admission) = tokio::task::spawn_blocking(move || {
        let pack_admission = projection
            .admission
            .ok_or_else(|| HubApiError::CasError("Git projection missing admission".to_owned()))?;
        let (objects, mut response_body) =
            shallow::negotiate(projection.objects, &wants, &haves, &shallow_request)?;
        if v2_fetch.is_some() && !done {
            return Ok((v2::fetch_response(common_haves, &[], None)?, pack_admission));
        } else if !done && !response_body.is_empty() {
            return Ok((response_body, pack_admission));
        }
        let pack_data = generate_pack(&object_filter.apply(objects, &wants)?)?;
        if v2_fetch.is_some() {
            return Ok((
                v2::fetch_response(common_haves, &response_body, Some(&pack_data))?,
                pack_admission,
            ));
        }
        response_body.extend_from_slice(pktline::encode_line("NAK\n")?.as_bytes());
        if sideband {
            response_body.extend_from_slice(&pktline::sideband_data(&pack_data));
            response_body.extend_from_slice(FLUSH.as_bytes());
        } else {
            response_body.extend_from_slice(&pack_data);
        }
        Ok::<_, HubApiError>((response_body, pack_admission))
    })
    .await
    .map_err(|error| HubApiError::CasError(error.to_string()))??;

    let mut resp_headers = axum::http::HeaderMap::new();
    resp_headers.insert(
        "content-type",
        HeaderValue::from_static("application/x-git-upload-pack-result"),
    );
    resp_headers.insert("cache-control", HeaderValue::from_static("no-cache"));
    resp_headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&response_body.len().to_string())
            .map_err(|error| HubApiError::CasError(error.to_string()))?,
    );

    Ok((
        resp_headers,
        super::admission::response_body(response_body, response_admission),
    )
        .into_response())
}

// ---- Helper functions ----

/// Upload-pack places have lines after the want-list flush. The generic
/// single-section pkt-line decoder intentionally stops at that flush.
fn decode_negotiation(mut bytes: &[u8]) -> Result<Vec<Vec<u8>>, HubApiError> {
    let invalid = || HubApiError::BadRequest("invalid upload-pack pkt-line".to_owned());
    let mut lines = Vec::new();
    while !bytes.is_empty() {
        let header = bytes.get(..4).ok_or_else(invalid)?;
        let length = std::str::from_utf8(header)
            .ok()
            .and_then(|value| usize::from_str_radix(value, 16).ok())
            .ok_or_else(invalid)?;
        if length <= 2 {
            bytes = bytes.get(4..).ok_or_else(invalid)?;
            continue;
        }
        let payload = bytes.get(4..length).ok_or_else(invalid)?;
        lines.push(payload.to_vec());
        bytes = bytes.get(length..).ok_or_else(invalid)?;
    }
    Ok(lines)
}

/// Send only wanted reachable objects absent from the client's known history.
/// Unknown have IDs cannot exclude any object from this repository.
#[cfg(test)]
fn negotiated_objects(
    objects: Vec<GitObject>,
    wants: &[String],
    haves: &[String],
) -> Result<Vec<GitObject>, HubApiError> {
    shallow::negotiate(objects, wants, haves, &shallow::Request::default())
        .map(|(objects, _updates)| objects)
}

#[cfg(test)]
mod negotiation_tests;

pub(super) fn parse_wants(lines: &[Vec<u8>]) -> Vec<String> {
    lines
        .iter()
        .filter_map(|line| {
            let s = std::str::from_utf8(line).ok()?;
            let s = s.trim();
            let hash = s.strip_prefix("want ")?;
            let hash = hash.split_whitespace().next()?;
            Some(hash.to_owned())
        })
        .collect()
}

pub(super) fn parse_haves(lines: &[Vec<u8>]) -> Vec<String> {
    lines
        .iter()
        .filter_map(|line| {
            let s = std::str::from_utf8(line).ok()?;
            let s = s.trim();
            let hash = s.strip_prefix("have ")?;
            let hash = hash.split_whitespace().next()?;
            Some(hash.to_owned())
        })
        .collect()
}

/// Generates a Git LFS pointer blob following the LFS spec v1.
///
/// Format (https://github.com/git-lfs/git-lfs/blob/main/spec.md):
/// ```text
/// version https://git-lfs.github.com/spec/v1
/// oid sha256:<oid>
/// size <size>
/// ```
pub(super) fn build_lfs_pointer_blob(oid: &str, size: u64) -> GitObject {
    let pointer =
        format!("version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize {size}\n");
    GitObject::blob(pointer.into_bytes())
}
