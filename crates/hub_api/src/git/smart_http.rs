//! Git Smart HTTP protocol handlers.
//!
//! Implements the server side of Git Smart HTTP for clone/fetch (upload-pack)
//! and push (receive-pack) operations.

use axum::{
    body::Bytes,
    extract::{Path, Query},
    http::{HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use super::pack::{GitObject, ObjectType, create_commit_object, create_tree_object, empty_pack, generate_pack};
use super::pktline::{self, FLUSH};
use crate::error::HubApiError;
use shardline_index::hub::HubRepoType;
use shardline_protocol::TokenScope;

/// Query parameters for `GET /info/refs`.
#[derive(Debug, Deserialize)]
pub struct InfoRefsQuery {
    pub service: Option<String>,
}

/// Represents a Git reference to advertise.
#[derive(Debug, Clone)]
struct GitRef {
    name: String,
    sha1: String,
}

/// Resolves the repo ID from the URL path components.
fn resolve_repo_id(repo_type: &str, ns: &str, repo: &str) -> String {
    format!("{repo_type}/{ns}/{repo}")
}

fn authorize_read(headers: &HeaderMap) -> Result<(), HubApiError> {
    let state = crate::state::get();
    if let Some(ref auth) = state.auth {
        let _ = auth.authorize(headers, TokenScope::Read)?;
    }
    Ok(())
}

fn authorize_write(headers: &HeaderMap) -> Result<(), HubApiError> {
    let state = crate::state::get();
    if let Some(ref auth) = state.auth {
        let _ = auth.authorize(headers, TokenScope::Write)?;
    }
    Ok(())
}

// ---- Discovery: GET /{type}/{ns}/{repo}/info/refs ----

/// Handles Git Smart HTTP discovery for upload-pack (clone/fetch).
///
/// Returns the refs advertisement in pkt-line format.
pub async fn info_refs_upload_pack(
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
    Query(query): Query<InfoRefsQuery>,
    headers: HeaderMap,
) -> Result<Response, HubApiError> {
    let service = query.service.as_deref().unwrap_or("git-upload-pack");
    if service != "git-upload-pack" && service != "git-receive-pack" {
        return Err(HubApiError::NotFound);
    }

    authorize_read(&headers)?;

    let repo_id = resolve_repo_id(&repo_type, &ns, &repo);
    let refs = collect_refs(&repo_id).await?;

    let mut body = String::new();
    body.push_str(&pktline::encode_line(&format!("# service={service}\n")));
    body.push_str(FLUSH);
    if refs.is_empty() {
        body.push_str(&pktline::encode_line(
            "0000000000000000000000000000000000000000 capabilities^{}\0side-band-64k thin-pack multi_ack_detailed\n",
        ));
    } else {
        let first = &refs[0];
        body.push_str(&pktline::encode_line(&format!(
            "{} {} capabilities^{{}}\0side-band-64k thin-pack multi_ack_detailed\n",
            first.sha1, first.name
        )));
        for r in &refs[1..] {
            body.push_str(&pktline::encode_line(&format!("{} {}\n", r.sha1, r.name)));
        }
    }
    body.push_str(FLUSH);

    let mut resp_headers = axum::http::HeaderMap::new();
    resp_headers.insert(
        "content-type",
        HeaderValue::from_static("application/x-git-upload-pack-advertisement"),
    );

    Ok((resp_headers, body).into_response())
}

/// Handles Git Smart HTTP discovery for receive-pack (push).
pub async fn info_refs_receive_pack(
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
    Query(query): Query<InfoRefsQuery>,
    headers: HeaderMap,
) -> Result<Response, HubApiError> {
    let service = query.service.as_deref().unwrap_or("git-receive-pack");
    if service != "git-upload-pack" && service != "git-receive-pack" {
        return Err(HubApiError::NotFound);
    }

    authorize_write(&headers)?;

    let repo_id = resolve_repo_id(&repo_type, &ns, &repo);
    let refs = collect_refs(&repo_id).await?;

    let mut body = String::new();
    body.push_str(&pktline::encode_line(&format!("# service={service}\n")));
    body.push_str(FLUSH);
    if refs.is_empty() {
        body.push_str(&pktline::encode_line(
            "0000000000000000000000000000000000000000 capabilities^{}\0report-status delete-refs side-band-64k quiet\n",
        ));
    } else {
        let first = &refs[0];
        body.push_str(&pktline::encode_line(&format!(
            "{} {} capabilities^{{}}\0report-status delete-refs side-band-64k quiet\n",
            first.sha1, first.name
        )));
        for r in &refs[1..] {
            body.push_str(&pktline::encode_line(&format!("{} {}\n", r.sha1, r.name)));
        }
    }
    body.push_str(FLUSH);

    let mut resp_headers = axum::http::HeaderMap::new();
    resp_headers.insert(
        "content-type",
        HeaderValue::from_static("application/x-git-receive-pack-advertisement"),
    );

    Ok((resp_headers, body).into_response())
}

// ---- Upload-pack: POST /{type}/{ns}/{repo}/git-upload-pack ----

/// Handles the upload-pack request (clone/fetch).
pub async fn upload_pack(
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, HubApiError> {
    authorize_read(&headers)?;

    let repo_id = resolve_repo_id(&repo_type, &ns, &repo);

    let request_lines = pktline::decode_lines(&body);
    let _wants = parse_wants(&request_lines);
    let _haves = parse_haves(&request_lines);

    let refs = collect_refs(&repo_id).await?;

    let pack_data = if refs.is_empty() {
        empty_pack()
    } else {
        generate_pack_for_refs(&refs).await
    };

    let mut response_body = pktline::sideband_data(&pack_data);
    response_body.extend_from_slice(FLUSH.as_bytes());

    let mut resp_headers = axum::http::HeaderMap::new();
    resp_headers.insert(
        "content-type",
        HeaderValue::from_static("application/x-git-upload-pack-result"),
    );
    resp_headers.insert("cache-control", HeaderValue::from_static("no-cache"));

    Ok((resp_headers, response_body).into_response())
}

// ---- Receive-pack: POST /{type}/{ns}/{repo}/git-receive-pack ----

/// Handles the receive-pack request (push).
pub async fn receive_pack(
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, HubApiError> {
    authorize_write(&headers)?;

    let repo_id = resolve_repo_id(&repo_type, &ns, &repo);

    let (updates, pack_data) = parse_receive_pack_request(&body);

    if updates.is_empty() {
        return build_report_response(&[]);
    }

    let objects = parse_pack_data(&pack_data);
    let mut results = Vec::new();

    for (_old_sha, new_sha, refname) in &updates {
        match store_push_objects(&repo_id, new_sha, &objects).await {
            Ok(()) => results.push((refname.clone(), true, None)),
            Err(e) => results.push((refname.clone(), false, Some(e))),
        }
    }

    build_report_response(&results)
}

// ---- Helper functions ----

/// Collects all refs from the HubStore for a given repo.
async fn collect_refs(repo_id: &str) -> Result<Vec<GitRef>, HubApiError> {
    let state = crate::state::get();
    let revisions = state
        .store
        .list_revisions(repo_id)
        .map_err(|_| HubApiError::RepoNotFound)?;

    let mut refs = Vec::new();
    for rev in revisions {
        if rev.ref_name == "HEAD" || rev.ref_name.is_empty() {
            refs.push(GitRef {
                name: "HEAD".to_string(),
                sha1: rev.sha.clone(),
            });
            refs.push(GitRef {
                name: "refs/heads/main".to_string(),
                sha1: rev.sha.clone(),
            });
        } else if rev.ref_name.starts_with("refs/") {
            refs.push(GitRef {
                name: rev.ref_name.clone(),
                sha1: rev.sha.clone(),
            });
        } else {
            refs.push(GitRef {
                name: format!("refs/heads/{}", rev.ref_name),
                sha1: rev.sha.clone(),
            });
        }
    }

    refs.sort_by(|a, b| a.name.cmp(&b.name));

    if let Some(pos) = refs.iter().position(|r| r.name == "HEAD") {
        let head = refs.remove(pos);
        refs.insert(0, head);
    }

    Ok(refs)
}

fn parse_wants(lines: &[Vec<u8>]) -> Vec<String> {
    lines
        .iter()
        .filter_map(|line| {
            let s = std::str::from_utf8(line).ok()?;
            let s = s.trim();
            let hash = s.strip_prefix("want ")?;
            let hash = hash.split_whitespace().next()?;
            Some(hash.to_string())
        })
        .collect()
}

fn parse_haves(lines: &[Vec<u8>]) -> Vec<String> {
    lines
        .iter()
        .filter_map(|line| {
            let s = std::str::from_utf8(line).ok()?;
            let s = s.trim();
            let hash = s.strip_prefix("have ")?;
            let hash = hash.split_whitespace().next()?;
            Some(hash.to_string())
        })
        .collect()
}

async fn generate_pack_for_refs(refs: &[GitRef]) -> Vec<u8> {
    let mut objects = Vec::new();
    let mut seen_trees = std::collections::HashSet::new();

    for git_ref in refs {
        if git_ref.name == "HEAD" {
            continue;
        }

        let tree_sha1 = compute_tree_sha1(git_ref);
        if seen_trees.insert(tree_sha1) {
            let tree = create_tree_object(&[]);
            objects.push(tree);
        }

        let commit = create_commit_object(
            &tree_sha1,
            None,
            "Shardline Hub <hub@shardline.dev>",
            &format!("Update {ref}", ref = git_ref.name),
        );
        objects.push(commit);
    }

    if objects.is_empty() {
        return empty_pack();
    }

    generate_pack(&objects)
}

fn compute_tree_sha1(ref_: &GitRef) -> [u8; 20] {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(format!("tree:{}", ref_.sha1).as_bytes());
    hasher.finalize().into()
}

fn parse_receive_pack_request(body: &[u8]) -> (Vec<(String, String, String)>, Vec<u8>) {
    let mut updates = Vec::new();
    let mut pack_start = 0;

    let lines = pktline::decode_lines(body);
    for line in &lines {
        let s = match std::str::from_utf8(line) {
            Ok(s) => s.trim().to_string(),
            Err(_) => continue,
        };

        if s.is_empty() {
            continue;
        }

        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() >= 3 {
            updates.push((
                parts[0].to_string(),
                parts[1].to_string(),
                parts[2].to_string(),
            ));
        }
    }

    let mut pos = 0;
    while pos + 4 <= body.len() {
        let hex_len = &body[pos..pos + 4];
        if let Ok(hex_str) = std::str::from_utf8(hex_len) {
            if let Ok(len) = u16::from_str_radix(hex_str, 16) {
                if len == 0 {
                    pack_start = pos + 4;
                    break;
                }
                pos += len as usize;
                continue;
            }
        }
        break;
    }

    let pack_data = if pack_start < body.len() {
        body[pack_start..].to_vec()
    } else {
        Vec::new()
    };

    (updates, pack_data)
}

fn parse_pack_data(data: &[u8]) -> Vec<GitObject> {
    if data.len() < 12 {
        return Vec::new();
    }

    if &data[0..4] != b"PACK" {
        return Vec::new();
    }

    let version = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    let num_objects = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);

    if version != 2 {
        return Vec::new();
    }

    let mut objects = Vec::new();
    let mut pos = 12;

    for _ in 0..num_objects {
        if pos >= data.len() {
            break;
        }

        let byte = data[pos];
        pos += 1;

        let obj_type = (byte >> 4) & 0x07;
        let mut size = (byte & 0x0f) as u64;
        let mut shift = 4;

        let mut current = byte;
        while current & 0x80 != 0 && pos < data.len() {
            current = data[pos];
            pos += 1;
            size |= ((current & 0x7f) as u64) << shift;
            shift += 7;
        }

        match obj_type {
            1 | 2 | 3 | 4 => {
                let remaining = &data[pos..];
                match decompress_zlib(remaining) {
                    Ok((decompressed, bytes_used)) => {
                        pos += bytes_used;
                        let ot = match obj_type {
                            1 => ObjectType::Commit,
                            2 => ObjectType::Tree,
                            3 => ObjectType::Blob,
                            _ => ObjectType::Tag,
                        };
                        objects.push(GitObject {
                            object_type: ot,
                            data: decompressed,
                        });
                    }
                    Err(_) => break,
                }
            }
            6 => {
                // OFS_DELTA — skip
                let mut current = data[pos];
                pos += 1;
                while current & 0x80 != 0 && pos < data.len() {
                    current = data[pos];
                    pos += 1;
                }
                let remaining = &data[pos..];
                if let Ok((_, bytes_used)) = decompress_zlib(remaining) {
                    pos += bytes_used;
                }
            }
            7 => {
                // REF_DELTA — skip
                if pos + 20 <= data.len() {
                    pos += 20;
                }
                let remaining = &data[pos..];
                if let Ok((_, bytes_used)) = decompress_zlib(remaining) {
                    pos += bytes_used;
                }
            }
            _ => break,
        }
    }

    objects
}

fn decompress_zlib(data: &[u8]) -> Result<(Vec<u8>, usize), Box<dyn std::error::Error>> {
    use flate2::read::ZlibDecoder;
    use std::io::Read;

    let mut decoder = ZlibDecoder::new(data);
    let mut output = Vec::new();
    decoder.read_to_end(&mut output)?;
    let bytes_used = decoder.total_in() as usize;
    Ok((output, bytes_used))
}

async fn store_push_objects(
    repo_id: &str,
    new_sha: &str,
    _objects: &[GitObject],
) -> Result<(), String> {
    let state = crate::state::get();
    state
        .store
        .create_revision(repo_id, None, new_sha, "main", &format!("Push to {new_sha}"))
        .map_err(|e| format!("failed to create revision: {e}"))?;

    Ok(())
}

fn build_report_response(
    results: &[(String, bool, Option<String>)],
) -> Result<Response, HubApiError> {
    let mut body = String::new();

    if results.is_empty() {
        body.push_str(&pktline::encode_line("unpack ok\n"));
    } else {
        body.push_str(&pktline::encode_line("unpack ok\n"));
        for (refname, ok, error) in results {
            if *ok {
                body.push_str(&pktline::encode_line(&format!("ok {refname}\n")));
            } else {
                let msg = error.as_deref().unwrap_or("failed");
                body.push_str(&pktline::encode_line(&format!("ng {refname} {msg}\n")));
            }
        }
    }
    body.push_str(FLUSH);

    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("application/x-git-receive-pack-result"),
    );

    Ok((headers, body).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_wants_from_request() {
        let lines = vec![
            b"want 8ab686eafeb1f44702738c8b0f24f2567c36da6d side-band-64k\n".to_vec(),
            b"want 0000000000000000000000000000000000000000\n".to_vec(),
        ];
        let wants = parse_wants(&lines);
        assert_eq!(wants.len(), 2);
        assert_eq!(wants[0], "8ab686eafeb1f44702738c8b0f24f2567c36da6d");
    }

    #[test]
    fn parse_haves_from_request() {
        let lines = vec![b"have 0000000000000000000000000000000000000000\n".to_vec()];
        let haves = parse_haves(&lines);
        assert_eq!(haves.len(), 1);
    }

    #[test]
    fn resolve_repo_id_format() {
        let id = resolve_repo_id("models", "org", "my-model");
        assert_eq!(id, "models/org/my-model");
    }
}
