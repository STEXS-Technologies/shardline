//! Upload-pack implementation for clone/fetch.

use axum::{
    body::{Body, to_bytes},
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
#[cfg(test)]
use shardline_index::hub::HubFileEntry;

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
    require_repository_binding(auth_ctx.as_ref(), &ns, &repo)?;

    let repo_id = resolve_repo_id(&repo_type, &ns, &repo);

    let body = to_bytes(body, MAX_UPLOAD_PACK_REQUEST_BYTES)
        .await
        .map_err(|error| {
            HubApiError::BadRequest(format!("upload-pack request too large: {error}"))
        })?;
    let request_lines = pktline::decode_lines(&body);
    let wants = parse_wants(&request_lines);
    let _haves = parse_haves(&request_lines);

    let capability = match auth_ctx {
        Some(ctx) => shardline_server_core::AuthorizedRepository::from_verified_context(
            ctx,
            shardline_protocol::TokenScope::Read,
        )?,
        None => shardline_server_core::AuthorizedRepository::anonymous_full_access(),
    };
    let projection =
        super::projection::project_history_async(state.clone(), repo_id.clone(), capability)
            .await?;
    let identities: std::collections::HashSet<_> = projection.identities.values().collect();
    for want in &wants {
        if !identities.contains(want) {
            return Err(HubApiError::BadRequest(
                "wanted Git commit is not in this repository".to_owned(),
            ));
        }
    }
    let sideband = request_lines.iter().any(|line| {
        std::str::from_utf8(line)
            .is_ok_and(|line| line.split_whitespace().any(|word| word == "side-band-64k"))
    });
    let response_body = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, HubApiError> {
        let pack_data = generate_pack(&projection.objects)?;
        let mut response_body = pktline::encode_line("NAK\n")?.into_bytes();
        if sideband {
            response_body.extend_from_slice(&pktline::sideband_data(&pack_data));
            response_body.extend_from_slice(FLUSH.as_bytes());
        } else {
            response_body.extend_from_slice(&pack_data);
        }
        Ok(response_body)
    })
    .await
    .map_err(|error| HubApiError::CasError(error.to_string()))??;

    let mut resp_headers = axum::http::HeaderMap::new();
    resp_headers.insert(
        "content-type",
        HeaderValue::from_static("application/x-git-upload-pack-result"),
    );
    resp_headers.insert("cache-control", HeaderValue::from_static("no-cache"));

    Ok((resp_headers, response_body).into_response())
}

// ---- Helper functions ----

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

// ---- Real content pack generation ----

/// Builds Git tree objects (root + all sub-trees) from a flat list of
/// file entries. Returns `(root_tree, sub_trees)`.
///
/// Directories are represented as sub-tree objects. All sub-trees are
/// returned in the second vector so the caller can add them to the pack.
#[cfg(test)]
pub(super) fn build_git_tree_objects(files: &[HubFileEntry]) -> (GitObject, Vec<GitObject>) {
    let refs: Vec<&HubFileEntry> = files.iter().collect();
    let mut sub_trees = Vec::new();
    let entries = build_tree_entries(&refs, "", &mut sub_trees);
    let root = tree_object_from_entries(&entries);
    (root, sub_trees)
}

/// Creates a Git tree object from owned (mode, name, sha1) entries.
#[cfg(test)]
fn tree_object_from_entries(entries: &[(u32, String, [u8; 20])]) -> GitObject {
    let mut tree_data = Vec::new();
    for (mode, name, sha1) in entries {
        let mode_str = format!("{mode:o}");
        tree_data.extend_from_slice(mode_str.as_bytes());
        tree_data.push(b' ');
        tree_data.extend_from_slice(name.as_bytes());
        tree_data.push(0);
        tree_data.extend_from_slice(sha1);
    }
    GitObject::tree(tree_data)
}

/// Recursively builds tree entries for a directory.
///
/// `prefix` is the current directory path (empty string for root).
/// `sub_trees` collects any sub-tree objects created during recursion.
#[cfg(test)]
fn build_tree_entries<'input>(
    files: &[&'input HubFileEntry],
    prefix: &str,
    sub_trees: &mut Vec<GitObject>,
) -> Vec<(u32, String, [u8; 20])> {
    let mut result = Vec::new();
    let mut children: std::collections::HashMap<String, Vec<&'input HubFileEntry>> =
        std::collections::HashMap::new();

    for file in files {
        let relative = if prefix.is_empty() {
            file.path.as_str()
        } else {
            file.path
                .strip_prefix(&format!("{prefix}/"))
                .unwrap_or(&file.path)
        };

        if let Some((name, _rest)) = relative.split_once('/') {
            children.entry(name.to_owned()).or_default().push(file);
        } else if !relative.is_empty() {
            let blob = if file.is_lfs {
                build_lfs_pointer_blob(&file.sha, file.size)
            } else {
                build_inline_blob(file)
            };
            let blob_sha = blob.sha1();
            result.push((0o100644, relative.to_owned(), blob_sha));
        }
    }

    result.sort_by(|a, b| a.1.cmp(&b.1));

    let mut dir_names: Vec<String> = children.keys().cloned().collect();
    dir_names.sort();

    for dir_name in &dir_names {
        // SAFETY: dir_name comes from children.keys(), guaranteed to exist
        let dir_files = match children.get(dir_name) {
            Some(f) => f,
            // This arm is unreachable because dir_name comes from keys()
            None => continue,
        };
        let sub_prefix = if prefix.is_empty() {
            dir_name.clone()
        } else {
            format!("{prefix}/{dir_name}")
        };
        let sub_entries = build_tree_entries(dir_files, &sub_prefix, sub_trees);
        let subtree = tree_object_from_entries(&sub_entries);
        let subtree_sha = subtree.sha1();
        sub_trees.push(subtree);
        result.push((0o40000, dir_name.clone(), subtree_sha));
    }

    result
}

/// Generates a Git blob object for an inline (non-LFS) file.
///
/// Since the actual file content is not stored in the HubStore (only the
/// content hash), this generates a deterministic placeholder that Git can
/// check out. The blob contains the file's SHA identifier so the content
/// is at least addressable.
#[cfg(test)]
pub(super) fn build_inline_blob(file: &HubFileEntry) -> GitObject {
    // Use the file's content hash as deterministic blob content.
    // This ensures the same file always produces the same blob SHA.
    let content = format!("shardline:{}:{}", file.sha, file.size);
    GitObject::blob(content.into_bytes())
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

/// Generates a `.gitattributes` blob that tells Git to treat LFS files
/// as LFS-tracked. Returns `None` if no files are LFS-tracked.
#[cfg(test)]
pub(super) fn build_gitattributes_blob(files: &[HubFileEntry]) -> Option<GitObject> {
    let lfs_files: Vec<&HubFileEntry> = files.iter().filter(|f| f.is_lfs).collect();
    if lfs_files.is_empty() {
        return None;
    }

    let mut content = String::new();
    // Sort for deterministic output.
    let mut sorted = lfs_files;
    sorted.sort_by(|a, b| a.path.cmp(&b.path));

    for file in &sorted {
        use std::fmt::Write;
        writeln!(
            &mut content,
            "{} filter=lfs diff=lfs merge=lfs -text",
            file.path
        )
        .ok();
    }

    Some(GitObject::blob(content.into_bytes()))
}
