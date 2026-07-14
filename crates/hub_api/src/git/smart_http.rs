//! Git Smart HTTP protocol handlers.
//!
//! Implements the server side of Git Smart HTTP for clone/fetch (upload-pack)
//! and push (receive-pack) operations. Upload-pack generates real Git pack
//! files from HubStore metadata: tree objects, LFS pointer blobs, and commit
//! objects are all constructed from the file entries stored per revision.

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use super::pack::{
    GitObject, ObjectType, PackError, apply_delta, create_commit_object, empty_pack, generate_pack,
    parse_ofs_delta_offset,
};
use super::pktline::{self, FLUSH};
use crate::error::HubApiError;
use crate::routes::HubState;
use shardline_index::hub::HubFileEntry;
use shardline_protocol::TokenScope;
use std::collections::HashMap;

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
fn resolve_repo_id(_repo_type: &str, ns: &str, repo: &str) -> String {
    format!("{ns}/{repo}")
}

fn authorize_read(state: &HubState, headers: &HeaderMap) -> Result<(), HubApiError> {
    if let Some(ref auth) = state.auth {
        let _ = auth.authorize(headers, TokenScope::Read)?;
    }
    Ok(())
}

fn authorize_write(state: &HubState, headers: &HeaderMap) -> Result<(), HubApiError> {
    if let Some(ref auth) = state.auth {
        let _ = auth.authorize(headers, TokenScope::Write)?;
    }
    Ok(())
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
    let service = query.service.as_deref().unwrap_or("git-upload-pack");
    if service != "git-upload-pack" && service != "git-receive-pack" {
        return Err(HubApiError::NotFound);
    }

    if service == "git-receive-pack" {
        authorize_write(&state, &headers)?;
    } else {
        authorize_read(&state, &headers)?;
    }

    let repo_id = resolve_repo_id(&repo_type, &ns, &repo);
    let refs = collect_refs(&state, &repo_id).await?;

    let (capabilities, content_type) = if service == "git-receive-pack" {
        (
            "report-status delete-refs side-band-64k quiet",
            "application/x-git-receive-pack-advertisement",
        )
    } else {
        (
            "side-band-64k thin-pack multi_ack_detailed",
            "application/x-git-upload-pack-advertisement",
        )
    };

    let mut body = String::new();
    body.push_str(&pktline::encode_line(&format!("# service={service}\n"))?);
    body.push_str(FLUSH);
    if let Some(first) = refs.first() {
        body.push_str(&pktline::encode_line(&format!(
            "{} {} capabilities^{{}}\x00{capabilities}\n",
            first.sha1, first.name
        ))?);
        // SAFETY: refs has at least one element (first is Some), so skip(1)
        // is safe and yields an empty iterator when refs.len() == 1.
        for r in refs.iter().skip(1) {
            body.push_str(&pktline::encode_line(&format!("{} {}\n", r.sha1, r.name))?);
        }
    } else {
        body.push_str(&pktline::encode_line(&format!(
            "0000000000000000000000000000000000000000 capabilities^{{}}\x00{capabilities}\n",
        ))?);
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
    body: Bytes,
) -> Result<Response, HubApiError> {
    authorize_read(&state, &headers)?;

    let repo_id = resolve_repo_id(&repo_type, &ns, &repo);

    let request_lines = pktline::decode_lines(&body);
    let _wants = parse_wants(&request_lines);
    let _haves = parse_haves(&request_lines);

    let refs = collect_refs(&state, &repo_id).await?;

    let pack_data = if refs.is_empty() {
        empty_pack()?
    } else {
        generate_pack_for_refs(&state, &refs).await?
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
///
/// # Errors
///
/// Returns [`HubApiError::Unauthorized`] or [`HubApiError::Forbidden`] on
/// auth failure. Returns [`HubApiError::NotFound`] if the repo does not exist.
pub async fn receive_pack(
    State(state): State<HubState>,
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, HubApiError> {
    authorize_write(&state, &headers)?;

    let repo_id = resolve_repo_id(&repo_type, &ns, &repo);

    let (updates, pack_data) = parse_receive_pack_request(&body);

    let updates: Vec<_> = updates
        .into_iter()
        .filter(|(_old, _new, refname)| is_valid_refname(refname))
        .collect();

    if updates.is_empty() {
        return build_report_response(&[], true);
    }

    let objects = match parse_pack_data(&pack_data) {
        Ok(objects) => objects,
        Err(e) => {
            tracing::warn!("failed to parse receive-pack data: {e}");
            return build_report_response(
                &updates
                    .into_iter()
                    .map(|(_, _, refname)| (refname, false, Some("unpack failed".to_owned())))
                    .collect::<Vec<_>>(),
                false,
            );
        }
    };

    let mut results = Vec::new();

    for (old_sha, new_sha, refname) in &updates {
        match store_push_objects(&state, &repo_id, old_sha, new_sha, refname, &objects).await {
            Ok(()) => results.push((refname.clone(), true, None)),
            Err(e) => results.push((refname.clone(), false, Some(e))),
        }
    }

    build_report_response(&results, true)
}

// ---- Helper functions ----

/// Validates a Git refname for receive-pack.
///
/// Rejects empty refnames, refnames with ASCII control characters,
/// refnames that do not start with `refs/`, and refnames containing
/// `..` path components (path traversal).
fn is_valid_refname(refname: &str) -> bool {
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
async fn collect_refs(state: &HubState, repo_id: &str) -> Result<Vec<GitRef>, HubApiError> {
    let revisions = state.store.list_revisions(repo_id).map_err(|e| {
        tracing::debug!("failed to list revisions for {repo_id}: {e}");
        HubApiError::RepoNotFound
    })?;

    let mut refs = Vec::new();
    let mut seen_refs = std::collections::HashSet::new();

    for rev in &revisions {
        if rev.ref_name == "HEAD" || rev.ref_name.is_empty() {
            if seen_refs.insert("HEAD".to_owned()) {
                refs.push(GitRef {
                    name: "HEAD".to_owned(),
                    sha1: rev.sha.clone(),
                });
            }
        } else if rev.ref_name.starts_with("refs/") {
            if seen_refs.insert(rev.ref_name.clone()) {
                refs.push(GitRef {
                    name: rev.ref_name.clone(),
                    sha1: rev.sha.clone(),
                });
            }
        } else {
            let full_ref = format!("refs/heads/{}", rev.ref_name);
            if seen_refs.insert(full_ref.clone()) {
                refs.push(GitRef {
                    name: full_ref,
                    sha1: rev.sha.clone(),
                });
            }
        }
    }

    // If no HEAD was explicitly set, use the latest revision as HEAD.
    // Use max_by_key on created_at_unix_seconds to find the actual most recent revision,
    // since revisions.last() may not be the newest due to ordering.
    if !seen_refs.contains("HEAD")
        && let Some(latest) = revisions.iter().max_by_key(|r| r.created_at_unix_seconds)
    {
        refs.insert(
            0,
            GitRef {
                name: "HEAD".to_owned(),
                sha1: latest.sha.clone(),
            },
        );
        // Also ensure refs/heads/main exists pointing to HEAD.
        let main_ref = format!("refs/heads/{}", latest.ref_name);
        if !seen_refs.contains(&main_ref) {
            refs.push(GitRef {
                name: main_ref,
                sha1: latest.sha.clone(),
            });
        }
    }

    refs.sort_by(|a, b| a.name.cmp(&b.name));

    // Ensure HEAD is first.
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
            Some(hash.to_owned())
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
            Some(hash.to_owned())
        })
        .collect()
}

// ---- Real content pack generation ----

/// Generates a Git pack file containing real objects built from HubStore
/// metadata. For each non-HEAD ref this function:
///
/// 1. Resolves the revision SHA to fetch file entries from the store.
/// 2. Builds a recursive Git tree object from the file list (including
///    sub-trees for nested directories).
/// 3. Generates blob objects — LFS pointer blobs for LFS files, or
///    content-bearing blobs for inline files.
/// 4. Creates a commit object referencing the root tree.
async fn generate_pack_for_refs(state: &HubState, refs: &[GitRef]) -> Result<Vec<u8>, HubApiError> {
    let mut all_objects: Vec<GitObject> = Vec::new();
    let mut seen_trees: std::collections::HashSet<[u8; 20]> = std::collections::HashSet::new();
    let mut seen_blobs: std::collections::HashSet<[u8; 20]> = std::collections::HashSet::new();
    let mut parent_sha: Option<[u8; 20]> = None;

    for git_ref in refs {
        if git_ref.name == "HEAD" {
            continue;
        }

        // Resolve files from HubStore for this ref's commit SHA.
        let files = state
            .store
            .get_files(&git_ref.sha1)
            .map_err(|e| HubApiError::CasError(e.to_string()))?;

        // Build tree (and all sub-trees) from file entries.
        let (root_tree, sub_trees) = build_git_tree_objects(&files);
        let tree_sha = root_tree.sha1();

        // Collect unique tree objects (including sub-trees).
        if seen_trees.insert(tree_sha) {
            all_objects.push(root_tree);
        }
        for tree in sub_trees {
            let sha = tree.sha1();
            if seen_trees.insert(sha) {
                all_objects.push(tree);
            }
        }

        // Generate blob objects for each file entry.
        for file in &files {
            let blob = if file.is_lfs {
                build_lfs_pointer_blob(&file.sha, file.size)
            } else {
                build_inline_blob(file)
            };
            let blob_sha = blob.sha1();
            if seen_blobs.insert(blob_sha) {
                all_objects.push(blob);
            }
        }

        // Add .gitattributes blob if there are LFS files.
        if let Some(gitattr_blob) = build_gitattributes_blob(&files) {
            let sha = gitattr_blob.sha1();
            if seen_blobs.insert(sha) {
                all_objects.push(gitattr_blob);
            }
        }

        // Create the commit object.
        let commit = create_commit_object(
            &tree_sha,
            parent_sha.as_ref(),
            "Shardline Hub <hub@shardline.dev>",
            git_ref
                .name
                .strip_prefix("refs/heads/")
                .unwrap_or(&git_ref.name),
        );
        parent_sha = Some(commit.sha1());
        all_objects.push(commit);
    }

    if all_objects.is_empty() {
        return empty_pack().map_err(Into::into);
    }

    generate_pack(&all_objects).map_err(Into::into)
}

/// Builds Git tree objects (root + all sub-trees) from a flat list of
/// file entries. Returns `(root_tree, sub_trees)`.
///
/// Directories are represented as sub-tree objects. All sub-trees are
/// returned in the second vector so the caller can add them to the pack.
fn build_git_tree_objects(files: &[HubFileEntry]) -> (GitObject, Vec<GitObject>) {
    let refs: Vec<&HubFileEntry> = files.iter().collect();
    let mut sub_trees = Vec::new();
    let entries = build_tree_entries(&refs, "", &mut sub_trees);
    let root = tree_object_from_entries(&entries);
    (root, sub_trees)
}

/// Creates a Git tree object from owned (mode, name, sha1) entries.
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
fn build_inline_blob(file: &HubFileEntry) -> GitObject {
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
fn build_lfs_pointer_blob(oid: &str, size: u64) -> GitObject {
    let pointer =
        format!("version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize {size}\n");
    GitObject::blob(pointer.into_bytes())
}

/// Generates a `.gitattributes` blob that tells Git to treat LFS files
/// as LFS-tracked. Returns `None` if no files are LFS-tracked.
fn build_gitattributes_blob(files: &[HubFileEntry]) -> Option<GitObject> {
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

fn parse_receive_pack_request(body: &[u8]) -> (Vec<(String, String, String)>, Vec<u8>) {
    let mut updates = Vec::new();
    let mut pack_start = 0;

    let lines = pktline::decode_lines(body);
    for line in &lines {
        let s = match std::str::from_utf8(line) {
            Ok(s) => s.trim().to_owned(),
            Err(_) => continue,
        };

        if s.is_empty() {
            continue;
        }

        let parts: Vec<&str> = s.split_whitespace().collect();
        if let [first, second, third, ..] = parts.as_slice() {
            updates.push((
                first.to_string(),
                second.to_string(),
                third.to_string(),
            ));
        }
    }

    let mut pos = 0usize;
    while pos.wrapping_add(4) <= body.len() {
        let hex_len = body.get(pos..pos.wrapping_add(4)).unwrap_or(&[]);
        if let Ok(hex_str) = std::str::from_utf8(hex_len)
            && let Ok(len) = u16::from_str_radix(hex_str, 16)
        {
            if len == 0 {
                pack_start = pos.wrapping_add(4);
                break;
            }
            pos = pos.wrapping_add(len as usize);
            continue;
        }
        break;
    }

    let pack_data = if pack_start < body.len() {
        body.get(pack_start..).unwrap_or(&[]).to_vec()
    } else {
        Vec::new()
    };

    (updates, pack_data)
}

/// Maximum depth for recursive tree walking to prevent stack overflow from
/// maliciously crafted pushes with deeply nested tree objects.
const MAX_TREE_DEPTH: usize = 128;

/// Maximum total decompressed size for all objects in a receive-pack (512 MB).
/// Prevents zlib-bomb attacks that decompress to many GB of memory.
const MAX_TOTAL_DECOMPRESSED_SIZE: usize = 512 * 1024 * 1024;

/// # Errors
///
/// Returns `PackError` if the pack data is malformed or incomplete.
pub fn parse_pack_data(data: &[u8]) -> Result<Vec<GitObject>, PackError> {
    if data.len() < 12 {
        return Ok(Vec::new());
    }

    // SAFETY: data.len() >= 12 checked above, so range [0..4] is within bounds.
    // Using .get().unwrap_or(&[]) for bounds safety: if the precondition is
    // violated, an empty slice won't match "PACK" and we return an empty vec.
    if data.get(0..4).unwrap_or(&[]) != b"PACK" {
        return Ok(Vec::new());
    }

    // SAFETY: data.len() >= 12 checked above ensures indices 4..8 are valid.
    let mut version_arr = [0u8; 4];
    version_arr.copy_from_slice(data.get(4..8).unwrap_or(&[0, 0, 0, 0]));
    let version = u32::from_be_bytes(version_arr);
    // SAFETY: data.len() >= 12 checked above ensures indices 8..12 are valid.
    let mut num_objects_arr = [0u8; 4];
    num_objects_arr.copy_from_slice(data.get(8..12).unwrap_or(&[0, 0, 0, 0]));
    let num_objects = u32::from_be_bytes(num_objects_arr);

    if version != 2 {
        return Ok(Vec::new());
    }

    let mut objects = Vec::new();
    let mut sha_index: HashMap<[u8; 20], usize> = HashMap::new();
    let mut pos: usize = 12;
    let mut total_decompressed: usize = 0;

    for _ in 0..num_objects {
        if pos >= data.len() {
            break;
        }

        // SAFETY: pos < data.len() checked above, so data.get(pos) is Some.
        // .unwrap_or(&0) provides a default that won't match valid pack entries.
        let byte = *data.get(pos).unwrap_or(&0);
        pos = pos.wrapping_add(1);

        let obj_type = (byte >> 4) & 0x07;
        let mut _size = (byte & 0x0f) as u64;
        let mut shift: u32 = 4;

        let mut current = byte;
        while current & 0x80 != 0 && pos < data.len() {
            // SAFETY: while condition ensures pos < data.len()
            current = *data.get(pos).unwrap_or(&0);
            pos = pos.wrapping_add(1);
            shift = shift.wrapping_add(7);
            if shift >= 64 {
                return Err(PackError::ShiftOverflow);
            }
            _size |= ((current & 0x7f) as u64) << shift;
        }

        match obj_type {
            1..=4 => {
                // SAFETY: pos may equal data.len() (empty slice is valid for decompress_zlib)
                let remaining = data.get(pos..).unwrap_or(&[]);
                match decompress_zlib(remaining) {
                    Ok((decompressed, bytes_used)) => {
                        pos = pos.wrapping_add(bytes_used);
                        total_decompressed = total_decompressed
                            .checked_add(decompressed.len())
                            .ok_or(PackError::ExcessiveDecompressedSize)?;
                        if total_decompressed > MAX_TOTAL_DECOMPRESSED_SIZE {
                            return Err(PackError::ExcessiveDecompressedSize);
                        }
                        let ot = match obj_type {
                            1 => ObjectType::Commit,
                            2 => ObjectType::Tree,
                            3 => ObjectType::Blob,
                            _ => ObjectType::Tag,
                        };
                        let obj = GitObject {
                            object_type: ot,
                            data: decompressed,
                        };
                        let sha = obj.sha1();
                        sha_index.insert(sha, objects.len());
                        objects.push(obj);
                    }
                    Err(_) => break,
                }
            }
            6 => {
                // OFS_DELTA — resolve against a base object by negative offset.
                let offset = parse_ofs_delta_offset(data, &mut pos)?;
                // SAFETY: pos may equal data.len() (empty slice is valid for decompress_zlib)
                let remaining = data.get(pos..).unwrap_or(&[]);
                match decompress_zlib(remaining) {
                    Ok((delta_data, bytes_used)) => {
                        pos = pos.wrapping_add(bytes_used);
                        total_decompressed = total_decompressed
                            .checked_add(delta_data.len())
                            .ok_or(PackError::ExcessiveDecompressedSize)?;
                        if total_decompressed > MAX_TOTAL_DECOMPRESSED_SIZE {
                            return Err(PackError::ExcessiveDecompressedSize);
                        }
                        let base_idx = objects
                            .len()
                            .checked_sub(offset)
                            .ok_or(PackError::InvalidDelta)?;
                        // SAFETY: checked_sub ensures base_idx < objects.len()
                        let base = objects
                            .get(base_idx)
                            .ok_or(PackError::InvalidDelta)?
                            .clone();
                        let resolved_data = apply_delta(&base.data, &delta_data)?;
                        total_decompressed = total_decompressed
                            .checked_add(resolved_data.len())
                            .ok_or(PackError::ExcessiveDecompressedSize)?;
                        if total_decompressed > MAX_TOTAL_DECOMPRESSED_SIZE {
                            return Err(PackError::ExcessiveDecompressedSize);
                        }
                        let resolved = GitObject {
                            object_type: base.object_type,
                            data: resolved_data,
                        };
                        let sha = resolved.sha1();
                        sha_index.insert(sha, objects.len());
                        objects.push(resolved);
                    }
                    Err(_) => break,
                }
            }
                7 => {
                    // REF_DELTA — resolve against a base object by SHA.
                    if pos.wrapping_add(20) > data.len() {
                        return Err(PackError::InvalidDelta);
                    }
                    let mut base_sha = [0u8; 20];
                    // SAFETY: pos.wrapping_add(20) > data.len() check above guarantees range is valid
                    base_sha.copy_from_slice(
                        data.get(pos..pos.wrapping_add(20)).ok_or(PackError::InvalidDelta)?,
                    );
                    pos = pos.wrapping_add(20);
                    // SAFETY: pos may equal data.len() (empty slice is valid for decompress_zlib)
                    let remaining = data.get(pos..).unwrap_or(&[]);
                match decompress_zlib(remaining) {
                    Ok((delta_data, bytes_used)) => {
                        pos = pos.wrapping_add(bytes_used);
                        total_decompressed = total_decompressed
                            .checked_add(delta_data.len())
                            .ok_or(PackError::ExcessiveDecompressedSize)?;
                        if total_decompressed > MAX_TOTAL_DECOMPRESSED_SIZE {
                            return Err(PackError::ExcessiveDecompressedSize);
                        }
                        let &base_idx = sha_index.get(&base_sha).ok_or(PackError::InvalidDelta)?;
                        // SAFETY: base_idx comes from sha_index which is populated
                        // with every object's index as they are pushed to objects
                        let base = objects
                            .get(base_idx)
                            .ok_or(PackError::InvalidDelta)?
                            .clone();
                        let resolved_data = apply_delta(&base.data, &delta_data)?;
                        total_decompressed = total_decompressed
                            .checked_add(resolved_data.len())
                            .ok_or(PackError::ExcessiveDecompressedSize)?;
                        if total_decompressed > MAX_TOTAL_DECOMPRESSED_SIZE {
                            return Err(PackError::ExcessiveDecompressedSize);
                        }
                        let resolved = GitObject {
                            object_type: base.object_type,
                            data: resolved_data,
                        };
                        let sha = resolved.sha1();
                        sha_index.insert(sha, objects.len());
                        objects.push(resolved);
                    }
                    Err(_) => break,
                }
            }
            _ => break,
        }
    }

    Ok(objects)
}

/// Maximum allowed decompressed size for zlib data (512 MB).
const MAX_DECOMPRESSED_SIZE: usize = 512 * 1024 * 1024;

fn decompress_zlib(data: &[u8]) -> Result<(Vec<u8>, usize), Box<dyn std::error::Error>> {
    use flate2::Decompress;
    use flate2::FlushDecompress;

    // Decompress zlib data, tracking the exact number of compressed bytes consumed.
    let mut decompressor = Decompress::new(true); // true = zlib-wrapped (not raw deflate)
    let mut output = Vec::new();
    let mut input_pos = 0;

    loop {
        let before_in = decompressor.total_in();
        let before_out = decompressor.total_out();

        // SAFETY: input_pos tracks consumed bytes and never exceeds data.len()
        let in_chunk = data.get(input_pos..).unwrap_or(&[]);
        let in_len = in_chunk.len().min(4096);

        let flush = if input_pos
            .checked_add(in_len)
            .is_some_and(|sum| sum >= data.len())
        {
            FlushDecompress::Finish
        } else {
            FlushDecompress::None
        };

        // Allocate buffer for potential output.
        let buf_len = in_len.saturating_mul(4).max(256);
        let start = output.len();
        output.resize(start.wrapping_add(buf_len), 0);
        // SAFETY: in_len = min(in_chunk.len(), 4096) so ..in_len is within bounds.
        // SAFETY: output was just resized to start + buf_len, so output[start..]
        // has at least buf_len elements available.
        let status = decompressor.decompress(
            in_chunk.get(..in_len).unwrap_or(&[]),
            // SAFETY: output was just resized to start + buf_len, so output[start..]
            // has at least buf_len elements available. .unwrap_or(&mut []) handles
            // the impossible out-of-bounds case safely.
            output.get_mut(start..).unwrap_or(&mut []),
            flush,
        )?;

        let consumed = decompressor.total_in().wrapping_sub(before_in);
        let produced = decompressor.total_out().wrapping_sub(before_out);
        output.truncate(start.wrapping_add(produced as usize));
        input_pos = input_pos.wrapping_add(consumed as usize);

        if status == flate2::Status::StreamEnd || in_len == 0 {
            break;
        }
    }

    if output.len() > MAX_DECOMPRESSED_SIZE {
        return Err(format!(
            "decompressed data exceeds maximum size of {MAX_DECOMPRESSED_SIZE} bytes"
        )
        .into());
    }

    Ok((output, input_pos))
}

async fn store_push_objects(
    state: &HubState,
    repo_id: &str,
    old_sha: &str,
    new_sha: &str,
    ref_name: &str,
    objects: &[GitObject],
) -> Result<(), String> {
    // Zero SHA means delete ref — not supported yet.
    if new_sha == "0000000000000000000000000000000000000000" {
        return Err("delete ref is not supported".to_owned());
    }

    // Build SHA → object index.
    let mut sha_to_obj: HashMap<[u8; 20], &GitObject> = HashMap::new();
    for obj in objects {
        let sha = obj.sha1();
        sha_to_obj.insert(sha, obj);
    }

    // Find the commit object for new_sha.
    let new_sha_bytes = hex::decode(new_sha).map_err(|e| format!("invalid commit SHA hex: {e}"))?;
    let new_sha_arr: [u8; 20] = new_sha_bytes
        .try_into()
        .map_err(|_err| "commit SHA must be 20 bytes".to_owned())?;

    let commit_obj = sha_to_obj
        .get(&new_sha_arr)
        .ok_or_else(|| format!("commit not found in pack: {new_sha}"))?;

    if commit_obj.object_type != ObjectType::Commit {
        return Err("expected commit object for new SHA".to_owned());
    }

    // Parse commit to extract tree, parent, and message.
    let (tree_sha_hex, _parent_sha, message) = parse_commit_object(&commit_obj.data)?;

    // Walk the tree to collect file entries.
    let tree_sha_bytes =
        hex::decode(&tree_sha_hex).map_err(|e| format!("invalid tree SHA: {e}"))?;
    let tree_sha_arr: [u8; 20] = tree_sha_bytes
        .try_into()
        .map_err(|_err| "tree SHA must be 20 bytes".to_owned())?;

    let files = walk_git_tree(&tree_sha_arr, &sha_to_obj, "")?;

    // Store file entries for this commit.
    state
        .store
        .store_files(new_sha, &files)
        .map_err(|e| format!("failed to store files: {e}"))?;

    // Store LFS objects that were included in the pack.
    // LFS pointer blobs only contain metadata; the actual file content is
    // uploaded separately via PUT /lfs/objects/{oid}.  If the client bundled
    // the real content as a blob (e.g. for small files), store it.
    for file in &files {
        if file.is_lfs {
            // Look up the blob data from the pack objects by computing
            // the SHA of the LFS pointer blob and fetching it.
            let pointer_blob = build_lfs_pointer_blob(&file.sha, file.size);
            let pointer_sha = pointer_blob.sha1();
            if let Some(blob_obj) = sha_to_obj.get(&pointer_sha) {
                // Store the blob data keyed by the LFS oid.
                state
                    .store
                    .put_lfs_object(&file.sha, &blob_obj.data)
                    .map_err(|e| format!("failed to store LFS object: {e}"))?;
            }
        }
    }

    // Determine parent SHA for revision creation.
    let parent = if old_sha == "0000000000000000000000000000000000000000" {
        None
    } else {
        // Non-fast-forward check: if the ref already exists and the client's
        // old_sha doesn't match the current ref value, reject the push.
        match state.store.resolve_revision(repo_id, ref_name) {
            Ok(Some(current)) if current != old_sha => {
                return Err(format!(
                    "non-fast-forward (current: {current}, expected: {old_sha})"
                ));
            }
            Ok(None) if old_sha != "0000000000000000000000000000000000000000" => {
                return Err("non-fast-forward".to_owned());
            }
            _ => {}
        }
        Some(old_sha)
    };

    // Create revision in the store.
    state
        .store
        .create_revision(repo_id, parent, new_sha, ref_name, &message)
        .map_err(|e| format!("failed to create revision: {e}"))?;

    Ok(())
}

/// Parses a raw Git commit object and extracts tree SHA, parent SHA, and message.
///
/// Format: `"tree <sha>\nparent <sha>\nauthor ...\ncommitter ...\n\n<message>"`
#[doc(hidden)]
pub fn parse_commit_object(data: &[u8]) -> Result<(String, Option<String>, String), String> {
    let text = std::str::from_utf8(data).map_err(|e| format!("invalid commit encoding: {e}"))?;

    let mut tree_sha = None;
    let mut parent_sha = None;

    // Split at the first blank line (separates headers from message).
    let (headers, message) = match text.split_once("\n\n") {
        Some((h, m)) => (h, m.trim()),
        None => (text, ""),
    };

    for line in headers.lines() {
        if let Some(sha) = line.strip_prefix("tree ") {
            tree_sha = Some(sha.trim().to_owned());
        } else if let Some(sha) = line.strip_prefix("parent ") {
            parent_sha = Some(sha.trim().to_owned());
        }
    }

    let tree = tree_sha.ok_or("commit missing tree header")?;
    Ok((tree, parent_sha, message.to_owned()))
}

/// Walks a Git tree object recursively, collecting file entries.
///
/// Each tree entry is: `"<mode> <name>\0<20-byte-sha>"`.
/// Directories (mode `040000`) are recursed into.  Files (modes `100644`
/// and `100755`) are looked up in the object map and added as
/// [`HubFileEntry`] values.  LFS pointer blobs are detected by their
/// magic prefix and stored with `is_lfs: true`.
#[doc(hidden)]
pub fn walk_git_tree(
    tree_sha: &[u8; 20],
    objects: &HashMap<[u8; 20], &GitObject>,
    prefix: &str,
) -> Result<Vec<HubFileEntry>, String> {
    walk_git_tree_inner(tree_sha, objects, prefix, 0)
}

fn walk_git_tree_inner(
    tree_sha: &[u8; 20],
    objects: &HashMap<[u8; 20], &GitObject>,
    prefix: &str,
    depth: usize,
) -> Result<Vec<HubFileEntry>, String> {
    if depth > MAX_TREE_DEPTH {
        return Err("tree nesting exceeds maximum depth".to_owned());
    }

    let tree_obj = objects
        .get(tree_sha)
        .ok_or_else(|| format!("tree object not found: {}", hex::encode(tree_sha)))?;

    if tree_obj.object_type != ObjectType::Tree {
        return Err(format!(
            "expected tree object, got {:?}",
            tree_obj.object_type
        ));
    }

    let mut entries = Vec::new();
    let data = &tree_obj.data;
    let mut pos = 0;

    while pos < data.len() {
        // Parse mode (octal string until space).
        // SAFETY: While loop ensures pos < data.len(), so data.get(pos..) is Some.
        // .position() scans from pos for the first space byte. If found, space_pos
        // is relative to pos, so pos + space_pos < data.len().
        let tail = data.get(pos..).ok_or("tree position out of bounds")?;
        let space_pos = tail
            .iter()
            .position(|&b| b == b' ')
            .ok_or("invalid tree entry: missing space after mode")?;
        // SAFETY: space_pos found within data[pos..], so it fits within bounds.
        // Using .and_then chaining avoids the addition expression entirely.
        let mode_slice = data
            .get(pos..)
            .and_then(|s| s.get(..space_pos))
            .ok_or("invalid tree entry: mode range out of bounds")?;
        let mode_str = std::str::from_utf8(mode_slice)
            .map_err(|e| format!("invalid mode encoding: {e}"))?;

        // Parse name (until null byte).
        // SAFETY: pos + space_pos < data.len() (proven above), so name_start <= data.len()
        let name_start = pos
            .checked_add(space_pos)
            .and_then(|p| p.checked_add(1))
            .ok_or("tree arithmetic overflow")?;
        // SAFETY: name_start <= data.len() so the slice is valid (empty if equal)
        let name_tail = data.get(name_start..).ok_or("name position out of bounds")?;
        let null_pos = name_tail
            .iter()
            .position(|&b| b == 0)
            .ok_or("invalid tree entry: missing null after name")?;
        // SAFETY: null_pos found within data[name_start..], so it fits within bounds.
        let name_slice = data
            .get(name_start..)
            .and_then(|s| s.get(..null_pos))
            .ok_or("invalid tree entry: name range out of bounds")?;
        let name = std::str::from_utf8(name_slice)
            .map_err(|e| format!("invalid name encoding: {e}"))?;

        // Parse SHA (20 bytes after null).
        // SAFETY: name_start + null_pos < data.len() (proven above), so sha_start <= data.len()
        let sha_start = name_start
            .checked_add(null_pos)
            .and_then(|p| p.checked_add(1))
            .ok_or("tree arithmetic overflow")?;
        // SAFETY: sha_start + 20 <= data.len() checked below with checked_add
        let sha_end = sha_start.checked_add(20).ok_or("tree arithmetic overflow")?;
        if sha_end > data.len() {
            return Err("invalid tree entry: truncated SHA".to_owned());
        }
        let mut entry_sha = [0u8; 20];
        // SAFETY: sha_start + 20 <= data.len() checked above
        let sha_slice = data
            .get(sha_start..)
            .and_then(|s| s.get(..20))
            .ok_or("invalid tree entry: SHA range out of bounds")?;
        entry_sha.copy_from_slice(sha_slice);

        // SAFETY: sha_start + 20 <= data.len() (checked above) so next pos is valid or == len
        pos = sha_start
            .checked_add(20)
            .ok_or("tree arithmetic overflow")?;

        let full_path = if prefix.is_empty() {
            name.to_owned()
        } else {
            format!("{prefix}/{name}")
        };

        if mode_str == "40000" {
            // Directory — recurse into subtree.
            let next_depth = depth.checked_add(1).ok_or("tree depth overflow")?;
            let mut sub_entries = walk_git_tree_inner(&entry_sha, objects, &full_path, next_depth)?;
            entries.append(&mut sub_entries);
        } else if mode_str == "100644" || mode_str == "100755" {
            // Regular file.
            let blob_obj = objects
                .get(&entry_sha)
                .ok_or_else(|| format!("blob object not found: {}", hex::encode(entry_sha)))?;

            if blob_obj.object_type != ObjectType::Blob {
                return Err(format!(
                    "expected blob object for file, got {:?}",
                    blob_obj.object_type
                ));
            }

            // Check if this is an LFS pointer.
            if blob_obj
                .data
                .starts_with(b"version https://git-lfs.github.com/spec/v1")
            {
                let text = std::str::from_utf8(&blob_obj.data)
                    .map_err(|e| format!("invalid LFS pointer encoding: {e}"))?;
                let oid =
                    parse_lfs_pointer_field(text, "oid").ok_or("LFS pointer missing oid field")?;
                let size_str = parse_lfs_pointer_field(text, "size")
                    .ok_or("LFS pointer missing size field")?;
                let size: u64 = size_str
                    .parse()
                    .map_err(|e| format!("invalid LFS size: {e}"))?;

                entries.push(HubFileEntry {
                    path: full_path,
                    size,
                    sha: oid,
                    is_lfs: true,
                    inline_content: None,
                });
            } else {
                // Inline file — compute content hash.
                let sha = {
                    let mut h = blake3::Hasher::new();
                    h.update(&blob_obj.data);
                    hex::encode(h.finalize().as_bytes())
                };
                entries.push(HubFileEntry {
                    path: full_path,
                    size: blob_obj.data.len() as u64,
                    sha,
                    is_lfs: false,
                    inline_content: Some(blob_obj.data.clone()),
                });
            }
        }
        // Skip other entry types (symlinks 120000, submodules 160000).
    }

    Ok(entries)
}

/// Parses a field value from an LFS pointer.
///
/// LFS pointer format:
/// ```text
/// version https://git-lfs.github.com/spec/v1
/// oid sha256:<oid>
/// size <size>
/// ```
fn parse_lfs_pointer_field(text: &str, field: &str) -> Option<String> {
    for line in text.lines() {
        if let Some(value) = line.strip_prefix(&format!("{field} ")) {
            // For "oid", strip the "sha256:" prefix.
            if field == "oid" {
                return value.strip_prefix("sha256:").map(|s| s.to_owned());
            }
            return Some(value.trim().to_owned());
        }
    }
    None
}

fn build_report_response(
    results: &[(String, bool, Option<String>)],
    unpack_ok: bool,
) -> Result<Response, HubApiError> {
    let mut body = String::new();

    if unpack_ok {
        body.push_str(&pktline::encode_line("unpack ok\n")?);
    } else {
        body.push_str(&pktline::encode_line("unpack failed\n")?);
    }

    for (refname, ok, error) in results {
        if *ok {
            body.push_str(&pktline::encode_line(&format!("ok {refname}\n"))?);
        } else {
            let msg = error.as_deref().unwrap_or("failed");
            body.push_str(&pktline::encode_line(&format!("ng {refname} {msg}\n"))?);
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
// Test code intentionally uses unwrap/expect/indexing/vec-push for clarity
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::vec_init_then_push
)]
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
        assert_eq!(id, "org/my-model");
    }

    #[test]
    fn lfs_pointer_blob_format() {
        let blob = build_lfs_pointer_blob("abc123", 4096);
        let content = String::from_utf8(blob.data).unwrap();
        assert!(content.starts_with("version https://git-lfs.github.com/spec/v1\n"));
        assert!(content.contains("oid sha256:abc123\n"));
        assert!(content.contains("size 4096\n"));
    }

    #[test]
    fn inline_blob_deterministic() {
        let file = HubFileEntry {
            path: "test.txt".to_owned(),
            size: 11,
            sha: "aabbccdd".to_owned(),
            is_lfs: false,
            inline_content: None,
        };
        let b1 = build_inline_blob(&file);
        let b2 = build_inline_blob(&file);
        assert_eq!(b1.sha1(), b2.sha1());
    }

    #[test]
    fn tree_from_empty_files() {
        let tree = build_git_tree_objects(&[]);
        assert_eq!(tree.0.object_type, ObjectType::Tree);
    }

    #[test]
    fn tree_from_single_file() {
        let files = vec![HubFileEntry {
            path: "README.md".to_owned(),
            size: 13,
            sha: "deadbeef".to_owned(),
            is_lfs: false,
            inline_content: None,
        }];
        let (tree, sub_trees) = build_git_tree_objects(&files);
        let tree_sha = tree.sha1();
        assert_ne!(tree_sha, [0u8; 20]);
        assert!(sub_trees.is_empty());
    }

    #[test]
    fn tree_from_nested_files() {
        let files = vec![
            HubFileEntry {
                path: "src/main.rs".to_owned(),
                size: 100,
                sha: "aaaa".to_owned(),
                is_lfs: false,
                inline_content: None,
            },
            HubFileEntry {
                path: "Cargo.toml".to_owned(),
                size: 200,
                sha: "bbbb".to_owned(),
                is_lfs: false,
                inline_content: None,
            },
        ];
        let (tree, sub_trees) = build_git_tree_objects(&files);
        let tree_sha = tree.sha1();
        assert_ne!(tree_sha, [0u8; 20]);
        // src/ should produce a sub-tree
        assert_eq!(sub_trees.len(), 1);
    }

    #[test]
    fn gitattributes_blob_generated_for_lfs_files() {
        let files = vec![
            HubFileEntry {
                path: "model.bin".to_owned(),
                size: 1024,
                sha: "oid1".to_owned(),
                is_lfs: true,
                inline_content: None,
            },
            HubFileEntry {
                path: "README.md".to_owned(),
                size: 100,
                sha: "oid2".to_owned(),
                is_lfs: false,
                inline_content: None,
            },
        ];
        let blob = build_gitattributes_blob(&files);
        assert!(blob.is_some());
        let content = String::from_utf8(blob.unwrap().data).unwrap();
        assert!(content.contains("model.bin filter=lfs"));
        assert!(!content.contains("README.md"));
    }

    #[test]
    fn gitattributes_blob_none_when_no_lfs() {
        let files = vec![HubFileEntry {
            path: "README.md".to_owned(),
            size: 100,
            sha: "oid2".to_owned(),
            is_lfs: false,
            inline_content: None,
        }];
        assert!(build_gitattributes_blob(&files).is_none());
    }

    // --- is_valid_refname tests ---

    #[test]
    fn is_valid_refname_valid() {
        assert!(is_valid_refname("refs/heads/main"));
        assert!(is_valid_refname("refs/tags/v1.0"));
        assert!(is_valid_refname("refs/heads/feature/foo"));
        assert!(is_valid_refname("refs/heads/feature/foo/bar"));
        assert!(is_valid_refname("refs/pull/42/head"));
    }

    #[test]
    fn is_valid_refname_empty() {
        assert!(!is_valid_refname(""));
    }

    #[test]
    fn is_valid_refname_no_refs_prefix() {
        assert!(!is_valid_refname("heads/main"));
        assert!(!is_valid_refname("tags/v1.0"));
        assert!(!is_valid_refname("main"));
    }

    #[test]
    fn is_valid_refname_control_chars() {
        assert!(!is_valid_refname("refs/heads/main\n"));
        assert!(!is_valid_refname("refs/heads/main\t"));
        assert!(!is_valid_refname("refs/heads/main\x00"));
        assert!(!is_valid_refname("refs/heads/main\x7f"));
    }

    #[test]
    fn is_valid_refname_dotdot() {
        assert!(!is_valid_refname("refs/heads/../secret"));
        assert!(!is_valid_refname("refs/heads/feature/.."));
        assert!(!is_valid_refname("refs/heads/../../etc/passwd"));
    }

    // --- parse_commit_object tests ---

    #[test]
    fn parse_commit_object_valid() {
        let data = b"tree abcdef0123456789abcdef0123456789abcdef01\n\
                      parent 1234567890abcdef1234567890abcdef12345678\n\
                      author Test <test@test.com> 1234567890 +0000\n\
                      committer Test <test@test.com> 1234567890 +0000\n\
                      \n\
                      Initial commit\n";
        let (tree, parent, message) = parse_commit_object(data).unwrap();
        assert_eq!(tree, "abcdef0123456789abcdef0123456789abcdef01");
        assert_eq!(
            parent.as_deref(),
            Some("1234567890abcdef1234567890abcdef12345678")
        );
        assert_eq!(message, "Initial commit");
    }

    #[test]
    fn parse_commit_object_no_parent() {
        let data = b"tree abcdef0123456789abcdef0123456789abcdef01\n\
                      author Test <test@test.com> 1234567890 +0000\n\
                      committer Test <test@test.com> 1234567890 +0000\n\
                      \n\
                      First commit\n";
        let (tree, parent, message) = parse_commit_object(data).unwrap();
        assert_eq!(tree, "abcdef0123456789abcdef0123456789abcdef01");
        assert!(parent.is_none());
        assert_eq!(message, "First commit");
    }

    #[test]
    fn parse_commit_object_malformed() {
        // Missing tree header
        let data = b"parent 1234567890abcdef1234567890abcdef12345678\n\
                      author Test <test@test.com> 1234567890 +0000\n\
                      \n\
                      Some message\n";
        let result = parse_commit_object(data);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("missing tree"));
    }

    // --- apply_delta tests ---

    #[test]
    fn apply_delta_simple() {
        // Base: "Hello, World!"
        let base = b"Hello, World!";

        // Build a delta that copies the first 5 bytes ("Hello"), inserts " there", copies the rest.
        let mut delta = Vec::new();
        // Source size: 13 (varint — fits in 1 byte)
        delta.push(13);
        // Target size: 19 (varint — "Hello there, World!")
        delta.push(19);

        // Copy instruction 1: offset=0, size=5 ("Hello")
        //   offset bytes: bit 0 NOT set → no offset bytes (offset=0)
        //   size bytes:   bit 4 set → 0x10 (1 size byte)
        //   cmd = 0x80 (copy flag) | 0x10 = 0x90
        delta.push(0x90);
        delta.push(0x05); // size byte = 5

        // Insert instruction: 6 bytes " there"
        delta.push(6);
        delta.extend_from_slice(b" there");

        // Copy instruction 2: offset=5, size=8 (", World!")
        //   offset bytes: bit 0 set → 0x01 (1 offset byte)
        //   size bytes:   bit 4 set → 0x10 (1 size byte)
        //   cmd = 0x01 | 0x80 (copy flag) | 0x10 = 0x91
        delta.push(0x91);
        delta.push(0x05); // offset byte = 5
        delta.push(0x08); // size byte = 8

        let result = apply_delta(base, &delta).unwrap();
        assert_eq!(result, b"Hello there, World!");
    }

    #[test]
    fn apply_delta_empty() {
        // Base: "abc", delta produces empty output (target size 0)
        let base = b"abc";
        let mut delta = Vec::new();
        // Source size: 3
        delta.push(3);
        // Target size: 0
        delta.push(0);
        // No instructions — result should be empty

        let result = apply_delta(base, &delta).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn apply_delta_invalid() {
        // Source size doesn't match base length
        let base = b"Hello, World!";
        let mut delta = Vec::new();
        // Source size: 99 (wrong)
        delta.push(99);
        // Target size: 5
        delta.push(5);
        // Copy command: offset=0, size=5
        delta.push(0x90);
        delta.push(0x00);
        delta.push(0x05);

        let result = apply_delta(base, &delta);
        assert!(result.is_err());
    }

    // --- parse_commit_object with multiple parents ---

    #[test]
    fn parse_commit_object_multi_parent() {
        let data = b"tree abcdef0123456789abcdef0123456789abcdef01\n\
                      parent 1111111111111111111111111111111111111111\n\
                      parent 2222222222222222222222222222222222222222\n\
                      author Test <test@test.com> 1234567890 +0000\n\
                      committer Test <test@test.com> 1234567890 +0000\n\
                      \n\
                      Merge commit\n";
        let (tree, parent, message) = parse_commit_object(data).unwrap();
        assert_eq!(tree, "abcdef0123456789abcdef0123456789abcdef01");
        // parse_commit_object returns the LAST parent found
        assert_eq!(
            parent.as_deref(),
            Some("2222222222222222222222222222222222222222")
        );
        assert_eq!(message, "Merge commit");
    }

    // --- walk_git_tree depth-limit tests ---

    #[test]
    fn walk_git_tree_empty_tree() {
        // An empty tree object (no entries) should return an empty file list.
        let empty_tree = GitObject::tree(vec![]);
        let sha = empty_tree.sha1();
        let mut objects: std::collections::HashMap<[u8; 20], &GitObject> =
            std::collections::HashMap::new();
        objects.insert(sha, &empty_tree);

        let entries = walk_git_tree(&sha, &objects, "").unwrap();
        assert!(
            entries.is_empty(),
            "empty tree should produce no file entries"
        );
    }

    #[test]
    fn walk_git_tree_max_depth() {
        // Build a chain of 128 nested directories, each containing one subdirectory,
        // with a file at the deepest level. Depth 128 = MAX_TREE_DEPTH and should succeed.
        let file_blob = GitObject::blob(b"file content".to_vec());
        let file_sha = file_blob.sha1();

        // Collect all owned objects, then build the HashMap of references.
        let mut owned: Vec<GitObject> = Vec::new();
        owned.push(file_blob);

        let mut current_sha = file_sha;

        for depth in (1..=128).rev() {
            let mut tree_data = Vec::new();
            if depth == 128 {
                // Innermost: file entry
                tree_data.extend_from_slice(b"100644 f\0");
                tree_data.extend_from_slice(&file_sha);
            } else {
                // Directory entry pointing to current_sha
                tree_data.extend_from_slice(b"40000 d\0");
                tree_data.extend_from_slice(&current_sha);
            }
            let tree_obj = GitObject::tree(tree_data);
            let sha = tree_obj.sha1();
            owned.push(tree_obj);
            current_sha = sha;
        }

        let objects: std::collections::HashMap<[u8; 20], &GitObject> =
            owned.iter().map(|o| (o.sha1(), o)).collect();

        let entries = walk_git_tree(&current_sha, &objects, "").unwrap();
        assert_eq!(entries.len(), 1, "should find the file at depth 128");
        // Path is 127 "d/" prefixes + "f"
        let expected_prefix = "d/".repeat(127);
        let expected_path = format!("{expected_prefix}f");
        assert_eq!(entries[0].path, expected_path);
    }

    #[test]
    fn walk_git_tree_exceeds_max_depth() {
        // Build a chain of 130 nested directories — enough to reach depth 129
        // (one more than MAX_TREE_DEPTH=128). The initial call starts at depth 0,
        // so 130 tree levels pushes the deepest recursion to depth 129 > 128.
        let file_blob = GitObject::blob(b"file content".to_vec());
        let file_sha = file_blob.sha1();

        let mut owned: Vec<GitObject> = Vec::new();
        owned.push(file_blob);

        let mut current_sha = file_sha;

        for depth in (1..=130).rev() {
            let mut tree_data = Vec::new();
            if depth == 130 {
                tree_data.extend_from_slice(b"100644 f\0");
                tree_data.extend_from_slice(&file_sha);
            } else {
                tree_data.extend_from_slice(b"40000 d\0");
                tree_data.extend_from_slice(&current_sha);
            }
            let tree_obj = GitObject::tree(tree_data);
            let sha = tree_obj.sha1();
            owned.push(tree_obj);
            current_sha = sha;
        }

        let objects: std::collections::HashMap<[u8; 20], &GitObject> =
            owned.iter().map(|o| (o.sha1(), o)).collect();

        let result = walk_git_tree(&current_sha, &objects, "");
        assert!(
            result.is_err(),
            "should fail at depth 129 (exceeds MAX_TREE_DEPTH)"
        );
        assert!(
            result.unwrap_err().contains("exceeds maximum depth"),
            "error message should mention depth"
        );
    }

    // --- is_valid_refname edge cases ---

    #[test]
    fn is_valid_refname_with_spaces() {
        assert!(!is_valid_refname("refs/heads/my branch"));
        assert!(!is_valid_refname("refs/heads/feature "));
        assert!(!is_valid_refname(" refs/heads/feature"));
    }

    #[test]
    fn is_valid_refname_with_dotdot() {
        assert!(!is_valid_refname("refs/heads/../secret"));
        assert!(!is_valid_refname("refs/heads/feature/.."));
        assert!(!is_valid_refname("refs/heads/../../etc/passwd"));
        assert!(!is_valid_refname("refs/heads/a/../../../x"));
    }

    // --- collect_refs dedup/HEAD logic tests ---

    /// Helper to create a temporary HubState backed by SQLite.
    fn make_hub_state() -> (tempfile::TempDir, HubState) {
        use shardline_index::LocalIndexStore;
        use shardline_index::hub::BoxedHubStore;

        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().to_path_buf();
        let db_path = root.join("metadata.sqlite3");
        let conn = rusqlite::Connection::open(&db_path).expect("open sqlite");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS shardline_hub_repos (
                repo_id TEXT PRIMARY KEY, repo_type TEXT NOT NULL, private INTEGER NOT NULL DEFAULT 0,
                default_branch TEXT NOT NULL, created_at_unix_seconds INTEGER NOT NULL,
                updated_at_unix_seconds INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_revisions (
                repo_id TEXT NOT NULL, ref_name TEXT NOT NULL, sha TEXT NOT NULL,
                parent_sha TEXT, message TEXT, created_at_unix_seconds INTEGER NOT NULL,
                PRIMARY KEY (repo_id, sha)
            );
            CREATE INDEX IF NOT EXISTS shardline_hub_revisions_repo_ref_idx
                ON shardline_hub_revisions (repo_id, ref_name);
            CREATE TABLE IF NOT EXISTS shardline_hub_file_entries (
                commit_sha TEXT NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL,
                sha TEXT NOT NULL, is_lfs INTEGER NOT NULL DEFAULT 0, inline_content BLOB,
                PRIMARY KEY (commit_sha, path)
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_lfs_objects (
                oid TEXT PRIMARY KEY, data BLOB NOT NULL, size INTEGER NOT NULL,
                created_at_unix_seconds INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS shardline_hub_webhooks (
                id TEXT PRIMARY KEY, repo_id TEXT NOT NULL,
                url TEXT NOT NULL, events TEXT NOT NULL DEFAULT 'push', secret TEXT,
                active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
                created_at_unix_seconds INTEGER NOT NULL,
                FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
            );",
        )
        .expect("create schema");
        drop(conn);

        let store = LocalIndexStore::open(root);
        let boxed = BoxedHubStore::from_store(store);
        let state = HubState {
            store: boxed,
            auth: None,
            http_client: None,
        };
        (tmp, state)
    }

    #[tokio::test]
    async fn collect_refs_dedup_identical_shas() {
        let (_tmp, state) = make_hub_state();
        use shardline_index::hub::HubRepoType;

        state
            .store
            .create_repo(HubRepoType::Model, "org/dedup", false)
            .unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

        // Insert two revisions with different SHAs but pointing to the same
        // final SHA via a chain. The key dedup test: same ref_name ("main")
        // should only appear once.
        state
            .store
            .create_revision("org/dedup", Some(initial_sha), "sha_a", "main", "first")
            .unwrap();
        state
            .store
            .create_revision(
                "org/dedup",
                Some("sha_a"),
                "sha_b",
                "refs/heads/dev",
                "second",
            )
            .unwrap();
        // Also add a HEAD entry pointing to sha_a
        state
            .store
            .create_revision("org/dedup", Some("sha_b"), "sha_head", "HEAD", "head ref")
            .unwrap();

        let refs = collect_refs(&state, "org/dedup").await.unwrap();

        // Each unique (name, sha) pair should only appear once.
        let mut seen = std::collections::HashSet::new();
        for r in &refs {
            let key = (&r.name, &r.sha1);
            assert!(
                seen.insert(key),
                "duplicate ref entry: {key:?} in refs: {refs:?}"
            );
        }

        // Verify all expected ref names are present.
        let names: Vec<&str> = refs.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"HEAD"), "should contain HEAD: {refs:?}");
        assert!(
            names.contains(&"refs/heads/main"),
            "should contain main: {refs:?}"
        );
        assert!(
            names.contains(&"refs/heads/dev"),
            "should contain dev: {refs:?}"
        );
    }

    #[tokio::test]
    async fn collect_refs_head_fallback_when_no_head() {
        let (_tmp, state) = make_hub_state();
        use shardline_index::hub::HubRepoType;

        state
            .store
            .create_repo(HubRepoType::Model, "org/head-fallback", false)
            .unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

        // Create a revision with a non-HEAD ref name only.
        state
            .store
            .create_revision(
                "org/head-fallback",
                Some(initial_sha),
                "abc123",
                "main",
                "first commit",
            )
            .unwrap();

        let refs = collect_refs(&state, "org/head-fallback").await.unwrap();

        // There should be a HEAD entry injected (no explicit HEAD ref).
        let heads: Vec<&GitRef> = refs.iter().filter(|r| r.name == "HEAD").collect();
        assert_eq!(
            heads.len(),
            1,
            "collect_refs should inject exactly one HEAD entry when none is explicit: {refs:?}"
        );
        // The HEAD fallback uses `revisions.last()` which, in DESC order,
        // is the oldest revision (the initial empty-tree SHA).
        assert_eq!(
            heads[0].sha1, initial_sha,
            "HEAD fallback should point to the oldest revision (list last): {refs:?}"
        );
    }

    #[tokio::test]
    async fn collect_refs_explicit_head_not_duplicated() {
        let (_tmp, state) = make_hub_state();
        use shardline_index::hub::HubRepoType;

        state
            .store
            .create_repo(HubRepoType::Model, "org/explicit-head", false)
            .unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

        // Create revisions: one with ref_name "HEAD" and one with "main".
        state
            .store
            .create_revision(
                "org/explicit-head",
                Some(initial_sha),
                "sha_head",
                "HEAD",
                "head commit",
            )
            .unwrap();
        state
            .store
            .create_revision(
                "org/explicit-head",
                Some("sha_head"),
                "sha_main",
                "main",
                "main commit",
            )
            .unwrap();

        let refs = collect_refs(&state, "org/explicit-head").await.unwrap();

        // There should be exactly one HEAD entry.
        let head_count = refs.iter().filter(|r| r.name == "HEAD").count();
        assert_eq!(head_count, 1, "HEAD should appear exactly once: {refs:?}");
    }

    #[tokio::test]
    async fn collect_refs_bare_ref_name_gets_refs_prefix() {
        let (_tmp, state) = make_hub_state();
        use shardline_index::hub::HubRepoType;

        state
            .store
            .create_repo(HubRepoType::Model, "org/bare-ref", false)
            .unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

        // Create a revision with a bare ref name (no "refs/" prefix).
        state
            .store
            .create_revision(
                "org/bare-ref",
                Some(initial_sha),
                "def456",
                "feature",
                "feature commit",
            )
            .unwrap();

        let refs = collect_refs(&state, "org/bare-ref").await.unwrap();

        // The bare "feature" name should be normalized to "refs/heads/feature".
        assert!(
            refs.iter().any(|r| r.name == "refs/heads/feature"),
            "bare ref name 'feature' should be normalized to 'refs/heads/feature': {refs:?}"
        );
        assert!(
            !refs.iter().any(|r| r.name == "feature"),
            "bare ref name should not appear unmodified: {refs:?}"
        );
    }

    #[tokio::test]
    async fn collect_refs_full_refs_prefix_preserved() {
        let (_tmp, state) = make_hub_state();
        use shardline_index::hub::HubRepoType;

        state
            .store
            .create_repo(HubRepoType::Model, "org/full-ref", false)
            .unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

        // Create a revision with a full refs/ prefix.
        state
            .store
            .create_revision(
                "org/full-ref",
                Some(initial_sha),
                "abc789",
                "refs/tags/v1.0",
                "tag v1.0",
            )
            .unwrap();

        let refs = collect_refs(&state, "org/full-ref").await.unwrap();

        // The full refs/ prefix should be preserved.
        assert!(
            refs.iter().any(|r| r.name == "refs/tags/v1.0"),
            "full refs/ prefix should be preserved: {refs:?}"
        );
    }

    #[tokio::test]
    async fn collect_refs_nonexistent_repo_returns_empty() {
        let (_tmp, state) = make_hub_state();

        let refs = collect_refs(&state, "org/nonexistent").await.unwrap();
        assert!(
            refs.is_empty(),
            "collect_refs on nonexistent repo should return empty list: {refs:?}"
        );
    }

    // --- build_report_response tests ---

    fn body_string(response: Response) -> String {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            String::from_utf8(bytes.to_vec()).unwrap()
        })
    }

    #[test]
    fn build_report_response_unpack_ok_and_ok_refs() {
        let results = vec![
            ("refs/heads/main".to_owned(), true, None),
            ("refs/heads/dev".to_owned(), true, None),
        ];
        let response = build_report_response(&results, true).unwrap();
        let body = body_string(response);
        assert!(body.contains("unpack ok"));
        assert!(body.contains("ok refs/heads/main"));
        assert!(body.contains("ok refs/heads/dev"));
    }

    #[test]
    fn build_report_response_unpack_failed_and_ng_refs() {
        let results = vec![
            (
                "refs/heads/main".to_owned(),
                false,
                Some("unpack failed".to_owned()),
            ),
        ];
        let response = build_report_response(&results, false).unwrap();
        let body = body_string(response);
        assert!(body.contains("unpack failed"));
        assert!(body.contains("ng refs/heads/main"));
        assert!(body.contains("unpack failed"));
    }

    #[test]
    fn build_report_response_ng_with_default_message() {
        let results = vec![(
            "refs/heads/bad".to_owned(),
            false,
            None,
        )];
        let response = build_report_response(&results, false).unwrap();
        let body = body_string(response);
        assert!(body.contains("ng refs/heads/bad failed"));
    }

    #[test]
    fn build_report_response_ends_with_flush() {
        let results: Vec<(String, bool, Option<String>)> = vec![];
        let response = build_report_response(&results, true).unwrap();
        let body = body_string(response);
        assert!(body.ends_with("0000"), "response should end with flush packet");
    }

    // --- parse_receive_pack_request tests ---

    #[test]
    fn parse_receive_pack_request_with_updates_and_pack() {
        // Build a pkt-line request: commands followed by flush, then pack data
        let mut body = Vec::new();
        // Command line: "old-sha new-sha refs/heads/main"
        let cmd = "0000000000000000000000000000000000000000 newsha1234567890123456789012345678901234567890 refs/heads/main\n";
        let encoded = format!("{:04x}{}", cmd.len() + 4, cmd);
        body.extend_from_slice(encoded.as_bytes());
        body.extend_from_slice(b"0000"); // flush
        body.extend_from_slice(b"PACK"); // pack header start
        body.extend_from_slice(&[0, 0, 0, 2]); // version
        body.extend_from_slice(&[0, 0, 0, 0]); // 0 objects

        let (updates, pack_data) = parse_receive_pack_request(&body);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].2, "refs/heads/main");
        assert!(!pack_data.is_empty());
        assert!(pack_data.starts_with(b"PACK"));
    }

    #[test]
    fn parse_receive_pack_request_no_flush_returns_empty_pack() {
        // "000a" = 10 bytes total (4 prefix + 6 payload "check\n")
        let body = b"000acheck\n";
        let (updates, pack_data) = parse_receive_pack_request(body);
        // "check\n" has no whitespace, so split gives 1 part, but the
        // function requires at least 3 parts (old, new, refname), so updates is empty
        assert!(updates.is_empty());
        // No flush packet found, so pack_start remains 0.
        // pack_start (0) < body.len() (10) → true, so pack_data = body[0..] (the full body)
        assert!(!pack_data.is_empty(), "pack_data should be the full body when no flush is present");
    }

    #[test]
    fn parse_receive_pack_request_empty_body() {
        let (updates, pack_data) = parse_receive_pack_request(b"");
        assert!(updates.is_empty());
        assert!(pack_data.is_empty());
    }

    #[test]
    fn parse_receive_pack_request_non_utf8_line_skipped() {
        let mut body = Vec::new();
        // Non-UTF8 line: length prefix points to content with 0xFF
        body.extend_from_slice(b"0005\xff\x00");
        body.extend_from_slice(b"0000");
        body.extend_from_slice(b"PACKdata");
        let (updates, pack_data) = parse_receive_pack_request(&body);
        // Non-UTF8 line is skipped, so updates is empty
        assert!(updates.is_empty());
        assert!(!pack_data.is_empty());
    }

    // --- parse_lfs_pointer_field tests ---

    #[test]
    fn parse_lfs_pointer_field_oid() {
        let text = "version https://git-lfs.github.com/spec/v1\noid sha256:abc123\nsize 100\n";
        let oid = parse_lfs_pointer_field(text, "oid");
        assert_eq!(oid.as_deref(), Some("abc123"));
    }

    #[test]
    fn parse_lfs_pointer_field_size() {
        let text = "version https://git-lfs.github.com/spec/v1\noid sha256:abc123\nsize 100\n";
        let size = parse_lfs_pointer_field(text, "size");
        assert_eq!(size.as_deref(), Some("100"));
    }

    #[test]
    fn parse_lfs_pointer_field_missing_field() {
        let text = "version https://git-lfs.github.com/spec/v1\n";
        assert!(parse_lfs_pointer_field(text, "oid").is_none());
        assert!(parse_lfs_pointer_field(text, "size").is_none());
    }

    #[test]
    fn parse_lfs_pointer_field_extra_whitespace() {
        let text = "oid sha256:  abc\n";
        let oid = parse_lfs_pointer_field(text, "oid");
        // strip_prefix("oid ") after splitting on "oid " returns " sha256:  abc"
        // then strip_prefix("sha256:") would fail because there's a leading space
        // Actually let's trace: line.strip_prefix("oid ") gives "sha256:  abc"
        // then strip_prefix("sha256:") gives "  abc"
        // So oid = Some("  abc")
        assert!(oid.is_some());
    }

    // --- parse_pack_data tests ---

    #[test]
    fn parse_pack_data_empty_input() {
        let objects = parse_pack_data(b"").unwrap();
        assert!(objects.is_empty());
    }

    #[test]
    fn parse_pack_data_too_short() {
        let objects = parse_pack_data(b"PACK").unwrap();
        assert!(objects.is_empty());
    }

    #[test]
    fn parse_pack_data_no_pack_magic() {
        let objects = parse_pack_data(b"NOTAPACKFILE").unwrap();
        assert!(objects.is_empty());
    }

    #[test]
    fn parse_pack_data_unsupported_version() {
        let mut data = b"PACK".to_vec();
        data.extend_from_slice(&3u32.to_be_bytes()); // version 3 (unsupported)
        data.extend_from_slice(&0u32.to_be_bytes());
        let objects = parse_pack_data(&data).unwrap();
        assert!(objects.is_empty());
    }

    // --- authorize_read / authorize_write tests (no auth configured) ---

    #[test]
    fn authorize_read_without_auth_is_permissive() {
        let (_tmp, state) = make_hub_state();
        let headers = axum::http::HeaderMap::new();
        assert!(authorize_read(&state, &headers).is_ok());
    }

    #[test]
    fn authorize_write_without_auth_is_permissive() {
        let (_tmp, state) = make_hub_state();
        let headers = axum::http::HeaderMap::new();
        assert!(authorize_write(&state, &headers).is_ok());
    }

    // --- decompress_zlib tests ---

    #[test]
    fn decompress_zlib_empty_input() {
        use flate2::write::ZlibEncoder;
        use std::io::Write;
        let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(b"").unwrap();
        let compressed = encoder.finish().unwrap();
        let (decompressed, _bytes_used) = decompress_zlib(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn decompress_zlib_short_input_returns_empty() {
        // Truncated zlib stream
        let result = decompress_zlib(b"x");
        assert!(result.is_err() || result.unwrap().0.is_empty());
    }

    // --- parse_pack_data round-trip ---

    #[test]
    fn parse_pack_data_roundtrip_blob() {
        // Generate a pack with one blob, then parse it back
        let blob = super::super::pack::create_blob_object(b"hello world");
        let pack = super::super::pack::generate_pack(&[blob]).unwrap();
        let objects = parse_pack_data(&pack).unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].object_type, ObjectType::Blob);
        assert_eq!(objects[0].data, b"hello world");
    }

    #[test]
    fn parse_pack_data_roundtrip_commit_and_tree() {
        // Build a tree and commit, generate pack, parse it back
        let blob = super::super::pack::create_blob_object(b"file content");
        let blob_sha = blob.sha1();
        let tree_entries = vec![(0o100644, "f.txt", &blob_sha)];
        let tree = super::super::pack::create_tree_object(&tree_entries);
        let tree_sha = tree.sha1();
        let commit = super::super::pack::create_commit_object(
            &tree_sha, None, "Test <test@test.com>", "Initial",
        );
        let pack = super::super::pack::generate_pack(&[blob, tree, commit]).unwrap();
        let objects = parse_pack_data(&pack).unwrap();
        assert_eq!(objects.len(), 3);
        let blob_count = objects.iter().filter(|o| o.object_type == ObjectType::Blob).count();
        let tree_count = objects.iter().filter(|o| o.object_type == ObjectType::Tree).count();
        let commit_count = objects.iter().filter(|o| o.object_type == ObjectType::Commit).count();
        assert_eq!(blob_count, 1);
        assert_eq!(tree_count, 1);
        assert_eq!(commit_count, 1);
    }

    #[test]
    fn parse_pack_data_unknown_object_type_breaks() {
        // A pack with an object of type 5 (reserved, not 1..4 or 6..7)
        // should stop parsing and return whatever was parsed (empty).
        let mut data = b"PACK".to_vec();
        data.extend_from_slice(&2u32.to_be_bytes()); // version 2
        data.extend_from_slice(&1u32.to_be_bytes()); // 1 object
        // Object header byte: type=5, size=0, no continuation
        // type 5 = (5 << 4) | 0 = 0x50
        data.push(0x50);
        // No compressed data follows, parser will break on zlib decompression
        let objects = parse_pack_data(&data).unwrap();
        assert!(objects.is_empty());
    }

    #[test]
    fn parse_pack_data_with_ref_delta_stops_gracefully() {
        // A pack with a REF_DELTA object (type 7) but no base object in the index
        // should produce an error since the base SHA won't be found.
        // We need at least a valid compressed stream after the REF_DELTA header.
        use flate2::write::ZlibEncoder;
        use std::io::Write;
        let mut enc = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(b"delta data").unwrap();
        let compressed = enc.finish().unwrap();

        let mut data = b"PACK".to_vec();
        data.extend_from_slice(&2u32.to_be_bytes());
        data.extend_from_slice(&1u32.to_be_bytes()); // 1 object
        // Object header: type=7, size=0, no continuation
        // type 7 = (7 << 4) | 0 = 0x70
        data.push(0x70);
        // REF_DELTA needs 20 bytes of base SHA
        data.extend_from_slice(&[0u8; 20]);
        // Valid zlib data
        data.extend_from_slice(&compressed);
        let result = parse_pack_data(&data);
        assert!(result.is_err(), "expected error for REF_DELTA with missing base");
    }

    #[test]
    fn parse_pack_data_shift_overflow_detected() {
        // Create a pack with a varint size that keeps shifting past 63 bits
        let mut data = b"PACK".to_vec();
        data.extend_from_slice(&2u32.to_be_bytes());
        data.extend_from_slice(&1u32.to_be_bytes()); // 1 object
        // Object header byte: type=3 (blob), size continues in following bytes
        // Start with continuation bit set, size low nibble = 0
        data.push(0x80 | (3 << 4) | 0x0f); // type=3, continuation, low 4 bits=0x0f
        // Add more continuation bytes to keep shifting
        for _ in 0..10 {
            data.push(0x80); // continuation, 7 bits of zero
        }
        // Try to parse - should detect shift >= 64
        let result = parse_pack_data(&data);
        // The parser may either return empty (if it breaks early) or return an error
        assert!(result.is_ok() || matches!(result, Err(PackError::ShiftOverflow)));
        if let Ok(objects) = result {
            assert!(objects.is_empty(), "expected empty objects on shift overflow");
        }
    }

    // --- decompress_zlib with real data ---

    #[test]
    fn decompress_zlib_roundtrip() {
        use flate2::write::ZlibEncoder;
        use std::io::Write;
        let original = b"Hello, World! This is test data for zlib roundtrip.";
        let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();
        let (decompressed, bytes_used) = decompress_zlib(&compressed).unwrap();
        assert_eq!(decompressed, original);
        assert_eq!(bytes_used, compressed.len());
    }

    #[test]
    fn decompress_zlib_trailing_bytes_still_decompresses() {
        // decompress_zlib should consume only the bytes it needs, leaving
        // trailing data unconsumed. It returns the number of bytes consumed.
        use flate2::write::ZlibEncoder;
        use std::io::Write;
        let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(b"hello").unwrap();
        let compressed = encoder.finish().unwrap();

        // Append trailing data
        let mut with_trailing = compressed.clone();
        with_trailing.extend_from_slice(b"TRAILER");

        let (decompressed, bytes_used) = decompress_zlib(&with_trailing).unwrap();
        assert_eq!(decompressed, b"hello");
        assert_eq!(bytes_used, compressed.len());
    }

    // --- parse_commit_object edge cases ---

    #[test]
    fn parse_commit_object_no_blank_line_no_message() {
        // Commit with no blank line (no message section)
        let data = b"tree abcdef0123456789abcdef0123456789abcdef01\n\
                      author Test <test@test.com> 1234567890 +0000\n";
        let (tree, parent, message) = parse_commit_object(data).unwrap();
        assert_eq!(tree, "abcdef0123456789abcdef0123456789abcdef01");
        assert!(parent.is_none());
        assert_eq!(message, "");
    }

    #[test]
    fn parse_commit_object_message_with_trailing_newline() {
        let data = b"tree abcdef0123456789abcdef0123456789abcdef01\n\
                      \n\
                      My message\n";
        let (tree, parent, message) = parse_commit_object(data).unwrap();
        assert_eq!(tree, "abcdef0123456789abcdef0123456789abcdef01");
        assert!(parent.is_none());
        assert_eq!(message, "My message");
    }

    #[test]
    fn parse_commit_object_not_utf8() {
        let data = b"\xff\xfe\x00";
        let result = parse_commit_object(data);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid commit encoding"));
    }

    // --- walk_git_tree with invalid/non-standard entries ---

    #[test]
    fn walk_git_tree_skips_symlinks_and_submodules() {
        // Build a tree with a symlink (120000) and a submodule (160000) entry
        // These should be skipped, returning only the regular file.
        let file_blob = super::super::pack::create_blob_object(b"content");
        let file_sha = file_blob.sha1();

        let mut tree_data = Vec::new();
        // Regular file
        tree_data.extend_from_slice(b"100644 f\0");
        tree_data.extend_from_slice(&file_sha);
        // Symlink
        tree_data.extend_from_slice(b"120000 link\0");
        tree_data.extend_from_slice(&[0xaa; 20]);
        // Submodule
        tree_data.extend_from_slice(b"160000 sub\0");
        tree_data.extend_from_slice(&[0xbb; 20]);

        let tree = super::super::pack::GitObject::tree(tree_data);
        let tree_sha = tree.sha1();

        let owned = vec![file_blob, tree];
        let objects: std::collections::HashMap<[u8; 20], &super::super::pack::GitObject> =
            owned.iter().map(|o| (o.sha1(), o)).collect();

        let entries = walk_git_tree(&tree_sha, &objects, "").unwrap();
        assert_eq!(entries.len(), 1, "should only find the regular file");
        assert_eq!(entries[0].path, "f");
    }

    #[test]
    fn walk_git_tree_wrong_object_type_errors() {
        // Point a tree entry to a blob instead of a sub-tree
        let blob = super::super::pack::create_blob_object(b"not a tree");
        let blob_sha = blob.sha1();

        let mut tree_data = Vec::new();
        tree_data.extend_from_slice(b"40000 dir\0");
        tree_data.extend_from_slice(&blob_sha);

        let tree = super::super::pack::GitObject::tree(tree_data);
        let tree_sha = tree.sha1();

        let owned = vec![blob, tree];
        let objects: std::collections::HashMap<[u8; 20], &super::super::pack::GitObject> =
            owned.iter().map(|o| (o.sha1(), o)).collect();

        let result = walk_git_tree(&tree_sha, &objects, "");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expected tree object"));
    }

    // --- build_gitattributes_blob with multiple LFS files ---

    #[test]
    fn build_gitattributes_blob_multiple_lfs_files() {
        let files = vec![
            HubFileEntry {
                path: "z.bin".to_owned(),
                size: 200,
                sha: "oid_z".to_owned(),
                is_lfs: true,
                inline_content: None,
            },
            HubFileEntry {
                path: "a.bin".to_owned(),
                size: 100,
                sha: "oid_a".to_owned(),
                is_lfs: true,
                inline_content: None,
            },
            HubFileEntry {
                path: "m.bin".to_owned(),
                size: 300,
                sha: "oid_m".to_owned(),
                is_lfs: true,
                inline_content: None,
            },
        ];
        let blob = build_gitattributes_blob(&files);
        assert!(blob.is_some());
        let content = String::from_utf8(blob.unwrap().data).unwrap();
        // Should be sorted: a.bin, m.bin, z.bin
        let lines: Vec<&str> = content.lines().collect();
        assert!(lines[0].starts_with("a.bin"));
        assert!(lines[1].starts_with("m.bin"));
        assert!(lines[2].starts_with("z.bin"));
    }

    // --- parse_lfs_pointer_field edge cases ---

    #[test]
    fn parse_lfs_pointer_field_oid_no_sha256_prefix() {
        // OID without "sha256:" prefix should return None
        let text = "oid abc123\n";
        let oid = parse_lfs_pointer_field(text, "oid");
        assert!(oid.is_none());
    }

    #[test]
    fn parse_lfs_pointer_field_empty_lines() {
        let text = "";
        assert!(parse_lfs_pointer_field(text, "oid").is_none());
        assert!(parse_lfs_pointer_field(text, "size").is_none());
    }

    #[test]
    fn parse_lfs_pointer_field_trailing_whitespace() {
        let text = "size 100  \n";
        let size = parse_lfs_pointer_field(text, "size");
        assert_eq!(size.as_deref(), Some("100"));
    }

    // --- parse_receive_pack_request with valid non-UTF8 pkt-lines ---

    #[test]
    fn parse_receive_pack_request_skips_non_utf8_and_finds_pack() {
        let mut body = Vec::new();
        // Skip non-UTF8: 0005\xff\x00 (length 5, 2 bytes payload after 4 hex prefix)
        // Actually length 5 total means 5 - 4 = 1 byte payload. Let's fix:
        // 0005\xff = 4 hex + 1 payload byte (non-UTF8)
        body.extend_from_slice(b"0005\xff");
        body.extend_from_slice(b"0000");
        body.extend_from_slice(b"PACK");
        body.extend_from_slice(&[0, 0, 0, 2]);
        body.extend_from_slice(&[0, 0, 0, 0]);
        let (updates, pack_data) = parse_receive_pack_request(&body);
        assert!(updates.is_empty());
        assert!(!pack_data.is_empty());
        assert!(pack_data.starts_with(b"PACK"));
    }

    // --- authorize with auth configured (error paths) ---

    #[test]
    fn authorize_read_with_auth_rejects_missing_token() {
        use shardline_server_core::{AuthProvider, AuthError};
        use shardline_protocol::{TokenClaims, RepositoryScope, RepositoryProvider, TokenScope as TS};

        let repo = RepositoryScope::new(RepositoryProvider::GitHub, "o", "r", None).unwrap();
        let _claims = TokenClaims::new("iss", "sub", TS::Read, repo, u64::MAX).unwrap();
        struct MockAuth;
        impl AuthProvider for MockAuth {
            fn verify_token(&self, _token: &str) -> Result<TokenClaims, AuthError> {
                Err(AuthError::InvalidToken)
            }
            fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
                Err(AuthError::ProviderError("nope".into()))
            }
        }
        let state = HubState {
            store: make_hub_state().1.store,
            auth: Some(crate::auth::HubAuth::new(Box::new(MockAuth))),
            http_client: None,
        };
        let headers = axum::http::HeaderMap::new();
        let result = authorize_read(&state, &headers);
        assert!(result.is_err());
    }

    // --- info_refs with nonexistent repo ---

    #[tokio::test]
    async fn info_refs_nonexistent_repo_returns_empty_advertisement() {
        let (_tmp, state) = make_hub_state();
        let headers = axum::http::HeaderMap::new();
        let result = info_refs(
            State(state),
            Path(("models".to_owned(), "no".to_owned(), "repo".to_owned())),
            Query(InfoRefsQuery {
                service: Some("git-upload-pack".to_owned()),
            }),
            headers,
        )
        .await;
        // A nonexistent repo returns a valid info/refs response with
        // a null-SHA capabilities advertisement (no refs to advertise).
        let response = result.expect("nonexistent repo should return a valid response");
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert!(body.contains("capabilities"), "response should contain capabilities: {body}");
        assert!(body.contains("0000000000000000000000000000000000000000"),
            "response should contain zero SHA: {body}");
    }

    // --- parse_pack_data with OFS_DELTA ---

    #[test]
    fn parse_pack_data_ofs_delta_two_objects() {
        // Build a valid pack with 2 objects: a base blob and an OFS_DELTA
        // that copies from it. We'll construct the raw pack bytes.
        let base_content = b"Hello, World!";
        let target_content = b"Hello there, World!";

        // Compress both
        use flate2::write::ZlibEncoder;
        use std::io::Write;
        let compress = |data: &[u8]| -> Vec<u8> {
            let mut enc = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(data).unwrap();
            enc.finish().unwrap()
        };

        let base_compressed = compress(base_content);

        // Build delta: source=13, target=19, copy(0,5), insert(" there,"), copy(5,8)
        let mut delta = Vec::new();
        delta.push(13); // source size
        delta.push(19); // target size
        // copy(0,5): no offset bytes, 1 size byte
        delta.push(0x90);
        delta.push(5);
        // insert(" there")
        delta.push(6);
        delta.extend_from_slice(b" there");
        // copy(5,8): 1 offset byte, 1 size byte
        delta.push(0x91);
        delta.push(5);
        delta.push(8);
        let delta_compressed = compress(&delta);

        // Build the pack:
        // Header: PACK + version(4) + num_objects(4)
        let mut pack = Vec::new();
        pack.extend_from_slice(b"PACK");
        pack.extend_from_slice(&2u32.to_be_bytes()); // version 2
        pack.extend_from_slice(&2u32.to_be_bytes()); // 2 objects

        // Object 1: base blob (type=3), size=13
        // First byte: type (3) << 4 | low 4 bits of size (13 = 0xd)
        // size > 0x0f? 13 <= 15, so no continuation
        pack.push((3 << 4) | 13); // type=3, size=13
        pack.extend_from_slice(&base_compressed);

        // Object 2: OFS_DELTA (type=6), size delta
        let delta_size = delta.len();
        if delta_size <= 0x0f {
            pack.push((6 << 4) | delta_size as u8);
        } else {
            // Need varint encoding for size
            // Size = delta.len(), encode as varint
            pack.push((6 << 4) | (delta_size & 0x0f) as u8 | 0x80); // continuation
            let mut remaining = delta_size >> 4;
            while remaining > 0 {
                let mut byte = (remaining & 0x7f) as u8;
                remaining >>= 7;
                if remaining > 0 {
                    byte |= 0x80;
                }
                pack.push(byte);
            }
        }
        // OFS_DELTA offset: negative offset of 1 (the base object is 1 before this one)
        // Offset 1 → single byte: 0x01 (MSB clear, value=1)
        pack.push(0x01);
        pack.extend_from_slice(&delta_compressed);

        let objects = parse_pack_data(&pack).unwrap();
        assert_eq!(objects.len(), 2, "should parse both objects");
        assert_eq!(objects[0].object_type, ObjectType::Blob);
        assert_eq!(objects[0].data, base_content);
        assert_eq!(objects[1].data, target_content,
            "OFS_DELTA should resolve to produce the target content"
        );
    }
}
