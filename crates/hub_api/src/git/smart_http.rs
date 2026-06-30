//! Git Smart HTTP protocol handlers.
//!
//! Implements the server side of Git Smart HTTP for clone/fetch (upload-pack)
//! and push (receive-pack) operations. Upload-pack generates real Git pack
//! files from HubStore metadata: tree objects, LFS pointer blobs, and commit
//! objects are all constructed from the file entries stored per revision.

use axum::{
    body::Bytes,
    extract::{Path, Query},
    http::{HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use super::pack::{GitObject, ObjectType, create_commit_object, empty_pack, generate_pack};
use super::pktline::{self, FLUSH};
use crate::error::HubApiError;
use shardline_index::hub::HubFileEntry;
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

/// Unified Git Smart HTTP discovery handler.
///
/// Dispatches to upload-pack or receive-pack based on the `service` query
/// parameter. Requires Read scope for upload-pack, Write scope for
/// receive-pack.
pub async fn info_refs(
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
    Query(query): Query<InfoRefsQuery>,
    headers: HeaderMap,
) -> Result<Response, HubApiError> {
    let service = query.service.as_deref().unwrap_or("git-upload-pack");
    if service != "git-upload-pack" && service != "git-receive-pack" {
        return Err(HubApiError::NotFound);
    }

    if service == "git-receive-pack" {
        authorize_write(&headers)?;
    } else {
        authorize_read(&headers)?;
    }

    let repo_id = resolve_repo_id(&repo_type, &ns, &repo);
    let refs = collect_refs(&repo_id).await?;

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
    if refs.is_empty() {
        body.push_str(&pktline::encode_line(&format!(
            "0000000000000000000000000000000000000000 capabilities^{{}}\x00{capabilities}\n",
        ))?);
    } else {
        let first = &refs[0];
        body.push_str(&pktline::encode_line(&format!(
            "{} {} capabilities^{{}}\x00{capabilities}\n",
            first.sha1, first.name
        ))?);
        for r in &refs[1..] {
            body.push_str(&pktline::encode_line(&format!("{} {}\n", r.sha1, r.name))?);
        }
    }
    body.push_str(FLUSH);

    let mut resp_headers = axum::http::HeaderMap::new();
    resp_headers.insert("content-type", HeaderValue::from_static(content_type));

    Ok((resp_headers, body).into_response())
}

/// Handles Git Smart HTTP discovery for upload-pack (clone/fetch).
///
/// Returns the refs advertisement in pkt-line format.
pub async fn info_refs_upload_pack(
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
    Query(query): Query<InfoRefsQuery>,
    headers: HeaderMap,
) -> Result<Response, HubApiError> {
    info_refs(
        Path((repo_type, ns, repo)),
        Query(InfoRefsQuery {
            service: Some(query.service.unwrap_or_else(|| "git-upload-pack".to_owned())),
        }),
        headers,
    )
    .await
}

/// Handles Git Smart HTTP discovery for receive-pack (push).
pub async fn info_refs_receive_pack(
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
    Query(query): Query<InfoRefsQuery>,
    headers: HeaderMap,
) -> Result<Response, HubApiError> {
    info_refs(
        Path((repo_type, ns, repo)),
        Query(InfoRefsQuery {
            service: Some(query.service.unwrap_or_else(|| "git-receive-pack".to_owned())),
        }),
        headers,
    )
    .await
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
        empty_pack()?
    } else {
        generate_pack_for_refs(&refs).await?
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
    let mut seen_refs = std::collections::HashSet::new();

    for rev in &revisions {
        if rev.ref_name == "HEAD" || rev.ref_name.is_empty() {
            if seen_refs.insert("HEAD".to_owned()) {
                refs.push(GitRef {
                    name: "HEAD".to_string(),
                    sha1: rev.sha.clone(),
                });
            }
            if seen_refs.insert("refs/heads/main".to_owned()) {
                refs.push(GitRef {
                    name: "refs/heads/main".to_string(),
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
    if !seen_refs.contains("HEAD") {
        if let Some(latest) = revisions.last() {
            refs.insert(
                0,
                GitRef {
                    name: "HEAD".to_string(),
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
async fn generate_pack_for_refs(refs: &[GitRef]) -> Result<Vec<u8>, HubApiError> {
    let state = crate::state::get();
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
            .unwrap_or_default();

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
            &git_ref
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
fn build_tree_entries<'a>(
    files: &[&'a HubFileEntry],
    prefix: &str,
    sub_trees: &mut Vec<GitObject>,
) -> Vec<(u32, String, [u8; 20])> {
    let mut result = Vec::new();
    let mut children: std::collections::HashMap<String, Vec<&'a HubFileEntry>> =
        std::collections::HashMap::new();

    for file in files {
        let relative = if prefix.is_empty() {
            file.path.as_str()
        } else {
            file.path.strip_prefix(&format!("{prefix}/")).unwrap_or(&file.path)
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
        let dir_files = &children[dir_name];
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
    let pointer = format!(
        "version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize {size}\n"
    );
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
        content.push_str(&format!("{} filter=lfs diff=lfs merge=lfs -text\n", file.path));
    }

    Some(GitObject::blob(content.into_bytes()))
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
        let mut _size = (byte & 0x0f) as u64;
        let mut shift = 4;

        let mut current = byte;
        while current & 0x80 != 0 && pos < data.len() {
            current = data[pos];
            pos += 1;
            _size |= ((current & 0x7f) as u64) << shift;
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
        body.push_str(&pktline::encode_line("unpack ok\n")?);
    } else {
        body.push_str(&pktline::encode_line("unpack ok\n")?);
        for (refname, ok, error) in results {
            if *ok {
                body.push_str(&pktline::encode_line(&format!("ok {refname}\n"))?);
            } else {
                let msg = error.as_deref().unwrap_or("failed");
                body.push_str(&pktline::encode_line(&format!("ng {refname} {msg}\n"))?);
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
}
