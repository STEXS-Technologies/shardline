//! Receive-pack implementation for push.

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
};
use std::collections::HashMap;

use super::super::pack::{GitObject, ObjectType};
use super::super::pktline::{self, FLUSH};
use super::error::SmartHttpError;
use super::pack_parse::parse_pack_data;
use super::ref_advertisement::{authorize_write, is_valid_refname, resolve_repo_id};
use super::tree_walk::{parse_commit_object, walk_git_tree};
use super::upload_pack::build_lfs_pointer_blob;
use crate::{error::HubApiError, routes::HubState};
use shardline_index::hub::canonical_ref_name;
use shardline_protocol::ShardlineHash;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};

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

    let has_object_updates = updates
        .iter()
        .any(|(_, new_sha, _)| new_sha != "0000000000000000000000000000000000000000");
    let objects = if has_object_updates {
        match parse_pack_data(&pack_data) {
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
        }
    } else {
        Vec::new()
    };

    let mut results = Vec::new();

    for (old_sha, new_sha, refname) in &updates {
        let result = if new_sha == "0000000000000000000000000000000000000000" {
            delete_push_ref(&state, &repo_id, old_sha, refname)
        } else {
            store_push_objects(&state, &repo_id, old_sha, new_sha, refname, &objects).await
        };
        match result {
            Ok(()) => results.push((refname.clone(), true, None)),
            Err(e) => results.push((refname.clone(), false, Some(e.to_string()))),
        }
    }

    build_report_response(&results, true)
}

pub(super) fn parse_receive_pack_request(body: &[u8]) -> (Vec<(String, String, String)>, Vec<u8>) {
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
            updates.push((first.to_string(), second.to_string(), third.to_string()));
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

async fn store_push_objects(
    state: &HubState,
    repo_id: &str,
    old_sha: &str,
    new_sha: &str,
    ref_name: &str,
    objects: &[GitObject],
) -> Result<(), SmartHttpError> {
    // Build SHA → object index.
    let mut sha_to_obj: HashMap<[u8; 20], &GitObject> = HashMap::new();
    for obj in objects {
        let sha = obj.sha1();
        sha_to_obj.insert(sha, obj);
    }

    // Find the commit object for new_sha.
    let new_sha_bytes =
        hex::decode(new_sha).map_err(|e| SmartHttpError::InvalidCommitShaHex(e.to_string()))?;
    let new_sha_arr: [u8; 20] = new_sha_bytes
        .try_into()
        .map_err(|_err| SmartHttpError::CommitShaMustBe20Bytes)?;

    let commit_obj = sha_to_obj
        .get(&new_sha_arr)
        .ok_or_else(|| SmartHttpError::CommitNotFoundInPack(new_sha.to_owned()))?;

    if commit_obj.object_type != ObjectType::Commit {
        return Err(SmartHttpError::ExpectedCommitObject);
    }

    // Parse commit to extract tree, parent, and message.
    let (tree_sha_hex, _parent_sha, message) = parse_commit_object(&commit_obj.data)?;

    // Walk the tree to collect file entries.
    let tree_sha_bytes =
        hex::decode(&tree_sha_hex).map_err(|e| SmartHttpError::InvalidTreeSha(e.to_string()))?;
    let tree_sha_arr: [u8; 20] = tree_sha_bytes
        .try_into()
        .map_err(|_err| SmartHttpError::TreeShaMustBe20Bytes)?;

    let files = walk_git_tree(&tree_sha_arr, &sha_to_obj, "")?;

    // Store file entries for this commit.
    state
        .store
        .store_files(new_sha, &files)
        .map_err(|e| SmartHttpError::StoreFiles(e.to_string()))?;

    // Store LFS objects that were included in the pack.
    // LFS pointer blobs only contain metadata; the actual file content is
    // uploaded separately via PUT /lfs/objects/{oid}.  If the client bundled
    // the real content as a blob (e.g. for small files), store it via
    // ObjectStore rather than Postgres BYTEA.
    for file in &files {
        if file.is_lfs {
            // Look up the blob data from the pack objects by computing
            // the SHA of the LFS pointer blob and fetching it.
            let pointer_blob = build_lfs_pointer_blob(&file.sha, file.size);
            let pointer_sha = pointer_blob.sha1();
            if let Some(blob_obj) = sha_to_obj.get(&pointer_sha) {
                let key = ObjectKey::parse(&format!("lfs/{}", file.sha))
                    .map_err(|e| SmartHttpError::StoreLfsObject(e.to_string()))?;
                let object_body = ObjectBody::from_slice(&blob_obj.data);
                let integrity = ObjectIntegrity::new(
                    ShardlineHash::from_bytes(*blake3::hash(&blob_obj.data).as_bytes()),
                    blob_obj.data.len() as u64,
                );
                state
                    .object_store
                    .put_if_absent(&key, object_body, &integrity)
                    .map_err(|e| SmartHttpError::StoreLfsObject(e.to_string()))?;
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
                return Err(SmartHttpError::NonFastForward(format!(
                    "non-fast-forward (current: {current}, expected: {old_sha})"
                )));
            }
            Ok(None) if old_sha != "0000000000000000000000000000000000000000" => {
                return Err(SmartHttpError::NonFastForward(
                    "non-fast-forward".to_owned(),
                ));
            }
            _ => {}
        }
        Some(old_sha)
    };

    // Create revision in the store.
    state
        .store
        .create_revision(repo_id, parent, new_sha, ref_name, &message)
        .map_err(|e| SmartHttpError::CreateRevision(e.to_string()))?;

    Ok(())
}

fn delete_push_ref(
    state: &HubState,
    repo_id: &str,
    old_sha: &str,
    ref_name: &str,
) -> Result<(), SmartHttpError> {
    if old_sha == "0000000000000000000000000000000000000000" {
        return Err(SmartHttpError::CannotDeleteNonExistentRef);
    }
    state
        .store
        .delete_ref(repo_id, canonical_ref_name(ref_name), old_sha)
        .map_err(|e| SmartHttpError::DeleteRef(e.to_string()))
}

pub(super) fn build_report_response(
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
