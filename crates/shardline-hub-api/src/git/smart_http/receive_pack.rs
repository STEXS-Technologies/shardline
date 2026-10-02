//! Receive-pack implementation for push.

use axum::{
    body::{Body, to_bytes},
    extract::{Path, State},
    http::{HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
};
use std::collections::HashMap;

use super::super::pack::{GitObject, ObjectType};
use super::super::pktline::{self, FLUSH};
use super::MAX_RECEIVE_PACK_REQUEST_BYTES;
use super::error::SmartHttpError;
use super::pack_parse::parse_pack_data_with_budget;
use super::ref_advertisement::{authorize_write_with_context, is_valid_refname, resolve_repo_id};
use super::tree_walk::{parse_commit_object, walk_git_tree};
use crate::{
    error::HubApiError,
    routes::{HubState, lfs_object_key, require_repository_binding},
};
use shardline_index::hub::{HubFileEntry, canonical_ref_name};
use shardline_protocol::{ShardlineHash, TokenScope};
use shardline_server_core::AuthorizedRepository;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};

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
    body: Body,
) -> Result<Response, HubApiError> {
    let auth_ctx = authorize_write_with_context(&state, &headers)?;
    require_repository_binding(auth_ctx.as_ref(), &ns, &repo)?;

    let repo_id = resolve_repo_id(&repo_type, &ns, &repo);
    // This smart-http path predates the `HubRepository` extractor migration
    // (open item); mint the capability from the already-verified context the
    // same way the extractor does, so the LFS object keys stay namespaced by
    // the verified token's repository scope.
    let capability = match auth_ctx {
        Some(ctx) => AuthorizedRepository::from_verified_context(ctx, TokenScope::Write)?,
        None => AuthorizedRepository::anonymous_full_access(),
    };

    let body = to_bytes(body, MAX_RECEIVE_PACK_REQUEST_BYTES)
        .await
        .map_err(|error| {
            HubApiError::BadRequest(format!("receive-pack request too large: {error}"))
        })?;
    let (updates, packed_tail) = parse_receive_pack_request(&body);
    let pack_start = body.len().saturating_sub(packed_tail.len());

    let updates: Vec<_> = updates
        .into_iter()
        .filter(|(_old, _new, refname)| is_valid_refname(refname))
        .collect();

    if updates.is_empty() {
        return build_report_response(&[], true);
    }

    let projection = super::projection::project_history_async(
        state.clone(),
        repo_id.clone(),
        capability.clone(),
    )
    .await?;
    let new_ids: Vec<_> = updates.iter().map(|(_, sha, _)| sha.clone()).collect();
    // Only read-only projection and pack processing run outside this future.
    // Mutations below remain synchronous so cancellation cannot release the
    // server's maintenance barrier while a detached storage write continues.
    let (identities, objects) = tokio::task::spawn_blocking(move || {
        let bases: HashMap<_, _> = projection
            .objects
            .iter()
            .map(|object| (object.sha1(), object))
            .collect();
        let base_ids: std::collections::HashSet<_> = bases.keys().map(hex::encode).collect();
        let pack_data = body.get(pack_start..).unwrap_or(&[]);
        let has_updates = new_ids
            .iter()
            .any(|sha| sha != "0000000000000000000000000000000000000000");
        let parsed = if !has_updates
            || (pack_data.is_empty()
                && new_ids.iter().all(|sha| {
                    sha == "0000000000000000000000000000000000000000" || base_ids.contains(sha)
                })) {
            Ok(Vec::new())
        } else {
            parse_pack_data_with_budget(pack_data, &bases, super::limits::MAX_GIT_PROJECTED_BYTES)
        };
        let parsed = parsed.map(|mut objects| {
            let mut seen: std::collections::HashSet<_> =
                objects.iter().map(GitObject::sha1).collect();
            for object in projection.objects {
                if seen.insert(object.sha1()) {
                    objects.push(object);
                }
            }
            objects
        });
        (projection.identities, parsed)
    })
    .await
    .map_err(|e| HubApiError::BadRequest(format!("pack worker failed: {e}")))?;
    let objects = match objects {
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
        let resolved_old = identities
            .iter()
            .find_map(|(hub, git)| (git == old_sha).then(|| hub.clone()))
            .unwrap_or_else(|| old_sha.clone());
        let result = if new_sha == "0000000000000000000000000000000000000000" {
            delete_push_ref(&state, &repo_id, &resolved_old, refname)
        } else {
            store_push_objects(
                &state,
                &repo_id,
                &resolved_old,
                new_sha,
                refname,
                &objects,
                &capability,
            )
        };
        match result {
            Ok(()) => results.push((refname.clone(), true, None)),
            Err(e) => results.push((refname.clone(), false, Some(e.to_string()))),
        }
    }

    build_report_response(&results, true)
}

pub(super) fn parse_receive_pack_request(body: &[u8]) -> (Vec<(String, String, String)>, &[u8]) {
    let mut updates = Vec::new();
    let mut pack_start = 0;

    let lines = pktline::decode_lines(body);
    for line in &lines {
        let s = match std::str::from_utf8(line) {
            Ok(s) => s
                .split_once('\0')
                .map_or(s, |(command, _)| command)
                .trim()
                .to_owned(),
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
        body.get(pack_start..).unwrap_or(&[])
    } else {
        &[]
    };

    (updates, pack_data)
}

fn store_push_objects(
    state: &HubState,
    repo_id: &str,
    old_sha: &str,
    new_sha: &str,
    ref_name: &str,
    objects: &[GitObject],
    auth: &AuthorizedRepository,
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

    // Cap commit message length to prevent database bloat and log injection,
    // matching the NDJSON commit API limit. Use char-boundary-safe truncation
    // to avoid panicking on multi-byte UTF-8 characters.
    let message: String = message
        .chars()
        .take(crate::commit::MAX_COMMIT_MSG_LEN)
        .collect();

    // Walk the tree to collect file entries.
    let tree_sha_bytes =
        hex::decode(&tree_sha_hex).map_err(|e| SmartHttpError::InvalidTreeSha(e.to_string()))?;
    let tree_sha_arr: [u8; 20] = tree_sha_bytes
        .try_into()
        .map_err(|_err| SmartHttpError::TreeShaMustBe20Bytes)?;

    let files = walk_git_tree(&tree_sha_arr, &sha_to_obj, "")?;

    // Determine parent SHA for revision creation.
    //
    // This MUST happen before any file-entry / LFS-object persistence: a
    // non-fast-forward push is rejected outright, and a rejected push must not
    // leave write side effects behind (orphaned file entries keyed by a commit
    // SHA no revision will ever reference, and LFS objects nobody will read).
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

    // Build an O(1) index of blob content (keyed by its sha256, the LFS OID)
    // once, before resolving any per-file LFS content. This avoids re-hashing
    // every pack blob for every LFS file (previously O(files × blobs)). The map
    // borrows the parsed pack objects and lives only as long as `objects`.
    let content_by_sha256: HashMap<String, &GitObject> = objects
        .iter()
        .filter(|obj| obj.object_type == ObjectType::Blob)
        .map(|obj| (content_sha256(&obj.data), obj))
        .collect();

    // A pointer is metadata, never payload. Validate all referenced LFS
    // payloads before persisting this tree or any inline content.
    let mut validated_lfs = HashMap::new();
    for file in files.iter().filter(|file| file.is_lfs) {
        if let Some(previous_size) = validated_lfs.insert(&file.sha, file.size) {
            if previous_size != file.size {
                return Err(SmartHttpError::StoreLfsObject(
                    "inconsistent LFS sizes".to_owned(),
                ));
            }
            continue;
        }
        let key = lfs_object_key(&file.sha, auth)
            .map_err(|e| SmartHttpError::StoreLfsObject(e.to_string()))?;
        let existing = state
            .object_store
            .metadata(&key)
            .map_err(|e| SmartHttpError::StoreLfsObject(e.to_string()))?;
        if let Some(metadata) = existing {
            if metadata.length() != file.size {
                return Err(SmartHttpError::StoreLfsObject(
                    "LFS size mismatch".to_owned(),
                ));
            }
            use sha2::Digest;
            let mut digest = sha2::Sha256::new();
            let mut offset = 0;
            while offset < file.size {
                let end = offset.saturating_add(1024 * 1024).min(file.size);
                let range = shardline_protocol::ByteRange::new(offset, end.saturating_sub(1))
                    .map_err(|e| SmartHttpError::StoreLfsObject(e.to_string()))?;
                let chunk = state
                    .object_store
                    .read_range(&key, range)
                    .map_err(|e| SmartHttpError::StoreLfsObject(e.to_string()))?;
                if chunk.len() as u64 != end.saturating_sub(offset) {
                    return Err(SmartHttpError::StoreLfsObject(
                        "truncated LFS payload".to_owned(),
                    ));
                }
                digest.update(&chunk);
                offset = end;
            }
            if hex::encode(digest.finalize()) != file.sha {
                return Err(SmartHttpError::StoreLfsObject(
                    "LFS digest mismatch".to_owned(),
                ));
            }
        } else {
            let blob = find_lfs_blob(file, &sha_to_obj, &content_by_sha256)
                .ok_or_else(|| SmartHttpError::LfsContentNotFoundInPack(file.sha.clone()))?;
            let integrity = ObjectIntegrity::new(
                ShardlineHash::from_bytes(*blake3::hash(&blob.data).as_bytes()),
                file.size,
            );
            state
                .object_store
                .put_if_absent(&key, ObjectBody::from_slice(&blob.data), &integrity)
                .map_err(|e| SmartHttpError::StoreLfsObject(e.to_string()))?;
        }
    }

    // Keep inline bytes available to both Hub resolve and later NDJSON edits.
    // Exact Git packs retain modes/authorship; this shared CAS retains content.
    let inline_by_hash: HashMap<_, _> = objects
        .iter()
        .filter(|obj| obj.object_type == ObjectType::Blob)
        .map(|obj| (blake3::hash(&obj.data).to_hex().to_string(), obj))
        .collect();
    for file in files.iter().filter(|file| !file.is_lfs) {
        let blob = inline_by_hash
            .get(&file.sha)
            .ok_or_else(|| SmartHttpError::BlobObjectNotFound(file.sha.clone()))?;
        let key = lfs_object_key(&file.sha, auth)
            .map_err(|e| SmartHttpError::StoreLfsObject(e.to_string()))?;
        let integrity = ObjectIntegrity::new(
            ShardlineHash::from_bytes(*blake3::hash(&blob.data).as_bytes()),
            file.size,
        );
        state
            .object_store
            .put_if_absent(&key, ObjectBody::from_slice(&blob.data), &integrity)
            .map_err(|e| SmartHttpError::StoreLfsObject(e.to_string()))?;
    }

    state
        .store
        .store_files(new_sha, &files)
        .map_err(|e| SmartHttpError::StoreFiles(e.to_string()))?;

    super::projection::archive_objects(state, repo_id, new_sha, objects, auth)
        .map_err(|e| SmartHttpError::StoreFiles(e.to_string()))?;

    // Create revision in the store.
    state
        .store
        .create_revision(repo_id, parent, new_sha, ref_name, &message)
        .map_err(|e| SmartHttpError::CreateRevision(e.to_string()))?;

    Ok(())
}

/// Finds actual payload by SHA256 and size; pointer bytes are never content.
pub(super) fn find_lfs_blob<'obj>(
    file: &HubFileEntry,
    _sha_to_obj: &HashMap<[u8; 20], &'obj GitObject>,
    content_by_sha256: &HashMap<String, &'obj GitObject>,
) -> Option<&'obj GitObject> {
    content_by_sha256
        .get(&file.sha)
        .copied()
        .filter(|blob| blob.data.len() as u64 == file.size)
}

/// Computes the lowercase hex sha256 of `data` (the LFS OID format).
fn content_sha256(data: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(data))
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
