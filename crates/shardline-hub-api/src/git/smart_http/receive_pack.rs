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
use shardline_index::hub::{
    HubFileEntry, HubRefCreateOutcome, HubRefUpdateOutcome, canonical_ref_name,
};
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
        // Multiple Hub revisions may project to the same Git commit. Resolve
        // the named ref's identity rather than reverse-searching history aliases.
        let resolved_old = state
            .store
            .resolve_revision(&repo_id, refname)
            .ok()
            .flatten()
            .filter(|current| identities.get(current) == Some(old_sha))
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
        // Avoid known-conflicting payload writes. The final storage primitive
        // still checks absence atomically, excluding concurrent creators.
        if state
            .store
            .resolve_revision(repo_id, ref_name)
            .map_err(|error| SmartHttpError::CreateRevision(error.to_string()))?
            .is_some()
        {
            return Err(SmartHttpError::NonFastForward(
                "ref already exists".to_owned(),
            ));
        }
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
    if parent.is_none() {
        match state
            .store
            .create_revision_if_absent(repo_id, parent, new_sha, ref_name, &message)
            .map_err(|error| SmartHttpError::CreateRevision(error.to_string()))?
        {
            HubRefCreateOutcome::Created(_) => {}
            HubRefCreateOutcome::AlreadyExists => {
                return Err(SmartHttpError::NonFastForward(
                    "ref already exists".to_owned(),
                ));
            }
            HubRefCreateOutcome::Unsupported => {
                return Err(SmartHttpError::CreateRevision(
                    "atomic ref creation is unsupported by this store".to_owned(),
                ));
            }
        }
    } else {
        match state
            .store
            .update_revision_if_current(repo_id, old_sha, new_sha, ref_name, &message)
            .map_err(|error| SmartHttpError::CreateRevision(error.to_string()))?
        {
            HubRefUpdateOutcome::Updated(_) => {}
            HubRefUpdateOutcome::Conflict => {
                return Err(SmartHttpError::NonFastForward(
                    "ref is missing or no longer matches expected head".to_owned(),
                ));
            }
            HubRefUpdateOutcome::Unsupported => {
                return Err(SmartHttpError::CreateRevision(
                    "atomic ref update is unsupported by this store".to_owned(),
                ));
            }
        }
    }

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

#[cfg(test)]
mod publication_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use shardline_index::hub::*;
    use shardline_index::{LocalIndexStore, LocalIndexStoreError};
    struct DeleteDuringTreeWrite {
        inner: LocalIndexStore,
    }
    impl HubStore for DeleteDuringTreeWrite {
        type Error = LocalIndexStoreError;
        fn create_repo(
            &self,
            repo_type: HubRepoType,
            name: &str,
            private: bool,
        ) -> Result<HubRepo, Self::Error> {
            shardline_index::hub::HubStore::create_repo(&self.inner, repo_type, name, private)
        }
        fn get_repo(&self, repo_id: &str) -> Result<Option<HubRepo>, Self::Error> {
            shardline_index::hub::HubStore::get_repo(&self.inner, repo_id)
        }
        fn list_repos(&self) -> Result<Vec<HubRepo>, Self::Error> {
            shardline_index::hub::HubStore::list_repos(&self.inner)
        }
        fn search_repos(
            &self,
            repo_type: Option<HubRepoType>,
            name_prefix: &str,
            limit: usize,
        ) -> Result<Vec<HubRepo>, Self::Error> {
            shardline_index::hub::HubStore::search_repos(&self.inner, repo_type, name_prefix, limit)
        }
        fn search_repos_with_options(
            &self,
            repo_type: Option<HubRepoType>,
            name_prefix: &str,
            limit: usize,
            options: &HubRepoSearchOptions,
        ) -> Result<Vec<HubRepo>, Self::Error> {
            shardline_index::hub::HubStore::search_repos_with_options(
                &self.inner,
                repo_type,
                name_prefix,
                limit,
                options,
            )
        }
        fn create_revision(
            &self,
            repo_id: &str,
            parent_sha: Option<&str>,
            new_sha: &str,
            ref_name: &str,
            message: &str,
        ) -> Result<HubRevision, Self::Error> {
            shardline_index::hub::HubStore::create_revision(
                &self.inner,
                repo_id,
                parent_sha,
                new_sha,
                ref_name,
                message,
            )
        }
        fn list_refs(&self, repo_id: &str) -> Result<Vec<HubRef>, Self::Error> {
            shardline_index::hub::HubStore::list_refs(&self.inner, repo_id)
        }
        fn delete_ref(
            &self,
            repo_id: &str,
            ref_name: &str,
            expected_sha: &str,
        ) -> Result<(), Self::Error> {
            shardline_index::hub::HubStore::delete_ref(&self.inner, repo_id, ref_name, expected_sha)
        }
        fn list_revisions(&self, repo_id: &str) -> Result<Vec<HubRevision>, Self::Error> {
            shardline_index::hub::HubStore::list_revisions(&self.inner, repo_id)
        }
        fn resolve_revision(
            &self,
            repo_id: &str,
            revision: &str,
        ) -> Result<Option<String>, Self::Error> {
            shardline_index::hub::HubStore::resolve_revision(&self.inner, repo_id, revision)
        }
        fn store_files(&self, commit_sha: &str, files: &[HubFileEntry]) -> Result<(), Self::Error> {
            shardline_index::hub::HubStore::store_files(&self.inner, commit_sha, files)?;
            self.inner.delete_ref(
                "owner/interleave",
                "selected",
                shardline_index::hub::EMPTY_HUB_REVISION,
            )?;
            Ok(())
        }
        fn get_files(&self, commit_sha: &str) -> Result<Vec<HubFileEntry>, Self::Error> {
            shardline_index::hub::HubStore::get_files(&self.inner, commit_sha)
        }
        fn create_webhook(
            &self,
            repo_id: &str,
            url: &str,
            events: &[String],
            secret: Option<&str>,
        ) -> Result<HubWebhook, Self::Error> {
            shardline_index::hub::HubStore::create_webhook(
                &self.inner,
                repo_id,
                url,
                events,
                secret,
            )
        }
        fn list_webhooks(&self, repo_id: &str) -> Result<Vec<HubWebhook>, Self::Error> {
            shardline_index::hub::HubStore::list_webhooks(&self.inner, repo_id)
        }
        fn delete_repo(&self, repo_id: &str) -> Result<(), Self::Error> {
            shardline_index::hub::HubStore::delete_repo(&self.inner, repo_id)
        }
        fn delete_webhook(&self, repo_id: &str, webhook_id: &str) -> Result<(), Self::Error> {
            shardline_index::hub::HubStore::delete_webhook(&self.inner, repo_id, webhook_id)
        }
        fn update_webhook_secret(
            &self,
            repo_id: &str,
            webhook_id: &str,
            secret: Option<&str>,
        ) -> Result<(), Self::Error> {
            shardline_index::hub::HubStore::update_webhook_secret(
                &self.inner,
                repo_id,
                webhook_id,
                secret,
            )
        }
        fn webhooks_for_event(
            &self,
            repo_id: &str,
            event: &str,
        ) -> Result<Vec<HubWebhook>, Self::Error> {
            shardline_index::hub::HubStore::webhooks_for_event(&self.inner, repo_id, event)
        }
        fn update_revision_if_current(
            &self,
            repo_id: &str,
            expected_sha: &str,
            new_sha: &str,
            ref_name: &str,
            message: &str,
        ) -> Result<HubRefUpdateOutcome, Self::Error> {
            self.inner
                .update_revision_if_current(repo_id, expected_sha, new_sha, ref_name, message)
        }
    }
    #[test]
    fn push_rejects_ref_deleted_after_old_head_validation() {
        let (tmp, mut state) = super::super::tests::make_hub_state();
        let inner = LocalIndexStore::open(tmp.path().to_owned());
        inner
            .create_repo(HubRepoType::Model, "owner/interleave", true)
            .unwrap();
        inner
            .create_revision(
                "owner/interleave",
                Some(EMPTY_HUB_REVISION),
                EMPTY_HUB_REVISION,
                "selected",
                "branch",
            )
            .unwrap();
        state.store = BoxedHubStore::from_store(DeleteDuringTreeWrite {
            inner: inner.clone(),
        });
        let blob = crate::git::pack::create_blob_object(b"file");
        let blob_sha = blob.sha1();
        let tree = crate::git::pack::create_tree_object(&[(0o100644, "file", &blob_sha)]);
        let commit = crate::git::pack::create_commit_object(
            &tree.sha1(),
            None,
            "Test <test@test.com>",
            "update",
        );
        let new_sha = hex::encode(commit.sha1());
        let result = store_push_objects(
            &state,
            "owner/interleave",
            EMPTY_HUB_REVISION,
            &new_sha,
            "selected",
            &[blob, tree, commit],
            &AuthorizedRepository::anonymous_full_access(),
        );
        assert!(matches!(result, Err(SmartHttpError::NonFastForward(_))));
        assert!(
            !inner
                .list_refs("owner/interleave")
                .unwrap()
                .iter()
                .any(|r| r.ref_name == "selected")
        );
        assert!(
            !inner
                .list_revisions("owner/interleave")
                .unwrap()
                .iter()
                .any(|r| r.sha == new_sha)
        );
    }
}
