//! Exact, repository-scoped Git object identities for Hub path metadata.

use std::collections::{BTreeMap, HashMap};

use shardline_index::hub::HubFileEntry;
use shardline_server_core::AuthorizedRepository;

use super::{
    super::pack::{GitObject, ObjectType},
    identity::IDENTITY_ADMISSION,
    limits::MAX_GIT_PROJECTED_BYTES,
    pack_parse::parse_pack_data_with_budget,
    projection::{metadata_tree, projected_file_key, read_member_archive_key, read_object_bounded},
    tree_walk::parse_commit_object,
    upload_pack::build_lfs_pointer_blob,
};
use crate::{error::HubApiError, routes::HubState};

fn failure(error: impl std::fmt::Display) -> HubApiError {
    HubApiError::CasError(error.to_string())
}

pub(crate) struct GitPathMetadata {
    pub oid: String,
    pub mode: String,
    pub xet_hash: Option<String>,
}

pub(crate) async fn xet_file_hash(
    state: HubState,
    auth: AuthorizedRepository,
    sha256: String,
    size: u64,
) -> Result<Option<String>, HubApiError> {
    let permit = IDENTITY_ADMISSION.acquire().await.map_err(failure)?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        verified_xet_hash(&state, &auth, &sha256, size)
    })
    .await
    .map_err(failure)?
}

pub(super) fn verified_xet_hash(
    state: &HubState,
    auth: &AuthorizedRepository,
    sha256: &str,
    size: u64,
) -> Result<Option<String>, HubApiError> {
    let Some(scope) = auth.repository() else {
        return Ok(None);
    };
    let Some(identity) = state
        .store
        .lookup_xet_file_identity(scope, sha256)
        .map_err(failure)?
    else {
        return Ok(None);
    };
    if identity.total_bytes != size
        || identity.sha256 != sha256
        || identity.repository_key()
            != shardline_index::XetFileIdentity::repository_key_for_scope(scope)
        || hex::decode(&identity.file_id).map_or(true, |bytes| bytes.len() != 32)
    {
        return Err(failure("Hub Xet identity does not match file metadata"));
    }
    Ok(Some(identity.file_id))
}

/// Keep synchronous object reads and hashing off async executor threads, and
/// retain the shared Git admission permit if the requesting task is cancelled.
pub(crate) async fn path_oids(
    state: HubState,
    repo_id: String,
    revision: String,
    files: Vec<HubFileEntry>,
    paths: Vec<String>,
    auth: AuthorizedRepository,
) -> Result<HashMap<String, GitPathMetadata>, HubApiError> {
    let permit = IDENTITY_ADMISSION.acquire().await.map_err(failure)?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut result = path_oids_sync(&state, &repo_id, &revision, &files, &paths, &auth)?;
        for file in &files {
            if file.is_lfs
                && let Some(metadata) = result.get_mut(&file.path)
            {
                metadata.xet_hash = verified_xet_hash(&state, &auth, &file.sha, file.size)?;
            }
        }
        Ok(result)
    })
    .await
    .map_err(failure)?
}

/// Read only hash-verified tree metadata after the caller ensures the public
/// commit identity exists. Tree listing does not need payload/Xet lookups.
pub(crate) async fn tree_oids(
    state: HubState,
    repo: String,
    revision: String,
    paths: Vec<String>,
    auth: AuthorizedRepository,
) -> Result<HashMap<String, GitPathMetadata>, HubApiError> {
    let permit = IDENTITY_ADMISSION.acquire().await.map_err(failure)?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        path_oids_sync(&state, &repo, &revision, &[], &paths, &auth)
    })
    .await
    .map_err(failure)?
}

fn archived_object(
    state: &HubState,
    repo: &str,
    oid: &str,
    auth: &AuthorizedRepository,
) -> Result<GitObject, HubApiError> {
    optional_archived_object(state, repo, oid, auth)?
        .ok_or_else(|| failure("original Git object archive is unavailable"))
}

fn optional_archived_object(
    state: &HubState,
    repo: &str,
    oid: &str,
    auth: &AuthorizedRepository,
) -> Result<Option<GitObject>, HubApiError> {
    // Root revision selection is authorized by the caller. Subsequent OIDs
    // come exclusively from the hash-verified commit and tree objects below.
    let key = read_member_archive_key(state, repo, oid, auth)?;
    let Some(bytes) = read_object_bounded(state, &key, MAX_GIT_PROJECTED_BYTES)? else {
        return Ok(None);
    };
    let mut objects = parse_pack_data_with_budget(&bytes, &HashMap::new(), MAX_GIT_PROJECTED_BYTES)
        .map_err(failure)?;
    if objects.len() != 1 {
        return Err(failure("Git archive must contain exactly one object"));
    }
    let object = objects.pop().ok_or_else(|| failure("empty Git archive"))?;
    if hex::encode(object.sha1()) != oid {
        return Err(failure("archived Git object identity mismatch"));
    }
    Ok(Some(object))
}

fn tree_with_repair(
    state: &HubState,
    repo: &str,
    revision: &str,
    oid: &str,
    auth: &AuthorizedRepository,
    repair: bool,
    repaired: &mut bool,
) -> Result<GitObject, HubApiError> {
    if let Some(object) = optional_archived_object(state, repo, oid, auth)? {
        return Ok(object);
    }
    if repair && !*repaired {
        super::projection::project_history_with_roots(
            state,
            repo,
            auth,
            1_000_000,
            Some(&[revision.to_owned()]),
        )?;
        *repaired = true;
        return archived_object(state, repo, oid, auth);
    }
    Err(failure("Git tree archive is unavailable"))
}

pub(crate) fn path_oids_sync(
    state: &HubState,
    repo: &str,
    revision: &str,
    files: &[HubFileEntry],
    paths: &[String],
    auth: &AuthorizedRepository,
) -> Result<HashMap<String, GitPathMetadata>, HubApiError> {
    let mut remaining_content = MAX_GIT_PROJECTED_BYTES;
    path_oids_with_budget_sync(
        state,
        repo,
        revision,
        files,
        paths,
        auth,
        &mut remaining_content,
    )
}

/// Charge actual legacy payload reads across a history traversal. Verified
/// cached tree lookups do not consume a logical-file-size payload budget.
pub(crate) fn path_oids_with_budget_sync(
    state: &HubState,
    repo: &str,
    revision: &str,
    files: &[HubFileEntry],
    paths: &[String],
    auth: &AuthorizedRepository,
    remaining_content: &mut usize,
) -> Result<HashMap<String, GitPathMetadata>, HubApiError> {
    let archived = revision.len() == 40 && revision != "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
    let identity = if archived {
        None
    } else {
        super::identity::read_identity(state, repo, revision, auth)?
    };
    let use_archives = archived || identity.is_some();
    let mut repaired = false;
    let mut trees = HashMap::new();
    let root = if archived {
        let commit = archived_object(state, repo, revision, auth)?;
        if commit.object_type != ObjectType::Commit {
            return Err(failure("revision archive is not a Git commit"));
        }
        parse_commit_object(&commit.data).map_err(failure)?.0
    } else if let Some(identity) = identity {
        let root = parse_commit_object(&identity.data).map_err(failure)?.0;
        let tree = tree_with_repair(state, repo, revision, &root, auth, true, &mut repaired)?;
        trees.insert(root.clone(), tree);
        root
    } else {
        let mut blobs = BTreeMap::new();
        let mut cache = HashMap::new();
        for file in files {
            if !paths.iter().any(|path| {
                file.path == *path
                    || file
                        .path
                        .strip_prefix(path)
                        .is_some_and(|tail| tail.starts_with('/'))
            }) {
                continue;
            }
            if file.path.split('/').count() > 128 || file.path.len() > 1024 {
                return Err(failure("Git path exceeds metadata limits"));
            }
            let cache_key = (file.sha.clone(), file.size, file.is_lfs);
            let oid = if let Some(oid) = cache.get(&cache_key) {
                *oid
            } else {
                let blob = if file.is_lfs {
                    build_lfs_pointer_blob(&file.sha, file.size)
                } else {
                    let length = usize::try_from(file.size).map_err(failure)?;
                    if length > MAX_GIT_PROJECTED_BYTES {
                        return Err(failure("inline Git content exceeds metadata limit"));
                    }
                    *remaining_content = remaining_content
                        .checked_sub(length)
                        .ok_or_else(|| failure("Git path metadata exceeds content work limit"))?;
                    let key = projected_file_key(state, repo, file, auth)?;
                    let bytes = read_object_bounded(state, &key, length)?
                        .ok_or_else(|| failure("missing inline Git content"))?;
                    if bytes.len() as u64 != file.size
                        || blake3::hash(&bytes).to_hex().as_str() != file.sha
                    {
                        return Err(failure("inline Git content integrity mismatch"));
                    }
                    GitObject::blob(bytes)
                };
                let oid = blob.sha1();
                cache.insert(cache_key, oid);
                oid
            };
            blobs.insert(file.path.clone(), oid);
        }
        let (root, objects) = metadata_tree(&blobs)?;
        trees.extend(
            objects
                .into_iter()
                .map(|object| (hex::encode(object.sha1()), object)),
        );
        hex::encode(root)
    };
    let mut remaining = MAX_GIT_PROJECTED_BYTES;
    let mut result = HashMap::new();
    let mut children = HashMap::new();
    for path in paths {
        let mut oid = root.clone();
        let mut mode = "40000".to_owned();
        let mut found = true;
        let mut parts = path.split('/').peekable();
        while let Some(part) = parts.next() {
            if !trees.contains_key(&oid) {
                if !use_archives || trees.len() >= 100_000 {
                    return Err(failure(
                        "missing Git tree or metadata object limit exceeded",
                    ));
                }
                let tree =
                    tree_with_repair(state, repo, revision, &oid, auth, !archived, &mut repaired)?;
                remaining = remaining
                    .checked_sub(tree.data.len())
                    .ok_or_else(|| failure("Git path metadata exceeds byte limit"))?;
                trees.insert(oid.clone(), tree);
            }
            let tree = trees.get(&oid).ok_or_else(|| failure("missing Git tree"))?;
            if !children.contains_key(&oid) {
                children.insert(oid.clone(), tree_children(tree)?);
            }
            let Some((child, child_mode)) = children
                .get(&oid)
                .and_then(|entries| entries.get(part))
                .cloned()
            else {
                found = false;
                break;
            };
            if parts.peek().is_some() && !matches!(child_mode.as_str(), "40000" | "040000") {
                found = false;
                break;
            }
            oid = child;
            mode = child_mode;
        }
        if found {
            result.insert(
                path.clone(),
                GitPathMetadata {
                    oid,
                    mode,
                    xet_hash: None,
                },
            );
        }
    }
    Ok(result)
}

fn tree_children(tree: &GitObject) -> Result<HashMap<String, (String, String)>, HubApiError> {
    if tree.object_type != ObjectType::Tree {
        return Err(failure("Git path archive is not a tree"));
    }
    let mut data = tree.data.as_slice();
    let mut children = HashMap::new();
    while !data.is_empty() {
        let nul = data
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| failure("invalid Git tree entry"))?;
        let entry =
            std::str::from_utf8(data.get(..nul).ok_or_else(|| failure("invalid Git tree"))?)
                .map_err(failure)?;
        let (mode, entry_name) = entry
            .split_once(' ')
            .ok_or_else(|| failure("invalid Git tree mode"))?;
        let tail = data
            .get(nul.saturating_add(1)..)
            .ok_or_else(|| failure("invalid Git tree tail"))?;
        let child = tail
            .get(..20)
            .ok_or_else(|| failure("truncated Git tree OID"))?;
        if children
            .insert(entry_name.to_owned(), (hex::encode(child), mode.to_owned()))
            .is_some()
        {
            return Err(failure("duplicate Git tree entry"));
        }
        data = tail
            .get(20..)
            .ok_or_else(|| failure("truncated Git tree tail"))?;
    }
    Ok(children)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::super::{projection::archive_objects, tests::make_hub_state};
    use super::*;
    use crate::git::pack::create_commit_object;
    use shardline_protocol::ShardlineHash;
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};

    #[test]
    fn inline_path_uses_actual_verified_content_and_rejects_mismatch() {
        let (_temp, state) = make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        let content = b"real file content\n";
        let digest = *blake3::hash(content).as_bytes();
        let file = HubFileEntry {
            path: "nested/file.txt".to_owned(),
            size: content.len() as u64,
            sha: hex::encode(digest),
            is_lfs: false,
        };
        let key = crate::routes::lfs_object_key(&file.sha, &auth).unwrap();
        state
            .object_store
            .put_if_absent(
                &key,
                ObjectBody::from_slice(content),
                &ObjectIntegrity::new(ShardlineHash::from_bytes(digest), file.size),
            )
            .unwrap();
        let paths = vec![file.path.clone(), "nested".to_owned(), "missing".to_owned()];
        let result = path_oids_sync(
            &state,
            "team/model",
            &"a".repeat(64),
            std::slice::from_ref(&file),
            &paths,
            &auth,
        )
        .unwrap();
        let expected_blob = GitObject::blob(content.to_vec()).sha1();
        assert_eq!(
            result.get(&file.path).map(|entry| &entry.oid),
            Some(&hex::encode(expected_blob))
        );
        let mut tree = b"100644 file.txt\0".to_vec();
        tree.extend_from_slice(&expected_blob);
        assert_eq!(
            result.get("nested").map(|entry| &entry.oid),
            Some(&hex::encode(GitObject::tree(tree).sha1()))
        );
        assert!(!result.contains_key("missing"));
        let mut duplicate = file.clone();
        duplicate.path = "nested/copy.txt".to_owned();
        let mut remaining = content.len();
        path_oids_with_budget_sync(
            &state,
            "team/model",
            &"a".repeat(64),
            &[file.clone(), duplicate],
            &paths,
            &auth,
            &mut remaining,
        )
        .unwrap();
        assert_eq!(
            remaining, 0,
            "identical blobs are read only once per projection"
        );
        assert!(
            path_oids_with_budget_sync(
                &state,
                "team/model",
                &"a".repeat(64),
                std::slice::from_ref(&file),
                &paths,
                &auth,
                &mut remaining,
            )
            .is_err(),
            "legacy reads must retain their cumulative work limit"
        );
        let mut bad = file;
        bad.size += 1;
        assert!(
            path_oids_sync(&state, "team/model", &"a".repeat(64), &[bad], &paths, &auth).is_err()
        );
    }

    #[test]
    fn git_origin_uses_archived_tree_identity_and_rejects_wrong_scope() {
        let (_temp, state) = make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        let blob = GitObject::blob(b"#!/bin/sh\necho test\n".to_vec());
        let mut subtree = b"100755 run.sh\0".to_vec();
        subtree.extend_from_slice(&blob.sha1());
        let subtree = GitObject::tree(subtree);
        let mut root = b"40000 bin\0".to_vec();
        root.extend_from_slice(&subtree.sha1());
        let root = GitObject::tree(root);
        let commit =
            create_commit_object(&root.sha1(), None, "Test <test@example.com>", "executable");
        let revision = hex::encode(commit.sha1());
        let expected_blob = hex::encode(blob.sha1());
        let expected_tree = hex::encode(subtree.sha1());
        archive_objects(
            &state,
            "team/model",
            &revision,
            &[blob, subtree, root, commit],
            &auth,
        )
        .unwrap();
        let paths = vec!["bin".to_owned(), "bin/run.sh".to_owned()];
        let result = path_oids_sync(&state, "team/model", &revision, &[], &paths, &auth).unwrap();
        assert_eq!(
            result.get("bin").map(|entry| &entry.oid),
            Some(&expected_tree)
        );
        assert_eq!(
            result.get("bin/run.sh").map(|entry| &entry.oid),
            Some(&expected_blob)
        );
        assert!(path_oids_sync(&state, "other/model", &revision, &[], &paths, &auth).is_err());
    }

    #[tokio::test]
    async fn hub_tree_proofs_avoid_payload_reads_and_repair_only_from_verified_sources() {
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};
        let (_temp, state) = make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        let repo = "team/model";
        state
            .store
            .create_repo(shardline_index::hub::HubRepoType::Model, repo, false)
            .unwrap();
        let parent = state.store.resolve_revision(repo, "main").unwrap().unwrap();
        let content = b"verified metadata source";
        let digest = blake3::hash(content);
        let result = crate::routes::apply_commit(
            &state,
            repo,
            &parent,
            &crate::commit::ParsedCommit {
                message: "tree proof".to_owned(),
                parent_commit: None,
                instructions: vec![crate::commit::CommitInstruction::InlineFile {
                    path: "nested/file.txt".to_owned(),
                    content: content.to_vec(),
                }],
            },
            &auth,
        )
        .await
        .unwrap();
        let revision = result.0.commit_id;
        let paths = vec!["nested".to_owned(), "nested/file.txt".to_owned()];
        let expected = hex::encode(GitObject::blob(content.to_vec()).sha1());
        let payload_key = crate::routes::lfs_object_key(&digest.to_hex(), &auth).unwrap();
        let identity = super::super::identity::read_identity(&state, repo, &revision, &auth)
            .unwrap()
            .unwrap();
        let root = parse_commit_object(&identity.data).unwrap().0;
        let tree_key = read_member_archive_key(&state, repo, &root, &auth).unwrap();
        let blob_archive = read_member_archive_key(&state, repo, &expected, &auth).unwrap();
        assert!(
            state
                .object_store
                .metadata(&blob_archive)
                .unwrap()
                .is_none(),
            "metadata retention must not duplicate payloads"
        );
        state.object_store.delete_if_present(&payload_key).unwrap();
        let mut no_payload_budget = 0;
        path_oids_with_budget_sync(
            &state,
            repo,
            &revision,
            &[],
            &paths,
            &auth,
            &mut no_payload_budget,
        )
        .unwrap();
        let metadata = path_oids_sync(&state, repo, &revision, &[], &paths, &auth).unwrap();
        assert_eq!(metadata["nested/file.txt"].oid, expected);
        state.object_store.delete_if_present(&tree_key).unwrap();
        assert!(
            path_oids_sync(&state, repo, &revision, &[], &paths, &auth).is_err(),
            "missing cache must not be repaired from missing content"
        );
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*digest.as_bytes()),
            content.len() as u64,
        );
        state
            .object_store
            .put_if_absent(&payload_key, ObjectBody::from_slice(content), &integrity)
            .unwrap();
        let metadata = path_oids_sync(&state, repo, &revision, &[], &paths, &auth).unwrap();
        assert_eq!(metadata["nested/file.txt"].oid, expected);
        state.object_store.delete_if_present(&tree_key).unwrap();
        let corrupt = b"not a Git pack";
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(corrupt).as_bytes()),
            corrupt.len() as u64,
        );
        state
            .object_store
            .put_if_absent(&tree_key, ObjectBody::from_slice(corrupt), &integrity)
            .unwrap();
        assert!(
            path_oids_sync(&state, repo, &revision, &[], &paths, &auth).is_err(),
            "corrupt proof must fail instead of silently falling back"
        );
    }

    #[tokio::test]
    async fn expanded_history_recognizes_a_git_mode_change_with_identical_content() {
        let (_temp, state) = make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        let repo = "team/model";
        state
            .store
            .create_repo(shardline_index::hub::HubRepoType::Model, repo, false)
            .unwrap();
        let content = b"#!/bin/sh\necho test\n";
        let blob = GitObject::blob(content.to_vec());
        let file = HubFileEntry {
            path: "run.sh".to_owned(),
            size: content.len() as u64,
            sha: blake3::hash(content).to_hex().to_string(),
            is_lfs: false,
        };
        let mut parent = None;
        for (mode, message) in [("100644", "initial"), ("100755", "make executable")] {
            let mut data = format!("{mode} run.sh\0").into_bytes();
            data.extend_from_slice(&blob.sha1());
            let tree = GitObject::tree(data);
            let commit = create_commit_object(
                &tree.sha1(),
                parent.as_ref(),
                "Test <test@example.com>",
                message,
            );
            let revision = hex::encode(commit.sha1());
            let parent_id = parent.map(hex::encode);
            archive_objects(
                &state,
                repo,
                &revision,
                &[blob.clone(), tree, commit.clone()],
                &auth,
            )
            .unwrap();
            state
                .store
                .store_files(&revision, std::slice::from_ref(&file))
                .unwrap();
            state
                .store
                .create_revision(repo, parent_id.as_deref(), &revision, "main", message)
                .unwrap();
            parent = Some(commit.sha1());
        }
        let head = hex::encode(parent.unwrap());
        let metadata = crate::routes::path_history::last_path_commits(
            &state,
            repo,
            &head,
            std::slice::from_ref(&file),
            std::slice::from_ref(&file.path),
            &auth,
        )
        .await
        .unwrap();
        assert_eq!(metadata.get("run.sh").unwrap()["title"], "make executable");
        assert_eq!(metadata.get("run.sh").unwrap()["id"], head);
    }
}
