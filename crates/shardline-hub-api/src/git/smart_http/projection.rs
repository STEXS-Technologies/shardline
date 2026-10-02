//! A stable Git view of Hub history. Hub revision IDs remain opaque, while Git
//! references always identify the exact commit objects sent in upload-pack.
use std::collections::{BTreeMap, HashMap};

use crate::{
    error::HubApiError,
    routes::{HubState, lfs_object_key},
};
use shardline_index::hub::{HubFileEntry, HubRevision};
use shardline_protocol::{ByteRange, ShardlineHash};
use shardline_server_core::AuthorizedRepository;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};

use super::super::pack::{GitObject, ObjectType, generate_pack};
use super::{pack_parse::parse_pack_data, upload_pack::build_lfs_pointer_blob};

pub(super) struct GitProjection {
    pub(super) identities: HashMap<String, String>,
    pub(super) objects: Vec<GitObject>,
}

fn failure(error: impl std::fmt::Display) -> HubApiError {
    HubApiError::CasError(error.to_string())
}

fn archive_key(
    repo_id: &str,
    revision: &str,
    auth: &AuthorizedRepository,
) -> Result<ObjectKey, HubApiError> {
    ObjectKey::parse(&format!(
        "protocols/hub/git/{}/{}/{}",
        shardline_server_core::protocol_support::scope_namespace(auth.namespace()),
        blake3::hash(repo_id.as_bytes()).to_hex(),
        revision
    ))
    .map_err(failure)
}

fn read_object(state: &HubState, key: &ObjectKey) -> Result<Option<Vec<u8>>, HubApiError> {
    let Some(meta) = state.object_store.metadata(key).map_err(failure)? else {
        return Ok(None);
    };
    if meta.length() > super::limits::MAX_GIT_PROJECTED_BYTES.saturating_add(65536) as u64 {
        return Err(failure("Git object exceeds pack memory limit"));
    }
    if meta.length() == 0 {
        return Ok(Some(Vec::new()));
    }
    let range = ByteRange::new(0, meta.length().saturating_sub(1)).map_err(failure)?;
    state
        .object_store
        .read_range(key, range)
        .map(Some)
        .map_err(failure)
}

/// Retain exact Git objects before publishing a Git-origin revision. Rebuilding
/// these from Hub file metadata would lose author, modes, and commit identity.
pub(super) fn archive_objects(
    state: &HubState,
    repo_id: &str,
    revision: &str,
    objects: &[GitObject],
    auth: &AuthorizedRepository,
) -> Result<(), HubApiError> {
    if !objects
        .iter()
        .any(|obj| obj.object_type == ObjectType::Commit && hex::encode(obj.sha1()) == revision)
    {
        return Err(failure("Git revision not present in archive input"));
    }
    let total = objects
        .iter()
        .try_fold(0usize, |total, object| total.checked_add(object.data.len()))
        .ok_or_else(|| failure("Git archive size overflow"))?;
    if total > super::limits::MAX_GIT_PROJECTED_BYTES || objects.len() > 100_000 {
        return Err(failure("Git archive exceeds object limits"));
    }
    for object in objects {
        let sha = hex::encode(object.sha1());
        let key = archive_key(repo_id, &sha, auth)?;
        if let Some(existing) = read_object(state, &key)? {
            let parsed = parse_pack_data(&existing).map_err(failure)?;
            let same = parsed.len() == 1
                && parsed.first().is_some_and(|stored| {
                    stored.object_type == object.object_type && stored.data == object.data
                });
            if !same {
                return Err(failure("conflicting immutable Git object archive"));
            }
            continue;
        }
        let data = generate_pack(std::slice::from_ref(object)).map_err(failure)?;
        let integrity = ObjectIntegrity::new(
            ShardlineHash::from_bytes(*blake3::hash(&data).as_bytes()),
            data.len() as u64,
        );
        state
            .object_store
            .put_if_absent(&key, ObjectBody::from_slice(&data), &integrity)
            .map_err(failure)?;
    }
    Ok(())
}

pub(super) fn project_history(
    state: &HubState,
    repo_id: &str,
    auth: &AuthorizedRepository,
) -> Result<GitProjection, HubApiError> {
    let revisions = state
        .store
        .list_revisions_bounded(repo_id, 10_000)
        .map_err(failure)?;
    if revisions.len() > 10_000 {
        return Err(failure("Git history exceeds 10000 revisions"));
    }
    let mut reachable = std::collections::HashSet::new();
    let by_sha: HashMap<_, _> = revisions.iter().map(|r| (r.sha.as_str(), r)).collect();
    let refs = state
        .store
        .list_refs_bounded(repo_id, 10_000)
        .map_err(failure)?;
    let mut stack: Vec<_> = refs.iter().map(|r| r.sha.as_str()).collect();
    while let Some(sha) = stack.pop() {
        // Legacy short IDs have no trustworthy tree and form a recovery boundary.
        if sha.len() == 16 {
            continue;
        }
        if !reachable.insert(sha.to_owned()) {
            continue;
        }
        if let Some(revision) = by_sha.get(sha)
            && let Some(parent) = revision.parent_sha.as_deref()
        {
            stack.push(parent);
        }
    }
    let mut revisions: HashMap<_, _> = revisions
        .into_iter()
        .filter(|r| reachable.contains(&r.sha))
        .map(|r| (r.sha.clone(), r))
        .collect();
    let mut ordered = Vec::new();
    let mut visited = std::collections::HashSet::new();
    let mut visiting = std::collections::HashSet::new();
    for start in reachable {
        let mut ancestry = vec![(start, false)];
        while let Some((sha, finish)) = ancestry.pop() {
            if visited.contains(&sha) {
                continue;
            }
            let Some(revision) = revisions.get(&sha) else {
                continue;
            };
            if finish {
                visiting.remove(&sha);
                visited.insert(sha.clone());
                ordered.push(sha);
            } else {
                if !visiting.insert(sha.clone()) {
                    return Err(failure("cyclic Hub commit ancestry"));
                }
                ancestry.push((sha, true));
                if let Some(parent) = &revision.parent_sha
                    && revisions.contains_key(parent)
                {
                    ancestry.push((parent.clone(), false));
                }
            }
        }
    }
    let mut projection = GitProjection {
        identities: HashMap::new(),
        objects: Vec::new(),
    };
    let mut seen = std::collections::HashSet::new();
    let mut blob_cache = HashMap::new();
    let mut total_bytes = 0usize;
    for sha in ordered {
        let revision = revisions
            .remove(&sha)
            .ok_or_else(|| failure("missing ordered revision"))?;
        let parent = revision
            .parent_sha
            .as_ref()
            .filter(|p| projection.identities.contains_key(*p));
        let objects =
            if let Some(bytes) = read_object(state, &archive_key(repo_id, &revision.sha, auth)?)? {
                let objects = load_archived_graph(
                    state,
                    repo_id,
                    &revision.sha,
                    bytes,
                    auth,
                    &seen,
                    super::limits::MAX_GIT_PROJECTED_BYTES.saturating_sub(total_bytes),
                )?;
                projection
                    .identities
                    .insert(revision.sha.clone(), revision.sha.clone());
                objects
            } else {
                if revision.sha.len() == 40
                    && revision.sha != "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3"
                {
                    return Err(failure(
                        "original Git objects unavailable; restore the Git object archive",
                    ));
                }
                let parent_sha = parent.and_then(|p| projection.identities.get(p));
                let files = state
                    .store
                    .get_files_bounded(&revision.sha, 100_000)
                    .map_err(failure)?;
                let objects = project_revision(
                    state,
                    &revision,
                    &files,
                    parent_sha,
                    auth,
                    &mut blob_cache,
                    super::limits::MAX_GIT_PROJECTED_BYTES.saturating_sub(total_bytes),
                )?;
                let commit = objects
                    .last()
                    .ok_or_else(|| failure("missing projected commit"))?;
                projection
                    .identities
                    .insert(revision.sha.clone(), hex::encode(commit.sha1()));
                objects
            };
        for object in objects {
            if seen.insert(object.sha1()) {
                total_bytes = total_bytes
                    .checked_add(object.data.len())
                    .ok_or_else(|| failure("Git pack size overflow"))?;
                if total_bytes > super::limits::MAX_GIT_PROJECTED_BYTES
                    || projection.objects.len() >= 100_000
                {
                    return Err(failure("Git projection exceeds pack object limits"));
                }
                projection.objects.push(object);
            }
        }
    }
    Ok(projection)
}

fn load_archived_graph(
    state: &HubState,
    repo_id: &str,
    commit_sha: &str,
    commit_bytes: Vec<u8>,
    auth: &AuthorizedRepository,
    existing: &std::collections::HashSet<[u8; 20]>,
    remaining_bytes: usize,
) -> Result<Vec<GitObject>, HubApiError> {
    let mut pending = vec![(commit_sha.to_owned(), Some(commit_bytes))];
    let mut visited = std::collections::HashSet::new();
    let mut objects = Vec::new();
    let mut total_bytes = 0usize;
    while let Some((sha, bytes)) = pending.pop() {
        let raw_sha: [u8; 20] = hex::decode(&sha)
            .map_err(failure)?
            .try_into()
            .map_err(|_invalid_length| failure("invalid archived Git object ID"))?;
        if existing.contains(&raw_sha) || !visited.insert(raw_sha) {
            continue;
        }
        if visited.len() > 100_000 {
            return Err(failure("Git archive exceeds object count limit"));
        }
        let bytes = match bytes {
            Some(bytes) => bytes,
            None => read_object(state, &archive_key(repo_id, &sha, auth)?)?
                .ok_or_else(|| failure("missing archived Git object"))?,
        };
        let mut parsed = parse_pack_data(&bytes).map_err(failure)?;
        if parsed.len() != 1 {
            return Err(failure("Git archive must contain one object"));
        }
        let object = parsed.pop().ok_or_else(|| failure("empty Git archive"))?;
        if object.sha1() != raw_sha {
            return Err(failure("archived Git object identity mismatch"));
        }
        total_bytes = total_bytes
            .checked_add(object.data.len())
            .ok_or_else(|| failure("Git archive size overflow"))?;
        if total_bytes > remaining_bytes {
            return Err(failure("Git archive exceeds byte limit"));
        }
        for child in object_children(&object)? {
            pending.push((child, None));
        }
        objects.push(object);
    }
    Ok(objects)
}

fn object_children(object: &GitObject) -> Result<Vec<String>, HubApiError> {
    let mut children = Vec::new();
    match object.object_type {
        ObjectType::Commit => {
            let text = std::str::from_utf8(&object.data).map_err(failure)?;
            let headers = text.split_once("\n\n").map_or(text, |(headers, _)| headers);
            for line in headers.lines() {
                if let Some(sha) = line
                    .strip_prefix("tree ")
                    .or_else(|| line.strip_prefix("parent "))
                {
                    children.push(sha.to_owned());
                }
            }
        }
        ObjectType::Tree => {
            let mut data = object.data.as_slice();
            while !data.is_empty() {
                let separator = data
                    .iter()
                    .position(|byte| *byte == 0)
                    .ok_or_else(|| failure("invalid archived Git tree"))?;
                let entry = data
                    .get(..separator)
                    .ok_or_else(|| failure("invalid archived Git tree entry"))?;
                let tail = data
                    .get(separator.saturating_add(1)..)
                    .ok_or_else(|| failure("invalid archived Git tree tail"))?;
                let sha = tail
                    .get(..20)
                    .ok_or_else(|| failure("truncated archived Git tree"))?;
                // Submodule commits belong to another repository; Git packs don't include them.
                if !entry.starts_with(b"160000 ") {
                    children.push(hex::encode(sha));
                }
                data = tail
                    .get(20..)
                    .ok_or_else(|| failure("truncated archived Git tree"))?;
            }
        }
        ObjectType::Blob | ObjectType::Tag => {}
    }
    Ok(children)
}

#[allow(
    clippy::too_many_arguments,
    reason = "explicit revision context and shared content budget"
)]
fn project_revision(
    state: &HubState,
    revision: &HubRevision,
    files: &[HubFileEntry],
    parent: Option<&String>,
    auth: &AuthorizedRepository,
    cache: &mut HashMap<(String, u64, bool), [u8; 20]>,
    remaining_bytes: usize,
) -> Result<Vec<GitObject>, HubApiError> {
    if files.len() > 100_000 {
        return Err(failure("Git tree exceeds file count limit"));
    }
    let mut available = remaining_bytes;
    let mut blobs = BTreeMap::new();
    let mut objects = Vec::new();
    for file in files {
        if file.path.split('/').count() > 128 {
            return Err(failure("Git tree nesting exceeds maximum depth"));
        }
        let cache_key = (file.sha.clone(), file.size, file.is_lfs);
        if let Some(sha) = cache.get(&cache_key) {
            blobs.insert(file.path.clone(), *sha);
            continue;
        }
        let expected_size = if file.is_lfs {
            build_lfs_pointer_blob(&file.sha, file.size).data.len()
        } else {
            usize::try_from(file.size).map_err(failure)?
        };
        available = available
            .checked_sub(expected_size)
            .ok_or_else(|| failure("Git inline content exceeds projection byte limit"))?;
        let blob = if file.is_lfs {
            build_lfs_pointer_blob(&file.sha, file.size)
        } else {
            let key = lfs_object_key(&file.sha, auth)?;
            let bytes = read_object(state, &key)?
                .ok_or_else(|| failure(format!("missing inline content: {}", file.path)))?;
            if bytes.len() as u64 != file.size {
                return Err(failure("inline content length mismatch"));
            }
            if blake3::hash(&bytes).to_hex().as_str() != file.sha {
                return Err(failure("inline content hash mismatch"));
            }
            GitObject::blob(bytes)
        };
        let sha = blob.sha1();
        cache.insert(cache_key, sha);
        blobs.insert(file.path.clone(), sha);
        objects.push(blob);
    }
    let root = build_tree(&blobs, &mut objects)?;
    let mut data = format!("tree {}\n", hex::encode(root));
    if let Some(parent) = parent {
        use std::fmt::Write;
        writeln!(&mut data, "parent {parent}").map_err(failure)?;
    }
    use std::fmt::Write;
    writeln!(&mut data, "author Shardline Hub <hub@shardline.dev> {} +0000\ncommitter Shardline Hub <hub@shardline.dev> {} +0000\n\n{}\n\nShardline-Revision: {}\nShardline-Repository: {}", revision.created_at_unix_seconds, revision.created_at_unix_seconds, revision.message.as_deref().unwrap_or(""), revision.sha, revision.repo_id).map_err(failure)?;
    objects.push(GitObject::commit(data.into_bytes()));
    Ok(objects)
}

fn build_tree(
    files: &BTreeMap<String, [u8; 20]>,
    objects: &mut Vec<GitObject>,
) -> Result<[u8; 20], HubApiError> {
    let mut entries = Vec::new();
    let mut directories: BTreeMap<String, BTreeMap<String, [u8; 20]>> = BTreeMap::new();
    for (path, sha) in files {
        if let Some((dir, relative)) = path.split_once('/') {
            directories
                .entry(dir.to_owned())
                .or_default()
                .insert(relative.to_owned(), *sha);
        } else {
            entries.push((path.clone(), false, *sha));
        }
    }
    for (dir, children) in directories {
        if files.contains_key(&dir) {
            return Err(failure("Git tree has file/directory path conflict"));
        }
        entries.push((dir, true, build_tree(&children, objects)?));
    }
    // Git compares tree names as though directories end in '/'.
    entries.sort_by_key(|(name, dir, _)| {
        if *dir {
            format!("{name}/")
        } else {
            name.clone()
        }
    });
    let mut bytes = Vec::new();
    for (name, dir, sha) in entries {
        bytes.extend_from_slice(if dir { b"40000 " } else { b"100644 " });
        bytes.extend_from_slice(name.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&sha);
    }
    let tree = GitObject::tree(bytes);
    let sha = tree.sha1();
    objects.push(tree);
    Ok(sha)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::super::super::pack::create_commit_object;
    use super::*;
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
    use shardline_server_core::{AuthProvider, LocalHmacProvider};

    fn capability(provider_kind: RepositoryProvider) -> AuthorizedRepository {
        let provider = LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap();
        let repo = RepositoryScope::new(provider_kind, "alice", "repo", None).unwrap();
        let claims =
            TokenClaims::new("local", "subject", TokenScope::Write, repo, u64::MAX).unwrap();
        let token = provider.mint_token(&claims).unwrap();
        AuthorizedRepository::verify_and_authorize(&provider, &token, TokenScope::Write).unwrap()
    }

    fn raw_revision(state: &HubState, auth: &AuthorizedRepository) -> String {
        state
            .store
            .create_repo(
                shardline_index::hub::HubRepoType::Model,
                "alice/repo",
                false,
            )
            .unwrap();
        let tree = GitObject::tree(Vec::new());
        let commit = create_commit_object(
            &tree.sha1(),
            None,
            "Git Test <test@example.com>",
            "original",
        );
        let sha = hex::encode(commit.sha1());
        archive_objects(state, "alice/repo", &sha, &[tree, commit], auth).unwrap();
        state
            .store
            .create_revision("alice/repo", None, &sha, "main", "original")
            .unwrap();
        sha
    }

    #[test]
    fn raw_archive_is_repository_and_provider_scoped() {
        let (_temp, state) = super::super::tests::make_hub_state();
        let github = capability(RepositoryProvider::GitHub);
        let other = capability(RepositoryProvider::GitLab);
        let sha = raw_revision(&state, &github);
        assert_ne!(
            archive_key("alice/repo", &sha, &github).unwrap(),
            archive_key("alice/repo", &sha, &other).unwrap()
        );
        assert_ne!(
            archive_key("alice/repo", &sha, &github).unwrap(),
            archive_key("bob/repo", &sha, &github).unwrap()
        );
        let projected = project_history(&state, "alice/repo", &github).unwrap();
        assert_eq!(projected.identities.get(&sha), Some(&sha));
        assert!(
            project_history(&state, "alice/repo", &other).is_err(),
            "wrong namespace must not synthesize a false replacement commit"
        );
    }

    #[test]
    fn raw_archive_rejects_replaced_object_bytes() {
        let (_temp, state) = super::super::tests::make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        let sha = raw_revision(&state, &auth);
        let key = archive_key("alice/repo", &sha, &auth).unwrap();
        let original = parse_pack_data(&read_object(&state, &key).unwrap().unwrap()).unwrap();
        state.object_store.delete_if_present(&key).unwrap();
        let bytes = generate_pack(&[GitObject::blob(b"wrong object".to_vec())]).unwrap();
        let integrity = ObjectIntegrity::new(
            ShardlineHash::from_bytes(*blake3::hash(&bytes).as_bytes()),
            bytes.len() as u64,
        );
        state
            .object_store
            .put_if_absent(&key, ObjectBody::from_slice(&bytes), &integrity)
            .unwrap();
        assert!(project_history(&state, "alice/repo", &auth).is_err());
        assert!(
            archive_objects(&state, "alice/repo", &sha, &original, &auth).is_err(),
            "conflicting stored bytes must fail before revision publication"
        );
    }

    #[test]
    fn projected_commit_identity_ignores_reference_spelling() {
        let (_temp, state) = super::super::tests::make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        let mut revision = HubRevision {
            repo_id: "alice/repo".to_owned(),
            ref_name: "main".to_owned(),
            sha: "a".repeat(64),
            parent_sha: None,
            message: Some("same tree".to_owned()),
            created_at_unix_seconds: 42,
        };
        let main = project_revision(
            &state,
            &revision,
            &[],
            None,
            &auth,
            &mut HashMap::new(),
            super::super::limits::MAX_GIT_PROJECTED_BYTES,
        )
        .unwrap();
        revision.ref_name = "refs/tags/v1".to_owned();
        let tag = project_revision(
            &state,
            &revision,
            &[],
            None,
            &auth,
            &mut HashMap::new(),
            super::super::limits::MAX_GIT_PROJECTED_BYTES,
        )
        .unwrap();
        assert_eq!(main.last().unwrap().sha1(), tag.last().unwrap().sha1());
    }
}
