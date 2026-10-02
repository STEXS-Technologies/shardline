//! A stable Git view of Hub history. Hub revision IDs remain opaque, while Git
//! references always identify the exact commit objects sent in upload-pack.
use std::collections::{BTreeMap, HashMap};

use crate::{
    error::HubApiError,
    routes::{HubState, lfs_object_key},
};
use sha1::{Digest, Sha1};
use shardline_index::hub::{HubFileEntry, HubRevision};
use shardline_protocol::{ByteRange, ShardlineHash};
use shardline_server_core::AuthorizedRepository;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};

use super::super::pack::{GitObject, ObjectType, generate_pack};
#[cfg(test)]
use super::pack_parse::parse_pack_data;
use super::{pack_parse::parse_pack_data_with_budget, upload_pack::build_lfs_pointer_blob};

/// Bound cumulative metadata work even when most Git objects deduplicate.
const MAX_PROJECTED_FILE_ENTRIES: usize = 1_000_000;

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
    read_object_bounded(
        state,
        key,
        super::limits::MAX_GIT_PROJECTED_BYTES.saturating_add(65536),
    )
}

fn read_object_bounded(
    state: &HubState,
    key: &ObjectKey,
    maximum_length: usize,
) -> Result<Option<Vec<u8>>, HubApiError> {
    let Some(meta) = state.object_store.metadata(key).map_err(failure)? else {
        return Ok(None);
    };
    if meta.length() > maximum_length as u64 {
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
            let parsed = parse_pack_data_with_budget(&existing, &HashMap::new(), object.data.len())
                .map_err(failure)?;
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

/// Synchronous storage/SQL adapters and Git hashing must not occupy Tokio's
/// executor threads. The immutable read task may finish after cancellation.
pub(super) async fn project_history_async(
    state: HubState,
    repo_id: String,
    auth: AuthorizedRepository,
) -> Result<GitProjection, HubApiError> {
    tokio::task::spawn_blocking(move || project_history(&state, &repo_id, &auth))
        .await
        .map_err(failure)?
}

/// One shared quota across revisions. Charge unique objects when constructed,
/// rather than after a complete revision has already been materialized.
struct ProjectionBudget {
    remaining_bytes: usize,
    remaining_objects: usize,
    seen: std::collections::HashSet<[u8; 20]>,
}

impl ProjectionBudget {
    fn append(
        &mut self,
        objects: &mut Vec<GitObject>,
        object: GitObject,
    ) -> Result<[u8; 20], HubApiError> {
        let sha = object.sha1();
        if self.seen.contains(&sha) {
            return Ok(sha);
        }
        let bytes = self
            .remaining_bytes
            .checked_sub(object.data.len())
            .ok_or_else(|| failure("Git projection exceeds byte limit"))?;
        let count = self
            .remaining_objects
            .checked_sub(1)
            .ok_or_else(|| failure("Git projection exceeds object count limit"))?;
        self.remaining_bytes = bytes;
        self.remaining_objects = count;
        self.seen.insert(sha);
        objects.push(object);
        Ok(sha)
    }
}

pub(super) fn project_history(
    state: &HubState,
    repo_id: &str,
    auth: &AuthorizedRepository,
) -> Result<GitProjection, HubApiError> {
    project_history_with_file_budget(state, repo_id, auth, MAX_PROJECTED_FILE_ENTRIES)
}

fn project_history_with_file_budget(
    state: &HubState,
    repo_id: &str,
    auth: &AuthorizedRepository,
    maximum_file_entries: usize,
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
    let mut budget = ProjectionBudget {
        remaining_bytes: super::limits::MAX_GIT_PROJECTED_BYTES,
        remaining_objects: 100_000,
        seen: std::collections::HashSet::new(),
    };
    let mut blob_cache = HashMap::new();
    let mut tree_cache = HashMap::new();
    let mut remaining_files = maximum_file_entries;
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
                let objects =
                    load_archived_graph(state, repo_id, &revision.sha, bytes, auth, &mut budget)?;
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
                    .get_files_bounded(&revision.sha, remaining_files.min(100_000))
                    .map_err(failure)?;
                remaining_files = remaining_files
                    .checked_sub(files.len())
                    .ok_or_else(|| failure("Git projection exceeds cumulative file entry limit"))?;
                let (objects, commit_sha) = project_revision(
                    state,
                    &revision,
                    &files,
                    parent_sha,
                    auth,
                    &mut blob_cache,
                    &mut tree_cache,
                    &mut budget,
                )?;
                projection
                    .identities
                    .insert(revision.sha.clone(), hex::encode(commit_sha));
                objects
            };
        projection.objects.extend(objects);
    }
    Ok(projection)
}

fn load_archived_graph(
    state: &HubState,
    repo_id: &str,
    commit_sha: &str,
    commit_bytes: Vec<u8>,
    auth: &AuthorizedRepository,
    budget: &mut ProjectionBudget,
) -> Result<Vec<GitObject>, HubApiError> {
    let mut pending = vec![(commit_sha.to_owned(), Some(commit_bytes))];
    let mut visited = std::collections::HashSet::new();
    let mut discovered = std::collections::HashSet::from([commit_sha.to_owned()]);
    let maximum_objects = budget.remaining_objects;
    let mut objects = Vec::new();
    while let Some((sha, bytes)) = pending.pop() {
        let raw_sha: [u8; 20] = hex::decode(&sha)
            .map_err(failure)?
            .try_into()
            .map_err(|_invalid_length| failure("invalid archived Git object ID"))?;
        if budget.seen.contains(&raw_sha) || !visited.insert(raw_sha) {
            continue;
        }
        if visited.len() > 100_000 {
            return Err(failure("Git archive exceeds object count limit"));
        }
        if budget.remaining_objects == 0 {
            return Err(failure("Git archive exceeds object count limit"));
        }
        let bytes = match bytes {
            Some(bytes) => bytes,
            None => read_object(state, &archive_key(repo_id, &sha, auth)?)?
                .ok_or_else(|| failure("missing archived Git object"))?,
        };
        let mut parsed =
            parse_pack_data_with_budget(&bytes, &HashMap::new(), budget.remaining_bytes)
                .map_err(failure)?;
        if parsed.len() != 1 {
            return Err(failure("Git archive must contain one object"));
        }
        let object = parsed.pop().ok_or_else(|| failure("empty Git archive"))?;
        if object.sha1() != raw_sha {
            return Err(failure("archived Git object identity mismatch"));
        }
        for child in object_children(&object)? {
            let child_sha: [u8; 20] = hex::decode(&child)
                .map_err(failure)?
                .try_into()
                .map_err(|_invalid_length| failure("invalid archived Git child ID"))?;
            if budget.seen.contains(&child_sha)
                || visited.contains(&child_sha)
                || !discovered.insert(child.clone())
            {
                continue;
            }
            if discovered.len() > maximum_objects {
                return Err(failure("Git archive exceeds pending object count limit"));
            }
            pending.push((child, None));
        }
        budget.append(&mut objects, object)?;
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
                    if children.len() >= 100_000 {
                        return Err(failure("Git archive exceeds child count limit"));
                    }
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
                    if children.len() >= 100_000 {
                        return Err(failure("Git archive exceeds child count limit"));
                    }
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
    tree_cache: &mut HashMap<[u8; 32], [u8; 20]>,
    budget: &mut ProjectionBudget,
) -> Result<(Vec<GitObject>, [u8; 20]), HubApiError> {
    if files.len() > 100_000 {
        return Err(failure("Git tree exceeds file count limit"));
    }
    let mut blobs = BTreeMap::new();
    let mut objects = Vec::new();
    for file in files {
        if file.path.len() > 1024 {
            return Err(failure("Git tree path exceeds maximum length"));
        }
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
        if expected_size > budget.remaining_bytes {
            return Err(failure("Git inline content exceeds projection byte limit"));
        }
        let blob = if file.is_lfs {
            build_lfs_pointer_blob(&file.sha, file.size)
        } else {
            let key = lfs_object_key(&file.sha, auth)?;
            let bytes = read_object_bounded(state, &key, expected_size)?
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
        budget.append(&mut objects, blob)?;
    }
    let mut fingerprint = blake3::Hasher::new();
    fingerprint.update(b"shardline-git-projected-tree-v1\0");
    for (path, sha) in &blobs {
        fingerprint.update(&(path.len() as u64).to_le_bytes());
        fingerprint.update(path.as_bytes());
        fingerprint.update(sha);
    }
    let fingerprint = *fingerprint.finalize().as_bytes();
    let root = if let Some(sha) = tree_cache.get(&fingerprint) {
        *sha
    } else {
        let root = build_tree(&blobs, &mut objects, budget)?;
        tree_cache.insert(fingerprint, root);
        root
    };
    let mut counter = CountSink {
        length: 0,
        cap: super::limits::MAX_GIT_PROJECTED_BYTES,
    };
    format_commit(&mut counter, root, parent, revision).map_err(failure)?;
    let mut digest = Sha1::new();
    digest.update(format!("commit {}\0", counter.length).as_bytes());
    let mut hash = HashSink(digest);
    format_commit(&mut hash, root, parent, revision).map_err(failure)?;
    let candidate: [u8; 20] = hash.0.finalize().into();
    if budget.seen.contains(&candidate) {
        return Ok((objects, candidate));
    }
    if counter.length > budget.remaining_bytes {
        return Err(failure("Git commit exceeds byte limit"));
    }
    if budget.remaining_objects == 0 {
        return Err(failure("Git commit exceeds object count limit"));
    }
    let mut data = BoundedCommit {
        text: String::new(),
        limit: budget.remaining_bytes,
    };
    format_commit(&mut data, root, parent, revision).map_err(failure)?;
    // Deduplication may emit no objects for a commit already loaded from an
    // archive. Its identity remains valid independently of the output vector.
    let commit_sha = budget.append(&mut objects, GitObject::commit(data.text.into_bytes()))?;
    Ok((objects, commit_sha))
}

// Count and hash the same borrowed fields before allocating a commit body.
// The per-object cap remains the total export ceiling; only unseen objects
// consume the remaining unique-object quota.
struct CountSink {
    length: usize,
    cap: usize,
}
impl std::fmt::Write for CountSink {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        let next = self.length.checked_add(text.len()).ok_or(std::fmt::Error)?;
        if next > self.cap {
            return Err(std::fmt::Error);
        }
        self.length = next;
        Ok(())
    }
}
struct HashSink(Sha1);
impl std::fmt::Write for HashSink {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.0.update(text.as_bytes());
        Ok(())
    }
}
fn format_commit(
    out: &mut impl std::fmt::Write,
    root: [u8; 20],
    parent: Option<&String>,
    revision: &HubRevision,
) -> std::fmt::Result {
    writeln!(out, "tree {}", hex::encode(root))?;
    if let Some(parent) = parent {
        writeln!(out, "parent {parent}")?;
    }
    writeln!(
        out,
        "author Shardline Hub <hub@shardline.dev> {} +0000\ncommitter Shardline Hub <hub@shardline.dev> {} +0000\n\n{}\n\nShardline-Revision: {}\nShardline-Repository: {}",
        revision.created_at_unix_seconds,
        revision.created_at_unix_seconds,
        revision.message.as_deref().unwrap_or(""),
        revision.sha,
        revision.repo_id
    )?;
    Ok(())
}
/// Formatting commit metadata also obeys the quota before growing a buffer.
struct BoundedCommit {
    text: String,
    limit: usize,
}

impl std::fmt::Write for BoundedCommit {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        if text.len() > self.limit.saturating_sub(self.text.len()) {
            return Err(std::fmt::Error);
        }
        self.text.push_str(text);
        Ok(())
    }
}

fn build_tree(
    files: &BTreeMap<String, [u8; 20]>,
    objects: &mut Vec<GitObject>,
    budget: &mut ProjectionBudget,
) -> Result<[u8; 20], HubApiError> {
    // Keep one borrowed, sorted file list. Cloning remaining path maps at each
    // directory level retains O(file_count * depth * path_length) memory.
    let files: Vec<_> = files
        .iter()
        .map(|(path, sha)| (path.as_str(), *sha))
        .collect();
    build_tree_slice(&files, 0, objects, budget)
}

fn build_tree_slice(
    files: &[(&str, [u8; 20])],
    prefix_length: usize,
    objects: &mut Vec<GitObject>,
    budget: &mut ProjectionBudget,
) -> Result<[u8; 20], HubApiError> {
    let mut entries = Vec::new();
    let mut names = std::collections::HashSet::new();
    let mut offset = 0usize;
    while let Some((path, sha)) = files.get(offset) {
        let relative = path
            .get(prefix_length..)
            .ok_or_else(|| failure("invalid Git tree path"))?;
        if let Some((directory, _)) = relative.split_once('/') {
            if !names.insert(directory) {
                return Err(failure("Git tree has file/directory path conflict"));
            }
            let mut end = offset.saturating_add(1);
            while let Some((candidate, _)) = files.get(end) {
                let candidate = candidate
                    .get(prefix_length..)
                    .ok_or_else(|| failure("invalid Git tree path"))?;
                if candidate.split_once('/').map(|(dir, _)| dir) != Some(directory) {
                    break;
                }
                end = end.saturating_add(1);
            }
            let children = files
                .get(offset..end)
                .ok_or_else(|| failure("invalid Git tree range"))?;
            let child = build_tree_slice(
                children,
                prefix_length
                    .saturating_add(directory.len())
                    .saturating_add(1),
                objects,
                budget,
            )?;
            entries.push((directory, true, child));
            offset = end;
        } else {
            if !names.insert(relative) {
                return Err(failure("Git tree has duplicate path"));
            }
            entries.push((relative, false, *sha));
            offset = offset.saturating_add(1);
        }
    }
    // Git compares tree names as though directories end in '/'. Compare
    // iterators directly to avoid allocating strings in every sort comparison.
    entries.sort_by(|(left, left_dir, _), (right, right_dir, _)| {
        left.bytes()
            .chain(left_dir.then_some(b'/'))
            .cmp(right.bytes().chain(right_dir.then_some(b'/')))
    });
    let length = entries
        .iter()
        .try_fold(0usize, |length, (name, dir, _)| {
            length
                .checked_add(name.len())
                .and_then(|value| value.checked_add(if *dir { 27 } else { 28 }))
        })
        .ok_or_else(|| failure("Git tree size overflow"))?;
    // Hash the borrowed tree entries before allocating. Shared subtrees can
    // be reused even when their size exceeds the remaining *new* byte quota.
    let mut digest = Sha1::new();
    digest.update(format!("tree {length}\0").as_bytes());
    for (name, dir, sha) in &entries {
        digest.update(if *dir {
            b"40000 ".as_slice()
        } else {
            b"100644 ".as_slice()
        });
        digest.update(name.as_bytes());
        digest.update([0]);
        digest.update(sha);
    }
    let tree_sha: [u8; 20] = digest.finalize().into();
    if budget.seen.contains(&tree_sha) {
        return Ok(tree_sha);
    }
    if length > budget.remaining_bytes {
        return Err(failure("Git tree exceeds byte limit"));
    }
    if budget.remaining_objects == 0 {
        return Err(failure("Git tree exceeds object count limit"));
    }
    let mut bytes = Vec::with_capacity(length);
    for (name, dir, sha) in entries {
        bytes.extend_from_slice(if dir { b"40000 " } else { b"100644 " });
        bytes.extend_from_slice(name.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&sha);
    }
    budget.append(objects, GitObject::tree(bytes))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::super::super::pack::create_commit_object;
    use super::*;
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
    use shardline_server_core::{AuthProvider, LocalHmacProvider};

    fn test_budget(bytes: usize, objects: usize) -> ProjectionBudget {
        ProjectionBudget {
            remaining_bytes: bytes,
            remaining_objects: objects,
            seen: std::collections::HashSet::new(),
        }
    }

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
            &mut HashMap::new(),
            &mut test_budget(super::super::limits::MAX_GIT_PROJECTED_BYTES, 100_000),
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
            &mut HashMap::new(),
            &mut test_budget(super::super::limits::MAX_GIT_PROJECTED_BYTES, 100_000),
        )
        .unwrap();
        assert_eq!(main.1, tag.1);
    }
    #[test]
    fn duplicate_projected_commit_keeps_identity_without_charging_objects_twice() {
        let (_temp, state) = super::super::tests::make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        let revision = revision(&"a".repeat(64));
        let mut blobs = HashMap::new();
        let mut trees = HashMap::new();
        let mut budget = test_budget(4096, 100);
        let (first, first_sha) = project_revision(
            &state,
            &revision,
            &[],
            None,
            &auth,
            &mut blobs,
            &mut trees,
            &mut budget,
        )
        .unwrap();
        assert!(!first.is_empty());
        let before = (budget.remaining_bytes, budget.remaining_objects);
        let (duplicate, duplicate_sha) = project_revision(
            &state,
            &revision,
            &[],
            None,
            &auth,
            &mut blobs,
            &mut trees,
            &mut budget,
        )
        .unwrap();
        assert!(duplicate.is_empty());
        assert_eq!(duplicate_sha, first_sha);
        assert_eq!((budget.remaining_bytes, budget.remaining_objects), before);
    }

    #[test]
    fn repeated_commit_at_exact_payload_quota_preserves_sha_and_unique_budget() {
        let (_temp, state) = super::super::tests::make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        let parent = "c".repeat(40);
        for message in ["same commit", "μ-model 😀\n東京", "", "line1\nline2"] {
            for timestamp in [0, 42, 1_700_000_000, i64::MAX as u64] {
                for parent in [None, Some(&parent)] {
                    let mut revision = revision(&"a".repeat(64));
                    revision.message = Some(message.to_owned());
                    revision.created_at_unix_seconds = timestamp;
                    let (original, expected_sha) = project_revision(
                        &state,
                        &revision,
                        &[],
                        parent,
                        &auth,
                        &mut HashMap::new(),
                        &mut HashMap::new(),
                        &mut test_budget(4096, 100),
                    )
                    .unwrap();
                    let payload_bytes: usize =
                        original.iter().map(|object| object.data.len()).sum();
                    let mut budget = test_budget(payload_bytes, 2);
                    let mut blobs = HashMap::new();
                    let mut trees = HashMap::new();
                    let (first, sha) = project_revision(
                        &state,
                        &revision,
                        &[],
                        parent,
                        &auth,
                        &mut blobs,
                        &mut trees,
                        &mut budget,
                    )
                    .unwrap();
                    assert_eq!(sha, expected_sha);
                    assert_eq!(first.len(), 2);
                    assert_eq!((budget.remaining_bytes, budget.remaining_objects), (0, 0));
                    let seen = budget.seen.clone();
                    let (duplicate, duplicate_sha) = project_revision(
                        &state,
                        &revision,
                        &[],
                        parent,
                        &auth,
                        &mut blobs,
                        &mut trees,
                        &mut budget,
                    )
                    .unwrap();
                    assert!(duplicate.is_empty());
                    assert_eq!(duplicate_sha, expected_sha);
                    assert_eq!((budget.remaining_bytes, budget.remaining_objects), (0, 0));
                    assert_eq!(budget.seen, seen);
                    // The same bytes are still subject to both quotas when the
                    // commit has not already been included in this export.
                    budget.seen.remove(&expected_sha);
                    budget.remaining_bytes = payload_bytes - 1;
                    budget.remaining_objects = 1;
                    assert!(
                        project_revision(
                            &state,
                            &revision,
                            &[],
                            parent,
                            &auth,
                            &mut blobs,
                            &mut trees,
                            &mut budget
                        )
                        .is_err()
                    );
                    budget.remaining_bytes = payload_bytes;
                    budget.remaining_objects = 0;
                    assert!(
                        project_revision(
                            &state,
                            &revision,
                            &[],
                            parent,
                            &auth,
                            &mut blobs,
                            &mut trees,
                            &mut budget
                        )
                        .is_err()
                    );
                    budget.remaining_objects = 1;
                    let (new_objects, new_sha) = project_revision(
                        &state,
                        &revision,
                        &[],
                        parent,
                        &auth,
                        &mut blobs,
                        &mut trees,
                        &mut budget,
                    )
                    .unwrap();
                    assert_eq!(new_objects.len(), 1);
                    assert_eq!(new_sha, expected_sha);
                    assert_eq!((budget.remaining_bytes, budget.remaining_objects), (0, 0));
                }
            }
        }
    }

    #[test]
    fn counted_commit_enforces_existing_global_cap_in_utf8_bytes() {
        use std::fmt::Write;
        let limit = super::super::limits::MAX_GIT_PROJECTED_BYTES;
        let mut counter = CountSink {
            length: limit - 2,
            cap: limit,
        };
        assert!(counter.write_str("μ").is_ok());
        assert_eq!(counter.length, limit);
        assert!(counter.write_str("x").is_err());
        assert_eq!(counter.length, limit);
    }

    fn revision(sha: &str) -> HubRevision {
        HubRevision {
            repo_id: "alice/repo".to_owned(),
            ref_name: "main".to_owned(),
            sha: sha.to_owned(),
            parent_sha: None,
            message: Some("message".to_owned()),
            created_at_unix_seconds: 42,
        }
    }

    #[test]
    fn tree_materialization_stops_at_object_quota() {
        let files: BTreeMap<_, _> = (0..100)
            .map(|index| (format!("directory-{index:03}/deep/file"), [index as u8; 20]))
            .collect();
        let mut objects = Vec::new();
        let mut budget = test_budget(usize::MAX, 5);
        assert!(build_tree(&files, &mut objects, &mut budget).is_err());
        assert_eq!(
            objects.len(),
            5,
            "fail while constructing, before materializing all directories"
        );
        assert_eq!(budget.remaining_objects, 0);
    }

    #[test]
    fn empty_revision_still_obeys_payload_budget() {
        let (_temp, state) = super::super::tests::make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        assert!(
            project_revision(
                &state,
                &revision(&"a".repeat(64)),
                &[],
                None,
                &auth,
                &mut HashMap::new(),
                &mut HashMap::new(),
                &mut test_budget(1, 100)
            )
            .is_err()
        );
    }

    #[test]
    fn unchanged_deep_tree_reuses_objects_without_charging_twice() {
        let (_temp, state) = super::super::tests::make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        let files = vec![HubFileEntry {
            path: "nested/deeper/file".to_owned(),
            sha: "a".repeat(64),
            size: 123,
            is_lfs: true,
        }];
        let mut blobs = HashMap::new();
        let mut trees = HashMap::new();
        let mut budget = test_budget(4096, 6);
        let first = project_revision(
            &state,
            &revision(&"b".repeat(64)),
            &files,
            None,
            &auth,
            &mut blobs,
            &mut trees,
            &mut budget,
        )
        .unwrap();
        assert_eq!(first.0.len(), 5);
        let parent = hex::encode(first.1);
        let second = project_revision(
            &state,
            &revision(&"c".repeat(64)),
            &files,
            Some(&parent),
            &auth,
            &mut blobs,
            &mut trees,
            &mut budget,
        )
        .unwrap();
        assert_eq!(second.0.len(), 1, "unchanged blobs and tree must be reused");
        assert_eq!(budget.remaining_objects, 0);
        assert_eq!(trees.len(), 1);
    }

    #[test]
    fn cumulative_file_quota_is_enforced_before_decoding_next_tree() {
        let (_temp, state) = super::super::tests::make_hub_state();
        state
            .store
            .create_repo(
                shardline_index::hub::HubRepoType::Model,
                "alice/repo",
                false,
            )
            .unwrap();
        let files: Vec<_> = (0..2)
            .map(|index| HubFileEntry {
                path: format!("file{index}"),
                sha: "a".repeat(64),
                size: 0,
                is_lfs: true,
            })
            .collect();
        let mut parent = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3".to_owned();
        for sha in ["b".repeat(64), "c".repeat(64)] {
            state.store.store_files(&sha, &files).unwrap();
            state
                .store
                .create_revision("alice/repo", Some(&parent), &sha, "main", "unchanged")
                .unwrap();
            parent = sha;
        }
        let auth = AuthorizedRepository::anonymous_full_access();
        assert!(project_history_with_file_budget(&state, "alice/repo", &auth, 3).is_err());
        assert!(project_history_with_file_budget(&state, "alice/repo", &auth, 4).is_ok());
    }

    #[test]
    fn inline_read_rejects_inconsistent_length_before_body_access() {
        let (_temp, state) = super::super::tests::make_hub_state();
        let key = ObjectKey::parse("hub/oversized-inline").unwrap();
        let bytes = vec![0x5a; 4096];
        let integrity = ObjectIntegrity::new(
            ShardlineHash::from_bytes(*blake3::hash(&bytes).as_bytes()),
            bytes.len() as u64,
        );
        state
            .object_store
            .put_if_absent(&key, ObjectBody::from_slice(&bytes), &integrity)
            .unwrap();
        assert!(read_object_bounded(&state, &key, 1).is_err());
        assert_eq!(
            read_object_bounded(&state, &key, bytes.len())
                .unwrap()
                .unwrap(),
            bytes
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_metadata_read_does_not_stall_executor() {
        let (temp, state) = super::super::tests::make_hub_state();
        state
            .store
            .create_repo(
                shardline_index::hub::HubRepoType::Model,
                "alice/repo",
                false,
            )
            .unwrap();
        let database = temp.path().join("metadata.sqlite3");
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            let connection = rusqlite::Connection::open(database).unwrap();
            connection
                .execute_batch("PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE;")
                .unwrap();
            ready_tx.send(()).unwrap();
            // Watchdog lets a regressed synchronous handler return so the test
            // reports a failure rather than hanging the entire test process.
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(3));
            connection.execute_batch("COMMIT;").unwrap();
        });
        ready_rx.recv().unwrap();
        let started = std::time::Instant::now();
        let state_for_refs = state.clone();
        let refs = tokio::spawn(async move {
            super::super::ref_advertisement::collect_refs(&state_for_refs, "alice/repo").await
        });
        let projection = tokio::spawn(project_history_async(
            state,
            "alice/repo".to_owned(),
            AuthorizedRepository::anonymous_full_access(),
        ));
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let elapsed = started.elapsed();
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        projection.await.unwrap().unwrap();
        refs.await.unwrap().unwrap();
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "blocked storage must leave the async timer runnable: {elapsed:?}"
        );
    }
}
