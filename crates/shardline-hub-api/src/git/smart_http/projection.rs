//! A stable Git view of Hub history. Hub revision IDs remain opaque, while Git
//! references always identify the exact commit objects sent in upload-pack.
use std::collections::{BTreeMap, HashMap};

use crate::{error::HubApiError, routes::HubState};
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
    // Async exports retain the existing identity admission slot through pack
    // generation. A cancelled waiter cannot admit replacement blocking work.
    pub(super) admission: Option<tokio::sync::SemaphorePermit<'static>>,
}

fn failure(error: impl std::fmt::Display) -> HubApiError {
    HubApiError::CasError(error.to_string())
}

pub(super) fn archive_key(
    repo_id: &str,
    revision: &str,
    auth: &AuthorizedRepository,
) -> Result<ObjectKey, HubApiError> {
    archive_key_for_scope(repo_id, revision, auth.namespace())
}

fn archive_key_for_scope(
    repo_id: &str,
    revision: &str,
    scope: Option<&shardline_protocol::RepositoryScope>,
) -> Result<ObjectKey, HubApiError> {
    ObjectKey::parse(&format!(
        "protocols/hub/git/{}/{}/{}",
        shardline_server_core::protocol_support::scope_namespace(scope),
        blake3::hash(repo_id.as_bytes()).to_hex(),
        revision
    ))
    .map_err(failure)
}

/// Constructed only while projecting a selected repository commit or walking
/// hash-verified references from it. This is not an arbitrary object lookup.
pub(crate) struct GitArchiveMember<'member> {
    repo: &'member str,
    oid: &'member str,
}

/// A file selected from an authorized immutable revision, or from the trusted
/// file entries of a selected Git projection revision. Fields stay private.
pub(crate) struct PublishedFileMember<'member> {
    repo: &'member str,
    oid: &'member str,
}

impl PublishedFileMember<'_> {
    pub(crate) const fn repo(&self) -> &str {
        self.repo
    }
    pub(crate) const fn oid(&self) -> &str {
        self.oid
    }
}

/// One bounded publication snapshot shared by every object in an LFS batch.
pub(crate) struct ScopedPublishedFiles {
    repo: String,
    oids: std::collections::HashSet<String>,
}

impl ScopedPublishedFiles {
    pub(crate) fn key(
        &self,
        state: &HubState,
        oid: &str,
        auth: &AuthorizedRepository,
    ) -> Result<Option<ObjectKey>, HubApiError> {
        if !self.oids.contains(oid) {
            return Ok(None);
        }
        crate::object_locations::file_member_key(
            state,
            auth,
            &PublishedFileMember {
                repo: &self.repo,
                oid,
            },
        )
        .map(Some)
    }
}

pub(crate) async fn scoped_published_files(
    state: &HubState,
    auth: &AuthorizedRepository,
) -> Result<Option<ScopedPublishedFiles>, HubApiError> {
    let Some(scope) = auth.repository() else {
        return Ok(None);
    };
    if scope.revision().is_none() {
        return Ok(None);
    }
    let repo = format!("{}/{}", scope.owner(), scope.name());
    let Some(commits) = crate::revision_access::visible_commits(state, &repo, auth).await? else {
        return Ok(None);
    };
    let mut oids = std::collections::HashSet::new();
    let mut remaining = MAX_PROJECTED_FILE_ENTRIES;
    for commit in commits {
        let files = state
            .store
            .get_files_bounded(&commit, remaining.min(100_000))
            .map_err(failure)?;
        remaining = remaining
            .checked_sub(files.len())
            .ok_or_else(|| failure("LFS publication snapshot exceeds file entry limit"))?;
        oids.extend(files.into_iter().map(|file| file.sha));
    }
    Ok(Some(ScopedPublishedFiles { repo, oids }))
}

pub(crate) async fn file_key_at_revision(
    state: &HubState,
    repo: &str,
    requested: &str,
    commit: &str,
    path: &str,
    auth: &AuthorizedRepository,
) -> Result<ObjectKey, HubApiError> {
    crate::revision_access::authorize_read(state, repo, requested, commit, auth).await?;
    state
        .store
        .get_revision(repo, commit)
        .map_err(failure)?
        .ok_or(HubApiError::RevisionNotFound)?;
    let files = state
        .store
        .get_files_bounded(commit, 100_000)
        .map_err(failure)?;
    let file = files
        .iter()
        .find(|file| file.path == path)
        .ok_or(HubApiError::NotFound)?;
    crate::object_locations::file_member_key(
        state,
        auth,
        &PublishedFileMember {
            repo,
            oid: &file.sha,
        },
    )
}

/// The caller has selected this file entry from a trusted revision in its
/// authorized projection/history. Never use this for a hash-only wire request.
pub(super) fn projected_file_key(
    state: &HubState,
    repo: &str,
    file: &HubFileEntry,
    auth: &AuthorizedRepository,
) -> Result<ObjectKey, HubApiError> {
    crate::object_locations::file_member_key(
        state,
        auth,
        &PublishedFileMember {
            repo,
            oid: &file.sha,
        },
    )
}

impl GitArchiveMember<'_> {
    pub(crate) const fn repo(&self) -> &str {
        self.repo
    }
    pub(crate) const fn oid(&self) -> &str {
        self.oid
    }
}

pub(super) fn read_member_archive_key(
    state: &HubState,
    repo: &str,
    oid: &str,
    auth: &AuthorizedRepository,
) -> Result<ObjectKey, HubApiError> {
    let member = GitArchiveMember { repo, oid };
    crate::object_locations::locate_git_member(state, auth, &member, |scope| {
        archive_key_for_scope(repo, oid, scope)
    })
}

pub(super) fn read_identity_archive_key(
    state: &HubState,
    repo_id: &str,
    lookup: &str,
    auth: &AuthorizedRepository,
) -> Result<ObjectKey, HubApiError> {
    if let Some(scope) = auth.repository()
        && repo_id != format!("{}/{}", scope.owner(), scope.name())
    {
        return Err(HubApiError::Forbidden);
    }
    let revision = format!("identities-v1/{lookup}");
    crate::object_locations::locate_identity(state, auth, &revision, |scope| {
        archive_key_for_scope(repo_id, &revision, scope)
    })
}

fn read_object(state: &HubState, key: &ObjectKey) -> Result<Option<Vec<u8>>, HubApiError> {
    read_object_bounded(
        state,
        key,
        super::limits::MAX_GIT_PROJECTED_BYTES.saturating_add(65536),
    )
}

pub(super) fn read_object_bounded(
    state: &HubState,
    key: &ObjectKey,
    maximum_length: usize,
) -> Result<Option<Vec<u8>>, HubApiError> {
    let Some(meta) = state.object_store.metadata(key).map_err(|error| {
        crate::routes::object_io::storage_failure("Git archive metadata read", &error)
    })?
    else {
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
        .map_err(|error| {
            crate::routes::object_io::storage_failure("Git archive bytes read", &error)
        })
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
    archive_objects_with_limit(
        state,
        repo_id,
        revision,
        objects,
        auth,
        super::limits::MAX_GIT_PROJECTED_BYTES,
        false,
    )
}

/// Retain only authenticated tree/commit metadata, never another payload copy.
/// A 100,000-entry tree with 1024-byte paths remains below this metadata bound.
pub(super) fn archive_metadata_objects(
    state: &HubState,
    repo_id: &str,
    revision: &str,
    objects: &[GitObject],
    auth: &AuthorizedRepository,
) -> Result<(), HubApiError> {
    if objects
        .iter()
        .any(|object| object.object_type == ObjectType::Blob)
    {
        return Err(failure("payload object in metadata archive"));
    }
    archive_objects_with_limit(
        state,
        repo_id,
        revision,
        objects,
        auth,
        128 * 1024 * 1024,
        true,
    )
}

fn archive_objects_with_limit(
    state: &HubState,
    repo_id: &str,
    revision: &str,
    objects: &[GitObject],
    auth: &AuthorizedRepository,
    byte_limit: usize,
    trees_only: bool,
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
    if total > byte_limit || objects.len() > 100_000 {
        return Err(failure("Git archive exceeds object limits"));
    }
    for object in objects {
        if trees_only && object.object_type != ObjectType::Tree {
            continue;
        }
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
            crate::object_locations::remember(
                state,
                auth,
                shardline_index::hub::HubObjectKind::GitArchive,
                &sha,
            )?;
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
            .map_err(|error| {
                crate::routes::object_io::storage_failure("Git object archive write", &error)
            })?;
        crate::object_locations::remember(
            state,
            auth,
            shardline_index::hub::HubObjectKind::GitArchive,
            &sha,
        )?;
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
    project_history_with_admission(state, repo_id, auth, &super::identity::IDENTITY_ADMISSION).await
}

async fn project_history_with_admission(
    state: HubState,
    repo_id: String,
    auth: AuthorizedRepository,
    admission: &'static tokio::sync::Semaphore,
) -> Result<GitProjection, HubApiError> {
    let permit = admission.acquire().await.map_err(failure)?;
    project_history_admitted(state, repo_id, auth, permit).await
}

pub(super) async fn project_history_admitted(
    state: HubState,
    repo_id: String,
    auth: AuthorizedRepository,
    permit: tokio::sync::SemaphorePermit<'static>,
) -> Result<GitProjection, HubApiError> {
    tokio::task::spawn_blocking(move || {
        let mut projection = project_history(&state, &repo_id, &auth)?;
        super::identity::retain_projection_identities(&state, &repo_id, &projection, &auth)?;
        projection.admission = Some(permit);
        Ok(projection)
    })
    .await
    .map_err(failure)?
}

/// One shared quota across revisions. Charge unique objects when constructed,
/// rather than after a complete revision has already been materialized.
struct ProjectionBudget {
    remaining_bytes: usize,
    remaining_objects: usize,
    seen: std::collections::HashSet<[u8; 20]>,
    identity_only: bool,
}

impl ProjectionBudget {
    fn append(
        &mut self,
        objects: &mut Vec<GitObject>,
        object: GitObject,
    ) -> Result<[u8; 20], HubApiError> {
        let sha = object.sha1();
        self.append_with_known_sha(objects, object, sha)
    }

    // Private optimization for a digest computed from this exact immutable
    // object body. Callers must never pass an externally supplied object ID.
    fn append_with_known_sha(
        &mut self,
        objects: &mut Vec<GitObject>,
        object: GitObject,
        sha: [u8; 20],
    ) -> Result<[u8; 20], HubApiError> {
        if self.seen.contains(&sha) {
            return Ok(sha);
        }
        let bytes = self
            .remaining_bytes
            .checked_sub(object.data.len())
            .ok_or_else(|| {
                HubApiError::BadRequest("Git projection exceeds byte limit".to_owned())
            })?;
        let count = self.remaining_objects.checked_sub(1).ok_or_else(|| {
            HubApiError::BadRequest("Git projection exceeds object count limit".to_owned())
        })?;
        if !self.identity_only {
            self.remaining_bytes = bytes;
        }
        self.remaining_objects = count;
        self.seen.insert(sha);
        if !self.identity_only || object.object_type != ObjectType::Blob {
            objects.push(object);
        }
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
    project_history_with_roots(state, repo_id, auth, maximum_file_entries, None)
}

/// Explicit roots support immutable REST revisions, including commits retained
/// after their last branch is deleted. Git discovery continues to use live refs.
pub(super) fn project_history_with_roots(
    state: &HubState,
    repo_id: &str,
    auth: &AuthorizedRepository,
    maximum_file_entries: usize,
    requested_roots: Option<&[String]>,
) -> Result<GitProjection, HubApiError> {
    let revisions = if let Some(roots) = requested_roots {
        // Identity lookup must not impose the Git export history limit on
        // repositories with many unrelated or already-committed revisions.
        let mut pending = roots.to_vec();
        let mut seen = std::collections::HashSet::new();
        let mut selected = Vec::new();
        while let Some(sha) = pending.pop() {
            if !seen.insert(sha.clone()) {
                continue;
            }
            if seen.len() > 10_000 {
                return Err(failure(
                    "uncached Git identity ancestry exceeds 10000 revisions",
                ));
            }
            if !roots.contains(&sha)
                && super::identity::cached_public_commit_id(state, repo_id, &sha, auth)?.is_some()
            {
                continue;
            }
            let revision = state
                .store
                .get_revision(repo_id, &sha)
                .map_err(failure)?
                .ok_or_else(|| failure("missing repository revision metadata"))?;
            let git_origin = sha.len() == 40 && sha != "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
            if let Some(parent) = &revision.parent_sha
                && (!git_origin || roots.contains(&sha))
            {
                pending.push(parent.clone());
            }
            selected.push(revision);
        }
        selected
    } else {
        state
            .store
            .list_revisions_bounded(repo_id, 10_000)
            .map_err(failure)?
    };
    if revisions.len() > 10_000 {
        return Err(failure("Git history exceeds 10000 revisions"));
    }
    let mut reachable = std::collections::HashSet::new();
    let by_sha: HashMap<_, _> = revisions.iter().map(|r| (r.sha.as_str(), r)).collect();
    let visible_refs = crate::revision_access::visible_refs(state, repo_id, auth)?;
    let mut roots = match requested_roots {
        Some(roots) => {
            if roots.len() > 10_000 || roots.iter().any(|root| !by_sha.contains_key(root.as_str()))
            {
                return Err(failure(
                    "unknown repository commit or too many projection roots",
                ));
            }
            roots.to_vec()
        }
        None => state
            .store
            .list_refs_bounded(repo_id, 10_000)
            .map_err(failure)?
            .into_iter()
            .filter(|reference| {
                visible_refs.as_ref().is_none_or(|visible| {
                    visible.contains(shardline_index::hub::canonical_ref_name(
                        &reference.ref_name,
                    ))
                })
            })
            .map(|reference| reference.sha)
            .collect(),
    };
    if requested_roots.is_none()
        && roots.is_empty()
        && let Some(snapshot) = crate::revision_access::snapshot_commit(state, repo_id, auth)?
    {
        roots.push(snapshot);
    }
    let mut stack: Vec<_> = roots.iter().map(String::as_str).collect();
    let mut known_identities = HashMap::new();
    while let Some(sha) = stack.pop() {
        // Legacy short IDs have no trustworthy tree and form a recovery boundary.
        if sha.len() == 16 {
            continue;
        }
        if requested_roots.is_some()
            && !roots.iter().any(|root| root == sha)
            && sha.len() == 40
            && sha != "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3"
        {
            known_identities.insert(sha.to_owned(), sha.to_owned());
            continue;
        }
        // REST identity construction needs ancestor identities, not their
        // complete exported payloads. Reuse a durable parent commitment so
        // ordinary writes do not repeatedly read all previous inline content.
        // Git export must still materialize the complete reachable graph.
        if requested_roots.is_some()
            && !roots.iter().any(|root| root == sha)
            && let Some(public) =
                super::identity::cached_public_commit_id(state, repo_id, sha, auth)?
        {
            known_identities.insert(sha.to_owned(), public);
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
        admission: None,
        identities: known_identities,
        objects: Vec::new(),
    };
    let mut budget = ProjectionBudget {
        remaining_bytes: super::limits::MAX_GIT_PROJECTED_BYTES,
        remaining_objects: 100_000,
        seen: std::collections::HashSet::new(),
        identity_only: requested_roots.is_some(),
    };
    let mut blob_cache = HashMap::new();
    let mut tree_cache = HashMap::new();
    let mut remaining_files = maximum_file_entries;
    for sha in ordered {
        if budget.identity_only {
            budget.remaining_objects = 100_000;
            budget.seen.clear();
        }
        let revision = revisions
            .remove(&sha)
            .ok_or_else(|| failure("missing ordered revision"))?;
        let parent = revision
            .parent_sha
            .as_ref()
            .filter(|p| projection.identities.contains_key(*p));
        let objects = if let Some(bytes) = read_object(
            state,
            &read_member_archive_key(state, repo_id, &revision.sha, auth)?,
        )? {
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
        if requested_roots.is_some() {
            if revision.sha.len() != 40
                || revision.sha == "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3"
            {
                let public = projection
                    .identities
                    .get(&revision.sha)
                    .ok_or_else(|| failure("missing projected metadata identity"))?;
                archive_metadata_objects(state, repo_id, public, &objects, auth)?;
            }
            for object in &objects {
                if object.object_type == ObjectType::Commit
                    && (revision.sha.len() != 40
                        || revision.sha == "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3")
                {
                    super::identity::retain_identity(state, repo_id, &revision.sha, object, auth)?;
                }
            }
            if roots.contains(&sha) {
                projection.objects.extend(objects);
            }
        } else {
            projection.objects.extend(objects);
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
            None => read_object(state, &read_member_archive_key(state, repo_id, &sha, auth)?)?
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

pub(super) fn object_children(object: &GitObject) -> Result<Vec<String>, HubApiError> {
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
        validate_projected_path(&file.path)?;
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
            let key = projected_file_key(state, &revision.repo_id, file, auth)?;
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
        return Err(HubApiError::BadRequest(
            "Git commit exceeds object count limit".to_owned(),
        ));
    }
    let mut data = BoundedCommit {
        text: String::new(),
        limit: budget.remaining_bytes,
    };
    format_commit(&mut data, root, parent, revision).map_err(failure)?;
    // Deduplication may emit no objects for a commit already loaded from an
    // archive. Its identity remains valid independently of the output vector.
    let commit_sha = budget.append_with_known_sha(
        &mut objects,
        GitObject::commit(data.text.into_bytes()),
        candidate,
    )?;
    Ok((objects, commit_sha))
}

/// Project exactly one unpublished revision with the same unique-object and
/// per-object limits as identity recovery. No metadata ref is read or advanced.
pub(super) fn project_prepared_revision(
    state: &HubState,
    revision: &HubRevision,
    files: &[HubFileEntry],
    parent: &String,
    auth: &AuthorizedRepository,
) -> Result<(Vec<GitObject>, [u8; 20]), HubApiError> {
    let mut budget = ProjectionBudget {
        remaining_bytes: super::limits::MAX_GIT_PROJECTED_BYTES,
        remaining_objects: 100_000,
        seen: std::collections::HashSet::new(),
        identity_only: true,
    };
    project_revision(
        state,
        revision,
        files,
        Some(parent),
        auth,
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut budget,
    )
}

/// The same bounds apply before REST publication and while projecting legacy
/// revisions. Rejecting a new path must never first advance an authoritative ref.
pub(crate) fn validate_projected_path(path: &str) -> Result<(), HubApiError> {
    if path.len() > 1024 {
        return Err(HubApiError::PathValidation(
            "Git tree path exceeds maximum length".to_owned(),
        ));
    }
    if path.split('/').count() > 128 {
        return Err(HubApiError::PathValidation(
            "Git tree nesting exceeds maximum depth".to_owned(),
        ));
    }
    Ok(())
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

/// Build the same canonical trees for REST path metadata as for Git export.
pub(super) fn metadata_tree(
    files: &BTreeMap<String, [u8; 20]>,
) -> Result<([u8; 20], Vec<GitObject>), HubApiError> {
    let mut objects = Vec::new();
    let mut budget = ProjectionBudget {
        remaining_bytes: super::limits::MAX_GIT_PROJECTED_BYTES,
        remaining_objects: 100_000,
        seen: std::collections::HashSet::new(),
        identity_only: false,
    };
    let root = build_tree(files, &mut objects, &mut budget)?;
    Ok((root, objects))
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
        return Err(HubApiError::BadRequest(
            "Git tree exceeds byte limit".to_owned(),
        ));
    }
    if budget.remaining_objects == 0 {
        return Err(HubApiError::BadRequest(
            "Git tree exceeds object count limit".to_owned(),
        ));
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
            identity_only: false,
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
    fn explicit_projection_retains_identity_after_branch_deletion() {
        let (_temp, state) = super::super::tests::make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        state
            .store
            .create_repo(
                shardline_index::hub::HubRepoType::Model,
                "alice/repo",
                false,
            )
            .unwrap();
        let parent = state
            .store
            .resolve_revision("alice/repo", "main")
            .unwrap()
            .unwrap();
        let sha = "b".repeat(64);
        state.store.store_files(&sha, &[]).unwrap();
        state
            .store
            .create_revision("alice/repo", Some(&parent), &sha, "feature", "feature")
            .unwrap();
        let before = project_history(&state, "alice/repo", &auth).unwrap();
        let expected = before.identities.get(&sha).unwrap().clone();
        state
            .store
            .delete_ref("alice/repo", "feature", &sha)
            .unwrap();
        assert!(
            !project_history(&state, "alice/repo", &auth)
                .unwrap()
                .identities
                .contains_key(&sha)
        );
        let retained = project_history_with_roots(
            &state,
            "alice/repo",
            &auth,
            MAX_PROJECTED_FILE_ENTRIES,
            Some(std::slice::from_ref(&sha)),
        )
        .unwrap();
        assert_eq!(retained.identities.get(&sha), Some(&expected));
        assert!(
            retained
                .objects
                .iter()
                .any(|object| object.object_type == ObjectType::Commit
                    && hex::encode(object.sha1()) == expected)
        );
    }

    #[test]
    fn explicit_projection_rejects_commit_from_another_repository() {
        let (_temp, state) = super::super::tests::make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        let foreign = raw_revision(&state, &auth);
        state
            .store
            .create_repo(shardline_index::hub::HubRepoType::Model, "bob/repo", false)
            .unwrap();
        assert!(
            project_history_with_roots(
                &state,
                "bob/repo",
                &auth,
                MAX_PROJECTED_FILE_ENTRIES,
                Some(&[foreign]),
            )
            .is_err()
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
                    // Independently reproduce the established serialized
                    // commit contract and hash its emitted bytes using the
                    // ordinary GitObject implementation, not the candidate.
                    let mut expected_body =
                        format!("tree {}\n", hex::encode(GitObject::tree(Vec::new()).sha1()));
                    if let Some(parent) = parent {
                        expected_body.push_str(&format!("parent {parent}\n"));
                    }
                    expected_body.push_str(&format!(
                        "author Shardline Hub <hub@shardline.dev> {timestamp} +0000\ncommitter Shardline Hub <hub@shardline.dev> {timestamp} +0000\n\n{message}\n\nShardline-Revision: {}\nShardline-Repository: {}\n",
                        revision.sha, revision.repo_id,
                    ));
                    let expected_object = GitObject::commit(expected_body.into_bytes());
                    assert_eq!(expected_sha, expected_object.sha1());
                    assert_eq!(original.last().unwrap().data, expected_object.data);
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
                    assert_eq!(first.last().unwrap().sha1(), sha);
                    assert_eq!(first.last().unwrap().data, expected_object.data);
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
                    assert_eq!(new_objects.last().unwrap().sha1(), new_sha);
                    assert_eq!(new_objects.last().unwrap().data, expected_object.data);
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
    async fn cancelled_projection_keeps_admission_until_blocking_work_finishes() {
        static ADMISSION: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
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
            let _ = release_rx.recv_timeout(std::time::Duration::from_secs(3));
            connection.execute_batch("COMMIT;").unwrap();
        });
        ready_rx.recv().unwrap();
        let worker_state = state.clone();
        let operation = tokio::spawn(async move {
            project_history_with_admission(
                worker_state,
                "alice/repo".to_owned(),
                AuthorizedRepository::anonymous_full_access(),
                &ADMISSION,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while ADMISSION.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        operation.abort();
        assert!(matches!(operation.await, Err(error) if error.is_cancelled()));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), ADMISSION.acquire(),)
                .await
                .is_err(),
            "cancelled HTTP waiter must not admit overlapping blocking work"
        );
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        let projection = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            project_history_with_admission(
                state,
                "alice/repo".to_owned(),
                AuthorizedRepository::anonymous_full_access(),
                &ADMISSION,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            ADMISSION.available_permits(),
            0,
            "retained objects still own their slot"
        );
        drop(projection);
        assert_eq!(ADMISSION.available_permits(), 1);
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
        // Exercise each async adapter under its own blocking read. Running both
        // together also races their DELETE-to-WAL promotion, which is unrelated
        // to whether storage work stalls the current-thread executor.
        for project in [true, false] {
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
            let state_for_operation = state.clone();
            let operation = tokio::spawn(async move {
                if project {
                    project_history_async(
                        state_for_operation,
                        "alice/repo".to_owned(),
                        AuthorizedRepository::anonymous_full_access(),
                    )
                    .await
                    .map(|_| ())
                } else {
                    super::super::ref_advertisement::collect_refs(
                        &state_for_operation,
                        "alice/repo",
                    )
                    .await
                    .map(|_| ())
                }
            });
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let elapsed = started.elapsed();
            let still_blocked = !operation.is_finished();
            release_tx.send(()).unwrap();
            holder.join().unwrap();
            operation.await.unwrap().unwrap();
            assert!(
                still_blocked,
                "storage operation must wait for lock release"
            );
            assert!(
                elapsed < std::time::Duration::from_secs(1),
                "blocked storage must leave the async timer runnable: {elapsed:?}"
            );
        }
    }
}
