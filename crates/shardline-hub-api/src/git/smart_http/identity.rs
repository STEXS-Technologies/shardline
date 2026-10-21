//! Public Git identities over unchanged, repository-scoped Hub revisions.
//!
//! The cache retains the actual commit object, not an unauthenticated alias.
//! Its Git digest and embedded repository/revision bind both lookup directions.
//! It uses the existing Git archive namespace and requires no schema migration.

use std::collections::HashMap;

use shardline_protocol::ShardlineHash;
use shardline_server_core::AuthorizedRepository;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};

use super::{
    super::pack::{GitObject, ObjectType, generate_pack},
    pack_parse::parse_pack_data_with_budget,
    projection::{GitProjection, archive_key, project_history_with_roots, read_object_bounded},
};
use crate::{error::HubApiError, routes::HubState};

const MAX_IDENTITY_BYTES: usize = 20 * 1024 * 1024;
static IDENTITY_ADMISSION: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

fn failure(error: impl std::fmt::Display) -> HubApiError {
    HubApiError::CasError(error.to_string())
}

fn embedded_revision(object: &GitObject, repo_id: &str) -> Result<String, HubApiError> {
    if object.object_type != ObjectType::Commit {
        return Err(failure("identity archive does not contain a commit"));
    }
    let text = std::str::from_utf8(&object.data).map_err(failure)?;
    // Generated commits end with these trailers. User-supplied messages may
    // contain similar lines, so only the terminal pair is authoritative.
    let mut lines = text.lines().rev();
    if lines.next() != Some(format!("Shardline-Repository: {repo_id}").as_str()) {
        return Err(failure("identity archive repository mismatch"));
    }
    let revision = lines
        .next()
        .and_then(|line| line.strip_prefix("Shardline-Revision: "))
        .ok_or_else(|| failure("identity archive missing revision"))?;
    if revision.is_empty() || revision.len() > 1024 || revision.chars().any(char::is_control) {
        return Err(failure("invalid archived revision identity"));
    }
    Ok(revision.to_owned())
}

fn read_identity(
    state: &HubState,
    repo_id: &str,
    lookup: &str,
    auth: &AuthorizedRepository,
) -> Result<Option<GitObject>, HubApiError> {
    let key = archive_key(repo_id, &format!("identities-v1/{lookup}"), auth)?;
    let Some(bytes) = read_object_bounded(state, &key, MAX_IDENTITY_BYTES)? else {
        return Ok(None);
    };
    let mut objects = parse_pack_data_with_budget(&bytes, &HashMap::new(), MAX_IDENTITY_BYTES)
        .map_err(failure)?;
    if objects.len() != 1 {
        return Err(failure("identity archive must contain exactly one commit"));
    }
    let object = objects
        .pop()
        .ok_or_else(|| failure("empty identity archive"))?;
    let revision = embedded_revision(&object, repo_id)?;
    if lookup != revision && lookup != hex::encode(object.sha1()) {
        return Err(failure("identity archive digest mismatch"));
    }
    Ok(Some(object))
}

pub(super) fn cached_public_commit_id(
    state: &HubState,
    repo_id: &str,
    revision: &str,
    auth: &AuthorizedRepository,
) -> Result<Option<String>, HubApiError> {
    read_identity(state, repo_id, revision, auth)
        .map(|object| object.map(|object| hex::encode(object.sha1())))
}

pub(crate) async fn public_commit_id(
    state: &HubState,
    repo_id: &str,
    revision: &str,
    auth: &AuthorizedRepository,
) -> Result<String, HubApiError> {
    let permit = IDENTITY_ADMISSION.acquire().await.map_err(failure)?;
    let state = state.clone();
    let repo_id = repo_id.to_owned();
    let revision = revision.to_owned();
    let auth = auth.clone();
    tokio::task::spawn_blocking(move || {
        // Cancellation must not release admission while blocking work remains.
        let _permit = permit;
        public_commit_id_sync(&state, &repo_id, &revision, &auth)
    })
    .await
    .map_err(failure)?
}

fn public_commit_id_sync(
    state: &HubState,
    repo_id: &str,
    revision: &str,
    auth: &AuthorizedRepository,
) -> Result<String, HubApiError> {
    // Git-origin commits already have their exact public identity. The legacy
    // empty-tree marker is metadata, not a commit object, and must be projected.
    if revision.len() == 40 && revision != "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3" {
        return Ok(revision.to_owned());
    }
    if let Some(object) = read_identity(state, repo_id, revision, auth)? {
        return Ok(hex::encode(object.sha1()));
    }
    let projection = project_history_with_roots(
        state,
        repo_id,
        auth,
        1_000_000,
        Some(&[revision.to_owned()]),
    )?;
    let public = projection
        .identities
        .get(revision)
        .ok_or_else(|| failure("revision has no Git commit identity"))?;
    let object = projection
        .objects
        .iter()
        .find(|object| {
            object.object_type == ObjectType::Commit && hex::encode(object.sha1()) == *public
        })
        .ok_or_else(|| failure("projected identity commit is missing"))?;
    if embedded_revision(object, repo_id)? != revision {
        return Err(failure("projected identity revision mismatch"));
    }
    retain_identity(state, repo_id, revision, object, auth)?;
    Ok(public.clone())
}

pub(super) fn retain_identity(
    state: &HubState,
    repo_id: &str,
    revision: &str,
    object: &GitObject,
    auth: &AuthorizedRepository,
) -> Result<(), HubApiError> {
    let public = hex::encode(object.sha1());
    let bytes = generate_pack(std::slice::from_ref(object)).map_err(failure)?;
    let integrity = ObjectIntegrity::new(
        ShardlineHash::from_bytes(*blake3::hash(&bytes).as_bytes()),
        bytes.len() as u64,
    );
    // Persist both directions before returning an ID. A cancelled response or
    // restart cannot leave a returned ID dependent on an in-memory map.
    // Reverse lookup first: a forward-cache hit is then evidence that the
    // public ID was already made resolvable, even if this task was cancelled.
    for lookup in [public.as_str(), revision] {
        if let Some(stored) = read_identity(state, repo_id, lookup, auth)? {
            if stored.data != object.data {
                return Err(failure("conflicting immutable identity archive"));
            }
            continue;
        }
        let key = archive_key(repo_id, &format!("identities-v1/{lookup}"), auth)?;
        state
            .object_store
            .put_if_absent(&key, ObjectBody::from_slice(&bytes), &integrity)
            .map_err(failure)?;
        let stored = read_identity(state, repo_id, lookup, auth)?
            .ok_or_else(|| failure("identity archive publication missing"))?;
        if stored.data != object.data {
            return Err(failure("conflicting immutable identity archive"));
        }
    }
    Ok(())
}

/// Git discovery must publish the same reverse lookup as REST before exposing
/// a synthesized ID, even when no SDK request has visited that commit yet.
pub(super) fn retain_projection_identities(
    state: &HubState,
    repo_id: &str,
    projection: &GitProjection,
    auth: &AuthorizedRepository,
) -> Result<(), HubApiError> {
    let by_public: HashMap<_, _> = projection
        .identities
        .iter()
        .filter(|(internal, public)| internal != public)
        .map(|(internal, public)| (public.as_str(), internal.as_str()))
        .collect();
    for object in &projection.objects {
        if object.object_type == ObjectType::Commit
            && let Some(internal) = by_public.get(hex::encode(object.sha1()).as_str())
        {
            if embedded_revision(object, repo_id)? != *internal {
                return Err(failure("Git projection identity mismatch"));
            }
            retain_identity(state, repo_id, internal, object, auth)?;
        }
    }
    Ok(())
}

pub(crate) fn resolve_commit_id(
    state: &HubState,
    repo_id: &str,
    revision: &str,
    auth: &AuthorizedRepository,
) -> Result<Option<String>, HubApiError> {
    if let Some(internal) = state
        .store
        .resolve_revision(repo_id, revision)
        .map_err(failure)?
    {
        return Ok(Some(internal));
    }
    // Hub clients use short tag names, whereas authoritative metadata stores
    // tags under refs/tags/. Branches and exact immutable IDs retain priority.
    if !revision.is_empty()
        && !revision.starts_with("refs/")
        && let Some(internal) = state
            .store
            .resolve_revision(repo_id, &format!("refs/tags/{revision}"))
            .map_err(failure)?
    {
        return Ok(Some(internal));
    }
    if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(None);
    }
    let Some(object) = read_identity(state, repo_id, revision, auth)? else {
        return Ok(None);
    };
    let internal = embedded_revision(&object, repo_id)?;
    // The archive cannot establish repository membership or revive a deleted
    // repository. Authoritative metadata must still retain this exact commit.
    state
        .store
        .resolve_revision(repo_id, &internal)
        .map_err(failure)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored_revision(state: &HubState) -> String {
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
        let internal = "a".repeat(64);
        state.store.store_files(&internal, &[]).unwrap();
        state
            .store
            .create_revision("alice/repo", Some(&parent), &internal, "feature", "message")
            .unwrap();
        internal
    }

    #[test]
    fn public_and_legacy_ids_resolve_after_last_reference_is_deleted() {
        let (_temp, state) = super::super::tests::make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        let internal = stored_revision(&state);
        let public = public_commit_id_sync(&state, "alice/repo", &internal, &auth).unwrap();
        assert_eq!(public.len(), 40);
        assert_ne!(public, internal);
        state
            .store
            .delete_ref("alice/repo", "feature", &internal)
            .unwrap();
        // A cloned state carries no in-memory identity cache.
        let restarted = state.clone();
        for name in [&internal, &public] {
            assert_eq!(
                resolve_commit_id(&restarted, "alice/repo", name, &auth).unwrap(),
                Some(internal.clone())
            );
        }
        assert_eq!(
            public_commit_id_sync(&restarted, "alice/repo", &internal, &auth).unwrap(),
            public
        );
        assert!(
            resolve_commit_id(&restarted, "alice/repo", "feature", &auth)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn public_alias_does_not_grant_membership_in_another_repository() {
        let (_temp, state) = super::super::tests::make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        let internal = stored_revision(&state);
        let public = public_commit_id_sync(&state, "alice/repo", &internal, &auth).unwrap();
        state
            .store
            .create_repo(shardline_index::hub::HubRepoType::Model, "bob/repo", false)
            .unwrap();
        assert!(
            resolve_commit_id(&state, "bob/repo", &public, &auth)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn reversed_archive_cannot_substitute_a_different_commit() {
        let (_temp, state) = super::super::tests::make_hub_state();
        let auth = AuthorizedRepository::anonymous_full_access();
        let internal = stored_revision(&state);
        let public = public_commit_id_sync(&state, "alice/repo", &internal, &auth).unwrap();
        let key = archive_key("alice/repo", &format!("identities-v1/{public}"), &auth).unwrap();
        state.object_store.delete_if_present(&key).unwrap();
        let fake = GitObject::commit(format!("tree {}\n\nforged\n\nShardline-Revision: {internal}\nShardline-Repository: alice/repo\n", "0".repeat(40)).into_bytes());
        let bytes = generate_pack(&[fake]).unwrap();
        let integrity = ObjectIntegrity::new(
            ShardlineHash::from_bytes(*blake3::hash(&bytes).as_bytes()),
            bytes.len() as u64,
        );
        state
            .object_store
            .put_if_absent(&key, ObjectBody::from_slice(&bytes), &integrity)
            .unwrap();
        assert!(resolve_commit_id(&state, "alice/repo", &public, &auth).is_err());
    }
}
