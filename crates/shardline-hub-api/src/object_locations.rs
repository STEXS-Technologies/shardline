//! Repository-scoped physical addressing, independent of credential breadth.

use std::collections::HashSet;

use shardline_index::hub::HubObjectKind;
use shardline_protocol::RepositoryScope;
use shardline_server_core::AuthorizedRepository;
use shardline_storage::{ObjectKey, ObjectStore};

use crate::{error::HubApiError, routes::HubState};

fn failure(error: impl std::fmt::Display) -> HubApiError {
    HubApiError::CasError(error.to_string())
}

pub(crate) fn remember(
    state: &HubState,
    auth: &AuthorizedRepository,
    kind: HubObjectKind,
    object_id: &str,
) -> Result<(), HubApiError> {
    if let Some(scope) = auth.repository() {
        state
            .store
            .remember_object_location(scope, kind, object_id)
            .map_err(failure)?;
    }
    Ok(())
}

pub(crate) fn file_key(
    state: &HubState,
    oid: &str,
    auth: &AuthorizedRepository,
) -> Result<ObjectKey, HubApiError> {
    shardline_protocol_adapters::validate_content_hash(oid)?;
    locate(state, auth, HubObjectKind::File, oid, |scope| {
        ObjectKey::parse(&format!(
            "protocols/lfs/{}/objects/{oid}",
            shardline_server_core::protocol_support::scope_namespace(scope)
        ))
        .map_err(failure)
    })
}

/// Cross-storage-scope addressing requires proof of published file membership.
pub(crate) fn file_member_key(
    state: &HubState,
    auth: &AuthorizedRepository,
    member: &crate::git::smart_http::PublishedFileMember<'_>,
) -> Result<ObjectKey, HubApiError> {
    shardline_protocol_adapters::validate_content_hash(member.oid())?;
    if let Some(scope) = auth.repository()
        && member.repo() != format!("{}/{}", scope.owner(), scope.name())
    {
        return Err(HubApiError::Forbidden);
    }
    locate_with_policy(
        state,
        auth,
        HubObjectKind::File,
        member.oid(),
        |scope| {
            ObjectKey::parse(&format!(
                "protocols/lfs/{}/objects/{}",
                shardline_server_core::protocol_support::scope_namespace(scope),
                member.oid()
            ))
            .map_err(failure)
        },
        true,
    )
}

/// Locations are hints, never arbitrary keys: every candidate is rebuilt from
/// this caller's verified provider/owner/repository and a storage revision.
pub(crate) fn locate(
    state: &HubState,
    auth: &AuthorizedRepository,
    kind: HubObjectKind,
    object_id: &str,
    key: impl Fn(Option<&RepositoryScope>) -> Result<ObjectKey, HubApiError>,
) -> Result<ObjectKey, HubApiError> {
    locate_with_policy(state, auth, kind, object_id, key, false)
}

/// Internal commit-identity metadata, never a file-payload authorization grant.
/// The identity reader verifies the archived commit and embedded repository;
/// wire handlers must authorize its resolved revision before exposing metadata.
pub(crate) fn locate_identity(
    state: &HubState,
    auth: &AuthorizedRepository,
    object_id: &str,
    key: impl Fn(Option<&RepositoryScope>) -> Result<ObjectKey, HubApiError>,
) -> Result<ObjectKey, HubApiError> {
    object_id
        .strip_prefix("identities-v1/")
        // Legacy Hub revisions are opaque strings, not always hex digests.
        // ObjectKey validates path components; the identity parser binds the
        // final immutable object back to this exact repository and lookup.
        .filter(|lookup| {
            !lookup.is_empty() && lookup.len() <= 1024 && !lookup.chars().any(char::is_control)
        })
        .ok_or_else(|| failure("invalid commit identity lookup"))?;
    locate_with_policy(state, auth, HubObjectKind::GitArchive, object_id, key, true)
}

fn locate_with_policy(
    state: &HubState,
    auth: &AuthorizedRepository,
    kind: HubObjectKind,
    object_id: &str,
    key: impl Fn(Option<&RepositoryScope>) -> Result<ObjectKey, HubApiError>,
    verified_repository_lookup: bool,
) -> Result<ObjectKey, HubApiError> {
    let Some(scope) = auth.repository() else {
        return key(None);
    };
    let original = key(Some(scope))?;
    if let Some(location) = state
        .store
        .object_location(scope, kind, object_id)
        .map_err(failure)?
    {
        // Ordinary payload lookup stays limited to the storage revision.
        // Verified internal metadata or graph membership permits repository lookup.
        if verified_repository_lookup
            || scope.revision().is_none()
            || scope.revision() == location.storage_revision.as_deref()
        {
            let stored_scope = storage_scope(scope, location.storage_revision.as_deref())?;
            return key(Some(&stored_scope));
        }
    }
    if scope.revision().is_some() && !verified_repository_lookup {
        if state
            .object_store
            .metadata(&original)
            .map_err(failure)?
            .is_some()
        {
            cache_legacy(state, scope, kind, object_id);
        }
        return Ok(original);
    }

    let repo_id = format!("{}/{}", scope.owner(), scope.name());
    let Some(repo) = state.store.get_repo(&repo_id).map_err(failure)? else {
        return Ok(original);
    };
    let mut visited = HashSet::new();
    // The legacy `default_branch` field stores the main commit identity, not
    // necessarily its ref name. Older credentials commonly used literal main.
    for revision in [
        scope.revision(),
        None,
        Some("main"),
        Some(repo.default_branch.as_str()),
    ] {
        if let Some(found) =
            find_legacy(state, scope, kind, object_id, revision, &key, &mut visited)?
        {
            return Ok(found);
        }
    }
    // Do not turn a missing object into a scan of every branch or bucket.
    // Legacy reads with their original credential also populate this index.
    Ok(original)
}

/// A verified Git traversal may follow a member stored by another credential
/// for the same provider and repository. Possessing an arbitrary hash is not
/// enough: only projection can construct the membership witness.
pub(crate) fn locate_git_member(
    state: &HubState,
    auth: &AuthorizedRepository,
    member: &crate::git::smart_http::GitArchiveMember<'_>,
    key: impl Fn(Option<&RepositoryScope>) -> Result<ObjectKey, HubApiError>,
) -> Result<ObjectKey, HubApiError> {
    if let Some(scope) = auth.repository()
        && member.repo() != format!("{}/{}", scope.owner(), scope.name())
    {
        return Err(HubApiError::Forbidden);
    }
    locate_with_policy(
        state,
        auth,
        HubObjectKind::GitArchive,
        member.oid(),
        key,
        true,
    )
}

fn storage_scope(
    scope: &RepositoryScope,
    revision: Option<&str>,
) -> Result<RepositoryScope, HubApiError> {
    RepositoryScope::new(scope.provider(), scope.owner(), scope.name(), revision).map_err(failure)
}

fn find_legacy(
    state: &HubState,
    scope: &RepositoryScope,
    kind: HubObjectKind,
    object_id: &str,
    revision: Option<&str>,
    key: &impl Fn(Option<&RepositoryScope>) -> Result<ObjectKey, HubApiError>,
    visited: &mut HashSet<String>,
) -> Result<Option<ObjectKey>, HubApiError> {
    let source = storage_scope(scope, revision)?;
    let candidate = key(Some(&source))?;
    if !visited.insert(candidate.as_str().to_owned()) {
        return Ok(None);
    }
    if state
        .object_store
        .metadata(&candidate)
        .map_err(failure)?
        .is_none()
    {
        return Ok(None);
    }
    // Read-only runtime roles may discover legacy bytes without being able to
    // cache the result. The original object, not this optional index, is durable.
    cache_legacy(state, &source, kind, object_id);
    Ok(Some(candidate))
}

fn cache_legacy(state: &HubState, source: &RepositoryScope, kind: HubObjectKind, object_id: &str) {
    if let Err(error) = state
        .store
        .remember_object_location(source, kind, object_id)
    {
        tracing::warn!(error = %error, "could not cache legacy Hub object location");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use shardline_index::{
        LocalIndexStore,
        hub::{BoxedHubStore, HubRepoType},
    };
    use shardline_protocol::{RepositoryProvider, ShardlineHash, TokenClaims, TokenScope};
    use shardline_server_core::{AuthProvider, LocalHmacProvider, ServerObjectStore};
    use shardline_storage::{ObjectBody, ObjectIntegrity};

    fn capability(owner: &str, revision: Option<&str>) -> AuthorizedRepository {
        let provider = LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap();
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, owner, "model", revision).unwrap();
        let claims =
            TokenClaims::new("local", "subject", TokenScope::Read, scope, u64::MAX).unwrap();
        let token = provider.mint_token(&claims).unwrap();
        AuthorizedRepository::verify_and_authorize(&provider, &token, TokenScope::Read).unwrap()
    }

    fn state(root: &std::path::Path) -> HubState {
        HubState {
            store: BoxedHubStore::from_store(LocalIndexStore::new(root.join("metadata")).unwrap()),
            object_store: ServerObjectStore::local(root.join("objects")).unwrap(),
            auth: None,
            http_client: None,
            webhook_secret_cipher: None,
            public_base_url: "http://127.0.0.1:8080".to_owned(),
        }
    }

    fn persist(state: &HubState, auth: &AuthorizedRepository) -> (String, ObjectKey) {
        let data = b"immutable repository payload";
        let hash = *blake3::hash(data).as_bytes();
        let oid = hex::encode(hash);
        let key = crate::routes::lfs_object_key(&oid, auth).unwrap();
        state
            .object_store
            .put_if_absent(
                &key,
                ObjectBody::from_slice(data),
                &ObjectIntegrity::new(ShardlineHash::from_bytes(hash), data.len() as u64),
            )
            .unwrap();
        (oid, key)
    }

    #[test]
    fn indexed_locations_allow_broad_reads_without_widening_other_refs_or_repositories() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let main = capability("alice", Some("main"));
        let broad = capability("alice", None);
        let branch = capability("alice", Some("private-branch"));
        let other = capability("bob", None);
        let (oid, key) = persist(&state, &main);
        remember(&state, &main, HubObjectKind::File, &oid).unwrap();
        assert_eq!(file_key(&state, &oid, &broad).unwrap(), key);
        assert_eq!(file_key(&state, &oid, &main).unwrap(), key);
        let denied = file_key(&state, &oid, &branch).unwrap();
        assert_ne!(denied, key);
        assert!(state.object_store.metadata(&denied).unwrap().is_none());
        let denied = file_key(&state, &oid, &other).unwrap();
        assert_ne!(denied, key);
        assert!(state.object_store.metadata(&denied).unwrap().is_none());
    }

    #[test]
    fn broad_legacy_read_discovers_default_branch_without_moving_objects() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        state
            .store
            .create_repo(HubRepoType::Model, "alice/model", true)
            .unwrap();
        let main = capability("alice", Some("main"));
        let broad = capability("alice", None);
        let (oid, key) = persist(&state, &main);
        assert!(
            state
                .store
                .object_location(broad.repository().unwrap(), HubObjectKind::File, &oid)
                .unwrap()
                .is_none()
        );
        assert_eq!(file_key(&state, &oid, &broad).unwrap(), key);
        let location = state
            .store
            .object_location(broad.repository().unwrap(), HubObjectKind::File, &oid)
            .unwrap()
            .unwrap();
        assert_eq!(location.storage_revision.as_deref(), Some("main"));
        assert!(state.object_store.metadata(&key).unwrap().is_some());
    }
}
