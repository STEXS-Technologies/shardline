//! Prepare verified, durable Git metadata before authoritative ref publication.

use shardline_index::hub::{HubFileEntry, HubRevision};
use shardline_server_core::AuthorizedRepository;

use super::{
    identity::{IDENTITY_ADMISSION, public_commit_id_sync, read_identity, retain_identity},
    projection::{archive_metadata_objects, project_prepared_revision},
};
use crate::{error::HubApiError, routes::HubState};

fn failure(error: impl std::fmt::Display) -> HubApiError {
    HubApiError::CasError(error.to_string())
}

pub(crate) struct PreparedPublication {
    pub(crate) revision: HubRevision,
    pub(crate) public_id: String,
}

pub(crate) async fn prepare_publication(
    state: &HubState,
    revision: HubRevision,
    files: Vec<HubFileEntry>,
    auth: &AuthorizedRepository,
) -> Result<PreparedPublication, HubApiError> {
    let permit = IDENTITY_ADMISSION.acquire().await.map_err(failure)?;
    let state = state.clone();
    let auth = auth.clone();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let prepared = prepare_sync(&state, revision, &files, &auth)?;
        // Reject invalid projections before writing their potentially large
        // file-entry set. This metadata write is also admitted blocking work.
        state
            .store
            .store_files(&prepared.revision.sha, &files)
            .map_err(failure)?;
        Ok(prepared)
    })
    .await
    .map_err(failure)?
}

fn prepare_sync(
    state: &HubState,
    mut revision: HubRevision,
    files: &[HubFileEntry],
    auth: &AuthorizedRepository,
) -> Result<PreparedPublication, HubApiError> {
    // Existing immutable revisions retain their original creation time. A
    // prepared-but-unpublished archive also pins its time across safe retries.
    if let Some(stored) = state
        .store
        .get_revision(&revision.repo_id, &revision.sha)
        .map_err(failure)?
    {
        if stored.parent_sha != revision.parent_sha || stored.message != revision.message {
            return Err(failure("conflicting immutable revision metadata"));
        }
        revision.created_at_unix_seconds = stored.created_at_unix_seconds;
    } else if let Some(cached) = read_identity(state, &revision.repo_id, &revision.sha, auth)? {
        let text = std::str::from_utf8(&cached.data).map_err(failure)?;
        let headers = text
            .split_once("\n\n")
            .ok_or_else(|| failure("invalid prepared commit headers"))?
            .0;
        let time = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("committer Shardline Hub <hub@shardline.dev> ")
                    .and_then(|value| value.strip_suffix(" +0000"))
            })
            .ok_or_else(|| failure("invalid prepared commit timestamp"))?;
        revision.created_at_unix_seconds = time.parse().map_err(failure)?;
    }
    let parent = revision
        .parent_sha
        .as_deref()
        .ok_or_else(|| failure("prepared revision has no parent"))?;
    let public_parent = public_commit_id_sync(state, &revision.repo_id, parent, auth)?;
    let (objects, commit) =
        project_prepared_revision(state, &revision, files, &public_parent, auth)?;
    let public_id = hex::encode(commit);
    let object = objects
        .iter()
        .find(|object| object.sha1() == commit)
        .ok_or_else(|| failure("prepared Git commit is missing"))?;
    archive_metadata_objects(state, &revision.repo_id, &public_id, &objects, auth)?;
    retain_identity(state, &revision.repo_id, &revision.sha, object, auth)?;
    // These archives are derived evidence only. Resolution still checks the
    // authoritative revision table, so preparation alone exposes no new commit.
    Ok(PreparedPublication {
        revision,
        public_id,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use shardline_index::hub::{HubRefUpdateOutcome, HubRepoType};
    use shardline_storage::ObjectStore;

    #[test]
    fn prepared_identity_is_private_replayable_and_rebuildable() {
        let (_root, state) = super::super::tests::make_hub_state();
        let repo = "alice/prepared";
        let auth = AuthorizedRepository::anonymous_full_access();
        state
            .store
            .create_repo(HubRepoType::Model, repo, false)
            .unwrap();
        let parent = state.store.resolve_revision(repo, "main").unwrap().unwrap();
        let original_history = state.store.list_revisions(repo).unwrap().len();
        let revision = HubRevision {
            repo_id: repo.to_owned(),
            ref_name: "main".to_owned(),
            sha: "a".repeat(64),
            parent_sha: Some(parent),
            message: Some("prepared".to_owned()),
            created_at_unix_seconds: 1_700_000_123,
        };
        state.store.store_files(&revision.sha, &[]).unwrap();
        let first = prepare_sync(&state, revision.clone(), &[], &auth).unwrap();
        assert_eq!(
            state.store.list_revisions(repo).unwrap().len(),
            original_history
        );
        assert!(
            super::super::identity::resolve_commit_id(&state, repo, &first.public_id, &auth)
                .unwrap()
                .is_none()
        );
        let mut later = revision.clone();
        later.created_at_unix_seconds = 1_700_000_456;
        let retry = prepare_sync(&state, later, &[], &auth).unwrap();
        assert_eq!(first.public_id, retry.public_id);
        assert_eq!(
            retry.revision.created_at_unix_seconds,
            revision.created_at_unix_seconds
        );
        assert!(matches!(
            state
                .store
                .publish_prepared_revision(&retry.revision)
                .unwrap(),
            HubRefUpdateOutcome::Updated(_)
        ));
        assert_eq!(
            super::super::identity::resolve_commit_id(&state, repo, &first.public_id, &auth)
                .unwrap()
                .as_deref(),
            Some(revision.sha.as_str())
        );
        for lookup in [&first.public_id, &revision.sha] {
            let key = super::super::projection::archive_key(
                repo,
                &format!("identities-v1/{lookup}"),
                &auth,
            )
            .unwrap();
            state.object_store.delete_if_present(&key).unwrap();
        }
        assert_eq!(
            public_commit_id_sync(&state, repo, &revision.sha, &auth).unwrap(),
            first.public_id
        );
    }

    #[test]
    fn competing_prepared_writers_do_not_publish_losing_history() {
        let (_root, state) = super::super::tests::make_hub_state();
        let repo = "alice/competing";
        let auth = AuthorizedRepository::anonymous_full_access();
        state
            .store
            .create_repo(HubRepoType::Model, repo, false)
            .unwrap();
        let parent = state.store.resolve_revision(repo, "main").unwrap().unwrap();
        let revision = HubRevision {
            repo_id: repo.to_owned(),
            ref_name: "main".to_owned(),
            sha: "a".repeat(64),
            parent_sha: Some(parent),
            message: Some("prepared".to_owned()),
            created_at_unix_seconds: 1_700_000_123,
        };
        let mut other = revision.clone();
        other.sha = "b".repeat(64);
        for candidate in [&revision, &other] {
            state.store.store_files(&candidate.sha, &[]).unwrap();
        }
        let first = prepare_sync(&state, revision, &[], &auth).unwrap();
        let second = prepare_sync(&state, other, &[], &auth).unwrap();
        assert!(matches!(
            state
                .store
                .publish_prepared_revision(&first.revision)
                .unwrap(),
            HubRefUpdateOutcome::Updated(_)
        ));
        let count = state.store.list_revisions(repo).unwrap().len();
        assert!(matches!(
            state
                .store
                .publish_prepared_revision(&second.revision)
                .unwrap(),
            HubRefUpdateOutcome::Conflict
        ));
        assert_eq!(state.store.list_revisions(repo).unwrap().len(), count);
        assert!(
            super::super::identity::resolve_commit_id(&state, repo, &second.public_id, &auth)
                .unwrap()
                .is_none()
        );
    }
}
