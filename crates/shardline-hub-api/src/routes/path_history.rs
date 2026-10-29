//! Bounded first-parent last-change history for requested repository paths.

use std::collections::{HashMap, HashSet};

use shardline_index::hub::{HubFileEntry, HubRevision};
use shardline_server_core::AuthorizedRepository;

use super::HubState;
use crate::{
    error::HubApiError,
    git::smart_http::{IDENTITY_ADMISSION, path_oids_with_budget_sync, public_commit_id},
};

fn failure(error: impl std::fmt::Display) -> HubApiError {
    HubApiError::CasError(error.to_string())
}

pub(crate) async fn last_path_commits(
    state: &HubState,
    repo: &str,
    head: &str,
    files: &[HubFileEntry],
    paths: &[String],
    auth: &AuthorizedRepository,
) -> Result<HashMap<String, serde_json::Value>, HubApiError> {
    let permit = IDENTITY_ADMISSION.acquire().await.map_err(failure)?;
    let state_copy = state.clone();
    let repo_copy = repo.to_owned();
    let head = head.to_owned();
    let files = files.to_vec();
    let paths = paths.to_vec();
    let history_auth = auth.clone();
    let revisions = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        find_last_changes(
            &state_copy,
            &repo_copy,
            &head,
            &files,
            &paths,
            &history_auth,
        )
    })
    .await
    .map_err(failure)??;
    let mut public = HashMap::new();
    let mut result = HashMap::new();
    for (path, revision) in revisions {
        let metadata = if let Some(metadata) = public.get(&revision.sha) {
            metadata
        } else {
            let id = public_commit_id(state, repo, &revision.sha, auth).await?;
            let seconds = i64::try_from(revision.created_at_unix_seconds).map_err(failure)?;
            let date = chrono::DateTime::from_timestamp(seconds, 0)
                .ok_or_else(|| failure("invalid path commit timestamp"))?
                .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                .to_string();
            let title = revision
                .message
                .as_deref()
                .unwrap_or_default()
                .split('\n')
                .next()
                .unwrap_or_default();
            public.entry(revision.sha.clone()).or_insert_with(|| {
                serde_json::json!({
                    "id": id, "title": title, "date": date,
                })
            })
        };
        result.insert(path, metadata.clone());
    }
    Ok(result)
}

fn find_last_changes(
    state: &HubState,
    repo: &str,
    head: &str,
    files: &[HubFileEntry],
    paths: &[String],
    auth: &AuthorizedRepository,
) -> Result<HashMap<String, HubRevision>, HubApiError> {
    let snapshot = crate::revision_access::snapshot_commit(state, repo, auth)?;
    if snapshot.as_deref().is_some_and(|allowed| allowed != head) {
        return Err(HubApiError::Forbidden);
    }
    let mut reader = PathHistoryReader {
        state,
        repo,
        auth,
        remaining_entries: 1_000_000,
        remaining_content: 64 * 1024 * 1024,
    };
    let mut wanted = reader.signatures(head, files, paths)?;
    let mut result = HashMap::new();
    let mut seen = HashSet::new();
    let mut next = head.to_owned();
    while !wanted.is_empty() {
        if !seen.insert(next.clone()) || seen.len() > 10_000 {
            return Err(failure("path history is cyclic or exceeds 10000 revisions"));
        }
        let revision = state
            .store
            .get_revision(repo, &next)
            .map_err(failure)?
            .ok_or_else(|| failure("incomplete repository path history"))?;
        let Some(parent) = revision.parent_sha.as_deref() else {
            result.extend(wanted.into_keys().map(|path| (path, revision.clone())));
            break;
        };
        let parent_files = state
            .store
            .get_files_bounded(parent, reader.remaining_entries.min(100_000))
            .map_err(failure)?;
        let pending_paths: Vec<_> = wanted.keys().cloned().collect();
        let previous = reader.signatures(parent, &parent_files, &pending_paths)?;
        wanted.retain(|path, signature| {
            if previous.get(path) == Some(signature) {
                true
            } else {
                result.insert(path.clone(), revision.clone());
                false
            }
        });
        // Snapshot credentials may see a change made by this exact commit,
        // but must not learn older commit IDs, messages or timestamps. Leave
        // unchanged paths' lastCommit unknown instead of fabricating history.
        if snapshot.is_some() {
            break;
        }
        next = parent.to_owned();
    }
    Ok(result)
}

struct PathHistoryReader<'history> {
    state: &'history HubState,
    repo: &'history str,
    auth: &'history AuthorizedRepository,
    remaining_entries: usize,
    remaining_content: usize,
}

impl PathHistoryReader<'_> {
    fn signatures(
        &mut self,
        revision: &str,
        files: &[HubFileEntry],
        paths: &[String],
    ) -> Result<HashMap<String, (String, String)>, HubApiError> {
        self.remaining_entries = self
            .remaining_entries
            .checked_sub(files.len())
            .ok_or_else(|| failure("path history exceeds metadata entry limit"))?;
        Ok(path_oids_with_budget_sync(
            self.state,
            self.repo,
            revision,
            files,
            paths,
            self.auth,
            &mut self.remaining_content,
        )?
        .into_iter()
        .map(|(path, metadata)| (path, (metadata.oid, metadata.mode)))
        .collect())
    }
}
