//! Bounded commit-only ancestry checks; never load trees or file payloads.

use std::collections::{HashMap, HashSet};

use shardline_server_core::AuthorizedRepository;

use super::{
    super::pack::ObjectType,
    identity::IDENTITY_ADMISSION,
    pack_parse::parse_pack_data_with_budget,
    projection::{read_member_archive_key, read_object_bounded},
};
use crate::{error::HubApiError, routes::HubState};

const MAX_COMMITS: usize = 10_000;
const MAX_COMMIT_BYTES: usize = 20 * 1024 * 1024;
const MAX_HISTORY_BYTES: usize = 64 * 1024 * 1024;

fn failure(error: impl std::fmt::Display) -> HubApiError {
    HubApiError::CasError(error.to_string())
}

pub(crate) async fn is_ancestor(
    state: &HubState,
    repo: &str,
    ancestor: &str,
    head: &str,
    auth: &AuthorizedRepository,
) -> Result<bool, HubApiError> {
    let permit = IDENTITY_ADMISSION.acquire().await.map_err(failure)?;
    let state = state.clone();
    let repo = repo.to_owned();
    let ancestor = ancestor.to_owned();
    let head = head.to_owned();
    let auth = auth.clone();
    tokio::task::spawn_blocking(move || {
        // A cancelled caller must not release admission while work continues.
        let _permit = permit;
        contains(&state, &repo, &ancestor, &head, &auth)
    })
    .await
    .map_err(failure)?
}

fn contains(
    state: &HubState,
    repo: &str,
    ancestor: &str,
    head: &str,
    auth: &AuthorizedRepository,
) -> Result<bool, HubApiError> {
    Ok(walk(state, repo, Some(ancestor), head, auth)?.contains(ancestor))
}

pub(crate) async fn reachable_commits(
    state: &HubState,
    repo: &str,
    head: &str,
    auth: &AuthorizedRepository,
) -> Result<HashSet<String>, HubApiError> {
    let permit = IDENTITY_ADMISSION.acquire().await.map_err(failure)?;
    let state = state.clone();
    let repo = repo.to_owned();
    let head = head.to_owned();
    let auth = auth.clone();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        walk(&state, &repo, None, &head, &auth)
    })
    .await
    .map_err(failure)?
}

fn walk(
    state: &HubState,
    repo: &str,
    target: Option<&str>,
    head: &str,
    auth: &AuthorizedRepository,
) -> Result<HashSet<String>, HubApiError> {
    // A branch credential can traverse only from its own selected head. Once
    // bound, verified parent hashes may follow repository-scoped locations.
    if let Some(scope) = auth.repository() {
        if repo != format!("{}/{}", scope.owner(), scope.name()) {
            return Err(HubApiError::Forbidden);
        }
        if let Some(revision) = scope.revision()
            && super::identity::resolve_commit_id(state, repo, revision, auth)?.as_deref()
                != Some(head)
        {
            return Err(HubApiError::Forbidden);
        }
    }
    let mut pending = vec![(head.to_owned(), false)];
    let mut visited = HashSet::new();
    let mut active = HashSet::new();
    let mut remaining = MAX_HISTORY_BYTES;
    while let Some((sha, finish)) = pending.pop() {
        if finish {
            active.remove(&sha);
            visited.insert(sha);
            continue;
        }
        if target == Some(sha.as_str()) {
            return Ok(HashSet::from([sha]));
        }
        if visited.contains(&sha) {
            continue;
        }
        if !active.insert(sha.clone()) {
            return Err(failure("cyclic repository history"));
        }
        if active.len().saturating_add(visited.len()) > MAX_COMMITS {
            return Err(failure("repository ancestry exceeds 10000 commits"));
        }
        let parents = parents(state, repo, &sha, auth, &mut remaining)?;
        if pending.len().saturating_add(parents.len()) > MAX_COMMITS * 2 {
            return Err(failure("repository ancestry exceeds pending commit limit"));
        }
        pending.push((sha, true));
        pending.extend(parents.into_iter().map(|parent| (parent, false)));
    }
    Ok(visited)
}

fn parents(
    state: &HubState,
    repo: &str,
    sha: &str,
    auth: &AuthorizedRepository,
    remaining: &mut usize,
) -> Result<Vec<String>, HubApiError> {
    if sha.len() != 40 || sha == "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3" {
        let revision = state
            .store
            .get_revision(repo, sha)
            .map_err(failure)?
            .ok_or_else(|| failure("incomplete repository ancestry"))?;
        // Hub revisions carry messages too. Charge their fetched metadata to
        // the same budget as Git-origin commit archives before following it.
        let bytes = [
            revision.repo_id.as_str(),
            revision.ref_name.as_str(),
            revision.sha.as_str(),
            revision.parent_sha.as_deref().unwrap_or_default(),
            revision.message.as_deref().unwrap_or_default(),
        ]
        .into_iter()
        .try_fold(0_usize, |total, field| total.checked_add(field.len()))
        .filter(|bytes| *bytes <= MAX_COMMIT_BYTES)
        .ok_or_else(|| failure("repository commit metadata exceeds byte limit"))?;
        *remaining = remaining
            .checked_sub(bytes)
            .ok_or_else(|| failure("repository ancestry exceeds byte limit"))?;
        return Ok(revision.parent_sha.into_iter().collect());
    }
    let key = read_member_archive_key(state, repo, sha, auth)?;
    let bytes = read_object_bounded(state, &key, (*remaining).min(MAX_COMMIT_BYTES))?
        .ok_or_else(|| failure("missing archived ancestry commit"))?;
    *remaining = remaining
        .checked_sub(bytes.len())
        .ok_or_else(|| failure("repository ancestry exceeds byte limit"))?;
    let mut objects =
        parse_pack_data_with_budget(&bytes, &HashMap::new(), (*remaining).min(MAX_COMMIT_BYTES))
            .map_err(failure)?;
    if objects.len() != 1 {
        return Err(failure("ancestry archive must contain one commit"));
    }
    let object = objects
        .pop()
        .ok_or_else(|| failure("empty ancestry archive"))?;
    if object.object_type != ObjectType::Commit || hex::encode(object.sha1()) != sha {
        return Err(failure("ancestry commit identity mismatch"));
    }
    *remaining = remaining
        .checked_sub(object.data.len())
        .ok_or_else(|| failure("repository ancestry exceeds byte limit"))?;
    let end = object
        .data
        .windows(2)
        .position(|pair| pair == b"\n\n")
        .unwrap_or(object.data.len());
    // Git messages may declare a non-UTF-8 encoding. Ancestry uses only the
    // ASCII header block; message bytes cannot manufacture parent edges.
    let header_bytes = object
        .data
        .get(..end)
        .ok_or_else(|| failure("invalid commit headers"))?;
    let headers = std::str::from_utf8(header_bytes).map_err(failure)?;
    let mut parents = Vec::new();
    for parent in headers
        .lines()
        .filter_map(|line| line.strip_prefix("parent "))
    {
        if parent.len() != 40 || !parent.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(failure("invalid ancestry parent identity"));
        }
        if parents.len() >= MAX_COMMITS {
            return Err(failure("commit exceeds ancestry parent limit"));
        }
        parents.push(parent.to_owned());
    }
    Ok(parents)
}

#[cfg(test)]
mod tests;
