//! Bind Hub reads and mutations to authorized revisions, independently of storage.

use shardline_index::hub::canonical_ref_name;
use shardline_server_core::AuthorizedRepository;

use crate::{error::HubApiError, routes::HubState};

#[cfg(test)]
mod tests;

fn failure(error: impl std::fmt::Display) -> HubApiError {
    HubApiError::CasError(error.to_string())
}

/// Repository configuration is not a branch operation. The extractor enforces
/// Read/Write separately; a revision-limited credential cannot cover all refs.
pub(crate) fn authorize_repository_wide(
    repo: &str,
    auth: &AuthorizedRepository,
) -> Result<(), HubApiError> {
    if let Some(scope) = auth.repository()
        && (repo != format!("{}/{}", scope.owner(), scope.name()) || scope.revision().is_some())
    {
        return Err(HubApiError::Forbidden);
    }
    Ok(())
}

/// Mutations require the exact named target; read ancestry never grants writes.
/// The route's `HubRepository<true>` extractor separately requires Write scope.
pub(crate) fn authorize_write(
    repo: &str,
    target: &str,
    auth: &AuthorizedRepository,
) -> Result<(), HubApiError> {
    let Some(scope) = auth.repository() else {
        return Ok(());
    };
    if repo != format!("{}/{}", scope.owner(), scope.name()) {
        return Err(HubApiError::Forbidden);
    }
    if let Some(revision) = scope.revision()
        && canonical_ref_name(revision) != canonical_ref_name(target)
    {
        return Err(HubApiError::Forbidden);
    }
    Ok(())
}

pub(crate) async fn authorize_read(
    state: &HubState,
    repo: &str,
    requested: &str,
    commit: &str,
    auth: &AuthorizedRepository,
) -> Result<(), HubApiError> {
    let Some(scope) = auth.repository() else {
        return Ok(());
    };
    if repo != format!("{}/{}", scope.owner(), scope.name()) {
        return Err(HubApiError::Forbidden);
    }
    let Some(scoped) = scope.revision() else {
        return Ok(());
    };
    if scoped == requested {
        return Ok(());
    }
    // Canonical aliases name the same branch. An immutable credential must
    // never acquire the rights of a branch whose name happens to resemble it.
    let visible = visible_refs(state, repo, auth)?;
    if visible
        .as_ref()
        .is_some_and(|refs| refs.contains(canonical_ref_name(requested)))
    {
        return Ok(());
    }
    // A short tag name is an alias only when revision resolution cannot select
    // a branch or immutable revision first. Equal heads do not grant access
    // to a different named ref.
    if visible.as_ref().is_some_and(|refs| {
        refs.iter()
            .any(|name| name.strip_prefix("refs/tags/") == Some(requested))
    }) && state
        .store
        .resolve_revision(repo, requested)
        .map_err(failure)?
        .is_none()
    {
        return Ok(());
    }
    // Only immutable targets can be authorized through ancestry. A different
    // named ref remains denied even when it currently has an identical head.
    let immutable = requested == commit
        || (requested.len() == 40
            && requested.bytes().all(|byte| byte.is_ascii_hexdigit())
            && state
                .store
                .resolve_revision(repo, requested)
                .map_err(failure)?
                .is_none());
    if !immutable {
        return Err(HubApiError::Forbidden);
    }
    if crate::git::smart_http::resolve_commit_id(state, repo, scoped, auth)?.as_deref()
        == Some(commit)
        || branch_contains_commit(state, repo, scoped, commit, auth).await?
    {
        return Ok(());
    }
    Err(HubApiError::Forbidden)
}

/// Tags and immutable credentials authorize a snapshot, never its history.
pub(crate) fn snapshot_commit(
    state: &HubState,
    repo: &str,
    auth: &AuthorizedRepository,
) -> Result<Option<String>, HubApiError> {
    let Some(revision) = auth.repository().and_then(|scope| scope.revision()) else {
        return Ok(None);
    };
    let immutable = state
        .store
        .get_revision(repo, revision)
        .map_err(failure)?
        .is_some();
    let branch = !immutable
        && !canonical_ref_name(revision).starts_with("refs/tags/")
        && state
            .store
            .resolve_revision(repo, revision)
            .map_err(failure)?
            .is_some();
    if branch {
        return Ok(None);
    }
    crate::git::smart_http::resolve_commit_id(state, repo, revision, auth)
}

/// Derive the named-ref policy once, before iterating a ref listing. Immutable
/// credentials never gain branch-name visibility merely sharing a head.
pub(crate) fn visible_refs(
    state: &HubState,
    repo: &str,
    auth: &AuthorizedRepository,
) -> Result<Option<std::collections::HashSet<String>>, HubApiError> {
    let Some(scope) = auth.repository() else {
        return Ok(None);
    };
    if repo != format!("{}/{}", scope.owner(), scope.name()) {
        return Err(HubApiError::Forbidden);
    }
    let Some(revision) = scope.revision() else {
        return Ok(None);
    };
    let mut refs = std::collections::HashSet::new();
    if state
        .store
        .get_revision(repo, revision)
        .map_err(failure)?
        .is_some()
    {
        return Ok(Some(refs));
    }
    if state
        .store
        .resolve_revision(repo, revision)
        .map_err(failure)?
        .is_some()
    {
        refs.insert(canonical_ref_name(revision).to_owned());
    } else if !revision.starts_with("refs/") {
        let tag = format!("refs/tags/{revision}");
        if state
            .store
            .resolve_revision(repo, &tag)
            .map_err(failure)?
            .is_some()
        {
            refs.insert(tag);
        }
    }
    Ok(Some(refs))
}

pub(crate) async fn branch_contains_commit(
    state: &HubState,
    repo: &str,
    branch: &str,
    commit: &str,
    auth: &AuthorizedRepository,
) -> Result<bool, HubApiError> {
    if canonical_ref_name(branch).starts_with("refs/tags/")
        || state
            .store
            .get_revision(repo, branch)
            .map_err(failure)?
            .is_some()
    {
        return Ok(false);
    }
    // Indexed lookup distinguishes a real branch from an immutable or short
    // tag identity. Do not scan every repository ref to authorize a request.
    let Some(head) = state
        .store
        .resolve_revision(repo, branch)
        .map_err(failure)?
    else {
        return Ok(false);
    };
    if !crate::git::smart_http::is_ancestor(state, repo, commit, &head, auth).await? {
        return Ok(false);
    }
    Ok(state
        .store
        .resolve_revision(repo, branch)
        .map_err(failure)?
        .as_deref()
        == Some(head.as_str()))
}

/// One bounded traversal for a revision listing, rather than one walk per row.
/// `None` means repository-wide visibility; a set means exact allowed commits.
pub(crate) async fn visible_commits(
    state: &HubState,
    repo: &str,
    auth: &AuthorizedRepository,
) -> Result<Option<std::collections::HashSet<String>>, HubApiError> {
    let Some(scope) = auth.repository() else {
        return Ok(None);
    };
    if repo != format!("{}/{}", scope.owner(), scope.name()) {
        return Err(HubApiError::Forbidden);
    }
    let Some(revision) = scope.revision() else {
        return Ok(None);
    };
    let Some(head) = crate::git::smart_http::resolve_commit_id(state, repo, revision, auth)? else {
        return Ok(Some(std::collections::HashSet::new()));
    };
    let is_branch = !canonical_ref_name(revision).starts_with("refs/tags/")
        && state
            .store
            .get_revision(repo, revision)
            .map_err(failure)?
            .is_none()
        && state
            .store
            .resolve_revision(repo, revision)
            .map_err(failure)?
            .as_deref()
            == Some(head.as_str());
    if !is_branch {
        return Ok(Some(std::collections::HashSet::from([head])));
    }
    let commits = crate::git::smart_http::reachable_commits(state, repo, &head, auth).await?;
    if state
        .store
        .resolve_revision(repo, revision)
        .map_err(failure)?
        .as_deref()
        != Some(head.as_str())
    {
        return Err(HubApiError::Conflict(
            "branch changed during revision listing; retry".to_owned(),
        ));
    }
    Ok(Some(commits))
}
