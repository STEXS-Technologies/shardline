//! Recover cold metadata identities in bounded windows, oldest first.
//!
//! Only authoritative repository revisions and verified identity archives end
//! the walk. No new alias format, parent rewriting or authorization grant is
//! introduced. The existing identity admission slot covers the entire operation.

use super::{cached_public_commit_id, failure, project_history_with_roots};
use crate::{error::HubApiError, routes::HubState};
use shardline_server_core::AuthorizedRepository;

pub(super) const WINDOW: usize = 512;
const MAX_REVISIONS: usize = 1_000_000;
const EMPTY_TREE: &str = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

pub(super) fn recover(
    state: &HubState,
    repo: &str,
    requested: &str,
    auth: &AuthorizedRepository,
    maximum_file_entries: usize,
) -> Result<usize, HubApiError> {
    let checkpoints = checkpoints(state, repo, requested, auth)?;
    let mut remaining = maximum_file_entries;
    for root in checkpoints
        .into_iter()
        .rev()
        .chain(std::iter::once(requested.to_owned()))
    {
        let projection = project_history_with_roots(
            state,
            repo,
            auth,
            remaining,
            Some(std::slice::from_ref(&root)),
        )?;
        remaining = remaining
            .checked_sub(projection.file_entries)
            .ok_or_else(|| failure("Git identity recovery exceeds file entry limit"))?;
        // The projection durably retains each synthesized ancestor identity.
        // Drop its trees and caches before constructing the next window.
    }
    Ok(remaining)
}

fn checkpoints(
    state: &HubState,
    repo: &str,
    requested: &str,
    auth: &AuthorizedRepository,
) -> Result<Vec<String>, HubApiError> {
    let mut cursor = requested.to_owned();
    let mut roots = Vec::new();
    // Brent's cycle detector retains one ancestor rather than an unbounded
    // visited set. Checkpoint count and identifier length are independently
    // bounded, so discovery does not retain every revision or file listing.
    let mut anchor = cursor.clone();
    let mut power = 1_usize;
    let mut distance = 0_usize;
    for visited in 0..MAX_REVISIONS {
        if cursor.len() > 1024 {
            return Err(failure("Git identity revision exceeds length limit"));
        }
        if visited > 0
            && (cursor.len() == 16
                || (cursor.len() == 40 && cursor != EMPTY_TREE)
                || cached_public_commit_id(state, repo, &cursor, auth)?.is_some())
        {
            return Ok(roots);
        }
        let revision = state
            .store
            .get_revision(repo, &cursor)
            .map_err(failure)?
            .ok_or_else(|| failure("missing repository revision metadata"))?;
        if visited > 0 && visited % WINDOW == 0 {
            roots.push(cursor);
        }
        let Some(parent) = revision.parent_sha else {
            return Ok(roots);
        };
        distance = distance.saturating_add(1);
        if parent == anchor {
            return Err(failure("cyclic Hub commit ancestry"));
        }
        if distance == power {
            anchor = parent.clone();
            power = power.saturating_mul(2);
            distance = 0;
        }
        cursor = parent;
    }
    Err(failure("Git identity recovery exceeds revision work limit"))
}
