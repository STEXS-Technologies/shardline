//! Retry only unconditional branch publication; prepared content stays immutable.

use super::*;

// In a finite cohort of 32 one-shot writers, each failed comparison means
// another writer published. Allow the remaining writer to survive all 31
// competing publications. Continuous churn can still exhaust this bound.
const MAX_PUBLICATION_ATTEMPTS: u32 = 32;

pub(super) struct CommitTarget<'request> {
    pub(super) parent_sha: &'request str,
    pub(super) ref_name: &'request str,
    pub(super) retry_if_moved: bool,
}

pub(super) async fn publish(
    state: &HubState,
    repo_id: &str,
    target: CommitTarget<'_>,
    parsed: &ParsedCommit,
    instructions: &[stream::PreparedInstruction],
    auth: &AuthorizedRepository,
) -> Result<Json<CommitResponse>, HubApiError> {
    let attempts = if target.retry_if_moved {
        MAX_PUBLICATION_ATTEMPTS
    } else {
        1
    };
    let mut parent = target.parent_sha.to_owned();
    for attempt in 0..attempts {
        crate::revision_access::authorize_write(repo_id, target.ref_name, auth)?;
        if target.retry_if_moved {
            // Fully qualify the branch so a deleted hexadecimal branch name
            // cannot resolve as a historical commit. Missing branches stay gone.
            parent = state
                .store
                .resolve_revision(repo_id, &format!("refs/heads/{}", target.ref_name))
                .map_err(|error| HubApiError::CasError(error.to_string()))?
                .ok_or(HubApiError::RevisionNotFound)?;
        }
        if let Some(response) = try_apply_commit_to_ref(
            state,
            repo_id,
            &parent,
            target.ref_name,
            parsed,
            instructions.iter().cloned().map(Ok),
            auth,
        )
        .await?
        {
            return Ok(response);
        }
        // Only a failed atomic ref comparison reaches this point. Malformed
        // requests, storage failures and explicit-parent conflicts are errors,
        // never a reason to repeat uploads or reinterpret conditional writes.
        if attempt.saturating_add(1) < attempts {
            // Cap backoff so the larger contention budget does not introduce
            // exponential request delays. Total scheduled backoff stays < 2s.
            tokio::time::sleep(std::time::Duration::from_millis(1_u64 << attempt.min(6))).await;
        }
    }
    Err(branch_changed())
}
