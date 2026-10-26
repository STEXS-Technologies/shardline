//! Select authoritative metadata reachable from one bounded ref observation.
use std::collections::HashSet;

use super::*;

pub(super) fn live_revisions(
    state: &HubState,
    repo: &str,
    auth: &AuthorizedRepository,
) -> Result<(Vec<HubRevision>, Vec<String>), HubApiError> {
    let visible = crate::revision_access::visible_refs(state, repo, auth)?;
    let mut roots: Vec<_> = state
        .store
        .list_refs_bounded(repo, 10_000)
        .map_err(failure)?
        .into_iter()
        .filter(|reference| {
            visible.as_ref().is_none_or(|allowed| {
                allowed.contains(shardline_index::hub::canonical_ref_name(
                    &reference.ref_name,
                ))
            })
        })
        .map(|reference| reference.sha)
        .collect();
    if roots.is_empty()
        && let Some(snapshot) = crate::revision_access::snapshot_commit(state, repo, auth)?
    {
        roots.push(snapshot);
    }
    if roots.is_empty() {
        return Ok((Vec::new(), roots));
    }
    // Keep the single-query path for ordinary repositories. Only a proven
    // listing overflow warrants indexed traversal; never mask storage errors.
    match state.store.list_revisions_bounded(repo, 10_000) {
        Ok(revisions) => return Ok((revisions, roots)),
        Err(error) if error.is::<shardline_index::hub::HubRevisionListingOverflow>() => {}
        Err(error) => return Err(failure(error)),
    }
    let mut pending = roots.clone();
    let mut seen = HashSet::new();
    let mut revisions = Vec::new();
    while let Some(sha) = pending.pop() {
        if !seen.insert(sha.clone()) {
            continue;
        }
        if seen.len() > 10_000 {
            return Err(failure("Git history exceeds 10000 revisions"));
        }
        let revision = state
            .store
            .get_revision(repo, &sha)
            .map_err(failure)?
            .ok_or_else(|| failure("missing repository revision metadata"))?;
        // Legacy short identities remain the existing recovery boundary.
        if sha.len() != 16
            && let Some(parent) = &revision.parent_sha
        {
            pending.push(parent.clone());
        }
        revisions.push(revision);
    }
    Ok((revisions, roots))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn detached_history_does_not_consume_live_export_budget() {
        let (root, state) = super::super::super::tests::make_hub_state();
        state
            .store
            .create_repo(
                shardline_index::hub::HubRepoType::Model,
                "alice/repo",
                false,
            )
            .unwrap();
        let auth = AuthorizedRepository::anonymous_full_access();
        let before = project_history(&state, "alice/repo", &auth).unwrap();
        let mut connection =
            rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
        let transaction = connection.transaction().unwrap();
        for number in 0..10_001 {
            transaction.execute(
                "INSERT INTO shardline_hub_revisions (repo_id,ref_name,sha,parent_sha,message,created_at_unix_seconds) VALUES ('alice/repo','detached',?1,NULL,'detached',42)",
                [format!("{number:064x}")],
            ).unwrap();
        }
        transaction.commit().unwrap();
        let overflow = state
            .store
            .list_revisions_bounded("alice/repo", 10_000)
            .unwrap_err();
        assert!(overflow.is::<shardline_index::hub::HubRevisionListingOverflow>());
        let after = project_history(&state, "alice/repo", &auth).unwrap();
        assert_eq!(before.identities, after.identities);
        assert_eq!(
            generate_pack(&before.objects).unwrap(),
            generate_pack(&after.objects).unwrap()
        );
        let head = state
            .store
            .resolve_revision("alice/repo", "main")
            .unwrap()
            .unwrap();
        connection.execute(
            "UPDATE shardline_hub_revisions SET parent_sha=?2 WHERE repo_id='alice/repo' AND sha=?1",
            rusqlite::params![head, "f".repeat(64)],
        ).unwrap();
        assert!(project_history(&state, "alice/repo", &auth).is_err());
        connection.execute(
            "UPDATE shardline_hub_revisions SET parent_sha=sha WHERE repo_id='alice/repo' AND sha=?1",
            [&head],
        ).unwrap();
        assert!(project_history(&state, "alice/repo", &auth).is_err());
    }
}
