#![no_main]
#![allow(clippy::indexing_slicing)]

use std::collections::BTreeMap;

use libfuzzer_sys::fuzz_target;
use shardline_index::{
    LocalIndexStore,
    hub::{HubRepoType, HubStore, ensure_hub_tables},
};

const INITIAL_SHA: &str = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

fuzz_target!(|data: &[u8]| {
    let Ok(tempdir) = tempfile::tempdir() else {
        return;
    };
    if ensure_hub_tables(tempdir.path()).is_err() {
        return;
    }
    let store = LocalIndexStore::open(tempdir.path().to_path_buf());
    let repo_id = "fuzz/hub-refs";
    if store
        .create_repo(HubRepoType::Model, repo_id, false)
        .is_err()
    {
        return;
    }

    let mut expected_refs = BTreeMap::from([("main".to_owned(), INITIAL_SHA.to_owned())]);
    let mut latest_branch_commits = BTreeMap::new();

    for (step, instruction) in data.as_chunks::<3>().0.iter().take(96).enumerate() {
        let branch = format!("feature-{}", instruction[0] % 8);
        match instruction[1] % 4 {
            // Create, advance, or recreate a branch from immutable history.
            0 => {
                let parent = expected_refs
                    .get(&branch)
                    .map(String::as_str)
                    .unwrap_or(INITIAL_SHA);
                let sha = format!("fuzz-{}-{step}", instruction[0]);
                assert!(
                    store
                        .create_revision(
                            repo_id,
                            Some(parent),
                            &sha,
                            &format!("refs/heads/{branch}"),
                            "fuzz ref operation",
                        )
                        .is_ok()
                );
                expected_refs.insert(branch.clone(), sha.clone());
                latest_branch_commits.insert(branch.clone(), sha);
            }
            // An exact compare-and-delete removes only the active name.
            1 => {
                if let Some(current_sha) = expected_refs.get(&branch).cloned() {
                    assert!(
                        store
                            .delete_ref(repo_id, &format!("refs/heads/{branch}"), &current_sha)
                            .is_ok()
                    );
                    expected_refs.remove(&branch);
                    assert_eq!(
                        store.resolve_revision(repo_id, &branch).ok().flatten(),
                        None
                    );
                    assert_eq!(
                        store.resolve_revision(repo_id, &current_sha).ok().flatten(),
                        Some(current_sha)
                    );
                }
            }
            // A stale target must not mutate the current active ref.
            2 => {
                assert!(
                    store
                        .delete_ref(repo_id, &branch, &format!("stale-{step}"))
                        .is_err()
                );
            }
            // The default branch is never deletable through the Git ref API.
            _ => {
                let main_sha = expected_refs.get("main").map(String::as_str);
                assert!(
                    main_sha.is_some_and(|sha| store.delete_ref(repo_id, "main", sha).is_err())
                );
            }
        }

        let active_refs = store.list_refs(repo_id).ok().map(|refs| {
            refs.into_iter()
                .map(|reference| (reference.ref_name, reference.sha))
                .collect::<BTreeMap<_, _>>()
        });
        assert_eq!(active_refs, Some(expected_refs.clone()));

        for (known_branch, known_sha) in &latest_branch_commits {
            assert_eq!(
                store.resolve_revision(repo_id, known_sha).ok().flatten(),
                Some(known_sha.clone())
            );
            assert_eq!(
                store.resolve_revision(repo_id, known_branch).ok().flatten(),
                expected_refs.get(known_branch).cloned()
            );
        }
    }
});
