//! Shared atomicity, normalization and concurrent-writer contract.
#![allow(clippy::unwrap_used)]
use super::*;

fn change(name: &str, old: Option<&str>, new: Option<&str>) -> HubRefChange {
    HubRefChange {
        ref_name: name.to_owned(),
        expected_sha: old.map(str::to_owned),
        new_sha: new.map(str::to_owned),
        message: "atomic ref fixture".to_owned(),
    }
}

pub(crate) fn contract<S: HubStore>(store: &S)
where
    S::Error: std::fmt::Debug,
{
    let repo = "atomic/contract";
    store.create_repo(HubRepoType::Model, repo, false).unwrap();
    store
        .create_revision(repo, None, "initial", "main", "Initial")
        .unwrap();
    assert_eq!(
        store
            .change_refs_atomically(
                repo,
                &[
                    change("alpha", None, Some("a")),
                    change("beta", None, Some("b")),
                ]
            )
            .unwrap(),
        HubRefBatchOutcome::Applied
    );
    assert_eq!(
        store
            .change_refs_atomically(
                repo,
                &[
                    change("alpha", Some("a"), Some("uncommitted")),
                    change("beta", Some("stale"), Some("replacement")),
                ]
            )
            .unwrap(),
        HubRefBatchOutcome::Conflict
    );
    assert_eq!(
        store.resolve_revision(repo, "alpha").unwrap().as_deref(),
        Some("a")
    );
    assert!(store.get_revision(repo, "uncommitted").unwrap().is_none());
    assert_eq!(store.list_refs(repo).unwrap().len(), 3);
    for changes in [
        vec![
            change("alpha", Some("a"), Some("x")),
            change("refs/heads/alpha", Some("a"), Some("y")),
        ],
        vec![
            change("alpha", Some("a"), Some("x")),
            change("refs/heads/main", Some("initial"), None),
        ],
        vec![change("alpha", None, Some("x"))],
    ] {
        assert_eq!(
            store.change_refs_atomically(repo, &changes).unwrap(),
            HubRefBatchOutcome::Conflict
        );
        assert_eq!(
            store.resolve_revision(repo, "alpha").unwrap().as_deref(),
            Some("a")
        );
    }
    assert_eq!(
        store
            .change_refs_atomically(
                repo,
                &[
                    change("alpha", Some("a"), Some("next")),
                    change("beta", Some("b"), None),
                    change("refs/tags/release", None, Some("next")),
                    change("main", Some("initial"), Some("next")),
                ]
            )
            .unwrap(),
        HubRefBatchOutcome::Applied
    );
    assert!(store.resolve_revision(repo, "beta").unwrap().is_none());
    assert_eq!(
        store.get_repo(repo).unwrap().unwrap().default_branch,
        "next"
    );
    assert!(store.get_revision(repo, "b").unwrap().is_some());
    assert_eq!(store.list_refs(repo).unwrap().len(), 3);
    let barrier = std::sync::Barrier::new(2);
    let runtime = tokio::runtime::Handle::try_current().ok();
    let outcomes = std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            let _entered = runtime.as_ref().map(tokio::runtime::Handle::enter);
            barrier.wait();
            store
                .change_refs_atomically(
                    repo,
                    &[
                        change("alpha", Some("next"), Some("winner-one")),
                        change("refs/tags/release", Some("next"), Some("winner-one")),
                    ],
                )
                .unwrap()
        });
        let second = scope.spawn(|| {
            let _entered = runtime.as_ref().map(tokio::runtime::Handle::enter);
            barrier.wait();
            store
                .change_refs_atomically(
                    repo,
                    &[
                        change("refs/tags/release", Some("next"), Some("winner-two")),
                        change("alpha", Some("next"), Some("winner-two")),
                    ],
                )
                .unwrap()
        });
        [first.join().unwrap(), second.join().unwrap()]
    });
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == HubRefBatchOutcome::Applied)
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == HubRefBatchOutcome::Conflict)
            .count(),
        1
    );
    let refs = store.list_refs(repo).unwrap();
    let alpha = refs
        .iter()
        .find(|reference| reference.ref_name == "alpha")
        .unwrap();
    let tag = refs
        .iter()
        .find(|reference| reference.ref_name == "refs/tags/release")
        .unwrap();
    assert_eq!(
        alpha.sha, tag.sha,
        "concurrent batches cannot split winners"
    );
}
