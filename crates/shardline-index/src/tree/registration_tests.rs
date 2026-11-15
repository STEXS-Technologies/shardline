#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

fn entry(owner: &str, revision: &str, path: &str, timestamp: u64) -> TreeEntry {
    TreeEntry {
        provider: "generic".to_owned(),
        owner: owner.to_owned(),
        repo: "repo".to_owned(),
        revision: revision.to_owned(),
        path: path.to_owned(),
        file_id: "ab".repeat(32),
        size_bytes: 100,
        updated_at_unix_seconds: timestamp,
    }
}

async fn tree_scan_contract<S: TreeStore>(store: &S, owner: &str)
where
    S::Error: std::fmt::Debug,
{
    let namespace = TreeKey::new("generic", owner, "repo", "main");
    let mut inserted = Vec::new();
    let mut paths = vec![
        "aaa",
        "aaa/early",
        "literal%",
        "literal%/child",
        "literal_/child",
        "literal_neighbor",
        "ζ",
        "ζ/child",
        "ζ/last",
        "ζ-other",
        "\u{d7ff}",
        "\u{d7ff}/child",
        "\u{e000}/neighbor",
        "\u{10ffff}",
        "\u{10ffff}/child",
        "\u{10ffff}\u{10ffff}/child",
    ];
    paths.sort_unstable();
    for path in &paths {
        let row = entry(owner, "main", path, 1);
        store.upsert_tree_entry(&row).await.unwrap();
        inserted.push(row);
    }
    for mut scoped in [
        entry(owner, "feature", "outside/revision", 1),
        entry(owner, "main", "outside/provider", 1),
        entry(owner, "main", "outside/owner", 1),
        entry(owner, "main", "outside/repo", 1),
    ] {
        if scoped.path.ends_with("provider") {
            scoped.provider = "other".to_owned();
        }
        if scoped.path.ends_with("owner") {
            scoped.owner = format!("{owner}-other");
        }
        if scoped.path.ends_with("repo") {
            scoped.repo = "other".to_owned();
        }
        store.upsert_tree_entry(&scoped).await.unwrap();
        inserted.push(scoped);
    }
    for prefix in [
        "",
        "aaa",
        "literal%",
        "literal_",
        "ζ",
        "\u{d7ff}",
        "\u{10ffff}",
        "\u{10ffff}\u{10ffff}",
        "missing",
    ] {
        for cursor in [None, Some("literal%"), Some("ζ/child"), Some("\u{10ffff}")] {
            for limit in [0, 1, 2, 100] {
                let expected = paths
                    .iter()
                    .copied()
                    .filter(|path| {
                        prefix.is_empty()
                            || *path == prefix
                            || path
                                .strip_prefix(prefix)
                                .is_some_and(|rest| rest.starts_with('/'))
                    })
                    .filter(|path| cursor.is_none_or(|after| *path > after))
                    .take(limit)
                    .collect::<Vec<_>>();
                let actual = store
                    .scan_tree(&namespace, prefix, cursor, limit)
                    .await
                    .unwrap();
                let actual_paths = actual
                    .iter()
                    .map(|row| row.path.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual_paths, expected,
                    "prefix={prefix:?} cursor={cursor:?} limit={limit}"
                );
            }
        }
    }
    for row in inserted {
        let row_namespace = TreeKey::new(&row.provider, &row.owner, &row.repo, &row.revision);
        assert_eq!(
            store
                .delete_tree_entries(&row_namespace, &row.path, false)
                .await
                .unwrap(),
            1
        );
    }
}

async fn revision_listing_contract<S: TreeStore>(store: &S, owner: &str)
where
    S::Error: std::fmt::Debug,
{
    let repo = RepoKey::new("generic", owner, "repo");
    let names = ["rev-00", "rev-01", "rev-02", "rev-03", "rev-04"];
    for name in names.into_iter().rev() {
        let revision = RevisionRecord {
            provider: repo.provider.clone(),
            owner: repo.owner.clone(),
            repo: repo.repo.clone(),
            revision: name.to_owned(),
            created_at_unix_seconds: 1,
            updated_at_unix_seconds: 1,
        };
        assert!(store.upsert_revision(&revision).await.unwrap());
    }
    for cursor in [None, Some(""), Some("rev-02"), Some("rev-025"), Some("z")] {
        for limit in [0, 1, 3, 100] {
            let expected: Vec<_> = names
                .iter()
                .copied()
                .filter(|name| cursor.is_none_or(|cursor| *name > cursor))
                .take(limit)
                .collect();
            let actual = store.list_revisions(&repo, cursor, limit).await.unwrap();
            assert_eq!(
                actual
                    .iter()
                    .map(|record| record.revision.as_str())
                    .collect::<Vec<_>>(),
                expected,
                "cursor={cursor:?} limit={limit}"
            );
        }
    }
    for name in names {
        assert_eq!(store.delete_revision(&repo, name).await.unwrap(), 1);
    }
}

async fn revision_pruning_contract<S: TreeStore>(store: &S, owner: &str)
where
    S::Error: std::fmt::Debug,
{
    let repo = RepoKey::new("generic", owner, "repo");
    let other_repo = RepoKey::new("generic", owner, "other-repo");
    for (revisions, survivors, cap) in [
        (
            [
                ("rev-b", 200),
                ("rev-a", 500),
                ("rev-c", 300),
                ("rev-e", 100),
                ("rev-d", 400),
            ],
            vec!["rev-a", "rev-d"],
            2,
        ),
        (
            [
                ("rev-00", 7),
                ("rev-01", 7),
                ("rev-02", 7),
                ("rev-03", 7),
                ("rev-04", 7),
            ],
            vec!["rev-02", "rev-03", "rev-04"],
            3,
        ),
    ] {
        for (revision, timestamp) in revisions {
            store
                .register_tree_entry(&entry(owner, revision, "file", timestamp), 10, 10)
                .await
                .unwrap();
        }
        let mut other = entry(owner, "main", "file", 1);
        other.repo = other_repo.repo.clone();
        store.register_tree_entry(&other, 10, 10).await.unwrap();
        for untouched_cap in [5, 10] {
            assert_eq!(
                store
                    .prune_revisions_over_cap(&repo, untouched_cap)
                    .await
                    .unwrap(),
                0
            );
            assert_eq!(store.count_revisions(&repo).await.unwrap(), 5);
            assert_eq!(store.count_tree_entries(&repo).await.unwrap(), 5);
        }
        assert_eq!(
            store.prune_revisions_over_cap(&repo, cap).await.unwrap(),
            5_u64.saturating_sub(cap as u64)
        );
        let remaining = store.list_revisions(&repo, None, 100).await.unwrap();
        assert_eq!(
            remaining
                .iter()
                .map(|row| row.revision.as_str())
                .collect::<Vec<_>>(),
            survivors
        );
        assert_eq!(store.count_revisions(&repo).await.unwrap(), cap as u64);
        for (revision, _) in revisions {
            let tree = TreeKey::new("generic", owner, "repo", revision);
            assert_eq!(
                store.scan_tree(&tree, "", None, 100).await.unwrap().len(),
                usize::from(survivors.contains(&revision))
            );
        }
        assert_eq!(store.count_revisions(&other_repo).await.unwrap(), 1);
        let other_revisions = store.list_revisions(&other_repo, None, 100).await.unwrap();
        assert_eq!(other_revisions.first().unwrap().revision, "main");
        assert_eq!(
            store
                .tree_entry(
                    &TreeKey::new("generic", owner, "other-repo", "main"),
                    "file"
                )
                .await
                .unwrap(),
            Some(other)
        );
        for key in [&repo, &other_repo] {
            let count = store.count_revisions(key).await.unwrap();
            assert_eq!(store.prune_revisions_over_cap(key, 0).await.unwrap(), count);
            assert_eq!(store.count_revisions(key).await.unwrap(), 0);
            assert_eq!(store.count_tree_entries(key).await.unwrap(), 0);
        }
    }
}

async fn transactional_registration<S: TreeStore>(store: &S, owner: &str)
where
    S::Error: std::fmt::Debug,
{
    let e = entry(owner, "main", "file", 20);
    let repo = RepoKey::new(&e.provider, &e.owner, &e.repo);
    let rev = RevisionRecord {
        provider: e.provider.clone(),
        owner: e.owner.clone(),
        repo: e.repo.clone(),
        revision: "main".to_owned(),
        created_at_unix_seconds: 10,
        updated_at_unix_seconds: 11,
    };
    assert!(store.create_revision_if_absent(&rev).await.unwrap());
    assert_eq!(
        store.register_tree_entry(&e, 1, 2).await.unwrap(),
        TreeRegistrationOutcome::Registered(TreeEntryOutcome { created: true })
    );
    let mut refreshed = e.clone();
    refreshed.updated_at_unix_seconds = 21;
    assert_eq!(
        store.register_tree_entry(&refreshed, 1, 2).await.unwrap(),
        TreeRegistrationOutcome::Registered(TreeEntryOutcome { created: false })
    );
    let original = store.revision(&repo, "main").await.unwrap().unwrap();
    assert_eq!(original.created_at_unix_seconds, 10);
    assert_eq!(original.updated_at_unix_seconds, 21);
    let mut conflict = rev.clone();
    conflict.created_at_unix_seconds = 999;
    conflict.updated_at_unix_seconds = 999;
    assert!(!store.create_revision_if_absent(&conflict).await.unwrap());
    assert_eq!(
        store.revision(&repo, "main").await.unwrap().unwrap(),
        original
    );
    assert_eq!(
        store
            .register_tree_entry(&entry(owner, "other", "file", 99), 1, 2)
            .await
            .unwrap(),
        TreeRegistrationOutcome::LimitExceeded
    );
    assert!(store.revision(&repo, "other").await.unwrap().is_none());
    assert_eq!(
        store
            .register_tree_entry(&entry(owner, "main", "second", 99), 1, 1)
            .await
            .unwrap(),
        TreeRegistrationOutcome::LimitExceeded
    );
    assert!(
        store
            .tree_entry(&TreeKey::new("generic", owner, "repo", "main"), "second")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.revision(&repo, "main").await.unwrap().unwrap(),
        original
    );
}

async fn registration_rollback<S: TreeStore>(store: &S, owner: &str)
where
    S::Error: std::fmt::Debug,
{
    let valid = entry(owner, "existing", "file", 12);
    assert!(matches!(
        store.register_tree_entry(&valid, 10, 10).await.unwrap(),
        TreeRegistrationOutcome::Registered(_)
    ));
    let repo = RepoKey::new("generic", owner, "repo");
    let original = store.revision(&repo, "existing").await.unwrap().unwrap();
    let mut failed_refresh = valid.clone();
    failed_refresh.size_bytes = u64::MAX;
    failed_refresh.updated_at_unix_seconds = 99;
    assert!(
        store
            .register_tree_entry(&failed_refresh, 10, 10)
            .await
            .is_err()
    );
    assert_eq!(
        store.revision(&repo, "existing").await.unwrap().unwrap(),
        original
    );
    assert_eq!(
        store
            .tree_entry(&TreeKey::new("generic", owner, "repo", "existing"), "file")
            .await
            .unwrap()
            .unwrap(),
        valid
    );
    let mut invalid = entry(owner, "failed", "file", 99);
    invalid.size_bytes = u64::MAX; // rejected by persistent stores after revision upsert
    assert!(store.register_tree_entry(&invalid, 10, 10).await.is_err());
    assert!(
        store
            .revision(&RepoKey::new("generic", owner, "repo"), "failed")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .tree_entry(&TreeKey::new("generic", owner, "repo", "failed"), "file")
            .await
            .unwrap()
            .is_none()
    );
}

async fn concurrent_register_delete<S: TreeStore>(store: &S, owner: &str)
where
    S::Error: std::fmt::Debug,
{
    let repo = RepoKey::new("generic", owner, "repo");
    for i in 0..100 {
        let rev = format!("race{i}");
        let e = entry(owner, &rev, "file", 100);
        let (registered, deleted) = tokio::join!(
            store.register_tree_entry(&e, 1000, 1000),
            store.delete_revision(&repo, &rev),
        );
        assert!(matches!(
            registered.unwrap(),
            TreeRegistrationOutcome::Registered(_)
        ));
        deleted.unwrap();
        assert!(
            store.revision(&repo, &rev).await.unwrap().is_some()
                || store
                    .tree_entry(&TreeKey::new("generic", owner, "repo", &rev), "file")
                    .await
                    .unwrap()
                    .is_none()
        );
    }
}

async fn concurrent_registration_capacity<S: TreeStore>(store: &S, owner: &str)
where
    S::Error: std::fmt::Debug,
{
    let first = entry(owner, "first", "file", 100);
    let second = entry(owner, "second", "file", 100);
    let (a, b) = tokio::join!(
        store.register_tree_entry(&first, 1, 1),
        store.register_tree_entry(&second, 1, 1)
    );
    let results = [a.unwrap(), b.unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, TreeRegistrationOutcome::Registered(_)))
            .count(),
        1
    );
    let repo = RepoKey::new("generic", owner, "repo");
    assert_eq!(store.count_revisions(&repo).await.unwrap(), 1);
    assert_eq!(store.count_tree_entries(&repo).await.unwrap(), 1);
}

async fn concurrent_register_prune<S: TreeStore>(store: &S, owner: &str)
where
    S::Error: std::fmt::Debug,
{
    let repo = RepoKey::new("generic", owner, "repo");
    for i in 0..100 {
        let rev = format!("race{i}");
        let e = entry(owner, &rev, "file", 100);
        let (registered, pruned) = tokio::join!(
            store.register_tree_entry(&e, 1000, 1000),
            store.prune_revisions_over_cap(&repo, 0)
        );
        assert!(matches!(
            registered.unwrap(),
            TreeRegistrationOutcome::Registered(_)
        ));
        pruned.unwrap();
        assert!(
            store.revision(&repo, &rev).await.unwrap().is_some()
                || store
                    .tree_entry(&TreeKey::new("generic", owner, "repo", &rev), "file")
                    .await
                    .unwrap()
                    .is_none()
        );
    }
}

async fn concurrent_revision_creation<S: TreeStore>(store: &S, owner: &str)
where
    S::Error: std::fmt::Debug,
{
    let outcomes = futures_util::future::join_all((0..16).map(|i| async move {
        let rev = RevisionRecord {
            provider: "generic".to_owned(),
            owner: owner.to_owned(),
            repo: "repo".to_owned(),
            revision: format!("create{i}"),
            created_at_unix_seconds: 10,
            updated_at_unix_seconds: 20,
        };
        (
            rev.clone(),
            store.create_revision_bounded(&rev, 3).await.unwrap(),
        )
    }))
    .await;
    assert_eq!(
        outcomes
            .iter()
            .filter(|(_, o)| *o == RevisionCreationOutcome::Created)
            .count(),
        3
    );
    let repo = RepoKey::new("generic", owner, "repo");
    assert_eq!(store.count_revisions(&repo).await.unwrap(), 3);
    for (rev, outcome) in outcomes {
        match outcome {
            RevisionCreationOutcome::Created => {
                let mut conflict = rev.clone();
                conflict.created_at_unix_seconds = 999;
                conflict.updated_at_unix_seconds = 999;
                assert_eq!(
                    store.create_revision_bounded(&conflict, 0).await.unwrap(),
                    RevisionCreationOutcome::AlreadyExists
                );
                assert_eq!(
                    store.revision(&repo, &rev.revision).await.unwrap().unwrap(),
                    rev
                );
            }
            RevisionCreationOutcome::LimitExceeded => assert!(
                store
                    .revision(&repo, &rev.revision)
                    .await
                    .unwrap()
                    .is_none()
            ),
            RevisionCreationOutcome::AlreadyExists => {
                assert_ne!(
                    outcome,
                    RevisionCreationOutcome::AlreadyExists,
                    "distinct fresh revisions must not conflict"
                )
            }
        }
    }
}

async fn mixed_creation_registration_capacity<S: TreeStore>(store: &S, owner: &str)
where
    S::Error: std::fmt::Debug,
{
    let e = entry(owner, "registered", "file", 100);
    let rev = RevisionRecord {
        provider: e.provider.clone(),
        owner: e.owner.clone(),
        repo: e.repo.clone(),
        revision: "created".to_owned(),
        created_at_unix_seconds: 10,
        updated_at_unix_seconds: 20,
    };
    let (created, registered) = tokio::join!(
        store.create_revision_bounded(&rev, 1),
        store.register_tree_entry(&e, 1, 10)
    );
    let created = created.unwrap();
    let registered = registered.unwrap();
    assert!(matches!(
        (created, registered),
        (
            RevisionCreationOutcome::Created,
            TreeRegistrationOutcome::LimitExceeded
        ) | (
            RevisionCreationOutcome::LimitExceeded,
            TreeRegistrationOutcome::Registered(_)
        )
    ));
    assert_eq!(
        store
            .count_revisions(&RepoKey::new("generic", owner, "repo"))
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn memory_tree_registration_transaction_contract() {
    let store = crate::MemoryIndexStore::new();
    tree_scan_contract(&store, "memory-scan-contract").await;
    revision_listing_contract(&store, "memory-listing-contract").await;
    revision_pruning_contract(&store, "memory-pruning-contract").await;
    transactional_registration(&store, "memory").await;
    concurrent_register_delete(&store, "memory-race").await;
    concurrent_register_prune(&store, "memory-prune").await;
    concurrent_registration_capacity(&store, "memory-cap").await;
    concurrent_revision_creation(&store, "memory-creation").await;
    mixed_creation_registration_capacity(&store, "memory-mixed").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_tree_registration_transaction_contract() {
    let tmp = tempfile::tempdir().unwrap();
    let store = crate::LocalIndexStore::open(tmp.path().to_path_buf());
    tree_scan_contract(&store, "sqlite-scan-contract").await;
    revision_listing_contract(&store, "sqlite-listing-contract").await;
    revision_pruning_contract(&store, "sqlite-pruning-contract").await;
    transactional_registration(&store, "sqlite").await;
    registration_rollback(&store, "sqlite-rollback").await;
    concurrent_register_delete(&store, "sqlite-race").await;
    concurrent_register_prune(&store, "sqlite-prune").await;
    concurrent_registration_capacity(&store, "sqlite-cap").await;
    concurrent_revision_creation(&store, "sqlite-creation").await;
    mixed_creation_registration_capacity(&store, "sqlite-mixed").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_tree_registration_transaction_contract() {
    let Some(database_url) = std::env::var("SHARDLINE_TREE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    let store = crate::PostgresIndexStore::new(pool.clone());
    let owner = format!("registration-{}", std::process::id());
    tree_scan_contract(&store, &format!("{owner}-scan-contract")).await;
    revision_listing_contract(&store, &format!("{owner}-listing-contract")).await;
    revision_pruning_contract(&store, &format!("{owner}-pruning-contract")).await;
    transactional_registration(&store, &owner).await;
    registration_rollback(&store, &format!("{owner}-rollback")).await;
    concurrent_register_delete(&store, &format!("{owner}-race")).await;
    concurrent_register_prune(&store, &format!("{owner}-prune")).await;
    concurrent_registration_capacity(&store, &format!("{owner}-cap")).await;
    concurrent_revision_creation(&store, &format!("{owner}-creation")).await;
    mixed_creation_registration_capacity(&store, &format!("{owner}-mixed")).await;
    for cleanup_owner in [
        &owner,
        &format!("{owner}-rollback"),
        &format!("{owner}-race"),
        &format!("{owner}-prune"),
        &format!("{owner}-cap"),
        &format!("{owner}-creation"),
        &format!("{owner}-mixed"),
    ] {
        sqlx::query("DELETE FROM shardline_tree_entries WHERE provider='generic' AND owner=$1 AND repo='repo'").bind(cleanup_owner).execute(&pool).await.unwrap();
        sqlx::query(
            "DELETE FROM shardline_revisions WHERE provider='generic' AND owner=$1 AND repo='repo'",
        )
        .bind(cleanup_owner)
        .execute(&pool)
        .await
        .unwrap();
    }
    pool.close().await;
}
