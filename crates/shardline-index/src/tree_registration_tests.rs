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
