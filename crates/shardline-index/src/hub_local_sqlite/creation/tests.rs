#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::hub::{HubCreationOutcome, HubStore};

fn actor() -> HubCreationActor {
    HubCreationActor {
        issuer: "issuer".to_owned(),
        subject: "alice".to_owned(),
    }
}

#[test]
fn shared_policy_preserves_defaults_retries_visibility_revocation_and_rate_history() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalIndexStore::new(root.path().to_owned()).unwrap();
    store
        .create_repo(HubRepoType::Model, "legacy/old", true)
        .unwrap();
    assert!(store.hub_creation_policy("legacy").unwrap().is_none());
    let mut policy = HubCreationPolicy {
        max_repositories: Some(2),
        max_creations: Some(2),
        ..HubCreationPolicy::default()
    };
    store
        .set_hub_creation_policy("alice", Some(&policy))
        .unwrap();
    assert_eq!(
        store.hub_creation_policy("alice").unwrap(),
        Some(policy.clone())
    );
    assert!(
        store
            .create_repo(HubRepoType::Model, "alice/old-writer", false)
            .is_err()
    );
    let creator = actor();
    for name in ["alice/one", "alice/two"] {
        assert!(matches!(
            store
                .create_repo_authorized(HubRepoType::Model, name, true, Some(&creator))
                .unwrap(),
            HubCreationOutcome::Created(_)
        ));
    }
    assert!(
        store
            .create_repo_authorized(HubRepoType::Dataset, "alice/one", false, Some(&creator))
            .is_err()
    );
    assert!(store.get_repo("alice/one").unwrap().unwrap().private);
    assert!(matches!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/three", false, Some(&creator))
            .unwrap(),
        HubCreationOutcome::QuotaExceeded
    ));
    policy.max_repositories = Some(100);
    store
        .set_hub_creation_policy("alice", Some(&policy))
        .unwrap();
    assert!(matches!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/three", false, Some(&creator))
            .unwrap(),
        HubCreationOutcome::RateLimited
    ));
    policy.denied_creators.push(creator.clone());
    store
        .set_hub_creation_policy("alice", Some(&policy))
        .unwrap();
    assert!(matches!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/one", false, Some(&creator))
            .unwrap(),
        HubCreationOutcome::Denied
    ));
    assert!(store.get_repo("alice/three").unwrap().is_none());
    let conn = open_hub_connection_rw(root.path()).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM shardline_hub_creation_events WHERE namespace='alice'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);
    let identity:(String,String) = conn.query_row("SELECT creation_issuer,creation_subject FROM shardline_hub_repos WHERE repo_id='alice/one'",[],|row|Ok((row.get(0)?,row.get(1)?))).unwrap();
    assert_eq!(identity, (creator.issuer, creator.subject));
    policy.denied_creators.clear();
    store
        .set_hub_creation_policy("alice", Some(&policy))
        .unwrap();
    store.delete_repo("alice/one").unwrap();
    assert!(store.get_repo("alice/one").unwrap().is_none());
    assert!(matches!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/three", false, Some(&actor()))
            .unwrap(),
        HubCreationOutcome::RateLimited
    ));
    store.set_hub_creation_policy("alice", None).unwrap();
    assert!(store.hub_creation_policy("alice").unwrap().is_none());
    store
        .create_repo(HubRepoType::Model, "alice/three", false)
        .unwrap();
}

#[test]
fn competing_connections_cannot_exceed_namespace_capacity() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalIndexStore::new(root.path().to_owned()).unwrap();
    store
        .set_hub_creation_policy(
            "alice",
            Some(&HubCreationPolicy {
                max_repositories: Some(3),
                ..HubCreationPolicy::default()
            }),
        )
        .unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(12));
    let workers: Vec<_> = (0..12)
        .map(|number| {
            let root = root.path().to_owned();
            let barrier = std::sync::Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                LocalIndexStore::open(root)
                    .create_repo_authorized(
                        HubRepoType::Model,
                        &format!("alice/repo-{number}"),
                        true,
                        Some(&actor()),
                    )
                    .unwrap()
            })
        })
        .collect();
    let mut created = 0_usize;
    let mut limited = 0_usize;
    for worker in workers {
        match worker.join().unwrap() {
            HubCreationOutcome::Created(_) => created = created.checked_add(1).unwrap(),
            HubCreationOutcome::QuotaExceeded => limited = limited.checked_add(1).unwrap(),
            outcome @ (HubCreationOutcome::Denied | HubCreationOutcome::RateLimited) => {
                assert!(
                    matches!(outcome, HubCreationOutcome::QuotaExceeded),
                    "unexpected creation outcome: {outcome:?}"
                );
            }
        }
    }
    assert_eq!((created, limited), (3, 9));
    assert_eq!(store.list_repos().unwrap().len(), 3);
}

#[test]
fn a_waiting_creator_observes_revocation_and_failed_publication_spends_no_rate() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalIndexStore::new(root.path().to_owned()).unwrap();
    store
        .set_hub_creation_policy(
            "alice",
            Some(&HubCreationPolicy {
                max_creations: Some(1),
                ..HubCreationPolicy::default()
            }),
        )
        .unwrap();
    let mut conn = open_hub_connection_rw(root.path()).unwrap();
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    tx.execute(
        "UPDATE shardline_hub_creation_policies SET enabled=0 WHERE namespace='alice'",
        [],
    )
    .unwrap();
    let path = root.path().to_owned();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        LocalIndexStore::open(path)
            .create_repo_authorized(HubRepoType::Model, "alice/waiting", true, Some(&actor()))
            .unwrap()
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    tx.commit().unwrap();
    assert!(matches!(worker.join().unwrap(), HubCreationOutcome::Denied));
    assert!(store.get_repo("alice/waiting").unwrap().is_none());
    store
        .set_hub_creation_policy(
            "alice",
            Some(&HubCreationPolicy {
                max_creations: Some(1),
                ..HubCreationPolicy::default()
            }),
        )
        .unwrap();
    conn.execute_batch("CREATE TRIGGER reject_initial_ref BEFORE INSERT ON shardline_hub_refs BEGIN SELECT RAISE(ABORT,'fixture failure'); END;").unwrap();
    assert!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/retry", true, Some(&actor()))
            .is_err()
    );
    assert!(store.get_repo("alice/retry").unwrap().is_none());
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM shardline_hub_creation_events WHERE namespace='alice'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
    conn.execute_batch("DROP TRIGGER reject_initial_ref")
        .unwrap();
    assert!(matches!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/retry", true, Some(&actor()))
            .unwrap(),
        HubCreationOutcome::Created(_)
    ));
    conn.execute("UPDATE shardline_hub_creation_events SET created_at_unix_seconds=0 WHERE namespace='alice'",[]).unwrap();
    assert!(matches!(
        store
            .create_repo_authorized(
                HubRepoType::Model,
                "alice/after-window",
                true,
                Some(&actor())
            )
            .unwrap(),
        HubCreationOutcome::Created(_)
    ));
}
