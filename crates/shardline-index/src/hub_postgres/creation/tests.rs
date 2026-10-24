#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::hub::{HubCreationOutcome, HubStore};

fn actor() -> HubCreationActor {
    HubCreationActor {
        issuer: "issuer".to_owned(),
        subject: "alice".to_owned(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_policy_preserves_retries_limits_and_creator_revocation() {
    let Some(pool) = crate::postgres::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool.clone());
    store
        .create_repo(HubRepoType::Model, "legacy/old", true)
        .unwrap();
    let mut policy = HubCreationPolicy {
        max_repositories: Some(1),
        max_creations: Some(1),
        ..HubCreationPolicy::default()
    };
    store
        .set_hub_creation_policy("alice", Some(&policy))
        .unwrap();
    assert!(
        store
            .create_repo(HubRepoType::Model, "alice/old-writer", false)
            .is_err()
    );
    assert!(matches!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/one", true, Some(&actor()))
            .unwrap(),
        HubCreationOutcome::Created(_)
    ));
    assert!(
        store
            .create_repo_authorized(HubRepoType::Dataset, "alice/one", false, Some(&actor()))
            .is_err()
    );
    assert!(store.get_repo("alice/one").unwrap().unwrap().private);
    assert!(matches!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/two", false, Some(&actor()))
            .unwrap(),
        HubCreationOutcome::QuotaExceeded
    ));
    policy.max_repositories = Some(10);
    store
        .set_hub_creation_policy("alice", Some(&policy))
        .unwrap();
    assert!(matches!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/two", false, Some(&actor()))
            .unwrap(),
        HubCreationOutcome::RateLimited
    ));
    policy.denied_creators.push(actor());
    store
        .set_hub_creation_policy("alice", Some(&policy))
        .unwrap();
    assert_eq!(
        store.hub_creation_policy("alice").unwrap(),
        Some(policy.clone())
    );
    assert!(matches!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/one", false, Some(&actor()))
            .unwrap(),
        HubCreationOutcome::Denied
    ));
    assert!(store.get_repo("alice/two").unwrap().is_none());
    let events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM shardline_hub_creation_events WHERE namespace='alice'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(events, 1);
    policy.denied_creators.clear();
    store
        .set_hub_creation_policy("alice", Some(&policy))
        .unwrap();
    store.delete_repo("alice/one").unwrap();
    assert!(store.get_repo("alice/one").unwrap().is_none());
    assert!(matches!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/two", false, Some(&actor()))
            .unwrap(),
        HubCreationOutcome::RateLimited
    ));
    store.set_hub_creation_policy("alice", None).unwrap();
    assert!(store.hub_creation_policy("alice").unwrap().is_none());
    store
        .create_repo(HubRepoType::Model, "alice/two", true)
        .unwrap();
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_connections_cannot_exceed_namespace_capacity() {
    let Some(pool) = crate::postgres::connect_isolated_postgres().await else {
        return;
    };
    PostgresIndexStore::new(pool.clone())
        .set_hub_creation_policy(
            "alice",
            Some(&HubCreationPolicy {
                max_repositories: Some(3),
                ..HubCreationPolicy::default()
            }),
        )
        .unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(12));
    let mut workers = Vec::new();
    for number in 0..12 {
        let store = PostgresIndexStore::new(pool.clone());
        let barrier = std::sync::Arc::clone(&barrier);
        workers.push(tokio::task::spawn_blocking(move || {
            barrier.wait();
            store
                .create_repo_authorized(
                    HubRepoType::Model,
                    &format!("alice/repo-{number}"),
                    true,
                    Some(&actor()),
                )
                .unwrap()
        }));
    }
    let mut created = 0_usize;
    for worker in workers {
        let outcome = worker.await.unwrap();
        assert!(matches!(
            outcome,
            HubCreationOutcome::Created(_) | HubCreationOutcome::QuotaExceeded
        ));
        if matches!(outcome, HubCreationOutcome::Created(_)) {
            created = created.checked_add(1).unwrap();
        }
    }
    assert_eq!(created, 3);
    assert_eq!(
        PostgresIndexStore::new(pool.clone())
            .list_repos()
            .unwrap()
            .len(),
        3
    );
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeatable_read_cannot_miss_a_newly_published_policy() {
    let Some(pool) = crate::postgres::connect_isolated_postgres().await else {
        return;
    };
    let mut stale = pool.begin().await.unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *stale)
        .await
        .unwrap();
    let _: i64 = sqlx::query_scalar("SELECT count(*) FROM shardline_hub_repos")
        .fetch_one(&mut *stale)
        .await
        .unwrap();
    PostgresIndexStore::new(pool.clone())
        .set_hub_creation_policy(
            "alice",
            Some(&HubCreationPolicy {
                enabled: false,
                ..HubCreationPolicy::default()
            }),
        )
        .unwrap();
    let error = sqlx::query("INSERT INTO shardline_hub_repos(repo_id,repo_type,private,default_branch,created_at_unix_seconds,updated_at_unix_seconds,creation_authorized) VALUES ('alice/stale','model',true,'main',0,0,true)")
        .execute(&mut *stale).await.unwrap_err();
    assert_eq!(
        error.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("40001")
    );
    stale.rollback().await.unwrap();
    assert!(
        PostgresIndexStore::new(pool.clone())
            .get_repo("alice/stale")
            .unwrap()
            .is_none()
    );
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_creator_waiting_on_publication_observes_revocation() {
    let Some(pool) = crate::postgres::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool.clone());
    store
        .set_hub_creation_policy("alice", Some(&HubCreationPolicy::default()))
        .unwrap();
    let mut owner = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(1397506896,hashtext('alice'))")
        .execute(&mut *owner)
        .await
        .unwrap();
    sqlx::query("UPDATE shardline_hub_creation_policies SET enabled=false WHERE namespace='alice'")
        .execute(&mut *owner)
        .await
        .unwrap();
    let (started, ready) = tokio::sync::oneshot::channel();
    let worker_pool = pool.clone();
    let worker = tokio::spawn(async move {
        let mut transaction = worker_pool.begin().await.unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
        started.send(pid).unwrap();
        let result = sqlx::query("INSERT INTO shardline_hub_repos(repo_id,repo_type,private,default_branch,created_at_unix_seconds,updated_at_unix_seconds,creation_authorized) VALUES ('alice/waiting','model',true,'main',0,0,true)")
            .execute(&mut *transaction).await;
        transaction.rollback().await.unwrap();
        result
    });
    let pid = ready.await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT coalesce(wait_event='advisory',false) FROM pg_stat_activity WHERE pid=$1",
            )
            .bind(pid)
            .fetch_one(&pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("creator must actually wait for the publication lock");
    owner.commit().await.unwrap();
    let error = worker.await.unwrap().unwrap_err();
    assert_eq!(
        error.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("SL403")
    );
    assert!(store.get_repo("alice/waiting").unwrap().is_none());
    let events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM shardline_hub_creation_events WHERE namespace='alice'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(events, 0);
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reference_publication_does_not_spend_creation_rate() {
    let Some(pool) = crate::postgres::connect_isolated_postgres().await else {
        return;
    };
    let store = PostgresIndexStore::new(pool.clone());
    store
        .set_hub_creation_policy(
            "alice",
            Some(&HubCreationPolicy {
                max_creations: Some(1),
                ..HubCreationPolicy::default()
            }),
        )
        .unwrap();
    sqlx::raw_sql("CREATE FUNCTION reject_initial_ref() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture failure'; END; $$; CREATE TRIGGER reject_initial_ref BEFORE INSERT ON shardline_hub_refs FOR EACH ROW EXECUTE FUNCTION reject_initial_ref();")
        .execute(&pool).await.unwrap();
    assert!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/retry", true, Some(&actor()))
            .is_err()
    );
    assert!(store.get_repo("alice/retry").unwrap().is_none());
    let events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM shardline_hub_creation_events WHERE namespace='alice'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(events, 0);
    sqlx::query("DROP TRIGGER reject_initial_ref ON shardline_hub_refs")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store
            .create_repo_authorized(HubRepoType::Model, "alice/retry", true, Some(&actor()))
            .unwrap(),
        HubCreationOutcome::Created(_)
    ));
    pool.close().await;
}
