//! Pause the real background upgrade at its SQL write while another node rotates.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use shardline_hub_api::secrets::{WebhookSecretCipher, upgrade_webhook_secrets_with_report};
use shardline_index::{
    PostgresIndexStore,
    hub::{BoxedHubStore, HubRepoType},
};
use shardline_protocol::SecretBytes;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_background_upgrade_preserves_rotation_while_its_update_waits() {
    let Some(url) = shardline_test_support::postgres_fixture_url("DATABASE_URL") else {
        return;
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("owned migrated secret-upgrade fixture");
    let store = BoxedHubStore::from_store(PostgresIndexStore::new(pool.clone()));
    let repo = format!(
        "upgrade-race/{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    store.create_repo(HubRepoType::Model, &repo, false).unwrap();
    let hook = store
        .create_webhook(
            &repo,
            "https://example.com/hook",
            &["push".to_owned()],
            Some("observed-plaintext"),
        )
        .unwrap();
    let cipher = WebhookSecretCipher::new(SecretBytes::new(vec![37; 32])).unwrap();
    let mut rotation = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM shardline_hub_webhooks WHERE repo_id=$1 AND id=$2 FOR UPDATE")
        .bind(&repo)
        .bind(&hook.id)
        .fetch_one(&mut *rotation)
        .await
        .unwrap();
    let upgrading_store = store.clone();
    let upgrade = tokio::task::spawn_blocking(move || {
        upgrade_webhook_secrets_with_report(&upgrading_store, &cipher)
    });
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock'
                 AND query LIKE 'UPDATE shardline_hub_webhooks SET secret=$1%' AND pid<>pg_backend_pid())",
            ).fetch_one(&pool).await.unwrap();
            if blocked { break; }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("upgrade must read the old secret and wait at its real conditional UPDATE");
    sqlx::query("UPDATE shardline_hub_webhooks SET secret=$1 WHERE repo_id=$2 AND id=$3")
        .bind("rotated-during-upgrade")
        .bind(&repo)
        .bind(&hook.id)
        .execute(&mut *rotation)
        .await
        .unwrap();
    rotation.commit().await.unwrap();
    let report = tokio::time::timeout(Duration::from_secs(20), upgrade)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.inspected, 1);
    assert_eq!(report.upgraded, 0);
    assert_eq!(report.failures, 0);
    let current = store.list_webhooks(&repo).unwrap().remove(0);
    assert_eq!(
        current.secret.as_ref().unwrap().expose_secret(),
        "rotated-during-upgrade"
    );
    store.delete_webhook(&repo, &hook.id).unwrap();
    store.delete_repo(&repo).unwrap();
    pool.close().await;
}
