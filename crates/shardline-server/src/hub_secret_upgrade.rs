//! Existing embedded Hub upgrade, bound to its actual work session.

use std::time::Duration;

use penelope_core::task::Digest;
use shardline_hub_api::secrets::{WebhookSecretCipher, WebhookSecretUpgradeReport};
use shardline_index::{PostgresIndexStore, hub::BoxedHubStore};
use sqlx::PgPool;

use crate::{ServerConfig, ServerError, task_tracking::EmbeddedTaskTracker};

async fn prepare_postgres(
    config: &ServerConfig,
    url: &str,
) -> Result<(PgPool, BoxedHubStore, Option<Digest>), ServerError> {
    let pool = crate::maintenance_barrier::postgres_nonreplaceable_work_pool(url)?;
    let mut connection = pool
        .acquire()
        .await
        .map_err(shardline_index::PostgresMetadataStoreError::from)?;
    let scope = match tokio::time::timeout(
        Duration::from_secs(5),
        crate::task_tracking::resolved_postgres_deployment_fingerprint(config, &mut connection),
    )
    .await
    {
        Ok(Ok(scope)) => Some(scope),
        Ok(Err(error)) => {
            tracing::warn!(%error, "Hub work scope unavailable; legacy history continues");
            None
        }
        Err(error) => {
            tracing::warn!(%error, "Hub work scope timed out; legacy history continues");
            None
        }
    };
    drop(connection);
    let store = BoxedHubStore::from_store(PostgresIndexStore::new(pool.clone()));
    Ok((pool, store, scope))
}

/// Called on the server's existing blocking pool. The work store retains its
/// original session until the actual sweep finishes, including caller aborts.
pub(crate) fn run(
    config: &ServerConfig,
    local_store: &BoxedHubStore,
    cipher: &WebhookSecretCipher,
    tracker: &EmbeddedTaskTracker,
) -> WebhookSecretUpgradeReport {
    let tracking = tracker.clone().without_history_scope().begin(
        "hub-webhook-secret-upgrade",
        b"hub-webhook-secret-upgrade.v1",
        shardline_reliability::MaintenanceArtifactKind::LifecycleRepair,
    );
    if let Err(error) = &tracking {
        crate::task_tracking::embedded::tracking_error(error.as_ref());
    }
    tracing::info!("starting background webhook-secret upgrade sweep");
    let runtime = tokio::runtime::Handle::current();
    let report = config.index_postgres_url().map_or_else(
        || shardline_hub_api::secrets::upgrade_webhook_secrets_with_report(local_store, cipher),
        |url| match runtime.block_on(prepare_postgres(config, url)) {
            Ok((pool, store, scope)) => {
                if let (Ok(receipt), Some(scope)) = (&tracking, scope)
                    && let Err(error) = receipt.bind_history_scope(scope)
                {
                    crate::task_tracking::embedded::tracking_error(error.as_ref());
                }
                let report =
                    shardline_hub_api::secrets::upgrade_webhook_secrets_with_report_fail_fast(
                        &store, cipher,
                    );
                runtime.block_on(pool.close());
                report
            }
            Err(error) => {
                tracing::warn!(%error, "Hub upgrade work session unavailable; lazy per-row upgrade remains available");
                WebhookSecretUpgradeReport {
                    inspected: 0,
                    upgraded: 0,
                    failures: 1,
                }
            }
        },
    );
    if let Ok(receipt) = tracking
        && let Err(error) = receipt.finish(
            report.failures == 0,
            format!(
                "inspected: {}\nupgraded: {}\nfailures: {}",
                report.inspected, report.upgraded, report.failures
            )
            .as_bytes(),
        )
    {
        crate::task_tracking::embedded::tracking_error(error.as_ref());
    }
    if report.failures == 0 {
        tracing::info!(
            inspected = report.inspected,
            upgraded = report.upgraded,
            "background webhook-secret upgrade sweep completed"
        );
    } else {
        tracing::warn!(
            inspected = report.inspected,
            upgraded = report.upgraded,
            failures = report.failures,
            "background webhook-secret upgrade sweep incomplete; lazy per-row upgrade remains available"
        );
    }
    report
}

#[cfg(test)]
#[path = "hub_secret_upgrade_tests.rs"]
mod tests;
