use std::{future::Future, sync::Arc, time::Duration};

use ed25519_dalek::SigningKey;
use sqlx::PgPool;

use super::{TaskEvidenceStore, TaskRunIdentity, resolved_postgres_deployment_fingerprint};
use crate::ServerConfig;

struct Observer {
    store: TaskEvidenceStore,
    identity: TaskRunIdentity,
    key: SigningKey,
}

tokio::task_local! {
    static OBSERVER: Arc<Observer>;
}

impl TaskEvidenceStore {
    /// Observes metadata scopes reported by maintenance inside this future.
    /// Bindings use the original run key and manifest. This adds no connection,
    /// process or execution authority. Spawned tasks do not inherit the context.
    /// Publication failures warn without changing the work's result or retrying.
    pub async fn observe_history_scopes<T>(
        &self,
        identity: TaskRunIdentity,
        key: SigningKey,
        work: impl Future<Output = T>,
    ) -> T {
        OBSERVER
            .scope(
                Arc::new(Observer {
                    store: self.clone(),
                    identity,
                    key,
                }),
                work,
            )
            .await
    }
}

pub(crate) async fn observe_work_pool(config: &ServerConfig, pool: &PgPool) {
    let Ok(observer) = OBSERVER.try_with(Arc::clone) else {
        return;
    };
    // This is the existing work pool, whose single session already holds the
    // maintenance barrier. Never open another connection to infer its target.
    let scope = tokio::time::timeout(Duration::from_secs(5), async {
        let mut connection = pool
            .acquire()
            .await
            .map_err(shardline_index::PostgresMetadataStoreError::from)?;
        resolved_postgres_deployment_fingerprint(config, &mut connection).await
    })
    .await;
    let scope = match scope {
        Ok(Ok(scope)) => scope,
        Ok(Err(error)) => {
            tracing::warn!(%error, "maintenance scope unavailable; legacy history continues");
            return;
        }
        Err(error) => {
            tracing::warn!(%error, "maintenance scope timed out; legacy history continues");
            return;
        }
    };
    match tokio::task::spawn_blocking(move || {
        observer
            .store
            .publish_history_scope_binding(&observer.identity, &observer.key, scope)
    })
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(%error, "maintenance scope binding unavailable; legacy history continues")
        }
        Err(error) => {
            tracing::warn!(%error, "maintenance scope publication unavailable; legacy history continues")
        }
    }
}
