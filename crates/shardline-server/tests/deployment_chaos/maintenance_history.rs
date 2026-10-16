//! Inspect automatic metadata history without blocking async runtime workers.

use penelope::core::task::Digest;
use shardline_server::{
    ServerConfig, ServerError,
    task_tracking::{TaskEvidenceStore, TaskRunSummary},
};
use sqlx::PgPool;

pub(super) struct MaintenanceHistory {
    store: TaskEvidenceStore,
    scope: Digest,
}

impl MaintenanceHistory {
    pub(super) async fn from_postgres_pool(config: &ServerConfig, pool: &PgPool) -> Self {
        let (store, scope) = TaskEvidenceStore::from_postgres_pool(config, pool)
            .await
            .unwrap();
        Self {
            store,
            scope: scope.expect("owned upgraded fixture must expose metadata history"),
        }
    }

    pub(super) fn summaries(
        &self,
        deployment: Digest,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TaskRunSummary>, ServerError> {
        tokio::task::block_in_place(|| {
            self.store
                .summaries_with_scope(deployment, Some(self.scope), after, limit)
        })
    }

    pub(super) fn read(&self, key: &str, maximum: u64) -> Result<Option<Vec<u8>>, ServerError> {
        tokio::task::block_in_place(|| self.store.read(key, maximum))
    }
}
