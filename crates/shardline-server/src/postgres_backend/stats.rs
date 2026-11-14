use crate::{ServerError, model::ServerStatsResponse};

impl super::PostgresBackend {
    /// Returns backend storage stats.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when metadata inventory cannot be loaded.
    pub async fn stats(&self) -> Result<ServerStatsResponse, ServerError> {
        crate::storage_stats::storage_stats(&self.object_store(), &self.record_store).await
    }
}
