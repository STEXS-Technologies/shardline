use shardline_index::RecordStore;
use shardline_storage::ObjectPrefix;

use crate::{
    ServerError,
    chunk_store::chunk_hash_from_chunk_object_key_if_present,
    model::ServerStatsResponse,
    overflow::{checked_add, checked_increment},
};

impl super::PostgresBackend {
    /// Returns backend storage stats.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when metadata inventory cannot be loaded.
    pub async fn stats(&self) -> Result<ServerStatsResponse, ServerError> {
        let object_store = self.object_store();
        let prefix = ObjectPrefix::parse("").map_err(|_error| ServerError::InvalidContentHash)?;
        let mut chunks = 0_u64;
        let mut chunk_bytes = 0_u64;
        crate::object_store::visit_object_prefix(&object_store, &prefix, |metadata| {
            let is_chunk = chunk_hash_from_chunk_object_key_if_present(metadata.key())?.is_some();
            if is_chunk {
                chunks = checked_increment(chunks)?;
                chunk_bytes = checked_add(chunk_bytes, metadata.length())?;
            }

            Ok(())
        })?;
        let mut files = 0_u64;
        RecordStore::visit_latest_record_locators(&self.record_store, |_locator| {
            files = checked_increment(files)?;
            Ok::<(), ServerError>(())
        })
        .await?;

        Ok(ServerStatsResponse {
            chunks,
            chunk_bytes,
            files,
        })
    }
}
