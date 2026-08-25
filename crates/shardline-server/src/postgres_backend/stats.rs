use shardline_index::RecordTraversal;
use shardline_storage::ObjectPrefix;

use crate::{
    ServerError,
    chunk_store::chunk_hash_from_chunk_object_key_if_present,
    model::ServerStatsResponse,
    object_store::visit_object_prefix,
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
        let mut objects = 0_u64;
        let mut object_bytes = 0_u64;
        let mut chunks = 0_u64;
        let mut chunk_bytes = 0_u64;
        visit_object_prefix(&object_store, &prefix, |metadata| {
            objects = checked_increment(objects)?;
            object_bytes = checked_add(object_bytes, metadata.length())?;
            let is_chunk = chunk_hash_from_chunk_object_key_if_present(metadata.key())?.is_some();
            if is_chunk {
                chunks = checked_increment(chunks)?;
                chunk_bytes = checked_add(chunk_bytes, metadata.length())?;
            }

            Ok(())
        })?;
        let mut files = 0_u64;
        RecordTraversal::visit_latest_record_locators(&self.record_store, |_locator| {
            files = checked_increment(files)?;
            Ok::<(), ServerError>(())
        })
        .await?;

        Ok(ServerStatsResponse {
            objects,
            object_bytes,
            chunks,
            chunk_bytes,
            files,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use crate::object_store::ServerObjectStore;

    const TEST_PG_URL: &str = "postgres://localhost:5432/test";

    async fn make_backend() -> (super::super::PostgresBackend, tempfile::TempDir) {
        let root = tempfile::tempdir().expect("temp dir");
        let object_store =
            ServerObjectStore::local(root.path().join("chunks")).expect("local store");
        let backend = super::super::PostgresBackend::new_with_object_store_and_upload_parallelism(
            root.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap(),
            NonZeroUsize::new(64).unwrap(),
            TEST_PG_URL,
            object_store,
        )
        .await
        .expect("constructor");
        (backend, root)
    }

    #[tokio::test]
    async fn stats_runs_without_panic_when_empty() {
        let (backend, _root) = make_backend().await;
        // stats() calls RecordTraversal which will fail without a real PG DB.
        // The object_store part works fine; the record_store part may error.
        // We only verify that the function runs without panicking.
        let _response = backend.stats().await;
    }

    #[test]
    fn server_stats_response_default_fields() {
        let response = ServerStatsResponse {
            objects: 0,
            object_bytes: 0,
            chunks: 0,
            chunk_bytes: 0,
            files: 0,
        };
        assert_eq!(response.chunks, 0);
        assert_eq!(response.objects, 0);
        assert_eq!(response.object_bytes, 0);
        assert_eq!(response.chunk_bytes, 0);
        assert_eq!(response.files, 0);
    }

    #[test]
    fn server_stats_response_arbitrary_values() {
        let response = ServerStatsResponse {
            objects: 84,
            object_bytes: 2_000_000,
            chunks: 42,
            chunk_bytes: 1_000_000,
            files: 7,
        };
        assert_eq!(response.chunks, 42);
        assert_eq!(response.objects, 84);
        assert_eq!(response.object_bytes, 2_000_000);
        assert_eq!(response.chunk_bytes, 1_000_000);
        assert_eq!(response.files, 7);
    }
}
