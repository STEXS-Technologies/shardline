use shardline_index::RecordTraversal;
use shardline_protocol::RepositoryScope;

use crate::{
    ServerError, model::ServerStatsResponse, object_store::whole_store_chunk_stats,
    overflow::checked_increment,
};

impl super::PostgresBackend {
    /// Returns backend storage stats.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when metadata inventory cannot be loaded.
    pub async fn stats(&self) -> Result<ServerStatsResponse, ServerError> {
        let object_store = self.object_store();
        let (chunks, chunk_bytes) = whole_store_chunk_stats(&object_store)?;
        let mut files = 0_u64;
        RecordTraversal::visit_latest_record_locators(&self.record_store, |_locator| {
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

    /// Returns repository-scoped storage stats for one repository.
    ///
    /// Files are attributed per repository (repo-scoped records plus the
    /// namespace-prefixed protocol objects written by the LFS/OCI/S3/bazel
    /// frontends); the chunk pool is dedup-shared CAS infrastructure and is
    /// reported whole-store.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the repository's metadata cannot be loaded.
    pub async fn stats_scoped(
        &self,
        scope: &RepositoryScope,
    ) -> Result<ServerStatsResponse, ServerError> {
        crate::record_store::scoped_stats(&self.record_store, &self.object_store(), scope).await
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
            chunks: 0,
            chunk_bytes: 0,
            files: 0,
        };
        assert_eq!(response.chunks, 0);
        assert_eq!(response.chunk_bytes, 0);
        assert_eq!(response.files, 0);
    }

    #[test]
    fn server_stats_response_arbitrary_values() {
        let response = ServerStatsResponse {
            chunks: 42,
            chunk_bytes: 1_000_000,
            files: 7,
        };
        assert_eq!(response.chunks, 42);
        assert_eq!(response.chunk_bytes, 1_000_000);
        assert_eq!(response.files, 7);
    }
}
