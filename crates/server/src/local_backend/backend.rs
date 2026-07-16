use std::{num::NonZeroUsize, path::PathBuf};

use shardline_index::{
    FileChunkRecord, LocalIndexStore, LocalRecordStore, ReconstructionStore, RecordTraversal,
};
use shardline_storage::{ObjectPrefix, ObjectStore};

use crate::{
    ServerError, ServerFrontend,
    config::default_upload_max_in_flight_chunks,
    local_path::ensure_directory_path_components_are_not_symlinked,
    model::ServerStatsResponse,
    object_store::ServerObjectStore,
    overflow::{checked_add, checked_increment},
};

use super::records::read_record;

/// Local filesystem backend for file chunk storage and reconstruction metadata.
#[derive(Debug, Clone)]
pub struct LocalBackend {
    pub(super) public_base_url: String,
    pub(super) chunk_size: NonZeroUsize,
    pub(super) upload_max_in_flight_chunks: NonZeroUsize,
    pub(super) server_frontends: Vec<ServerFrontend>,
    pub(super) index_store: LocalIndexStore,
    pub(super) record_store: LocalRecordStore,
    pub(super) object_store: ServerObjectStore,
}

impl LocalBackend {
    /// Creates a local backend and initializes its directory structure.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the local directories cannot be created.
    pub async fn new(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
    ) -> Result<Self, ServerError> {
        let object_store = ServerObjectStore::local(root.join("chunks"))?;
        Self::new_with_object_store(root, public_base_url, chunk_size, object_store).await
    }

    /// Creates a local backend with explicit upload chunk parallelism.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the local directories cannot be created.
    pub async fn new_with_upload_parallelism(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        upload_max_in_flight_chunks: NonZeroUsize,
    ) -> Result<Self, ServerError> {
        let object_store = ServerObjectStore::local(root.join("chunks"))?;
        Self::new_with_object_store_and_upload_parallelism(
            root,
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
            object_store,
        )
        .await
    }

    pub(crate) async fn new_with_object_store(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        object_store: ServerObjectStore,
    ) -> Result<Self, ServerError> {
        Self::new_with_object_store_and_upload_parallelism_with_frontends(
            root,
            public_base_url,
            chunk_size,
            default_upload_max_in_flight_chunks(),
            object_store,
            &[ServerFrontend::Xet],
        )
        .await
    }

    pub(crate) async fn new_with_object_store_and_upload_parallelism(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        upload_max_in_flight_chunks: NonZeroUsize,
        object_store: ServerObjectStore,
    ) -> Result<Self, ServerError> {
        Self::new_with_object_store_and_upload_parallelism_with_frontends(
            root,
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
            object_store,
            &[ServerFrontend::Xet],
        )
        .await
    }

    pub(crate) async fn new_with_object_store_and_upload_parallelism_with_frontends(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        upload_max_in_flight_chunks: NonZeroUsize,
        object_store: ServerObjectStore,
        server_frontends: &[ServerFrontend],
    ) -> Result<Self, ServerError> {
        ensure_directory_path_components_are_not_symlinked(&root)?;
        let backend = Self {
            index_store: LocalIndexStore::open(root.clone()),
            record_store: LocalRecordStore::open(root),
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
            server_frontends: server_frontends.to_vec(),
            object_store,
        };
        Ok(backend)
    }

    /// Returns the public base URL used in generated download links.
    #[must_use]
    pub fn public_base_url(&self) -> &str {
        &self.public_base_url
    }

    pub(crate) const fn object_backend_name(&self) -> &'static str {
        self.object_store.backend_name()
    }

    /// Verifies that local storage paths remain reachable.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the local object store or metadata roots cannot be
    /// traversed.
    pub async fn ready(&self) -> Result<(), ServerError> {
        let object_store = self.object_store();
        if let Some(local_root) = object_store.local_root() {
            tokio::fs::create_dir_all(local_root).await?;
        } else {
            let probe_key = shardline_storage::ObjectKey::parse("health/probe")
                .map_err(|_error| ServerError::InvalidContentHash)?;
            let _object_store_reachable = object_store.metadata(&probe_key)?;
        }
        let _latest = RecordTraversal::list_latest_record_locators(&self.record_store).await?;
        let _reconstructions =
            ReconstructionStore::list_reconstruction_file_ids(&self.index_store)?;
        Ok(())
    }

    /// Returns local backend storage stats.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when local metadata cannot be traversed.
    pub async fn stats(&self) -> Result<ServerStatsResponse, ServerError> {
        let object_store = self.object_store();
        let prefix = ObjectPrefix::parse("").map_err(|_error| ServerError::InvalidContentHash)?;
        let mut chunks = 0_u64;
        let mut chunk_bytes = 0_u64;
        crate::object_store::visit_object_prefix(&object_store, &prefix, |metadata| {
            let is_chunk =
                crate::chunk_store::chunk_hash_from_chunk_object_key_if_present(metadata.key())?
                    .is_some();
            if is_chunk {
                chunks = checked_increment(chunks)?;
                chunk_bytes = checked_add(chunk_bytes, metadata.length())?;
            }

            Ok(())
        })?;
        let files = u64::try_from(
            RecordTraversal::list_latest_record_locators(&self.record_store)
                .await?
                .len(),
        )?;

        Ok(ServerStatsResponse {
            chunks,
            chunk_bytes,
            files,
        })
    }

    pub(crate) fn object_store(&self) -> ServerObjectStore {
        self.object_store.clone()
    }

    pub(super) async fn read_record(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        repository_scope: Option<&shardline_protocol::RepositoryScope>,
    ) -> Result<shardline_index::FileRecord, ServerError> {
        read_record(&self.record_store, file_id, content_hash, repository_scope).await
    }
}

#[must_use]
pub fn chunk_hash(bytes: &[u8]) -> shardline_protocol::ShardlineHash {
    let digest = blake3::hash(bytes);
    shardline_protocol::ShardlineHash::from_bytes(*digest.as_bytes())
}

pub(crate) fn content_hash(
    total_bytes: u64,
    chunk_size: u64,
    chunks: &[FileChunkRecord],
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&total_bytes.to_le_bytes());
    hasher.update(&chunk_size.to_le_bytes());
    for chunk in chunks {
        hasher.update(chunk.hash.as_bytes());
        hasher.update(&chunk.offset.to_le_bytes());
        hasher.update(&chunk.length.to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use shardline_index::FileChunkRecord;
    use shardline_storage::{ObjectKey, ObjectStore};

    use super::LocalBackend;
    use super::{chunk_hash, content_hash};

    #[test]
    fn chunk_hash_is_deterministic() {
        let data = b"hello world";
        let hash1 = chunk_hash(data);
        let hash2 = chunk_hash(data);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn chunk_hash_differs_for_different_inputs() {
        let hash1 = chunk_hash(b"hello");
        let hash2 = chunk_hash(b"world");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn chunk_hash_returns_fixed_length() {
        let hash = chunk_hash(b"test data");
        assert_eq!(hash.as_bytes().len(), 32);
    }

    #[test]
    fn chunk_hash_empty_input() {
        let hash = chunk_hash(b"");
        // Should still produce a valid hash
        assert_eq!(hash.as_bytes().len(), 32);
    }

    #[test]
    fn content_hash_is_deterministic() {
        let chunks = [FileChunkRecord {
            hash: "abc".to_owned(),
            offset: 0,
            length: 10,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 10,
        }];
        let hash1 = content_hash(10, 10, &chunks);
        let hash2 = content_hash(10, 10, &chunks);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn content_hash_differs_for_different_chunks() {
        let chunks_a = [FileChunkRecord {
            hash: "abc".to_owned(),
            offset: 0,
            length: 10,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 10,
        }];
        let chunks_b = [FileChunkRecord {
            hash: "def".to_owned(),
            offset: 0,
            length: 10,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 10,
        }];
        assert_ne!(
            content_hash(10, 10, &chunks_a),
            content_hash(10, 10, &chunks_b)
        );
    }

    #[test]
    fn content_hash_empty_chunks() {
        let hash = content_hash(0, 0, &[]);
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64); // blake3 hex is 64 chars
    }

    #[test]
    fn content_hash_includes_total_bytes_and_chunk_size() {
        let chunks = [FileChunkRecord {
            hash: "abc".to_owned(),
            offset: 0,
            length: 10,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 10,
        }];
        // Different total_bytes should produce different hashes
        assert_ne!(content_hash(10, 10, &chunks), content_hash(20, 10, &chunks));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_new_creates_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await;
        assert!(backend.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_public_base_url_returns_configured_url() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://localhost:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();
        assert_eq!(backend.public_base_url(), "http://localhost:8080");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_object_backend_name_returns_local() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();
        assert_eq!(backend.object_backend_name(), "local");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_ready_succeeds_with_empty_store() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();
        let result = backend.ready().await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_stats_returns_zero_counts_for_empty_store() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();
        let stats = backend.stats().await.unwrap();
        assert_eq!(stats.chunks, 0);
        assert_eq!(stats.chunk_bytes, 0);
        assert_eq!(stats.files, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_object_store_returns_usable_store() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();
        let store = backend.object_store();
        let key = ObjectKey::parse("test/probe").unwrap();
        let _meta = store.metadata(&key);
        // Should not panic
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_new_with_upload_parallelism_creates_backend() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new_with_upload_parallelism(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
            NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
        )
        .await;
        assert!(backend.is_ok());
        let backend = backend.unwrap();
        assert_eq!(backend.public_base_url(), "http://127.0.0.1:8080");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_read_record_returns_not_found_for_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();
        let result = backend.read_record("nonexistent.txt", None, None).await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_read_record_rejects_invalid_file_id() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();
        let result = backend.read_record("../bad", None, None).await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_ready_with_blackhole_store_hits_non_local_path() {
        let tmp = tempfile::tempdir().unwrap();
        let blackhole = crate::object_store::ServerObjectStore::blackhole();
        // The blackhole store has no local_root, so ready() goes through the
        // else branch (ObjectKey::parse + metadata probe).
        // Blackhole returns Ok(None) for metadata, not an error, so ready() continues
        // to the record store checks and eventually succeeds.
        let backend = LocalBackend::new_with_object_store(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
            blackhole,
        )
        .await
        .unwrap();
        let result = backend.ready().await;
        assert!(
            result.is_ok(),
            "ready() with blackhole store should succeed since metadata returns Ok(None), got: {:?}",
            result.err()
        );
    }

    #[test]
    fn content_hash_with_single_chunk() {
        let chunks = [FileChunkRecord {
            hash: "abc".to_owned(),
            offset: 0,
            length: 10,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 10,
        }];
        let hash = content_hash(10, 10, &chunks);
        assert_eq!(hash.len(), 64); // blake3 hex
    }

    #[test]
    fn content_hash_with_multiple_chunks() {
        let chunks = [
            FileChunkRecord {
                hash: "abc".to_owned(),
                offset: 0,
                length: 10,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 10,
            },
            FileChunkRecord {
                hash: "def".to_owned(),
                offset: 10,
                length: 20,
                range_start: 1,
                range_end: 2,
                packed_start: 10,
                packed_end: 30,
            },
        ];
        let hash = content_hash(30, 10, &chunks);
        assert_eq!(hash.len(), 64);
        assert!(!hash.is_empty());
    }
}
