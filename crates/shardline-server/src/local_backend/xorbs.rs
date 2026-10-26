use axum::body::Bytes;
use std::num::NonZeroU64;

use shardline_cas::{CasCoordinator, CasLimits};
use shardline_index::UploadIntent;
use shardline_protocol::{ByteRange, RepositoryScope};
use shardline_storage::ObjectStore;

use super::LocalBackend;
use crate::{
    ServerError,
    download_stream::{
        ServerByteStream, VerifiedXorb, object_byte_stream, prepare_xorb_byte_range,
    },
    upload_ingest::{RequestBodyReader, stage_body_with_spill, store_admitted_xorb},
    xet_adapter::{XorbUploadResponse, resolve_dedupe_shard_object, xorb_object_key},
};

impl LocalBackend {
    /// Stores a raw xorb body under its content hash.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the supplied hash is invalid, the body hash does not
    /// match, or persistence fails.
    pub async fn upload_xorb(
        &self,
        expected_hash: &str,
        body: Bytes,
    ) -> Result<XorbUploadResponse, ServerError> {
        let _barrier =
            crate::maintenance_barrier::acquire_local_shared(&self.metadata_root).await?;
        self.upload_xorb_stream(expected_hash, RequestBodyReader::from_bytes(body))
            .await
    }

    /// Stores a bounded raw xorb body under its content hash.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when request streaming, hash validation, or persistence
    /// fails.
    pub(crate) async fn upload_xorb_stream(
        &self,
        expected_hash: &str,
        mut body: RequestBodyReader,
    ) -> Result<XorbUploadResponse, ServerError> {
        let object_store = self.object_store();
        let staged = stage_body_with_spill(&mut body).await?;
        let (staged, body_length) =
            crate::upload_identity::xorb_length(expected_hash, staged, &self.stream_work_pool)
                .await?;
        let intent_id = crate::upload_identity::xorb(expected_hash, body_length);
        let permit = self
            .stream_work_pool
            .acquire()
            .await
            .map_err(|error| ServerError::Io(std::io::Error::other(error)))?;
        let object_key = xorb_object_key(expected_hash).map_err(ServerError::from)?;
        let intent = UploadIntent::new(
            intent_id.clone(),
            object_key.as_str().to_owned(),
            expected_hash.to_owned(),
            body_length,
        );
        let coordinator = CasCoordinator::new(
            self.index_store.clone(),
            (),
            (),
            CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX),
        );
        let intent = coordinator
            .content_addressed_upload_attempt(&intent)
            .await?;
        coordinator
            .with_upload_intent(&intent, move || async move {
                store_admitted_xorb(&object_store, expected_hash, staged, permit).await
            })
            .await
    }

    pub(crate) async fn read_dedupe_shard_stream(
        &self,
        hash_hex: &str,
    ) -> Result<(ServerByteStream, u64), ServerError> {
        let object_store = self.object_store();
        let (object_key, total_length) =
            resolve_dedupe_shard_object(&self.index_store, &object_store, hash_hex).await?;
        let byte_stream = object_byte_stream(object_store, object_key, total_length).await?;

        Ok((byte_stream, total_length))
    }

    /// Prepares a fully verified xorb for subsequent range authorization.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash is invalid, the xorb is missing, or the
    /// stored length or content fails verification.
    pub(crate) async fn prepare_xorb_range(
        &self,
        hash_hex: &str,
        total_length: u64,
    ) -> Result<VerifiedXorb, ServerError> {
        let object_store = self.object_store();
        let object_key = xorb_object_key(hash_hex)?;

        prepare_xorb_byte_range(
            &object_store,
            &object_key,
            hash_hex,
            total_length,
            self.stream_work_pool.clone(),
        )
        .await
    }

    /// Loads the stored byte length for a serialized xorb object.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash is invalid or the xorb is missing.
    pub async fn xorb_length(&self, hash_hex: &str) -> Result<u64, ServerError> {
        let object_store = self.object_store();
        let object_key = xorb_object_key(hash_hex)?;
        let metadata = object_store.metadata(&object_key)?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };

        Ok(metadata.length())
    }

    pub(crate) async fn repository_xorb_coverage(
        &self,
        hash: &str,
        scope: &RepositoryScope,
        requested: Option<ByteRange>,
    ) -> Result<crate::record_store::XorbRangeCoverage, ServerError> {
        crate::record_store::repository_xorb_coverage(&self.record_store, hash, scope, requested)
            .await
    }

    pub(crate) async fn repository_references_xorb(
        &self,
        hash_hex: &str,
        repository_scope: &RepositoryScope,
    ) -> Result<bool, ServerError> {
        super::records::repository_references_xorb(&self.record_store, hash_hex, repository_scope)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::LocalBackend;
    use crate::xet_adapter::xorb_object_key;

    #[test]
    fn xorb_object_key_accepts_valid_hash() {
        let hash = "a".repeat(64);
        let key = xorb_object_key(&hash);
        assert!(key.is_ok());
        let key = key.unwrap();
        assert!(key.as_str().contains(&hash));
    }

    #[test]
    fn xorb_object_key_rejects_short_hash() {
        let hash = "abc123";
        let key = xorb_object_key(hash);
        assert!(key.is_err());
    }

    #[test]
    fn xorb_object_key_rejects_non_hex_hash() {
        let hash = "z".repeat(64);
        let key = xorb_object_key(&hash);
        assert!(key.is_err());
    }

    #[test]
    fn xorb_object_key_rejects_empty_hash() {
        let key = xorb_object_key("");
        assert!(key.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_xorb_length_returns_not_found_for_missing_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();

        let hash = "aa".repeat(32);
        let result = backend.xorb_length(&hash).await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_xorb_length_rejects_invalid_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();

        let result = backend.xorb_length("short").await;
        assert!(result.is_err());
    }
}
