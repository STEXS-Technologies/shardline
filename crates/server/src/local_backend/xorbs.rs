use axum::body::Bytes;
use shardline_protocol::{ByteRange, RepositoryScope};
use shardline_storage::ObjectStore;

use super::LocalBackend;
use crate::{
    ServerError,
    download_stream::{ServerByteStream, object_byte_range_stream, object_byte_stream},
    upload_ingest::RequestBodyReader,
    xet_adapter::{
        XorbUploadResponse, resolve_dedupe_shard_object, store_uploaded_xorb_bytes, xorb_object_key,
    },
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
        let uploaded_body = crate::upload_ingest::read_body_to_bytes(&mut body).await?;
        let object_store = self.object_store();
        store_uploaded_xorb_bytes(&object_store, expected_hash, &uploaded_body)
            .map_err(ServerError::from)
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

    /// Streams a stored xorb byte range by hash.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash is invalid, the xorb is missing, or the
    /// requested byte range cannot be served.
    pub(crate) async fn read_xorb_range_stream(
        &self,
        hash_hex: &str,
        total_length: u64,
        range: ByteRange,
    ) -> Result<ServerByteStream, ServerError> {
        let object_store = self.object_store();
        let object_key = xorb_object_key(hash_hex)?;

        object_byte_range_stream(object_store, object_key, total_length, range).await
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
