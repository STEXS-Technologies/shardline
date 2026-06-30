use axum::body::Bytes;
use shardline_protocol::{ByteRange, RepositoryScope};

use super::LocalBackend;
use crate::{
    ServerError,
    download_stream::{ServerByteStream, object_byte_range_stream, object_byte_stream},
    upload_ingest::RequestBodyReader,
    xet_adapter::{
        XorbUploadResponse, resolve_dedupe_shard_object, store_uploaded_xorb_bytes,
        xorb_object_key,
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

    pub(crate) async fn dedupe_shard_length(&self, hash_hex: &str) -> Result<u64, ServerError> {
        let object_store = self.object_store();
        let (_object_key, total_length) =
            resolve_dedupe_shard_object(&self.index_store, &object_store, hash_hex).await?;

        Ok(total_length)
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
