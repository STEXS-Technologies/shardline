use shardline_protocol::ByteRange;
use shardline_storage::{
    DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata, ObjectPrefix, ObjectStore,
    PutOutcome,
};

use super::LocalBackend;
use crate::{
    ServerError,
    download_stream::{ServerByteStream, object_byte_range_stream, object_byte_stream},
    object_store::read_full_object,
    protocol_support::shared_sha256_object_key,
};

impl LocalBackend {
    pub(crate) fn put_object_bytes_if_absent(
        &self,
        object_key: &ObjectKey,
        bytes: Vec<u8>,
    ) -> Result<PutOutcome, ServerError> {
        let integrity = ObjectIntegrity::new(
            super::chunk_hash(&bytes),
            u64::try_from(bytes.len())?,
        );
        Ok(self.object_store().put_if_absent(
            object_key,
            ObjectBody::from_vec(bytes),
            &integrity,
        )?)
    }

    pub(crate) fn put_sha256_addressed_object_bytes_if_absent(
        &self,
        object_key: &ObjectKey,
        digest_hex: &str,
        bytes: Vec<u8>,
    ) -> Result<PutOutcome, ServerError> {
        let canonical_key = shared_sha256_object_key(digest_hex)?;
        let integrity = ObjectIntegrity::new(
            super::chunk_hash(&bytes),
            u64::try_from(bytes.len())?,
        );
        let canonical_outcome = self.object_store().put_if_absent(
            &canonical_key,
            ObjectBody::from_vec(bytes),
            &integrity,
        )?;
        if canonical_key == *object_key {
            return Ok(canonical_outcome);
        }
        Ok(self
            .object_store()
            .copy_if_absent(&canonical_key, object_key)?)
    }

    pub(crate) fn copy_object_if_absent(
        &self,
        source: &ObjectKey,
        destination: &ObjectKey,
    ) -> Result<PutOutcome, ServerError> {
        Ok(self.object_store().copy_if_absent(source, destination)?)
    }

    pub(crate) fn put_object_bytes_overwrite(
        &self,
        object_key: &ObjectKey,
        bytes: Vec<u8>,
    ) -> Result<(), ServerError> {
        let integrity = ObjectIntegrity::new(
            super::chunk_hash(&bytes),
            u64::try_from(bytes.len())?,
        );
        Ok(self.object_store().put_overwrite(
            object_key,
            ObjectBody::from_vec(bytes),
            &integrity,
        )?)
    }

    pub(crate) fn put_sha256_addressed_object_file(
        &self,
        object_key: &ObjectKey,
        digest_hex: &str,
        path: &std::path::Path,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, ServerError> {
        let canonical_key = shared_sha256_object_key(digest_hex)?;
        let canonical_outcome =
            self.object_store()
                .put_content_addressed_file(&canonical_key, path, integrity)?;
        if canonical_key == *object_key {
            return Ok(canonical_outcome);
        }
        Ok(self
            .object_store()
            .copy_if_absent(&canonical_key, object_key)?)
    }

    pub(crate) async fn object_length(&self, object_key: &ObjectKey) -> Result<u64, ServerError> {
        let metadata = self.object_store().metadata(object_key)?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };
        Ok(metadata.length())
    }

    pub(crate) async fn read_object(&self, object_key: &ObjectKey) -> Result<Vec<u8>, ServerError> {
        let object_store = self.object_store();
        let metadata = object_store.metadata(object_key)?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };
        let object_key = object_key.clone();
        tokio::task::spawn_blocking(move || {
            read_full_object(&object_store, &object_key, metadata.length())
        })
        .await
        .map_err(ServerError::BlockingTask)?
    }

    pub(crate) async fn read_object_stream(
        &self,
        object_key: &ObjectKey,
        total_length: u64,
        range: Option<ByteRange>,
    ) -> Result<ServerByteStream, ServerError> {
        let object_store = self.object_store();
        if let Some(range) = range {
            return object_byte_range_stream(object_store, object_key.clone(), total_length, range)
                .await;
        }

        object_byte_stream(object_store, object_key.clone(), total_length).await
    }

    pub(crate) fn visit_object_prefix<Visitor>(
        &self,
        prefix: &ObjectPrefix,
        visitor: Visitor,
    ) -> Result<(), ServerError>
    where
        Visitor: FnMut(ObjectMetadata) -> Result<(), ServerError>,
    {
        crate::object_store::visit_object_prefix(&self.object_store(), prefix, visitor)
    }

    pub(crate) fn list_object_flat_namespace_page(
        &self,
        prefix: &ObjectPrefix,
        start_after: Option<&ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, ServerError> {
        Ok(self
            .object_store()
            .list_flat_namespace_page(prefix, start_after, limit)?)
    }

    pub(crate) async fn delete_object_if_present(
        &self,
        object_key: &ObjectKey,
    ) -> Result<DeleteOutcome, ServerError> {
        Ok(self.object_store().delete_if_present(object_key)?)
    }
}
