use shardline_protocol::ByteRange;
use shardline_storage::{
    DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata, ObjectPrefix,
    ObjectStore, PutOutcome,
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
        tokio::task::block_in_place(|| {
            let integrity =
                ObjectIntegrity::new(super::chunk_hash(&bytes), u64::try_from(bytes.len())?);
            Ok(self.object_store().put_if_absent(
                object_key,
                ObjectBody::from_vec(bytes),
                &integrity,
            )?)
        })
    }

    pub(crate) fn put_sha256_addressed_object_bytes_if_absent(
        &self,
        object_key: &ObjectKey,
        digest_hex: &str,
        bytes: Vec<u8>,
    ) -> Result<PutOutcome, ServerError> {
        tokio::task::block_in_place(|| {
            let canonical_key = shared_sha256_object_key(digest_hex)?;
            let integrity =
                ObjectIntegrity::new(super::chunk_hash(&bytes), u64::try_from(bytes.len())?);
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
        })
    }

    pub(crate) fn copy_object_if_absent(
        &self,
        source: &ObjectKey,
        destination: &ObjectKey,
    ) -> Result<PutOutcome, ServerError> {
        tokio::task::block_in_place(|| {
            Ok(self.object_store().copy_if_absent(source, destination)?)
        })
    }

    pub(crate) fn put_object_bytes_overwrite(
        &self,
        object_key: &ObjectKey,
        bytes: Vec<u8>,
    ) -> Result<(), ServerError> {
        tokio::task::block_in_place(|| {
            let integrity =
                ObjectIntegrity::new(super::chunk_hash(&bytes), u64::try_from(bytes.len())?);
            Ok(self.object_store().put_overwrite(
                object_key,
                ObjectBody::from_vec(bytes),
                &integrity,
            )?)
        })
    }

    pub(crate) fn put_sha256_addressed_object_file(
        &self,
        object_key: &ObjectKey,
        digest_hex: &str,
        path: &std::path::Path,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, ServerError> {
        tokio::task::block_in_place(|| {
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
        })
    }

    pub(crate) async fn object_length(&self, object_key: &ObjectKey) -> Result<u64, ServerError> {
        let object_store = self.object_store();
        let object_key = object_key.clone();
        tokio::task::spawn_blocking(move || {
            let metadata = object_store.metadata(&object_key)?;
            let Some(metadata) = metadata else {
                return Err(ServerError::NotFound);
            };
            Ok(metadata.length())
        })
        .await
        .map_err(ServerError::BlockingTask)?
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
        tokio::task::block_in_place(|| {
            crate::object_store::visit_object_prefix(&self.object_store(), prefix, visitor)
        })
    }

    pub(crate) fn list_object_flat_namespace_page(
        &self,
        prefix: &ObjectPrefix,
        start_after: Option<&ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, ServerError> {
        tokio::task::block_in_place(|| {
            Ok(self
                .object_store()
                .list_flat_namespace_page(prefix, start_after, limit)?)
        })
    }

    pub(crate) async fn delete_object_if_present(
        &self,
        object_key: &ObjectKey,
    ) -> Result<DeleteOutcome, ServerError> {
        let object_store = self.object_store();
        let object_key = object_key.clone();
        tokio::task::spawn_blocking(move || Ok(object_store.delete_if_present(&object_key)?))
            .await
            .map_err(ServerError::BlockingTask)?
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use shardline_storage::{ObjectKey, ObjectPrefix};

    use super::LocalBackend;

    async fn make_backend() -> (LocalBackend, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let chunk_size = NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN);
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            chunk_size,
        )
        .await
        .unwrap();
        (backend, tmp)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_object_bytes_if_absent_inserts_new_object() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("test-key").unwrap();
        let result = backend.put_object_bytes_if_absent(&key, b"hello".to_vec());
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_object_bytes_if_absent_idempotent() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("test-key2").unwrap();
        let first = backend.put_object_bytes_if_absent(&key, b"hello".to_vec());
        assert!(first.is_ok());
        let second = backend.put_object_bytes_if_absent(&key, b"hello".to_vec());
        assert!(second.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_sha256_addressed_object_bytes_if_absent_stores_object() {
        let (backend, _tmp) = make_backend().await;
        let body = b"sha256-payload";
        let digest_hex = "ab".repeat(32);
        let canonical_key = crate::protocol_support::shared_sha256_object_key(&digest_hex).unwrap();
        let result = backend.put_sha256_addressed_object_bytes_if_absent(
            &canonical_key,
            &digest_hex,
            body.to_vec(),
        );
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_object_bytes_overwrite_stores_object() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("overwrite-key").unwrap();
        let result = backend.put_object_bytes_overwrite(&key, b"overwrite-data".to_vec());
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn visit_object_prefix_returns_ok_with_empty_store() {
        let (backend, _tmp) = make_backend().await;
        let prefix = ObjectPrefix::parse("nonexistent-prefix").unwrap();
        let mut visited = Vec::new();
        let result = backend.visit_object_prefix(&prefix, |meta| {
            visited.push(meta.key().as_str().to_owned());
            Ok(())
        });
        assert!(result.is_ok());
        assert!(visited.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn visit_object_prefix_lists_stored_objects() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("prefix1/obj1").unwrap();
        backend
            .put_object_bytes_if_absent(&key, b"data1".to_vec())
            .unwrap();
        let key2 = ObjectKey::parse("prefix1/obj2").unwrap();
        backend
            .put_object_bytes_if_absent(&key2, b"data2".to_vec())
            .unwrap();

        let prefix = ObjectPrefix::parse("prefix1").unwrap();
        let mut keys = Vec::new();
        backend
            .visit_object_prefix(&prefix, |meta| {
                keys.push(meta.key().as_str().to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_length_returns_stored_length() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("len-key").unwrap();
        let body = b"length-check";
        backend
            .put_object_bytes_if_absent(&key, body.to_vec())
            .unwrap();
        let length = backend.object_length(&key).await.unwrap();
        assert_eq!(length, body.len() as u64);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_length_returns_not_found_for_missing_key() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("missing-key").unwrap();
        let result = backend.object_length(&key).await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_object_if_present_removes_object() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("delete-me").unwrap();
        backend
            .put_object_bytes_if_absent(&key, b"to-delete".to_vec())
            .unwrap();
        let outcome = backend.delete_object_if_present(&key).await.unwrap();
        assert_eq!(outcome, shardline_storage::DeleteOutcome::Deleted);
        let length = backend.object_length(&key).await;
        assert!(matches!(length, Err(crate::ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_object_if_present_returns_not_found_for_missing() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("already-missing").unwrap();
        let outcome = backend.delete_object_if_present(&key).await.unwrap();
        assert_eq!(outcome, shardline_storage::DeleteOutcome::NotFound);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_object_if_absent_copies_new_object() {
        let (backend, _tmp) = make_backend().await;
        let src = ObjectKey::parse("src-key").unwrap();
        let dst = ObjectKey::parse("dst-key").unwrap();
        backend
            .put_object_bytes_if_absent(&src, b"copy-source".to_vec())
            .unwrap();
        let result = backend.copy_object_if_absent(&src, &dst);
        assert!(result.is_ok());
        let src_len = backend.object_length(&src).await.unwrap();
        let dst_len = backend.object_length(&dst).await.unwrap();
        assert_eq!(src_len, dst_len);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_object_returns_stored_bytes() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("read-test-key").unwrap();
        let data = b"readable-content";
        backend
            .put_object_bytes_if_absent(&key, data.to_vec())
            .unwrap();
        let result = backend.read_object(&key).await.unwrap();
        assert_eq!(result.as_slice(), data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_object_returns_not_found_for_missing() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("missing-read-key").unwrap();
        let result = backend.read_object(&key).await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_object_stream_returns_stream_for_existing_object() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("stream-test-key").unwrap();
        let data = b"stream-content";
        backend
            .put_object_bytes_if_absent(&key, data.to_vec())
            .unwrap();
        let result = backend.read_object_stream(&key, data.len() as u64, None).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_object_flat_namespace_page_returns_paginated_results() {
        let (backend, _tmp) = make_backend().await;
        let prefix = ObjectPrefix::parse("list-prefix").unwrap();
        for i in 0..3 {
            let key = ObjectKey::parse(&format!("list-prefix/obj{i}")).unwrap();
            backend
                .put_object_bytes_if_absent(&key, b"data".to_vec())
                .unwrap();
        }
        let page = backend
            .list_object_flat_namespace_page(&prefix, None, 10)
            .unwrap();
        assert_eq!(page.len(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_object_stream_with_range_returns_substream() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("range-stream-key").unwrap();
        let data = b"hello-world-range";
        backend
            .put_object_bytes_if_absent(&key, data.to_vec())
            .unwrap();
        use shardline_protocol::ByteRange;
        let range = ByteRange::new(0, 4).unwrap();
        let result = backend
            .read_object_stream(&key, data.len() as u64, Some(range))
            .await;
        assert!(result.is_ok());
    }
}
