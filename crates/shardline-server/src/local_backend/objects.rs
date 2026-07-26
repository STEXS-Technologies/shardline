use shardline_protocol::ByteRange;
use shardline_storage::{DeleteOutcome, ObjectKey, ObjectMetadata, ObjectPrefix, ObjectStore};

use super::LocalBackend;
use crate::{
    ServerError,
    download_stream::{ServerByteStream, object_byte_range_stream, object_byte_stream},
    object_store::{read_full_object, visit_object_prefix},
};

impl LocalBackend {
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
        tokio::task::block_in_place(|| visit_object_prefix(&self.object_store(), prefix, visitor))
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

    use shardline_storage::{
        ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix, ObjectStore, PutOutcome,
    };

    use super::LocalBackend;

    trait ObjectStoreFixture {
        fn put_fixture(
            &self,
            key: &ObjectKey,
            bytes: Vec<u8>,
        ) -> Result<PutOutcome, crate::ServerError>;
        fn copy_fixture(
            &self,
            source: &ObjectKey,
            destination: &ObjectKey,
        ) -> Result<PutOutcome, crate::ServerError>;
        fn overwrite_fixture(
            &self,
            key: &ObjectKey,
            bytes: Vec<u8>,
        ) -> Result<(), crate::ServerError>;
    }

    impl ObjectStoreFixture for LocalBackend {
        fn put_fixture(
            &self,
            key: &ObjectKey,
            bytes: Vec<u8>,
        ) -> Result<PutOutcome, crate::ServerError> {
            let integrity =
                ObjectIntegrity::new(super::super::chunk_hash(&bytes), bytes.len() as u64);
            Ok(self
                .object_store()
                .put_if_absent(key, ObjectBody::from_vec(bytes), &integrity)?)
        }

        fn copy_fixture(
            &self,
            source: &ObjectKey,
            destination: &ObjectKey,
        ) -> Result<PutOutcome, crate::ServerError> {
            Ok(self.object_store().copy_if_absent(source, destination)?)
        }

        fn overwrite_fixture(
            &self,
            key: &ObjectKey,
            bytes: Vec<u8>,
        ) -> Result<(), crate::ServerError> {
            let integrity =
                ObjectIntegrity::new(super::super::chunk_hash(&bytes), bytes.len() as u64);
            Ok(self
                .object_store()
                .put_overwrite(key, ObjectBody::from_vec(bytes), &integrity)?)
        }
    }

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
    async fn put_fixture_inserts_new_object() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("test-key").unwrap();
        let result = backend.put_fixture(&key, b"hello".to_vec());
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_fixture_idempotent() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("test-key2").unwrap();
        let first = backend.put_fixture(&key, b"hello".to_vec());
        assert!(first.is_ok());
        let second = backend.put_fixture(&key, b"hello".to_vec());
        assert!(second.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canonical_key_fixture_stores_object() {
        let (backend, _tmp) = make_backend().await;
        let body = b"sha256-payload";
        let digest_hex = "ab".repeat(32);
        let canonical_key = crate::protocol_support::shared_sha256_object_key(&digest_hex).unwrap();
        let result = backend.put_fixture(&canonical_key, body.to_vec());
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overwrite_fixture_stores_object() {
        let (backend, _tmp) = make_backend().await;
        let key = ObjectKey::parse("overwrite-key").unwrap();
        let result = backend.overwrite_fixture(&key, b"overwrite-data".to_vec());
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
        backend.put_fixture(&key, b"data1".to_vec()).unwrap();
        let key2 = ObjectKey::parse("prefix1/obj2").unwrap();
        backend.put_fixture(&key2, b"data2".to_vec()).unwrap();

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
        backend.put_fixture(&key, body.to_vec()).unwrap();
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
        backend.put_fixture(&key, b"to-delete".to_vec()).unwrap();
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
    async fn copy_fixture_copies_new_object() {
        let (backend, _tmp) = make_backend().await;
        let src = ObjectKey::parse("src-key").unwrap();
        let dst = ObjectKey::parse("dst-key").unwrap();
        backend.put_fixture(&src, b"copy-source".to_vec()).unwrap();
        let result = backend.copy_fixture(&src, &dst);
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
        backend.put_fixture(&key, data.to_vec()).unwrap();
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
        backend.put_fixture(&key, data.to_vec()).unwrap();
        let result = backend
            .read_object_stream(&key, data.len() as u64, None)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn list_object_flat_namespace_page_returns_paginated_results() {
        let (backend, _tmp) = make_backend().await;
        let prefix = ObjectPrefix::parse("list-prefix").unwrap();
        for i in 0..3 {
            let key = ObjectKey::parse(&format!("list-prefix/obj{i}")).unwrap();
            backend.put_fixture(&key, b"data".to_vec()).unwrap();
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
        backend.put_fixture(&key, data.to_vec()).unwrap();
        use shardline_protocol::ByteRange;
        let range = ByteRange::new(0, 4).unwrap();
        let result = backend
            .read_object_stream(&key, data.len() as u64, Some(range))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canonical_key_file_fixture_is_readable() {
        use sha2::Digest;
        use std::io::Write;
        let (backend, _tmp) = make_backend().await;
        let data = b"sha256-content-for-file";
        let digest_hex = hex::encode(sha2::Sha256::digest(data));
        let canonical_key = crate::protocol_support::shared_sha256_object_key(&digest_hex).unwrap();

        // Write to a temp file in the backend's temp dir so block_in_place can access it.
        let tmp_path = _tmp.path().join("sha256-upload.tmp");
        let mut tmpfile = std::fs::File::create(&tmp_path).unwrap();
        tmpfile.write_all(data).unwrap();
        tmpfile.sync_all().unwrap();
        drop(tmpfile);

        let bytes = std::fs::read(&tmp_path);
        assert!(bytes.is_ok());
        let Ok(bytes) = bytes else {
            return;
        };
        let result = backend.put_fixture(&canonical_key, bytes);
        assert!(
            result.is_ok(),
            "canonical fixture write failed: {:?}",
            result.err()
        );
        let length = backend.object_length(&canonical_key).await.unwrap();
        assert_eq!(length, data.len() as u64);
    }
}
