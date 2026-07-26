use std::sync::Arc;

use async_trait::async_trait;

use shardline_protocol::ByteRange;

use crate::{
    DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata, ObjectPrefix, PutOutcome,
};

/// Asynchronous object storage adapter contract.
///
/// This is the async counterpart of [`ObjectStore`](crate::ObjectStore).
/// All production storage adapters should implement this trait directly.
/// The [`SyncObjectStoreBridge`] adapter wraps a synchronous [`ObjectStore`](crate::ObjectStore)
/// implementor for gradual migration.
///
/// # Cancellation
///
/// Implementations should document cancellation semantics for each method.
/// Dropping the returned future must not leave partial objects visible.
#[async_trait]
pub trait AsyncObjectStore: Send + Sync {
    /// Adapter-specific error type.
    type Error: std::error::Error + Send + Sync;

    /// Stores an object if no identical object exists yet.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when storage fails or when an existing object
    /// conflicts with the supplied integrity metadata.
    async fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error>;

    /// Reads an inclusive byte range from an object.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the object is missing, the range cannot
    /// be served, or storage fails.
    async fn read_range(&self, key: &ObjectKey, range: ByteRange) -> Result<Vec<u8>, Self::Error>;

    /// Returns whether an object exists.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when storage cannot answer the existence check.
    async fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error>;

    /// Returns stored metadata for an object.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when storage cannot answer the metadata lookup.
    async fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error>;

    /// Lists objects under a validated key prefix.
    ///
    /// Implementations should paginate the underlying storage. The returned
    /// vector must not materialize unbounded data in memory.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when inventory lookup fails.
    async fn list_prefix(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error>;

    /// Visits objects under a validated key prefix without requiring callers
    /// to own the full inventory at once.
    ///
    /// The default implementation delegates to [`list_prefix`](Self::list_prefix).
    /// Streaming-capable backends should override this to avoid materializing
    /// the full inventory.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when inventory lookup fails or when the
    /// visitor rejects an object.
    async fn visit_prefix<Visitor, VisitorError>(
        &self,
        prefix: &ObjectPrefix,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(ObjectMetadata) -> Result<(), VisitorError> + Send,
    {
        for metadata in self.list_prefix(prefix).await.map_err(Into::into)? {
            visitor(metadata)?;
        }
        Ok(())
    }

    /// Deletes an object if it exists.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when deletion fails.
    async fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error>;
}

/// Wraps a synchronous [`ObjectStore`](crate::ObjectStore) implementor as an
/// [`AsyncObjectStore`].
///
/// Each method calls [`tokio::task::spawn_blocking`] to run the sync operation
/// on a dedicated blocking thread pool. The associated error type is
/// `Box<dyn std::error::Error + Send + Sync>`.
///
/// This is intended for gradual migration. Long-term, adapters should implement
/// [`AsyncObjectStore`] directly.
#[derive(Clone)]
pub struct SyncObjectStoreBridge<S> {
    inner: Arc<S>,
}

impl<S> SyncObjectStoreBridge<S> {
    /// Wraps a synchronous store.
    #[must_use]
    pub fn new(inner: S) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Returns a reference to the inner synchronous store.
    #[must_use]
    pub fn inner(&self) -> &S {
        self.inner.as_ref()
    }
}

/// Error produced by the [`SyncObjectStoreBridge`].
#[derive(Debug)]
pub struct BridgeError {
    details: String,
}

impl BridgeError {
    #[must_use]
    pub fn new(details: impl Into<String>) -> Self {
        Self {
            details: details.into(),
        }
    }
}

impl std::fmt::Display for BridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl std::error::Error for BridgeError {}

impl From<std::io::Error> for BridgeError {
    fn from(e: std::io::Error) -> Self {
        Self::new(e.to_string())
    }
}

impl From<String> for BridgeError {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

#[async_trait]
impl<S, E> AsyncObjectStore for SyncObjectStoreBridge<S>
where
    S: crate::ObjectStore<Error = E> + Send + Sync + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    type Error = BridgeError;

    async fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error> {
        let inner = Arc::clone(&self.inner);
        let key = key.clone();
        let integrity = *integrity;
        let body_bytes = body.as_slice().to_vec();
        tokio::task::spawn_blocking(move || {
            inner.put_if_absent(&key, ObjectBody::from_vec(body_bytes), &integrity)
        })
        .await
        .map_err(|join| BridgeError::new(format!("blocking task failed: {join}")))?
        .map_err(|e| BridgeError::new(format!("{e}")))
    }

    async fn read_range(&self, key: &ObjectKey, range: ByteRange) -> Result<Vec<u8>, Self::Error> {
        let inner = Arc::clone(&self.inner);
        let key = key.clone();
        tokio::task::spawn_blocking(move || inner.read_range(&key, range))
            .await
            .map_err(|join| BridgeError::new(format!("blocking task failed: {join}")))?
            .map_err(|e| BridgeError::new(format!("{e}")))
    }

    async fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
        let inner = Arc::clone(&self.inner);
        let key = key.clone();
        tokio::task::spawn_blocking(move || inner.contains(&key))
            .await
            .map_err(|join| BridgeError::new(format!("blocking task failed: {join}")))?
            .map_err(|e| BridgeError::new(format!("{e}")))
    }

    async fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
        let inner = Arc::clone(&self.inner);
        let key = key.clone();
        tokio::task::spawn_blocking(move || inner.metadata(&key))
            .await
            .map_err(|join| BridgeError::new(format!("blocking task failed: {join}")))?
            .map_err(|e| BridgeError::new(format!("{e}")))
    }

    async fn list_prefix(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error> {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.clone();
        tokio::task::spawn_blocking(move || inner.list_prefix(&prefix))
            .await
            .map_err(|join| BridgeError::new(format!("blocking task failed: {join}")))?
            .map_err(|e| BridgeError::new(format!("{e}")))
    }

    async fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
        let inner = Arc::clone(&self.inner);
        let key = key.clone();
        tokio::task::spawn_blocking(move || inner.delete_if_present(&key))
            .await
            .map_err(|join| BridgeError::new(format!("blocking task failed: {join}")))?
            .map_err(|e| BridgeError::new(format!("{e}")))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Mutex, MutexGuard, PoisonError};
    use tokio::runtime::Runtime;

    use crate::{
        AsyncObjectStore, DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata,
        ObjectPrefix, ObjectStore, PutOutcome, SyncObjectStoreBridge,
    };
    use shardline_protocol::{ByteRange, ShardlineHash};

    /// Minimal sync store for testing the bridge.
    #[derive(Clone, Default)]
    struct TestStore {
        objects: std::sync::Arc<Mutex<HashMap<ObjectKey, Vec<u8>>>>,
    }

    impl TestStore {
        fn objects(&self) -> MutexGuard<'_, HashMap<ObjectKey, Vec<u8>>> {
            self.objects.lock().unwrap_or_else(PoisonError::into_inner)
        }
    }

    impl ObjectStore for TestStore {
        type Error = std::io::Error;
        fn put_if_absent(
            &self,
            key: &ObjectKey,
            body: ObjectBody<'_>,
            _integrity: &ObjectIntegrity,
        ) -> Result<PutOutcome, Self::Error> {
            let mut map = self.objects();
            if map.contains_key(key) {
                return Ok(PutOutcome::AlreadyExists);
            }
            map.insert(key.clone(), body.as_slice().to_vec());
            Ok(PutOutcome::Inserted)
        }
        fn read_range(&self, key: &ObjectKey, range: ByteRange) -> Result<Vec<u8>, Self::Error> {
            let map = self.objects();
            let data = map
                .get(key)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "missing"))?;
            let start = usize::try_from(range.start()).map_err(|_error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "range start exceeds usize",
                )
            })?;
            let end = range
                .end_inclusive()
                .checked_add(1)
                .and_then(|end| usize::try_from(end).ok())
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid range end")
                })?;
            Ok(data[start..end].to_vec())
        }
        fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
            Ok(self.objects().contains_key(key))
        }
        fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
            let map = self.objects();
            Ok(map
                .get(key)
                .map(|d| ObjectMetadata::new(key.clone(), d.len() as u64, None)))
        }
        fn list_prefix(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error> {
            let map = self.objects();
            let mut items: Vec<_> = map
                .iter()
                .filter(|(k, _)| k.as_str().starts_with(prefix.as_str()))
                .map(|(k, d)| ObjectMetadata::new(k.clone(), d.len() as u64, None))
                .collect();
            items.sort_by(|a, b| a.key().as_str().cmp(b.key().as_str()));
            Ok(items)
        }
        fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
            Ok(if self.objects().remove(key).is_some() {
                DeleteOutcome::Deleted
            } else {
                DeleteOutcome::NotFound
            })
        }
    }

    fn rt() -> Runtime {
        Runtime::new().expect("tokio runtime")
    }

    #[test]
    fn bridge_put_if_absent_inserts_new_object() {
        let bridge = SyncObjectStoreBridge::new(TestStore::default());
        let key = ObjectKey::parse("test/key").unwrap();
        let integrity = ObjectIntegrity::new(ShardlineHash::from_bytes([1; 32]), 4);
        let result =
            rt().block_on(bridge.put_if_absent(&key, ObjectBody::from_slice(b"data"), &integrity));
        assert!(matches!(result, Ok(PutOutcome::Inserted)));
    }

    #[test]
    fn bridge_put_if_absent_idempotent() {
        let bridge = SyncObjectStoreBridge::new(TestStore::default());
        let key = ObjectKey::parse("test/key").unwrap();
        let integrity = ObjectIntegrity::new(ShardlineHash::from_bytes([1; 32]), 4);
        rt().block_on(bridge.put_if_absent(&key, ObjectBody::from_slice(b"data"), &integrity))
            .unwrap();
        let second =
            rt().block_on(bridge.put_if_absent(&key, ObjectBody::from_slice(b"data"), &integrity));
        assert!(matches!(second, Ok(PutOutcome::AlreadyExists)));
    }

    #[test]
    fn bridge_contains_returns_true_for_stored_object() {
        let bridge = SyncObjectStoreBridge::new(TestStore::default());
        let key = ObjectKey::parse("test/key").unwrap();
        let integrity = ObjectIntegrity::new(ShardlineHash::from_bytes([1; 32]), 4);
        rt().block_on(bridge.put_if_absent(&key, ObjectBody::from_slice(b"data"), &integrity))
            .unwrap();
        let found = rt().block_on(bridge.contains(&key));
        assert!(matches!(found, Ok(true)));
    }

    #[test]
    fn bridge_contains_returns_false_for_missing() {
        let bridge = SyncObjectStoreBridge::new(TestStore::default());
        let key = ObjectKey::parse("test/missing").unwrap();
        let found = rt().block_on(bridge.contains(&key));
        assert!(matches!(found, Ok(false)));
    }

    #[test]
    fn bridge_delete_if_present_removes_object() {
        let bridge = SyncObjectStoreBridge::new(TestStore::default());
        let key = ObjectKey::parse("test/key").unwrap();
        let integrity = ObjectIntegrity::new(ShardlineHash::from_bytes([1; 32]), 4);
        rt().block_on(bridge.put_if_absent(&key, ObjectBody::from_slice(b"data"), &integrity))
            .unwrap();
        let deleted = rt().block_on(bridge.delete_if_present(&key));
        assert!(matches!(deleted, Ok(DeleteOutcome::Deleted)));
        let found = rt().block_on(bridge.contains(&key));
        assert!(matches!(found, Ok(false)));
    }

    #[test]
    fn bridge_delete_if_present_returns_not_found_for_missing() {
        let bridge = SyncObjectStoreBridge::new(TestStore::default());
        let key = ObjectKey::parse("test/missing").unwrap();
        let deleted = rt().block_on(bridge.delete_if_present(&key));
        assert!(matches!(deleted, Ok(DeleteOutcome::NotFound)));
    }

    #[test]
    fn bridge_metadata_returns_length() {
        let bridge = SyncObjectStoreBridge::new(TestStore::default());
        let key = ObjectKey::parse("test/key").unwrap();
        let integrity = ObjectIntegrity::new(ShardlineHash::from_bytes([1; 32]), 10);
        rt().block_on(bridge.put_if_absent(
            &key,
            ObjectBody::from_slice(b"1234567890"),
            &integrity,
        ))
        .unwrap();
        let meta = rt().block_on(bridge.metadata(&key));
        assert!(matches!(meta, Ok(Some(m)) if m.length() == 10));
    }

    #[test]
    fn bridge_inner_returns_correct_store() {
        let store = TestStore::default();
        let bridge = SyncObjectStoreBridge::new(store);
        let inner = bridge.inner();
        assert!(
            !inner
                .metadata(&ObjectKey::parse("test/k").unwrap())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn bridge_read_range_returns_data() {
        let bridge = SyncObjectStoreBridge::new(TestStore::default());
        let key = ObjectKey::parse("test/rr").unwrap();
        let body = b"hello world";
        let hash = ShardlineHash::from_bytes(*blake3::hash(body).as_bytes());
        rt().block_on(bridge.put_if_absent(
            &key,
            ObjectBody::from_slice(body),
            &ObjectIntegrity::new(hash, body.len() as u64),
        ))
        .unwrap();
        let range = ByteRange::new(0, 4).unwrap();
        let data = rt().block_on(bridge.read_range(&key, range)).unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn bridge_read_range_missing_returns_error() {
        let bridge = SyncObjectStoreBridge::new(TestStore::default());
        let key = ObjectKey::parse("test/missing").unwrap();
        let range = ByteRange::new(0, 0).unwrap();
        let result = rt().block_on(bridge.read_range(&key, range));
        assert!(result.is_err(), "read_range on missing key should error");
    }

    #[test]
    fn bridge_list_prefix_returns_filtered_keys() {
        let bridge = SyncObjectStoreBridge::new(TestStore::default());
        let integrity = ObjectIntegrity::new(ShardlineHash::from_bytes([1; 32]), 4);
        rt().block_on(bridge.put_if_absent(
            &ObjectKey::parse("ns/a").unwrap(),
            ObjectBody::from_slice(b"data"),
            &integrity,
        ))
        .unwrap();
        rt().block_on(bridge.put_if_absent(
            &ObjectKey::parse("ns/b").unwrap(),
            ObjectBody::from_slice(b"data"),
            &integrity,
        ))
        .unwrap();
        rt().block_on(bridge.put_if_absent(
            &ObjectKey::parse("other/c").unwrap(),
            ObjectBody::from_slice(b"data"),
            &integrity,
        ))
        .unwrap();
        let prefix = ObjectPrefix::parse("ns/").unwrap();
        let items = rt().block_on(bridge.list_prefix(&prefix));
        assert!(matches!(items, Ok(ref v) if v.len() == 2));
    }
}
