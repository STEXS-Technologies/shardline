use std::io::{Error as IoError, Read};
use std::num::TryFromIntError;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use shardline_index::{LocalRecordStore, PostgresRecordStore, RecordStore, RecordTraversal};
use shardline_protocol::ByteRange;
use shardline_storage::{
    AsyncObjectStore, DeleteOutcome, LocalObjectStore, LocalObjectStoreError, ObjectBody,
    ObjectIntegrity, ObjectKey, ObjectMetadata, ObjectPrefix, ObjectStore, PutOutcome,
    S3ObjectStore, S3ObjectStoreConfig, S3ObjectStoreError,
};
use thiserror::Error;

/// Object-store backend error.
#[derive(Debug, Error)]
pub enum ServerObjectStoreError {
    /// Requested content was not found.
    #[error("content not found")]
    NotFound,
    /// Arithmetic overflowed a checked bound.
    #[error("arithmetic overflow")]
    Overflow,
    /// A content hash was malformed.
    #[error("content hash must be 64 hexadecimal characters")]
    InvalidContentHash,
    /// Stored object metadata disagreed with the expected transfer length.
    #[error("stored object length did not match indexed metadata")]
    StoredObjectLengthMismatch,
    /// Local storage IO failed.
    #[error("local storage operation failed")]
    Local(#[from] LocalObjectStoreError),
    /// S3-compatible object-storage access failed.
    #[error("s3 object storage operation failed")]
    S3(#[from] S3ObjectStoreError),
    /// A local filesystem I/O error occurred.
    #[error("local storage io failed")]
    Io(#[from] IoError),
    /// Numeric conversion exceeded supported bounds.
    #[error("numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
}

/// Unified object-store backend that delegates to local, S3, or blackhole storage.
#[derive(Debug, Clone)]
pub enum ServerObjectStore {
    /// Local filesystem object store.
    Local(LocalObjectStore),
    /// S3-compatible object store.
    S3(S3ObjectStore),
    /// Blackhole object store that discards all writes.
    Blackhole,
}

impl ObjectStore for ServerObjectStore {
    type Error = ServerObjectStoreError;

    fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error> {
        match self {
            Self::Local(store) => Ok(ObjectStore::put_if_absent(store, key, body, integrity)?),
            Self::S3(store) => Ok(ObjectStore::put_if_absent(store, key, body, integrity)?),
            Self::Blackhole => Ok(PutOutcome::Inserted),
        }
    }

    fn read_range(&self, key: &ObjectKey, range: ByteRange) -> Result<Vec<u8>, Self::Error> {
        match self {
            Self::Local(store) => Ok(ObjectStore::read_range(store, key, range)?),
            Self::S3(store) => Ok(ObjectStore::read_range(store, key, range)?),
            Self::Blackhole => Err(ServerObjectStoreError::NotFound),
        }
    }

    fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
        match self {
            Self::Local(store) => Ok(ObjectStore::contains(store, key)?),
            Self::S3(store) => Ok(ObjectStore::contains(store, key)?),
            Self::Blackhole => Ok(false),
        }
    }

    fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
        match self {
            Self::Local(store) => Ok(ObjectStore::metadata(store, key)?),
            Self::S3(store) => Ok(ObjectStore::metadata(store, key)?),
            Self::Blackhole => Ok(None),
        }
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error> {
        match self {
            Self::Local(store) => Ok(ObjectStore::list_prefix(store, prefix)?),
            Self::S3(store) => Ok(ObjectStore::list_prefix(store, prefix)?),
            Self::Blackhole => Ok(Vec::new()),
        }
    }

    fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
        match self {
            Self::Local(store) => Ok(ObjectStore::delete_if_present(store, key)?),
            Self::S3(store) => Ok(ObjectStore::delete_if_present(store, key)?),
            Self::Blackhole => Ok(DeleteOutcome::NotFound),
        }
    }
}

#[async_trait]
impl AsyncObjectStore for ServerObjectStore {
    type Error = ServerObjectStoreError;

    async fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error> {
        match self {
            Self::Local(store) => AsyncObjectStore::put_if_absent(store, key, body, integrity)
                .await
                .map_err(Into::into),
            Self::S3(store) => AsyncObjectStore::put_if_absent(store, key, body, integrity)
                .await
                .map_err(Into::into),
            Self::Blackhole => Ok(PutOutcome::Inserted),
        }
    }

    async fn read_range(
        &self,
        key: &ObjectKey,
        range: ByteRange,
    ) -> Result<Vec<u8>, Self::Error> {
        match self {
            Self::Local(store) => AsyncObjectStore::read_range(store, key, range)
                .await
                .map_err(Into::into),
            Self::S3(store) => AsyncObjectStore::read_range(store, key, range)
                .await
                .map_err(Into::into),
            Self::Blackhole => Err(ServerObjectStoreError::NotFound),
        }
    }

    async fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
        match self {
            Self::Local(store) => AsyncObjectStore::contains(store, key)
                .await
                .map_err(Into::into),
            Self::S3(store) => AsyncObjectStore::contains(store, key)
                .await
                .map_err(Into::into),
            Self::Blackhole => Ok(false),
        }
    }

    async fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
        match self {
            Self::Local(store) => AsyncObjectStore::metadata(store, key)
                .await
                .map_err(Into::into),
            Self::S3(store) => AsyncObjectStore::metadata(store, key)
                .await
                .map_err(Into::into),
            Self::Blackhole => Ok(None),
        }
    }

    async fn list_prefix(
        &self,
        prefix: &ObjectPrefix,
    ) -> Result<Vec<ObjectMetadata>, Self::Error> {
        match self {
            Self::Local(store) => AsyncObjectStore::list_prefix(store, prefix)
                .await
                .map_err(Into::into),
            Self::S3(store) => AsyncObjectStore::list_prefix(store, prefix)
                .await
                .map_err(Into::into),
            Self::Blackhole => Ok(Vec::new()),
        }
    }

    async fn delete_if_present(
        &self,
        key: &ObjectKey,
    ) -> Result<DeleteOutcome, Self::Error> {
        match self {
            Self::Local(store) => AsyncObjectStore::delete_if_present(store, key)
                .await
                .map_err(Into::into),
            Self::S3(store) => AsyncObjectStore::delete_if_present(store, key)
                .await
                .map_err(Into::into),
            Self::Blackhole => Ok(DeleteOutcome::NotFound),
        }
    }
}

impl ServerObjectStore {
    /// Creates a local filesystem object store rooted at the given path.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError::Local`] if the local store cannot be created.
    pub fn local(root: impl Into<PathBuf>) -> Result<Self, ServerObjectStoreError> {
        Ok(Self::Local(LocalObjectStore::new(root.into())?))
    }

    /// Creates an S3-compatible object store from the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError::S3`] if the S3 store cannot be created.
    pub fn s3(config: S3ObjectStoreConfig) -> Result<Self, ServerObjectStoreError> {
        Ok(Self::S3(S3ObjectStore::new(config)?))
    }

    /// Creates a blackhole object store that discards all writes.
    #[must_use]
    pub const fn blackhole() -> Self {
        Self::Blackhole
    }

    /// Stores an object, overwriting any existing object at the given key.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError`] on storage backend failures.
    pub fn put_overwrite(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<(), ServerObjectStoreError> {
        match self {
            Self::Local(store) => store
                .put_overwrite(key, body, integrity)
                .map_err(Into::into),
            Self::S3(store) => store
                .put_overwrite(key, body, integrity)
                .map_err(Into::into),
            Self::Blackhole => Ok(()),
        }
    }

    /// Visits all objects under the given prefix, invoking the visitor for each.
    ///
    /// # Errors
    ///
    /// Returns any error produced by the visitor or the underlying storage backend.
    pub fn visit_prefix<F, E>(&self, prefix: &ObjectPrefix, mut visitor: F) -> Result<(), E>
    where
        F: FnMut(ObjectMetadata) -> Result<(), E>,
        E: From<LocalObjectStoreError> + From<S3ObjectStoreError>,
    {
        match self {
            Self::Local(store) => ObjectStore::visit_prefix(store, prefix, &mut visitor),
            Self::S3(store) => ObjectStore::visit_prefix(store, prefix, &mut visitor),
            Self::Blackhole => Ok(()),
        }
    }

    /// Lists objects under the given prefix with pagination.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError`] on storage backend failures.
    pub fn list_flat_namespace_page(
        &self,
        prefix: &ObjectPrefix,
        start_after: Option<&ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, ServerObjectStoreError> {
        match self {
            Self::Local(store) => store
                .list_flat_namespace_page(prefix, start_after, limit)
                .map_err(Into::into),
            Self::S3(store) => store
                .list_flat_namespace_page(prefix, start_after, limit)
                .map_err(Into::into),
            Self::Blackhole => Ok(Vec::new()),
        }
    }

    /// Returns the local filesystem path for an object key, if backed by local storage.
    #[must_use]
    pub fn local_path_for_key(&self, key: &ObjectKey) -> Option<PathBuf> {
        match self {
            Self::Local(store) => Some(store.path_for_key(key)),
            Self::S3(_store) => None,
            Self::Blackhole => None,
        }
    }

    /// Copies an object from source to destination if no object exists at the destination.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError::NotFound`] for blackhole stores or
    /// storage backend errors.
    pub fn copy_if_absent(
        &self,
        source: &ObjectKey,
        destination: &ObjectKey,
    ) -> Result<PutOutcome, ServerObjectStoreError> {
        match self {
            Self::Local(store) => store
                .copy_object_if_absent(source, destination)
                .map_err(Into::into),
            Self::S3(store) => store
                .copy_object_if_absent(source, destination)
                .map_err(Into::into),
            Self::Blackhole => Err(ServerObjectStoreError::NotFound),
        }
    }

    /// Stores a content-addressed file from the local filesystem.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError`] on storage backend failures.
    pub fn put_content_addressed_file(
        &self,
        key: &ObjectKey,
        path: &Path,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, ServerObjectStoreError> {
        match self {
            Self::Local(store) => store
                .put_temporary_file_if_absent(key, path, integrity)
                .map_err(Into::into),
            Self::S3(store) => store
                .put_content_addressed_file(key, path, integrity)
                .map_err(Into::into),
            Self::Blackhole => Ok(PutOutcome::Inserted),
        }
    }

    /// Returns the local filesystem root, if backed by local storage.
    #[must_use]
    pub fn local_root(&self) -> Option<&Path> {
        match self {
            Self::Local(store) => Some(store.root()),
            Self::S3(_store) => None,
            Self::Blackhole => None,
        }
    }

    /// Returns the backend name for this object store.
    #[must_use]
    pub const fn backend_name(&self) -> &'static str {
        match self {
            Self::Local(_store) => "local",
            Self::S3(_store) => "s3",
            Self::Blackhole => "blackhole",
        }
    }

    /// Probes the storage backend for connectivity at startup.
    ///
    /// For local storage, verifies the root directory exists and is accessible.
    /// For S3, issues a lightweight list request to confirm the endpoint and
    /// bucket are reachable. Blackhole stores always succeed.
    ///
    /// Returns `Ok(())` when reachable, or `Err(message)` with the failure reason.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the storage root is inaccessible, the S3 endpoint/bucket
    /// is unreachable, or the storage configuration is invalid.
    pub fn probe(&self) -> Result<(), String> {
        match self {
            Self::Local(store) => {
                let root = store.root();
                if !root.exists() {
                    return Err(format!(
                        "local storage root directory does not exist: {}",
                        root.display()
                    ));
                }
                if !root.is_dir() {
                    return Err(format!(
                        "local storage root path is not a directory: {}",
                        root.display()
                    ));
                }
                // Verify write access by checking we can stat the directory
                std::fs::metadata(root)
                    .map(|m| {
                        if !m.permissions().readonly() {
                            Ok(())
                        } else {
                            Err(format!(
                                "local storage root is read-only: {}",
                                root.display()
                            ))
                        }
                    })
                    .map_err(|e| format!("local storage root inaccessible: {e}"))?
            }
            Self::S3(store) => {
                // Attempt a lightweight list with an empty prefix to verify connectivity
                let empty_prefix = ObjectPrefix::parse("")
                    .map_err(|e| format!("failed to create probe prefix: {e}"))?;
                ObjectStore::list_prefix(store, &empty_prefix)
                    .map_err(|e| format!("s3 storage probe failed: {e}"))?;
                Ok(())
            }
            Self::Blackhole => Ok(()),
        }
    }

    /// Reads the full contents of an object from the store.
    ///
    /// # Errors
    ///
    /// Returns [`ServerObjectStoreError`] on storage backend failures, length
    /// mismatches, or arithmetic overflows.
    pub fn read_full_object(
        &self,
        object_key: &ObjectKey,
        length: u64,
    ) -> Result<Vec<u8>, ServerObjectStoreError> {
        if length == 0 {
            return Ok(Vec::new());
        }

        if let Self::Local(store) = self {
            let file = store.open_object_file(object_key)?;
            let actual_length = file.metadata()?.len();
            if actual_length != length {
                return Err(ServerObjectStoreError::StoredObjectLengthMismatch);
            }
            let capacity = usize::try_from(length)?;
            let mut output = Vec::with_capacity(capacity);
            let mut limited = file.take(length);
            Read::read_to_end(&mut limited, &mut output)?;
            if output.len() != capacity {
                return Err(ServerObjectStoreError::StoredObjectLengthMismatch);
            }
            return Ok(output);
        }

        let end = length
            .checked_sub(1)
            .ok_or(ServerObjectStoreError::Overflow)?;
        let range = ByteRange::new(0, end).map_err(|_error| ServerObjectStoreError::Overflow)?;
        ObjectStore::read_range(self, object_key, range)
    }
}

/// Reads the full contents of an object from the store.
///
/// # Errors
///
/// Returns [`ServerObjectStoreError`] on storage backend failures, length
/// mismatches, or arithmetic overflows.
pub fn read_full_object(
    store: &ServerObjectStore,
    object_key: &ObjectKey,
    length: u64,
) -> Result<Vec<u8>, ServerObjectStoreError> {
    store.read_full_object(object_key, length)
}

/// Operation-time record-store classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpsRecordKind {
    /// Latest record.
    Latest,
    /// Version record.
    Version,
}

/// Extra locator metadata needed by operator tooling.
pub trait OpsRecordStore: RecordStore {
    /// Renders a stable operator-facing location for one record locator.
    fn locator_display(&self, locator: &<Self as RecordTraversal>::Locator) -> String;

    /// Extracts the file identifier implied by a locator.
    fn locator_file_id(
        &self,
        locator: &<Self as RecordTraversal>::Locator,
        kind: OpsRecordKind,
    ) -> Option<String>;

    /// Extracts the immutable content hash implied by a version locator.
    fn locator_content_hash(
        &self,
        locator: &<Self as RecordTraversal>::Locator,
        kind: OpsRecordKind,
    ) -> Option<String>;
}

impl OpsRecordStore for LocalRecordStore {
    fn locator_display(&self, locator: &<Self as RecordTraversal>::Locator) -> String {
        locator.record_key().to_owned()
    }

    fn locator_file_id(
        &self,
        locator: &<Self as RecordTraversal>::Locator,
        _kind: OpsRecordKind,
    ) -> Option<String> {
        Some(locator.file_id().to_owned())
    }

    fn locator_content_hash(
        &self,
        locator: &<Self as RecordTraversal>::Locator,
        kind: OpsRecordKind,
    ) -> Option<String> {
        if kind != OpsRecordKind::Version {
            return None;
        }

        locator.content_hash().map(ToOwned::to_owned)
    }
}

impl OpsRecordStore for PostgresRecordStore {
    fn locator_display(&self, locator: &<Self as RecordTraversal>::Locator) -> String {
        locator.record_key().to_owned()
    }

    fn locator_file_id(
        &self,
        locator: &<Self as RecordTraversal>::Locator,
        _kind: OpsRecordKind,
    ) -> Option<String> {
        Some(locator.file_id().to_owned())
    }

    fn locator_content_hash(
        &self,
        locator: &<Self as RecordTraversal>::Locator,
        kind: OpsRecordKind,
    ) -> Option<String> {
        if kind != OpsRecordKind::Version {
            return None;
        }

        locator.content_hash().map(ToOwned::to_owned)
    }
}
