use std::io::{Cursor, Error as IoError, Read, Seek, SeekFrom, Write};
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
use tempfile::NamedTempFile;
use thiserror::Error;

const MAX_BUFFERED_OBJECT_BYTES: u64 = 64 * 1024 * 1024;

mod checkpoint_writer;

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

/// Failure from a bounded object read, kept separate from the established
/// object-store error enum so adding this API does not break exhaustive matches.
#[derive(Debug, Error)]
pub enum BoundedObjectReadError {
    /// The object exceeds the requested in-memory limit.
    #[error("object exceeds the configured in-memory read limit")]
    ObjectTooLarge,
    /// Storage access or response verification failed.
    #[error(transparent)]
    Store(#[from] ServerObjectStoreError),
}

/// An object materialized for random-access validation.
///
/// Small S3 objects can stay in memory; larger objects use a streamed temporary
/// file. Local objects retain their existing inode and require no copy.
pub enum MaterializedObject {
    /// A local immutable object file.
    Local(std::fs::File),
    /// A streamed S3 object that exceeded the caller's in-memory limit.
    Temporary(NamedTempFile),
    /// A bounded S3 object read directly into memory.
    Memory(Cursor<Vec<u8>>),
}

impl Read for MaterializedObject {
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, IoError> {
        match self {
            Self::Local(file) => file.read(buffer),
            Self::Temporary(file) => file.as_file_mut().read(buffer),
            Self::Memory(cursor) => cursor.read(buffer),
        }
    }
}

impl Seek for MaterializedObject {
    fn seek(&mut self, position: SeekFrom) -> Result<u64, IoError> {
        match self {
            Self::Local(file) => file.seek(position),
            Self::Temporary(file) => file.as_file_mut().seek(position),
            Self::Memory(cursor) => cursor.seek(position),
        }
    }
}

/// Accumulates a bounded S3 response in memory and creates a scratch file only
/// if the response crosses the in-memory limit. This keeps eligible reads
/// independent of temporary-directory availability.
struct BoundedObjectSink {
    maximum_memory_length: usize,
    memory: Vec<u8>,
    temporary: Option<NamedTempFile>,
}

impl BoundedObjectSink {
    fn new(maximum_memory_length: u64) -> Result<Self, ServerObjectStoreError> {
        Ok(Self {
            maximum_memory_length: usize::try_from(maximum_memory_length)?,
            memory: Vec::new(),
            temporary: None,
        })
    }

    fn spill_to_temporary(&mut self) -> Result<(), IoError> {
        let mut temporary = NamedTempFile::new()?;
        temporary.write_all(&self.memory)?;
        // The scratch file now owns these bytes. Release the allocation too:
        // retaining capacity keeps the full memory budget charged throughout
        // the remaining remote transfer even though it is no longer used.
        self.memory = Vec::new();
        self.temporary = Some(temporary);
        Ok(())
    }

    fn into_object(self) -> Result<MaterializedObject, IoError> {
        match self.temporary {
            Some(mut temporary) => {
                temporary.as_file_mut().seek(SeekFrom::Start(0))?;
                Ok(MaterializedObject::Temporary(temporary))
            }
            None => Ok(MaterializedObject::Memory(Cursor::new(self.memory))),
        }
    }
}

impl Write for BoundedObjectSink {
    fn write(&mut self, buffer: &[u8]) -> Result<usize, IoError> {
        if let Some(temporary) = &mut self.temporary {
            return temporary.as_file_mut().write(buffer);
        }
        let next_length = self
            .memory
            .len()
            .checked_add(buffer.len())
            .ok_or_else(|| IoError::other("bounded object length overflow"))?;
        if next_length <= self.maximum_memory_length {
            if self.memory.capacity() < next_length {
                let target_capacity = next_length
                    .max(self.memory.capacity().saturating_mul(2).max(4096))
                    .min(self.maximum_memory_length);
                let additional = target_capacity.saturating_sub(self.memory.len());
                self.memory.try_reserve_exact(additional).map_err(|error| {
                    IoError::new(
                        std::io::ErrorKind::OutOfMemory,
                        format!("failed to reserve bounded object memory: {error}"),
                    )
                })?;
            }
            self.memory.extend_from_slice(buffer);
            return Ok(buffer.len());
        }
        self.spill_to_temporary()?;
        self.temporary
            .as_mut()
            .ok_or_else(|| IoError::other("bounded object spill was not created"))?
            .as_file_mut()
            .write(buffer)
    }

    fn flush(&mut self) -> Result<(), IoError> {
        self.temporary
            .as_mut()
            .map_or(Ok(()), |temporary| temporary.as_file_mut().flush())
    }
}

/// Unified object-store backend that delegates to local, S3, or blackhole storage.
///
/// Construct one of the three backends with [`ServerObjectStore::local`],
/// [`ServerObjectStore::s3`], or [`ServerObjectStore::blackhole`], then use the
/// [`ObjectStore`] trait methods ([`ObjectStore::put_if_absent`],
/// [`ObjectStore::read_range`], [`ObjectStore::list_prefix`], ...) on it.
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

    fn put_verified_if_absent(
        &self,
        key: &ObjectKey,
        body: shardline_storage::VerifiedObjectBody,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error> {
        match self {
            Self::Local(store) => Ok(ObjectStore::put_verified_if_absent(
                store, key, body, integrity,
            )?),
            Self::S3(store) => Ok(ObjectStore::put_verified_if_absent(
                store, key, body, integrity,
            )?),
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

    async fn read_range(&self, key: &ObjectKey, range: ByteRange) -> Result<Vec<u8>, Self::Error> {
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

    async fn read_suffix(
        &self,
        key: &ObjectKey,
        length: u64,
    ) -> Result<Option<(Vec<u8>, u64)>, Self::Error> {
        match self {
            Self::Local(store) => AsyncObjectStore::read_suffix(store, key, length)
                .await
                .map_err(Into::into),
            Self::S3(store) => AsyncObjectStore::read_suffix(store, key, length)
                .await
                .map_err(Into::into),
            Self::Blackhole => Ok(None),
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

    async fn list_prefix(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error> {
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

    async fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
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
    /// Reads a small object, with a 64 MiB bound and complete-body validation.
    ///
    /// S3 uses the GET response's length instead of a preceding HEAD.
    ///
    /// # Errors
    /// Returns an error for oversized objects, invalid transfers or storage failures.
    pub fn read_object_bytes(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<Vec<u8>>, ServerObjectStoreError> {
        if let Self::S3(store) = self {
            return Ok(store.read_object_bytes(key, MAX_BUFFERED_OBJECT_BYTES)?);
        }
        let Some(metadata) = ObjectStore::metadata(self, key)? else {
            return Ok(None);
        };
        self.read_full_object(key, metadata.length()).map(Some)
    }

    /// Materializes an object into an unlinked temporary file using bounded
    /// memory and a single streaming GET for S3. This is for random-access
    /// parsers (xorb/shard formats), not for HTTP delivery; callers must stream
    /// the resulting file onward.
    ///
    /// # Errors
    ///
    /// Returns an error when the temporary file cannot be created, a range
    /// read fails, or the stored object length does not match `length`.
    pub fn materialize_object_to_tempfile(
        &self,
        object_key: &ObjectKey,
        length: u64,
    ) -> Result<NamedTempFile, ServerObjectStoreError> {
        let mut output = NamedTempFile::new().map_err(ServerObjectStoreError::Io)?;
        if length == 0 {
            return Ok(output);
        }

        match self {
            Self::Local(store) => {
                let mut input = store.open_object_file(object_key)?;
                let actual = input.metadata()?.len();
                if actual != length {
                    return Err(ServerObjectStoreError::StoredObjectLengthMismatch);
                }
                std::io::copy(&mut input, output.as_file_mut())
                    .map_err(ServerObjectStoreError::Io)?;
            }
            Self::S3(store) => {
                let end = length
                    .checked_sub(1)
                    .ok_or(ServerObjectStoreError::Overflow)?;
                let range =
                    ByteRange::new(0, end).map_err(|_error| ServerObjectStoreError::Overflow)?;
                store.copy_range_to_file(object_key, range, output.as_file_mut())?;
            }
            Self::Blackhole => return Err(ServerObjectStoreError::NotFound),
        }
        Ok(output)
    }

    /// Materializes an object into a temporary file when it exists, returning
    /// the length observed with the object bytes. For S3 this uses one full GET
    /// instead of a HEAD followed by a ranged GET.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access, length verification, or temporary
    /// file I/O fails.
    pub fn materialize_object_to_tempfile_if_present(
        &self,
        object_key: &ObjectKey,
    ) -> Result<Option<(NamedTempFile, u64)>, ServerObjectStoreError> {
        let mut output = NamedTempFile::new().map_err(ServerObjectStoreError::Io)?;
        match self {
            Self::Local(store) => {
                let Some(metadata) = ObjectStore::metadata(store, object_key)? else {
                    return Ok(None);
                };
                let mut input = store.open_object_file(object_key)?;
                let length = input.metadata()?.len();
                if length != metadata.length() {
                    return Err(ServerObjectStoreError::StoredObjectLengthMismatch);
                }
                std::io::copy(&mut input, output.as_file_mut())
                    .map_err(ServerObjectStoreError::Io)?;
                Ok(Some((output, length)))
            }
            Self::S3(store) => store
                .copy_object_to_file_if_present(object_key, output.as_file_mut())?
                .map(|length| (output, length))
                .map_or(Ok(None), |object| Ok(Some(object))),
            Self::Blackhole => Ok(None),
        }
    }

    /// Materializes an object for random-access validation while avoiding
    /// scratch-file data writes for small S3 objects. The S3 response itself
    /// chooses memory or file output from its length, so large objects are not
    /// probed or downloaded twice. Local objects remain open at their durable
    /// path without a copy.
    ///
    /// # Errors
    ///
    /// Returns an error when object access, response-length validation, or
    /// temporary-file I/O fails.
    pub fn materialize_bounded_object_if_present(
        &self,
        object_key: &ObjectKey,
        maximum_memory_length: u64,
    ) -> Result<Option<(MaterializedObject, u64)>, ServerObjectStoreError> {
        self.materialize_bounded_object_if_present_with_checkpoint(
            object_key,
            maximum_memory_length,
            &|| Ok(()),
        )
    }

    /// Materializes an object with cancellation checks before access and each
    /// S3 output frame. An already-pending provider read retains its timeout.
    /// Partial scratch files and memory are discarded on error.
    ///
    /// # Errors
    /// Returns storage, checkpoint or temporary-file errors. Interrupted
    /// checkpoint writes become ConnectionAborted so write_all cannot retry
    /// cancellation indefinitely.
    pub fn materialize_bounded_object_if_present_with_checkpoint(
        &self,
        object_key: &ObjectKey,
        maximum_memory_length: u64,
        checkpoint: &(dyn Fn() -> std::io::Result<()> + Sync),
    ) -> Result<Option<(MaterializedObject, u64)>, ServerObjectStoreError> {
        checkpoint()?;
        match self {
            Self::Local(store) => {
                let Some(metadata) = ObjectStore::metadata(store, object_key)? else {
                    return Ok(None);
                };
                let file = store.open_object_file(object_key)?;
                let length = file.metadata()?.len();
                if length != metadata.length() {
                    return Err(ServerObjectStoreError::StoredObjectLengthMismatch);
                }
                checkpoint()?;
                Ok(Some((MaterializedObject::Local(file), length)))
            }
            Self::S3(store) => {
                let mut output = BoundedObjectSink::new(maximum_memory_length)?;
                let mut guarded = checkpoint_writer::CheckpointWriter {
                    output: &mut output,
                    checkpoint,
                };
                let result = store.copy_object_to_writer_if_present(object_key, &mut guarded);
                checkpoint()?;
                let Some(length) = result? else {
                    return Ok(None);
                };
                Ok(Some((
                    output.into_object().map_err(ServerObjectStoreError::Io)?,
                    length,
                )))
            }
            Self::Blackhole => Ok(None),
        }
    }

    /// Creates a local filesystem object store rooted at the given path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use shardline_protocol::ShardlineHash;
    /// use shardline_server_core::ServerObjectStore;
    /// use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
    ///
    /// // Creates the root directory if it does not exist.
    /// let store = ServerObjectStore::local("/var/lib/shardline/objects")?;
    ///
    /// let key = ObjectKey::parse("chunks/aa/bb/example.xorb")?;
    /// let body = ObjectBody::from_slice(b"payload");
    /// let integrity = ObjectIntegrity::new(ShardlineHash::from_bytes([7; 32]), 7);
    /// ObjectStore::put_if_absent(&store, &key, body, &integrity)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
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
    ///
    /// Writes report success without touching the filesystem, reads return
    /// `NotFound`, existence checks return `false`, and deletes report the
    /// object as absent. Useful for dry runs and tests.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_protocol::ShardlineHash;
    /// use shardline_server_core::ServerObjectStore;
    /// use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome};
    ///
    /// let store = ServerObjectStore::blackhole();
    /// let key = ObjectKey::parse("chunks/aa/bb/example.xorb")?;
    /// let body = ObjectBody::from_slice(b"payload");
    /// let integrity = ObjectIntegrity::new(ShardlineHash::from_bytes([7; 32]), 7);
    ///
    /// assert_eq!(
    ///     ObjectStore::put_if_absent(&store, &key, body, &integrity)?,
    ///     PutOutcome::Inserted
    /// );
    /// assert!(!ObjectStore::contains(&store, &key)?);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub const fn blackhole() -> Self {
        Self::Blackhole
    }

    /// Stores an object, overwriting any existing object at the given key.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_protocol::ShardlineHash;
    /// use shardline_server_core::ServerObjectStore;
    /// use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey};
    ///
    /// let store = ServerObjectStore::blackhole();
    /// let key = ObjectKey::parse("chunks/aa/bb/example.xorb")?;
    /// let body = ObjectBody::from_slice(b"payload");
    /// let integrity = ObjectIntegrity::new(ShardlineHash::from_bytes([7; 32]), 7);
    /// store.put_overwrite(&key, body, &integrity)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
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

    /// Opens an existing local object for a streaming duplicate check.
    /// Remote and blackhole stores return `None` so callers retain their usual
    /// staging and publication path.
    ///
    /// # Errors
    ///
    /// Returns an error when a local object exists but cannot be safely opened.
    #[cfg(unix)]
    pub fn open_local_object_file_if_present(
        &self,
        object_key: &ObjectKey,
    ) -> Result<Option<std::fs::File>, ServerObjectStoreError> {
        match self {
            Self::Local(store) => store
                .open_object_file_if_present(object_key)
                .map_err(Into::into),
            Self::S3(_) | Self::Blackhole => Ok(None),
        }
    }

    /// Confirms the pinned object is still published at `object_key` and
    /// synchronizes its bytes and parent directory before duplicate
    /// acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns an error if the object was removed or replaced, or if the local
    /// filesystem could not establish durability.
    #[cfg(unix)]
    pub fn verify_local_object_file_is_durable(
        &self,
        object_key: &ObjectKey,
        pinned: &std::fs::File,
    ) -> Result<(), ServerObjectStoreError> {
        match self {
            Self::Local(store) => store
                .verify_open_object_is_still_durable(object_key, pinned)
                .map_err(Into::into),
            Self::S3(_) | Self::Blackhole => Err(ServerObjectStoreError::Io(IoError::new(
                std::io::ErrorKind::Unsupported,
                "durable pinned-object verification requires local storage",
            ))),
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
    /// # Examples
    ///
    /// ```
    /// use shardline_server_core::ServerObjectStore;
    ///
    /// // Blackhole stores always report as reachable.
    /// assert!(ServerObjectStore::blackhole().probe().is_ok());
    /// ```
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
        // Whole-object reads are retained only for small metadata/format
        // records. Large payloads must use `materialize_object_to_tempfile`
        // or a range stream so memory use stays independent of file size.
        if length > MAX_BUFFERED_OBJECT_BYTES {
            return Err(ServerObjectStoreError::Overflow);
        }
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

    /// Reads a bounded object when present and returns the length observed with
    /// the bytes. For S3 this is one full GET instead of a HEAD followed by a
    /// ranged GET.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access or length verification fails, or
    /// when the object exceeds `maximum_length`.
    pub fn read_object_if_present(
        &self,
        object_key: &ObjectKey,
        maximum_length: u64,
    ) -> Result<Option<(Vec<u8>, u64)>, BoundedObjectReadError> {
        match self {
            Self::Local(store) => {
                let Some(metadata) = ObjectStore::metadata(store, object_key)
                    .map_err(ServerObjectStoreError::from)?
                else {
                    return Ok(None);
                };
                let file = store
                    .open_object_file(object_key)
                    .map_err(ServerObjectStoreError::from)?;
                let length = file.metadata().map_err(ServerObjectStoreError::from)?.len();
                if length != metadata.length() {
                    return Err(ServerObjectStoreError::StoredObjectLengthMismatch.into());
                }
                if length > maximum_length {
                    return Err(BoundedObjectReadError::ObjectTooLarge);
                }
                let capacity = usize::try_from(length).map_err(ServerObjectStoreError::from)?;
                let mut output = Vec::with_capacity(capacity);
                let mut limited = file.take(length);
                Read::read_to_end(&mut limited, &mut output)
                    .map_err(ServerObjectStoreError::from)?;
                if output.len() != capacity {
                    return Err(ServerObjectStoreError::StoredObjectLengthMismatch.into());
                }
                Ok(Some((output, length)))
            }
            Self::S3(store) => store
                .read_object_if_present(object_key, maximum_length)
                .map_err(|error| match error {
                    shardline_storage::S3ObjectReadError::ObjectTooLarge => {
                        BoundedObjectReadError::ObjectTooLarge
                    }
                    shardline_storage::S3ObjectReadError::Store(error) => {
                        BoundedObjectReadError::Store(ServerObjectStoreError::S3(error))
                    }
                }),
            Self::Blackhole => Ok(None),
        }
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

/// Classifies whether a record locator points at the latest file revision or at
/// one specific immutable version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpsRecordKind {
    /// Latest record.
    Latest,
    /// Version record.
    Version,
}

/// Extra locator metadata needed by operator tooling.
pub trait OpsRecordStore: RecordStore {
    /// Returns derived metadata with a complete, consistent member index.
    /// Callers must still authenticate the descriptor against verified content.
    fn native_indexed_xorb<'operation>(
        &'operation self,
        _xorb: &'operation str,
    ) -> shardline_index::RecordStoreFuture<
        'operation,
        Option<shardline_index::XetXorbDescriptor>,
        <Self as RecordTraversal>::Error,
    > {
        Box::pin(async { Ok(None) })
    }

    /// Whether operator rebuild can persist verified native member locations.
    fn supports_native_locations(&self) -> bool {
        false
    }

    /// Installs derived metadata only after the caller verifies the full source.
    fn cache_native_location<'operation>(
        &'operation self,
        _descriptor: &'operation shardline_index::XetXorbDescriptor,
    ) -> shardline_index::RecordStoreFuture<'operation, (), <Self as RecordTraversal>::Error> {
        Box::pin(async { Ok(()) })
    }

    /// Finds the indexed source used by native member reads. A descriptor is
    /// only a discovery hint; tooling must authenticate it against source data.
    /// Adapters without a native index conservatively require standalone copies.
    fn native_chunk_location<'operation>(
        &'operation self,
        _member: &'operation str,
        _xorb: &'operation str,
    ) -> shardline_index::RecordStoreFuture<
        'operation,
        Option<shardline_index::XetXorbDescriptor>,
        <Self as RecordTraversal>::Error,
    > {
        Box::pin(async { Ok(None) })
    }

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
    fn native_indexed_xorb<'operation>(
        &'operation self,
        xorb: &'operation str,
    ) -> shardline_index::RecordStoreFuture<
        'operation,
        Option<shardline_index::XetXorbDescriptor>,
        <Self as RecordTraversal>::Error,
    > {
        Box::pin(async move { self.xet_indexed_xorb_descriptor(xorb).await })
    }

    fn supports_native_locations(&self) -> bool {
        true
    }

    fn cache_native_location<'operation>(
        &'operation self,
        descriptor: &'operation shardline_index::XetXorbDescriptor,
    ) -> shardline_index::RecordStoreFuture<'operation, (), <Self as RecordTraversal>::Error> {
        Box::pin(async move {
            self.cache_xet_xorb_descriptors(std::slice::from_ref(descriptor))
                .await
        })
    }

    fn native_chunk_location<'operation>(
        &'operation self,
        member: &'operation str,
        xorb: &'operation str,
    ) -> shardline_index::RecordStoreFuture<
        'operation,
        Option<shardline_index::XetXorbDescriptor>,
        <Self as RecordTraversal>::Error,
    > {
        Box::pin(async move { self.xet_chunk_location(member, xorb).await })
    }
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
    fn native_indexed_xorb<'operation>(
        &'operation self,
        xorb: &'operation str,
    ) -> shardline_index::RecordStoreFuture<
        'operation,
        Option<shardline_index::XetXorbDescriptor>,
        <Self as RecordTraversal>::Error,
    > {
        Box::pin(async move { self.xet_indexed_xorb_descriptor(xorb).await })
    }

    fn supports_native_locations(&self) -> bool {
        true
    }

    fn cache_native_location<'operation>(
        &'operation self,
        descriptor: &'operation shardline_index::XetXorbDescriptor,
    ) -> shardline_index::RecordStoreFuture<'operation, (), <Self as RecordTraversal>::Error> {
        Box::pin(async move {
            self.cache_xet_xorb_descriptors(std::slice::from_ref(descriptor))
                .await
        })
    }
    fn native_chunk_location<'operation>(
        &'operation self,
        member: &'operation str,
        xorb: &'operation str,
    ) -> shardline_index::RecordStoreFuture<
        'operation,
        Option<shardline_index::XetXorbDescriptor>,
        <Self as RecordTraversal>::Error,
    > {
        Box::pin(async move { self.xet_chunk_location(member, xorb).await })
    }
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

#[cfg(test)]
mod bounded_object_sink_tests {
    use std::io::{Read, Write};

    use super::{BoundedObjectSink, MaterializedObject};

    #[test]
    fn small_object_sink_never_creates_a_temporary_file() {
        let mut sink = BoundedObjectSink::new(32).unwrap();
        sink.write_all(b"small validated object").unwrap();

        assert!(sink.temporary.is_none());
        let mut object = sink.into_object().unwrap();
        assert!(matches!(object, MaterializedObject::Memory(_)));
        let mut actual = Vec::new();
        object.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, b"small validated object");
    }

    #[test]
    fn object_sink_spills_at_the_limit_and_preserves_all_bytes() {
        let mut sink = BoundedObjectSink::new(4).unwrap();
        sink.write_all(b"abcd").unwrap();
        assert!(sink.temporary.is_none());
        sink.write_all(b"ef").unwrap();
        assert!(sink.temporary.is_some());
        assert_eq!(sink.memory.capacity(), 0, "spilled reads must release RAM");

        let mut object = sink.into_object().unwrap();
        assert!(matches!(object, MaterializedObject::Temporary(_)));
        let mut actual = Vec::new();
        object.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, b"abcdef");
    }

    #[test]
    fn dropping_a_spilled_object_removes_its_scratch_file() {
        let mut sink = BoundedObjectSink::new(4).unwrap();
        sink.write_all(b"abcd").unwrap();
        sink.write_all(b"efgh").unwrap();
        let scratch = sink.temporary.as_ref().unwrap().path().to_owned();
        assert!(scratch.exists());
        let object = sink.into_object().unwrap();
        assert!(scratch.exists());
        drop(object);
        assert!(!scratch.exists());
    }
}
