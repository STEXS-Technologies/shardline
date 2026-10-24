use std::{
    fmt,
    fs::File,
    future::Future,
    io::{Error as IoError, Write},
    ops::Range,
    path::Path,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use futures_util::{StreamExt, TryStreamExt};
use object_store::{
    CopyMode, CopyOptions, Error as ExternalObjectStoreError, GetOptions, GetResult,
    ObjectStore as ExternalObjectStore, ObjectStoreExt, PutMode, PutPayload, aws::AmazonS3,
    multipart::MultipartStore, path::Path as ObjectStorePath,
};
use shardline_protocol::{ByteRange, ShardlineHash};
use tokio::{
    runtime::{Builder, Handle, Runtime, RuntimeFlavor},
    task::block_in_place,
};

use crate::{
    AsyncObjectStore, DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata,
    ObjectPrefix, ObjectStore, PutOutcome,
};

use super::{
    LARGE_COPY_CHUNK_BYTES, MAX_SINGLE_COPY_BYTES, S3_TEMP_ARTIFACT_AGE_SECONDS, S3ByteStream,
    S3ObjectReadError, S3ObjectStoreConfig, S3ObjectStoreError, STREAM_UPLOAD_CHUNK_BYTES,
    is_temp_upload_key,
};

mod copy_compare;
mod file_compare;
mod file_upload;
mod full_read;
pub(super) mod temp_cleanup;

#[cfg(test)]
mod conditional_upload_tests;

#[cfg(test)]
mod temp_cleanup_tests;

/// S3-compatible implementation of [`ObjectStore`].
#[derive(Clone)]
pub struct S3ObjectStore {
    pub(crate) inner: AmazonS3,
    pub(super) single_attempt_inner: AmazonS3,
    conditional_completion_inner: Arc<super::client::ConditionalCompletionClient>,
    existence_hints: Arc<super::existence_hints::ExistenceHints>,
    write_coordination: Arc<super::write_coordination::WriteCoordination>,
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) key_prefix: Option<String>,
}

async fn existing_object_outcome(
    store: &AmazonS3,
    location: &ObjectStorePath,
    integrity: &ObjectIntegrity,
) -> Result<PutOutcome, S3ObjectStoreError> {
    let result = store
        .get(location)
        .await
        .map_err(S3ObjectStoreError::External)?;
    // GET carries the length of the same object whose bytes are verified;
    // a separate HEAD adds a request and can observe a different revision.
    if result.meta.size != integrity.length() {
        return Err(S3ObjectStoreError::ExistingObjectConflict);
    }
    let mut stream = result.into_stream();
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(S3ObjectStoreError::External)?;
        length = length
            .checked_add(
                u64::try_from(chunk.len())
                    .map_err(|_error| S3ObjectStoreError::ExistingObjectConflict)?,
            )
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        if length > integrity.length() {
            return Err(S3ObjectStoreError::ExistingObjectConflict);
        }
        hasher.update(&chunk);
    }
    let hash = ShardlineHash::from_bytes(*hasher.finalize().as_bytes());
    if length != integrity.length() || hash != integrity.hash() {
        return Err(S3ObjectStoreError::ExistingObjectConflict);
    }
    Ok(PutOutcome::AlreadyExists)
}

impl fmt::Debug for S3ObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3ObjectStore")
            .field("inner", &"***")
            .field("runtime", &"shared")
            .field("key_prefix", &self.key_prefix)
            .finish()
    }
}

impl S3ObjectStore {
    /// Builds an S3-compatible object store adapter.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when configuration or runtime initialization
    /// fails.
    pub fn new(config: S3ObjectStoreConfig) -> Result<Self, S3ObjectStoreError> {
        if config.allow_http {
            eprintln!(
                "[shardline-s3] WARNING: allow_http is enabled for bucket '{}' — credentials and data will be transmitted over plaintext HTTP",
                config.bucket
            );
        }
        super::credentials::validate_s3_config(&config)?;
        let inner = super::client::build_amazon_s3_client(&config)?;
        let single_attempt_inner = super::client::build_single_attempt_client(&config)?;
        let conditional_completion_inner = Arc::new(
            super::client::ConditionalCompletionClient::new(config.clone()),
        );

        // Process-wide ownership keeps the bridge independent of the runtime in
        // which an adapter was constructed and avoids dropping a Tokio runtime
        // while inside another runtime. Worker threads are shared across stores.
        static SYNC_RUNTIME: Mutex<Option<Arc<Runtime>>> = Mutex::new(None);
        let mut shared = SYNC_RUNTIME
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let runtime = if let Some(runtime) = shared.as_ref() {
            Arc::clone(runtime)
        } else {
            // Retain only successful initialization. Temporary resource pressure
            // must not make every later adapter construction fail permanently.
            let runtime = Arc::new(
                Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_name("shardline-s3-object-store")
                    .enable_all()
                    .build()
                    .map_err(S3ObjectStoreError::Runtime)?,
            );
            *shared = Some(Arc::clone(&runtime));
            runtime
        };
        drop(shared);
        Ok(Self {
            inner,
            single_attempt_inner,
            conditional_completion_inner,
            existence_hints: Arc::default(),
            write_coordination: Arc::default(),
            runtime,
            key_prefix: config.key_prefix,
        })
    }

    async fn put_verified_bytes(
        &self,
        key: &ObjectKey,
        bytes: bytes::Bytes,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, S3ObjectStoreError> {
        let location = self.location_for_key(key)?;
        // A preceding writer can populate the hint before this writer probes.
        // Every waiter still verifies remote bytes; this is request ordering,
        // never a substitute for conditional creation or integrity checking.
        let ownership = self.write_coordination.acquire(key).await;
        if self.existence_hints.contains(key) {
            // Verification reads can overlap. Only uncertain creates need
            // ordering; serializing large dedup reads would hurt throughput.
            drop(ownership);
            match existing_object_outcome(&self.inner, &location, integrity).await {
                Ok(outcome) => return Ok(outcome),
                Err(S3ObjectStoreError::ExistingObjectConflict) => {
                    return Err(S3ObjectStoreError::ExistingObjectConflict);
                }
                // A hint may outlive deletion or a permission change. The
                // original conditional create remains authoritative, including
                // support for credentials that can create but cannot read.
                Err(_probe_error) => {}
            }
        }
        let outcome = match self
            .inner
            .put_opts(&location, bytes.into(), PutMode::Create.into())
            .await
        {
            Ok(_result) => Ok(PutOutcome::Inserted),
            Err(ExternalObjectStoreError::AlreadyExists { .. })
            | Err(ExternalObjectStoreError::Precondition { .. }) => {
                existing_object_outcome(&self.inner, &location, integrity).await
            }
            Err(error) => Err(S3ObjectStoreError::External(error)),
        };
        if outcome.is_ok() {
            self.existence_hints.remember(key);
        }
        outcome
    }

    /// Creates an immutable object and verifies its complete stored bytes.
    ///
    /// Existing objects retain the ordinary full integrity check. Newly created
    /// objects are read back before success, so callers can publish a packed
    /// container without first persisting and verifying its constituent chunks.
    /// Verification streams the response instead of retaining a second copy.
    ///
    /// # Errors
    ///
    /// Returns a storage or integrity error if creation or read-back fails.
    /// A failed read-back never establishes that the newly written object is
    /// safe to publish; it may remain as an unreachable immutable object.
    pub async fn put_if_absent_with_read_back(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, S3ObjectStoreError> {
        let outcome = AsyncObjectStore::put_if_absent(self, key, body, integrity).await?;
        if outcome == PutOutcome::Inserted {
            let location = self.location_for_key(key)?;
            existing_object_outcome(&self.inner, &location, integrity).await?;
        }
        Ok(outcome)
    }

    /// Sends one delete without transparently retrying an uncertain mutation.
    ///
    /// A successful response settles this request. An error may mean the object
    /// was deleted; the caller must preserve its durable pending-delete marker.
    /// Other operations retain their existing retry policy.
    ///
    /// # Errors
    ///
    /// Returns a key validation or remote failure, without retrying the delete.
    pub async fn delete_if_present_single_attempt(
        &self,
        key: &ObjectKey,
    ) -> Result<DeleteOutcome, S3ObjectStoreError> {
        let location = self.location_for_key(key)?;
        match self.single_attempt_inner.delete(&location).await {
            Ok(()) => Ok(DeleteOutcome::Deleted),
            Err(ExternalObjectStoreError::NotFound { .. }) => Ok(DeleteOutcome::NotFound),
            Err(error) => Err(S3ObjectStoreError::External(error)),
        }
    }

    fn block_on<T: Send>(
        &self,
        future: impl Future<Output = Result<T, ExternalObjectStoreError>> + Send,
    ) -> Result<T, S3ObjectStoreError> {
        self.block_on_result(async { future.await.map_err(S3ObjectStoreError::External) })
    }

    fn block_on_result<T: Send, FutureError: Send>(
        &self,
        future: impl Future<Output = Result<T, FutureError>> + Send,
    ) -> Result<T, FutureError>
    where
        S3ObjectStoreError: Into<FutureError>,
    {
        if let Ok(handle) = Handle::try_current() {
            if handle.runtime_flavor() == RuntimeFlavor::MultiThread {
                return block_in_place(|| self.runtime.block_on(future));
            }
            // A current-thread runtime cannot block_in_place or drive nested
            // block_on. A scoped thread can borrow the operation's inputs while
            // the independent shared runtime continues driving transport I/O.
            return std::thread::scope(|scope| {
                let worker = std::thread::Builder::new()
                    .name("shardline-s3-sync-bridge".to_owned())
                    .spawn_scoped(scope, || self.runtime.block_on(future))
                    .map_err(S3ObjectStoreError::Runtime)
                    .map_err(Into::into)?;
                worker.join().map_err(|_panic| {
                    S3ObjectStoreError::Runtime(IoError::other("synchronous S3 worker panicked"))
                        .into()
                })?
            });
        }
        self.runtime.block_on(future)
    }

    pub(crate) fn location_for_key(
        &self,
        key: &ObjectKey,
    ) -> Result<ObjectStorePath, S3ObjectStoreError> {
        let location = self.key_prefix.as_ref().map_or_else(
            || key.as_str().to_owned(),
            |prefix| format!("{prefix}/{}", key.as_str()),
        );
        ObjectStorePath::parse(location).map_err(S3ObjectStoreError::Path)
    }

    pub(crate) fn location_for_prefix(
        &self,
        prefix: &ObjectPrefix,
    ) -> Result<ObjectStorePath, S3ObjectStoreError> {
        let location = self.key_prefix.as_ref().map_or_else(
            || prefix.as_str().to_owned(),
            |key_prefix| {
                if prefix.as_str().is_empty() {
                    key_prefix.clone()
                } else {
                    format!("{key_prefix}/{}", prefix.as_str())
                }
            },
        );
        ObjectStorePath::parse(location).map_err(S3ObjectStoreError::Path)
    }

    /// Lists a bounded page of direct child objects under a flat namespace prefix.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the underlying object-store listing fails or a
    /// listed object cannot be represented as a validated direct child under `prefix`.
    pub fn list_flat_namespace_page(
        &self,
        prefix: &ObjectPrefix,
        start_after: Option<&ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, S3ObjectStoreError> {
        let location = self.location_for_prefix(prefix)?;
        let start_after = start_after
            .map(|key| self.location_for_key(key))
            .transpose()?;
        self.block_on_result(async {
            let mut listed = start_after.as_ref().map_or_else(
                || self.inner.list(Some(&location)),
                |start_after| self.inner.list_with_offset(Some(&location), start_after),
            );
            let mut metadata = Vec::with_capacity(limit);
            while metadata.len() < limit {
                let Some(entry) = listed
                    .try_next()
                    .await
                    .map_err(S3ObjectStoreError::External)?
                else {
                    break;
                };
                let item = self.metadata_from_external(&entry)?;
                if !item.key().as_str().starts_with(prefix.as_str()) {
                    continue;
                }
                let key_str = item.key().as_str();
                let Some(remainder) = key_str.strip_prefix(prefix.as_str()) else {
                    continue;
                };
                if remainder.is_empty() || remainder.contains('/') {
                    continue;
                }
                // Skip temp upload artifacts (e.g., "key.xorb.tmp.0.12345").
                if is_temp_upload_key(remainder) {
                    continue;
                }
                metadata.push(item);
            }
            Ok(metadata)
        })
    }

    /// Streams a validated byte range directly from S3-compatible storage.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the supplied range is invalid or the
    /// upstream object-store adapter fails the ranged get request.
    pub async fn stream_range(
        &self,
        key: &ObjectKey,
        range: ByteRange,
    ) -> Result<S3ByteStream, S3ObjectStoreError> {
        let location = self.location_for_key(key)?;
        let expected_range = validated_external_range(range)?;
        let result = self
            .inner
            .get_opts(
                &location,
                GetOptions::new().with_range(Some(expected_range.clone())),
            )
            .await
            .map_err(S3ObjectStoreError::External)?;

        stream_payload_for_range(result, expected_range)
    }

    /// Streams a range while checking the complete object length on that GET.
    /// No separate HEAD is needed; absence is reported as `None`.
    ///
    /// # Errors
    /// Returns an error for a length/range mismatch or a provider failure.
    pub async fn stream_range_if_present(
        &self,
        key: &ObjectKey,
        range: ByteRange,
        expected_length: u64,
    ) -> Result<Option<S3ByteStream>, S3ObjectStoreError> {
        let location = self.location_for_key(key)?;
        let expected_range = validated_external_range(range)?;
        let result = match self
            .inner
            .get_opts(
                &location,
                GetOptions::new().with_range(Some(expected_range.clone())),
            )
            .await
        {
            Ok(result) => result,
            Err(ExternalObjectStoreError::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(S3ObjectStoreError::External(error)),
        };
        if result.meta.size != expected_length {
            return Err(S3ObjectStoreError::IntegrityLengthMismatch);
        }
        stream_payload_for_range(result, expected_range).map(Some)
    }

    /// Copies a byte range into a file with one streaming GET.
    ///
    /// Uses the same runtime bridge as other synchronous storage operations.
    /// The response is consumed one transport frame at a time, without collecting
    /// the object in memory. Callers must discard the destination on failure.
    ///
    /// # Errors
    ///
    /// Returns an error when the GET or file write fails, or the response does
    /// not contain exactly the requested number of bytes.
    pub fn copy_range_to_file(
        &self,
        key: &ObjectKey,
        range: ByteRange,
        output: &mut File,
    ) -> Result<(), S3ObjectStoreError> {
        let expected = range.len().ok_or(S3ObjectStoreError::RangeOutOfBounds)?;
        self.block_on_result(async {
            let mut stream = self.stream_range(key, range).await?;
            let mut written = 0_u64;
            while let Some(bytes) = stream.next().await {
                let bytes = bytes?;
                let length = u64::try_from(bytes.len())
                    .map_err(|_error| S3ObjectStoreError::RangeOutOfBounds)?;
                written = written
                    .checked_add(length)
                    .filter(|total| *total <= expected)
                    .ok_or(S3ObjectStoreError::RangeOutOfBounds)?;
                std::io::Write::write_all(output, &bytes)?;
            }
            if written != expected {
                return Err(S3ObjectStoreError::RangeOutOfBounds);
            }
            Ok(())
        })
    }

    /// Copies a complete object into a file with one GET, returning `None` when
    /// the key does not exist. The response metadata and bytes belong to the
    /// same GET, so callers do not need a separate HEAD that could observe a
    /// different object state.
    ///
    /// # Errors
    ///
    /// Returns an error when the GET or file write fails, or the streamed body
    /// length disagrees with the length reported by that GET.
    pub fn copy_object_to_file_if_present(
        &self,
        key: &ObjectKey,
        output: &mut File,
    ) -> Result<Option<u64>, S3ObjectStoreError> {
        let location = self.location_for_key(key)?;
        self.block_on_result(async {
            let result = match self.inner.get(&location).await {
                Ok(result) => result,
                Err(ExternalObjectStoreError::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(S3ObjectStoreError::External(error)),
            };
            let expected_length = result.meta.size;
            let mut written = 0_u64;
            let mut stream = stream_payload_for_range(result, 0..expected_length)?;
            while let Some(bytes) = stream.next().await {
                let bytes = bytes?;
                written = written
                    .checked_add(
                        u64::try_from(bytes.len())
                            .map_err(|_error| S3ObjectStoreError::IntegrityLengthMismatch)?,
                    )
                    .filter(|length| *length <= expected_length)
                    .ok_or(S3ObjectStoreError::IntegrityLengthMismatch)?;
                std::io::Write::write_all(output, &bytes).map_err(S3ObjectStoreError::Io)?;
            }
            if written != expected_length {
                return Err(S3ObjectStoreError::IntegrityLengthMismatch);
            }
            Ok(Some(written))
        })
    }

    /// Reads an object into memory only when the response declares it is within
    /// `maximum_memory_length`; larger objects are streamed to `output` using
    /// the same GET. Returns the observed length and either the bounded bytes or
    /// `None` when the object was written to the file.
    ///
    /// This lets callers avoid scratch-file payload writes for small objects
    /// without a size-probe request or a second GET for large objects.
    ///
    /// # Errors
    ///
    /// Returns an error when the GET fails or the streamed length disagrees
    /// with response metadata.
    pub fn copy_object_to_writer_if_present<W: Write + Send>(
        &self,
        key: &ObjectKey,
        output: &mut W,
    ) -> Result<Option<u64>, S3ObjectStoreError> {
        let location = self.location_for_key(key)?;
        self.block_on_result(async {
            let result = match self.inner.get(&location).await {
                Ok(result) => result,
                Err(ExternalObjectStoreError::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(S3ObjectStoreError::External(error)),
            };
            let expected_length = result.meta.size;
            let mut written = 0_u64;
            let mut stream = stream_payload_for_range(result, 0..expected_length)?;
            while let Some(bytes) = stream.next().await {
                let bytes = bytes?;
                written = written
                    .checked_add(
                        u64::try_from(bytes.len())
                            .map_err(|_error| S3ObjectStoreError::IntegrityLengthMismatch)?,
                    )
                    .filter(|length| *length <= expected_length)
                    .ok_or(S3ObjectStoreError::IntegrityLengthMismatch)?;
                output.write_all(&bytes).map_err(S3ObjectStoreError::Io)?;
            }
            if written != expected_length {
                return Err(S3ObjectStoreError::IntegrityLengthMismatch);
            }
            Ok(Some(expected_length))
        })
    }

    /// Reads a bounded object with one full GET, returning `None` when the key
    /// does not exist. The response metadata and bytes belong to the same GET,
    /// avoiding a separate HEAD request.
    ///
    /// # Errors
    ///
    /// Returns an error when the GET fails, the object exceeds `maximum_length`,
    /// or the streamed body length disagrees with the GET response metadata.
    pub fn read_object_if_present(
        &self,
        key: &ObjectKey,
        maximum_length: u64,
    ) -> Result<Option<(Vec<u8>, u64)>, S3ObjectReadError> {
        let location = self.location_for_key(key)?;
        self.block_on_result(async {
            let result = match self.inner.get(&location).await {
                Ok(result) => result,
                Err(ExternalObjectStoreError::NotFound { .. }) => return Ok(None),
                Err(error) => {
                    return Err(S3ObjectStoreError::External(error).into());
                }
            };
            let expected_length = result.meta.size;
            if expected_length > maximum_length {
                return Err(S3ObjectReadError::ObjectTooLarge);
            }
            let capacity = usize::try_from(expected_length)
                .map_err(|_error| S3ObjectReadError::ObjectTooLarge)?;
            let mut output = Vec::with_capacity(capacity);
            let mut stream = stream_payload_for_range(result, 0..expected_length)?;
            while let Some(bytes) = stream.next().await {
                let bytes = bytes?;
                let next_length = output
                    .len()
                    .checked_add(bytes.len())
                    .ok_or(S3ObjectStoreError::IntegrityLengthMismatch)?;
                if next_length > capacity {
                    return Err(S3ObjectStoreError::IntegrityLengthMismatch.into());
                }
                output.extend_from_slice(&bytes);
            }
            if output.len() != capacity {
                return Err(S3ObjectStoreError::IntegrityLengthMismatch.into());
            }
            Ok(Some((output, expected_length)))
        })
    }

    pub(crate) fn metadata_from_external(
        &self,
        metadata: &object_store::ObjectMeta,
    ) -> Result<ObjectMetadata, S3ObjectStoreError> {
        let raw_key = metadata.location.as_ref();
        let key = if let Some(prefix) = &self.key_prefix {
            let prefix = format!("{prefix}/");
            raw_key
                .strip_prefix(&prefix)
                .ok_or(S3ObjectStoreError::InvalidListedKey)?
        } else {
            raw_key
        };
        let key = ObjectKey::parse(key).map_err(|_error| S3ObjectStoreError::InvalidListedKey)?;
        // The S3 LastModified is observed by the backend, so it is immune to
        // writer/GC wall-clock divergence; attach it when available.
        let backend_modified_unix_nanos = metadata
            .last_modified
            .timestamp_nanos_opt()
            .and_then(|nanos| u64::try_from(nanos).ok());
        let mut metadata = ObjectMetadata::new(key, metadata.size, None);
        if let Some(modified_unix_nanos) = backend_modified_unix_nanos {
            metadata = metadata.with_modified(modified_unix_nanos);
        }
        Ok(metadata)
    }

    /// Stores bytes at a key, replacing any existing object.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when integrity validation or the overwrite
    /// operation fails.
    pub fn put_overwrite(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<(), S3ObjectStoreError> {
        verify_integrity(body.as_slice(), integrity)?;
        let location = self.location_for_key(key)?;
        let bytes = body.into_bytes();

        // Write to a reserved temp key first to avoid destroying the existing
        // object on partial multipart failure. Only copy to the live key after
        // the new content is fully durable. The reserved `__tmp/` namespace
        // keeps the temp key from ever colliding with (or shadowing) a user
        // key (F-80).
        let temp_key = super::temp_key_for(key)?;
        let temp_location = self.location_for_key(&temp_key)?;
        self.block_on(
            self.inner
                .put_opts(&temp_location, bytes.into(), PutMode::Create.into()),
        )?;

        // Atomically replace the live key with the temp content via copy,
        // then remove the temp key.
        let result = self
            .block_on(self.inner.copy(&temp_location, &location))
            .inspect_err(|_error| {
                // Best-effort cleanup of temp object
                self.block_on(self.inner.delete(&temp_location)).ok();
            });
        self.block_on(self.inner.delete(&temp_location)).ok();
        result
    }

    /// Streams a caller-validated local file into S3-compatible storage if the destination
    /// key is absent.
    ///
    /// # Concurrency
    ///
    /// Multipart completion atomically requires the final key to be absent.
    /// If another writer wins, compare its bytes against the local file before
    /// accepting an identical replay. No partial object becomes visible.
    ///
    /// # Errors
    ///
    /// Callers are expected to have already validated the file hash before invoking this
    /// method. The S3 adapter rechecks file length up front and fully compares against an
    /// existing destination object on conflict.
    ///
    /// Returns [`S3ObjectStoreError`] when the local file length does not match the
    /// supplied integrity metadata, multipart upload fails, or an existing destination
    /// object conflicts with the file contents.
    pub fn put_file_if_absent(
        &self,
        key: &ObjectKey,
        path: &Path,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, S3ObjectStoreError> {
        if let Some(outcome) = existing_object_outcome_from_file(self, key, path, integrity)? {
            return Ok(outcome);
        }

        let location = self.location_for_key(key)?;
        match self.stream_file_if_absent(&location, path, integrity.length()) {
            Ok(()) => Ok(PutOutcome::Inserted),
            Err(S3ObjectStoreError::External(ExternalObjectStoreError::AlreadyExists {
                ..
            }))
            | Err(S3ObjectStoreError::External(ExternalObjectStoreError::Precondition {
                ..
            })) => existing_object_outcome_from_file(self, key, path, integrity)?
                .ok_or(S3ObjectStoreError::ExistingObjectConflict),
            Err(error) => Err(error),
        }
    }

    /// Streams a caller-validated content-addressed local file directly into an
    /// unpublished multipart upload and conditionally publishes the final key.
    ///
    /// This path is intended for immutable digest-addressed objects, where concurrent
    /// writers for the same key can only be writing the same bytes. Conditional
    /// completion preserves competing writers; existing bytes are compared.
    ///
    /// # Errors
    ///
    /// Callers are expected to have already validated the file hash before invoking this
    /// method. The S3 adapter rechecks file length up front and fully compares against an
    /// existing destination object on conflict.
    ///
    /// Returns [`S3ObjectStoreError`] when the local file length does not match the
    /// supplied integrity metadata, an existing destination object conflicts with the
    /// file contents, or the multipart upload fails.
    pub fn put_content_addressed_file(
        &self,
        key: &ObjectKey,
        path: &Path,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, S3ObjectStoreError> {
        self.put_file_if_absent(key, path, integrity)
    }

    /// Copies an existing object to a new key if the destination key is absent.
    ///
    /// This uses the S3-compatible provider's server-side copy path instead of reading
    /// the full source object back into process memory.
    ///
    /// # Concurrency
    ///
    /// Both the server-side copy and the large-object streaming fallback use
    /// conditional multipart completion, so a competing destination writer is
    /// protected atomically at publication. An existing destination is compared
    /// with the source before returning `AlreadyExists`. Sources must remain
    /// immutable during copying and comparison, as in content-addressed usage.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the source is missing, the destination
    /// conflicts with different bytes, or the underlying copy operation fails.
    pub fn copy_object_if_absent(
        &self,
        source: &ObjectKey,
        destination: &ObjectKey,
    ) -> Result<PutOutcome, S3ObjectStoreError> {
        if source == destination {
            return if ObjectStore::metadata(self, source)?.is_some() {
                Ok(PutOutcome::AlreadyExists)
            } else {
                Err(S3ObjectStoreError::External(
                    ExternalObjectStoreError::NotFound {
                        path: source.as_str().to_owned(),
                        source: Box::new(IoError::from(std::io::ErrorKind::NotFound)),
                    },
                ))
            };
        }

        let source_location = self.location_for_key(source)?;
        let destination_location = self.location_for_key(destination)?;
        // Try the conditional server-side copy first. A source HEAD on every
        // successful copy adds a request and can race with the copy itself.
        // The copy operation reports a missing source and atomically protects
        // the destination, so metadata is only needed on failure: to compare
        // an existing destination, or to select the >5 GiB streaming fallback.
        match self.block_on(self.inner.copy_opts(
            &source_location,
            &destination_location,
            CopyOptions::new().with_mode(CopyMode::Create),
        )) {
            Ok(()) => Ok(PutOutcome::Inserted),
            Err(S3ObjectStoreError::External(ExternalObjectStoreError::AlreadyExists {
                ..
            }))
            | Err(S3ObjectStoreError::External(ExternalObjectStoreError::Precondition {
                ..
            })) => {
                let source_len = self.copy_source_length(source)?;
                existing_copy_outcome(self, source, destination, source_len)
            }
            Err(error) => {
                let source_len = match ObjectStore::metadata(self, source) {
                    Ok(Some(metadata)) => metadata.length(),
                    Ok(None) => return Err(missing_copy_source(source)),
                    // Keep the original copy failure when the follow-up
                    // metadata request cannot explain it.
                    Err(_) => return Err(error),
                };

                // S3-compatible providers report CopyObject/UploadPartCopy
                // size-limit failures differently. The source length is the
                // provider-independent signal for selecting the established
                // streaming multipart path.
                if source_len > MAX_SINGLE_COPY_BYTES {
                    self.block_on_result(self.streaming_large_copy(
                        &source_location,
                        &destination_location,
                        source,
                        destination,
                        source_len,
                    ))
                } else {
                    Err(error)
                }
            }
        }
    }

    fn copy_source_length(&self, source: &ObjectKey) -> Result<u64, S3ObjectStoreError> {
        ObjectStore::metadata(self, source)?
            .map(|metadata| metadata.length())
            .ok_or_else(|| missing_copy_source(source))
    }

    /// Copies a large object from `source_location` to `destination_location`
    /// by streaming the content through the server.  Used when the source exceeds
    /// S3's single-part COPY limit (5 GiB).
    async fn streaming_large_copy(
        &self,
        source_location: &ObjectStorePath,
        destination_location: &ObjectStorePath,
        source: &ObjectKey,
        destination: &ObjectKey,
        source_len: u64,
    ) -> Result<PutOutcome, S3ObjectStoreError> {
        // Fast check: if the destination already exists, compare content.
        if let Some(_dest_meta) = ObjectStore::metadata(self, destination)? {
            return existing_copy_outcome(self, source, destination, source_len);
        }

        let store = self.inner.clone();
        let src = source_location.clone();
        let dst = destination_location.clone();
        let len = source_len;

        // Initialize before creating remote state so a local client-construction
        // failure cannot leave an unpublished multipart upload behind.
        let conditional_completion = self.conditional_completion_inner.get().await?;

        let upload_id = store
            .create_multipart(&dst)
            .await
            .map_err(S3ObjectStoreError::External)?;

        let mut offset = 0_u64;
        let mut part_idx = 0_usize;
        let mut part_ids = Vec::new();

        let result: Result<(), S3ObjectStoreError> = async {
            while offset < len {
                let chunk_end = offset.saturating_add(LARGE_COPY_CHUNK_BYTES).min(len);
                let chunk = store
                    .get_range(&src, offset..chunk_end)
                    .await
                    .map_err(S3ObjectStoreError::External)?;
                let payload = PutPayload::from_bytes(chunk);
                let part_id = store
                    .put_part(&dst, &upload_id, part_idx, payload)
                    .await
                    .map_err(S3ObjectStoreError::External)?;
                part_ids.push(part_id);
                part_idx = part_idx.wrapping_add(1);
                offset = chunk_end;
            }
            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                // The provider must enforce absence atomically at publication.
                // Another HEAD here cannot fence a competing node's later PUT.
                // This dedicated client adds If-None-Match only to completion,
                // retaining the dependency's signing, XML parsing and retries.
                match conditional_completion
                    .complete_multipart(&dst, &upload_id, part_ids)
                    .await
                {
                    Ok(_) => Ok(PutOutcome::Inserted),
                    Err(
                        ExternalObjectStoreError::AlreadyExists { .. }
                        | ExternalObjectStoreError::Precondition { .. },
                    ) => {
                        store.abort_multipart(&dst, &upload_id).await.ok();
                        existing_copy_outcome(self, source, destination, source_len)
                    }
                    Err(error) => {
                        store.abort_multipart(&dst, &upload_id).await.ok();
                        Err(S3ObjectStoreError::External(error))
                    }
                }
            }
            Err(error) => {
                store.abort_multipart(&dst, &upload_id).await.ok();
                Err(error)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn delete_location_if_present(
        &self,
        location: &ObjectStorePath,
    ) -> Result<(), S3ObjectStoreError> {
        match self.block_on(self.inner.delete(location)) {
            Ok(()) => Ok(()),
            Err(S3ObjectStoreError::External(ExternalObjectStoreError::NotFound { .. })) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Reaps S3 temp-upload artifacts whose observed `LastModified` is older
    /// than `S3_TEMP_ARTIFACT_AGE_SECONDS`.
    ///
    /// S3 temp keys (`__tmp/shardline-overwrite/<key-digest>.<counter>.<pid>.<nanos>`
    /// and `__tmp/shardline-stream-upload/<nanos>-<pid>-<counter>`) are
    /// normally deleted immediately after promotion, but a crash between the
    /// temp write and the copy/delete strands them forever (F-48). The normal
    /// listing paths skip temp keys, so this sweep enumerates the reserved `__tmp/` subtree
    /// and deletes only keys matching the exact generated-temp grammars
    /// (`is_temp_upload_key`) whose `LastModified` — S3 backend truth, immune
    /// to writer/GC wall-clock divergence — is older than the age bound. User
    /// objects are never matched (both grammars live in the reserved `__tmp/`
    /// namespace user keys cannot reach; F-80). Returns
    /// `(reaped_count, reaped_bytes)`.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the store cannot be enumerated or a
    /// delete fails.
    pub fn sweep_stale_temp_keys(
        &self,
        now_unix_seconds: u64,
    ) -> Result<(usize, u64), S3ObjectStoreError> {
        let cutoff_unix_nanos =
            u128::from(now_unix_seconds.saturating_sub(S3_TEMP_ARTIFACT_AGE_SECONDS))
                * 1_000_000_000;
        let prefix =
            ObjectPrefix::parse("__tmp/").map_err(|_error| S3ObjectStoreError::InvalidListedKey)?;
        let location = self.location_for_prefix(&prefix)?;
        self.block_on_result(async {
            let mut listed = self.inner.list(Some(&location));
            let mut reaped = 0_usize;
            let mut reaped_bytes = 0_u64;
            let mut batch = Vec::with_capacity(temp_cleanup::BATCH_SIZE);
            while let Some(entry) = listed
                .try_next()
                .await
                .map_err(S3ObjectStoreError::External)?
            {
                let metadata = self.metadata_from_external(&entry)?;
                if !is_temp_upload_key(metadata.key().as_str()) {
                    continue;
                }
                // Age bound on the S3-observed LastModified (backend truth). A
                // backend exposing no mtime keeps the key — never risk reaping
                // a live in-flight artifact.
                let Some(modified_unix_nanos) = metadata.modified_unix_nanos() else {
                    continue;
                };
                if u128::from(modified_unix_nanos) >= cutoff_unix_nanos {
                    continue;
                }
                let temp_location = self.location_for_key(metadata.key())?;
                batch.push(temp_location);
                if batch.len() == temp_cleanup::BATCH_SIZE {
                    temp_cleanup::delete_batch(
                        &self.inner,
                        &self.single_attempt_inner,
                        std::mem::take(&mut batch),
                    )
                    .await?;
                }
                reaped = reaped.saturating_add(1);
                reaped_bytes = reaped_bytes.saturating_add(metadata.length());
            }
            temp_cleanup::delete_batch(&self.inner, &self.single_attempt_inner, batch).await?;
            Ok((reaped, reaped_bytes))
        })
    }
}

impl ObjectStore for S3ObjectStore {
    type Error = S3ObjectStoreError;

    /// Stores an object if no identical object exists yet.
    ///
    /// New keys use conditional create. Recently successful keys first verify
    /// the current remote bytes, avoiding a rejected write for deduplication.
    /// Hints are bounded and never replace length and digest verification.
    /// A failed probe falls back to the original conditional create.
    fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error> {
        verify_integrity(body.as_slice(), integrity)?;
        self.block_on_result(self.put_verified_bytes(key, body.into_bytes(), integrity))
    }

    fn read_range(&self, key: &ObjectKey, range: ByteRange) -> Result<Vec<u8>, Self::Error> {
        let location = self.location_for_key(key)?;
        let external_range = validated_external_range(range)?;
        let result = self.block_on(self.inner.get_opts(
            &location,
            GetOptions::new().with_range(Some(external_range.clone())),
        ))?;
        if result.range != external_range {
            return Err(S3ObjectStoreError::RangeOutOfBounds);
        }
        let bytes = self.block_on(async {
            let mut acc = Vec::new();
            let mut stream = result.into_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                acc.extend_from_slice(&chunk);
            }
            Ok::<_, ExternalObjectStoreError>(acc)
        })?;
        Ok(bytes)
    }

    fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
        ObjectStore::metadata(self, key).map(|metadata| metadata.is_some())
    }

    fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
        let location = self.location_for_key(key)?;
        match self.block_on(self.inner.head(&location)) {
            Ok(metadata) => self.metadata_from_external(&metadata).map(Some),
            Err(S3ObjectStoreError::External(ExternalObjectStoreError::NotFound { .. })) => {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error> {
        let mut metadata = Vec::new();
        ObjectStore::visit_prefix(self, prefix, |entry| {
            metadata.push(entry);
            Ok::<(), S3ObjectStoreError>(())
        })?;
        metadata.sort_by(|left, right| left.key().as_str().cmp(right.key().as_str()));
        Ok(metadata)
    }

    fn visit_prefix<Visitor, VisitorError>(
        &self,
        prefix: &ObjectPrefix,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(ObjectMetadata) -> Result<(), VisitorError>,
    {
        let location = self.location_for_prefix(prefix).map_err(Into::into)?;
        let mut listed = self.inner.list(Some(&location));
        if Handle::try_current()
            .is_ok_and(|handle| handle.runtime_flavor() != RuntimeFlavor::MultiThread)
        {
            while let Some(entry) = self.block_on(listed.try_next()).map_err(Into::into)? {
                let metadata = self.metadata_from_external(&entry).map_err(Into::into)?;
                // Keep callbacks on the caller's thread: the synchronous visitor
                // contract permits non-Send state and errors.
                if !is_temp_upload_key(metadata.key().as_str()) {
                    visitor(metadata)?;
                }
            }
            return Ok(());
        }
        // Outside a runtime and in multi-thread runtimes, drive the whole scan
        // once on the caller's thread instead of entering an executor per item.
        let operation = async {
            while let Some(entry) = listed
                .try_next()
                .await
                .map_err(S3ObjectStoreError::External)
                .map_err(Into::into)?
            {
                let metadata = self.metadata_from_external(&entry).map_err(Into::into)?;
                if !is_temp_upload_key(metadata.key().as_str()) {
                    visitor(metadata)?;
                }
            }
            Ok(())
        };
        if Handle::try_current().is_ok() {
            block_in_place(|| self.runtime.block_on(operation))
        } else {
            self.runtime.block_on(operation)
        }
    }

    fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
        let location = self.location_for_key(key)?;
        match self.block_on(self.inner.delete(&location)) {
            Ok(()) => Ok(DeleteOutcome::Deleted),
            Err(S3ObjectStoreError::External(ExternalObjectStoreError::NotFound { .. })) => {
                Ok(DeleteOutcome::NotFound)
            }
            Err(error) => Err(error),
        }
    }
}

#[async_trait]
impl AsyncObjectStore for S3ObjectStore {
    type Error = S3ObjectStoreError;

    async fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error> {
        verify_integrity(body.as_slice(), integrity)?;
        let store = self.clone();
        let key = key.clone();
        let integrity = *integrity;
        let bytes = body.into_bytes();
        super::async_write::run(&self.runtime, async move {
            store.put_verified_bytes(&key, bytes, &integrity).await
        })
        .await
    }

    async fn read_range(&self, key: &ObjectKey, range: ByteRange) -> Result<Vec<u8>, Self::Error> {
        let location = self.location_for_key(key)?;
        let external_range = validated_external_range(range)?;
        let result = self
            .inner
            .get_opts(
                &location,
                GetOptions::new().with_range(Some(external_range.clone())),
            )
            .await
            .map_err(S3ObjectStoreError::External)?;
        if result.range != external_range {
            return Err(S3ObjectStoreError::RangeOutOfBounds);
        }
        let expected_length = external_range
            .end
            .checked_sub(external_range.start)
            .ok_or(S3ObjectStoreError::RangeOutOfBounds)?;
        let mut acc = Vec::new();
        let mut stream = result.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(S3ObjectStoreError::External)?;
            let next_length = acc
                .len()
                .checked_add(chunk.len())
                .and_then(|length| u64::try_from(length).ok())
                .ok_or(S3ObjectStoreError::RangeOutOfBounds)?;
            if next_length > expected_length {
                return Err(S3ObjectStoreError::RangeOutOfBounds);
            }
            acc.extend_from_slice(&chunk);
        }
        if u64::try_from(acc.len()).map_err(|_error| S3ObjectStoreError::RangeOutOfBounds)?
            != expected_length
        {
            return Err(S3ObjectStoreError::RangeOutOfBounds);
        }
        Ok(acc)
    }

    async fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
        AsyncObjectStore::metadata(self, key)
            .await
            .map(|metadata| metadata.is_some())
    }

    async fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
        let location = self.location_for_key(key)?;
        match self.inner.head(&location).await {
            Ok(meta) => self.metadata_from_external(&meta).map(Some),
            Err(ExternalObjectStoreError::NotFound { .. }) => Ok(None),
            Err(error) => Err(S3ObjectStoreError::External(error)),
        }
    }

    async fn list_prefix(&self, prefix: &ObjectPrefix) -> Result<Vec<ObjectMetadata>, Self::Error> {
        let mut metadata = Vec::new();
        AsyncObjectStore::visit_prefix(self, prefix, |entry| {
            metadata.push(entry);
            Ok::<(), S3ObjectStoreError>(())
        })
        .await?;
        metadata.sort_by(|left, right| left.key().as_str().cmp(right.key().as_str()));
        Ok(metadata)
    }

    async fn visit_prefix<Visitor, VisitorError>(
        &self,
        prefix: &ObjectPrefix,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(ObjectMetadata) -> Result<(), VisitorError> + Send,
    {
        let location = self.location_for_prefix(prefix).map_err(Into::into)?;
        let mut listed = self.inner.list(Some(&location));
        while let Some(entry) = listed
            .try_next()
            .await
            .map_err(S3ObjectStoreError::External)
            .map_err(Into::into)?
        {
            let meta = self.metadata_from_external(&entry).map_err(Into::into)?;
            // Skip temp upload artifacts.
            if is_temp_upload_key(meta.key().as_str()) {
                continue;
            }
            visitor(meta)?;
        }

        Ok(())
    }

    async fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
        let location = self.location_for_key(key)?;
        match self.inner.delete(&location).await {
            Ok(()) => Ok(DeleteOutcome::Deleted),
            Err(ExternalObjectStoreError::NotFound { .. }) => Ok(DeleteOutcome::NotFound),
            Err(error) => Err(S3ObjectStoreError::External(error)),
        }
    }
}

pub(crate) fn validated_external_range(range: ByteRange) -> Result<Range<u64>, S3ObjectStoreError> {
    let Some(length) = range.len() else {
        return Err(S3ObjectStoreError::RangeOutOfBounds);
    };
    let end_exclusive = range
        .start()
        .checked_add(length)
        .ok_or(S3ObjectStoreError::RangeOutOfBounds)?;

    Ok(range.start()..end_exclusive)
}

pub(crate) fn stream_payload_for_range(
    result: GetResult,
    expected_range: Range<u64>,
) -> Result<S3ByteStream, S3ObjectStoreError> {
    if result.range != expected_range {
        return Err(S3ObjectStoreError::RangeOutOfBounds);
    }

    Ok(Box::pin(
        result.into_stream().map_err(S3ObjectStoreError::External),
    ))
}

pub(crate) fn verify_integrity(
    bytes: &[u8],
    integrity: &ObjectIntegrity,
) -> Result<(), S3ObjectStoreError> {
    let body_length =
        u64::try_from(bytes.len()).map_err(|_error| S3ObjectStoreError::IntegrityLengthMismatch)?;
    if body_length != integrity.length() {
        return Err(S3ObjectStoreError::IntegrityLengthMismatch);
    }

    let actual = chunk_hash(bytes);
    if actual != integrity.hash() {
        return Err(S3ObjectStoreError::IntegrityHashMismatch);
    }

    Ok(())
}

pub(crate) fn existing_object_outcome_from_file(
    store: &S3ObjectStore,
    key: &ObjectKey,
    path: &Path,
    integrity: &ObjectIntegrity,
) -> Result<Option<PutOutcome>, S3ObjectStoreError> {
    verify_file_length(path, integrity)?;
    let location = store.location_for_key(key)?;
    // Compare the complete response with the verified local file, retaining
    // the released single full-GET path even for providers without range support.
    let result = match store.block_on(store.inner.get(&location)) {
        Ok(result) => result,
        Err(S3ObjectStoreError::External(ExternalObjectStoreError::NotFound { .. })) => {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    file_compare::compare_existing_file(store, result, path, integrity.length()).map(Some)
}

fn missing_copy_source(source: &ObjectKey) -> S3ObjectStoreError {
    S3ObjectStoreError::External(ExternalObjectStoreError::NotFound {
        path: source.as_str().to_owned(),
        source: Box::new(IoError::from(std::io::ErrorKind::NotFound)),
    })
}

pub(crate) fn existing_copy_outcome(
    store: &S3ObjectStore,
    source: &ObjectKey,
    destination: &ObjectKey,
    source_length: u64,
) -> Result<PutOutcome, S3ObjectStoreError> {
    if source_length == 0 {
        return match ObjectStore::metadata(store, destination)? {
            Some(metadata) if metadata.length() == 0 => Ok(PutOutcome::AlreadyExists),
            _ => Err(S3ObjectStoreError::ExistingObjectConflict),
        };
    }

    copy_compare::compare_existing_copy(store, source, destination, source_length)
}

pub(crate) fn verify_file_length(
    path: &Path,
    integrity: &ObjectIntegrity,
) -> Result<(), S3ObjectStoreError> {
    let metadata = std::fs::metadata(path).map_err(S3ObjectStoreError::Io)?;
    if metadata.len() != integrity.length() {
        return Err(S3ObjectStoreError::IntegrityLengthMismatch);
    }
    Ok(())
}

pub(crate) fn chunk_hash(bytes: &[u8]) -> ShardlineHash {
    let digest = blake3::hash(bytes);
    ShardlineHash::from_bytes(*digest.as_bytes())
}

#[cfg(test)]
pub(crate) fn temporary_upload_location(key_prefix: &Option<String>) -> ObjectStorePath {
    use super::types::TEMP_UPLOAD_COUNTER;
    use std::{
        sync::atomic::Ordering,
        time::{SystemTime, UNIX_EPOCH},
    };
    let counter = TEMP_UPLOAD_COUNTER.fetch_add(1, Ordering::Relaxed);
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    let relative = format!(
        "__tmp/shardline-stream-upload/{unix_nanos}-{}-{counter}",
        std::process::id()
    );
    let path = key_prefix
        .as_ref()
        .map_or_else(|| relative.clone(), |prefix| format!("{prefix}/{relative}"));
    ObjectStorePath::from(path)
}
