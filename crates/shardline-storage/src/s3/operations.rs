use std::{
    fmt,
    fs::File,
    future::Future,
    io::{Error as IoError, Read},
    ops::Range,
    path::Path,
    sync::{Arc, atomic::Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures_util::{StreamExt, TryStreamExt};
use object_store::{
    CopyMode, CopyOptions, Error as ExternalObjectStoreError, GetOptions, GetResult,
    ObjectStore as ExternalObjectStore, ObjectStoreExt, PutMode, PutPayload, WriteMultipart,
    aws::AmazonS3, multipart::MultipartStore, path::Path as ObjectStorePath,
};
use shardline_protocol::{ByteRange, ShardlineHash};
use tokio::{
    fs::File as TokioFile,
    io::AsyncReadExt,
    runtime::{Builder, Handle, Runtime},
    task::block_in_place,
};

use crate::{
    AsyncObjectStore, DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata,
    ObjectPrefix, ObjectStore, PutOutcome,
};

use super::{
    LARGE_COPY_CHUNK_BYTES, MAX_SINGLE_COPY_BYTES, S3ByteStream, S3ObjectStoreConfig,
    S3ObjectStoreError, STREAM_UPLOAD_CHUNK_BYTES, TEMP_UPLOAD_COUNTER, is_temp_upload_key,
};

/// S3-compatible implementation of [`ObjectStore`].
#[derive(Clone)]
pub struct S3ObjectStore {
    pub(crate) inner: AmazonS3,
    pub(crate) runtime: Option<Arc<Runtime>>,
    pub(crate) key_prefix: Option<String>,
}

impl fmt::Debug for S3ObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3ObjectStore")
            .field("inner", &"***")
            .field(
                "runtime",
                &self.runtime.as_ref().map(|_runtime| "configured"),
            )
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
        super::credentials::validate_s3_config(&config)?;
        let inner = super::client::build_amazon_s3_client(&config)?;

        let runtime = if Handle::try_current().is_ok() {
            None
        } else {
            Some(Arc::new(
                Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_name("shardline-s3-object-store")
                    .enable_all()
                    .build()
                    .map_err(S3ObjectStoreError::Runtime)?,
            ))
        };
        Ok(Self {
            inner,
            runtime,
            key_prefix: config.key_prefix,
        })
    }

    fn block_on<T>(
        &self,
        future: impl Future<Output = Result<T, ExternalObjectStoreError>>,
    ) -> Result<T, S3ObjectStoreError> {
        if let Ok(handle) = Handle::try_current() {
            return block_in_place(|| handle.block_on(future))
                .map_err(S3ObjectStoreError::External);
        }

        let runtime = self
            .runtime
            .as_ref()
            .ok_or(S3ObjectStoreError::RuntimeUnavailable)?;
        runtime
            .block_on(future)
            .map_err(S3ObjectStoreError::External)
    }

    fn block_on_result<T, FutureError>(
        &self,
        future: impl Future<Output = Result<T, FutureError>>,
    ) -> Result<T, FutureError>
    where
        S3ObjectStoreError: Into<FutureError>,
    {
        if let Ok(handle) = Handle::try_current() {
            return block_in_place(|| handle.block_on(future));
        }

        let runtime = self
            .runtime
            .as_ref()
            .ok_or(S3ObjectStoreError::RuntimeUnavailable)
            .map_err(Into::into)?;
        runtime.block_on(future)
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
        Ok(ObjectMetadata::new(key, metadata.size, None))
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

        // Write to a temp key first to avoid destroying the existing object
        // on partial multipart failure. Only copy to the live key after the
        // new content is fully durable.
        let counter = TEMP_UPLOAD_COUNTER.fetch_add(1, Ordering::Relaxed);
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let pid = std::process::id();
        let temp_suffix = format!("tmp.{counter}.{pid}.{now_nanos}");
        let temp_key = ObjectKey::parse(&format!("{}.{temp_suffix}", key.as_str()))
            .map_err(|_err| S3ObjectStoreError::InvalidListedKey)?;
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
    /// # TOCTOU Race Window
    ///
    /// This method has a two-stage TOCTOU window.  First, the `metadata()` probe
    /// checks whether the destination key exists.  Second, the temporary
    /// upload is copied to the final key with `CopyMode::Create`.
    /// Between these two points, a concurrent writer may insert an object at the
    /// same destination key.  The `CopyMode::Create` atomic copy then fails with
    /// `AlreadyExists`, and the method falls through to the conflict-resolution
    /// path which compares existing bytes against the local file.  For
    /// content-addressed callers this is safe because all writers for the same
    /// digest key write identical bytes.  The conflict-resolution path validates
    /// this.  For non-content-addressed callers (e.g. `put_overwrite` paths), a
    /// concurrent overwrite between the check and the copy could be silently
    /// reverted by the `CopyMode::Create` failure — the older writer's object
    /// survives and the newer writer's copy is rejected.
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
        verify_file_length(path, integrity)?;
        if let Some(existing) = ObjectStore::metadata(self, key)? {
            return existing_object_outcome_from_file(
                self,
                key,
                existing.length(),
                path,
                integrity,
            );
        }

        let location = self.location_for_key(key)?;
        let temporary = temporary_upload_location(&self.key_prefix);
        let upload_result = self.stream_file_to_location(&temporary, path);
        if let Err(error) = upload_result {
            self.delete_location_if_present(&temporary).ok();
            return Err(error);
        }

        let copy_result = self.block_on(self.inner.copy_opts(
            &temporary,
            &location,
            CopyOptions::new().with_mode(CopyMode::Create),
        ));
        // Best-effort cleanup of temp object — ignore failure, the canonical
        // copy already succeeded.
        self.delete_location_if_present(&temporary).ok();
        match copy_result {
            Ok(()) => Ok(PutOutcome::Inserted),
            Err(S3ObjectStoreError::External(ExternalObjectStoreError::AlreadyExists {
                ..
            }))
            | Err(S3ObjectStoreError::External(ExternalObjectStoreError::Precondition {
                ..
            })) => {
                let existing_length = ObjectStore::metadata(self, key)?
                    .ok_or(S3ObjectStoreError::ExistingObjectConflict)?
                    .length();
                existing_object_outcome_from_file(self, key, existing_length, path, integrity)
            }
            Err(error) => Err(error),
        }
    }

    /// Streams a caller-validated content-addressed local file to a temp key and
    /// atomically promotes it to the final key.
    ///
    /// This path is intended for immutable digest-addressed objects, where concurrent
    /// writers for the same key can only be writing the same bytes.  A temp key +
    /// conditional copy eliminates the TOCTOU window between the existence check
    /// and the write.
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
        verify_file_length(path, integrity)?;
        if let Some(existing) = ObjectStore::metadata(self, key)? {
            return existing_object_outcome_from_file(
                self,
                key,
                existing.length(),
                path,
                integrity,
            );
        }

        let location = self.location_for_key(key)?;
        let temporary = temporary_upload_location(&self.key_prefix);
        if let Err(error) = self.stream_file_to_location(&temporary, path) {
            self.delete_location_if_present(&temporary).ok();
            return Err(error);
        }

        let copy_result = self.block_on(self.inner.copy_opts(
            &temporary,
            &location,
            CopyOptions::new().with_mode(CopyMode::Create),
        ));
        // Best-effort cleanup — ignore failure, data is at the canonical key.
        self.delete_location_if_present(&temporary).ok();
        match copy_result {
            Ok(()) => Ok(PutOutcome::Inserted),
            Err(S3ObjectStoreError::External(ExternalObjectStoreError::AlreadyExists {
                ..
            }))
            | Err(S3ObjectStoreError::External(ExternalObjectStoreError::Precondition {
                ..
            })) => {
                let existing_length = ObjectStore::metadata(self, key)?
                    .ok_or(S3ObjectStoreError::ExistingObjectConflict)?
                    .length();
                existing_object_outcome_from_file(self, key, existing_length, path, integrity)
            }
            Err(error) => Err(error),
        }
    }

    /// Copies an existing object to a new key if the destination key is absent.
    ///
    /// This uses the S3-compatible provider's server-side copy path instead of reading
    /// the full source object back into process memory.
    ///
    /// # TOCTOU Race Window
    ///
    /// This method has a TOCTOU window between the `metadata()` destination
    /// existence probe implicit in the `CopyMode::Create` copy operation and the
    /// copy itself.  The `CopyMode::Create` is atomic at the S3 API
    /// level — either the destination is absent and the copy succeeds, or the
    /// destination exists and the copy fails with `AlreadyExists`.  However, the
    /// earlier source-metadata check and the source-or-destination
    /// equality check are not atomic with the copy.  A concurrent
    /// writer that deletes the source between the metadata check and the copy
    /// will cause the copy to fail with `NotFound`.  A concurrent writer that
    /// inserts the destination key between the metadata check and the copy will
    /// cause the copy to fail with `AlreadyExists`, triggering the
    /// conflict-resolution comparison.  This is safe for content-addressed usage
    /// where source and destination represent the same logical content.
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

        let Some(source_metadata) = ObjectStore::metadata(self, source)? else {
            return Err(S3ObjectStoreError::External(
                ExternalObjectStoreError::NotFound {
                    path: source.as_str().to_owned(),
                    source: Box::new(IoError::from(std::io::ErrorKind::NotFound)),
                },
            ));
        };

        let source_location = self.location_for_key(source)?;
        let destination_location = self.location_for_key(destination)?;
        let source_len = source_metadata.length();

        // S3 single-part COPY is limited to 5 GiB.  For larger objects, fall
        // back to a streaming multipart copy that reads the source in chunks
        // and re-uploads them to the destination.
        if source_len > MAX_SINGLE_COPY_BYTES {
            return self.block_on_result(self.streaming_large_copy(
                &source_location,
                &destination_location,
                source,
                destination,
                source_len,
            ));
        }

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
            })) => existing_copy_outcome(self, source, destination, source_len),
            Err(error) => Err(error),
        }
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
                // Re-check destination existence before completing.  A
                // concurrent writer may have promoted content to the
                // target key while we were uploading parts.
                if ObjectStore::metadata(self, destination)?.is_some() {
                    store.abort_multipart(&dst, &upload_id).await.ok();
                    // Content is identical for content-addressed keys,
                    // so returning AlreadyExists is correct.
                    return Ok(PutOutcome::AlreadyExists);
                }
                match store.complete_multipart(&dst, &upload_id, part_ids).await {
                    Ok(_) => Ok(PutOutcome::Inserted),
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

    fn stream_file_to_location(
        &self,
        location: &ObjectStorePath,
        path: &Path,
    ) -> Result<(), S3ObjectStoreError> {
        let store = self.inner.clone();
        let location = location.clone();
        let path = path.to_path_buf();
        self.block_on_result(async move {
            let upload = store.put_multipart(&location).await?;
            let mut writer = WriteMultipart::new_with_chunk_size(upload, STREAM_UPLOAD_CHUNK_BYTES);
            let mut file = TokioFile::open(&path)
                .await
                .map_err(S3ObjectStoreError::Io)?;
            let mut buffer = vec![0_u8; STREAM_UPLOAD_CHUNK_BYTES];
            loop {
                let read = match file.read(&mut buffer).await {
                    Ok(read) => read,
                    Err(error) => {
                        let _ignored = writer.abort().await;
                        return Err(S3ObjectStoreError::Io(error));
                    }
                };
                if read == 0 {
                    break;
                }
                let chunk = buffer
                    .get(..read)
                    .ok_or(S3ObjectStoreError::IntegrityLengthMismatch)?;
                writer.write(chunk);
                if let Err(error) = writer.wait_for_capacity(4).await {
                    let _ignored = writer.abort().await;
                    return Err(S3ObjectStoreError::External(error));
                }
            }
            writer
                .finish()
                .await
                .map_err(S3ObjectStoreError::External)?;
            Ok(())
        })
    }

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
}

impl ObjectStore for S3ObjectStore {
    type Error = S3ObjectStoreError;

    /// Stores an object if no identical object exists yet.
    ///
    /// This method skips the HEAD existence check and relies entirely on
    /// `PutMode::Create` (which sends `If-None-Match: *`) for conflict
    /// detection.  For first-time uploads this saves one round-trip.  If the
    /// object already exists, S3 returns `412 Precondition Failed` and the
    /// method returns [`PutOutcome::AlreadyExists`].
    ///
    /// This is safe for content-addressed keys because the same digest always
    /// maps to the same bytes — a race between two concurrent writers both
    /// putting identical content is harmless.
    fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error> {
        verify_integrity(body.as_slice(), integrity)?;
        let location = self.location_for_key(key)?;
        let bytes = body.into_bytes();
        let write = self.block_on(self.inner.put_opts(
            &location,
            bytes.into(),
            PutMode::Create.into(),
        ));
        match write {
            Ok(_result) => Ok(PutOutcome::Inserted),
            Err(S3ObjectStoreError::External(ExternalObjectStoreError::AlreadyExists {
                ..
            }))
            | Err(S3ObjectStoreError::External(ExternalObjectStoreError::Precondition {
                ..
            })) => Ok(PutOutcome::AlreadyExists),
            Err(error) => Err(error),
        }
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
        self.block_on_result(async {
            let mut listed = self.inner.list(Some(&location));
            while let Some(entry) = listed
                .try_next()
                .await
                .map_err(S3ObjectStoreError::External)
                .map_err(Into::into)?
            {
                let metadata = self.metadata_from_external(&entry).map_err(Into::into)?;
                // Skip temp upload artifacts.
                if is_temp_upload_key(metadata.key().as_str()) {
                    continue;
                }
                visitor(metadata)?;
            }

            Ok(())
        })
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
        let location = self.location_for_key(key)?;
        let bytes = body.into_bytes();
        match self
            .inner
            .put_opts(&location, bytes.into(), PutMode::Create.into())
            .await
        {
            Ok(_result) => Ok(PutOutcome::Inserted),
            Err(ExternalObjectStoreError::AlreadyExists { .. })
            | Err(ExternalObjectStoreError::Precondition { .. }) => {
                Ok(PutOutcome::AlreadyExists)
            }
            Err(error) => Err(S3ObjectStoreError::External(error)),
        }
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
        let mut acc = Vec::new();
        let mut stream = result.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(S3ObjectStoreError::External)?;
            acc.extend_from_slice(&chunk);
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

pub(crate) fn existing_object_outcome(
    store: &S3ObjectStore,
    key: &ObjectKey,
    existing_length: u64,
    expected_bytes: &[u8],
    integrity: &ObjectIntegrity,
) -> Result<PutOutcome, S3ObjectStoreError> {
    if existing_length != integrity.length() {
        return Err(S3ObjectStoreError::ExistingObjectConflict);
    }
    if existing_length == 0 {
        verify_integrity(expected_bytes, integrity)?;
        return Ok(PutOutcome::AlreadyExists);
    }

    // Stream-compare in chunks to avoid loading the full object into memory.
    // Content-addressed keys always produce matching bytes for identical hashes,
    // so a full in-memory comparison is unnecessary and causes OOM at 5GiB+.
    let mut offset = 0_u64;
    while offset < existing_length {
        let remaining = existing_length
            .checked_sub(offset)
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        let to_read = remaining.min(super::STREAM_COMPARE_CHUNK_BYTES as u64);
        let end = offset
            .checked_add(to_read)
            .and_then(|value| value.checked_sub(1))
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        let range = ByteRange::new(offset, end)
            .map_err(|_error| S3ObjectStoreError::ExistingObjectConflict)?;
        let existing_chunk = ObjectStore::read_range(store, key, range)?;
        let expected_chunk = expected_bytes
            .get(offset as usize..)
            .and_then(|slice| slice.get(..to_read as usize))
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        if existing_chunk.as_slice() != expected_chunk {
            return Err(S3ObjectStoreError::ExistingObjectConflict);
        }
        offset = end.saturating_add(1);
    }
    Ok(PutOutcome::AlreadyExists)
}

pub(crate) fn existing_object_outcome_from_file(
    store: &S3ObjectStore,
    key: &ObjectKey,
    existing_length: u64,
    path: &Path,
    integrity: &ObjectIntegrity,
) -> Result<PutOutcome, S3ObjectStoreError> {
    verify_file_length(path, integrity)?;
    if existing_length != integrity.length() {
        return Err(S3ObjectStoreError::ExistingObjectConflict);
    }
    let mut file = File::open(path).map_err(S3ObjectStoreError::Io)?;
    let mut offset = 0_u64;
    let mut buffer = vec![0_u8; super::STREAM_COMPARE_CHUNK_BYTES];
    while offset < existing_length {
        let remaining = existing_length
            .checked_sub(offset)
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        let to_read = usize::try_from(remaining.min(super::STREAM_COMPARE_CHUNK_BYTES as u64))
            .map_err(|_error| S3ObjectStoreError::ExistingObjectConflict)?;
        let chunk = buffer
            .get_mut(..to_read)
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        file.read_exact(chunk).map_err(S3ObjectStoreError::Io)?;
        let end = offset
            .checked_add(
                u64::try_from(to_read)
                    .map_err(|_error| S3ObjectStoreError::ExistingObjectConflict)?,
            )
            .and_then(|value| value.checked_sub(1))
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        let range = ByteRange::new(offset, end)
            .map_err(|_error| S3ObjectStoreError::ExistingObjectConflict)?;
        let existing = ObjectStore::read_range(store, key, range)?;
        let expected = buffer
            .get(..to_read)
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        if existing.as_slice() != expected {
            return Err(S3ObjectStoreError::ExistingObjectConflict);
        }
        offset = end.saturating_add(1);
    }
    Ok(PutOutcome::AlreadyExists)
}

pub(crate) fn existing_copy_outcome(
    store: &S3ObjectStore,
    source: &ObjectKey,
    destination: &ObjectKey,
    source_length: u64,
) -> Result<PutOutcome, S3ObjectStoreError> {
    let Some(destination_metadata) = ObjectStore::metadata(store, destination)? else {
        return Err(S3ObjectStoreError::ExistingObjectConflict);
    };
    if destination_metadata.length() != source_length {
        return Err(S3ObjectStoreError::ExistingObjectConflict);
    }
    if source_length == 0 {
        return Ok(PutOutcome::AlreadyExists);
    }

    let mut offset = 0_u64;
    while offset < source_length {
        let remaining = source_length
            .checked_sub(offset)
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        let to_read = remaining.min(super::STREAM_COMPARE_CHUNK_BYTES as u64);
        let end = offset
            .checked_add(to_read)
            .and_then(|value| value.checked_sub(1))
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        let range = ByteRange::new(offset, end)
            .map_err(|_error| S3ObjectStoreError::ExistingObjectConflict)?;
        let source_bytes = ObjectStore::read_range(store, source, range)?;
        let destination_bytes = ObjectStore::read_range(store, destination, range)?;
        if source_bytes != destination_bytes {
            return Err(S3ObjectStoreError::ExistingObjectConflict);
        }
        offset = end.saturating_add(1);
    }
    Ok(PutOutcome::AlreadyExists)
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

pub(crate) fn temporary_upload_location(key_prefix: &Option<String>) -> ObjectStorePath {
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
