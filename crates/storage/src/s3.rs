use std::{
    fmt,
    fs::File,
    future::Future,
    io::{Error as IoError, Read},
    ops::Range,
    path::Path,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use object_store::{
    CopyMode, CopyOptions, Error as ExternalObjectStoreError, GetOptions, GetResult,
    ObjectStore as ExternalObjectStore, ObjectStoreExt, PutMode, WriteMultipart,
    aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut, S3CopyIfNotExists},
    path::Path as ObjectStorePath,
};
use shardline_protocol::{ByteRange, SecretString, ShardlineHash};
use thiserror::Error;
use tokio::{
    fs::File as TokioFile,
    io::AsyncReadExt,
    runtime::{Builder, Handle, Runtime},
    task::block_in_place,
};

use crate::{
    DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectMetadata, ObjectPrefix,
    ObjectPrefixError, ObjectStore, PutOutcome,
};

/// Async byte stream returned from ranged S3 reads.
pub type S3ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, S3ObjectStoreError>> + Send>>;

/// Result of beginning a direct multipart upload for an immutable destination key.
pub enum BeginMultipartUploadResult {
    /// The destination already exists.
    AlreadyExists,
    /// The caller can stream bytes into the returned multipart writer.
    Upload(S3MultipartUploadWriter),
}

/// Multipart upload writer for direct request-body streaming into S3-compatible storage.
pub struct S3MultipartUploadWriter {
    writer: WriteMultipart,
}

impl S3MultipartUploadWriter {
    /// Queues bytes into the multipart writer.
    pub fn write(&mut self, bytes: &[u8]) {
        self.writer.write(bytes);
    }

    /// Waits until the multipart writer has spare upload capacity.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the upstream multipart writer fails.
    pub async fn wait_for_capacity(
        &mut self,
        max_in_flight_parts: usize,
    ) -> Result<(), S3ObjectStoreError> {
        self.writer
            .wait_for_capacity(max_in_flight_parts)
            .await
            .map_err(S3ObjectStoreError::External)
    }

    /// Finishes the multipart upload.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the upstream multipart finalize call fails.
    pub async fn finish(self) -> Result<(), S3ObjectStoreError> {
        self.writer
            .finish()
            .await
            .map(|_result| ())
            .map_err(S3ObjectStoreError::External)
    }

    /// Aborts the multipart upload.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the upstream multipart abort call fails.
    pub async fn abort(self) -> Result<(), S3ObjectStoreError> {
        self.writer
            .abort()
            .await
            .map_err(S3ObjectStoreError::External)
    }
}

const STREAM_UPLOAD_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const STREAM_COMPARE_CHUNK_BYTES: usize = 256 * 1024;
static TEMP_UPLOAD_COUNTER: AtomicU64 = AtomicU64::new(0);

/// S3-compatible object store configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct S3ObjectStoreConfig {
    bucket: String,
    region: String,
    endpoint: Option<String>,
    access_key_id: Option<SecretString>,
    secret_access_key: Option<SecretString>,
    session_token: Option<SecretString>,
    key_prefix: Option<String>,
    allow_http: bool,
    virtual_hosted_style_request: bool,
}

impl S3ObjectStoreConfig {
    /// Creates S3-compatible object storage configuration.
    #[must_use]
    pub const fn new(bucket: String, region: String) -> Self {
        Self {
            bucket,
            region,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            key_prefix: None,
            allow_http: false,
            virtual_hosted_style_request: false,
        }
    }

    /// Adds a custom S3-compatible endpoint URL.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: Option<String>) -> Self {
        self.endpoint = endpoint;
        self
    }

    /// Adds static access-key credentials.
    #[must_use]
    pub fn with_credentials(
        mut self,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
    ) -> Self {
        self.access_key_id = access_key_id.map(SecretString::new);
        self.secret_access_key = secret_access_key.map(SecretString::new);
        self.session_token = session_token.map(SecretString::new);
        self
    }

    /// Adds an object-key prefix under the bucket.
    #[must_use]
    pub fn with_key_prefix(mut self, key_prefix: Option<&str>) -> Self {
        self.key_prefix = key_prefix.and_then(normalize_prefix);
        self
    }

    /// Allows HTTP endpoints for local S3-compatible deployments.
    #[must_use]
    pub const fn with_allow_http(mut self, allow_http: bool) -> Self {
        self.allow_http = allow_http;
        self
    }

    /// Enables virtual-hosted-style requests.
    #[must_use]
    pub const fn with_virtual_hosted_style_request(
        mut self,
        virtual_hosted_style_request: bool,
    ) -> Self {
        self.virtual_hosted_style_request = virtual_hosted_style_request;
        self
    }

    /// Returns the configured bucket.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the configured key prefix.
    #[must_use]
    pub fn key_prefix(&self) -> Option<&str> {
        self.key_prefix.as_deref()
    }
}

impl fmt::Debug for S3ObjectStoreConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3ObjectStoreConfig")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field(
                "access_key_id",
                &self.access_key_id.as_ref().map(|_value| "***"),
            )
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_value| "***"),
            )
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_value| "***"),
            )
            .field("key_prefix", &self.key_prefix)
            .field("allow_http", &self.allow_http)
            .field(
                "virtual_hosted_style_request",
                &self.virtual_hosted_style_request,
            )
            .finish()
    }
}

/// S3-compatible implementation of [`ObjectStore`].
#[derive(Clone)]
pub struct S3ObjectStore {
    inner: AmazonS3,
    runtime: Option<Arc<Runtime>>,
    key_prefix: Option<String>,
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
        validate_config(&config)?;
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(config.bucket)
            .with_region(config.region)
            .with_allow_http(config.allow_http)
            .with_virtual_hosted_style_request(config.virtual_hosted_style_request)
            .with_copy_if_not_exists(S3CopyIfNotExists::Multipart)
            .with_conditional_put(S3ConditionalPut::ETagMatch);

        if let Some(endpoint) = config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        match (config.access_key_id, config.secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => {
                builder = builder
                    .with_access_key_id(access_key_id.expose_secret())
                    .with_secret_access_key(secret_access_key.expose_secret());
            }
            (None, None) => {}
            (Some(_), None) | (None, Some(_)) => {
                return Err(S3ObjectStoreError::IncompleteCredentials);
            }
        }
        if let Some(session_token) = config.session_token {
            builder = builder.with_token(session_token.expose_secret());
        }

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
            inner: builder.build()?,
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

    fn location_for_key(&self, key: &ObjectKey) -> Result<ObjectStorePath, S3ObjectStoreError> {
        let location = self.key_prefix.as_ref().map_or_else(
            || key.as_str().to_owned(),
            |prefix| format!("{prefix}/{}", key.as_str()),
        );
        ObjectStorePath::parse(location).map_err(S3ObjectStoreError::Path)
    }

    fn location_for_prefix(
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
                let Some(remainder) = item.key().as_str().strip_prefix(prefix.as_str()) else {
                    continue;
                };
                if remainder.is_empty() || remainder.contains('/') {
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

    /// Begins a direct multipart upload to a content-addressed destination key.
    ///
    /// This path is intended for immutable digest-addressed objects, where callers
    /// validate the stream contents independently and concurrent writers for the same
    /// key can only be writing identical bytes.
    ///
    /// # TOCTOU Race Window
    ///
    /// This method has a time-of-check-to-time-of-use window between the
    /// `metadata()` existence probe (line 448) and the `put_multipart()` call
    /// (line 453).  If two concurrent callers both see the key as absent and
    /// both start multipart uploads, both will receive
    /// [`BeginMultipartUploadResult::Upload`].  The final `finish()` on each
    /// writer will race: the first to complete stores the object; the second
    /// may fail with `AlreadyExists` if the S3 backend enforces
    /// `S3ConditionalPut::ETagMatch` on multipart completion, or it may
    /// silently overwrite.  This is safe because content-addressed keys
    /// guarantee all concurrent writers are writing identical bytes — a
    /// duplicate multipart upload for the same digest produces the same
    /// object.  Callers must ensure they only write bytes matching the
    /// content hash embedded in the key.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the destination lookup or multipart
    /// initialization fails.
    pub async fn begin_content_addressed_upload(
        &self,
        key: &ObjectKey,
    ) -> Result<BeginMultipartUploadResult, S3ObjectStoreError> {
        if self.metadata(key)?.is_some() {
            return Ok(BeginMultipartUploadResult::AlreadyExists);
        }

        let location = self.location_for_key(key)?;
        let upload = self
            .inner
            .put_multipart(&location)
            .await
            .map_err(S3ObjectStoreError::External)?;
        Ok(BeginMultipartUploadResult::Upload(
            S3MultipartUploadWriter {
                writer: WriteMultipart::new_with_chunk_size(upload, STREAM_UPLOAD_CHUNK_BYTES),
            },
        ))
    }

    fn metadata_from_external(
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
        self.block_on(
            self.inner
                .put_opts(&location, bytes.into(), PutMode::Overwrite.into()),
        )?;
        Ok(())
    }

    /// Streams a caller-validated local file into S3-compatible storage if the destination
    /// key is absent.
    ///
    /// # TOCTOU Race Window
    ///
    /// This method has a two-stage TOCTOU window.  First, the `metadata()` probe
    /// (line 522) checks whether the destination key exists.  Second, the temporary
    /// upload is copied to the final key with `CopyMode::Create` (line 542).
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
        if let Some(existing) = self.metadata(key)? {
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
            self.delete_location_if_present(&temporary)?;
            return Err(error);
        }

        let copy_result = self.block_on(self.inner.copy_opts(
            &temporary,
            &location,
            CopyOptions::new().with_mode(CopyMode::Create),
        ));
        self.delete_location_if_present(&temporary)?;
        match copy_result {
            Ok(()) => Ok(PutOutcome::Inserted),
            Err(S3ObjectStoreError::External(ExternalObjectStoreError::AlreadyExists {
                ..
            }))
            | Err(S3ObjectStoreError::External(ExternalObjectStoreError::Precondition {
                ..
            })) => {
                let existing_length = self
                    .metadata(key)?
                    .ok_or(S3ObjectStoreError::ExistingObjectConflict)?
                    .length();
                existing_object_outcome_from_file(self, key, existing_length, path, integrity)
            }
            Err(error) => Err(error),
        }
    }

    /// Streams a caller-validated content-addressed local file directly to its final key.
    ///
    /// This path is intended for immutable digest-addressed objects, where concurrent
    /// writers for the same key can only be writing the same bytes. It avoids the
    /// temporary-object plus copy step used by [`Self::put_file_if_absent`].
    ///
    /// # TOCTOU Race Window
    ///
    /// This method has a TOCTOU window between the `metadata()` existence probe
    /// (line 586) and the multipart upload written by `stream_file_to_location`
    /// (line 597).  Two concurrent callers that both see the key as absent will
    /// both start streaming their file to the same final key.  The second
    /// multipart upload to complete may fail with `AlreadyExists` (if the S3
    /// backend enforces `S3ConditionalPut::ETagMatch` on multipart completion)
    /// or may silently overwrite the first upload's bytes.  This is safe for
    /// content-addressed keys because all concurrent writers write identical
    /// bytes — either writer's output produces the same object content.
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
        if let Some(existing) = self.metadata(key)? {
            return existing_object_outcome_from_file(
                self,
                key,
                existing.length(),
                path,
                integrity,
            );
        }

        let location = self.location_for_key(key)?;
        self.stream_file_to_location(&location, path)?;
        Ok(PutOutcome::Inserted)
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
    /// copy itself (line 682).  The `CopyMode::Create` is atomic at the S3 API
    /// level — either the destination is absent and the copy succeeds, or the
    /// destination exists and the copy fails with `AlreadyExists`.  However, the
    /// earlier source-metadata check (line 671) and the source-or-destination
    /// equality check (line 658) are not atomic with the copy.  A concurrent
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
            return if self.metadata(source)?.is_some() {
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

        let Some(source_metadata) = self.metadata(source)? else {
            return Err(S3ObjectStoreError::External(
                ExternalObjectStoreError::NotFound {
                    path: source.as_str().to_owned(),
                    source: Box::new(IoError::from(std::io::ErrorKind::NotFound)),
                },
            ));
        };

        let source_location = self.location_for_key(source)?;
        let destination_location = self.location_for_key(destination)?;
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
            })) => existing_copy_outcome(self, source, destination, source_metadata.length()),
            Err(error) => Err(error),
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

    fn delete_location_if_present(
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
    /// # TOCTOU Race Window
    ///
    /// The `metadata()` existence probe (line 783) and `PutMode::Create` write
    /// (line 794) are not atomic.  Two concurrent callers that both see the key
    /// as absent will both attempt a `PutMode::Create` write.  The second write
    /// will fail with `AlreadyExists`, and the conflict-resolution path compares
    /// the existing bytes against the caller's body.  For content-addressed keys
    /// this is safe because both callers write identical bytes.  The
    /// conflict-resolution path validates the match and returns
    /// [`PutOutcome::AlreadyExists`] on success or
    /// [`S3ObjectStoreError::ExistingObjectConflict`] on mismatch.
    fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error> {
        verify_integrity(body.as_slice(), integrity)?;
        let location = self.location_for_key(key)?;
        if let Some(existing) = self.metadata(key)? {
            return existing_object_outcome(
                self,
                key,
                existing.length(),
                body.as_slice(),
                integrity,
            );
        }

        let bytes = body.into_bytes();
        let write = self.block_on(self.inner.put_opts(
            &location,
            bytes.clone().into(),
            PutMode::Create.into(),
        ));
        match write {
            Ok(_result) => Ok(PutOutcome::Inserted),
            Err(S3ObjectStoreError::External(ExternalObjectStoreError::AlreadyExists {
                ..
            }))
            | Err(S3ObjectStoreError::External(ExternalObjectStoreError::Precondition {
                ..
            })) => {
                existing_object_outcome(self, key, integrity.length(), bytes.as_ref(), integrity)
            }
            Err(error) => Err(error),
        }
    }

    fn read_range(&self, key: &ObjectKey, range: ByteRange) -> Result<Vec<u8>, Self::Error> {
        let location = self.location_for_key(key)?;
        let Some(length) = range.len() else {
            return Err(S3ObjectStoreError::RangeOutOfBounds);
        };
        let end_exclusive = range
            .start()
            .checked_add(length)
            .ok_or(S3ObjectStoreError::RangeOutOfBounds)?;
        let bytes = self.block_on(
            self.inner
                .get_range(&location, range.start()..end_exclusive),
        )?;
        if u64::try_from(bytes.len()).map_err(|_error| S3ObjectStoreError::RangeOutOfBounds)?
            != length
        {
            return Err(S3ObjectStoreError::RangeOutOfBounds);
        }
        Ok(bytes.to_vec())
    }

    fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
        self.metadata(key).map(|metadata| metadata.is_some())
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
        self.visit_prefix(prefix, |entry| {
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

/// S3 object-store adapter failure.
#[derive(Debug, Error)]
pub enum S3ObjectStoreError {
    /// Required S3 credentials were only partially provided.
    #[error("s3 object store credentials must include both access key id and secret access key")]
    IncompleteCredentials,
    /// The S3 bucket name was empty.
    #[error("s3 object store bucket must not be empty")]
    EmptyBucket,
    /// The S3 region was empty.
    #[error("s3 object store region must not be empty")]
    EmptyRegion,
    /// The configured key prefix could not be represented as a safe storage prefix.
    #[error("s3 object store key prefix was invalid")]
    InvalidKeyPrefix(#[source] ObjectPrefixError),
    /// The supplied body length did not match the expected integrity metadata.
    #[error("object body length did not match expected integrity")]
    IntegrityLengthMismatch,
    /// The supplied body hash did not match the expected integrity metadata.
    #[error("object body hash did not match expected integrity")]
    IntegrityHashMismatch,
    /// An existing object for the same key had different bytes.
    #[error("object key already exists with conflicting bytes")]
    ExistingObjectConflict,
    /// The requested byte range exceeded the stored object length.
    #[error("requested byte range exceeded stored object length")]
    RangeOutOfBounds,
    /// An object listed from S3 could not be represented as a validated object key.
    #[error("s3 listed an object outside the configured key prefix")]
    InvalidListedKey,
    /// Local temporary-file access failed.
    #[error("temporary file operation failed")]
    Io(#[from] IoError),
    /// Object-store path conversion failed.
    #[error("object-store path conversion failed")]
    Path(#[source] object_store::path::Error),
    /// Runtime initialization failed.
    #[error("s3 object store runtime initialization failed")]
    Runtime(#[source] IoError),
    /// No Tokio runtime was available for a synchronous S3 operation.
    #[error("s3 object store runtime is unavailable")]
    RuntimeUnavailable,
    /// S3-compatible object store operation failed.
    #[error("s3 object store operation failed")]
    External(#[from] ExternalObjectStoreError),
}

fn validate_config(config: &S3ObjectStoreConfig) -> Result<(), S3ObjectStoreError> {
    if config.bucket.trim().is_empty() {
        return Err(S3ObjectStoreError::EmptyBucket);
    }
    if config.region.trim().is_empty() {
        return Err(S3ObjectStoreError::EmptyRegion);
    }
    if let Some(prefix) = &config.key_prefix {
        ObjectPrefix::parse(prefix).map_err(S3ObjectStoreError::InvalidKeyPrefix)?;
    }
    Ok(())
}

fn normalize_prefix(value: &str) -> Option<String> {
    let trimmed = value.trim_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn validated_external_range(range: ByteRange) -> Result<Range<u64>, S3ObjectStoreError> {
    let Some(length) = range.len() else {
        return Err(S3ObjectStoreError::RangeOutOfBounds);
    };
    let end_exclusive = range
        .start()
        .checked_add(length)
        .ok_or(S3ObjectStoreError::RangeOutOfBounds)?;

    Ok(range.start()..end_exclusive)
}

fn stream_payload_for_range(
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

fn verify_integrity(bytes: &[u8], integrity: &ObjectIntegrity) -> Result<(), S3ObjectStoreError> {
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

fn existing_object_outcome(
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
    let range = ByteRange::new(
        0,
        existing_length
            .checked_sub(1)
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?,
    )
    .map_err(|_error| S3ObjectStoreError::ExistingObjectConflict)?;
    let existing = store.read_range(key, range)?;
    verify_integrity(&existing, integrity)?;
    if existing == expected_bytes {
        return Ok(PutOutcome::AlreadyExists);
    }

    Err(S3ObjectStoreError::ExistingObjectConflict)
}

fn existing_object_outcome_from_file(
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
    let mut buffer = vec![0_u8; STREAM_COMPARE_CHUNK_BYTES];
    while offset < existing_length {
        let remaining = existing_length
            .checked_sub(offset)
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        let to_read = usize::try_from(remaining.min(STREAM_COMPARE_CHUNK_BYTES as u64))
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
        let existing = store.read_range(key, range)?;
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

fn existing_copy_outcome(
    store: &S3ObjectStore,
    source: &ObjectKey,
    destination: &ObjectKey,
    source_length: u64,
) -> Result<PutOutcome, S3ObjectStoreError> {
    let Some(destination_metadata) = store.metadata(destination)? else {
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
        let to_read = remaining.min(STREAM_COMPARE_CHUNK_BYTES as u64);
        let end = offset
            .checked_add(to_read)
            .and_then(|value| value.checked_sub(1))
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        let range = ByteRange::new(offset, end)
            .map_err(|_error| S3ObjectStoreError::ExistingObjectConflict)?;
        let source_bytes = store.read_range(source, range)?;
        let destination_bytes = store.read_range(destination, range)?;
        if source_bytes != destination_bytes {
            return Err(S3ObjectStoreError::ExistingObjectConflict);
        }
        offset = end.saturating_add(1);
    }
    Ok(PutOutcome::AlreadyExists)
}

fn verify_file_length(path: &Path, integrity: &ObjectIntegrity) -> Result<(), S3ObjectStoreError> {
    let metadata = std::fs::metadata(path).map_err(S3ObjectStoreError::Io)?;
    if metadata.len() != integrity.length() {
        return Err(S3ObjectStoreError::IntegrityLengthMismatch);
    }
    Ok(())
}

fn temporary_upload_location(key_prefix: &Option<String>) -> ObjectStorePath {
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

fn chunk_hash(bytes: &[u8]) -> ShardlineHash {
    let digest = blake3::hash(bytes);
    ShardlineHash::from_bytes(*digest.as_bytes())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::StreamExt;
    use object_store::{
        GetOptions, ObjectStore as ExternalObjectStore, ObjectStoreExt, memory::InMemory,
        path::Path as ObjectStorePath,
    };
    use shardline_protocol::ByteRange;

    use super::{
        S3ObjectStore, S3ObjectStoreConfig, stream_payload_for_range, validated_external_range,
    };
    use crate::ObjectKey;

    #[test]
    fn s3_config_normalizes_key_prefix() {
        let config = S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
            .with_key_prefix(Some("/tenant-a/"));

        assert_eq!(config.key_prefix(), Some("tenant-a"));
    }

    #[test]
    fn s3_location_applies_key_prefix() {
        let store = S3ObjectStore::new(
            S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
                .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
                .with_allow_http(true)
                .with_credentials(Some("access".to_owned()), Some("secret".to_owned()), None)
                .with_key_prefix(Some("tenant-a")),
        );
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };
        let key = ObjectKey::parse("xorbs/default/aa/hash.xorb");
        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let location = store.location_for_key(&key);

        assert!(location.is_ok());
        if let Ok(location) = location {
            assert_eq!(location.as_ref(), "tenant-a/xorbs/default/aa/hash.xorb");
        }
    }

    #[test]
    fn s3_store_rejects_traversal_key_prefix() {
        let store = S3ObjectStore::new(
            S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
                .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
                .with_allow_http(true)
                .with_key_prefix(Some("../tenant-b")),
        );

        assert!(matches!(
            store,
            Err(super::S3ObjectStoreError::InvalidKeyPrefix(_))
        ));
    }

    #[test]
    fn s3_store_rejects_dot_segment_key_prefix_after_normalization() {
        let store = S3ObjectStore::new(
            S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
                .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
                .with_allow_http(true)
                .with_key_prefix(Some("/tenant-a/./objects/")),
        );

        assert!(matches!(
            store,
            Err(super::S3ObjectStoreError::InvalidKeyPrefix(_))
        ));
    }

    #[test]
    fn s3_store_debug_redacts_credentials() {
        let store = S3ObjectStore::new(
            S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
                .with_endpoint(Some("http://127.0.0.1:9000".to_owned()))
                .with_allow_http(true)
                .with_credentials(
                    Some("access-key".to_owned()),
                    Some("secret-key".to_owned()),
                    Some("session-token".to_owned()),
                )
                .with_key_prefix(Some("tenant-a")),
        );
        assert!(store.is_ok());
        let Ok(store) = store else {
            return;
        };

        let rendered = format!("{store:?}");

        assert!(!rendered.contains("access-key"));
        assert!(!rendered.contains("secret-key"));
        assert!(!rendered.contains("session-token"));
    }

    #[test]
    fn validated_external_range_converts_inclusive_byte_range() {
        let range = ByteRange::new(3, 8);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };

        let external = validated_external_range(range);

        assert!(external.is_ok());
        assert_eq!(external.ok(), Some(3..9));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_payload_for_range_rejects_mismatched_result_range() {
        let store = InMemory::new();
        let location = ObjectStorePath::from("tenant/object");
        assert!(
            store
                .put(&location, Bytes::from_static(b"abcd").into())
                .await
                .is_ok()
        );
        let result = store
            .get_opts(&location, GetOptions::new().with_range(Some(0..4)))
            .await;
        assert!(result.is_ok());
        let Ok(result) = result else {
            return;
        };

        let stream = stream_payload_for_range(result, 1..5);

        assert!(matches!(
            stream,
            Err(super::S3ObjectStoreError::RangeOutOfBounds)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_payload_for_range_preserves_streamed_bytes() {
        let store = InMemory::new();
        let location = ObjectStorePath::from("tenant/object");
        assert!(
            store
                .put(&location, Bytes::from_static(b"abcd").into())
                .await
                .is_ok()
        );
        let result = store
            .get_opts(&location, GetOptions::new().with_range(Some(0..4)))
            .await;
        assert!(result.is_ok());
        let Ok(result) = result else {
            return;
        };

        let stream = stream_payload_for_range(result, 0..4);
        assert!(stream.is_ok());
        let Ok(mut stream) = stream else {
            return;
        };
        let mut observed = Vec::new();
        while let Some(item) = stream.next().await {
            assert!(item.is_ok());
            let Ok(chunk) = item else {
                return;
            };
            observed.extend_from_slice(&chunk);
        }

        assert_eq!(observed, b"abcd");
    }
}
