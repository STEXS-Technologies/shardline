#![allow(
    clippy::let_underscore_must_use,
    clippy::manual_inspect,
)]

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
    ObjectStore as ExternalObjectStore, ObjectStoreExt, PutMode, PutPayload, WriteMultipart,
    aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut, S3CopyIfNotExists},
    multipart::{MultipartStore, PartId},
    path::Path as ObjectStorePath,
};
use shardline_protocol::{ByteRange, SecretString, ShardlineHash};
use thiserror::Error;

/// Generates a unique temp key derived from a canonical key using a monotonic
/// counter and nanosecond timestamp.
fn temp_key_for(key: &ObjectKey) -> Result<ObjectKey, S3ObjectStoreError> {
    let counter = TEMP_UPLOAD_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = std::process::id();
    let suffix = format!("tmp.{counter}.{pid}.{now_nanos}");
    ObjectKey::parse(&format!("{}.{suffix}", key.as_str()))
        .map_err(|_err| S3ObjectStoreError::InvalidListedKey)
}

/// Returns `true` if `key` looks like a temp upload artifact produced by
/// [`temp_key_for`], i.e. contains `.tmp.` followed by at least one digit.
fn is_temp_upload_key(key: &str) -> bool {
    key.find(".tmp.").is_some_and(|pos| {
        // SAFETY: pos comes from find(".tmp.") which matches 5 chars,
        // so pos + 5 is always <= key.len(). The get() call is a safety
        // net — it will never return None in practice.
        key.as_bytes()
            .get(pos.saturating_add(5))
            .is_some_and(|b| b.is_ascii_digit())
    })
}

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
    /// The second field is a temp key used for TOCTOU-safe promotion.
    Upload(S3MultipartUploadWriter, ObjectKey),
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
/// Maximum object size that S3's single-part COPY supports (5 GiB).
const MAX_SINGLE_COPY_BYTES: u64 = 5 * 1024 * 1024 * 1024;
/// Chunk size used when copying objects >5 GiB via streaming multipart.
const LARGE_COPY_CHUNK_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB
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

    /// Begins a direct multipart upload to a content-addressed destination key.
    ///
    /// This path is intended for immutable digest-addressed objects, where callers
    /// validate the stream contents independently and concurrent writers for the same
    /// key can only be writing identical bytes.
    ///
    /// # TOCTOU Safety
    ///
    /// The initial existence check is a fast-path optimization only.  The multipart
    /// upload is started on a **temp key** derived from the canonical key.  After the
    /// caller streams data and calls [`S3ObjectStore::finish_content_addressed_upload`],
    /// the content is atomically promoted to the canonical key via a conditional copy
    /// (`CopyMode::Create`).  If a concurrent writer has already promoted the same
    /// canonical key, the copy returns [`PutOutcome::AlreadyExists`] and the temp
    /// content is discarded — eliminating the TOCTOU window.
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

        let temp_key = temp_key_for(key)?;
        let location = self.location_for_key(&temp_key)?;
        let upload = self
            .inner
            .put_multipart(&location)
            .await
            .map_err(S3ObjectStoreError::External)?;
        Ok(BeginMultipartUploadResult::Upload(
            S3MultipartUploadWriter {
                writer: WriteMultipart::new_with_chunk_size(upload, STREAM_UPLOAD_CHUNK_BYTES),
            },
            temp_key,
        ))
    }

    /// Finishes a content-addressed upload and atomically promotes the temp content
    /// to the canonical key.
    ///
    /// After the caller has streamed all bytes through the writer returned by
    /// [`begin_content_addressed_upload`], this method:
    /// 1. Finalizes the multipart upload to the temp key.
    /// 2. Atomically copies temp → canonical using [`CopyMode::Create`].
    /// 3. Deletes the temp key.
    ///
    /// If the canonical key already exists (concurrent writer finished first),
    /// returns [`PutOutcome::AlreadyExists`].  The content is identical since it
    /// is content-addressed.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the multipart finalization, conditional
    /// copy, or cleanup fails.
    pub async fn finish_content_addressed_upload(
        &self,
        upload: S3MultipartUploadWriter,
        temp_key: &ObjectKey,
        canonical_key: &ObjectKey,
    ) -> Result<PutOutcome, S3ObjectStoreError> {
        upload.finish().await?;

        // Conditional copy — fails if canonical already exists.
        match self.copy_object_if_absent(temp_key, canonical_key) {
            Ok(outcome) => {
                let _ = self.delete_if_present(temp_key);
                Ok(outcome)
            }
            Err(error) => {
                let _ = self.delete_if_present(temp_key);
                Err(error)
            }
        }
    }

    /// Starts a resumable multipart upload at a temporary S3 location.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the multipart upload cannot be
    /// initialized.
    pub async fn create_resumable_upload(
        &self,
        key: &ObjectKey,
    ) -> Result<String, S3ObjectStoreError> {
        let location = self.location_for_key(key)?;
        self.inner
            .create_multipart(&location)
            .await
            .map_err(S3ObjectStoreError::External)
    }

    /// Uploads one part into an existing resumable multipart upload.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the object key is invalid or the part
    /// upload fails.
    pub async fn upload_resumable_part(
        &self,
        key: &ObjectKey,
        upload_id: &str,
        part_idx: usize,
        bytes: Bytes,
    ) -> Result<String, S3ObjectStoreError> {
        let location = self.location_for_key(key)?;
        let multipart_id = upload_id.to_owned();
        let part = self
            .inner
            .put_part(&location, &multipart_id, part_idx, bytes.into())
            .await
            .map_err(S3ObjectStoreError::External)?;
        Ok(part.content_id)
    }

    /// Completes a resumable multipart upload once all parts are uploaded.
    ///
    /// The `parts` parameter is a vector of `(part_number, etag)` tuples.  Part
    /// numbers must be 0-indexed and consecutive from 0 (inclusive) to
    /// `parts.len() - 1` (inclusive).  Duplicate or missing part numbers are
    /// rejected.  Parts are sorted by part number before the S3 CompleteMultipartUpload
    /// request is sent.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the final completion request fails or
    /// part numbering is invalid.
    pub async fn complete_resumable_upload(
        &self,
        key: &ObjectKey,
        upload_id: &str,
        parts: Vec<(usize, String)>,
    ) -> Result<(), S3ObjectStoreError> {
        let count = parts.len();
        if count == 0 {
            return Err(S3ObjectStoreError::InvalidUploadParts);
        }

        // Validate consecutive 0..count numbering.
        let mut indexed: Vec<(usize, String)> = parts;
        indexed.sort_by_key(|(part_number, _etag)| *part_number);
        for (expected, (part_number, _etag)) in indexed.iter().enumerate() {
            if *part_number != expected {
                return Err(S3ObjectStoreError::InvalidUploadParts);
            }
        }

        let location = self.location_for_key(key)?;
        let multipart_id = upload_id.to_owned();
        let part_ids = indexed
            .into_iter()
            .map(|(_part_number, content_id)| PartId { content_id })
            .collect();
        let _result = self
            .inner
            .complete_multipart(&location, &multipart_id, part_ids)
            .await
            .map_err(S3ObjectStoreError::External)?;
        Ok(())
    }

    /// Aborts a resumable multipart upload and discards any uploaded parts.
    ///
    /// # Errors
    ///
    /// Returns [`S3ObjectStoreError`] when the abort request fails.
    pub async fn abort_resumable_upload(
        &self,
        key: &ObjectKey,
        upload_id: &str,
    ) -> Result<(), S3ObjectStoreError> {
        let location = self.location_for_key(key)?;
        let multipart_id = upload_id.to_owned();
        self.inner
            .abort_multipart(&location, &multipart_id)
            .await
            .map_err(S3ObjectStoreError::External)
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
        self.block_on(self.inner.put_opts(
            &temp_location,
            bytes.into(),
            PutMode::Create.into(),
        ))?;

        // Atomically replace the live key with the temp content via copy,
        // then remove the temp key.
        let result = self
            .block_on(self.inner.copy(&temp_location, &location))
            .map_err(|error| {
                // Best-effort cleanup of temp object
                let _ = self.block_on(self.inner.delete(&temp_location));
                error
            });
        let _ = self.block_on(self.inner.delete(&temp_location));
        result
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
            let _ = self.delete_location_if_present(&temporary);
            return Err(error);
        }

        let copy_result = self.block_on(self.inner.copy_opts(
            &temporary,
            &location,
            CopyOptions::new().with_mode(CopyMode::Create),
        ));
        // Best-effort cleanup of temp object — ignore failure, the canonical
        // copy already succeeded.
        let _ = self.delete_location_if_present(&temporary);
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
        if let Err(error) = self.stream_file_to_location(&temporary, path) {
            let _ = self.delete_location_if_present(&temporary);
            return Err(error);
        }

        let copy_result = self.block_on(self.inner.copy_opts(
            &temporary,
            &location,
            CopyOptions::new().with_mode(CopyMode::Create),
        ));
        // Best-effort cleanup — ignore failure, data is at the canonical key.
        let _ = self.delete_location_if_present(&temporary);
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
        if let Some(_dest_meta) = self.metadata(destination)? {
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
                if self.metadata(destination)?.is_some() {
                    let _ = store.abort_multipart(&dst, &upload_id).await;
                    // Content is identical for content-addressed keys,
                    // so returning AlreadyExists is correct.
                    return Ok(PutOutcome::AlreadyExists);
                }
                match store.complete_multipart(&dst, &upload_id, part_ids).await {
                    Ok(_) => Ok(PutOutcome::Inserted),
                    Err(error) => {
                        let _ = store.abort_multipart(&dst, &upload_id).await;
                        Err(S3ObjectStoreError::External(error))
                    }
                }
            }
            Err(error) => {
                let _ = store.abort_multipart(&dst, &upload_id).await;
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
                let existing_length = self
                    .metadata(key)?
                    .ok_or(S3ObjectStoreError::ExistingObjectConflict)?
                    .length();
                existing_object_outcome(self, key, existing_length, bytes.as_ref(), integrity)
            }
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
            use futures_util::StreamExt;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                acc.extend_from_slice(&chunk);
            }
            Ok::<_, ExternalObjectStoreError>(acc)
        })?;
        Ok(bytes)
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
    /// An upload parts list had missing, duplicate, or out-of-order part numbers.
    #[error("upload parts list has invalid part numbering")]
    InvalidUploadParts,
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

    // Stream-compare in chunks to avoid loading the full object into memory.
    // Content-addressed keys always produce matching bytes for identical hashes,
    // so a full in-memory comparison is unnecessary and causes OOM at 5GiB+.
    let mut offset = 0_u64;
    while offset < existing_length {
        let remaining = existing_length
            .checked_sub(offset)
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        let to_read = remaining.min(STREAM_COMPARE_CHUNK_BYTES as u64);
        let end = offset
            .checked_add(to_read)
            .and_then(|value| value.checked_sub(1))
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        let range = ByteRange::new(offset, end)
            .map_err(|_error| S3ObjectStoreError::ExistingObjectConflict)?;
        let existing_chunk = store.read_range(key, range)?;
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
    use shardline_protocol::{ByteRange, ShardlineHash};

    use super::{
        S3ObjectStore, S3ObjectStoreConfig, S3ObjectStoreError, is_temp_upload_key,
        stream_payload_for_range, validated_external_range, normalize_prefix,
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

    #[test]
    fn s3_store_rejects_empty_bucket() {
        let config = S3ObjectStoreConfig::new(String::new(), "us-east-1".to_owned());
        let result = S3ObjectStore::new(config);
        assert!(matches!(result, Err(S3ObjectStoreError::EmptyBucket)));
    }

    #[test]
    fn s3_store_default_config_uses_standard_s3_endpoint() {
        let config = S3ObjectStoreConfig::new("my-bucket".to_owned(), "us-east-1".to_owned());
        // A default config (no endpoint, no credentials) builds the AmazonS3 client
        // successfully — it just won't be able to reach AWS without network.
        let store = S3ObjectStore::new(config);
        assert!(store.is_ok());
    }

    #[test]
    fn is_temp_upload_key_matches_standard_temp_format() {
        assert!(is_temp_upload_key("uploads/obj.tmp.42"));
        assert!(is_temp_upload_key("obj.tmp.0"));
        assert!(is_temp_upload_key("obj.tmp.999999"));
    }

    #[test]
    fn is_temp_upload_key_rejects_non_temp() {
        assert!(!is_temp_upload_key("uploads/obj"));
        assert!(!is_temp_upload_key("obj.tmp."));
        assert!(!is_temp_upload_key("obj.tmp.abc"));
        assert!(!is_temp_upload_key("obj.tmpx.42"));
    }

    #[test]
    fn validated_external_range_zero_length_rejected() {
        // ByteRange(5, 3) has start > end, which is invalid
        let result = shardline_protocol::ByteRange::new(5, 3);
        assert!(result.is_err());
    }

    #[test]
    fn s3_error_display_and_debug_are_implemented() {
        let err = S3ObjectStoreError::RangeOutOfBounds;
        let display = format!("{err}");
        assert!(!display.is_empty(), "Display should produce a message");

        let debug = format!("{err:?}");
        assert!(!debug.is_empty(), "Debug should produce a message");
    }

    #[test]
    fn s3_store_with_full_credentials_accepts_custom_session_token() {
        let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_credentials(
                Some("key".to_owned()),
                Some("secret".to_owned()),
                Some("token".to_owned()),
            );
        // Full credentials (key + secret + session token) should build successfully
        // without needing any external endpoint.
        let store = S3ObjectStore::new(config);
        assert!(store.is_ok());
    }

    // ── normalize_prefix ─────────────────────────────────────────────────

    #[test]
    fn normalize_prefix_trims_trailing_slashes() {
        assert_eq!(normalize_prefix("prefix/"), Some("prefix".to_owned()));
    }

    #[test]
    fn normalize_prefix_trims_leading_slashes() {
        assert_eq!(normalize_prefix("/prefix"), Some("prefix".to_owned()));
    }

    #[test]
    fn normalize_prefix_trims_both_slashes() {
        assert_eq!(normalize_prefix("/prefix/"), Some("prefix".to_owned()));
    }

    #[test]
    fn normalize_prefix_returns_none_for_empty_after_trim() {
        assert_eq!(normalize_prefix(""), None);
        assert_eq!(normalize_prefix("/"), None);
        assert_eq!(normalize_prefix("///"), None);
    }

    #[test]
    fn normalize_prefix_preserves_inner_slashes() {
        assert_eq!(
            normalize_prefix("/a/b/c/"),
            Some("a/b/c".to_owned())
        );
    }

    // ── validate_config ───────────────────────────────────────────────────

    #[test]
    fn validate_config_rejects_empty_region() {
        let config = S3ObjectStoreConfig::new("bucket".to_owned(), String::new());
        let result = S3ObjectStore::new(config);
        assert!(matches!(result, Err(S3ObjectStoreError::EmptyRegion)));
    }

    #[test]
    fn validate_config_rejects_empty_bucket() {
        let config = S3ObjectStoreConfig::new(String::new(), "region".to_owned());
        let result = S3ObjectStore::new(config);
        assert!(matches!(result, Err(S3ObjectStoreError::EmptyBucket)));
    }

    // ── S3ObjectStoreConfig accessors ──────────────────────────────────────

    #[test]
    fn s3_config_bucket_accessor() {
        let config = S3ObjectStoreConfig::new("my-bucket".to_owned(), "us-east-1".to_owned());
        assert_eq!(config.bucket(), "my-bucket");
    }

    #[test]
    fn s3_config_bucket_accessor_with_prefix() {
        let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_key_prefix(Some("pfx"));
        assert_eq!(config.key_prefix(), Some("pfx"));
    }

    #[test]
    fn s3_config_bucket_accessor_no_prefix() {
        let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned());
        assert_eq!(config.key_prefix(), None);
    }

    // ── S3ObjectStoreConfig Debug redaction ────────────────────────────────

    #[test]
    fn s3_config_debug_redacts_credentials() {
        let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_credentials(
                Some("access-key-123".to_owned()),
                Some("secret-key-456".to_owned()),
                Some("session-token-789".to_owned()),
            );
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("access-key-123"));
        assert!(!rendered.contains("secret-key-456"));
        assert!(!rendered.contains("session-token-789"));
        assert!(rendered.contains("***"));
    }

    // ── chunk_hash ────────────────────────────────────────────────────────

    #[test]
    fn chunk_hash_produces_consistent_result() {
        let a = super::chunk_hash(b"hello world");
        let b = super::chunk_hash(b"hello world");
        assert_eq!(a, b);
    }

    #[test]
    fn chunk_hash_differs_for_different_inputs() {
        let a = super::chunk_hash(b"abc");
        let b = super::chunk_hash(b"xyz");
        assert_ne!(a, b);
    }

    #[test]
    fn chunk_hash_handles_empty_input() {
        let hash = super::chunk_hash(b"");
        let expected = ShardlineHash::from_bytes(*blake3::hash(b"").as_bytes());
        assert_eq!(hash, expected);
    }

    // ── Error variant Display ─────────────────────────────────────────────

    #[test]
    fn s3_error_display_incomplete_credentials() {
        let err = S3ObjectStoreError::IncompleteCredentials;
        assert_eq!(
            err.to_string(),
            "s3 object store credentials must include both access key id and secret access key"
        );
    }

    #[test]
    fn s3_error_display_empty_bucket() {
        let err = S3ObjectStoreError::EmptyBucket;
        assert_eq!(err.to_string(), "s3 object store bucket must not be empty");
    }

    #[test]
    fn s3_error_display_empty_region() {
        let err = S3ObjectStoreError::EmptyRegion;
        assert_eq!(err.to_string(), "s3 object store region must not be empty");
    }

    #[test]
    fn s3_error_display_integrity_length_mismatch() {
        let err = S3ObjectStoreError::IntegrityLengthMismatch;
        assert_eq!(
            err.to_string(),
            "object body length did not match expected integrity"
        );
    }

    #[test]
    fn s3_error_display_integrity_hash_mismatch() {
        let err = S3ObjectStoreError::IntegrityHashMismatch;
        assert_eq!(
            err.to_string(),
            "object body hash did not match expected integrity"
        );
    }

    #[test]
    fn s3_error_display_existing_object_conflict() {
        let err = S3ObjectStoreError::ExistingObjectConflict;
        assert_eq!(
            err.to_string(),
            "object key already exists with conflicting bytes"
        );
    }

    #[test]
    fn s3_error_display_range_out_of_bounds() {
        let err = S3ObjectStoreError::RangeOutOfBounds;
        assert_eq!(
            err.to_string(),
            "requested byte range exceeded stored object length"
        );
    }

    #[test]
    fn s3_error_display_invalid_listed_key() {
        let err = S3ObjectStoreError::InvalidListedKey;
        assert_eq!(
            err.to_string(),
            "s3 listed an object outside the configured key prefix"
        );
    }

    #[test]
    fn s3_error_display_invalid_upload_parts() {
        let err = S3ObjectStoreError::InvalidUploadParts;
        assert_eq!(
            err.to_string(),
            "upload parts list has invalid part numbering"
        );
    }

    #[test]
    fn s3_error_display_runtime_unavailable() {
        let err = S3ObjectStoreError::RuntimeUnavailable;
        assert_eq!(
            err.to_string(),
            "s3 object store runtime is unavailable"
        );
    }

    #[test]
    fn s3_error_display_invalid_key_prefix() {
        use crate::ObjectPrefixError;
        let err = S3ObjectStoreError::InvalidKeyPrefix(ObjectPrefixError::UnsafePath);
        let msg = err.to_string();
        assert!(msg.contains("key prefix was invalid"));
    }

    #[test]
    fn s3_error_display_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let err = S3ObjectStoreError::Io(io_err);
        let msg = err.to_string();
        assert!(msg.contains("temporary file operation failed"));
    }

    #[test]
    fn s3_error_display_path() {
        use object_store::path::Error as PathError;
        let err = S3ObjectStoreError::Path(PathError::InvalidPath {
            path: "/bad".into(),
        });
        let msg = err.to_string();
        assert!(msg.contains("object-store path conversion failed"));
    }

    #[test]
    fn s3_error_display_runtime() {
        let io_err = std::io::Error::other("thread spawn failed");
        let err = S3ObjectStoreError::Runtime(io_err);
        let msg = err.to_string();
        assert!(msg.contains("s3 object store runtime initialization failed"));
    }

    #[test]
    fn s3_error_display_external() {
        let ext_err = object_store::Error::NotImplemented {
            operation: "test".to_owned(),
            implementer: "test".to_owned(),
        };
        let err = S3ObjectStoreError::External(ext_err);
        let msg = err.to_string();
        assert!(msg.contains("s3 object store operation failed"));
    }

    // ── S3ObjectStoreConfig builder ───────────────────────────────────────

    #[test]
    fn s3_config_with_virtual_hosted_style() {
        let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_virtual_hosted_style_request(true);
        let store = S3ObjectStore::new(config);
        // Should build successfully (the setting just changes how requests are made)
        assert!(store.is_ok());
    }

    // ── validated_external_range edge cases ────────────────────────────

    #[test]
    fn validated_external_range_single_byte() {
        let range = ByteRange::new(5, 5);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };

        let external = validated_external_range(range);

        assert!(external.is_ok());
        assert_eq!(external.ok(), Some(5..6));
    }

    #[test]
    fn validated_external_range_large_range() {
        // A large valid range
        let range = ByteRange::new(0, u64::MAX - 1).expect("valid large range");
        let external = validated_external_range(range);
        // Should succeed
        assert!(external.is_ok());
        assert_eq!(external.ok(), Some(0..u64::MAX));
    }

    // ── S3MultipartUploadWriter via InMemory ──────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn s3_multipart_upload_writer_write_and_abort() {
        use object_store::ObjectStoreExt;
        use object_store::WriteMultipart;

        let store = InMemory::new();
        let location = ObjectStorePath::from("test/multi-part-obj");

        let upload = store
            .put_multipart(&location)
            .await
            .expect("should create multipart upload");
        let writer =
            WriteMultipart::new_with_chunk_size(upload, 8 * 1024 * 1024);
        let mut multipart = super::S3MultipartUploadWriter { writer };

        // Write some bytes
        multipart.write(b"hello multipart");
        // Wait with zero permits — should be fine since nothing is in flight
        let wait = multipart.wait_for_capacity(0).await;
        assert!(wait.is_ok());

        // Abort before finish — should succeed
        let abort = multipart.abort().await;
        assert!(abort.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn s3_multipart_upload_writer_abort_unstarted() {
        use object_store::ObjectStoreExt;
        use object_store::WriteMultipart;

        let store = InMemory::new();
        let location = ObjectStorePath::from("test/abort-unstarted");

        let upload = store
            .put_multipart(&location)
            .await
            .expect("should create multipart upload");
        let writer =
            WriteMultipart::new_with_chunk_size(upload, 8 * 1024 * 1024);
        let multipart = super::S3MultipartUploadWriter { writer };

        // Abort immediately without writing anything
        let abort = multipart.abort().await;
        assert!(abort.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn s3_multipart_upload_writer_wait_with_zero_permits() {
        use object_store::ObjectStoreExt;
        use object_store::WriteMultipart;

        let store = InMemory::new();
        let location = ObjectStorePath::from("test/zero-permits");

        let upload = store
            .put_multipart(&location)
            .await
            .expect("should create multipart upload");
        let writer =
            WriteMultipart::new_with_chunk_size(upload, 8 * 1024 * 1024);
        let mut multipart = super::S3MultipartUploadWriter { writer };

        // wait_for_capacity with 0 permits is valid when no parts are queued
        let wait = multipart.wait_for_capacity(0).await;
        assert!(wait.is_ok());
    }

    // ── S3ObjectStoreConfig builder methods ────────────────────────────────

    #[test]
    fn s3_config_with_virtual_hosted_style_true() {
        let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_virtual_hosted_style_request(true);
        let store = S3ObjectStore::new(config);
        assert!(store.is_ok());
    }

    #[test]
    fn s3_config_with_virtual_hosted_style_false() {
        let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_virtual_hosted_style_request(false);
        let store = S3ObjectStore::new(config);
        assert!(store.is_ok());
    }

    #[test]
    fn s3_config_with_key_prefix_none_is_noop() {
        let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_key_prefix(None);
        assert_eq!(config.key_prefix(), None);
    }

    #[test]
    fn s3_config_with_key_prefix_trimmed() {
        let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_key_prefix(Some("/tenant-b/"));
        assert_eq!(config.key_prefix(), Some("tenant-b"));
    }

    // ── streaming_large_copy config validation ─────────────────────────────

    #[test]
    fn streaming_large_copy_rejects_empty_bucket_config() {
        let config = S3ObjectStoreConfig::new(String::new(), "r".to_owned());
        let result = S3ObjectStore::new(config);
        assert!(matches!(result, Err(S3ObjectStoreError::EmptyBucket)));
    }

    #[test]
    fn streaming_large_copy_rejects_empty_region_config() {
        let config = S3ObjectStoreConfig::new("b".to_owned(), String::new());
        let result = S3ObjectStore::new(config);
        assert!(matches!(result, Err(S3ObjectStoreError::EmptyRegion)));
    }

    // ── Incomplete credentials rejection ──────────────────────────────────

    #[test]
    fn s3_store_rejects_incomplete_credentials_key_without_secret() {
        let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_credentials(Some("key".to_owned()), None, None);
        let store = S3ObjectStore::new(config);
        assert!(matches!(store, Err(S3ObjectStoreError::IncompleteCredentials)));
    }

    #[test]
    fn s3_store_rejects_incomplete_credentials_secret_without_key() {
        let config = S3ObjectStoreConfig::new("b".to_owned(), "r".to_owned())
            .with_credentials(None, Some("secret".to_owned()), None);
        let store = S3ObjectStore::new(config);
        assert!(matches!(store, Err(S3ObjectStoreError::IncompleteCredentials)));
    }

    // ── MinIO-backed integration tests ───────────────────────────────────

    mod minio_tests {
        use std::sync::atomic::{AtomicBool, Ordering};

        use shardline_test_support::DockerLocalStack;
        use shardline_test_support::S3RawConfig;

        use super::super::{
            BeginMultipartUploadResult, S3ObjectStore, S3ObjectStoreConfig,
            S3ObjectStoreError,
        };
        use crate::{
            DeleteOutcome, ObjectBody, ObjectIntegrity, ObjectKey, ObjectPrefix,
            ObjectStore as ObjectStoreTrait, PutOutcome,
        };
        use shardline_protocol::ByteRange;

        /// Shared MinIO init guard: only starts containers once across all tests.
        static MINIO_INIT: AtomicBool = AtomicBool::new(false);
        static INIT_LOCK: std::sync::OnceLock<()> = std::sync::OnceLock::new();

        fn ensure_minio() -> Option<DockerLocalStack> {
            if !DockerLocalStack::docker_available() {
                return None;
            }
            INIT_LOCK.get_or_init(|| {
                MINIO_INIT.store(true, Ordering::SeqCst);
            });
            if !MINIO_INIT.load(Ordering::SeqCst) {
                return None;
            }
            // Start fresh stack per-test-call so we get clean state.
            DockerLocalStack::builder()
                .with_minio()
                .start()
                .ok()
                .flatten()
        }

        fn build_s3_store(stack: &DockerLocalStack, key_prefix: Option<&str>) -> S3ObjectStore {
            let raw: S3RawConfig = stack.s3_raw_config(key_prefix).unwrap();
            let config = S3ObjectStoreConfig::new(raw.bucket, raw.region)
                .with_endpoint(raw.endpoint)
                .with_allow_http(raw.allow_http)
                .with_credentials(raw.access_key, raw.secret_key, raw.session_token)
                .with_key_prefix(raw.key_prefix.as_deref());
            S3ObjectStore::new(config).unwrap()
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn minio_put_and_get_roundtrip() {
            let stack = match ensure_minio() {
                Some(s) => s,
                None => return,
            };
            let prefix = stack.unique_s3_key_prefix("test-roundtrip");
            let store = build_s3_store(&stack, Some(&prefix));
            let key = ObjectKey::parse("objects/hash.xorb").unwrap();
            let body = b"hello minio";
            let integrity = ObjectIntegrity::new(super::super::chunk_hash(body), 11);

            let inserted = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
            assert!(
                matches!(inserted, Ok(PutOutcome::Inserted)),
                "expected Ok(Inserted), got {inserted:?}"
            );

            let contains = store.contains(&key);
            assert!(matches!(contains, Ok(true)));

            let read = store.read_range(&key, ByteRange::new(0, 10).unwrap());
            assert!(read.is_ok());
            assert_eq!(read.unwrap(), b"hello minio");

            let deleted = store.delete_if_present(&key);
            assert!(matches!(deleted, Ok(DeleteOutcome::Deleted)));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn minio_put_if_absent_idempotent() {
            let stack = match ensure_minio() {
                Some(s) => s,
                None => return,
            };
            let store = build_s3_store(&stack, Some("test-idempotent"));
            let key = ObjectKey::parse("objects/same.xorb").unwrap();
            let body = b"same content";
            let integrity = ObjectIntegrity::new(super::super::chunk_hash(body), 12);

            let first = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
            assert!(matches!(first, Ok(PutOutcome::Inserted)));

            let second = store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity);
            assert!(matches!(second, Ok(PutOutcome::AlreadyExists)));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn minio_list_flat_namespace_page_pagination() {
            let stack = match ensure_minio() {
                Some(s) => s,
                None => return,
            };
            let prefix = stack.unique_s3_key_prefix("test-list-page");
            let store = build_s3_store(&stack, Some(&prefix));
            let ns_prefix = ObjectPrefix::parse("ns/").unwrap();

            // Insert three objects under the same flat namespace
            for i in 0..3u64 {
                let key = ObjectKey::parse(&format!("ns/key{i:020}")).unwrap();
                let body = b"data";
                let integrity = ObjectIntegrity::new(super::super::chunk_hash(body), 4);
                store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity).unwrap();
            }

            // Full listing
            let all = store.list_flat_namespace_page(&ns_prefix, None, 10).unwrap();
            assert_eq!(all.len(), 3, "should list all 3 objects");

            // Pagination with start_after
            let after_first = ObjectKey::parse("ns/key00000000000000000000").unwrap();
            let page = store.list_flat_namespace_page(&ns_prefix, Some(&after_first), 10).unwrap();
            assert_eq!(page.len(), 2, "should list remaining 2 objects after first key");

            // Limit
            let limited = store.list_flat_namespace_page(&ns_prefix, None, 2).unwrap();
            assert_eq!(limited.len(), 2, "should be limited to 2");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn minio_visit_prefix_with_multiple_entries() {
            let stack = match ensure_minio() {
                Some(s) => s,
                None => return,
            };
            let prefix = stack.unique_s3_key_prefix("test-visit-prefix");
            let store = build_s3_store(&stack, Some(&prefix));
            let ns_prefix = ObjectPrefix::parse("vp/").unwrap();

            for i in 0..5u64 {
                let key = ObjectKey::parse(&format!("vp/obj{i:010}")).unwrap();
                let body = b"x";
                let integrity = ObjectIntegrity::new(super::super::chunk_hash(body), 1);
                store.put_if_absent(&key, ObjectBody::from_slice(body), &integrity).unwrap();
            }

            let mut visited = Vec::new();
            let result: Result<(), S3ObjectStoreError> = store.visit_prefix(&ns_prefix, |meta| {
                visited.push(meta.key().clone());
                Ok(())
            });
            assert!(result.is_ok());
            assert_eq!(visited.len(), 5, "visit_prefix should visit all 5 entries");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn minio_begin_and_finish_content_addressed_upload() {
            let stack = match ensure_minio() {
                Some(s) => s,
                None => return,
            };
            let store = build_s3_store(&stack, Some("test-ca-upload"));
            let canonical = ObjectKey::parse("cas/deadbeef.xorb").unwrap();
            let body = b"content addressed payload";

            let begin = store.begin_content_addressed_upload(&canonical).await;
            assert!(begin.is_ok());
            let Ok(begin) = begin else { return };

            match begin {
                BeginMultipartUploadResult::AlreadyExists => {
                    // Someone else got there first — that's fine
                }
                BeginMultipartUploadResult::Upload(mut writer, temp_key) => {
                    writer.write(body);
                    writer.wait_for_capacity(4).await.unwrap();

                    let finish = store
                        .finish_content_addressed_upload(writer, &temp_key, &canonical)
                        .await;
                    assert!(finish.is_ok());
                    let outcome = finish.unwrap();
                    // Could be Inserted or AlreadyExists depending on timing
                    assert!(
                        outcome == PutOutcome::Inserted || outcome == PutOutcome::AlreadyExists,
                        "expected Inserted or AlreadyExists, got {outcome:?}"
                    );
                }
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn minio_put_content_addressed_file() {
            let stack = match ensure_minio() {
                Some(s) => s,
                None => return,
            };
            let store = build_s3_store(&stack, Some("test-put-ca-file"));
            let key = ObjectKey::parse("cas/filehash.xorb").unwrap();

            let tmp = tempfile::NamedTempFile::new().unwrap();
            let body = b"file content for ca test";
            std::fs::write(tmp.path(), body).unwrap();

            let integrity = ObjectIntegrity::new(super::super::chunk_hash(body), body.len() as u64);
            let result = store.put_content_addressed_file(&key, tmp.path(), &integrity);
            assert!(result.is_ok());

            // Verify it's readable
            let read = store.read_range(&key, ByteRange::new(0, body.len() as u64 - 1).unwrap()).unwrap();
            assert_eq!(read, body);

            // Second put is idempotent
            let second = store.put_content_addressed_file(&key, tmp.path(), &integrity);
            assert!(matches!(second, Ok(PutOutcome::AlreadyExists)));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn minio_put_file_if_absent() {
            let stack = match ensure_minio() {
                Some(s) => s,
                None => return,
            };
            let store = build_s3_store(&stack, Some("test-put-file-absent"));
            let key = ObjectKey::parse("files/test.bin").unwrap();

            let tmp = tempfile::NamedTempFile::new().unwrap();
            let body = b"file content for put_file_if_absent";
            std::fs::write(tmp.path(), body).unwrap();

            let integrity = ObjectIntegrity::new(super::super::chunk_hash(body), body.len() as u64);
            let result = store.put_file_if_absent(&key, tmp.path(), &integrity);
            assert!(result.is_ok());

            let read = store.read_range(&key, ByteRange::new(0, body.len() as u64 - 1).unwrap()).unwrap();
            assert_eq!(read, body);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn minio_copy_object_if_absent() {
            let stack = match ensure_minio() {
                Some(s) => s,
                None => return,
            };
            let store = build_s3_store(&stack, Some("test-copy-obj"));
            let src = ObjectKey::parse("src/original.xorb").unwrap();
            let dst = ObjectKey::parse("dst/copy.xorb").unwrap();

            let body = b"copy test data";
            let integrity = ObjectIntegrity::new(super::super::chunk_hash(body), 14);
            store.put_if_absent(&src, ObjectBody::from_slice(body), &integrity).unwrap();

            let copy = store.copy_object_if_absent(&src, &dst);
            assert!(matches!(copy, Ok(PutOutcome::Inserted)));

            let idempotent = store.copy_object_if_absent(&src, &dst);
            assert!(matches!(idempotent, Ok(PutOutcome::AlreadyExists)));
        }
    }
}
