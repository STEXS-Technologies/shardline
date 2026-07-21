use std::{
    fmt,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures_util::Stream;
use object_store::WriteMultipart;

use crate::ObjectKey;

use super::S3ObjectStoreError;

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
    pub(crate) writer: WriteMultipart,
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

impl fmt::Debug for S3MultipartUploadWriter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3MultipartUploadWriter")
            .finish_non_exhaustive()
    }
}

pub(crate) const STREAM_UPLOAD_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const STREAM_COMPARE_CHUNK_BYTES: usize = 256 * 1024;
/// Maximum object size that S3's single-part COPY supports (5 GiB).
pub(crate) const MAX_SINGLE_COPY_BYTES: u64 = 5 * 1024 * 1024 * 1024;
/// Chunk size used when copying objects >5 GiB via streaming multipart.
pub(crate) const LARGE_COPY_CHUNK_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB
pub(crate) static TEMP_UPLOAD_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generates a unique temp key derived from a canonical key using a monotonic
/// counter and nanosecond timestamp.
pub(crate) fn temp_key_for(key: &ObjectKey) -> Result<ObjectKey, S3ObjectStoreError> {
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
pub(crate) fn is_temp_upload_key(key: &str) -> bool {
    key.find(".tmp.").is_some_and(|pos| {
        // SAFETY: pos comes from find(".tmp.") which matches 5 chars,
        // so pos + 5 is always <= key.len(). The get() call is a safety
        // net — it will never return None in practice.
        key.as_bytes()
            .get(pos.saturating_add(5))
            .is_some_and(|b| b.is_ascii_digit())
    })
}

pub(crate) fn normalize_prefix(value: &str) -> Option<String> {
    let trimmed = value.trim_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}
