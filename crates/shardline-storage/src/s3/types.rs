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
/// Upper bound (seconds) for an S3 temp-upload artifact before it is reaped
/// by `S3ObjectStore::sweep_stale_temp_keys`.
///
/// Temp-then-copy uploads complete in seconds-to-minutes, so a `.tmp.` or
/// `__tmp/shardline-stream-upload/` artifact older than an hour is a stranded
/// remnant of a killed/crashed writer — never an in-flight write.
pub(crate) const S3_TEMP_ARTIFACT_AGE_SECONDS: u64 = 60 * 60;
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

/// Returns `true` if `key` matches one of the EXACT temp-upload grammars this
/// adapter generates — never a loose substring heuristic, so a user object
/// whose key merely contains `.tmp.<digits>` (dots are legal in S3 keys, e.g.
/// `data.tmp.1`) is not shadowed from listings or GC enumeration (F-49).
///
/// The generated grammars are:
/// (a) `<base>.tmp.<counter>.<pid>.<nanos>` — the `put_overwrite` and
///     content-addressed multipart temp suffix ([`temp_key_for`]), where the
///     counter is a monotonic `u64`, the pid is a `u32`, and the nanos is a
///     plausible unix-nanosecond timestamp (`>= 10^15`); and
/// (b) `__tmp/shardline-stream-upload/<nanos>-<pid>-<counter>` — the
///     stream-upload temp path ([`temporary_upload_location`]), inside the
///     reserved `__tmp/` namespace.
pub(crate) fn is_temp_upload_key(key: &str) -> bool {
    is_overwrite_temp_key(key) || is_stream_upload_temp_key(key)
}

/// Matches grammar (a): `<base>.tmp.<counter>.<pid>.<nanos>` with a
/// non-empty `<base>`. The final `.tmp.` is the delimiter (a base key may
/// itself contain `.tmp.`), and the three trailing groups must match the
/// EXACT generated shapes — not just any three decimal groups: the counter
/// must fit `u64`, the pid must fit `u32` (it comes from
/// [`std::process::id`]), and the nanos must be a plausible unix-nanosecond
/// timestamp (`10^15 <= nanos < 10^20`; the epoch clock's value since early
/// 1970, and generated values are ~1.7e18 today, staying 19-digit for ~300
/// years). User keys whose suffixes are three all-digit groups — date stamps
/// like `report.tmp.2026.01.15` / `backup.tmp.2026.08.16`, version tags like
/// `data.tmp.001.002.003`, `photos.tmp.1.2.3`, or OCI tags like
/// `v1.tmp.2026.01.15` — never match the generated shapes, so they are not
/// shadowed from listings and never reaped by the GC sweep (F-56).
fn is_overwrite_temp_key(key: &str) -> bool {
    let Some((base, suffix)) = key.rsplit_once(".tmp.") else {
        return false;
    };
    if base.is_empty() {
        return false;
    }
    let mut groups = suffix.split('.');
    let counter = groups.next();
    let pid = groups.next();
    let nanos = groups.next();
    if groups.next().is_some() {
        return false;
    }
    matches!(
        (counter, pid, nanos),
        (Some(counter), Some(pid), Some(nanos))
            if is_all_digits(counter)
                && is_all_digits(pid)
                && is_all_digits(nanos)
                && is_plausible_u64(counter)
                && is_plausible_u32(pid)
                && is_plausible_unix_nanos(nanos)
    )
}

/// True when `value` is a decimal number within `u64` range — the generated
/// counter comes from a monotonic `u64` atomic.
fn is_plausible_u64(value: &str) -> bool {
    value.parse::<u64>().is_ok()
}

/// True when `value` is a decimal number within `u32` range — the generated
/// pid is [`std::process::id`], a `u32`.
fn is_plausible_u32(value: &str) -> bool {
    value.parse::<u32>().is_ok()
}

/// True when `value` is a plausible unix-nanosecond timestamp: `10^15 <=
/// nanos < 10^20` — the value [`SystemTime::now`] has held since early 1970.
/// 16-digit dates like `2026.01.15` parse as decimal but fall below `10^15`.
fn is_plausible_unix_nanos(value: &str) -> bool {
    matches!(
        value.parse::<u128>(),
        Ok(nanos) if (1_000_000_000_000_000..100_000_000_000_000_000_000).contains(&nanos)
    )
}

/// Matches grammar (b): the reserved `__tmp/shardline-stream-upload/` prefix
/// followed by `<nanos>-<pid>-<counter>` (three decimal-digit groups separated
/// by single `-`).
fn is_stream_upload_temp_key(key: &str) -> bool {
    const PREFIX: &str = "__tmp/shardline-stream-upload/";
    let Some(rest) = key.strip_prefix(PREFIX) else {
        return false;
    };
    let mut groups = rest.split('-');
    let nanos = groups.next();
    let pid = groups.next();
    let counter = groups.next();
    if groups.next().is_some() {
        return false;
    }
    matches!(
        (nanos, pid, counter),
        (Some(nanos), Some(pid), Some(counter))
            if is_all_digits(nanos) && is_all_digits(pid) && is_all_digits(counter)
    )
}

fn is_all_digits(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

pub(crate) fn normalize_prefix(value: &str) -> Option<String> {
    let trimmed = value.trim_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}
