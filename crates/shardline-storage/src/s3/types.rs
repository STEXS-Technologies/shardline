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
/// Temp-then-copy uploads complete in seconds-to-minutes, so a
/// `__tmp/shardline-overwrite/` or `__tmp/shardline-stream-upload/` artifact
/// older than an hour is a stranded remnant of a killed/crashed writer —
/// never an in-flight write.
pub(crate) const S3_TEMP_ARTIFACT_AGE_SECONDS: u64 = 60 * 60;
pub(crate) static TEMP_UPLOAD_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The reserved namespace for overwrite/content-addressed temp artifacts.
///
/// User keys can never reach it: every user key lives under a managed prefix
/// (`protocols/...`, `oci/...`, `xorbs/...`), and S3 object keys in particular
/// live under `protocols/s3/{scope_namespace}/{key}` — they can never start
/// with `__tmp/`. Restricting the generated temps to this namespace
/// eliminates the shape-collision class entirely: `is_temp_upload_key` matches
/// ONLY the reserved namespace, so a user key that merely *resembles* a
/// generated temp (e.g. `data.tmp.1.1.1750000000000000000`, date stamps, OCI
/// version tags) is never shadowed from listings and never reaped by the GC
/// sweep (F-56 / F-80).
pub(crate) const OVERWRITE_TEMP_KEY_PREFIX: &str = "__tmp/shardline-overwrite/";

/// Generates a unique temp key in the reserved `__tmp/shardline-overwrite/`
/// namespace, derived from a canonical key using a monotonic counter and
/// nanosecond timestamp.
///
/// The generated shape is
/// `__tmp/shardline-overwrite/<key-digest>.<counter>.<pid>.<nanos>` where the
/// key digest is the lowercase hex BLAKE3 hash of the base key (so a stranded
/// temp stays traceable to its base), the counter is a monotonic `u64`, the
/// pid is a `u32`, and the nanos is a unix-nanosecond timestamp. The key
/// digest makes the artifact unique per base key; the counter keeps it unique
/// across subsequent temps for the same base.
pub(crate) fn temp_key_for(key: &ObjectKey) -> Result<ObjectKey, S3ObjectStoreError> {
    let counter = TEMP_UPLOAD_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = std::process::id();
    let key_digest = blake3::hash(key.as_str().as_bytes());
    let relative = format!("{OVERWRITE_TEMP_KEY_PREFIX}{key_digest}.{counter}.{pid}.{now_nanos}");
    ObjectKey::parse(&relative).map_err(|_err| S3ObjectStoreError::InvalidListedKey)
}

/// Returns `true` if `key` matches one of the EXACT temp-upload grammars this
/// adapter generates — never a loose substring heuristic, so a user object
/// whose key merely contains `.tmp.<digits>` (dots are legal in S3 keys, e.g.
/// `data.tmp.1`) is not shadowed from listings or GC enumeration (F-49).
///
/// The generated grammars are both inside the RESERVED `__tmp/` namespace that
/// user keys can never reach:
/// (a) `__tmp/shardline-overwrite/<key-digest>.<counter>.<pid>.<nanos>` — the
///     `put_overwrite` and content-addressed multipart temp key
///     ([`temp_key_for`]), where the key digest is 64 lowercase hex chars and
///     the three trailing groups are decimal digit groups; and
/// (b) `__tmp/shardline-stream-upload/<nanos>-<pid>-<counter>` — the
///     stream-upload temp path ([`temporary_upload_location`]).
///
/// Because the reserved prefix is the discriminator (F-80), the numeric-range
/// heuristics of the old `<base>.tmp.<counter>.<pid>.<nanos>` grammar are
/// gone: a user key shaped exactly like the old generated temps — e.g.
/// `data.tmp.1.1.1750000000000000000`, `report.tmp.2026.01.15`, or an OCI tag
/// `v1.tmp.123.456.1750000000000000000` — never matches and is never swept.
pub(crate) fn is_temp_upload_key(key: &str) -> bool {
    is_overwrite_temp_key(key) || is_stream_upload_temp_key(key)
}

/// Matches grammar (a): the reserved `__tmp/shardline-overwrite/` prefix
/// followed by `<key-digest>.<counter>.<pid>.<nanos>` — exactly four
/// dot-separated groups, a 64-hex key digest and three all-digit groups. The
/// reserved prefix is the discriminator: no user key can start with it, so the
/// tail check only tightens the match against hand-placed lookalikes; it never
/// classifies a user key.
fn is_overwrite_temp_key(key: &str) -> bool {
    let Some(rest) = key.strip_prefix(OVERWRITE_TEMP_KEY_PREFIX) else {
        return false;
    };
    let mut groups = rest.split('.');
    let key_digest = groups.next();
    let counter = groups.next();
    let pid = groups.next();
    let nanos = groups.next();
    if groups.next().is_some() {
        return false;
    }
    matches!(
        (key_digest, counter, pid, nanos),
        (Some(key_digest), Some(counter), Some(pid), Some(nanos))
            if is_64_hex(key_digest)
                && is_all_digits(counter)
                && is_all_digits(pid)
                && is_all_digits(nanos)
    )
}

/// True when `value` is exactly 64 ASCII hex characters — the shape
/// [`blake3::Hash`] formats to (lowercase hex).
fn is_64_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
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
