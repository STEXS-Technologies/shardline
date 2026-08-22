//! S3 multipart upload session model and disk-persisted session store.
//!
//! A multipart upload is a [`MultipartUploadSession`] persisted as JSON under
//! the server root directory (`<root>/s3-uploads/<upload_id>/session.json`),
//! with each uploaded part stored as a `part-{n}` file in the same directory
//! (mirroring the OCI upload-session pattern). Sessions carry a TTL, are
//! bounded by a max-active count, and are swept at startup and on creation.
//!
//! The store lives in this adapter (never depending on `shardline-server`);
//! the server wires it into the S3 handlers and streams request bodies to the
//! part files it resolves.
//!
//! # Path safety
//!
//! The only client-influenced path component is the upload id, which is
//! validated as lowercase hex (or `-`) of at most 64 bytes, and part numbers,
//! which are validated `1..=MAX_S3_PART_NUMBER`. Both are safe path
//! components, so session paths cannot escape the upload root.

use std::{
    collections::{BTreeMap, HashMap},
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Weak},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{fs, sync::Mutex, task::spawn_blocking};

/// The session directory name under the server root directory.
pub(crate) const S3_UPLOAD_DIR: &str = "s3-uploads";

/// The S3 protocol maximum part number.
pub const MAX_S3_PART_NUMBER: u32 = 10_000;

/// Maximum upload id length (hex upload ids are 32 chars).
const MAX_UPLOAD_ID_BYTES: usize = 64;

/// Serializes session-store mutations across the process.
static S3_UPLOAD_SESSION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Per-upload-session part-write locks keyed by upload id (weak values).
///
/// The process-wide [`S3_UPLOAD_SESSION_LOCK`] must never be held across a
/// network body stream (a slow `UploadPart` would stall every other tenant's
/// session operation), but a part-file write still needs to be exclusive with
/// the expiry sweep — which can delete the session directory — and with
/// `CompleteMultipartUpload`, which reads the part files. This per-session
/// lock provides that exclusivity without serializing unrelated sessions.
///
/// Entries hold weak references: the strong [`Arc`] returned by
/// [`acquire_session_part_lock`] keeps a session's entry alive for as long as
/// a guard is held (part write, completion ingest, or sweep delete), and dead
/// entries are evicted on the next acquire, so the map is bounded by the
/// number of sessions with work in flight (F-10).
static S3_UPLOAD_SESSION_PART_LOCKS: LazyLock<std::sync::Mutex<HashMap<String, Weak<Mutex<()>>>>> =
    LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

/// Returns the per-session part-write lock for an upload id, creating it on
/// first use.
///
/// Concurrent callers for the SAME upload id (concurrent `UploadPart`s, a
/// `CompleteMultipartUpload` reading part files, and the expiry sweep deleting
/// a session directory) receive the SAME mutex while any of them holds a
/// guard, so part files are never written, read, and removed concurrently.
/// Lock-ordering rule: never await [`lock_upload_sessions`] while holding the
/// guard returned here (the sweep takes both in the opposite order).
pub fn acquire_session_part_lock(upload_id: &str) -> Arc<Mutex<()>> {
    let mut map = S3_UPLOAD_SESSION_PART_LOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // Fast path: a live weak handle exists (a guard is still held for this
    // session), so return the same strong Arc to keep the write/read/delete
    // serialized.
    if let Some(live) = map.get(upload_id).and_then(Weak::upgrade) {
        return live;
    }
    // No live handle: drop dead entries so the map cannot grow with finished
    // sessions, then install a fresh mutex and return its strong Arc.
    map.retain(|_id, weak| weak.upgrade().is_some());
    let fresh = Arc::new(Mutex::new(()));
    map.insert(upload_id.to_owned(), Arc::downgrade(&fresh));
    fresh
}

/// S3 multipart upload session persistence failure.
#[derive(Debug, Error)]
pub enum S3SessionError {
    /// Local filesystem I/O failed.
    #[error("s3 upload session io failed")]
    Io(#[from] std::io::Error),
    /// Session JSON serialization or deserialization failed.
    #[error("s3 upload session json failed")]
    Json(#[from] serde_json::Error),
    /// The referenced upload session does not exist (or has expired).
    #[error("s3 upload session not found")]
    NotFound,
    /// The upload id is not a safe path component.
    #[error("s3 upload session id is invalid")]
    InvalidUploadId,
    /// The part number is outside `1..=10000`.
    #[error("s3 upload part number is out of range")]
    InvalidPartNumber,
    /// The completed upload is missing a part.
    #[error("s3 upload part {0} is missing")]
    MissingPart(u32),
    /// The maximum number of active upload sessions was reached.
    #[error("too many active s3 upload sessions")]
    TooManySessions,
    /// The upload session exceeded its aggregate byte quota.
    #[error("s3 upload session byte quota exceeded")]
    SessionQuotaExceeded,
    /// The aggregate byte quota across active sessions was exceeded.
    #[error("s3 upload aggregate byte quota exceeded")]
    AggregateQuotaExceeded,
    /// The global cap on part files across active sessions was exceeded.
    #[error("too many active s3 upload part files")]
    TooManyPartFiles,
    /// Numeric conversion exceeded supported bounds.
    #[error("s3 upload session overflow")]
    Overflow,
    /// A blocking worker task failed.
    #[error("s3 upload session blocking task failed")]
    BlockingTask(#[source] tokio::task::JoinError),
}

/// One stored part of a multipart upload session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultipartPart {
    /// The part byte length recorded at upload time.
    pub size_bytes: u64,
    /// The part file name relative to the session directory (`part-{n}`).
    pub file_name: String,
}

/// A disk-persisted S3 multipart upload session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultipartUploadSession {
    /// The bucket name (`{owner}.{name}`) the upload belongs to.
    pub bucket: String,
    /// The client-facing S3 object key.
    pub key: String,
    /// The sha256 repository-scope namespace of the target object.
    pub scope_namespace: String,
    /// The opaque upload id (also the session directory name).
    pub upload_id: String,
    /// Uploaded parts keyed by part number (order-preserving).
    pub parts: BTreeMap<u32, MultipartPart>,
    /// S3 user metadata (`x-amz-meta-*`) supplied at CreateMultipartUpload,
    /// stored as sorted `(name, value)` pairs and applied at completion.
    /// `#[serde(default)]` keeps pre-existing session files readable.
    #[serde(default)]
    pub user_metadata: Vec<(String, String)>,
    /// Unix seconds when the session was created.
    pub created_at_unix_seconds: u64,
    /// Unix seconds of the last part write (diagnostics; expiry is anchored to
    /// [`Self::created_at_unix_seconds`] so keep-alive parts cannot extend the
    /// session lifetime indefinitely).
    pub last_touched_unix_seconds: u64,
}

/// A held session-store lock (process mutex + advisory file lock).
pub struct S3UploadSessionLock {
    _process_guard: MutexGuard<'static, ()>,
    _file_lock: S3FileLock,
}

type MutexGuard<'lock, T> = tokio::sync::MutexGuard<'lock, T>;

pub(crate) struct S3FileLock {
    file: std::fs::File,
}

/// Cross-process advisory lock protecting one multipart session's part files.
///
/// The process-local per-session mutex remains useful for cheap serialization,
/// while this guard extends the same exclusion to replicas sharing the upload
/// root on a filesystem with advisory-lock support.
pub struct S3SessionPartFileLock {
    _file_lock: S3FileLock,
}

impl Drop for S3FileLock {
    fn drop(&mut self) {
        let _ignored = self.file.unlock();
    }
}

/// Generates a new random upload id (32 lowercase hex chars).
#[must_use]
pub fn new_upload_id() -> String {
    let mut bytes = [0_u8; 16];
    if getrandom::fill(&mut bytes).is_ok() {
        return hex::encode(bytes);
    }
    // Extremely unlikely fallback: process id + timestamp in hex.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    format!("{:x}-{:x}", std::process::id(), nanos)
}

/// Validates that an upload id is a safe path component (hex or `-`).
///
/// # Errors
///
/// Returns [`S3SessionError::InvalidUploadId`] when the id is empty, too long,
/// or contains characters outside `[0-9a-f-]`.
pub fn validate_upload_id(upload_id: &str) -> Result<(), S3SessionError> {
    if upload_id.is_empty()
        || upload_id.len() > MAX_UPLOAD_ID_BYTES
        || !upload_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
    {
        return Err(S3SessionError::InvalidUploadId);
    }
    Ok(())
}

/// Validates an S3 part number against the protocol bounds.
///
/// # Errors
///
/// Returns [`S3SessionError::InvalidPartNumber`] when the part number is zero
/// or exceeds [`MAX_S3_PART_NUMBER`].
pub const fn validate_part_number(part_number: u32) -> Result<(), S3SessionError> {
    if part_number == 0 || part_number > MAX_S3_PART_NUMBER {
        Err(S3SessionError::InvalidPartNumber)
    } else {
        Ok(())
    }
}

// ── Path helpers ─────────────────────────────────────────────────────────────

/// The upload root directory (`<root>/s3-uploads`).
#[must_use]
pub fn upload_dir(root: &Path) -> PathBuf {
    root.join(S3_UPLOAD_DIR)
}

/// The per-session directory (`<root>/s3-uploads/<upload_id>`).
///
/// # Errors
///
/// Returns [`S3SessionError::InvalidUploadId`] when the id is not a safe path
/// component.
pub fn session_dir(root: &Path, upload_id: &str) -> Result<PathBuf, S3SessionError> {
    validate_upload_id(upload_id)?;
    Ok(upload_dir(root).join(upload_id))
}

/// The session metadata path (`session.json` inside the session directory).
///
/// # Errors
///
/// Returns [`S3SessionError::InvalidUploadId`] when the id is not a safe path
/// component.
pub fn session_metadata_path(root: &Path, upload_id: &str) -> Result<PathBuf, S3SessionError> {
    Ok(session_dir(root, upload_id)?.join("session.json"))
}

/// The `part-{n}` file path for one part of a session.
///
/// # Errors
///
/// Returns [`S3SessionError::InvalidUploadId`] or
/// [`S3SessionError::InvalidPartNumber`] when the id or part number is invalid.
pub fn part_file_path(
    root: &Path,
    upload_id: &str,
    part_number: u32,
) -> Result<PathBuf, S3SessionError> {
    validate_upload_id(upload_id)?;
    validate_part_number(part_number)?;
    Ok(session_dir(root, upload_id)?.join(format!("part-{part_number}")))
}

// ── Locking ──────────────────────────────────────────────────────────────────

/// Acquires the process-wide session lock plus an advisory file lock.
///
/// # Errors
///
/// Returns [`S3SessionError::BlockingTask`] or [`S3SessionError::Io`] when the
/// file lock cannot be acquired.
pub async fn lock_upload_sessions(root: &Path) -> Result<S3UploadSessionLock, S3SessionError> {
    let process_guard = S3_UPLOAD_SESSION_LOCK.lock().await;
    let file_lock = acquire_session_file_lock(upload_dir(root).join(".sessions.lock")).await?;
    Ok(S3UploadSessionLock {
        _process_guard: process_guard,
        _file_lock: file_lock,
    })
}

/// Acquires the cross-process advisory lock for one session's part files.
///
/// The caller must acquire this while holding [`lock_upload_sessions`] and
/// after its process-local [`acquire_session_part_lock`] guard. That ordering
/// is also used by expiry, completion, and abort, preventing cross-replica
/// part writes from racing reads or deletion on a shared upload filesystem.
///
/// # Errors
///
/// Returns [`S3SessionError::NotFound`] if the session directory disappeared,
/// or an I/O/blocking-task error if the advisory lock cannot be acquired.
pub async fn lock_session_parts(
    root: &Path,
    upload_id: &str,
) -> Result<S3SessionPartFileLock, S3SessionError> {
    validate_upload_id(upload_id)?;
    let dir = session_dir(root, upload_id)?;
    if !dir.is_dir() {
        return Err(S3SessionError::NotFound);
    }
    let file_lock = acquire_existing_session_file_lock(dir.join(".parts.lock")).await?;
    Ok(S3SessionPartFileLock {
        _file_lock: file_lock,
    })
}

async fn acquire_session_file_lock(path: PathBuf) -> Result<S3FileLock, S3SessionError> {
    spawn_blocking(move || {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.lock()?;
        Ok(S3FileLock { file })
    })
    .await
    .map_err(S3SessionError::BlockingTask)?
}

async fn acquire_existing_session_file_lock(path: PathBuf) -> Result<S3FileLock, S3SessionError> {
    spawn_blocking(move || {
        let file = match std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(S3SessionError::NotFound);
            }
            Err(error) => return Err(S3SessionError::Io(error)),
        };
        file.lock()?;
        Ok(S3FileLock { file })
    })
    .await
    .map_err(S3SessionError::BlockingTask)?
}

// ── Session lifecycle ────────────────────────────────────────────────────────

/// Creates a new multipart upload session and returns its upload id.
///
/// Expired sessions are swept first; the max-active cap and the aggregate
/// byte quota are enforced against the remaining sessions.
///
/// # Errors
///
/// Returns [`S3SessionError::TooManySessions`] when the active-session cap is
/// reached, [`S3SessionError::AggregateQuotaExceeded`] when the aggregate byte
/// quota across active sessions is already exhausted, or
/// [`S3SessionError::Io`]/[`S3SessionError::Json`] on persistence failure.
#[allow(clippy::too_many_arguments)]
pub async fn create_session(
    root: &Path,
    bucket: &str,
    key: &str,
    scope_namespace: &str,
    ttl_seconds: NonZeroU64,
    max_active_sessions: NonZeroUsize,
    total_max_bytes: NonZeroU64,
    user_metadata: Vec<(String, String)>,
) -> Result<String, S3SessionError> {
    let _lock = lock_upload_sessions(root).await?;
    let now_unix_seconds = unix_now_seconds_checked()?;
    sweep_expired_sessions_locked(root, ttl_seconds, now_unix_seconds).await?;
    let active_sessions = count_active_sessions_locked(root, ttl_seconds, now_unix_seconds).await?;
    if active_sessions >= max_active_sessions.get() {
        return Err(S3SessionError::TooManySessions);
    }
    let total_bytes = total_active_bytes_locked(root, ttl_seconds, now_unix_seconds).await?;
    if total_bytes >= total_max_bytes.get() {
        return Err(S3SessionError::AggregateQuotaExceeded);
    }
    let upload_id = new_upload_id();
    let dir = session_dir(root, &upload_id)?;
    fs::create_dir_all(&dir).await?;
    let session = MultipartUploadSession {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        scope_namespace: scope_namespace.to_owned(),
        upload_id: upload_id.clone(),
        parts: BTreeMap::new(),
        user_metadata,
        created_at_unix_seconds: now_unix_seconds,
        last_touched_unix_seconds: now_unix_seconds,
    };
    persist_session(root, &upload_id, &session).await?;
    Ok(upload_id)
}

/// Reads a session, treating missing or expired sessions as [`NotFound`](S3SessionError::NotFound).
///
/// An expired session is removed as a side effect.
///
/// # Errors
///
/// Returns [`S3SessionError::NotFound`] when the session does not exist or has
/// expired, [`S3SessionError::InvalidUploadId`] for a malformed id, and
/// [`S3SessionError::Io`]/[`S3SessionError::Json`] on read failure.
pub async fn read_session(
    root: &Path,
    upload_id: &str,
    ttl_seconds: NonZeroU64,
) -> Result<MultipartUploadSession, S3SessionError> {
    validate_upload_id(upload_id)?;
    let now_unix_seconds = unix_now_seconds_checked()?;
    load_session_at(
        &session_dir(root, upload_id)?,
        ttl_seconds,
        now_unix_seconds,
    )
    .await
}

/// Records a stored part's size in the session metadata.
///
/// The part file itself is written by the caller (which streams the request
/// body to [`part_file_path`]); this persists the size under the session lock
/// and enforces the per-session and aggregate byte quotas plus the global
/// active-part-file cap.
///
/// # Errors
///
/// Returns [`S3SessionError::NotFound`] when the session is missing or expired,
/// [`S3SessionError::InvalidPartNumber`] for an out-of-range part number,
/// [`S3SessionError::SessionQuotaExceeded`]/[`S3SessionError::AggregateQuotaExceeded`]
/// when the byte quotas would be exceeded,
/// [`S3SessionError::TooManyPartFiles`] when the global active-part-file cap
/// would be exceeded, and [`S3SessionError::Io`]/[`S3SessionError::Json`] on
/// persistence failure.
#[allow(clippy::too_many_arguments)]
pub async fn store_part(
    root: &Path,
    upload_id: &str,
    part_number: u32,
    size_bytes: u64,
    ttl_seconds: NonZeroU64,
    session_max_bytes: NonZeroU64,
    total_max_bytes: NonZeroU64,
    max_active_part_files: NonZeroUsize,
) -> Result<(), S3SessionError> {
    validate_upload_id(upload_id)?;
    validate_part_number(part_number)?;
    let _lock = lock_upload_sessions(root).await?;
    store_part_locked(
        root,
        upload_id,
        part_number,
        size_bytes,
        ttl_seconds,
        session_max_bytes,
        total_max_bytes,
        max_active_part_files,
    )
    .await
}

/// The lock-free counterpart of [`store_part`]: the caller must hold the
/// session lock ([`lock_upload_sessions`]) for the duration of the mutation so
/// a concurrent sweep cannot remove the session directory mid-write.
///
/// # Errors
///
/// See [`store_part`].
#[allow(clippy::too_many_arguments)]
pub async fn store_part_locked(
    root: &Path,
    upload_id: &str,
    part_number: u32,
    size_bytes: u64,
    ttl_seconds: NonZeroU64,
    session_max_bytes: NonZeroU64,
    total_max_bytes: NonZeroU64,
    max_active_part_files: NonZeroUsize,
) -> Result<(), S3SessionError> {
    validate_upload_id(upload_id)?;
    validate_part_number(part_number)?;
    let now_unix_seconds = unix_now_seconds_checked()?;
    let mut session = load_session_at(
        &session_dir(root, upload_id)?,
        ttl_seconds,
        now_unix_seconds,
    )
    .await?;

    let (total_active_bytes, total_active_part_files) =
        total_active_usage_locked(root, ttl_seconds, now_unix_seconds).await?;
    enforce_part_quotas(
        &session,
        part_number,
        size_bytes,
        session_max_bytes,
        total_max_bytes,
        max_active_part_files,
        total_active_bytes,
        total_active_part_files,
    )?;

    session.parts.insert(
        part_number,
        MultipartPart {
            size_bytes,
            file_name: format!("part-{part_number}"),
        },
    );
    session.last_touched_unix_seconds = now_unix_seconds;
    persist_session(root, upload_id, &session).await
}

/// Validates a part against the per-session and aggregate byte quotas and the
/// global active-part-file cap WITHOUT persisting anything.
///
/// The caller must hold the session lock ([`lock_upload_sessions`]). Used by
/// the server's `UploadPart` handler to reject an over-quota or over-cap part
/// BEFORE its file is written (no write-then-delete); [`store_part_locked`]
/// re-checks the same quotas against the actually-streamed size and then
/// persists.
///
/// # Errors
///
/// Returns [`S3SessionError::NotFound`] when the session is missing or expired,
/// [`S3SessionError::InvalidPartNumber`] for an out-of-range part number,
/// [`S3SessionError::SessionQuotaExceeded`]/[`S3SessionError::AggregateQuotaExceeded`]
/// when the byte quotas would be exceeded,
/// [`S3SessionError::TooManyPartFiles`] when the global active-part-file cap
/// would be exceeded, and [`S3SessionError::Io`]/[`S3SessionError::Json`] on
/// read failure.
#[allow(clippy::too_many_arguments)]
pub async fn validate_part_quota_locked(
    root: &Path,
    upload_id: &str,
    part_number: u32,
    size_bytes: u64,
    ttl_seconds: NonZeroU64,
    session_max_bytes: NonZeroU64,
    total_max_bytes: NonZeroU64,
    max_active_part_files: NonZeroUsize,
) -> Result<(), S3SessionError> {
    validate_upload_id(upload_id)?;
    validate_part_number(part_number)?;
    let now_unix_seconds = unix_now_seconds_checked()?;
    let session = load_session_at(
        &session_dir(root, upload_id)?,
        ttl_seconds,
        now_unix_seconds,
    )
    .await?;
    let (total_active_bytes, total_active_part_files) =
        total_active_usage_locked(root, ttl_seconds, now_unix_seconds).await?;
    enforce_part_quotas(
        &session,
        part_number,
        size_bytes,
        session_max_bytes,
        total_max_bytes,
        max_active_part_files,
        total_active_bytes,
        total_active_part_files,
    )
}

/// The shared per-session + aggregate byte-quota and global part-file-count
/// enforcement for a stored (or about-to-be-stored) part.
///
/// An overwrite of an existing part number replaces the previous size (so only
/// the delta counts against the byte quotas) and does not materialize a NEW
/// part file (so only a new part number counts against the global
/// active-part-file cap). Both counts are computed from the session metadata
/// under the same lock section as the byte quotas, so the cap is enforced
/// atomically with the quota accounting and an over-cap part never reaches the
/// disk; deleting a session (abort/sweep/complete) removes its part files from
/// the count as a side effect of the next scan.
#[allow(clippy::too_many_arguments)]
fn enforce_part_quotas(
    session: &MultipartUploadSession,
    part_number: u32,
    size_bytes: u64,
    session_max_bytes: NonZeroU64,
    total_max_bytes: NonZeroU64,
    max_active_part_files: NonZeroUsize,
    total_active_bytes: u64,
    total_active_part_files: u64,
) -> Result<(), S3SessionError> {
    let previous_size = session
        .parts
        .get(&part_number)
        .map_or(0_u64, |part| part.size_bytes);
    let session_total = session
        .parts
        .values()
        .fold(0_u64, |total, part| total.saturating_add(part.size_bytes));
    let delta = size_bytes.saturating_sub(previous_size);
    let new_session_total = session_total.saturating_add(delta);
    if new_session_total > session_max_bytes.get() {
        return Err(S3SessionError::SessionQuotaExceeded);
    }
    if total_active_bytes.saturating_add(delta) > total_max_bytes.get() {
        return Err(S3SessionError::AggregateQuotaExceeded);
    }
    let new_part_file = if session.parts.contains_key(&part_number) {
        0_u64
    } else {
        1_u64
    };
    let cap = u64::try_from(max_active_part_files.get()).unwrap_or(u64::MAX);
    if total_active_part_files.saturating_add(new_part_file) > cap {
        return Err(S3SessionError::TooManyPartFiles);
    }
    Ok(())
}

/// Deletes a session and all of its part files.
///
/// Missing sessions are a no-op.
///
/// # Errors
///
/// Returns [`S3SessionError::InvalidUploadId`] for a malformed id and
/// [`S3SessionError::Io`] on deletion failure.
pub async fn delete_session(root: &Path, upload_id: &str) -> Result<(), S3SessionError> {
    validate_upload_id(upload_id)?;
    let _lock = lock_upload_sessions(root).await?;
    delete_session_locked(root, upload_id).await
}

/// The lock-free counterpart of [`delete_session`]: the caller must hold the
/// session lock ([`lock_upload_sessions`]) for the duration of the mutation.
///
/// # Errors
///
/// See [`delete_session`].
pub async fn delete_session_locked(root: &Path, upload_id: &str) -> Result<(), S3SessionError> {
    validate_upload_id(upload_id)?;
    delete_session_dir(&session_dir(root, upload_id)?).await
}

/// Removes every expired (or unreadable) session directory, returning the
/// number of sessions removed.
///
/// # Errors
///
/// Returns [`S3SessionError::Io`] when the upload directory cannot be read.
pub async fn sweep_expired_sessions(
    root: &Path,
    ttl_seconds: NonZeroU64,
) -> Result<usize, S3SessionError> {
    let _lock = lock_upload_sessions(root).await?;
    let now_unix_seconds = unix_now_seconds_checked()?;
    sweep_expired_sessions_locked(root, ttl_seconds, now_unix_seconds).await
}

/// Counts the currently active (unexpired) sessions.
///
/// # Errors
///
/// Returns [`S3SessionError::Io`] when the upload directory cannot be read.
pub async fn count_active_sessions(
    root: &Path,
    ttl_seconds: NonZeroU64,
) -> Result<usize, S3SessionError> {
    let _lock = lock_upload_sessions(root).await?;
    let now_unix_seconds = unix_now_seconds_checked()?;
    count_active_sessions_locked(root, ttl_seconds, now_unix_seconds).await
}

/// Returns whether a session has expired against the TTL.
///
/// Expiry is anchored to **session creation** (matching S3: the multipart
/// upload lifecycle runs from initiation; `UploadPart` does not extend it), so
/// keep-alive parts cannot keep a session alive indefinitely.
#[must_use]
pub const fn is_expired(
    session: &MultipartUploadSession,
    ttl_seconds: NonZeroU64,
    now_unix_seconds: u64,
) -> bool {
    session
        .created_at_unix_seconds
        .saturating_add(ttl_seconds.get())
        <= now_unix_seconds
}

// ── Internal helpers ─────────────────────────────────────────────────────────

fn unix_now_seconds_checked() -> Result<u64, S3SessionError> {
    shardline_server_core::unix_now_seconds_checked().map_err(|_error| S3SessionError::Overflow)
}

async fn load_session_at(
    dir: &Path,
    ttl_seconds: NonZeroU64,
    now_unix_seconds: u64,
) -> Result<MultipartUploadSession, S3SessionError> {
    let bytes = match fs::read(dir.join("session.json")).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(S3SessionError::NotFound);
        }
        Err(error) => return Err(S3SessionError::Io(error)),
    };
    let session: MultipartUploadSession = serde_json::from_slice(&bytes)?;
    if is_expired(&session, ttl_seconds, now_unix_seconds) {
        return Err(S3SessionError::NotFound);
    }
    Ok(session)
}

async fn persist_session(
    root: &Path,
    upload_id: &str,
    session: &MultipartUploadSession,
) -> Result<(), S3SessionError> {
    let path = session_metadata_path(root, upload_id)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let bytes = serde_json::to_vec(session)?;
    write_file_atomically(&path, &bytes).await
}

/// Writes bytes via a temporary file + rename so a crash never leaves a torn
/// `session.json`.
async fn write_file_atomically(path: &Path, bytes: &[u8]) -> Result<(), S3SessionError> {
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, bytes).await?;
    match fs::rename(&temporary, path).await {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ignored = fs::remove_file(&temporary).await;
            Err(S3SessionError::Io(error))
        }
    }
}

async fn delete_session_dir(path: &Path) -> Result<(), S3SessionError> {
    match fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(S3SessionError::Io(error)),
    }
}

async fn sweep_expired_sessions_locked(
    root: &Path,
    ttl_seconds: NonZeroU64,
    now_unix_seconds: u64,
) -> Result<usize, S3SessionError> {
    let dir = upload_dir(root);
    let mut entries = match fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(S3SessionError::Io(error)),
    };
    let mut removed = 0_usize;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if validate_upload_id(file_name).is_err() {
            continue;
        }
        let expired = match load_session_at(&path, ttl_seconds, now_unix_seconds).await {
            Ok(session) => is_expired(&session, ttl_seconds, now_unix_seconds),
            Err(S3SessionError::NotFound) => true,
            Err(_error) => continue,
        };
        if expired {
            // Serialize the delete against an in-flight part write for this
            // session: an UploadPart holds this lock across its body stream
            // (F-10), so we cannot remove the directory mid-write.
            let part_lock = acquire_session_part_lock(file_name);
            let _part_guard = part_lock.lock().await;
            let _part_file_guard = lock_session_parts(root, file_name).await?;
            if delete_session_dir(&path).await.is_ok() {
                removed = removed.saturating_add(1);
            }
        }
    }
    Ok(removed)
}

async fn count_active_sessions_locked(
    root: &Path,
    ttl_seconds: NonZeroU64,
    now_unix_seconds: u64,
) -> Result<usize, S3SessionError> {
    let dir = upload_dir(root);
    let mut entries = match fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(S3SessionError::Io(error)),
    };
    let mut active = 0_usize;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if validate_upload_id(file_name).is_err() {
            continue;
        }
        if load_session_at(&path, ttl_seconds, now_unix_seconds)
            .await
            .is_ok()
        {
            active = active.saturating_add(1);
        }
    }
    Ok(active)
}

/// Sums the stored part bytes across all active sessions (caller must hold
/// the session lock). Used to enforce the aggregate byte quota at session
/// creation; part writes use [`total_active_usage_locked`].
async fn total_active_bytes_locked(
    root: &Path,
    ttl_seconds: NonZeroU64,
    now_unix_seconds: u64,
) -> Result<u64, S3SessionError> {
    Ok(
        total_active_usage_locked(root, ttl_seconds, now_unix_seconds)
            .await?
            .0,
    )
}

/// Computes the aggregate stored-part bytes AND the total part-file count
/// across all active sessions (caller must hold the session lock). Used to
/// enforce the aggregate byte quota and the global active-part-file cap in a
/// single scan, so both limits are checked atomically under the same lock
/// section with no counter drift. Each active session contributes
/// `parts.len()` part files — one `part-{n}` file per stored part number —
/// so deleting a session (abort/sweep/complete) releases its slots as a side
/// effect of the next scan.
async fn total_active_usage_locked(
    root: &Path,
    ttl_seconds: NonZeroU64,
    now_unix_seconds: u64,
) -> Result<(u64, u64), S3SessionError> {
    let dir = upload_dir(root);
    let mut entries = match fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(error) => return Err(S3SessionError::Io(error)),
    };
    let mut total_bytes = 0_u64;
    let mut total_part_files = 0_u64;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if validate_upload_id(file_name).is_err() {
            continue;
        }
        if let Ok(session) = load_session_at(&path, ttl_seconds, now_unix_seconds).await {
            let session_total = session
                .parts
                .values()
                .fold(0_u64, |sum, part| sum.saturating_add(part.size_bytes));
            total_bytes = total_bytes.saturating_add(session_total);
            total_part_files = total_part_files
                .saturating_add(u64::try_from(session.parts.len()).unwrap_or(u64::MAX));
        }
    }
    Ok((total_bytes, total_part_files))
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::arithmetic_side_effects,
        clippy::option_if_let_else,
        clippy::unreachable,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use
    )]

    use std::num::{NonZeroU64, NonZeroUsize};

    use super::*;

    fn ttl(seconds: u64) -> NonZeroU64 {
        NonZeroU64::new(seconds).unwrap()
    }

    fn cap(count: usize) -> NonZeroUsize {
        NonZeroUsize::new(count).unwrap()
    }

    fn quota(bytes: u64) -> NonZeroU64 {
        NonZeroU64::new(bytes).unwrap()
    }

    async fn make_root() -> tempfile::TempDir {
        tempfile::TempDir::new().unwrap()
    }

    fn session_at(root: &Path, upload_id: &str) -> MultipartUploadSession {
        // Reads the session json directly for assertions.
        let bytes = std::fs::read(session_metadata_path(root, upload_id).unwrap()).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_part_file_lock_excludes_independent_openers() {
        let root = make_root().await;
        let upload_id = create_session(
            root.path(),
            "acme.models",
            "large.bin",
            "global",
            ttl(3600),
            cap(16),
            quota(1 << 40),
            Vec::new(),
        )
        .await
        .unwrap();
        let first = lock_session_parts(root.path(), &upload_id).await.unwrap();

        let (acquired_tx, mut acquired_rx) = tokio::sync::oneshot::channel();
        let waiter_root = root.path().to_path_buf();
        let waiter_id = upload_id.clone();
        let waiter = tokio::spawn(async move {
            let _second = lock_session_parts(&waiter_root, &waiter_id).await.unwrap();
            let _ignored = acquired_tx.send(());
        });

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut acquired_rx)
                .await
                .is_err()
        );
        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(2), &mut acquired_rx)
            .await
            .unwrap()
            .unwrap();
        waiter.await.unwrap();
    }

    #[test]
    fn new_upload_id_is_safe_path_component() {
        let id = new_upload_id();
        assert_eq!(id.len(), 32);
        assert!(validate_upload_id(&id).is_ok());
        assert!(validate_upload_id(&id).is_ok());
    }

    #[test]
    fn validate_upload_id_rejects_unsafe_values() {
        assert!(validate_upload_id("").is_err());
        assert!(validate_upload_id("has/slash").is_err());
        assert!(validate_upload_id("has space").is_err());
        assert!(validate_upload_id("..").is_err());
        assert!(validate_upload_id(&"a".repeat(65)).is_err());
        // Uppercase hex is a safe path component and is accepted.
        assert!(validate_upload_id("0aBc").is_ok());
    }

    #[test]
    fn validate_part_number_enforces_protocol_bounds() {
        assert!(validate_part_number(1).is_ok());
        assert!(validate_part_number(10_000).is_ok());
        assert!(validate_part_number(0).is_err());
        assert!(validate_part_number(10_001).is_err());
    }

    #[tokio::test]
    async fn create_store_read_part_sizes_roundtrip() {
        let root = make_root().await;
        let upload_id = create_session(
            root.path(),
            "acme.models",
            "data/model.pt",
            "global",
            ttl(3600),
            cap(16),
            quota(1 << 40),
            Vec::new(),
        )
        .await
        .unwrap();
        assert_eq!(upload_id.len(), 32);

        for (part, size) in [(1_u32, 64_u64), (2, 512), (3, 0)] {
            tokio::fs::write(
                part_file_path(root.path(), &upload_id, part).unwrap(),
                vec![0_u8; size as usize],
            )
            .await
            .unwrap();
            store_part(
                root.path(),
                &upload_id,
                part,
                size,
                ttl(3600),
                quota(1 << 40),
                quota(1 << 40),
                cap(200_000),
            )
            .await
            .unwrap();
        }

        let session = read_session(root.path(), &upload_id, ttl(3600))
            .await
            .unwrap();
        assert_eq!(session.bucket, "acme.models");
        assert_eq!(session.key, "data/model.pt");
        assert_eq!(session.scope_namespace, "global");
        assert_eq!(session.parts.len(), 3);
        assert_eq!(session.parts[&1].size_bytes, 64);
        assert_eq!(session.parts[&2].size_bytes, 512);
        assert_eq!(session.parts[&3].size_bytes, 0);
        assert_eq!(session.parts[&1].file_name, "part-1");
    }

    #[tokio::test]
    async fn store_part_overwrites_existing_part_size() {
        let root = make_root().await;
        let upload_id = create_session(
            root.path(),
            "acme.models",
            "k",
            "global",
            ttl(3600),
            cap(16),
            quota(1 << 40),
            Vec::new(),
        )
        .await
        .unwrap();
        store_part(
            root.path(),
            &upload_id,
            1,
            100,
            ttl(3600),
            quota(1 << 40),
            quota(1 << 40),
            cap(200_000),
        )
        .await
        .unwrap();
        store_part(
            root.path(),
            &upload_id,
            1,
            200,
            ttl(3600),
            quota(1 << 40),
            quota(1 << 40),
            cap(200_000),
        )
        .await
        .unwrap();
        let session = read_session(root.path(), &upload_id, ttl(3600))
            .await
            .unwrap();
        assert_eq!(session.parts.len(), 1);
        assert_eq!(session.parts[&1].size_bytes, 200);
    }

    #[tokio::test]
    async fn read_session_missing_and_invalid_id() {
        let root = make_root().await;
        assert!(matches!(
            read_session(root.path(), "00000000000000000000000000000000", ttl(3600)).await,
            Err(S3SessionError::NotFound)
        ));
        assert!(matches!(
            read_session(root.path(), "not a valid id!", ttl(3600)).await,
            Err(S3SessionError::InvalidUploadId)
        ));
    }

    #[tokio::test]
    async fn delete_session_removes_parts_and_is_idempotent() {
        let root = make_root().await;
        let upload_id = create_session(
            root.path(),
            "acme.models",
            "k",
            "global",
            ttl(3600),
            cap(16),
            quota(1 << 40),
            Vec::new(),
        )
        .await
        .unwrap();
        store_part(
            root.path(),
            &upload_id,
            1,
            10,
            ttl(3600),
            quota(1 << 40),
            quota(1 << 40),
            cap(200_000),
        )
        .await
        .unwrap();

        let dir = session_dir(root.path(), &upload_id).unwrap();
        assert!(dir.exists());
        delete_session(root.path(), &upload_id).await.unwrap();
        assert!(!dir.exists());
        // Idempotent.
        delete_session(root.path(), &upload_id).await.unwrap();
        assert!(matches!(
            read_session(root.path(), &upload_id, ttl(3600)).await,
            Err(S3SessionError::NotFound)
        ));
    }

    #[tokio::test]
    async fn expired_session_is_not_found_and_swept() {
        let root = make_root().await;
        let upload_id = create_session(
            root.path(),
            "acme.models",
            "k",
            "global",
            ttl(3600),
            cap(16),
            quota(1 << 40),
            Vec::new(),
        )
        .await
        .unwrap();
        // Rewrite the session with a last_touched in the past.
        let mut session = session_at(root.path(), &upload_id);
        session.created_at_unix_seconds = 1;
        persist_session(root.path(), &upload_id, &session)
            .await
            .unwrap();

        assert!(matches!(
            read_session(root.path(), &upload_id, ttl(3600)).await,
            Err(S3SessionError::NotFound)
        ));

        // Sweep removes it.
        let removed = sweep_expired_sessions(root.path(), ttl(3600))
            .await
            .unwrap();
        assert_eq!(removed, 1);
        assert!(!session_dir(root.path(), &upload_id).unwrap().exists());
    }

    #[tokio::test]
    async fn active_sessions_respect_max_cap() {
        let root = make_root().await;
        // Cap of 2: the first two creates succeed, the third is rejected.
        let first = create_session(
            root.path(),
            "a",
            "1",
            "g",
            ttl(3600),
            cap(2),
            quota(1 << 40),
            Vec::new(),
        )
        .await
        .unwrap();
        let second = create_session(
            root.path(),
            "a",
            "2",
            "g",
            ttl(3600),
            cap(2),
            quota(1 << 40),
            Vec::new(),
        )
        .await
        .unwrap();
        let third = create_session(
            root.path(),
            "a",
            "3",
            "g",
            ttl(3600),
            cap(2),
            quota(1 << 40),
            Vec::new(),
        )
        .await;
        assert!(matches!(third, Err(S3SessionError::TooManySessions)));

        // Deleting one frees the slot.
        delete_session(root.path(), &first).await.unwrap();
        let third = create_session(
            root.path(),
            "a",
            "3",
            "g",
            ttl(3600),
            cap(2),
            quota(1 << 40),
            Vec::new(),
        )
        .await;
        assert!(third.is_ok());
        let _ = second;
    }

    #[tokio::test]
    async fn count_active_sessions_ignores_expired() {
        let root = make_root().await;
        let live = create_session(
            root.path(),
            "a",
            "1",
            "g",
            ttl(3600),
            cap(16),
            quota(1 << 40),
            Vec::new(),
        )
        .await
        .unwrap();
        let stale = create_session(
            root.path(),
            "a",
            "2",
            "g",
            ttl(3600),
            cap(16),
            quota(1 << 40),
            Vec::new(),
        )
        .await
        .unwrap();
        let mut session = session_at(root.path(), &stale);
        session.created_at_unix_seconds = 1;
        persist_session(root.path(), &stale, &session)
            .await
            .unwrap();

        assert_eq!(
            count_active_sessions(root.path(), ttl(3600)).await.unwrap(),
            1
        );
        let _ = live;
    }

    #[tokio::test]
    async fn part_file_path_is_anchored_under_session_dir() {
        let root = make_root().await;
        let path = part_file_path(root.path(), "abc123", 7).unwrap();
        assert_eq!(path, upload_dir(root.path()).join("abc123").join("part-7"));
        assert!(part_file_path(root.path(), "bad/id", 1).is_err());
        assert!(part_file_path(root.path(), "abc123", 0).is_err());
        assert!(part_file_path(root.path(), "abc123", 10_001).is_err());
    }

    #[tokio::test]
    async fn store_part_enforces_session_byte_quota() {
        let root = make_root().await;
        let upload_id = create_session(
            root.path(),
            "acme.models",
            "k",
            "global",
            ttl(3600),
            cap(16),
            quota(1 << 40),
            Vec::new(),
        )
        .await
        .unwrap();
        store_part(
            root.path(),
            &upload_id,
            1,
            100,
            ttl(3600),
            quota(150),
            quota(1 << 40),
            cap(200_000),
        )
        .await
        .unwrap();
        // A second part pushes the session over its 150-byte quota.
        let result = store_part(
            root.path(),
            &upload_id,
            2,
            100,
            ttl(3600),
            quota(150),
            quota(1 << 40),
            cap(200_000),
        )
        .await;
        assert!(matches!(result, Err(S3SessionError::SessionQuotaExceeded)));
        // Replacing a part (same total) stays within the quota.
        store_part(
            root.path(),
            &upload_id,
            1,
            50,
            ttl(3600),
            quota(150),
            quota(1 << 40),
            cap(200_000),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn store_part_enforces_aggregate_byte_quota() {
        let root = make_root().await;
        let upload_id = create_session(
            root.path(),
            "acme.models",
            "k",
            "global",
            ttl(3600),
            cap(16),
            quota(200),
            Vec::new(),
        )
        .await
        .unwrap();
        store_part(
            root.path(),
            &upload_id,
            1,
            100,
            ttl(3600),
            quota(1 << 40),
            quota(200),
            cap(200_000),
        )
        .await
        .unwrap();
        // The aggregate (100) + 150 exceeds the 200-byte aggregate quota.
        let result = store_part(
            root.path(),
            &upload_id,
            2,
            150,
            ttl(3600),
            quota(1 << 40),
            quota(200),
            cap(200_000),
        )
        .await;
        assert!(matches!(
            result,
            Err(S3SessionError::AggregateQuotaExceeded)
        ));
    }

    #[tokio::test]
    async fn create_session_rejects_exhausted_aggregate_quota() {
        let root = make_root().await;
        let first = create_session(
            root.path(),
            "a",
            "1",
            "g",
            ttl(3600),
            cap(16),
            quota(200),
            Vec::new(),
        )
        .await
        .unwrap();
        store_part(
            root.path(),
            &first,
            1,
            200,
            ttl(3600),
            quota(1 << 40),
            quota(200),
            cap(200_000),
        )
        .await
        .unwrap();
        // The aggregate is exhausted: a new session is rejected.
        let second = create_session(
            root.path(),
            "a",
            "2",
            "g",
            ttl(3600),
            cap(16),
            quota(200),
            Vec::new(),
        )
        .await;
        assert!(matches!(
            second,
            Err(S3SessionError::AggregateQuotaExceeded)
        ));
    }

    #[tokio::test]
    async fn expiry_is_anchored_to_creation_not_keep_alive() {
        let root = make_root().await;
        let upload_id = create_session(
            root.path(),
            "acme.models",
            "k",
            "global",
            ttl(3600),
            cap(16),
            quota(1 << 40),
            Vec::new(),
        )
        .await
        .unwrap();
        // A keep-alive part refreshes last_touched but NOT the creation-based
        // expiry anchor; backdating created_at makes the session expire even
        // though last_touched is fresh.
        store_part(
            root.path(),
            &upload_id,
            1,
            10,
            ttl(3600),
            quota(1 << 40),
            quota(1 << 40),
            cap(200_000),
        )
        .await
        .unwrap();
        let mut session = session_at(root.path(), &upload_id);
        session.created_at_unix_seconds = 1;
        persist_session(root.path(), &upload_id, &session)
            .await
            .unwrap();
        assert!(matches!(
            read_session(root.path(), &upload_id, ttl(3600)).await,
            Err(S3SessionError::NotFound)
        ));
    }

    #[tokio::test]
    async fn validate_part_quota_locked_rejects_without_persisting() {
        // F-19: the pre-write quota validator rejects an over-quota part
        // WITHOUT touching the session (the server uses it before streaming a
        // part body, so no write-then-delete), and accepts an in-quota one.
        let root = make_root().await;
        let upload_id = create_session(
            root.path(),
            "acme.models",
            "k",
            "global",
            ttl(3600),
            cap(16),
            quota(1 << 40),
            Vec::new(),
        )
        .await
        .unwrap();
        store_part(
            root.path(),
            &upload_id,
            1,
            100,
            ttl(3600),
            quota(150),
            quota(1 << 40),
            cap(200_000),
        )
        .await
        .unwrap();

        // A part that would push the session over its 150-byte quota is
        // rejected; the session metadata is left untouched.
        let rejected = validate_part_quota_locked(
            root.path(),
            &upload_id,
            2,
            60,
            ttl(3600),
            quota(150),
            quota(1 << 40),
            cap(200_000),
        )
        .await;
        assert!(matches!(
            rejected,
            Err(S3SessionError::SessionQuotaExceeded)
        ));
        let session = read_session(root.path(), &upload_id, ttl(3600))
            .await
            .unwrap();
        assert_eq!(session.parts.len(), 1, "no part was persisted");

        // An in-quota part passes validation.
        validate_part_quota_locked(
            root.path(),
            &upload_id,
            2,
            40,
            ttl(3600),
            quota(150),
            quota(1 << 40),
            cap(200_000),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn validate_part_quota_locked_enforces_aggregate_quota() {
        // F-19: the aggregate quota is checked against the projected size.
        let root = make_root().await;
        let upload_id = create_session(
            root.path(),
            "acme.models",
            "k",
            "global",
            ttl(3600),
            cap(16),
            quota(200),
            Vec::new(),
        )
        .await
        .unwrap();
        store_part(
            root.path(),
            &upload_id,
            1,
            100,
            ttl(3600),
            quota(1 << 40),
            quota(200),
            cap(200_000),
        )
        .await
        .unwrap();

        let rejected = validate_part_quota_locked(
            root.path(),
            &upload_id,
            2,
            150,
            ttl(3600),
            quota(1 << 40),
            quota(200),
            cap(200_000),
        )
        .await;
        assert!(matches!(
            rejected,
            Err(S3SessionError::AggregateQuotaExceeded)
        ));
    }

    #[tokio::test]
    async fn part_file_cap_rejects_new_part_before_any_file_is_written() {
        // F-19: the global active-part-file cap is enforced (under the same
        // lock as the byte quotas) BEFORE a part file materializes; a NEW part
        // number consumes one slot while overwriting an existing number does
        // not, and a rejected part leaves no file on disk.
        let root = make_root().await;
        let upload_id = create_session(
            root.path(),
            "acme.models",
            "k",
            "global",
            ttl(3600),
            cap(16),
            quota(1 << 40),
            Vec::new(),
        )
        .await
        .unwrap();

        // A cap of 1: the first part file consumes the only slot.
        store_part(
            root.path(),
            &upload_id,
            1,
            10,
            ttl(3600),
            quota(1 << 40),
            quota(1 << 40),
            cap(1),
        )
        .await
        .unwrap();

        // The pre-write validator rejects a NEW part number at the cap.
        let rejected = validate_part_quota_locked(
            root.path(),
            &upload_id,
            2,
            10,
            ttl(3600),
            quota(1 << 40),
            quota(1 << 40),
            cap(1),
        )
        .await;
        assert!(matches!(rejected, Err(S3SessionError::TooManyPartFiles)));
        let session = read_session(root.path(), &upload_id, ttl(3600))
            .await
            .unwrap();
        assert_eq!(session.parts.len(), 1, "no part was persisted");

        // Overwriting the existing part number is allowed (no new file).
        store_part(
            root.path(),
            &upload_id,
            1,
            20,
            ttl(3600),
            quota(1 << 40),
            quota(1 << 40),
            cap(1),
        )
        .await
        .unwrap();

        // The post-write store path rejects the new part number too, so a
        // rejected part never leaves a file behind.
        let rejected = store_part(
            root.path(),
            &upload_id,
            2,
            10,
            ttl(3600),
            quota(1 << 40),
            quota(1 << 40),
            cap(1),
        )
        .await;
        assert!(matches!(rejected, Err(S3SessionError::TooManyPartFiles)));
        assert!(
            !part_file_path(root.path(), &upload_id, 2).unwrap().exists(),
            "an over-cap part must not materialize a file"
        );

        // Deleting the session frees the slot: a fresh session can store a
        // part again (the count is derived from live session metadata).
        delete_session(root.path(), &upload_id).await.unwrap();
        let fresh = create_session(
            root.path(),
            "acme.models",
            "k",
            "global",
            ttl(3600),
            cap(16),
            quota(1 << 40),
            Vec::new(),
        )
        .await
        .unwrap();
        store_part(
            root.path(),
            &fresh,
            1,
            10,
            ttl(3600),
            quota(1 << 40),
            quota(1 << 40),
            cap(1),
        )
        .await
        .unwrap();
    }
}
