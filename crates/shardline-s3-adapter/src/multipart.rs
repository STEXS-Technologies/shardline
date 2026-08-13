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
    collections::BTreeMap,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    sync::LazyLock,
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
    /// Unix seconds when the session was created.
    pub created_at_unix_seconds: u64,
    /// Unix seconds of the last part write (TTL is anchored to this).
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

// ── Session lifecycle ────────────────────────────────────────────────────────

/// Creates a new multipart upload session and returns its upload id.
///
/// Expired sessions are swept first; the max-active cap is enforced against
/// the remaining sessions.
///
/// # Errors
///
/// Returns [`S3SessionError::TooManySessions`] when the cap is reached, or
/// [`S3SessionError::Io`]/[`S3SessionError::Json`] on persistence failure.
pub async fn create_session(
    root: &Path,
    bucket: &str,
    key: &str,
    scope_namespace: &str,
    ttl_seconds: NonZeroU64,
    max_active_sessions: NonZeroUsize,
) -> Result<String, S3SessionError> {
    let _lock = lock_upload_sessions(root).await?;
    let now_unix_seconds = unix_now_seconds_checked()?;
    sweep_expired_sessions_locked(root, ttl_seconds, now_unix_seconds).await?;
    let active_sessions = count_active_sessions_locked(root, ttl_seconds, now_unix_seconds).await?;
    if active_sessions >= max_active_sessions.get() {
        return Err(S3SessionError::TooManySessions);
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
/// body to [`part_file_path`]); this persists the size and refreshes the
/// session TTL under the session lock.
///
/// # Errors
///
/// Returns [`S3SessionError::NotFound`] when the session is missing or expired,
/// [`S3SessionError::InvalidPartNumber`] for an out-of-range part number, and
/// [`S3SessionError::Io`]/[`S3SessionError::Json`] on persistence failure.
pub async fn store_part(
    root: &Path,
    upload_id: &str,
    part_number: u32,
    size_bytes: u64,
    ttl_seconds: NonZeroU64,
) -> Result<(), S3SessionError> {
    validate_upload_id(upload_id)?;
    validate_part_number(part_number)?;
    let _lock = lock_upload_sessions(root).await?;
    let now_unix_seconds = unix_now_seconds_checked()?;
    let mut session = load_session_at(
        &session_dir(root, upload_id)?,
        ttl_seconds,
        now_unix_seconds,
    )
    .await?;
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
#[must_use]
pub const fn is_expired(
    session: &MultipartUploadSession,
    ttl_seconds: NonZeroU64,
    now_unix_seconds: u64,
) -> bool {
    session
        .last_touched_unix_seconds
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
        if expired && delete_session_dir(&path).await.is_ok() {
            removed = removed.saturating_add(1);
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

    async fn make_root() -> tempfile::TempDir {
        tempfile::TempDir::new().unwrap()
    }

    fn session_at(root: &Path, upload_id: &str) -> MultipartUploadSession {
        // Reads the session json directly for assertions.
        let bytes = std::fs::read(session_metadata_path(root, upload_id).unwrap()).unwrap();
        serde_json::from_slice(&bytes).unwrap()
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
            store_part(root.path(), &upload_id, part, size, ttl(3600))
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
        )
        .await
        .unwrap();
        store_part(root.path(), &upload_id, 1, 100, ttl(3600))
            .await
            .unwrap();
        store_part(root.path(), &upload_id, 1, 200, ttl(3600))
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
        )
        .await
        .unwrap();
        store_part(root.path(), &upload_id, 1, 10, ttl(3600))
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
        )
        .await
        .unwrap();
        // Rewrite the session with a last_touched in the past.
        let mut session = session_at(root.path(), &upload_id);
        session.last_touched_unix_seconds = 1;
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
        let first = create_session(root.path(), "a", "1", "g", ttl(3600), cap(2))
            .await
            .unwrap();
        let second = create_session(root.path(), "a", "2", "g", ttl(3600), cap(2))
            .await
            .unwrap();
        let third = create_session(root.path(), "a", "3", "g", ttl(3600), cap(2)).await;
        assert!(matches!(third, Err(S3SessionError::TooManySessions)));

        // Deleting one frees the slot.
        delete_session(root.path(), &first).await.unwrap();
        let third = create_session(root.path(), "a", "3", "g", ttl(3600), cap(2)).await;
        assert!(third.is_ok());
        let _ = second;
    }

    #[tokio::test]
    async fn count_active_sessions_ignores_expired() {
        let root = make_root().await;
        let live = create_session(root.path(), "a", "1", "g", ttl(3600), cap(16))
            .await
            .unwrap();
        let stale = create_session(root.path(), "a", "2", "g", ttl(3600), cap(16))
            .await
            .unwrap();
        let mut session = session_at(root.path(), &stale);
        session.last_touched_unix_seconds = 1;
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
}
