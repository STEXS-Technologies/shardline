use std::{
    ffi::OsStr,
    io::Read,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
};

use blake3::Hasher as Blake3Hasher;
use getrandom::fill as getrandom_fill;
use sha2::{Digest, Sha256};
use shardline_protocol::RepositoryScope;
use shardline_storage::ObjectIntegrity;
use tokio::fs;
#[cfg(not(unix))]
use tokio::io::AsyncWriteExt;
use tokio::task::spawn_blocking;

use crate::OciAdapterError;
use crate::fs::{
    acquire_upload_session_file_lock, map_not_found, persist_upload_session,
    read_upload_file_async, unix_now_seconds_checked, upload_body_path, upload_dir,
    upload_file_exists_async, upload_file_len_async, upload_metadata_path,
    upload_session_lock_path, upload_tail_path, write_upload_metadata,
};
use crate::key::validate_repository;
use crate::protocol_support::{
    scope_namespace, validate_oci_repository_scope, validate_upload_session_id,
};
use crate::traits::OciBackend;
use crate::types::{OCI_UPLOAD_SESSION_LOCK, OciUploadSession, OciUploadSessionLock};

#[must_use]
pub fn new_upload_session_id() -> String {
    let mut bytes = [0_u8; 16];
    if getrandom_fill(&mut bytes).is_ok() {
        return hex::encode(bytes);
    }

    let fallback = format!(
        "{}:{}:{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("unnamed"),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_nanos())
    );
    let digest = Sha256::digest(fallback.as_bytes());
    let mut encoded = hex::encode(digest);
    encoded.truncate(32);
    encoded
}

/// # Errors
///
/// Returns an error when the file lock cannot be acquired.
pub async fn lock_upload_sessions(root: &Path) -> Result<OciUploadSessionLock, OciAdapterError> {
    let process_guard = OCI_UPLOAD_SESSION_LOCK.lock().await;
    let file_lock = acquire_upload_session_file_lock(upload_session_lock_path(root)).await?;
    Ok(OciUploadSessionLock {
        _process_guard: process_guard,
        _file_lock: file_lock,
    })
}

/// # Errors
///
/// Returns an error when the upload session cannot be created.
pub async fn create_upload_session<B: OciBackend>(
    root: &Path,
    backend: Option<&B>,
    repository: &str,
    repository_scope: Option<&RepositoryScope>,
    ttl_seconds: NonZeroU64,
    max_active_sessions: NonZeroUsize,
    use_s3_multipart: bool,
) -> Result<String, OciAdapterError> {
    let _lock = lock_upload_sessions(root).await?;
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    let now_unix_seconds = unix_now_seconds_checked()?;
    purge_expired_upload_sessions::<B>(root, backend, ttl_seconds, now_unix_seconds).await?;
    let active_sessions = count_active_upload_sessions(root, ttl_seconds).await?;
    if active_sessions >= max_active_sessions.get() {
        return Err(OciAdapterError::TooManyUploadSessions);
    }
    let session_id = new_upload_session_id();
    let upload_dir = upload_dir(root);
    fs::create_dir_all(&upload_dir).await?;
    let metadata = serde_json::to_vec(&OciUploadSession {
        repository: repository.to_owned(),
        scope_namespace: scope_namespace(repository_scope),
        created_at_unix_seconds: now_unix_seconds,
        last_touched_unix_seconds: now_unix_seconds,
        use_s3_multipart,
        s3_multipart: None,
    })?;
    if !use_s3_multipart {
        fs::write(upload_body_path(root, &session_id), []).await?;
    }
    if let Err(error) = write_upload_metadata(root, &session_id, metadata).await {
        delete_upload_session(root, &session_id).await?;
        return Err(error);
    }
    Ok(session_id)
}

/// # Errors
///
/// Returns an error when the upload session cannot be read.
pub async fn read_upload_session(
    root: &Path,
    session_id: &str,
    ttl_seconds: NonZeroU64,
) -> Result<OciUploadSession, OciAdapterError> {
    validate_upload_session_id(session_id)?;
    let metadata_path = upload_metadata_path(root, session_id);
    let bytes = read_upload_file_async(root, &metadata_path)
        .await
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                OciAdapterError::NotFound
            } else {
                OciAdapterError::Io(error)
            }
        })?;
    let session: OciUploadSession = serde_json::from_slice(&bytes)?;
    let now_unix_seconds = unix_now_seconds_checked()?;
    if upload_session_expired(&session, ttl_seconds, now_unix_seconds) {
        delete_upload_session(root, session_id).await?;
        return Err(OciAdapterError::NotFound);
    }
    let body_path = upload_body_path(root, session_id);
    let missing_local_body =
        !session.use_s3_multipart && upload_file_exists_async(root, &body_path).await.is_err();
    if missing_local_body {
        delete_upload_session(root, session_id).await?;
        return Err(OciAdapterError::NotFound);
    }
    Ok(session)
}

/// # Errors
///
/// Returns an error when bytes cannot be appended to the upload.
pub async fn append_upload_bytes(
    root: &Path,
    session_id: &str,
    bytes: &[u8],
) -> Result<u64, OciAdapterError> {
    validate_upload_session_id(session_id)?;
    let path = upload_body_path(root, session_id);
    append_upload_bytes_impl(root, path, bytes).await
}

#[cfg(unix)]
async fn append_upload_bytes_impl(
    root: &Path,
    path: PathBuf,
    bytes: &[u8],
) -> Result<u64, OciAdapterError> {
    let root = root.to_path_buf();
    let bytes = bytes.to_vec();
    spawn_blocking(move || crate::fs::append_file_anchored(&root, &path, &bytes))
        .await
        .map_err(OciAdapterError::BlockingTask)?
        .map_err(map_not_found)
}

#[cfg(not(unix))]
async fn append_upload_bytes_impl(
    root: &Path,
    path: PathBuf,
    bytes: &[u8],
) -> Result<u64, OciAdapterError> {
    let _ = root;
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .map_err(map_not_found)?;
    file.write_all(bytes).await?;
    let metadata = file.metadata().await?;
    Ok(metadata.len())
}

/// # Errors
///
/// Returns an error when the upload length cannot be determined.
pub async fn upload_length(root: &Path, session_id: &str) -> Result<u64, OciAdapterError> {
    validate_upload_session_id(session_id)?;
    let path = upload_body_path(root, session_id);
    upload_file_len_async(root, &path).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            OciAdapterError::NotFound
        } else {
            OciAdapterError::Io(error)
        }
    })
}

/// # Errors
///
/// Returns an error when the session ID is invalid or the body path cannot be resolved.
pub fn upload_body_path_for_session(
    root: &Path,
    session_id: &str,
) -> Result<PathBuf, OciAdapterError> {
    validate_upload_session_id(session_id)?;
    Ok(upload_body_path(root, session_id))
}

/// # Errors
///
/// Returns an error when the upload body cannot be read or hashed.
pub async fn upload_body_integrity(
    root: &Path,
    session_id: &str,
) -> Result<(String, ObjectIntegrity), OciAdapterError> {
    validate_upload_session_id(session_id)?;
    let path = upload_body_path(root, session_id);
    upload_body_integrity_impl(root, path).await
}

#[cfg(unix)]
async fn upload_body_integrity_impl(
    root: &Path,
    path: PathBuf,
) -> Result<(String, ObjectIntegrity), OciAdapterError> {
    let root = root.to_path_buf();
    spawn_blocking(move || {
        let mut file = crate::fs::open_anchored_file(&root, &path)?;
        let mut sha256 = Sha256::new();
        let mut blake3 = Blake3Hasher::new();
        let mut buffer = [0_u8; 256 * 1024];
        let mut total_length = 0_u64;
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            let slice = buffer.get(..read).ok_or(OciAdapterError::Overflow)?;
            sha256.update(slice);
            blake3.update(slice);
            total_length = total_length
                .checked_add(u64::try_from(read).map_err(|_error| OciAdapterError::Overflow)?)
                .ok_or(OciAdapterError::Overflow)?;
        }
        let sha256_hex = hex::encode(sha256.finalize());
        let blake3_hash =
            shardline_protocol::ShardlineHash::from_bytes(*blake3.finalize().as_bytes());
        Ok::<_, OciAdapterError>((sha256_hex, ObjectIntegrity::new(blake3_hash, total_length)))
    })
    .await
    .map_err(OciAdapterError::BlockingTask)?
}

#[cfg(not(unix))]
async fn upload_body_integrity_impl(
    root: &Path,
    path: PathBuf,
) -> Result<(String, ObjectIntegrity), OciAdapterError> {
    use std::fs::File;
    let _ = root;
    spawn_blocking(move || {
        let mut file = File::open(&path)?;
        let mut sha256 = Sha256::new();
        let mut blake3 = Blake3Hasher::new();
        let mut buffer = [0_u8; 256 * 1024];
        let mut total_length = 0_u64;
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            let slice = buffer.get(..read).ok_or(OciAdapterError::Overflow)?;
            sha256.update(slice);
            blake3.update(slice);
            total_length = total_length
                .checked_add(u64::try_from(read).map_err(|_error| OciAdapterError::Overflow)?)
                .ok_or(OciAdapterError::Overflow)?;
        }
        let sha256_hex = hex::encode(sha256.finalize());
        let blake3_hash =
            shardline_protocol::ShardlineHash::from_bytes(*blake3.finalize().as_bytes());
        Ok::<_, OciAdapterError>((sha256_hex, ObjectIntegrity::new(blake3_hash, total_length)))
    })
    .await
    .map_err(OciAdapterError::BlockingTask)?
}

/// # Errors
///
/// Returns an error when the upload session cannot be deleted.
/// Deletes the three session files (body, tail, metadata) under the upload root.
///
/// On Unix, each file is deleted using anchored (symlink-resistant) path resolution
/// via `delete_file_anchored` which uses `O_NOFOLLOW` directory traversal and
/// verifies the parent directory hasn't been replaced post-deletion.
/// On non-Unix, falls back to `tokio::fs::remove_file`.
pub async fn delete_upload_session(root: &Path, session_id: &str) -> Result<(), OciAdapterError> {
    validate_upload_session_id(session_id)?;
    let paths = [
        upload_body_path(root, session_id),
        upload_tail_path(root, session_id),
        upload_metadata_path(root, session_id),
    ];
    let mut first_error = None;
    for path in &paths {
        let result = delete_upload_file(root, path).await;
        match result {
            Ok(()) => {}
            Err(ref error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(OciAdapterError::Io(error));
                }
            }
        }
    }
    first_error.map_or_else(|| Ok(()), Err)
}

/// Deletes one file under the OCI upload root using anchored I/O on Unix.
#[cfg(unix)]
async fn delete_upload_file(root: &Path, path: &Path) -> std::io::Result<()> {
    let root = root.to_path_buf();
    let path = path.to_path_buf();
    spawn_blocking(move || crate::fs::delete_file_anchored(&root, &path))
        .await
        .map_err(std::io::Error::other)?
}

/// Non-Unix fallback for file deletion (no symlink protection).
#[cfg(not(unix))]
async fn delete_upload_file(root: &Path, path: &Path) -> std::io::Result<()> {
    let _ = root;
    fs::remove_file(path).await
}

/// # Errors
///
/// Returns an error when the upload session cannot be updated.
pub async fn touch_upload_session(
    root: &Path,
    session_id: &str,
    mut session: OciUploadSession,
) -> Result<(), OciAdapterError> {
    validate_upload_session_id(session_id)?;
    session.last_touched_unix_seconds = unix_now_seconds_checked()?;
    persist_upload_session(root, session_id, &session).await
}

#[must_use]
pub fn upload_session_length(session: &OciUploadSession) -> Option<u64> {
    session.use_s3_multipart.then(|| {
        session
            .s3_multipart
            .as_ref()
            .map_or(0, |multipart| multipart.total_length)
    })
}

pub(crate) const fn upload_session_expired(
    session: &OciUploadSession,
    ttl_seconds: NonZeroU64,
    now_unix_seconds: u64,
) -> bool {
    session
        .last_touched_unix_seconds
        .saturating_add(ttl_seconds.get())
        <= now_unix_seconds
}

pub(crate) async fn count_active_upload_sessions(
    root: &Path,
    ttl_seconds: NonZeroU64,
) -> Result<usize, OciAdapterError> {
    let upload_dir = upload_dir(root);
    let mut entries = match fs::read_dir(upload_dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(OciAdapterError::Io(error)),
    };
    let now_unix_seconds = unix_now_seconds_checked()?;
    let mut active_sessions = 0_usize;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension() != Some(OsStr::new("json")) {
            continue;
        }
        // Validate that the file contains a valid, unexpired session.
        let Ok(bytes) = fs::read(&path).await else {
            continue;
        };
        let Ok(session): Result<OciUploadSession, _> = serde_json::from_slice(&bytes) else {
            continue;
        };
        if upload_session_expired(&session, ttl_seconds, now_unix_seconds) {
            continue;
        }
        active_sessions = active_sessions.saturating_add(1);
    }
    Ok(active_sessions)
}

/// # Errors
///
/// Returns an error when expired upload sessions cannot be purged.
pub async fn purge_expired_upload_sessions<B: OciBackend>(
    root: &Path,
    backend: Option<&B>,
    ttl_seconds: NonZeroU64,
    now_unix_seconds: u64,
) -> Result<(), OciAdapterError> {
    let upload_dir = upload_dir(root);
    let mut entries = match fs::read_dir(upload_dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(OciAdapterError::Io(error)),
    };
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        match path.extension() {
            Some(extension) if extension == OsStr::new("json") => {}
            Some(extension)
                if extension == OsStr::new("bin") || extension == OsStr::new("tail") =>
            {
                let Some(stem) = path.file_stem().and_then(OsStr::to_str) else {
                    continue;
                };
                if validate_upload_session_id(stem).is_err() {
                    continue;
                }
                if fs::metadata(upload_metadata_path(root, stem))
                    .await
                    .is_err()
                {
                    let _deleted = fs::remove_file(&path).await;
                }
                continue;
            }
            _ => continue,
        }
        let Some(stem) = path.file_stem().and_then(OsStr::to_str) else {
            continue;
        };
        if validate_upload_session_id(stem).is_err() {
            continue;
        }
        let bytes = match fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(_error) => {
                delete_upload_session(root, stem).await?;
                continue;
            }
        };
        let session: OciUploadSession = match serde_json::from_slice(&bytes) {
            Ok(session) => session,
            Err(_error) => {
                delete_upload_session(root, stem).await?;
                continue;
            }
        };
        let missing_local_body =
            !session.use_s3_multipart && fs::metadata(upload_body_path(root, stem)).await.is_err();
        if upload_session_expired(&session, ttl_seconds, now_unix_seconds) || missing_local_body {
            // Abort S3 multipart before removing local metadata so orphaned
            // S3 uploads do not accumulate.
            if let (Some(backend), Some(multipart)) = (&backend, &session.s3_multipart)
                && !multipart.upload_id.is_empty()
            {
                let temp_key = shardline_storage::ObjectKey::parse(&multipart.temporary_object_key)
                    .map_err(|_err| OciAdapterError::InvalidContentHash)?;
                let _result = backend
                    .abort_resumable_object_upload(&temp_key, &multipart.upload_id)
                    .await;
            }
            delete_upload_session(root, stem).await?;
        }
    }
    Ok(())
}
