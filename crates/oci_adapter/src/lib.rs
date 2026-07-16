#![deny(unsafe_code)]
#![allow(unknown_lints, clippy::chunks_exact_to_as_chunks)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string,
        clippy::panic,
        clippy::match_wild_err_arm,
        clippy::ignored_unit_patterns
    )
)]

//! OCI Distribution protocol adapter for the Shardline server ecosystem.
//!
//! This crate provides OCI registry protocol support: upload session management,
//! manifest and blob key construction, content-addressed storage helpers, and
//! S3 multipart upload orchestration.

use std::{
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::{Read, Write},
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    sync::LazyLock,
};

use blake3::Hasher as Blake3Hasher;
use bytes::Bytes;
use getrandom::fill as getrandom_fill;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256, compress256, digest::generic_array::GenericArray};
use shardline_protocol::RepositoryScope;
#[cfg(unix)]
use shardline_storage::anchored_fs::{
    AnchoredPathOptions, ensure_parent_path_matches_anchor, open_anchored_target,
    remove_if_present, write_anchored_temporary_file,
};
use shardline_storage::{ObjectIntegrity, ObjectKey, ObjectPrefix, PutOutcome};
use tokio::fs;
#[cfg(not(unix))]
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, MutexGuard};
use tokio::task::spawn_blocking;

mod error;
mod protocol_support;
mod traits;

#[cfg(test)]
mod tests;

pub use error::OciAdapterError;
pub use traits::OciBackend;

use crate::protocol_support::{
    object_key, parse_sha256_digest, scope_namespace, stable_hex_id, validate_oci_repository_name,
    validate_oci_repository_scope, validate_oci_tag, validate_upload_session_id,
};

const OCI_UPLOAD_DIR: &str = "oci-uploads";
const OCI_S3_MULTIPART_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const SHA256_INITIAL_STATE: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];
static OCI_UPLOAD_SESSION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub struct OciUploadSessionLock {
    _process_guard: MutexGuard<'static, ()>,
    _file_lock: OciFileLock,
}

struct OciFileLock {
    file: File,
}

impl Drop for OciFileLock {
    fn drop(&mut self) {
        let _ignored = self.file.unlock();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciUploadSession {
    pub repository: String,
    #[serde(default = "global_scope_namespace")]
    pub scope_namespace: String,
    pub created_at_unix_seconds: u64,
    pub last_touched_unix_seconds: u64,
    #[serde(default)]
    pub use_s3_multipart: bool,
    #[serde(default)]
    pub s3_multipart: Option<OciS3MultipartUploadSession>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciS3MultipartUploadSession {
    pub temporary_object_key: String,
    pub upload_id: String,
    pub uploaded_part_ids: Vec<String>,
    pub total_length: u64,
    pub sha256_state: SerializableSha256State,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableSha256State {
    state: [u32; 8],
    total_length: u64,
    buffer: Vec<u8>,
}

impl Default for SerializableSha256State {
    fn default() -> Self {
        Self {
            state: SHA256_INITIAL_STATE,
            total_length: 0,
            buffer: Vec::new(),
        }
    }
}

impl SerializableSha256State {
    fn update(&mut self, bytes: &[u8]) -> Result<(), OciAdapterError> {
        self.total_length =
            shardline_server_core::checked_add(self.total_length, u64::try_from(bytes.len())?)
                .map_err(|_e| OciAdapterError::Overflow)?;
        let mut remaining = bytes;
        if !self.buffer.is_empty() {
            let needed = 64_usize.saturating_sub(self.buffer.len());
            let to_take = needed.min(remaining.len());
            let (consumed, rest) = remaining.split_at(to_take);
            self.buffer.extend_from_slice(consumed);
            remaining = rest;
            if self.buffer.len() == 64 {
                let block: [u8; 64] = self
                    .buffer
                    .as_slice()
                    .try_into()
                    .map_err(|_error| OciAdapterError::Overflow)?;
                self.compress_block(&block);
                self.buffer.clear();
            }
        }

        let mut chunks = remaining.chunks_exact(64);
        for chunk in &mut chunks {
            let block: [u8; 64] = chunk
                .try_into()
                .map_err(|_error| OciAdapterError::Overflow)?;
            self.compress_block(&block);
        }
        self.buffer.extend_from_slice(chunks.remainder());
        Ok(())
    }

    fn finalize_hex(&self) -> Result<String, OciAdapterError> {
        Ok(hex::encode(self.finalize_bytes()?))
    }

    fn compress_block(&mut self, block: &[u8; 64]) {
        let generic = GenericArray::clone_from_slice(block);
        compress256(&mut self.state, &[generic]);
    }

    fn finalize_bytes(&self) -> Result<[u8; 32], OciAdapterError> {
        let mut state = self.state;
        let mut buffer = self.buffer.clone();
        buffer.push(0x80);
        while buffer.len() % 64 != 56 {
            buffer.push(0);
        }
        let bit_length = self
            .total_length
            .checked_mul(8)
            .ok_or(OciAdapterError::Overflow)?;
        buffer.extend_from_slice(&bit_length.to_be_bytes());
        for chunk in buffer.chunks_exact(64) {
            let block: [u8; 64] = chunk
                .try_into()
                .map_err(|_error| OciAdapterError::Overflow)?;
            let generic = GenericArray::clone_from_slice(&block);
            compress256(&mut state, &[generic]);
        }
        let mut output = [0_u8; 32];
        for (chunk, value) in output.chunks_exact_mut(4).zip(state.iter()) {
            chunk.copy_from_slice(&value.to_be_bytes());
        }
        Ok(output)
    }
}

fn global_scope_namespace() -> String {
    "global".to_owned()
}

/// # Errors
///
/// Returns an error when the repository name is not a valid OCI repository name.
pub fn validate_repository(repository: &str) -> Result<(), OciAdapterError> {
    validate_oci_repository_name(repository)
}

/// # Errors
///
/// Returns an error when the reference is not a valid OCI tag or digest.
pub fn parse_reference(reference: &str) -> Result<OciReference, OciAdapterError> {
    if reference.starts_with("sha256:") {
        return Ok(OciReference::Digest(parse_sha256_digest(reference)?));
    }
    validate_oci_tag(reference)?;
    Ok(OciReference::Tag(reference.to_owned()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OciReference {
    Digest(String),
    Tag(String),
}

/// # Errors
///
/// Returns an error when the repository, digest, or scope is invalid.
pub fn oci_blob_key(
    repository: &str,
    digest_hex: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    object_key(&format!(
        "protocols/oci/{}/repos/{}/blobs/{}",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        digest_hex
    ))
}

/// # Errors
///
/// Returns an error when the repository, digest, or scope is invalid.
pub fn oci_manifest_key(
    repository: &str,
    digest_hex: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    object_key(&format!(
        "protocols/oci/{}/repos/{}/manifests/{}",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        digest_hex
    ))
}

/// # Errors
///
/// Returns an error when the repository, digest, or scope is invalid.
pub fn oci_manifest_media_type_key(
    repository: &str,
    digest_hex: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    object_key(&format!(
        "protocols/oci/{}/repos/{}/manifest-media-types/{}",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        digest_hex
    ))
}

/// # Errors
///
/// Returns an error when the repository, tag, or scope is invalid.
pub fn oci_tag_key(
    repository: &str,
    tag: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    validate_oci_tag(tag)?;
    object_key(&format!(
        "protocols/oci/{}/repos/{}/tags/{}",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        tag
    ))
}

/// Returns the object prefix for manifest digests in an OCI repository.
///
/// # Errors
///
/// Returns [`OciAdapterError`] when the repository name is invalid.
pub fn oci_manifest_prefix(
    repository: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectPrefix, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    ObjectPrefix::parse(&format!(
        "protocols/oci/{}/repos/{}/manifests/",
        scope_namespace(repository_scope),
        stable_hex_id(repository)
    ))
    .map_err(OciAdapterError::from)
}

/// # Errors
///
/// Returns [`OciAdapterError`] when the repository name is invalid or contains an
/// unsafe path.
pub fn oci_tag_prefix(
    repository: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectPrefix, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    ObjectPrefix::parse(&format!(
        "protocols/oci/{}/repos/{}/tags/",
        scope_namespace(repository_scope),
        stable_hex_id(repository)
    ))
    .map_err(OciAdapterError::from)
}

/// # Errors
///
/// Returns an error when the repository, digest, tag, or scope is invalid.
pub fn oci_tag_target_key(
    repository: &str,
    digest_hex: &str,
    tag: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    parse_sha256_digest(&format!("sha256:{digest_hex}"))?;
    validate_oci_tag(tag)?;
    object_key(&format!(
        "protocols/oci/{}/repos/{}/tag-targets/{}/{}",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        digest_hex,
        tag
    ))
}

/// # Errors
///
/// Returns an error when the repository, digest, or scope is invalid.
pub fn oci_tag_target_prefix(
    repository: &str,
    digest_hex: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectPrefix, OciAdapterError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    parse_sha256_digest(&format!("sha256:{digest_hex}"))?;
    ObjectPrefix::parse(&format!(
        "protocols/oci/{}/repos/{}/tag-targets/{}/",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        digest_hex
    ))
    .map_err(OciAdapterError::from)
}

#[must_use]
pub fn oci_blob_location(repository: &str, digest_hex: &str) -> String {
    format!("/v2/{repository}/blobs/sha256:{digest_hex}")
}

#[must_use]
pub fn oci_manifest_location(repository: &str, reference: &str) -> String {
    format!("/v2/{repository}/manifests/{reference}")
}

#[must_use]
pub fn upload_session_location(repository: &str, session_id: &str) -> String {
    format!("/v2/{repository}/blobs/uploads/{session_id}")
}

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

fn upload_dir(root: &Path) -> PathBuf {
    root.join(OCI_UPLOAD_DIR)
}

fn upload_session_lock_path(root: &Path) -> PathBuf {
    upload_dir(root).join(".sessions.lock")
}

fn upload_metadata_path(root: &Path, session_id: &str) -> PathBuf {
    upload_dir(root).join(format!("{session_id}.json"))
}

fn upload_body_path(root: &Path, session_id: &str) -> PathBuf {
    upload_dir(root).join(format!("{session_id}.bin"))
}

fn upload_tail_path(root: &Path, session_id: &str) -> PathBuf {
    upload_dir(root).join(format!("{session_id}.tail"))
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
    let missing_local_body = !session.use_s3_multipart
        && upload_file_exists_async(root, &body_path).await.is_err();
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

fn map_not_found(error: std::io::Error) -> OciAdapterError {
    if error.kind() == std::io::ErrorKind::NotFound {
        OciAdapterError::NotFound
    } else {
        OciAdapterError::Io(error)
    }
}

#[cfg(unix)]
async fn append_upload_bytes_impl(
    root: &Path,
    path: PathBuf,
    bytes: &[u8],
) -> Result<u64, OciAdapterError> {
    let root = root.to_path_buf();
    let bytes = bytes.to_vec();
    spawn_blocking(move || append_file_anchored(&root, &path, &bytes))
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
    upload_file_len_async(root, &path)
        .await
        .map_err(|error| {
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
        let mut file = open_anchored_file(&root, &path)?;
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
    spawn_blocking(move || delete_file_anchored(&root, &path))
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
    persist_upload_session(root, session_id, session).await
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

/// # Errors
///
/// Returns an error when the S3 multipart upload bytes cannot be appended.
pub async fn append_s3_multipart_upload_bytes<B: OciBackend>(
    root: &Path,
    backend: &B,
    session_id: &str,
    mut session: OciUploadSession,
    bytes: &[u8],
) -> Result<(OciUploadSession, u64), OciAdapterError> {
    validate_upload_session_id(session_id)?;
    if !session.use_s3_multipart {
        return Err(OciAdapterError::NotFound);
    }
    if bytes.is_empty() {
        let total_length = session
            .s3_multipart
            .as_ref()
            .map_or(0, |multipart| multipart.total_length);
        session.last_touched_unix_seconds = unix_now_seconds_checked()?;
        persist_upload_session(root, session_id, session.clone()).await?;
        return Ok((session, total_length));
    }

    ensure_s3_upload_started(root, backend, session_id, &mut session).await?;
    let mut tail = read_upload_tail(root, session_id).await?;
    tail.extend_from_slice(bytes);
    let total_length = {
        let multipart = session
            .s3_multipart
            .as_mut()
            .ok_or(OciAdapterError::NotFound)?;
        multipart.sha256_state.update(bytes)?;
        multipart.total_length =
            shardline_server_core::checked_add(multipart.total_length, u64::try_from(bytes.len())?)
                .map_err(|_e| OciAdapterError::Overflow)?;

        let temporary_object_key = ObjectKey::parse(&multipart.temporary_object_key)
            .map_err(|_error| OciAdapterError::InvalidContentHash)?;
        let upload_id = multipart.upload_id.clone();
        while tail.len() >= OCI_S3_MULTIPART_CHUNK_BYTES {
            let part_bytes: Vec<u8> = tail.drain(..OCI_S3_MULTIPART_CHUNK_BYTES).collect();
            let part_id = backend
                .upload_resumable_object_part(
                    &temporary_object_key,
                    &upload_id,
                    multipart.uploaded_part_ids.len(),
                    Bytes::from(part_bytes),
                )
                .await?;
            multipart.uploaded_part_ids.push(part_id);
        }
        multipart.total_length
    };
    write_upload_tail(root, session_id, &tail).await?;
    session.last_touched_unix_seconds = unix_now_seconds_checked()?;
    persist_upload_session(root, session_id, session.clone()).await?;
    Ok((session, total_length))
}

/// # Errors
///
/// Returns an error when the S3 multipart upload session cannot be finalized.
pub async fn finalize_s3_multipart_upload_session<B: OciBackend>(
    root: &Path,
    backend: &B,
    session_id: &str,
    session: OciUploadSession,
    object_key: &ObjectKey,
    digest_hex: &str,
    final_bytes: &[u8],
) -> Result<PutOutcome, OciAdapterError> {
    validate_upload_session_id(session_id)?;
    if !session.use_s3_multipart {
        return Err(OciAdapterError::NotFound);
    }
    let (session, _new_length) =
        append_s3_multipart_upload_bytes(root, backend, session_id, session, final_bytes).await?;
    let Some(multipart) = session.s3_multipart.as_ref() else {
        let observed = SerializableSha256State::default().finalize_hex()?;
        if observed != digest_hex {
            return Err(OciAdapterError::ExpectedBodyHashMismatch);
        }
        return backend.put_sha256_addressed_object_bytes_if_absent(
            object_key,
            digest_hex,
            Vec::new(),
        );
    };

    let observed = multipart.sha256_state.finalize_hex()?;
    let temporary_object_key = ObjectKey::parse(&multipart.temporary_object_key)
        .map_err(|_error| OciAdapterError::InvalidContentHash)?;
    if observed != digest_hex {
        let _ignored = backend
            .abort_resumable_object_upload(&temporary_object_key, &multipart.upload_id)
            .await;
        return Err(OciAdapterError::ExpectedBodyHashMismatch);
    }

    // Save IDs before fallible operations so we can abort on failure.
    let temp_key = temporary_object_key;
    let upload_id = multipart.upload_id.clone();

    let mut part_ids: Vec<String> = multipart.uploaded_part_ids.clone();
    let tail = read_upload_tail(root, session_id).await?;
    if !tail.is_empty() {
        match backend
            .upload_resumable_object_part(&temp_key, &upload_id, part_ids.len(), Bytes::from(tail))
            .await
        {
            Ok(part_id) => part_ids.push(part_id),
            Err(error) => {
                let _result = backend
                    .abort_resumable_object_upload(&temp_key, &upload_id)
                    .await;
                return Err(error);
            }
        }
    }
    if part_ids.is_empty() {
        let _ignored = backend
            .abort_resumable_object_upload(&temp_key, &upload_id)
            .await;
        return backend.put_sha256_addressed_object_bytes_if_absent(
            object_key,
            digest_hex,
            Vec::new(),
        );
    }

    // Attach part numbers for ordering validation by the S3 backend.
    let parts: Vec<(usize, String)> = part_ids.into_iter().enumerate().collect();

    match backend
        .complete_resumable_object_upload(&temp_key, &upload_id, parts)
        .await
    {
        Ok(()) => {}
        Err(error) => {
            let _result = backend
                .abort_resumable_object_upload(&temp_key, &upload_id)
                .await;
            return Err(error);
        }
    }
    let canonical_key = crate::protocol_support::shared_sha256_object_key(digest_hex)?;
    let canonical_outcome = backend.copy_object_if_absent(&temp_key, &canonical_key)?;
    let _deleted = backend.delete_object_if_present(&temp_key).await?;
    if canonical_key == *object_key {
        return Ok(canonical_outcome);
    }
    backend.copy_object_if_absent(&canonical_key, object_key)
}

/// # Errors
///
/// Returns an error when the S3 multipart upload session cannot be aborted.
pub async fn abort_s3_multipart_upload_session<B: OciBackend>(
    backend: &B,
    session: &OciUploadSession,
) -> Result<(), OciAdapterError> {
    let Some(multipart) = session.s3_multipart.as_ref() else {
        return Ok(());
    };
    let temporary_object_key = ObjectKey::parse(&multipart.temporary_object_key)
        .map_err(|_error| OciAdapterError::InvalidContentHash)?;
    backend
        .abort_resumable_object_upload(&temporary_object_key, &multipart.upload_id)
        .await
}

const fn upload_session_expired(
    session: &OciUploadSession,
    ttl_seconds: NonZeroU64,
    now_unix_seconds: u64,
) -> bool {
    session
        .last_touched_unix_seconds
        .saturating_add(ttl_seconds.get())
        <= now_unix_seconds
}

async fn count_active_upload_sessions(
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

async fn acquire_upload_session_file_lock(path: PathBuf) -> Result<OciFileLock, OciAdapterError> {
    spawn_blocking(move || {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.lock()?;
        Ok(OciFileLock { file })
    })
    .await
    .map_err(OciAdapterError::BlockingTask)?
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
                let temp_key = ObjectKey::parse(&multipart.temporary_object_key)
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

async fn write_upload_metadata(
    root: &Path,
    session_id: &str,
    bytes: Vec<u8>,
) -> Result<(), OciAdapterError> {
    validate_upload_session_id(session_id)?;
    let root = root.to_path_buf();
    let path = upload_metadata_path(&root, session_id);
    spawn_blocking(move || write_file_atomically(&root, &path, &bytes))
        .await
        .map_err(OciAdapterError::BlockingTask)?
        .map_err(OciAdapterError::Io)
}

async fn persist_upload_session(
    root: &Path,
    session_id: &str,
    session: OciUploadSession,
) -> Result<(), OciAdapterError> {
    let bytes = serde_json::to_vec(&session)?;
    write_upload_metadata(root, session_id, bytes).await
}

async fn ensure_s3_upload_started<B: OciBackend>(
    root: &Path,
    backend: &B,
    session_id: &str,
    session: &mut OciUploadSession,
) -> Result<(), OciAdapterError> {
    if session.s3_multipart.is_some() {
        return Ok(());
    }
    let temporary_object_key =
        oci_upload_temporary_object_key(&session.repository, &session.scope_namespace, session_id)?;

    // Persist a placeholder first so the upload_id is recoverable even if
    // the S3 create or subsequent persist fails.
    session.s3_multipart = Some(OciS3MultipartUploadSession {
        temporary_object_key: temporary_object_key.as_str().to_owned(),
        upload_id: String::new(),
        uploaded_part_ids: Vec::new(),
        total_length: 0,
        sha256_state: SerializableSha256State::default(),
    });
    persist_upload_session(root, session_id, session.clone()).await?;

    let Some(upload_id) = backend
        .create_resumable_object_upload(&temporary_object_key)
        .await?
    else {
        session.s3_multipart = None;
        // Overwrite the placeholder so on-disk state matches in-memory.
        // A subsequent read will attempt S3 upload creation again.
        let _result = persist_upload_session(root, session_id, session.clone()).await;
        return Err(OciAdapterError::NotFound);
    };

    session.s3_multipart = Some(OciS3MultipartUploadSession {
        temporary_object_key: temporary_object_key.as_str().to_owned(),
        upload_id,
        uploaded_part_ids: Vec::new(),
        total_length: 0,
        sha256_state: SerializableSha256State::default(),
    });
    persist_upload_session(root, session_id, session.clone()).await
}

fn oci_upload_temporary_object_key(
    repository: &str,
    scope_namespace: &str,
    session_id: &str,
) -> Result<ObjectKey, OciAdapterError> {
    validate_repository(repository)?;
    validate_upload_session_id(session_id)?;
    object_key(&format!(
        "protocols/oci/{scope_namespace}/repos/{}/upload-sessions/{session_id}",
        stable_hex_id(repository),
    ))
}

async fn read_upload_tail(root: &Path, session_id: &str) -> Result<Vec<u8>, OciAdapterError> {
    validate_upload_session_id(session_id)?;
    match fs::read(upload_tail_path(root, session_id)).await {
        Ok(bytes) => Ok(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(OciAdapterError::Io(error)),
    }
}

async fn write_upload_tail(
    root: &Path,
    session_id: &str,
    bytes: &[u8],
) -> Result<(), OciAdapterError> {
    validate_upload_session_id(session_id)?;
    let path = upload_tail_path(root, session_id);
    if bytes.is_empty() {
        match fs::remove_file(&path).await {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(OciAdapterError::Io(error)),
        }
    }
    fs::write(path, bytes).await.map_err(OciAdapterError::Io)
}

/// Opens a file under `root` using fd-relative paths that cannot follow symlinks.
///
/// Returns the opened file. The caller must not use the returned path outside of
/// `/proc/self/fd/` — see [`AnchoredTarget::final_path`].
#[cfg(unix)]
fn open_anchored_file(root: &Path, path: &Path) -> std::io::Result<File> {
    let anchored = open_anchored_target(
        root,
        path,
        AnchoredPathOptions::new(None, None),
        || std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root"),
    )?;
    let file = OpenOptions::new()
        .read(true)
        .open(anchored.final_path())?;
    Ok(file)
}

/// Reads a file under `root` using anchored (symlink-resistant) path resolution.
#[cfg(unix)]
fn read_file_anchored(root: &Path, path: &Path) -> std::io::Result<Vec<u8>> {
    let anchored = open_anchored_target(
        root,
        path,
        AnchoredPathOptions::new(None, None),
        || std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root"),
    )?;
    std::fs::read(anchored.final_path())
}

/// Deletes a file under `root` using anchored (symlink-resistant) path resolution.
///
/// After deletion, verifies that the parent directory has not been replaced
/// (catches TOCTOU rename+swap attacks).
#[cfg(unix)]
fn delete_file_anchored(root: &Path, path: &Path) -> std::io::Result<()> {
    let anchored = open_anchored_target(
        root,
        path,
        AnchoredPathOptions::new(None, None),
        || std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root"),
    )?;
    let final_path = anchored.final_path();
    match std::fs::remove_file(&final_path) {
        Ok(()) => {
            ensure_parent_path_matches_anchor(
                &anchored,
                "upload directory path changed during anchored delete",
            )?;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// Appends bytes to a file under `root` using anchored (symlink-resistant) path resolution.
///
/// Returns the new file length after the append.
#[cfg(unix)]
fn append_file_anchored(root: &Path, path: &Path, bytes: &[u8]) -> std::io::Result<u64> {
    let anchored = open_anchored_target(
        root,
        path,
        AnchoredPathOptions::new(None, None),
        || std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root"),
    )?;
    let mut file = OpenOptions::new().append(true).open(anchored.final_path())?;
    file.write_all(bytes)?;
    let metadata = file.metadata()?;
    Ok(metadata.len())
}

/// Opens a file under `root` for append using anchored (symlink-resistant) path resolution.
///
/// Reads a file under the OCI upload root using anchored (symlink-resistant) I/O.
#[cfg(unix)]
async fn read_upload_file_async(root: &Path, path: &Path) -> std::io::Result<Vec<u8>> {
    let root = root.to_path_buf();
    let path = path.to_path_buf();
    spawn_blocking(move || read_file_anchored(&root, &path)).await
        .map_err(std::io::Error::other)?
}

/// Returns the file length for a file under the OCI upload root using anchored I/O.
#[cfg(unix)]
async fn upload_file_len_async(root: &Path, path: &Path) -> std::io::Result<u64> {
    let root = root.to_path_buf();
    let path = path.to_path_buf();
    spawn_blocking(move || {
        let anchored = open_anchored_target(
            &root,
            &path,
            AnchoredPathOptions::new(None, None),
            || std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root"),
        )?;
        let file = File::open(anchored.final_path())?;
        let metadata = file.metadata()?;
        Ok(metadata.len())
    })
    .await
    .map_err(std::io::Error::other)?
}
 
/// Checks if a file under the OCI upload root exists using anchored I/O.
#[cfg(unix)]
async fn upload_file_exists_async(root: &Path, path: &Path) -> std::io::Result<()> {
    let root = root.to_path_buf();
    let path = path.to_path_buf();
    spawn_blocking(move || {
        let anchored = open_anchored_target(
            &root,
            &path,
            AnchoredPathOptions::new(None, None),
            || std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root"),
        )?;
        match File::open(anchored.final_path()) {
            Ok(_file) => Ok(()),
            Err(error) => Err(error),
        }
    })
    .await
    .map_err(std::io::Error::other)?
 }
 
#[cfg(not(unix))]
async fn read_upload_file_async(root: &Path, path: &Path) -> std::io::Result<Vec<u8>> {
    let _ = root;
    fs::read(path).await
}

#[cfg(not(unix))]
async fn upload_file_len_async(root: &Path, path: &Path) -> std::io::Result<u64> {
    let _ = root;
    fs::metadata(path).await.map(|m| m.len())
}

#[cfg(not(unix))]
async fn upload_file_exists_async(root: &Path, path: &Path) -> std::io::Result<()> {
    let _ = root;
    fs::metadata(path).await.map(|_| ())
}

fn unix_now_seconds_checked() -> Result<u64, OciAdapterError> {
    shardline_server_core::unix_now_seconds_checked().map_err(|_e| OciAdapterError::Overflow)
}

#[cfg(unix)]
fn write_file_atomically(root: &Path, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    fn invalid_path_error() -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "path must have a parent directory",
        )
    }
    let anchored = open_anchored_target(
        root,
        path,
        AnchoredPathOptions::new(None, None),
        invalid_path_error,
    )?;
    let final_path = anchored.final_path();
    let temporary = write_anchored_temporary_file(&anchored, bytes, None)?;
    match std::fs::rename(&temporary, &final_path) {
        Ok(()) => {}
        Err(error) => {
            remove_if_present(&temporary)?;
            return Err(error);
        }
    }
    if let Err(error) = ensure_parent_path_matches_anchor(
        &anchored,
        "upload directory path changed during anchored write",
    ) {
        remove_if_present(&final_path)?;
        return Err(error);
    }
    Ok(())
}

#[cfg(not(unix))]
fn write_file_atomically(root: &Path, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    // Defense-in-depth: ensure the path stays within the root directory.
    path.strip_prefix(root)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root"))?;
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "path must have a parent directory",
        )
    })?;
    std::fs::create_dir_all(parent)?;
    let temporary = write_temporary_file(path, bytes)?;
    std::fs::rename(&temporary, path)?;
    Ok(())
}

#[cfg(not(unix))]
fn write_temporary_file(path: &Path, bytes: &[u8]) -> std::io::Result<std::path::PathBuf> {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id();
    let seq = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = path.with_extension(format!("tmp-{pid}-{seq}-{now_nanos}"));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.flush()?;
    Ok(temporary)
}

#[cfg(test)]
mod write_file_atomically_tests {
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    use super::write_file_atomically;

    fn temp_root() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_path_buf();
        (dir, root)
    }

    #[test]
    fn creates_file_with_expected_content() {
        let (_dir, root) = temp_root();
        let path = root.join("metadata.json");
        let payload = b"{\"key\":\"value\"}";
        write_file_atomically(&root, &path, payload).unwrap();
        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, payload);
    }

    #[test]
    fn overwrites_existing_file() {
        let (_dir, root) = temp_root();
        let path = root.join("data.json");
        write_file_atomically(&root, &path, b"first").unwrap();
        write_file_atomically(&root, &path, b"second").unwrap();
        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"second");
    }

    #[test]
    fn handles_empty_bytes() {
        let (_dir, root) = temp_root();
        let path = root.join("empty.json");
        write_file_atomically(&root, &path, b"").unwrap();
        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"");
    }

    #[test]
    fn writes_into_nested_subdirectory() {
        let (_dir, root) = temp_root();
        let path = root.join("sub/dir/file.json");
        write_file_atomically(&root, &path, b"nested").unwrap();
        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"nested");
    }

    #[test]
    fn rejects_path_escaping_root() {
        let (_dir, root) = temp_root();
        // A path that resolves outside of root via `..`
        let path = root.join("../outside.json");
        let result = write_file_atomically(&root, &path, b"escape");
        assert!(result.is_err(), "must reject path escaping root");
    }

    #[test]
    fn rejects_path_absolute_outside_root() {
        let (_dir, root) = temp_root();
        let path = Path::new("/tmp/not-under-root.json").to_path_buf();
        let result = write_file_atomically(&root, &path, b"escape");
        assert!(result.is_err(), "must reject absolute path outside root");
    }

    #[test]
    fn respects_root_distinct_dirs() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();
        let path1 = dir1.path().join("file.json");
        let path2 = dir2.path().join("file.json");
        write_file_atomically(dir1.path(), &path1, b"alpha").unwrap();
        write_file_atomically(dir2.path(), &path2, b"beta").unwrap();
        assert_eq!(std::fs::read(&path1).unwrap(), b"alpha");
        assert_eq!(std::fs::read(&path2).unwrap(), b"beta");
    }

    #[cfg(unix)]
    #[test]
    fn rename_failure_cleans_up_temporary() {
        let (_dir, root) = temp_root();
        // Create a non-empty directory at the target path so rename fails
        // (rename(2) cannot overwrite a non-empty directory with a file).
        let target = root.join("target.json");
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("child"), b"x").unwrap();

        let result = write_file_atomically(&root, &target, b"data");
        assert!(
            result.is_err(),
            "rename should fail when target is a non-empty directory"
        );
        // The temporary file must have been cleaned up:
        // the directory should still contain only "child".
        let entries: Vec<_> = std::fs::read_dir(&target)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 1, "temp file should have been cleaned up");
    }

    #[cfg(unix)]
    #[test]
    fn post_rename_parent_anchor_check_passes() {
        // Verifies that ensure_parent_path_matches_anchor passes after a
        // successful atomic write — the post-rename integrity check should
        // not reject a valid path.
        let (_dir, root) = temp_root();
        let path = root.join("metadata.json");
        write_file_atomically(&root, &path, b"payload").unwrap();
        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"payload");
    }
}
