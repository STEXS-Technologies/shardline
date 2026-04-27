use std::{
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::Read,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    sync::LazyLock,
};

use blake3::Hasher as Blake3Hasher;
use bytes::Bytes;
use getrandom::fill as getrandom_fill;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256, compress256, digest::generic_array::GenericArray};
use shardline_storage::ObjectIntegrity;
use shardline_storage::{ObjectKey, ObjectPrefix};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, MutexGuard};
use tokio::task::spawn_blocking;

use crate::{
    ServerError,
    backend::ServerBackend,
    clock::unix_now_seconds_checked,
    local_fs::write_file_atomically,
    overflow::checked_add,
    protocol_support::{
        object_key, parse_sha256_digest, scope_namespace, stable_hex_id,
        validate_oci_repository_name, validate_oci_repository_scope, validate_oci_tag,
        validate_upload_session_id,
    },
};
use shardline_protocol::RepositoryScope;

const OCI_UPLOAD_DIR: &str = "oci-uploads";
const OCI_S3_MULTIPART_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const SHA256_INITIAL_STATE: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];
static OCI_UPLOAD_SESSION_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub(crate) struct OciUploadSessionLock {
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
pub(crate) struct OciUploadSession {
    pub(crate) repository: String,
    #[serde(default = "global_scope_namespace")]
    pub(crate) scope_namespace: String,
    pub(crate) created_at_unix_seconds: u64,
    pub(crate) last_touched_unix_seconds: u64,
    #[serde(default)]
    pub(crate) use_s3_multipart: bool,
    #[serde(default)]
    pub(crate) s3_multipart: Option<OciS3MultipartUploadSession>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OciS3MultipartUploadSession {
    pub(crate) temporary_object_key: String,
    pub(crate) upload_id: String,
    pub(crate) uploaded_part_ids: Vec<String>,
    pub(crate) total_length: u64,
    pub(crate) sha256_state: SerializableSha256State,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SerializableSha256State {
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
    fn update(&mut self, bytes: &[u8]) -> Result<(), ServerError> {
        self.total_length = checked_add(self.total_length, u64::try_from(bytes.len())?)?;
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
                    .map_err(|_error| ServerError::Overflow)?;
                self.compress_block(&block);
                self.buffer.clear();
            }
        }

        let mut chunks = remaining.chunks_exact(64);
        for chunk in &mut chunks {
            let block: [u8; 64] = chunk.try_into().map_err(|_error| ServerError::Overflow)?;
            self.compress_block(&block);
        }
        self.buffer.extend_from_slice(chunks.remainder());
        Ok(())
    }

    fn finalize_hex(&self) -> Result<String, ServerError> {
        Ok(hex::encode(self.finalize_bytes()?))
    }

    fn compress_block(&mut self, block: &[u8; 64]) {
        let generic = GenericArray::clone_from_slice(block);
        compress256(&mut self.state, &[generic]);
    }

    fn finalize_bytes(&self) -> Result<[u8; 32], ServerError> {
        let mut state = self.state;
        let mut buffer = self.buffer.clone();
        buffer.push(0x80);
        while buffer.len() % 64 != 56 {
            buffer.push(0);
        }
        let bit_length = self
            .total_length
            .checked_mul(8)
            .ok_or(ServerError::Overflow)?;
        buffer.extend_from_slice(&bit_length.to_be_bytes());
        for chunk in buffer.chunks_exact(64) {
            let block: [u8; 64] = chunk.try_into().map_err(|_error| ServerError::Overflow)?;
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

pub(crate) fn validate_repository(repository: &str) -> Result<(), ServerError> {
    validate_oci_repository_name(repository)
}

pub(crate) fn parse_reference(reference: &str) -> Result<OciReference, ServerError> {
    if reference.starts_with("sha256:") {
        return Ok(OciReference::Digest(parse_sha256_digest(reference)?));
    }
    validate_oci_tag(reference)?;
    Ok(OciReference::Tag(reference.to_owned()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OciReference {
    Digest(String),
    Tag(String),
}

pub(crate) fn oci_blob_key(
    repository: &str,
    digest_hex: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, ServerError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    object_key(&format!(
        "protocols/oci/{}/repos/{}/blobs/{}",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        digest_hex
    ))
}

pub(crate) fn oci_manifest_key(
    repository: &str,
    digest_hex: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, ServerError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    object_key(&format!(
        "protocols/oci/{}/repos/{}/manifests/{}",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        digest_hex
    ))
}

pub(crate) fn oci_manifest_media_type_key(
    repository: &str,
    digest_hex: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, ServerError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    object_key(&format!(
        "protocols/oci/{}/repos/{}/manifest-media-types/{}",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        digest_hex
    ))
}

pub(crate) fn oci_tag_key(
    repository: &str,
    tag: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, ServerError> {
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

pub(crate) fn oci_tag_prefix(
    repository: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectPrefix, ServerError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    ObjectPrefix::parse(&format!(
        "protocols/oci/{}/repos/{}/tags/",
        scope_namespace(repository_scope),
        stable_hex_id(repository)
    ))
    .map_err(ServerError::from)
}

pub(crate) fn oci_tag_target_key(
    repository: &str,
    digest_hex: &str,
    tag: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectKey, ServerError> {
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

pub(crate) fn oci_tag_target_prefix(
    repository: &str,
    digest_hex: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ObjectPrefix, ServerError> {
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    parse_sha256_digest(&format!("sha256:{digest_hex}"))?;
    ObjectPrefix::parse(&format!(
        "protocols/oci/{}/repos/{}/tag-targets/{}/",
        scope_namespace(repository_scope),
        stable_hex_id(repository),
        digest_hex
    ))
    .map_err(ServerError::from)
}

pub(crate) fn oci_blob_location(repository: &str, digest_hex: &str) -> String {
    format!("/v2/{repository}/blobs/sha256:{digest_hex}")
}

pub(crate) fn oci_manifest_location(repository: &str, reference: &str) -> String {
    format!("/v2/{repository}/manifests/{reference}")
}

pub(crate) fn upload_session_location(repository: &str, session_id: &str) -> String {
    format!("/v2/{repository}/blobs/uploads/{session_id}")
}

pub(crate) fn new_upload_session_id() -> String {
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

pub(crate) async fn lock_upload_sessions(root: &Path) -> Result<OciUploadSessionLock, ServerError> {
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

pub(crate) async fn create_upload_session(
    root: &Path,
    repository: &str,
    repository_scope: Option<&RepositoryScope>,
    ttl_seconds: NonZeroU64,
    max_active_sessions: NonZeroUsize,
    use_s3_multipart: bool,
) -> Result<String, ServerError> {
    let _lock = lock_upload_sessions(root).await?;
    validate_repository(repository)?;
    validate_oci_repository_scope(repository, repository_scope)?;
    let now_unix_seconds = unix_now_seconds_checked()?;
    purge_expired_upload_sessions(root, ttl_seconds, now_unix_seconds).await?;
    let active_sessions = count_active_upload_sessions(root).await?;
    if active_sessions >= max_active_sessions.get() {
        return Err(ServerError::TooManyUploadSessions);
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

pub(crate) async fn read_upload_session(
    root: &Path,
    session_id: &str,
    ttl_seconds: NonZeroU64,
) -> Result<OciUploadSession, ServerError> {
    validate_upload_session_id(session_id)?;
    let bytes = fs::read(upload_metadata_path(root, session_id))
        .await
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ServerError::NotFound
            } else {
                ServerError::Io(error)
            }
        })?;
    let session: OciUploadSession = serde_json::from_slice(&bytes)?;
    let now_unix_seconds = unix_now_seconds_checked()?;
    if upload_session_expired(&session, ttl_seconds, now_unix_seconds) {
        delete_upload_session(root, session_id).await?;
        return Err(ServerError::NotFound);
    }
    let missing_local_body = !session.use_s3_multipart
        && fs::metadata(upload_body_path(root, session_id))
            .await
            .is_err();
    if missing_local_body {
        delete_upload_session(root, session_id).await?;
        return Err(ServerError::NotFound);
    }
    Ok(session)
}

pub(crate) async fn append_upload_bytes(
    root: &Path,
    session_id: &str,
    bytes: &[u8],
) -> Result<u64, ServerError> {
    validate_upload_session_id(session_id)?;
    let path = upload_body_path(root, session_id);
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ServerError::NotFound
            } else {
                ServerError::Io(error)
            }
        })?;
    file.write_all(bytes).await?;
    let metadata = file.metadata().await?;
    Ok(metadata.len())
}

pub(crate) async fn upload_length(root: &Path, session_id: &str) -> Result<u64, ServerError> {
    validate_upload_session_id(session_id)?;
    let metadata = fs::metadata(upload_body_path(root, session_id))
        .await
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ServerError::NotFound
            } else {
                ServerError::Io(error)
            }
        })?;
    Ok(metadata.len())
}

pub(crate) fn upload_body_path_for_session(
    root: &Path,
    session_id: &str,
) -> Result<PathBuf, ServerError> {
    validate_upload_session_id(session_id)?;
    Ok(upload_body_path(root, session_id))
}

pub(crate) async fn upload_body_integrity(
    root: &Path,
    session_id: &str,
) -> Result<(String, ObjectIntegrity), ServerError> {
    validate_upload_session_id(session_id)?;
    let path = upload_body_path(root, session_id);
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
            let slice = buffer.get(..read).ok_or(ServerError::Overflow)?;
            sha256.update(slice);
            blake3.update(slice);
            total_length = total_length
                .checked_add(u64::try_from(read).map_err(|_error| ServerError::Overflow)?)
                .ok_or(ServerError::Overflow)?;
        }
        let sha256_hex = hex::encode(sha256.finalize());
        let blake3_hash =
            shardline_protocol::ShardlineHash::from_bytes(*blake3.finalize().as_bytes());
        Ok::<_, ServerError>((sha256_hex, ObjectIntegrity::new(blake3_hash, total_length)))
    })
    .await
    .map_err(ServerError::BlockingTask)?
}

pub(crate) async fn delete_upload_session(
    root: &Path,
    session_id: &str,
) -> Result<(), ServerError> {
    validate_upload_session_id(session_id)?;
    let metadata_path = upload_metadata_path(root, session_id);
    let body_path = upload_body_path(root, session_id);
    let tail_path = upload_tail_path(root, session_id);
    match fs::remove_file(body_path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(ServerError::Io(error)),
    }
    match fs::remove_file(tail_path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(ServerError::Io(error)),
    }
    match fs::remove_file(metadata_path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(ServerError::Io(error)),
    }
    Ok(())
}

pub(crate) async fn touch_upload_session(
    root: &Path,
    session_id: &str,
    mut session: OciUploadSession,
) -> Result<(), ServerError> {
    validate_upload_session_id(session_id)?;
    session.last_touched_unix_seconds = unix_now_seconds_checked()?;
    persist_upload_session(root, session_id, session).await
}

pub(crate) fn upload_session_length(session: &OciUploadSession) -> Option<u64> {
    session.use_s3_multipart.then(|| {
        session
            .s3_multipart
            .as_ref()
            .map_or(0, |multipart| multipart.total_length)
    })
}

pub(crate) async fn append_s3_multipart_upload_bytes(
    root: &Path,
    backend: &ServerBackend,
    session_id: &str,
    mut session: OciUploadSession,
    bytes: &[u8],
) -> Result<(OciUploadSession, u64), ServerError> {
    validate_upload_session_id(session_id)?;
    if !session.use_s3_multipart {
        return Err(ServerError::NotFound);
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
        let multipart = session.s3_multipart.as_mut().ok_or(ServerError::NotFound)?;
        multipart.sha256_state.update(bytes)?;
        multipart.total_length = checked_add(multipart.total_length, u64::try_from(bytes.len())?)?;

        let temporary_object_key = ObjectKey::parse(&multipart.temporary_object_key)
            .map_err(|_error| ServerError::InvalidContentHash)?;
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

pub(crate) async fn finalize_s3_multipart_upload_session(
    root: &Path,
    backend: &ServerBackend,
    session_id: &str,
    session: OciUploadSession,
    object_key: &ObjectKey,
    digest_hex: &str,
    final_bytes: &[u8],
) -> Result<shardline_storage::PutOutcome, ServerError> {
    validate_upload_session_id(session_id)?;
    if !session.use_s3_multipart {
        return Err(ServerError::NotFound);
    }
    let (session, _new_length) =
        append_s3_multipart_upload_bytes(root, backend, session_id, session, final_bytes).await?;
    let Some(multipart) = session.s3_multipart.as_ref() else {
        let observed = SerializableSha256State::default().finalize_hex()?;
        if observed != digest_hex {
            return Err(ServerError::ExpectedBodyHashMismatch);
        }
        return backend.put_sha256_addressed_object_bytes_if_absent(
            object_key,
            digest_hex,
            Vec::new(),
        );
    };

    let observed = multipart.sha256_state.finalize_hex()?;
    let temporary_object_key = ObjectKey::parse(&multipart.temporary_object_key)
        .map_err(|_error| ServerError::InvalidContentHash)?;
    if observed != digest_hex {
        let _ignored = backend
            .abort_resumable_object_upload(&temporary_object_key, &multipart.upload_id)
            .await;
        return Err(ServerError::ExpectedBodyHashMismatch);
    }

    let mut part_ids = multipart.uploaded_part_ids.clone();
    let tail = read_upload_tail(root, session_id).await?;
    if !tail.is_empty() {
        let part_id = backend
            .upload_resumable_object_part(
                &temporary_object_key,
                &multipart.upload_id,
                part_ids.len(),
                Bytes::from(tail),
            )
            .await?;
        part_ids.push(part_id);
    }
    if part_ids.is_empty() {
        let _ignored = backend
            .abort_resumable_object_upload(&temporary_object_key, &multipart.upload_id)
            .await;
        return backend.put_sha256_addressed_object_bytes_if_absent(
            object_key,
            digest_hex,
            Vec::new(),
        );
    }

    backend
        .complete_resumable_object_upload(&temporary_object_key, &multipart.upload_id, part_ids)
        .await?;
    let canonical_key = crate::protocol_support::shared_sha256_object_key(digest_hex)?;
    let canonical_outcome = backend.copy_object_if_absent(&temporary_object_key, &canonical_key)?;
    let _deleted = backend
        .delete_object_if_present(&temporary_object_key)
        .await?;
    if canonical_key == *object_key {
        return Ok(canonical_outcome);
    }
    backend.copy_object_if_absent(&canonical_key, object_key)
}

pub(crate) async fn abort_s3_multipart_upload_session(
    backend: &ServerBackend,
    session: &OciUploadSession,
) -> Result<(), ServerError> {
    let Some(multipart) = session.s3_multipart.as_ref() else {
        return Ok(());
    };
    let temporary_object_key = ObjectKey::parse(&multipart.temporary_object_key)
        .map_err(|_error| ServerError::InvalidContentHash)?;
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

async fn count_active_upload_sessions(root: &Path) -> Result<usize, ServerError> {
    let upload_dir = upload_dir(root);
    let mut entries = match fs::read_dir(upload_dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(ServerError::Io(error)),
    };
    let mut active_sessions = 0_usize;
    while let Some(entry) = entries.next_entry().await? {
        if entry.path().extension() == Some(OsStr::new("json")) {
            active_sessions = active_sessions.saturating_add(1);
        }
    }
    Ok(active_sessions)
}

async fn acquire_upload_session_file_lock(path: PathBuf) -> Result<OciFileLock, ServerError> {
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
    .map_err(ServerError::BlockingTask)?
}

pub(crate) async fn purge_expired_upload_sessions(
    root: &Path,
    ttl_seconds: NonZeroU64,
    now_unix_seconds: u64,
) -> Result<(), ServerError> {
    let upload_dir = upload_dir(root);
    let mut entries = match fs::read_dir(upload_dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(ServerError::Io(error)),
    };
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        match path.extension() {
            Some(extension) if extension == OsStr::new("json") => {}
            Some(extension) if extension == OsStr::new("bin") => {
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
            delete_upload_session(root, stem).await?;
        }
    }
    Ok(())
}

async fn write_upload_metadata(
    root: &Path,
    session_id: &str,
    bytes: Vec<u8>,
) -> Result<(), ServerError> {
    validate_upload_session_id(session_id)?;
    let root = root.to_path_buf();
    let path = upload_metadata_path(&root, session_id);
    spawn_blocking(move || write_file_atomically(&root, &path, &bytes))
        .await
        .map_err(ServerError::BlockingTask)?
        .map_err(ServerError::Io)
}

async fn persist_upload_session(
    root: &Path,
    session_id: &str,
    session: OciUploadSession,
) -> Result<(), ServerError> {
    let bytes = serde_json::to_vec(&session)?;
    write_upload_metadata(root, session_id, bytes).await
}

async fn ensure_s3_upload_started(
    root: &Path,
    backend: &ServerBackend,
    session_id: &str,
    session: &mut OciUploadSession,
) -> Result<(), ServerError> {
    if session.s3_multipart.is_some() {
        return Ok(());
    }
    let temporary_object_key =
        oci_upload_temporary_object_key(&session.repository, &session.scope_namespace, session_id)?;
    let Some(upload_id) = backend
        .create_resumable_object_upload(&temporary_object_key)
        .await?
    else {
        return Err(ServerError::NotFound);
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
) -> Result<ObjectKey, ServerError> {
    validate_repository(repository)?;
    validate_upload_session_id(session_id)?;
    object_key(&format!(
        "protocols/oci/{scope_namespace}/repos/{}/upload-sessions/{session_id}",
        stable_hex_id(repository),
    ))
}

async fn read_upload_tail(root: &Path, session_id: &str) -> Result<Vec<u8>, ServerError> {
    validate_upload_session_id(session_id)?;
    match fs::read(upload_tail_path(root, session_id)).await {
        Ok(bytes) => Ok(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(ServerError::Io(error)),
    }
}

async fn write_upload_tail(root: &Path, session_id: &str, bytes: &[u8]) -> Result<(), ServerError> {
    validate_upload_session_id(session_id)?;
    let path = upload_tail_path(root, session_id);
    if bytes.is_empty() {
        match fs::remove_file(&path).await {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(ServerError::Io(error)),
        }
    }
    fs::write(path, bytes).await.map_err(ServerError::Io)
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::{SerializableSha256State, new_upload_session_id};

    #[test]
    fn upload_session_ids_are_hex_and_not_reused_back_to_back() {
        let first = new_upload_session_id();
        let second = new_upload_session_id();

        assert_eq!(first.len(), 32);
        assert_eq!(second.len(), 32);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(second.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_ne!(first, second);
    }

    #[test]
    fn serializable_sha256_state_matches_reference_digest() {
        let mut state = SerializableSha256State::default();
        assert!(state.update(b"chunk-1").is_ok());
        assert!(state.update(&vec![b'x'; 1_000_000]).is_ok());
        assert!(state.update(b"chunk-3").is_ok());

        let mut reference = Sha256::new();
        reference.update(b"chunk-1");
        reference.update(vec![b'x'; 1_000_000]);
        reference.update(b"chunk-3");
        let expected = hex::encode(reference.finalize());

        assert!(matches!(
            state.finalize_hex().as_deref(),
            Ok(actual) if actual == expected
        ));
    }
}
