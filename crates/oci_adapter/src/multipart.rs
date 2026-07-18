use std::path::Path;

use bytes::Bytes;
use shardline_storage::{ObjectKey, PutOutcome};
use tokio::fs;

use crate::OciAdapterError;
use crate::fs::{persist_upload_session, unix_now_seconds_checked, upload_tail_path};
use crate::key::validate_repository;
use crate::protocol_support::{
    object_key, shared_sha256_object_key, stable_hex_id, validate_upload_session_id,
};
use crate::traits::OciBackend;
use crate::types::{
    OCI_S3_MULTIPART_CHUNK_BYTES, OciS3MultipartUploadSession, OciUploadSession,
    SerializableSha256State,
};

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
        persist_upload_session(root, session_id, &session).await?;
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
    persist_upload_session(root, session_id, &session).await?;
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
    let canonical_key = shared_sha256_object_key(digest_hex)?;
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

// ── Internal helpers ─────────────────────────────────────────────────────────

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
    persist_upload_session(root, session_id, session).await?;

    let Some(upload_id) = backend
        .create_resumable_object_upload(&temporary_object_key)
        .await?
    else {
        session.s3_multipart = None;
        // Overwrite the placeholder so on-disk state matches in-memory.
        // A subsequent read will attempt S3 upload creation again.
        let _result = persist_upload_session(root, session_id, session).await;
        return Err(OciAdapterError::NotFound);
    };

    session.s3_multipart = Some(OciS3MultipartUploadSession {
        temporary_object_key: temporary_object_key.as_str().to_owned(),
        upload_id,
        uploaded_part_ids: Vec::new(),
        total_length: 0,
        sha256_state: SerializableSha256State::default(),
    });
    persist_upload_session(root, session_id, session).await
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

pub(crate) async fn read_upload_tail(
    root: &Path,
    session_id: &str,
) -> Result<Vec<u8>, OciAdapterError> {
    validate_upload_session_id(session_id)?;
    match fs::read(upload_tail_path(root, session_id)).await {
        Ok(bytes) => Ok(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(OciAdapterError::Io(error)),
    }
}

pub(crate) async fn write_upload_tail(
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
