use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use shardline_reliability::SessionEvidenceLog;
#[cfg(unix)]
use shardline_storage::{
    AnchoredPathOptions, ensure_parent_path_matches_anchor, open_anchored_target,
    remove_if_present, sync_parent_directory, write_anchored_temporary_file,
};
use tokio::task::spawn_blocking;

use crate::{
    OciAdapterError, protocol_support,
    types::{OCI_UPLOAD_DIR, OciFileLock, OciUploadSession},
};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct PersistedOciUploadSession {
    #[serde(flatten)]
    pub(crate) session: OciUploadSession,
    #[serde(default)]
    pub(crate) evidence: SessionEvidenceLog,
}

// ── Path helpers ──────────────────────────────────────────────────────────────

pub(crate) fn upload_dir(root: &Path) -> PathBuf {
    root.join(OCI_UPLOAD_DIR)
}

pub(crate) fn upload_session_lock_path(root: &Path) -> PathBuf {
    upload_dir(root).join(".sessions.lock")
}

pub(crate) fn upload_metadata_path(root: &Path, session_id: &str) -> PathBuf {
    upload_dir(root).join(format!("{session_id}.json"))
}

pub(crate) fn upload_body_path(root: &Path, session_id: &str) -> PathBuf {
    upload_dir(root).join(format!("{session_id}.bin"))
}

pub(crate) fn upload_tail_path(root: &Path, session_id: &str) -> PathBuf {
    upload_dir(root).join(format!("{session_id}.tail"))
}

// ── File locking ─────────────────────────────────────────────────────────────

pub(crate) async fn acquire_upload_session_file_lock(
    path: PathBuf,
) -> Result<OciFileLock, OciAdapterError> {
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

// ── Metadata persistence ─────────────────────────────────────────────────────

pub(crate) async fn write_upload_metadata(
    root: &Path,
    session_id: &str,
    bytes: Vec<u8>,
) -> Result<(), OciAdapterError> {
    protocol_support::validate_upload_session_id(session_id)?;
    let root = root.to_path_buf();
    let path = upload_metadata_path(&root, session_id);
    spawn_blocking(move || write_file_atomically(&root, &path, &bytes))
        .await
        .map_err(OciAdapterError::BlockingTask)?
        .map_err(OciAdapterError::Io)
}

pub(crate) async fn persist_upload_session(
    root: &Path,
    session_id: &str,
    session: &OciUploadSession,
) -> Result<(), OciAdapterError> {
    let evidence = match read_persisted_upload_session(root, session_id).await {
        Ok((_previous, evidence)) => {
            let mut evidence = evidence;
            evidence
                .record(
                    &session.scope_namespace,
                    session_id,
                    &session.repository,
                    shardline_reliability::ResumableLifecycleState::Active,
                    shardline_reliability::ResumableLifecycleState::Active,
                )
                .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
            evidence
        }
        Err(OciAdapterError::NotFound) => {
            SessionEvidenceLog::new(&session.scope_namespace, session_id, &session.repository)
                .map_err(|error| OciAdapterError::Reliability(error.to_string()))?
        }
        Err(error) => return Err(error),
    };
    evidence
        .verify_for(&session.scope_namespace, session_id, &session.repository)
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    let bytes = serde_json::to_vec(&PersistedOciUploadSession {
        session: session.clone(),
        evidence,
    })?;
    write_upload_metadata(root, session_id, bytes).await
}

pub(crate) async fn read_persisted_upload_session(
    root: &Path,
    session_id: &str,
) -> Result<(OciUploadSession, SessionEvidenceLog), OciAdapterError> {
    protocol_support::validate_upload_session_id(session_id)?;
    let metadata_path = upload_metadata_path(root, session_id);
    let bytes = read_upload_file_async(root, &metadata_path)
        .await
        .map_err(map_not_found)?;
    let (session, stored_evidence) =
        match serde_json::from_slice::<PersistedOciUploadSession>(&bytes) {
            Ok(persisted) => (persisted.session, persisted.evidence),
            Err(wrapper_error) => {
                let session = serde_json::from_slice::<OciUploadSession>(&bytes)
                    .map_err(|_legacy_error| OciAdapterError::Json(wrapper_error))?;
                (session, SessionEvidenceLog::default())
            }
        };
    let evidence_was_missing = stored_evidence.is_empty();
    let evidence = if evidence_was_missing {
        SessionEvidenceLog::for_legacy_session(
            &session.scope_namespace,
            session_id,
            &session.repository,
        )
    } else {
        stored_evidence
            .verify_for(&session.scope_namespace, session_id, &session.repository)
            .map(|()| stored_evidence)
    }
    .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    if evidence_was_missing {
        let repaired_bytes = serde_json::to_vec(&PersistedOciUploadSession {
            session: session.clone(),
            evidence: evidence.clone(),
        })
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
        write_upload_metadata(root, session_id, repaired_bytes).await?;
    }
    Ok((session, evidence))
}

// ── Error mapping ────────────────────────────────────────────────────────────

pub(crate) fn map_not_found(error: std::io::Error) -> OciAdapterError {
    if error.kind() == std::io::ErrorKind::NotFound {
        OciAdapterError::NotFound
    } else {
        OciAdapterError::Io(error)
    }
}

// ── Time helpers ─────────────────────────────────────────────────────────────

pub(crate) fn unix_now_seconds_checked() -> Result<u64, OciAdapterError> {
    shardline_server_core::unix_now_seconds_checked().map_err(|_e| OciAdapterError::Overflow)
}

// ── Anchored (symlink-resistant) file I/O primitives (Unix) ──────────────────

/// Opens a file under `root` using fd-relative paths that cannot follow symlinks.
///
/// Returns the opened file. The caller must not use the returned path outside of
/// `/proc/self/fd/` — see [`AnchoredTarget::final_path`].
#[cfg(unix)]
pub(crate) fn open_anchored_file(root: &Path, path: &Path) -> std::io::Result<File> {
    let anchored = open_anchored_target(root, path, AnchoredPathOptions::new(None, None), || {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root")
    })?;
    let file = OpenOptions::new().read(true).open(anchored.final_path())?;
    Ok(file)
}

/// Reads a file under `root` using anchored (symlink-resistant) path resolution.
#[cfg(unix)]
pub(crate) fn read_file_anchored(root: &Path, path: &Path) -> std::io::Result<Vec<u8>> {
    let anchored = open_anchored_target(root, path, AnchoredPathOptions::new(None, None), || {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root")
    })?;
    std::fs::read(anchored.final_path())
}

/// Deletes a file under `root` using anchored (symlink-resistant) path resolution.
///
/// After deletion, verifies that the parent directory has not been replaced
/// (catches TOCTOU rename+swap attacks).
#[cfg(unix)]
pub(crate) fn delete_file_anchored(root: &Path, path: &Path) -> std::io::Result<()> {
    let anchored = open_anchored_target(root, path, AnchoredPathOptions::new(None, None), || {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root")
    })?;
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
pub(crate) fn append_file_anchored(root: &Path, path: &Path, bytes: &[u8]) -> std::io::Result<u64> {
    let anchored = open_anchored_target(root, path, AnchoredPathOptions::new(None, None), || {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root")
    })?;
    let mut file = OpenOptions::new()
        .append(true)
        .open(anchored.final_path())?;
    file.write_all(bytes)?;
    let metadata = file.metadata()?;
    Ok(metadata.len())
}

/// Opens a file under `root` for append using anchored (symlink-resistant) path resolution.
///
/// Reads a file under the OCI upload root using anchored (symlink-resistant) I/O.
#[cfg(unix)]
pub(crate) async fn read_upload_file_async(root: &Path, path: &Path) -> std::io::Result<Vec<u8>> {
    let root = root.to_path_buf();
    let path = path.to_path_buf();
    spawn_blocking(move || read_file_anchored(&root, &path))
        .await
        .map_err(std::io::Error::other)?
}

/// Returns the file length for a file under the OCI upload root using anchored I/O.
#[cfg(unix)]
pub(crate) async fn upload_file_len_async(root: &Path, path: &Path) -> std::io::Result<u64> {
    let root = root.to_path_buf();
    let path = path.to_path_buf();
    spawn_blocking(move || {
        let anchored =
            open_anchored_target(&root, &path, AnchoredPathOptions::new(None, None), || {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root")
            })?;
        let file = File::open(anchored.final_path())?;
        let metadata = file.metadata()?;
        Ok(metadata.len())
    })
    .await
    .map_err(std::io::Error::other)?
}

/// Checks if a file under the OCI upload root exists using anchored I/O.
#[cfg(unix)]
pub(crate) async fn upload_file_exists_async(root: &Path, path: &Path) -> std::io::Result<()> {
    let root = root.to_path_buf();
    let path = path.to_path_buf();
    spawn_blocking(move || {
        let anchored =
            open_anchored_target(&root, &path, AnchoredPathOptions::new(None, None), || {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root")
            })?;
        match File::open(anchored.final_path()) {
            Ok(_file) => Ok(()),
            Err(error) => Err(error),
        }
    })
    .await
    .map_err(std::io::Error::other)?
}

// ── Non-Unix async wrappers ──────────────────────────────────────────────────

#[cfg(not(unix))]
pub(crate) async fn read_upload_file_async(root: &Path, path: &Path) -> std::io::Result<Vec<u8>> {
    let _ = root;
    tokio::fs::read(path).await
}

#[cfg(not(unix))]
pub(crate) async fn upload_file_len_async(root: &Path, path: &Path) -> std::io::Result<u64> {
    let _ = root;
    tokio::fs::metadata(path).await.map(|m| m.len())
}

#[cfg(not(unix))]
pub(crate) async fn upload_file_exists_async(root: &Path, path: &Path) -> std::io::Result<()> {
    let _ = root;
    tokio::fs::metadata(path).await.map(|_| ())
}

// ── Atomic file writes ───────────────────────────────────────────────────────

#[cfg(unix)]
pub(crate) fn write_file_atomically(root: &Path, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
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
    sync_parent_directory(&anchored)?;
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn write_file_atomically(root: &Path, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
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
