use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Weak},
};

use shardline_reliability::{
    DigestSnapshot, PersistedMerkleJournalRecord, ResumableSessionSnapshotDomain,
    SessionEvidenceLog, SnapshotEvidenceEvent, SnapshotEvidenceLog, StateTransitionEvent,
    append_or_baseline_snapshot_evidence, build_persisted_merkle_chain_with_previous,
    build_typed_merkle_chain, canonical_state_digest, resumable_session_snapshot_identity,
    verify_and_append_session_transition, verify_persisted_merkle_chain, verify_session_evidence,
    verify_snapshot_evidence,
};
#[cfg(unix)]
use shardline_storage::{
    AnchoredPathOptions, ensure_parent_path_matches_anchor, open_anchored_target,
    remove_if_present, sync_parent_directory, write_anchored_temporary_file,
};
use tokio::{sync::Mutex, task::spawn_blocking};

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
    #[serde(default)]
    pub(crate) snapshot_evidence: SnapshotEvidenceLog<DigestSnapshot>,
    #[serde(default)]
    pub(crate) journal_head: Option<u64>,
    #[serde(default)]
    pub(crate) journal_bytes: Option<u64>,
    #[serde(default)]
    pub(crate) journal_evidence_sequence: Option<u64>,
    #[serde(default)]
    pub(crate) journal_snapshot_sequence: Option<u64>,
    #[serde(default)]
    pub(crate) merkle_evidence_sequence: Option<u64>,
    #[serde(default)]
    pub(crate) merkle_snapshot_sequence: Option<u64>,
    #[serde(default)]
    pub(crate) merkle_evidence_commit: Option<serde_json::Value>,
    #[serde(default)]
    pub(crate) merkle_snapshot_commit: Option<serde_json::Value>,
}

const SESSION_EVIDENCE_JOURNAL: &str = "evidence.log";
type SessionPersistLockKey = (PathBuf, String);
type SessionPersistLockMap = std::sync::Mutex<HashMap<SessionPersistLockKey, Weak<Mutex<()>>>>;

static OCI_SESSION_PERSIST_LOCKS: LazyLock<SessionPersistLockMap> =
    LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

pub(crate) fn session_persist_lock(root: &Path, session_id: &str) -> Arc<Mutex<()>> {
    let mut locks = OCI_SESSION_PERSIST_LOCKS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let key = (root.to_path_buf(), session_id.to_owned());
    if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
        return lock;
    }
    locks.retain(|_, lock| lock.strong_count() > 0);
    let lock = Arc::new(Mutex::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    lock
}

fn session_snapshot(
    session_id: &str,
    session: &OciUploadSession,
) -> Result<DigestSnapshot, OciAdapterError> {
    let operation = resumable_session_snapshot_identity(
        ResumableSessionSnapshotDomain::OciUpload,
        session.scope_namespace.clone(),
        session_id,
        session.repository.clone(),
    )
    .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    let digest = canonical_state_digest(&session.reliability_snapshot_v1())
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    Ok(DigestSnapshot::new(operation, digest))
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

pub(crate) fn upload_evidence_journal_path(root: &Path, session_id: &str) -> PathBuf {
    upload_dir(root).join(format!("{session_id}.{SESSION_EVIDENCE_JOURNAL}"))
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
    let persist_lock = session_persist_lock(root, session_id);
    let _guard = persist_lock.lock().await;
    let (evidence, mut snapshot_evidence) =
        match read_persisted_upload_session_with_snapshot(root, session_id).await {
            Ok((_previous, evidence, snapshot_evidence)) => {
                let (evidence, _) = verify_and_append_session_transition(
                    evidence,
                    &session.scope_namespace,
                    session_id,
                    &session.repository,
                    shardline_reliability::ResumableLifecycleState::Active,
                    shardline_reliability::ResumableLifecycleState::Active,
                )
                .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
                (evidence, snapshot_evidence)
            }
            Err(OciAdapterError::NotFound) => (
                SessionEvidenceLog::new(&session.scope_namespace, session_id, &session.repository)
                    .map_err(|error| OciAdapterError::Reliability(error.to_string()))?,
                SnapshotEvidenceLog::default(),
            ),
            Err(error) => return Err(error),
        };
    evidence
        .verify_for(&session.scope_namespace, session_id, &session.repository)
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    let snapshot = session_snapshot(session_id, session)?;
    snapshot_evidence = append_or_baseline_snapshot_evidence(snapshot_evidence, snapshot)
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    persist_upload_session_with_evidence(root, session_id, session, &evidence, snapshot_evidence)
        .await
}

pub(crate) async fn persist_upload_session_with_evidence(
    root: &Path,
    session_id: &str,
    session: &OciUploadSession,
    evidence: &SessionEvidenceLog,
    snapshot_evidence: SnapshotEvidenceLog<DigestSnapshot>,
) -> Result<(), OciAdapterError> {
    evidence
        .verify_for(&session.scope_namespace, session_id, &session.repository)
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    let snapshot = session_snapshot(session_id, session)?;
    let (snapshot_evidence, _) =
        shardline_reliability::verify_or_repair_snapshot_evidence(snapshot_evidence, snapshot)
            .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    let metadata_path = upload_metadata_path(root, session_id);
    let existing = match read_upload_file_async(root, &metadata_path).await {
        Ok(bytes) => match serde_json::from_slice::<PersistedOciUploadSession>(&bytes) {
            Ok(persisted) => Some(persisted),
            Err(error) if reliability_envelope_field_present(&bytes) => {
                return Err(OciAdapterError::Json(error));
            }
            Err(error) => {
                serde_json::from_slice::<OciUploadSession>(&bytes)
                    .map_err(|_legacy_error| OciAdapterError::Json(error))?;
                None
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(OciAdapterError::Io(error)),
    };
    let mut journal_head = existing.as_ref().and_then(|value| value.journal_head);
    let mut journal_bytes = existing.as_ref().and_then(|value| value.journal_bytes);
    let mut last_evidence_sequence = existing
        .as_ref()
        .and_then(|value| value.journal_evidence_sequence);
    let mut last_snapshot_sequence = existing
        .as_ref()
        .and_then(|value| value.journal_snapshot_sequence);
    let mut last_merkle_evidence_sequence = existing
        .as_ref()
        .and_then(|value| value.merkle_evidence_sequence);
    let mut last_merkle_snapshot_sequence = existing
        .as_ref()
        .and_then(|value| value.merkle_snapshot_sequence);
    let mut previous_merkle_evidence = existing
        .as_ref()
        .and_then(|value| value.merkle_evidence_commit.clone());
    let mut previous_merkle_snapshot = existing
        .as_ref()
        .and_then(|value| value.merkle_snapshot_commit.clone());

    if let Some(head) = journal_head {
        let records = read_evidence_journal(root, session_id).await?;
        let head = usize::try_from(head)?;
        let committed = records.get(..head).ok_or_else(|| {
            OciAdapterError::Reliability("OCI evidence journal head is invalid".into())
        })?;
        if last_evidence_sequence.is_none() || last_snapshot_sequence.is_none() {
            last_evidence_sequence = committed
                .iter()
                .flat_map(|record| record.evidence.iter())
                .filter_map(|event| event.get("sequence").and_then(serde_json::Value::as_u64))
                .max();
            last_snapshot_sequence = committed
                .iter()
                .flat_map(|record| record.snapshot_evidence.iter())
                .filter_map(|event| event.get("sequence").and_then(serde_json::Value::as_u64))
                .max();
        }
        if last_merkle_evidence_sequence.is_none() || last_merkle_snapshot_sequence.is_none() {
            for record in committed {
                if let (Some(event), Some(commit)) =
                    (record.evidence.last(), record.merkle_commits.last())
                {
                    last_merkle_evidence_sequence =
                        event.get("sequence").and_then(serde_json::Value::as_u64);
                    previous_merkle_evidence = Some(commit.clone());
                }
                if let (Some(event), Some(commit)) = (
                    record.snapshot_evidence.last(),
                    record.snapshot_merkle_commits.last(),
                ) {
                    last_merkle_snapshot_sequence =
                        event.get("sequence").and_then(serde_json::Value::as_u64);
                    previous_merkle_snapshot = Some(commit.clone());
                }
            }
        }
    }

    let evidence_json = evidence
        .events()
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?;
    let snapshot_evidence_json = snapshot_evidence
        .events()
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?;
    let evidence_to_append = if journal_head.is_some() {
        evidence_json
            .iter()
            .filter(|event| {
                last_evidence_sequence.is_none_or(|sequence| {
                    event
                        .get("sequence")
                        .and_then(serde_json::Value::as_u64)
                        .is_some_and(|value| value > sequence)
                })
            })
            .cloned()
            .collect()
    } else {
        evidence_json.clone()
    };
    let snapshot_to_append = if journal_head.is_some() {
        snapshot_evidence_json
            .iter()
            .filter(|event| {
                last_snapshot_sequence.is_none_or(|sequence| {
                    event
                        .get("sequence")
                        .and_then(serde_json::Value::as_u64)
                        .is_some_and(|value| value > sequence)
                })
            })
            .cloned()
            .collect()
    } else {
        snapshot_evidence_json.clone()
    };
    let merkle_events = evidence_json
        .iter()
        .filter(|event| {
            last_merkle_evidence_sequence.is_none_or(|sequence| {
                event
                    .get("sequence")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|value| value > sequence)
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let snapshot_merkle_events = snapshot_evidence_json
        .iter()
        .filter(|event| {
            last_merkle_snapshot_sequence.is_none_or(|sequence| {
                event
                    .get("sequence")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|value| value > sequence)
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let merkle_commits = build_persisted_merkle_chain_with_previous(
        shardline_reliability::OperationKind::ResumableSession,
        &merkle_events,
        previous_merkle_evidence.as_ref(),
    )
    .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    let snapshot_merkle_commits =
        build_typed_merkle_chain::<SnapshotEvidenceEvent<DigestSnapshot>>(
            &snapshot_merkle_events,
            previous_merkle_snapshot.as_ref(),
        )
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    if !evidence_to_append.is_empty()
        || !snapshot_to_append.is_empty()
        || !merkle_commits.is_empty()
        || !snapshot_merkle_commits.is_empty()
    {
        let appended_bytes = append_evidence_journal(
            root,
            session_id,
            journal_head.unwrap_or(0),
            journal_bytes,
            &PersistedMerkleJournalRecord {
                evidence: evidence_to_append,
                merkle_commits: merkle_commits.clone(),
                snapshot_evidence: snapshot_to_append,
                snapshot_merkle_commits: snapshot_merkle_commits.clone(),
            },
        )
        .await?;
        journal_bytes = Some(appended_bytes);
        journal_head = Some(
            journal_head
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(OciAdapterError::Overflow)?,
        );
        last_evidence_sequence = evidence.events().last().map(|event| event.sequence);
        last_snapshot_sequence = snapshot_evidence
            .events()
            .last()
            .map(|event| event.sequence);
        last_merkle_evidence_sequence = merkle_commits
            .last()
            .and_then(|commit| commit.get("body"))
            .and_then(|body| body.get("sequence"))
            .and_then(serde_json::Value::as_u64)
            .or(last_merkle_evidence_sequence);
        last_merkle_snapshot_sequence = snapshot_merkle_commits
            .last()
            .and_then(|commit| commit.get("body"))
            .and_then(|body| body.get("sequence"))
            .and_then(serde_json::Value::as_u64)
            .or(last_merkle_snapshot_sequence);
        previous_merkle_evidence = merkle_commits.last().cloned().or(previous_merkle_evidence);
        previous_merkle_snapshot = snapshot_merkle_commits
            .last()
            .cloned()
            .or(previous_merkle_snapshot);
    }
    let bytes = serde_json::to_vec(&PersistedOciUploadSession {
        session: session.clone(),
        evidence: SessionEvidenceLog::default(),
        snapshot_evidence: SnapshotEvidenceLog::default(),
        journal_head,
        journal_bytes,
        journal_evidence_sequence: last_evidence_sequence,
        journal_snapshot_sequence: last_snapshot_sequence,
        merkle_evidence_sequence: last_merkle_evidence_sequence,
        merkle_snapshot_sequence: last_merkle_snapshot_sequence,
        merkle_evidence_commit: previous_merkle_evidence,
        merkle_snapshot_commit: previous_merkle_snapshot,
    })?;
    write_upload_metadata(root, session_id, bytes).await
}

pub(crate) async fn read_persisted_upload_session(
    root: &Path,
    session_id: &str,
) -> Result<(OciUploadSession, SessionEvidenceLog), OciAdapterError> {
    let (session, evidence, _) =
        read_persisted_upload_session_with_snapshot(root, session_id).await?;
    Ok((session, evidence))
}

async fn read_persisted_upload_session_with_snapshot(
    root: &Path,
    session_id: &str,
) -> Result<
    (
        OciUploadSession,
        SessionEvidenceLog,
        SnapshotEvidenceLog<DigestSnapshot>,
    ),
    OciAdapterError,
> {
    protocol_support::validate_upload_session_id(session_id)?;
    let metadata_path = upload_metadata_path(root, session_id);
    let bytes = read_upload_file_async(root, &metadata_path)
        .await
        .map_err(map_not_found)?;
    let (session, mut stored_evidence, mut stored_snapshot_evidence, journal_head) =
        match serde_json::from_slice::<PersistedOciUploadSession>(&bytes) {
            Ok(persisted) => (
                persisted.session,
                persisted.evidence,
                persisted.snapshot_evidence,
                persisted.journal_head,
            ),
            Err(wrapper_error) => {
                if reliability_envelope_field_present(&bytes) {
                    return Err(OciAdapterError::Json(wrapper_error));
                }
                let session = serde_json::from_slice::<OciUploadSession>(&bytes)
                    .map_err(|_legacy_error| OciAdapterError::Json(wrapper_error))?;
                (
                    session,
                    SessionEvidenceLog::default(),
                    SnapshotEvidenceLog::default(),
                    None,
                )
            }
        };
    if let Some(head) = journal_head {
        let records = read_evidence_journal(root, session_id).await?;
        let head = usize::try_from(head).map_err(|_conversion_error| OciAdapterError::Overflow)?;
        let committed = records.get(..head).ok_or_else(|| {
            OciAdapterError::Reliability("OCI evidence journal head is missing".into())
        })?;
        let evidence_events = committed
            .iter()
            .flat_map(|record| record.evidence.iter().cloned())
            .map(serde_json::from_value::<StateTransitionEvent>)
            .collect::<Result<Vec<_>, _>>()?;
        let snapshot_events = committed
            .iter()
            .flat_map(|record| record.snapshot_evidence.iter().cloned())
            .map(serde_json::from_value::<SnapshotEvidenceEvent<DigestSnapshot>>)
            .collect::<Result<Vec<_>, _>>()?;
        stored_evidence = SessionEvidenceLog::from_events(evidence_events)
            .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
        stored_snapshot_evidence = SnapshotEvidenceLog::from_events(snapshot_events)
            .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
        let merkle_events = committed
            .iter()
            .flat_map(|record| record.evidence.iter().cloned())
            .collect::<Vec<_>>();
        let merkle_commits = committed
            .iter()
            .flat_map(|record| record.merkle_commits.iter().cloned())
            .collect::<Vec<_>>();
        let snapshot_merkle_events = committed
            .iter()
            .flat_map(|record| record.snapshot_evidence.iter().cloned())
            .collect::<Vec<_>>();
        let snapshot_merkle_commits = committed
            .iter()
            .flat_map(|record| record.snapshot_merkle_commits.iter().cloned())
            .collect::<Vec<_>>();
        verify_persisted_merkle_chain(
            shardline_reliability::OperationKind::ResumableSession,
            &merkle_events,
            &merkle_commits,
        )
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
        shardline_reliability::verify_typed_merkle_chain::<SnapshotEvidenceEvent<DigestSnapshot>>(
            &snapshot_merkle_events,
            &snapshot_merkle_commits,
        )
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    }
    let evidence = if stored_evidence.is_empty() {
        SessionEvidenceLog::for_legacy_session(
            &session.scope_namespace,
            session_id,
            &session.repository,
        )
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?
    } else {
        verify_session_evidence(
            &stored_evidence,
            &session.scope_namespace,
            session_id,
            &session.repository,
        )
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
        stored_evidence
    };
    let snapshot = session_snapshot(session_id, &session)?;
    let snapshot_evidence = if stored_snapshot_evidence.events().is_empty() {
        SnapshotEvidenceLog::baseline(snapshot)
            .map_err(|error| OciAdapterError::Reliability(error.to_string()))?
    } else {
        verify_snapshot_evidence(&stored_snapshot_evidence, &snapshot)
            .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
        stored_snapshot_evidence
    };
    Ok((session, evidence, snapshot_evidence))
}

fn reliability_envelope_field_present(bytes: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return false;
    };
    let Some(object) = value.as_object() else {
        return false;
    };
    ["evidence", "snapshot_evidence"]
        .into_iter()
        .any(|field| object.contains_key(field))
}

pub(crate) async fn read_evidence_journal(
    root: &Path,
    session_id: &str,
) -> Result<Vec<PersistedMerkleJournalRecord>, OciAdapterError> {
    let path = upload_evidence_journal_path(root, session_id);
    let bytes = match read_upload_file_async(root, &path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(OciAdapterError::Io(error)),
    };
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            serde_json::from_slice(line)
                .map_err(|error| OciAdapterError::Reliability(error.to_string()))
        })
        .collect()
}

async fn append_evidence_journal(
    root: &Path,
    session_id: &str,
    committed_head: u64,
    committed_bytes: Option<u64>,
    record: &PersistedMerkleJournalRecord,
) -> Result<u64, OciAdapterError> {
    let path = upload_evidence_journal_path(root, session_id);
    let mut bytes = serde_json::to_vec(record)?;
    bytes.push(b'\n');
    let root = root.to_path_buf();
    spawn_blocking(move || {
        append_journal_file(&root, &path, committed_head, committed_bytes, &bytes)
    })
    .await
    .map_err(OciAdapterError::BlockingTask)?
    .map_err(OciAdapterError::Io)
}

#[cfg(unix)]
fn append_journal_file(
    root: &Path,
    path: &Path,
    committed_head: u64,
    committed_bytes: Option<u64>,
    bytes: &[u8],
) -> std::io::Result<u64> {
    let anchored = open_anchored_target(
        root,
        path,
        AnchoredPathOptions::new(Some(0o750), Some(0o600)),
        || std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root"),
    )?;
    let old_len = prepare_journal_append(&anchored.final_path(), committed_head, committed_bytes)?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(anchored.final_path())?;
    file.write_all(bytes)?;
    file.sync_all()?;
    ensure_parent_path_matches_anchor(&anchored, "journal parent changed during append")?;
    old_len
        .checked_add(u64::try_from(bytes.len()).map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("journal byte length overflow: {error}"),
            )
        })?)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "journal byte length overflow",
            )
        })
}

#[cfg(not(unix))]
fn append_journal_file(
    _root: &Path,
    path: &Path,
    committed_head: u64,
    committed_bytes: Option<u64>,
    bytes: &[u8],
) -> std::io::Result<u64> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let old_len = prepare_journal_append(path, committed_head, committed_bytes)?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    old_len
        .checked_add(u64::try_from(bytes.len()).map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("journal byte length overflow: {error}"),
            )
        })?)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "journal byte length overflow",
            )
        })
}

fn prepare_journal_append(
    path: &Path,
    committed_head: u64,
    expected_bytes: Option<u64>,
) -> std::io::Result<u64> {
    if let Some(expected_bytes) = expected_bytes {
        let actual_bytes = match std::fs::metadata(path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if committed_head == 0 && expected_bytes == 0 {
                    return Ok(0);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "journal is missing its committed log",
                ));
            }
            Err(error) => return Err(error),
        };
        if actual_bytes < expected_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "journal is shorter than its committed byte length",
            ));
        }
        if actual_bytes > expected_bytes {
            let file = OpenOptions::new().write(true).open(path)?;
            file.set_len(expected_bytes)?;
            file.sync_all()?;
        }
        return Ok(expected_bytes);
    }
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if committed_head == 0 {
                return Ok(0);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "journal is missing its committed log",
            ));
        }
        Err(error) => return Err(error),
    };
    let mut records = 0_u64;
    let mut committed_bytes = 0_usize;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        if !line.is_empty() && line.iter().any(|byte| !byte.is_ascii_whitespace()) {
            records = records.checked_add(1).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "journal record count overflow",
                )
            })?;
            if records <= committed_head {
                committed_bytes = committed_bytes.checked_add(line.len()).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "journal byte count overflow",
                    )
                })?;
            }
        }
    }
    if records < committed_head {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "journal head exceeds its log",
        ));
    }
    if records > committed_head {
        let mut file = OpenOptions::new().write(true).truncate(true).open(path)?;
        let committed_prefix = bytes.get(..committed_bytes).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "committed journal byte length exceeds journal",
            )
        })?;
        file.write_all(committed_prefix)?;
        file.sync_all()?;
    }
    u64::try_from(committed_bytes).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("journal byte length overflow: {error}"),
        )
    })
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
