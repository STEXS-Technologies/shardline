use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Weak},
};

use serde::Deserialize;
use shardline_reliability::{
    DigestSnapshot, PersistedMerkleJournalRecord, ResumableSessionSnapshotDomain,
    SessionEvidenceLog, SnapshotEvidenceEvent, SnapshotEvidenceLog, StateTransitionEvent,
    append_or_baseline_snapshot_evidence, build_persisted_merkle_chain_with_previous,
    build_typed_merkle_chain, canonical_state_digest, events_after_sequence,
    resumable_session_snapshot_identity, verify_and_append_session_transition,
    verify_persisted_merkle_commit_with_previous,
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
    #[serde(default)]
    pub(crate) merkle_evidence_previous_commit: Option<serde_json::Value>,
    #[serde(default)]
    pub(crate) merkle_snapshot_previous_commit: Option<serde_json::Value>,
    #[serde(default)]
    pub(crate) journal_evidence_head: Option<StateTransitionEvent>,
    #[serde(default)]
    pub(crate) journal_snapshot_head: Option<SnapshotEvidenceEvent<DigestSnapshot>>,
    #[serde(default)]
    pub(crate) reliability_head_metadata: bool,
}

/// Session state together with its verified lifecycle and snapshot evidence.
type VerifiedUploadSession = (
    OciUploadSession,
    SessionEvidenceLog,
    SnapshotEvidenceLog<DigestSnapshot>,
);

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
    let file = spawn_blocking(move || {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        Ok::<_, OciAdapterError>(file)
    })
    .await
    .map_err(OciAdapterError::BlockingTask)??;
    wait_for_upload_session_file_lock(file).await
}

// OS lock waiting must remain cancellable; opening the file is the only
// operation admitted to the runtime's blocking pool.
async fn wait_for_upload_session_file_lock(
    file: std::fs::File,
) -> Result<OciFileLock, OciAdapterError> {
    let mut delay = std::time::Duration::from_millis(10);
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(OciFileLock { file }),
            Err(std::fs::TryLockError::WouldBlock) => {}
            Err(std::fs::TryLockError::Error(error))
                if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(std::fs::TryLockError::Error(error)) => return Err(OciAdapterError::Io(error)),
        }
        tokio::time::sleep(delay).await;
        delay = delay
            .saturating_mul(2)
            .min(std::time::Duration::from_millis(50));
    }
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
    let mut existing = match read_upload_file_async(root, &metadata_path).await {
        Ok(bytes) => Some(serde_json::from_slice::<PersistedOciUploadSession>(&bytes)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(OciAdapterError::Io(error)),
    };
    let mut journal_head = existing.as_ref().and_then(|value| value.journal_head);
    let mut journal_bytes = existing.as_ref().and_then(|value| value.journal_bytes);
    let mut last_evidence_sequence = existing.as_ref().and_then(|value| {
        value.journal_evidence_sequence.or_else(|| {
            value
                .journal_evidence_head
                .as_ref()
                .map(|event| event.sequence)
        })
    });
    let mut last_snapshot_sequence = existing.as_ref().and_then(|value| {
        value.journal_snapshot_sequence.or_else(|| {
            value
                .journal_snapshot_head
                .as_ref()
                .map(|event| event.sequence)
        })
    });
    let mut last_merkle_evidence_sequence = existing
        .as_ref()
        .and_then(|value| value.merkle_evidence_sequence);
    let mut last_merkle_snapshot_sequence = existing
        .as_ref()
        .and_then(|value| value.merkle_snapshot_sequence);
    let mut previous_merkle_evidence = existing
        .as_mut()
        .and_then(|value| value.merkle_evidence_commit.take());
    let mut previous_merkle_snapshot = existing
        .as_mut()
        .and_then(|value| value.merkle_snapshot_commit.take());
    let mut previous_merkle_evidence_previous = existing
        .as_mut()
        .and_then(|value| value.merkle_evidence_previous_commit.take());
    let mut previous_merkle_snapshot_previous = existing
        .as_mut()
        .and_then(|value| value.merkle_snapshot_previous_commit.take());

    if let Some(head) = journal_head
        && (last_evidence_sequence.is_none()
            || last_snapshot_sequence.is_none()
            || last_merkle_evidence_sequence.is_none()
            || last_merkle_snapshot_sequence.is_none()
            || previous_merkle_evidence.is_none()
            || previous_merkle_snapshot.is_none())
    {
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
                    previous_merkle_evidence_previous = previous_merkle_evidence.take();
                    previous_merkle_evidence = Some(commit.clone());
                }
                if let (Some(event), Some(commit)) = (
                    record.snapshot_evidence.last(),
                    record.snapshot_merkle_commits.last(),
                ) {
                    last_merkle_snapshot_sequence =
                        event.get("sequence").and_then(serde_json::Value::as_u64);
                    previous_merkle_snapshot_previous = previous_merkle_snapshot.take();
                    previous_merkle_snapshot = Some(commit.clone());
                }
            }
        }
    }

    let evidence_to_append =
        events_after_sequence(evidence.events(), journal_head.and(last_evidence_sequence))
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;
    let snapshot_to_append = events_after_sequence(
        snapshot_evidence.events(),
        journal_head.and(last_snapshot_sequence),
    )
    .iter()
    .map(serde_json::to_value)
    .collect::<Result<Vec<_>, _>>()?;
    let merkle_events = events_after_sequence(evidence.events(), last_merkle_evidence_sequence)
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?;
    let snapshot_merkle_events =
        events_after_sequence(snapshot_evidence.events(), last_merkle_snapshot_sequence)
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;
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
        let mut record = PersistedMerkleJournalRecord {
            evidence: evidence_to_append,
            merkle_commits,
            snapshot_evidence: snapshot_to_append,
            snapshot_merkle_commits,
        };
        let appended_bytes = append_evidence_journal(
            root,
            session_id,
            journal_head.unwrap_or(0),
            journal_bytes,
            &record,
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
        last_merkle_evidence_sequence = record
            .merkle_commits
            .last()
            .and_then(|commit| commit.get("body"))
            .and_then(|body| body.get("sequence"))
            .and_then(serde_json::Value::as_u64)
            .or(last_merkle_evidence_sequence);
        last_merkle_snapshot_sequence = record
            .snapshot_merkle_commits
            .last()
            .and_then(|commit| commit.get("body"))
            .and_then(|body| body.get("sequence"))
            .and_then(serde_json::Value::as_u64)
            .or(last_merkle_snapshot_sequence);
        if let Some(last) = record.merkle_commits.pop() {
            previous_merkle_evidence_previous =
                record.merkle_commits.pop().or(previous_merkle_evidence);
            previous_merkle_evidence = Some(last);
        }
        if let Some(last) = record.snapshot_merkle_commits.pop() {
            previous_merkle_snapshot_previous = record
                .snapshot_merkle_commits
                .pop()
                .or(previous_merkle_snapshot);
            previous_merkle_snapshot = Some(last);
        }
    }
    let journal_evidence_head = evidence.events().last().cloned().or_else(|| {
        existing
            .as_mut()
            .and_then(|value| value.journal_evidence_head.take())
    });
    let journal_snapshot_head = snapshot_evidence.events().last().cloned().or_else(|| {
        existing
            .as_mut()
            .and_then(|value| value.journal_snapshot_head.take())
    });
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
        merkle_evidence_previous_commit: previous_merkle_evidence_previous,
        merkle_snapshot_previous_commit: previous_merkle_snapshot_previous,
        journal_evidence_head,
        journal_snapshot_head,
        reliability_head_metadata: true,
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
) -> Result<VerifiedUploadSession, OciAdapterError> {
    if let Some(head) = read_persisted_upload_session_head(root, session_id).await? {
        return Ok(head);
    }
    read_persisted_upload_session_with_snapshot_full(root, session_id).await
}

async fn read_persisted_upload_session_head(
    root: &Path,
    session_id: &str,
) -> Result<Option<VerifiedUploadSession>, OciAdapterError> {
    protocol_support::validate_upload_session_id(session_id)?;
    let metadata_path = upload_metadata_path(root, session_id);
    let bytes = match read_upload_file_async(root, &metadata_path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(map_not_found(error)),
    };
    let persisted = serde_json::from_slice::<PersistedOciUploadSession>(&bytes)?;
    if !persisted.reliability_head_metadata
        || persisted.journal_evidence_head.is_none()
        || persisted.journal_snapshot_head.is_none()
        || persisted.journal_bytes.is_none()
        || persisted.merkle_evidence_commit.is_none()
        || persisted.merkle_snapshot_commit.is_none()
        || persisted.merkle_evidence_sequence.is_none()
        || persisted.merkle_snapshot_sequence.is_none()
    {
        return Ok(None);
    }
    let evidence_head = persisted
        .journal_evidence_head
        .clone()
        .ok_or_else(|| OciAdapterError::Reliability("OCI evidence head is missing".into()))?;
    let snapshot_head = persisted
        .journal_snapshot_head
        .clone()
        .ok_or_else(|| OciAdapterError::Reliability("OCI snapshot head is missing".into()))?;
    if persisted.journal_evidence_sequence != Some(evidence_head.sequence)
        || persisted.journal_snapshot_sequence != Some(snapshot_head.sequence)
        || persisted.merkle_evidence_sequence != Some(evidence_head.sequence)
        || persisted.merkle_snapshot_sequence != Some(snapshot_head.sequence)
    {
        return Err(OciAdapterError::Reliability(
            "OCI evidence head sequence metadata is inconsistent".into(),
        ));
    }
    let journal_bytes = persisted
        .journal_bytes
        .ok_or_else(|| OciAdapterError::Reliability("OCI journal byte head is missing".into()))?;
    let journal_path = upload_evidence_journal_path(root, session_id);
    let metadata = tokio::fs::metadata(&journal_path).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            OciAdapterError::Reliability("OCI evidence journal is missing".into())
        } else {
            OciAdapterError::Io(error)
        }
    })?;
    if metadata.len() < journal_bytes {
        return Err(OciAdapterError::Reliability(
            "OCI evidence journal is shorter than its committed byte length".into(),
        ));
    }
    let evidence = SessionEvidenceLog::from_head(evidence_head)
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    evidence
        .verify_for(
            &persisted.session.scope_namespace,
            session_id,
            &persisted.session.repository,
        )
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    let snapshot = session_snapshot(session_id, &persisted.session)?;
    let snapshot_evidence = SnapshotEvidenceLog::from_head(snapshot_head)
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    snapshot_evidence
        .verify_for(&snapshot)
        .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    verify_oci_merkle_heads(&evidence, &snapshot_evidence, &persisted)?;
    Ok(Some((persisted.session, evidence, snapshot_evidence)))
}

fn verify_oci_merkle_heads(
    evidence: &SessionEvidenceLog,
    snapshots: &SnapshotEvidenceLog<DigestSnapshot>,
    persisted: &PersistedOciUploadSession,
) -> Result<(), OciAdapterError> {
    let evidence_event = serde_json::to_value(
        evidence
            .events()
            .last()
            .ok_or_else(|| OciAdapterError::Reliability("OCI evidence head is empty".into()))?,
    )?;
    verify_persisted_merkle_commit_with_previous(
        shardline_reliability::OperationKind::ResumableSession,
        evidence_event,
        persisted.merkle_evidence_commit.clone(),
        persisted.merkle_evidence_previous_commit.clone(),
    )
    .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    let snapshot_event = serde_json::to_value(
        snapshots
            .events()
            .last()
            .ok_or_else(|| OciAdapterError::Reliability("OCI snapshot head is empty".into()))?,
    )?;
    let expected = build_typed_merkle_chain::<SnapshotEvidenceEvent<DigestSnapshot>>(
        &[snapshot_event],
        persisted.merkle_snapshot_previous_commit.as_ref(),
    )
    .map_err(|error| OciAdapterError::Reliability(error.to_string()))?
    .into_iter()
    .next()
    .ok_or_else(|| OciAdapterError::Reliability("OCI snapshot commitment is missing".into()))?;
    let observed = persisted
        .merkle_snapshot_commit
        .as_ref()
        .ok_or_else(|| OciAdapterError::Reliability("OCI snapshot commitment is missing".into()))?;
    let observed = shardline_reliability::ReliabilityMerkleCommit::deserialize(observed)?;
    let expected =
        serde_json::from_value::<shardline_reliability::ReliabilityMerkleCommit>(expected)?;
    if !observed.matches_expected(&expected) {
        return Err(OciAdapterError::Reliability(
            "OCI snapshot Merkle commitment mismatch".into(),
        ));
    }
    Ok(())
}

async fn read_persisted_upload_session_with_snapshot_full(
    root: &Path,
    session_id: &str,
) -> Result<VerifiedUploadSession, OciAdapterError> {
    protocol_support::validate_upload_session_id(session_id)?;
    let metadata_path = upload_metadata_path(root, session_id);
    let bytes = read_upload_file_async(root, &metadata_path)
        .await
        .map_err(map_not_found)?;
    let PersistedOciUploadSession {
        session,
        evidence: mut stored_evidence,
        snapshot_evidence: mut stored_snapshot_evidence,
        journal_head,
        ..
    } = serde_json::from_slice(&bytes)?;
    if let Some(head) = journal_head {
        let records = read_evidence_journal(root, session_id).await?;
        let head = usize::try_from(head).map_err(|_conversion_error| OciAdapterError::Overflow)?;
        let committed = records.get(..head).ok_or_else(|| {
            OciAdapterError::Reliability("OCI evidence journal head is missing".into())
        })?;
        let evidence_events = committed
            .iter()
            .flat_map(|record| record.evidence.iter())
            .map(StateTransitionEvent::deserialize)
            .collect::<Result<Vec<_>, _>>()?;
        let snapshot_events = committed
            .iter()
            .flat_map(|record| record.snapshot_evidence.iter())
            .map(SnapshotEvidenceEvent::<DigestSnapshot>::deserialize)
            .collect::<Result<Vec<_>, _>>()?;
        stored_evidence = SessionEvidenceLog::from_events(evidence_events)
            .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
        stored_snapshot_evidence = SnapshotEvidenceLog::from_events(snapshot_events)
            .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
        let journal = PersistedMerkleJournalRecord::from_records(committed);
        journal
            .verify_chains::<SnapshotEvidenceEvent<DigestSnapshot>>(
                shardline_reliability::OperationKind::ResumableSession,
            )
            .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    }
    let (evidence, _) = shardline_reliability::verify_or_repair_session_evidence(
        stored_evidence,
        &session.scope_namespace,
        session_id,
        &session.repository,
    )
    .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    let snapshot = session_snapshot(session_id, &session)?;
    let (snapshot_evidence, _) = shardline_reliability::verify_or_repair_snapshot_evidence(
        stored_snapshot_evidence,
        snapshot,
    )
    .map_err(|error| OciAdapterError::Reliability(error.to_string()))?;
    Ok((session, evidence, snapshot_evidence))
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

fn append_journal_file(
    root: &Path,
    path: &Path,
    committed_head: u64,
    committed_bytes: Option<u64>,
    bytes: &[u8],
) -> std::io::Result<u64> {
    #[cfg(unix)]
    let anchored = open_anchored_target(
        root,
        path,
        AnchoredPathOptions::new(Some(0o750), Some(0o600)),
        || std::io::Error::new(std::io::ErrorKind::InvalidInput, "path escapes root"),
    )?;
    #[cfg(unix)]
    let journal_path: &Path = &anchored.final_path();
    #[cfg(not(unix))]
    {
        let _ = root;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
    }
    #[cfg(not(unix))]
    let journal_path = path;
    let old_len = prepare_journal_append(journal_path, committed_head, committed_bytes)?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(journal_path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    #[cfg(unix)]
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
        }
        if records <= committed_head {
            committed_bytes = committed_bytes.checked_add(line.len()).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "journal byte count overflow",
                )
            })?;
        }
    }
    if records < committed_head {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "journal head exceeds its log",
        ));
    }
    let committed_length = u64::try_from(committed_bytes).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("journal byte length overflow: {error}"),
        )
    })?;
    if records > committed_head {
        let file = OpenOptions::new().write(true).open(path)?;
        let _committed_prefix = bytes.get(..committed_bytes).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "committed journal byte length exceeds journal",
            )
        })?;
        file.set_len(committed_length)?;
        file.sync_all()?;
    }
    Ok(committed_length)
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
    std::fs::remove_file(&final_path)?;
    ensure_parent_path_matches_anchor(
        &anchored,
        "upload directory path changed during anchored delete",
    )
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
        open_anchored_file(&root, &path)?
            .metadata()
            .map(|metadata| metadata.len())
    })
    .await
    .map_err(std::io::Error::other)?
}

/// Checks if a file under the OCI upload root exists using anchored I/O.
#[cfg(unix)]
pub(crate) async fn upload_file_exists_async(root: &Path, path: &Path) -> std::io::Result<()> {
    let root = root.to_path_buf();
    let path = path.to_path_buf();
    spawn_blocking(move || open_anchored_file(&root, &path).map(|_file| ()))
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

#[cfg(any(not(unix), test))]
struct PortableTemporaryFile {
    path: std::path::PathBuf,
    published: bool,
}

#[cfg(any(not(unix), test))]
impl Drop for PortableTemporaryFile {
    fn drop(&mut self) {
        if !self.published {
            // Cleanup must not replace the original write, flush or rename error.
            let _ignored = std::fs::remove_file(&self.path);
        }
    }
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
    let mut temporary = write_temporary_file(path, bytes)?;
    std::fs::rename(&temporary.path, path)?;
    temporary.published = true;
    Ok(())
}

#[cfg(not(unix))]
fn write_temporary_file(path: &Path, bytes: &[u8]) -> std::io::Result<PortableTemporaryFile> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "path must have a parent directory",
        )
    })?;
    let file_name = path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "path must have a file name",
        )
    })?;
    loop {
        let temporary = parent.join(shardline_storage::temporary_file_name(file_name));
        let opened_file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        };
        let temporary = PortableTemporaryFile {
            path: temporary,
            published: false,
        };
        // Close the writer before guard cleanup on platforms that cannot unlink
        // an open file, including early write/flush errors.
        let mut file = opened_file;
        file.write_all(bytes)?;
        file.flush()?;
        return Ok(temporary);
    }
}

#[cfg(test)]
mod session_lock_cancellation_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::time::Duration;

    #[test]
    fn cancelled_session_waiters_leave_blocking_work_and_shutdown_available() {
        let storage = tempfile::TempDir::new().unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let owner = runtime
            .block_on(crate::lock_upload_sessions(storage.path()))
            .unwrap();
        let waiters: Vec<_> = (0..2)
            .map(|_| {
                let root = storage.path().to_path_buf();
                runtime.spawn(async move {
                    let _guard = crate::lock_upload_sessions(&root).await.unwrap();
                })
            })
            .collect();
        runtime.block_on(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        runtime.block_on(async {
            for waiter in waiters {
                waiter.abort();
                assert!(waiter.await.unwrap_err().is_cancelled());
            }
        });
        let unrelated_work_progressed = runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_millis(250),
                tokio::task::spawn_blocking(|| 17),
            )
            .await
            .is_ok()
        });
        let (shutdown_done, shutdown_received) = std::sync::mpsc::channel();
        let shutdown = std::thread::spawn(move || {
            drop(runtime);
            shutdown_done.send(()).unwrap();
        });
        let shutdown_without_unlock = shutdown_received
            .recv_timeout(Duration::from_secs(1))
            .is_ok();
        // Always release the owner before assertions, so an old implementation
        // fails cleanly instead of hanging the test process during runtime drop.
        drop(owner);
        shutdown.join().unwrap();
        assert!(
            unrelated_work_progressed,
            "cancelled waiters retained both blocking workers"
        );
        assert!(
            shutdown_without_unlock,
            "cancelled lock tasks stalled runtime shutdown"
        );
    }
}

#[cfg(test)]
mod portable_temporary_guard_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::PortableTemporaryFile;
    use std::{
        fs,
        io::{self, Write},
    };

    #[test]
    fn partial_file_guard_cleanup_preserves_original_failure() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("temporary");
        let failed = (|| -> io::Result<()> {
            let opened = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            let _temporary = PortableTemporaryFile {
                path: path.clone(),
                published: false,
            };
            let mut writer = opened;
            writer.write_all(b"partial bytes")?;
            // Exercise cleanup after an error with a partially written file.
            Err(io::Error::from_raw_os_error(28))
        })();
        assert_eq!(failed.unwrap_err().raw_os_error(), Some(28));
        assert!(!path.exists());
    }

    #[test]
    fn published_guard_preserves_destination() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("temporary");
        let destination = root.path().join("destination");
        fs::write(&path, b"published bytes").unwrap();
        let mut temporary = PortableTemporaryFile {
            path,
            published: false,
        };
        fs::rename(&temporary.path, &destination).unwrap();
        temporary.published = true;
        drop(temporary);
        assert_eq!(fs::read(destination).unwrap(), b"published bytes");
    }
}

#[cfg(test)]
mod journal_tests {
    #[test]
    fn legacy_tail_trimming_counts_empty_lines_in_committed_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("journal");
        for (prefix, head) in [
            (b"\n{}\n".as_slice(), 1),
            (b"{}\n\n{}\n".as_slice(), 2),
            (b"{}\n\n".as_slice(), 1),
        ] {
            for orphaned in [false, true] {
                let mut bytes = prefix.to_vec();
                if orphaned {
                    bytes.extend_from_slice(b"{}\n");
                }
                std::fs::write(&path, bytes)?;
                let retained_length = super::prepare_journal_append(&path, head, None)?;
                if retained_length != u64::try_from(prefix.len())?
                    || std::fs::read(&path)? != prefix
                {
                    return Err(format!("committed prefix changed at journal head {head}").into());
                }
            }
        }
        Ok(())
    }
}
