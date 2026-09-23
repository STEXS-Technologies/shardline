use std::fs;
use std::io::{Error, ErrorKind, Write};
use std::path::{Path, PathBuf};

use serde::{Serialize, de::DeserializeOwned};
use shardline_reliability::{
    DigestSnapshot, ResumableLifecycleState, ResumableSessionSnapshotDomain, SessionEvidenceLog,
    SnapshotEvidenceLog, append_or_baseline_snapshot_evidence, canonical_state_digest,
    resumable_session_snapshot_identity, verify_and_append_session_transition,
};

use crate::ServerError;

/// The evidence sidecar is additive: historical LFS patch sessions without it
/// are reconstructed in memory and persisted by the next successful mutation.
const EVIDENCE_SUFFIX: &str = ".evidence";
const SNAPSHOT_SUFFIX: &str = ".snapshot";
const EVIDENCE_JOURNAL_SCHEMA: &str = "shardline.lfs.evidence-journal.v1";
const SNAPSHOT_JOURNAL_SCHEMA: &str = "shardline.lfs.snapshot-journal.v1";

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
struct JournalManifest {
    schema: String,
    head: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct LfsPatchMaterializedStateV1 {
    oid: String,
    scope_namespace: String,
    session_id: String,
    target_key: String,
    total_bytes: u64,
    ranges: Vec<(u64, u64)>,
    staging_length: u64,
    last_touched_unix_seconds: u64,
}

pub(super) struct LfsPatchSnapshotInput<'input> {
    pub(super) oid: &'input str,
    pub(super) scope_namespace: &'input str,
    pub(super) session_id: &'input str,
    pub(super) target_key: &'input str,
    pub(super) total_bytes: u64,
    pub(super) ranges: &'input [(u64, u64)],
    pub(super) staging_length: u64,
    pub(super) last_touched_unix_seconds: u64,
}

pub(super) fn evidence_path(dir: &Path, oid: &str) -> PathBuf {
    dir.join(format!("{oid}{EVIDENCE_SUFFIX}"))
}

fn snapshot_path(dir: &Path, oid: &str) -> PathBuf {
    dir.join(format!("{oid}{SNAPSHOT_SUFFIX}"))
}

fn evidence_journal_path(dir: &Path, oid: &str) -> PathBuf {
    dir.join(format!("{oid}{EVIDENCE_SUFFIX}.log"))
}

fn snapshot_journal_path(dir: &Path, oid: &str) -> PathBuf {
    dir.join(format!("{oid}{SNAPSHOT_SUFFIX}.log"))
}

fn journal_head(path: &Path, schema: &str) -> Result<Option<u64>, ServerError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let manifest: JournalManifest = serde_json::from_slice(&bytes).map_err(invalid_evidence)?;
    if manifest.schema != schema {
        return Err(invalid_evidence(format!(
            "unsupported LFS evidence journal schema: {}",
            manifest.schema
        )));
    }
    Ok(Some(manifest.head))
}

fn read_journal<T: DeserializeOwned>(
    manifest_path: &Path,
    journal_path: &Path,
    schema: &str,
) -> Result<Option<Vec<T>>, ServerError> {
    let Some(head) = journal_head(manifest_path, schema)? else {
        return Ok(None);
    };
    let bytes = fs::read(journal_path).map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            invalid_evidence("LFS evidence journal is missing its committed log")
        } else {
            ServerError::from(error)
        }
    })?;
    let mut events = Vec::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let mut record: Vec<T> = serde_json::from_slice(line).map_err(invalid_evidence)?;
        events.append(&mut record);
    }
    let count = usize::try_from(head).map_err(|_| invalid_evidence("LFS journal head overflow"))?;
    if count > events.len() {
        return Err(invalid_evidence(
            "LFS evidence journal head exceeds its log",
        ));
    }
    events.truncate(count);
    Ok(Some(events))
}

fn append_journal<T: Serialize>(
    dir: &Path,
    manifest_path: &Path,
    journal_path: &Path,
    schema: &str,
    previous_head: u64,
    events: &[T],
) -> Result<(), ServerError> {
    if events.is_empty() {
        return Ok(());
    }
    let mut bytes = serde_json::to_vec(events).map_err(invalid_evidence)?;
    bytes.push(b'\n');
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(journal_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    let manifest = JournalManifest {
        schema: schema.to_owned(),
        head: previous_head
            .checked_add(
                u64::try_from(events.len())
                    .map_err(|_| invalid_evidence("LFS evidence journal event count overflow"))?,
            )
            .ok_or_else(|| invalid_evidence("LFS evidence journal head overflow"))?,
    };
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(invalid_evidence)?;
    write_sidecar_atomically(dir, manifest_path, &manifest_bytes)
}

fn materialized_snapshot(input: &LfsPatchSnapshotInput<'_>) -> Result<DigestSnapshot, ServerError> {
    let operation = resumable_session_snapshot_identity(
        ResumableSessionSnapshotDomain::LfsPatch,
        input.scope_namespace.to_owned(),
        input.session_id.to_owned(),
        input.target_key.to_owned(),
    )
    .map_err(invalid_evidence)?;
    let state = LfsPatchMaterializedStateV1 {
        oid: input.oid.to_owned(),
        scope_namespace: input.scope_namespace.to_owned(),
        session_id: input.session_id.to_owned(),
        target_key: input.target_key.to_owned(),
        total_bytes: input.total_bytes,
        ranges: input.ranges.to_vec(),
        staging_length: input.staging_length,
        last_touched_unix_seconds: input.last_touched_unix_seconds,
    };
    let digest = canonical_state_digest(&state).map_err(invalid_evidence)?;
    Ok(DigestSnapshot::new(operation, digest))
}

pub(super) fn record_snapshot(
    dir: &Path,
    input: &LfsPatchSnapshotInput<'_>,
) -> Result<(), ServerError> {
    let snapshot = materialized_snapshot(input)?;
    let path = snapshot_path(dir, input.oid);
    let head = journal_head(&path, SNAPSHOT_JOURNAL_SCHEMA)?;
    let previous = load_snapshot_log(dir, input.oid)?.unwrap_or_default();
    let previous_len = previous.events().len();
    let mut log = previous;
    log = append_or_baseline_snapshot_evidence(log, snapshot).map_err(invalid_evidence)?;
    let events = if head.is_some() {
        &log.events()[previous_len..]
    } else {
        log.events()
    };
    append_journal(
        dir,
        &path,
        &snapshot_journal_path(dir, input.oid),
        SNAPSHOT_JOURNAL_SCHEMA,
        head.unwrap_or(0),
        events,
    )
}

/// Verifies the latest persisted materialized snapshot against the state the
/// caller reconstructed from disk. A missing snapshot is valid for legacy
/// sessions and will be recreated by the next successful mutation.
pub(super) fn verify_snapshot(
    dir: &Path,
    input: &LfsPatchSnapshotInput<'_>,
) -> Result<(), ServerError> {
    let Some(log) = load_snapshot_log(dir, input.oid)? else {
        return Ok(());
    };
    let expected = materialized_snapshot(input)?;
    log.verify_for(&expected).map_err(invalid_evidence)
}

fn load_snapshot_log(
    dir: &Path,
    oid: &str,
) -> Result<Option<SnapshotEvidenceLog<DigestSnapshot>>, ServerError> {
    if let Some(events) =
        read_journal::<shardline_reliability::SnapshotEvidenceEvent<DigestSnapshot>>(
            &snapshot_path(dir, oid),
            &snapshot_journal_path(dir, oid),
            SNAPSHOT_JOURNAL_SCHEMA,
        )?
    {
        return SnapshotEvidenceLog::from_events(events)
            .map(Some)
            .map_err(invalid_evidence);
    }
    let bytes = match fs::read(snapshot_path(dir, oid)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let events = serde_json::from_slice(&bytes).map_err(invalid_evidence)?;
    SnapshotEvidenceLog::from_events(events)
        .map(Some)
        .map_err(invalid_evidence)
}

pub(super) fn load(
    dir: &Path,
    oid: &str,
    scope_namespace: &str,
    session_id: &str,
    target_key: &str,
) -> Result<SessionEvidenceLog, ServerError> {
    let log = if let Some(events) = read_journal::<shardline_reliability::StateTransitionEvent>(
        &evidence_path(dir, oid),
        &evidence_journal_path(dir, oid),
        EVIDENCE_JOURNAL_SCHEMA,
    )? {
        SessionEvidenceLog::from_events(events).map_err(invalid_evidence)?
    } else {
        match fs::read(evidence_path(dir, oid)) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(invalid_evidence)?,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                SessionEvidenceLog::for_legacy_session(scope_namespace, session_id, target_key)
                    .map_err(invalid_evidence)?
            }
            Err(error) => return Err(error.into()),
        }
    };
    log.verify_for(scope_namespace, session_id, target_key)
        .map_err(invalid_evidence)?;
    Ok(log)
}

pub(super) fn record(
    dir: &Path,
    oid: &str,
    scope_namespace: &str,
    session_id: &str,
    target_key: &str,
    before: ResumableLifecycleState,
    after: ResumableLifecycleState,
) -> Result<(), ServerError> {
    let head = journal_head(&evidence_path(dir, oid), EVIDENCE_JOURNAL_SCHEMA)?;
    let log = load(dir, oid, scope_namespace, session_id, target_key)?;
    let previous_len = log.events().len();
    let (log, _) = verify_and_append_session_transition(
        log,
        scope_namespace,
        session_id,
        target_key,
        before,
        after,
    )
    .map_err(invalid_evidence)?;
    let path = evidence_path(dir, oid);
    let events = if head.is_some() {
        &log.events()[previous_len..]
    } else {
        log.events()
    };
    append_journal(
        dir,
        &path,
        &evidence_journal_path(dir, oid),
        EVIDENCE_JOURNAL_SCHEMA,
        head.unwrap_or(0),
        events,
    )
}

/// Commits one evidence sidecar atomically and removes the staging file on
/// every failed write or rename path. This keeps a failed local repair from
/// leaving an ambiguous second state artifact beside the verified sidecar.
fn write_sidecar_atomically(dir: &Path, path: &Path, bytes: &[u8]) -> Result<(), ServerError> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("sidecar");
    let temporary = path.with_extension(format!("{extension}.tmp"));
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)?;
        sync_directory(dir)?;
        Ok(())
    })();
    if result.is_err() {
        let _ignored = fs::remove_file(&temporary);
    }
    result
}

pub(super) fn transition(
    dir: &Path,
    oid: &str,
    scope_namespace: &str,
    session_id: &str,
    target_key: &str,
    before: ResumableLifecycleState,
    after: ResumableLifecycleState,
) -> Result<(), ServerError> {
    let log = load(dir, oid, scope_namespace, session_id, target_key)?;
    let current = log
        .events()
        .last()
        .map_or(ResumableLifecycleState::Active, |event| event.after);
    if current == after {
        return Ok(());
    }
    if current != before {
        return Err(invalid_evidence(format!(
            "unexpected LFS patch state: expected {}, found {}",
            before.as_str(),
            current.as_str()
        )));
    }
    record(
        dir,
        oid,
        scope_namespace,
        session_id,
        target_key,
        before,
        after,
    )
}

pub(super) fn complete(
    dir: &Path,
    oid: &str,
    scope_namespace: &str,
    session_id: &str,
    target_key: &str,
) -> Result<(), ServerError> {
    let log = load(dir, oid, scope_namespace, session_id, target_key)?;
    let current = log
        .events()
        .last()
        .map_or(ResumableLifecycleState::Active, |event| event.after);
    if current == ResumableLifecycleState::Completed {
        return Ok(());
    }
    if current == ResumableLifecycleState::Active {
        record(
            dir,
            oid,
            scope_namespace,
            session_id,
            target_key,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Completing,
        )?;
    } else if current != ResumableLifecycleState::Completing {
        return Err(invalid_evidence(format!(
            "cannot complete LFS patch from {}",
            current.as_str()
        )));
    }
    record(
        dir,
        oid,
        scope_namespace,
        session_id,
        target_key,
        ResumableLifecycleState::Completing,
        ResumableLifecycleState::Completed,
    )
}

pub(super) fn verify_integrity(dir: &Path, oid: &str) -> Result<(), ServerError> {
    if let Some(events) = read_journal::<shardline_reliability::StateTransitionEvent>(
        &evidence_path(dir, oid),
        &evidence_journal_path(dir, oid),
        EVIDENCE_JOURNAL_SCHEMA,
    )? {
        SessionEvidenceLog::from_events(events).map_err(invalid_evidence)?;
    } else {
        match fs::read(evidence_path(dir, oid)) {
            Ok(bytes) => {
                let events = serde_json::from_slice(&bytes).map_err(invalid_evidence)?;
                SessionEvidenceLog::from_events(events).map_err(invalid_evidence)?;
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    let _ = load_snapshot_log(dir, oid)?;
    Ok(())
}

pub(super) fn remove(dir: &Path, oid: &str) {
    drop(fs::remove_file(evidence_path(dir, oid)));
    drop(fs::remove_file(evidence_journal_path(dir, oid)));
    drop(fs::remove_file(snapshot_path(dir, oid)));
    drop(fs::remove_file(snapshot_journal_path(dir, oid)));
}

fn invalid_evidence(error: impl std::fmt::Display) -> ServerError {
    Error::new(
        ErrorKind::InvalidData,
        format!("invalid LFS patch evidence: {error}"),
    )
    .into()
}

#[cfg(unix)]
fn sync_directory(dir: &Path) -> Result<(), ServerError> {
    fs::File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_dir: &Path) -> Result<(), ServerError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCOPE: &str = "scope";
    const SESSION: &str = "session";
    const TARGET: &str = "objects/target";
    const OID: &str = "a";

    #[test]
    fn legacy_session_gets_canonical_baseline_and_can_advance() {
        let directory = tempfile::tempdir().expect("tempdir");
        let log = load(directory.path(), OID, SCOPE, SESSION, TARGET).expect("legacy baseline");
        assert_eq!(
            log.events().last().expect("baseline event").after,
            ResumableLifecycleState::Active
        );
        assert!(!evidence_path(directory.path(), OID).exists());

        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("record baseline-compatible mutation");
        assert!(evidence_path(directory.path(), OID).is_file());
        let stored = load(directory.path(), OID, SCOPE, SESSION, TARGET).expect("stored evidence");
        assert_eq!(stored.events().len(), 2);
    }

    #[test]
    fn uncommitted_lfs_journal_tail_is_ignored() {
        let directory = tempfile::tempdir().expect("tempdir");
        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("write evidence");
        let journal = evidence_journal_path(directory.path(), OID);
        let committed_record = fs::read_to_string(&journal).expect("journal");
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&journal)
            .expect("open journal");
        file.write_all(committed_record.as_bytes())
            .expect("append uncommitted tail");
        let loaded = load(directory.path(), OID, SCOPE, SESSION, TARGET).expect("load evidence");
        assert_eq!(loaded.events().len(), 2);
    }

    #[test]
    fn sidecar_write_failure_removes_temporary_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = evidence_path(directory.path(), OID);
        fs::create_dir(&path).expect("target directory");

        assert!(write_sidecar_atomically(directory.path(), &path, b"evidence").is_err());
        assert!(!directory.path().join("a.evidence.tmp").exists());
    }

    #[test]
    fn materialized_snapshot_history_is_append_only_and_tamper_checked() {
        let directory = tempfile::tempdir().expect("tempdir");
        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("lifecycle baseline");
        record_snapshot(
            directory.path(),
            &LfsPatchSnapshotInput {
                oid: OID,
                scope_namespace: SCOPE,
                session_id: SESSION,
                target_key: TARGET,
                total_bytes: 10,
                ranges: &[],
                staging_length: 0,
                last_touched_unix_seconds: 100,
            },
        )
        .expect("snapshot baseline");
        record_snapshot(
            directory.path(),
            &LfsPatchSnapshotInput {
                oid: OID,
                scope_namespace: SCOPE,
                session_id: SESSION,
                target_key: TARGET,
                total_bytes: 10,
                ranges: &[(0, 5)],
                staging_length: 5,
                last_touched_unix_seconds: 101,
            },
        )
        .expect("snapshot append");
        let stored = load_snapshot_log(directory.path(), OID)
            .expect("snapshot log")
            .expect("snapshot");
        assert_eq!(stored.events().len(), 2);
        verify_integrity(directory.path(), OID).expect("snapshot chain verifies");

        let journal = snapshot_journal_path(directory.path(), OID);
        let mut value: serde_json::Value = serde_json::from_str(
            fs::read_to_string(&journal)
                .expect("snapshot journal")
                .lines()
                .next()
                .expect("snapshot journal record"),
        )
        .expect("json");
        value[0]["state_digest"] = serde_json::json!("tampered");
        fs::write(&journal, serde_json::to_vec(&value).expect("tampered json")).expect("rewrite");
        assert!(verify_integrity(directory.path(), OID).is_err());
    }

    #[test]
    fn tampered_evidence_is_rejected_before_a_transition() {
        let directory = tempfile::tempdir().expect("tempdir");
        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("write evidence");
        let path = evidence_journal_path(directory.path(), OID);
        let mut value: serde_json::Value = serde_json::from_str(
            fs::read_to_string(&path)
                .expect("evidence bytes")
                .lines()
                .next()
                .expect("evidence journal record"),
        )
        .expect("json");
        value[0]["operation"]["repository"] = serde_json::json!("wrong-scope");
        fs::write(&path, serde_json::to_vec(&value).expect("tampered json")).expect("rewrite");

        let error = load(directory.path(), OID, SCOPE, SESSION, TARGET).expect_err("tampering");
        assert!(matches!(
            error,
            ServerError::Io(ref io_error) if io_error.kind() == ErrorKind::InvalidData
        ));
    }

    #[test]
    fn materialized_state_tampering_is_rejected_before_snapshot_append() {
        let directory = tempfile::tempdir().expect("tempdir");
        let input = LfsPatchSnapshotInput {
            oid: OID,
            scope_namespace: SCOPE,
            session_id: SESSION,
            target_key: TARGET,
            total_bytes: 10,
            ranges: &[(0, 5)],
            staging_length: 5,
            last_touched_unix_seconds: 100,
        };
        record_snapshot(directory.path(), &input).expect("snapshot baseline");
        let tampered = LfsPatchSnapshotInput {
            ranges: &[(0, 4)],
            ..input
        };
        assert!(verify_snapshot(directory.path(), &tampered).is_err());
        verify_snapshot(directory.path(), &input).expect("canonical snapshot");
    }

    #[test]
    fn completion_path_is_ordered_and_idempotent() {
        let directory = tempfile::tempdir().expect("tempdir");
        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("active evidence");
        transition(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Completing,
        )
        .expect("completion claim");
        complete(directory.path(), OID, SCOPE, SESSION, TARGET).expect("terminal evidence");
        complete(directory.path(), OID, SCOPE, SESSION, TARGET)
            .expect("repeated completion is idempotent");

        let log = load(directory.path(), OID, SCOPE, SESSION, TARGET).expect("verified log");
        assert_eq!(
            log.events().last().expect("terminal event").after,
            ResumableLifecycleState::Completed
        );
        assert_eq!(log.events().len(), 4);
    }

    #[test]
    fn identity_mismatch_cannot_advance_a_valid_chain() {
        let directory = tempfile::tempdir().expect("tempdir");
        record(
            directory.path(),
            OID,
            SCOPE,
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .expect("active evidence");
        let error = transition(
            directory.path(),
            OID,
            "other-scope",
            SESSION,
            TARGET,
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Completing,
        )
        .expect_err("identity mismatch");
        assert!(matches!(
            error,
            ServerError::Io(ref io_error) if io_error.kind() == ErrorKind::InvalidData
        ));
    }
}
