use std::fs;
use std::io::{Error, ErrorKind, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;
use shardline_reliability::{
    DigestSnapshot, ResumableLifecycleState, ResumableSessionSnapshotDomain, SessionEvidenceLog,
    SnapshotEvidenceLog, append_or_baseline_snapshot_evidence, canonical_state_digest,
    resumable_session_snapshot_identity, verify_and_append_session_transition,
};

use crate::ServerError;

/// The evidence sidecar is additive: historical LFS patch sessions without it
/// are reconstructed with the canonical active baseline on first access.
const EVIDENCE_SUFFIX: &str = ".evidence";
const SNAPSHOT_SUFFIX: &str = ".snapshot";

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
    let mut log = match fs::read(&path) {
        Ok(bytes) => {
            let events: Vec<_> = serde_json::from_slice(&bytes).map_err(invalid_evidence)?;
            SnapshotEvidenceLog::from_events(events).map_err(invalid_evidence)?
        }
        Err(error) if error.kind() == ErrorKind::NotFound => SnapshotEvidenceLog::default(),
        Err(error) => return Err(error.into()),
    };
    log = append_or_baseline_snapshot_evidence(log, snapshot).map_err(invalid_evidence)?;
    let bytes = serde_json::to_vec(&log).map_err(invalid_evidence)?;
    write_sidecar_atomically(dir, &path, &bytes)
}

/// Verifies the latest persisted materialized snapshot against the state the
/// caller reconstructed from disk. A missing snapshot is valid for legacy
/// sessions and will be recreated by the next successful mutation.
pub(super) fn verify_snapshot(
    dir: &Path,
    input: &LfsPatchSnapshotInput<'_>,
) -> Result<(), ServerError> {
    let path = snapshot_path(dir, input.oid);
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let events = serde_json::from_slice(&bytes).map_err(invalid_evidence)?;
    let log = SnapshotEvidenceLog::from_events(events).map_err(invalid_evidence)?;
    let expected = materialized_snapshot(input)?;
    log.verify_for(&expected).map_err(invalid_evidence)
}

pub(super) fn load(
    dir: &Path,
    oid: &str,
    scope_namespace: &str,
    session_id: &str,
    target_key: &str,
) -> Result<SessionEvidenceLog, ServerError> {
    let path = evidence_path(dir, oid);
    let (log, evidence_was_missing) = match fs::read(&path) {
        Ok(bytes) => (
            serde_json::from_slice(&bytes).map_err(invalid_evidence)?,
            false,
        ),
        Err(error) if error.kind() == ErrorKind::NotFound => (
            SessionEvidenceLog::for_legacy_session(scope_namespace, session_id, target_key)
                .map_err(invalid_evidence)?,
            true,
        ),
        Err(error) => return Err(error.into()),
    };
    log.verify_for(scope_namespace, session_id, target_key)
        .map_err(invalid_evidence)?;
    if evidence_was_missing {
        let bytes = serde_json::to_vec(&log).map_err(invalid_evidence)?;
        write_sidecar_atomically(dir, &path, &bytes)?;
    }
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
    let log = load(dir, oid, scope_namespace, session_id, target_key)?;
    let (log, _) = verify_and_append_session_transition(
        log,
        scope_namespace,
        session_id,
        target_key,
        before,
        after,
    )
    .map_err(invalid_evidence)?;
    let bytes = serde_json::to_vec(&log).map_err(invalid_evidence)?;
    let path = evidence_path(dir, oid);
    write_sidecar_atomically(dir, &path, &bytes)
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
    let evidence_file = evidence_path(dir, oid);
    let evidence_bytes = match fs::read(evidence_file) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let log: SessionEvidenceLog =
        serde_json::from_slice(&evidence_bytes).map_err(invalid_evidence)?;
    log.verify().map_err(invalid_evidence)?;
    let snapshot_file = snapshot_path(dir, oid);
    let snapshot_bytes = match fs::read(snapshot_file) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let events = serde_json::from_slice(&snapshot_bytes).map_err(invalid_evidence)?;
    let _: SnapshotEvidenceLog<DigestSnapshot> =
        SnapshotEvidenceLog::from_events(events).map_err(invalid_evidence)?;
    Ok(())
}

pub(super) fn remove(dir: &Path, oid: &str) {
    drop(fs::remove_file(evidence_path(dir, oid)));
    drop(fs::remove_file(snapshot_path(dir, oid)));
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
        assert!(evidence_path(directory.path(), OID).is_file());

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
        let stored = load(directory.path(), OID, SCOPE, SESSION, TARGET).expect("stored evidence");
        assert_eq!(stored.events().len(), 2);
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
        let path = snapshot_path(directory.path(), OID);
        let stored: SnapshotEvidenceLog<DigestSnapshot> =
            serde_json::from_slice(&fs::read(&path).expect("snapshot bytes")).expect("snapshot");
        assert_eq!(stored.events().len(), 2);
        verify_integrity(directory.path(), OID).expect("snapshot chain verifies");

        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).expect("snapshot bytes")).expect("json");
        value[0]["state_digest"] = serde_json::json!("tampered");
        fs::write(&path, serde_json::to_vec(&value).expect("tampered json")).expect("rewrite");
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
        let path = evidence_path(directory.path(), OID);
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).expect("evidence bytes")).expect("json");
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
