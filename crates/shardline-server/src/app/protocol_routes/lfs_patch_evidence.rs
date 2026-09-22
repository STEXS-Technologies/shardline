use std::fs;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};

use shardline_reliability::{ResumableLifecycleState, SessionEvidenceLog};

use crate::ServerError;

/// The evidence sidecar is additive: historical LFS patch sessions without it
/// are reconstructed with the canonical active baseline on first access.
const EVIDENCE_SUFFIX: &str = ".evidence";

pub(super) fn evidence_path(dir: &Path, oid: &str) -> PathBuf {
    dir.join(format!("{oid}{EVIDENCE_SUFFIX}"))
}

pub(super) fn load(
    dir: &Path,
    oid: &str,
    scope_namespace: &str,
    session_id: &str,
    target_key: &str,
) -> Result<SessionEvidenceLog, ServerError> {
    let path = evidence_path(dir, oid);
    let log = match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(invalid_evidence)?,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            SessionEvidenceLog::for_legacy_session(scope_namespace, session_id, target_key)
                .map_err(invalid_evidence)?
        }
        Err(error) => return Err(error.into()),
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
    let mut log = load(dir, oid, scope_namespace, session_id, target_key)?;
    log.record(
        scope_namespace.to_owned(),
        session_id.to_owned(),
        target_key.to_owned(),
        before,
        after,
    )
    .map_err(invalid_evidence)?;
    let bytes = serde_json::to_vec(&log).map_err(invalid_evidence)?;
    let path = evidence_path(dir, oid);
    let temporary = path.with_extension("evidence.tmp");
    fs::write(&temporary, bytes)?;
    fs::rename(temporary, path)?;
    Ok(())
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
    let path = evidence_path(dir, oid);
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let log: SessionEvidenceLog = serde_json::from_slice(&bytes).map_err(invalid_evidence)?;
    log.verify().map_err(invalid_evidence)
}

pub(super) fn remove(dir: &Path, oid: &str) {
    drop(fs::remove_file(evidence_path(dir, oid)));
}

fn invalid_evidence(error: impl std::fmt::Display) -> ServerError {
    Error::new(
        ErrorKind::InvalidData,
        format!("invalid LFS patch evidence: {error}"),
    )
    .into()
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
