use serde::{Deserialize, Serialize};

use crate::snapshot_event::{SnapshotEvidence, SnapshotEvidenceEvent, verify_snapshot_chain};
use crate::snapshot_log::SnapshotEvidenceLog;
use crate::{OperationIdentity, OperationKind, ReliabilityError};

/// Typed identity for one mutable OCI tag pointer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OciTagOperationId(String);

impl OciTagOperationId {
    fn new(scope_namespace: &str, repository: &str, tag: &str) -> Self {
        [scope_namespace, repository, tag]
            .into_iter()
            .map(|value| format!("{}:{value}", value.len()))
            .collect::<Vec<_>>()
            .join("")
            .into()
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for OciTagOperationId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Materialized OCI tag target, with `None` representing an absent tag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OciTagSnapshot {
    pub scope_namespace: String,
    pub repository: String,
    pub tag: String,
    pub digest_hex: Option<String>,
}

impl OciTagSnapshot {
    pub fn new(
        scope_namespace: impl Into<String>,
        repository: impl Into<String>,
        tag: impl Into<String>,
        digest_hex: Option<String>,
    ) -> Result<Self, ReliabilityError> {
        let snapshot = Self {
            scope_namespace: scope_namespace.into(),
            repository: repository.into(),
            tag: tag.into(),
            digest_hex,
        };
        for (field, value) in [
            ("scope_namespace", snapshot.scope_namespace.as_str()),
            ("repository", snapshot.repository.as_str()),
            ("tag", snapshot.tag.as_str()),
        ] {
            if value.is_empty() {
                return Err(ReliabilityError::EmptyField(field));
            }
        }
        Ok(snapshot)
    }

    fn operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        Ok(OperationIdentity::new(
            self.scope_namespace.clone(),
            self.repository.clone(),
            OciTagOperationId::new(&self.scope_namespace, &self.repository, &self.tag).0,
            OperationKind::OciTag,
        )?
        .with_object_key(self.tag.clone()))
    }
}

impl SnapshotEvidence for OciTagSnapshot {
    fn evidence_operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        self.operation()
    }

    fn validate_evidence_transition(&self, after: &Self) -> Result<(), ReliabilityError> {
        if self.scope_namespace != after.scope_namespace
            || self.repository != after.repository
            || self.tag != after.tag
        {
            return Err(ReliabilityError::OperationMismatch);
        }
        Ok(())
    }
}

pub type OciTagLifecycleEvent = SnapshotEvidenceEvent<OciTagSnapshot>;
pub type OciTagEvidenceLog = SnapshotEvidenceLog<OciTagSnapshot>;

pub fn verify_oci_tag_events(
    events: &[OciTagLifecycleEvent],
    expected: &OciTagSnapshot,
) -> Result<(), ReliabilityError> {
    verify_snapshot_chain(events)?;
    if events.last().is_some_and(|event| event.after == *expected) {
        Ok(())
    } else {
        Err(ReliabilityError::StateMismatch)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn tag_retarget_and_delete_have_integrity_checked_events() {
        let absent = OciTagSnapshot::new("tenant", "repo", "latest", None).unwrap();
        let first = OciTagSnapshot::new("tenant", "repo", "latest", Some("a".repeat(64))).unwrap();
        let second = OciTagSnapshot::new("tenant", "repo", "latest", Some("b".repeat(64))).unwrap();
        let mut log = OciTagEvidenceLog::baseline(absent).unwrap();
        log.record(first).unwrap();
        log.record(second).unwrap();
        log.record(OciTagSnapshot::new("tenant", "repo", "latest", None).unwrap())
            .unwrap();
        verify_oci_tag_events(
            log.events(),
            &OciTagSnapshot::new("tenant", "repo", "latest", None).unwrap(),
        )
        .unwrap();
    }
}
