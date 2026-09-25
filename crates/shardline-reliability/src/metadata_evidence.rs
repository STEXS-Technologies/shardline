use serde::{Deserialize, Serialize};

use crate::snapshot_event::{SnapshotEvidence, SnapshotEvidenceEvent, verify_snapshot_chain};
use crate::snapshot_log::SnapshotEvidenceLog;
use crate::{OperationIdentity, OperationKind, ReliabilityError};

/// Typed operation identifier for one Hub repository reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCommitOperationId(String);

impl MetadataCommitOperationId {
    fn new(repository: &str, ref_name: &str) -> Self {
        [repository, ref_name]
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

impl From<String> for MetadataCommitOperationId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Integrity-checkable materialized head of one Hub repository reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HubRefSnapshot {
    pub repository: String,
    pub ref_name: String,
    pub head_sha: Option<String>,
}

impl HubRefSnapshot {
    ///
    /// # Errors
    ///
    /// Returns an error when validation, integrity verification, or canonicalization fails.
    pub fn new(
        repository: impl Into<String>,
        ref_name: impl Into<String>,
        head_sha: Option<String>,
    ) -> Result<Self, ReliabilityError> {
        let snapshot = Self {
            repository: repository.into(),
            ref_name: ref_name.into(),
            head_sha,
        };
        if snapshot.repository.is_empty() {
            return Err(ReliabilityError::EmptyField("repository"));
        }
        if snapshot.ref_name.is_empty() {
            return Err(ReliabilityError::EmptyField("ref_name"));
        }
        Ok(snapshot)
    }

    fn operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        Ok(OperationIdentity::new(
            "hub",
            self.repository.clone(),
            MetadataCommitOperationId::new(&self.repository, &self.ref_name).0,
            OperationKind::MetadataCommit,
        )?
        .with_object_key(self.ref_name.clone()))
    }
}

impl SnapshotEvidence for HubRefSnapshot {
    fn evidence_operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        self.operation()
    }

    fn validate_evidence_transition(&self, after: &Self) -> Result<(), ReliabilityError> {
        if self.repository != after.repository || self.ref_name != after.ref_name {
            return Err(ReliabilityError::OperationMismatch);
        }
        Ok(())
    }
}

pub type HubRefLifecycleEvent = SnapshotEvidenceEvent<HubRefSnapshot>;
pub type HubRefEvidenceLog = SnapshotEvidenceLog<HubRefSnapshot>;

///
/// # Errors
///
/// Returns an error when validation, integrity verification, or canonicalization fails.
pub fn verify_hub_ref_events(
    events: &[HubRefLifecycleEvent],
    expected: &HubRefSnapshot,
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
    fn ref_head_advances_and_deletes_with_integrity_checked_events() {
        let empty = HubRefSnapshot::new("org/model", "feature", None).unwrap();
        let first = HubRefSnapshot::new("org/model", "feature", Some("sha-1".into())).unwrap();
        let mut log = HubRefEvidenceLog::baseline(empty).unwrap();
        log.record(first.clone()).unwrap();
        log.record(HubRefSnapshot::new("org/model", "feature", Some("sha-2".into())).unwrap())
            .unwrap();
        log.record(HubRefSnapshot::new("org/model", "feature", None).unwrap())
            .unwrap();
        verify_hub_ref_events(
            log.events(),
            &HubRefSnapshot::new("org/model", "feature", None).unwrap(),
        )
        .unwrap();
        assert_ne!(first.evidence_operation().unwrap().operation_id, "feature");
    }
}
