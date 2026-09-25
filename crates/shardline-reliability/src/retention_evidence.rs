use serde::{Deserialize, Serialize};

use crate::snapshot_event::{SnapshotEvidence, SnapshotEvidenceEvent, verify_snapshot_chain};
use crate::snapshot_log::SnapshotEvidenceLog;
use crate::{OperationIdentity, OperationKind, ReliabilityError};

/// Lifecycle state of a durable retention hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionHoldLifecycleState {
    Active,
    Released,
}

impl RetentionHoldLifecycleState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "Active",
            Self::Released => "Released",
        }
    }

    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Active, Self::Active)
                | (Self::Active, Self::Released)
                | (Self::Released, Self::Active)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionObjectIdentity {
    pub object_key: String,
}

impl RetentionObjectIdentity {
    ///
    /// # Errors
    ///
    /// Returns an error when validation, integrity verification, or canonicalization fails.
    pub fn new(object_key: impl Into<String>) -> Result<Self, ReliabilityError> {
        let identity = Self {
            object_key: object_key.into(),
        };
        if identity.object_key.is_empty() {
            return Err(ReliabilityError::EmptyField("object_key"));
        }
        Ok(identity)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionHoldSnapshot {
    pub object_key: String,
    pub reason: String,
    pub held_at_unix_seconds: u64,
    pub release_after_unix_seconds: Option<u64>,
    pub state: RetentionHoldLifecycleState,
}

impl RetentionHoldSnapshot {
    ///
    /// # Errors
    ///
    /// Returns an error when validation, integrity verification, or canonicalization fails.
    pub fn new(
        identity: RetentionObjectIdentity,
        reason: impl Into<String>,
        held_at_unix_seconds: u64,
        release_after_unix_seconds: Option<u64>,
        state: RetentionHoldLifecycleState,
    ) -> Result<Self, ReliabilityError> {
        let reason = reason.into();
        if reason.trim().is_empty() {
            return Err(ReliabilityError::EmptyField("reason"));
        }
        if release_after_unix_seconds.is_some_and(|release| release < held_at_unix_seconds) {
            return Err(ReliabilityError::InvalidTransition {
                before: "held_at_unix_seconds",
                after: "release_after_unix_seconds",
            });
        }
        Ok(Self {
            object_key: identity.object_key,
            reason,
            held_at_unix_seconds,
            release_after_unix_seconds,
            state,
        })
    }

    fn operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        Ok(OperationIdentity::new(
            "shardline-gc",
            "retention",
            self.object_key.clone(),
            OperationKind::RetentionHold,
        )?
        .with_object_key(self.object_key.clone()))
    }
}

impl SnapshotEvidence for RetentionHoldSnapshot {
    fn evidence_operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        self.operation()
    }

    fn validate_evidence_transition(&self, after: &Self) -> Result<(), ReliabilityError> {
        if self.object_key != after.object_key {
            return Err(ReliabilityError::OperationMismatch);
        }
        if !self.state.can_transition_to(after.state) {
            return Err(ReliabilityError::InvalidTransition {
                before: self.state.as_str(),
                after: after.state.as_str(),
            });
        }
        Ok(())
    }
}

pub type RetentionHoldLifecycleEvent = SnapshotEvidenceEvent<RetentionHoldSnapshot>;
pub type RetentionEvidenceLog = SnapshotEvidenceLog<RetentionHoldSnapshot>;

/// Verifies and records a new active retention hold after a prior hold was
/// released.
///
/// A recreated hold may carry different reason or retention timestamps. The
/// released snapshot in the journal is therefore authoritative for the
/// transition boundary; rebuilding it from the new hold would reject a valid
/// recreation as a state mismatch.
///
/// # Errors
///
/// Returns an error when the evidence chain is invalid, the prior state is
/// not `Released`, or the new snapshot cannot be appended.
pub fn verify_and_reactivate_retention_hold(
    mut evidence: RetentionEvidenceLog,
    snapshot: RetentionHoldSnapshot,
) -> Result<(RetentionEvidenceLog, bool), ReliabilityError> {
    if evidence.events().is_empty() {
        return Ok((RetentionEvidenceLog::baseline(snapshot)?, true));
    }
    if !evidence.is_head_only() {
        verify_snapshot_chain(evidence.events())?;
    }
    let last = evidence
        .events()
        .last()
        .ok_or(ReliabilityError::OperationMismatch)?;
    if last.after.state != RetentionHoldLifecycleState::Released {
        return Err(ReliabilityError::StateMismatch);
    }
    evidence.record(snapshot)?;
    Ok((evidence, false))
}

///
/// # Errors
///
/// Returns an error when validation, integrity verification, or canonicalization fails.
pub fn verify_retention_hold_lifecycle_chain(
    events: &[RetentionHoldLifecycleEvent],
) -> Result<(), ReliabilityError> {
    verify_snapshot_chain(events)
}

///
/// # Errors
///
/// Returns an error when validation, integrity verification, or canonicalization fails.
pub fn verify_retention_hold_lifecycle_events(
    events: &[RetentionHoldLifecycleEvent],
    expected: &RetentionHoldSnapshot,
) -> Result<(), ReliabilityError> {
    verify_retention_hold_lifecycle_chain(events)?;
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

    fn snapshot(state: RetentionHoldLifecycleState) -> RetentionHoldSnapshot {
        RetentionHoldSnapshot::new(
            RetentionObjectIdentity::new("aa/object").unwrap(),
            "legal hold",
            100,
            Some(200),
            state,
        )
        .unwrap()
    }

    #[test]
    fn active_release_active_chain_verifies() {
        let mut log =
            RetentionEvidenceLog::baseline(snapshot(RetentionHoldLifecycleState::Active)).unwrap();
        log.record(snapshot(RetentionHoldLifecycleState::Released))
            .unwrap();
        log.record(snapshot(RetentionHoldLifecycleState::Active))
            .unwrap();
        verify_retention_hold_lifecycle_events(
            log.events(),
            &snapshot(RetentionHoldLifecycleState::Active),
        )
        .unwrap();
    }

    #[test]
    fn tampered_reason_is_rejected() {
        let log =
            RetentionEvidenceLog::baseline(snapshot(RetentionHoldLifecycleState::Active)).unwrap();
        let mut events = log.events().to_vec();
        if let Some(event) = events.first_mut() {
            event.after.reason = "tampered".to_owned();
        }
        assert!(verify_retention_hold_lifecycle_chain(&events).is_err());
    }

    #[test]
    fn reactivation_uses_the_released_snapshot_as_transition_boundary() {
        let original = snapshot(RetentionHoldLifecycleState::Active);
        let released = snapshot(RetentionHoldLifecycleState::Released);
        let mut evidence = RetentionEvidenceLog::baseline(original).unwrap();
        evidence.record(released).unwrap();
        let changed = RetentionHoldSnapshot::new(
            RetentionObjectIdentity::new("aa/object").unwrap(),
            "updated legal hold",
            101,
            Some(202),
            RetentionHoldLifecycleState::Active,
        )
        .unwrap();

        let (evidence, was_baseline) =
            verify_and_reactivate_retention_hold(evidence, changed.clone()).unwrap();

        assert!(!was_baseline);
        assert_eq!(evidence.events().last().unwrap().after, changed);
        verify_retention_hold_lifecycle_events(evidence.events(), &changed).unwrap();
    }
}
