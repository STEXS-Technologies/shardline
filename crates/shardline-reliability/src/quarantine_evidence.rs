use serde::{Deserialize, Serialize};

use crate::snapshot_event::{SnapshotEvidence, SnapshotEvidenceEvent, verify_snapshot_chain};
use crate::snapshot_log::SnapshotEvidenceLog;
use crate::{OperationIdentity, OperationKind, ReliabilityError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuarantineLifecycleState {
    Active,
    Released,
}

impl QuarantineLifecycleState {
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
pub struct QuarantineObjectIdentity {
    pub object_key: String,
}

impl QuarantineObjectIdentity {
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
pub struct QuarantineSnapshot {
    pub object_key: String,
    pub observed_length: u64,
    pub first_seen_unreachable_at_unix_seconds: u64,
    pub delete_after_unix_seconds: u64,
    pub state: QuarantineLifecycleState,
}

impl QuarantineSnapshot {
    ///
    /// # Errors
    ///
    /// Returns an error when validation, integrity verification, or canonicalization fails.
    pub fn new(
        identity: QuarantineObjectIdentity,
        observed_length: u64,
        first_seen_unreachable_at_unix_seconds: u64,
        delete_after_unix_seconds: u64,
        state: QuarantineLifecycleState,
    ) -> Result<Self, ReliabilityError> {
        if delete_after_unix_seconds < first_seen_unreachable_at_unix_seconds {
            return Err(ReliabilityError::InvalidTransition {
                before: "first_seen_unreachable_at_unix_seconds",
                after: "delete_after_unix_seconds",
            });
        }
        Ok(Self {
            object_key: identity.object_key,
            observed_length,
            first_seen_unreachable_at_unix_seconds,
            delete_after_unix_seconds,
            state,
        })
    }

    fn operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        let operation = OperationIdentity::new(
            "shardline-gc",
            "quarantine",
            self.object_key.clone(),
            OperationKind::GarbageCollection,
        )?;
        Ok(operation.with_object_key(self.object_key.clone()))
    }
}

impl SnapshotEvidence for QuarantineSnapshot {
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

pub type QuarantineLifecycleEvent = SnapshotEvidenceEvent<QuarantineSnapshot>;

///
/// # Errors
///
/// Returns an error when validation, integrity verification, or canonicalization fails.
pub fn verify_quarantine_lifecycle_chain(
    events: &[QuarantineLifecycleEvent],
) -> Result<(), ReliabilityError> {
    verify_snapshot_chain(events)
}

///
/// # Errors
///
/// Returns an error when validation, integrity verification, or canonicalization fails.
pub fn verify_quarantine_lifecycle_events(
    events: &[QuarantineLifecycleEvent],
    expected: &QuarantineSnapshot,
) -> Result<(), ReliabilityError> {
    verify_quarantine_lifecycle_chain(events)?;
    if events.last().is_some_and(|event| event.after == *expected) {
        Ok(())
    } else {
        Err(ReliabilityError::StateMismatch)
    }
}

pub type QuarantineEvidenceLog = SnapshotEvidenceLog<QuarantineSnapshot>;

/// Verifies and records a new active quarantine observation after a prior
/// candidate was released.
///
/// A reactivated object may have a different observed length or retention
/// window. The released snapshot in the journal is therefore authoritative
/// for the transition's `before` state; reconstructing it from the new active
/// candidate would incorrectly reject legitimate reactivation.
///
/// # Errors
///
/// Returns an error when the evidence chain is invalid, the prior state is
/// not `Released`, or the new snapshot cannot be appended.
pub fn verify_and_reactivate_quarantine(
    mut evidence: QuarantineEvidenceLog,
    snapshot: QuarantineSnapshot,
) -> Result<(QuarantineEvidenceLog, bool), ReliabilityError> {
    if evidence.events().is_empty() {
        return Ok((QuarantineEvidenceLog::baseline(snapshot)?, true));
    }
    if !evidence.is_head_only() {
        verify_snapshot_chain(evidence.events())?;
    }
    let last = evidence
        .events()
        .last()
        .ok_or(ReliabilityError::OperationMismatch)?;
    if last.after.state != QuarantineLifecycleState::Released {
        return Err(ReliabilityError::StateMismatch);
    }
    evidence.record(snapshot)?;
    Ok((evidence, false))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn snapshot(state: QuarantineLifecycleState) -> QuarantineSnapshot {
        QuarantineSnapshot::new(
            QuarantineObjectIdentity::new("aa/object").unwrap(),
            42,
            100,
            200,
            state,
        )
        .unwrap()
    }

    #[test]
    fn evidence_round_trip_verifies_active_release_active_recovery() {
        let mut log =
            QuarantineEvidenceLog::baseline(snapshot(QuarantineLifecycleState::Active)).unwrap();
        log.record(snapshot(QuarantineLifecycleState::Released))
            .unwrap();
        log.record(snapshot(QuarantineLifecycleState::Active))
            .unwrap();
        log.verify_for(&snapshot(QuarantineLifecycleState::Active))
            .unwrap();
    }

    #[test]
    fn state_digest_tampering_is_rejected() {
        let mut log =
            QuarantineEvidenceLog::baseline(snapshot(QuarantineLifecycleState::Active)).unwrap();
        log.record(snapshot(QuarantineLifecycleState::Released))
            .unwrap();
        let mut events = log.events().to_vec();
        events.get_mut(1).unwrap().after.observed_length = 43;
        assert!(matches!(
            verify_quarantine_lifecycle_chain(&events),
            Err(ReliabilityError::StateDigestMismatch)
        ));
    }

    #[test]
    fn released_candidate_cannot_be_released_twice() {
        let active = snapshot(QuarantineLifecycleState::Active);
        let released = snapshot(QuarantineLifecycleState::Released);
        assert!(QuarantineLifecycleEvent::new(0, active, released.clone()).is_ok());
        assert!(matches!(
            QuarantineLifecycleEvent::new(1, released.clone(), released),
            Err(ReliabilityError::InvalidTransition { .. })
        ));
    }

    #[test]
    fn reactivation_uses_the_released_snapshot_as_transition_boundary() {
        let original = snapshot(QuarantineLifecycleState::Active);
        let released = snapshot(QuarantineLifecycleState::Released);
        let mut evidence = QuarantineEvidenceLog::baseline(original).unwrap();
        evidence.record(released).unwrap();
        let changed = QuarantineSnapshot::new(
            QuarantineObjectIdentity::new("aa/object").unwrap(),
            43,
            101,
            202,
            QuarantineLifecycleState::Active,
        )
        .unwrap();

        let (reactivated, was_baseline) =
            verify_and_reactivate_quarantine(evidence, changed.clone()).unwrap();
        assert!(!was_baseline);
        assert_eq!(reactivated.events().last().unwrap().after, changed);
    }
}
