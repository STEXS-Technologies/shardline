use serde::{Deserialize, Serialize};

use crate::{
    ReliabilityError,
    snapshot_event::{SnapshotEvidence, SnapshotEvidenceEvent, verify_snapshot_chain},
};

/// One canonical log container for every durable snapshot state machine.
///
/// The domain supplies only its typed snapshot and transition rules. This
/// newtype owns baseline creation, sequence allocation, append validation,
/// deserialization validation, and current-state verification for provider,
/// quarantine, and OCI snapshots alike.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEvidenceLog<S: SnapshotEvidence>(Vec<SnapshotEvidenceEvent<S>>);

impl<S: SnapshotEvidence> Default for SnapshotEvidenceLog<S> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<S: SnapshotEvidence> SnapshotEvidenceLog<S> {
    /// Wraps persisted events after validating their complete chain.
    pub fn from_events(events: Vec<SnapshotEvidenceEvent<S>>) -> Result<Self, ReliabilityError> {
        verify_snapshot_chain(&events)?;
        Ok(Self(events))
    }

    /// Creates the sequence-zero self-baseline for a materialized snapshot.
    pub fn baseline(snapshot: S) -> Result<Self, ReliabilityError> {
        Ok(Self(vec![SnapshotEvidenceEvent::new(
            0,
            snapshot.clone(),
            snapshot,
        )?]))
    }

    /// Appends one typed snapshot boundary and verifies the complete chain.
    pub fn record(&mut self, snapshot: S) -> Result<(), ReliabilityError> {
        let before = self
            .0
            .last()
            .map(|event| event.after.clone())
            .unwrap_or_else(|| snapshot.clone());
        let sequence = self
            .0
            .last()
            .map_or(0, |event| event.sequence.saturating_add(1));
        self.0
            .push(SnapshotEvidenceEvent::new(sequence, before, snapshot)?);
        let result = verify_snapshot_chain(&self.0);
        if result.is_err() {
            self.0.pop();
        }
        result
    }

    /// Verifies the chain and binds it to the current materialized snapshot.
    pub fn verify_for(&self, expected: &S) -> Result<(), ReliabilityError> {
        verify_snapshot_chain(&self.0)?;
        if self.0.last().is_some_and(|event| event.after == *expected) {
            Ok(())
        } else {
            Err(ReliabilityError::StateMismatch)
        }
    }

    /// Returns the ordered evidence events.
    #[must_use]
    pub fn events(&self) -> &[SnapshotEvidenceEvent<S>] {
        &self.0
    }

    #[cfg(test)]
    pub(crate) const fn events_mut(&mut self) -> &mut Vec<SnapshotEvidenceEvent<S>> {
        &mut self.0
    }
}

/// Verifies a persisted snapshot log against the materialized state, or
/// reconstructs its canonical baseline when the log is absent in legacy data.
///
/// The boolean reports whether repair was required so adapters can persist the
/// reconstructed envelope without implementing their own missing-evidence
/// interpretation.
pub fn verify_or_repair_snapshot_evidence<S: SnapshotEvidence>(
    stored: SnapshotEvidenceLog<S>,
    expected: S,
) -> Result<(SnapshotEvidenceLog<S>, bool), ReliabilityError> {
    if stored.events().is_empty() {
        return Ok((SnapshotEvidenceLog::baseline(expected)?, true));
    }
    stored.verify_for(&expected)?;
    Ok((stored, false))
}

/// Verifies persisted evidence against materialized state without creating or
/// persisting a legacy baseline. Normal reads should use this boundary;
/// baseline creation belongs to an explicit repair or mutation path.
pub fn verify_snapshot_evidence<S: SnapshotEvidence>(
    stored: &SnapshotEvidenceLog<S>,
    expected: &S,
) -> Result<(), ReliabilityError> {
    if stored.events().is_empty() {
        return Err(ReliabilityError::OperationMismatch);
    }
    stored.verify_for(expected)
}

/// Appends a materialized snapshot to an existing log, or creates its
/// canonical baseline when the log is absent.
pub fn append_or_baseline_snapshot_evidence<S: SnapshotEvidence>(
    mut stored: SnapshotEvidenceLog<S>,
    snapshot: S,
) -> Result<SnapshotEvidenceLog<S>, ReliabilityError> {
    if stored.events().is_empty() {
        return SnapshotEvidenceLog::baseline(snapshot);
    }
    stored.record(snapshot)?;
    Ok(stored)
}

/// Verifies the materialized state before a transition, repairs a missing
/// legacy baseline, and appends the next typed snapshot as one policy.
///
/// The boolean reports whether the returned log includes a reconstructed
/// baseline that the caller must persist alongside the transition.
pub fn verify_and_append_snapshot_transition<S: SnapshotEvidence>(
    stored: SnapshotEvidenceLog<S>,
    expected_before: S,
    after: S,
) -> Result<(SnapshotEvidenceLog<S>, bool), ReliabilityError> {
    let (mut evidence, baseline_was_missing) =
        verify_or_repair_snapshot_evidence(stored, expected_before)?;
    evidence.record(after)?;
    Ok((evidence, baseline_was_missing))
}
