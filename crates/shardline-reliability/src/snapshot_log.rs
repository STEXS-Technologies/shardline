use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotEvidenceLog<S: SnapshotEvidence> {
    events: Vec<SnapshotEvidenceEvent<S>>,
    head_only: bool,
}

impl<S> Serialize for SnapshotEvidenceLog<S>
where
    S: SnapshotEvidence,
    SnapshotEvidenceEvent<S>: Serialize,
{
    fn serialize<SerializerT>(
        &self,
        serializer: SerializerT,
    ) -> Result<SerializerT::Ok, SerializerT::Error>
    where
        SerializerT: Serializer,
    {
        self.events.serialize(serializer)
    }
}

impl<'de, S> Deserialize<'de> for SnapshotEvidenceLog<S>
where
    S: SnapshotEvidence + Deserialize<'de>,
    SnapshotEvidenceEvent<S>: Deserialize<'de>,
{
    fn deserialize<DeserializerT>(deserializer: DeserializerT) -> Result<Self, DeserializerT::Error>
    where
        DeserializerT: Deserializer<'de>,
    {
        Ok(Self {
            events: Vec::<SnapshotEvidenceEvent<S>>::deserialize(deserializer)?,
            head_only: false,
        })
    }
}

impl<S: SnapshotEvidence> Default for SnapshotEvidenceLog<S> {
    fn default() -> Self {
        Self {
            events: Vec::new(),
            head_only: false,
        }
    }
}

impl<S: SnapshotEvidence> SnapshotEvidenceLog<S> {
    /// Wraps persisted events after validating their complete chain.
    ///
    /// # Errors
    ///
    /// Returns an error when validation, integrity verification, or canonicalization fails.
    pub fn from_events(events: Vec<SnapshotEvidenceEvent<S>>) -> Result<Self, ReliabilityError> {
        verify_snapshot_chain(&events)?;
        Ok(Self {
            events,
            head_only: false,
        })
    }

    /// Wraps one already-persisted head event without loading its historical
    /// prefix. Full-chain verification remains available to fsck and repair.
    ///
    /// # Errors
    ///
    /// Returns an error when the head event's integrity digest is invalid.
    pub fn from_head(event: SnapshotEvidenceEvent<S>) -> Result<Self, ReliabilityError> {
        event.verify_integrity()?;
        Ok(Self {
            events: vec![event],
            head_only: true,
        })
    }

    /// Creates the sequence-zero self-baseline for a materialized snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when validation, integrity verification, or canonicalization fails.
    pub fn baseline(snapshot: S) -> Result<Self, ReliabilityError> {
        Ok(Self {
            events: vec![SnapshotEvidenceEvent::new(0, snapshot.clone(), snapshot)?],
            head_only: false,
        })
    }

    /// Appends one typed snapshot boundary and verifies the complete chain.
    ///
    /// # Errors
    ///
    /// Returns an error when validation, integrity verification, or canonicalization fails.
    pub fn record(&mut self, snapshot: S) -> Result<(), ReliabilityError> {
        let before = self
            .events
            .last()
            .map(|event| event.after.clone())
            .unwrap_or_else(|| snapshot.clone());
        let sequence = self.events.last().map_or(Ok(0), |event| {
            event
                .sequence
                .checked_add(1)
                .ok_or(ReliabilityError::ChainDiscontinuity)
        })?;
        let event = SnapshotEvidenceEvent::new(sequence, before, snapshot)?;
        if self.head_only {
            if self
                .events
                .last()
                .is_some_and(|previous| previous.operation != event.operation)
            {
                return Err(ReliabilityError::OperationMismatch);
            }
            self.events = vec![event];
            return Ok(());
        }
        self.events.push(event);
        let result = verify_snapshot_chain(&self.events);
        if result.is_err() {
            self.events.pop();
        }
        result
    }

    /// Verifies the chain and binds it to the current materialized snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when validation, integrity verification, or canonicalization fails.
    pub fn verify_for(&self, expected: &S) -> Result<(), ReliabilityError> {
        if self.head_only {
            let event = self
                .events
                .last()
                .ok_or(ReliabilityError::OperationMismatch)?;
            return super::snapshot_event::verify_snapshot_event(event, expected);
        }
        verify_snapshot_chain(&self.events)?;
        if self
            .events
            .last()
            .is_some_and(|event| event.after == *expected)
        {
            Ok(())
        } else {
            Err(ReliabilityError::StateMismatch)
        }
    }

    /// Returns the ordered evidence events.
    #[must_use]
    pub fn events(&self) -> &[SnapshotEvidenceEvent<S>] {
        &self.events
    }

    /// Returns whether this log intentionally contains only its durable head.
    #[must_use]
    pub const fn is_head_only(&self) -> bool {
        self.head_only
    }

    #[cfg(test)]
    pub(crate) const fn events_mut(&mut self) -> &mut Vec<SnapshotEvidenceEvent<S>> {
        &mut self.events
    }
}

/// Verifies a persisted snapshot log against the materialized state, or
/// reconstructs its canonical baseline when the log is absent in legacy data.
///
/// The boolean reports whether repair was required so adapters can persist the
/// reconstructed envelope without implementing their own missing-evidence
/// interpretation.
///
/// # Errors
///
/// Returns an error when validation, integrity verification, or canonicalization fails.
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
///
/// # Errors
///
/// Returns an error when validation, integrity verification, or canonicalization fails.
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
///
/// # Errors
///
/// Returns an error when validation, integrity verification, or canonicalization fails.
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
///
/// # Errors
///
/// Returns an error when validation, integrity verification, or canonicalization fails.
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
