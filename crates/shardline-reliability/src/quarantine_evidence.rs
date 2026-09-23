use penelope::ContentDigest as PenelopeDigest;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuarantineLifecycleEvent {
    pub operation: OperationIdentity,
    pub sequence: u64,
    pub before: QuarantineSnapshot,
    pub after: QuarantineSnapshot,
    pub state_digest: statechronicle::ContentDigest,
    pub process_digest: PenelopeDigest,
}

impl QuarantineLifecycleEvent {
    pub fn new(
        sequence: u64,
        before: QuarantineSnapshot,
        after: QuarantineSnapshot,
    ) -> Result<Self, ReliabilityError> {
        if before.object_key != after.object_key {
            return Err(ReliabilityError::OperationMismatch);
        }
        if !before.state.can_transition_to(after.state) {
            return Err(ReliabilityError::InvalidTransition {
                before: before.state.as_str(),
                after: after.state.as_str(),
            });
        }
        let operation = after.operation()?;
        let state_digest = state_digest(&after)?;
        let process_digest = process_digest(&operation, sequence, &before, &after)?;
        Ok(Self {
            operation,
            sequence,
            before,
            after,
            state_digest,
            process_digest,
        })
    }

    pub fn verify_integrity(&self) -> Result<(), ReliabilityError> {
        if self.operation != self.after.operation()? {
            return Err(ReliabilityError::OperationMismatch);
        }
        if self.state_digest != state_digest(&self.after)? {
            return Err(ReliabilityError::StateDigestMismatch);
        }
        if self.process_digest
            != process_digest(&self.operation, self.sequence, &self.before, &self.after)?
        {
            return Err(ReliabilityError::ProcessDigestMismatch);
        }
        Ok(())
    }
}

pub fn verify_quarantine_lifecycle_chain(
    events: &[QuarantineLifecycleEvent],
) -> Result<(), ReliabilityError> {
    let Some(first) = events.first() else {
        return Ok(());
    };
    let mut previous_after = None;
    let mut previous_sequence = None;
    for event in events {
        event.verify_integrity()?;
        if event.operation != first.operation {
            return Err(ReliabilityError::OperationMismatch);
        }
        if previous_sequence.is_some_and(|sequence| event.sequence <= sequence) {
            return Err(ReliabilityError::SequenceRegression);
        }
        if previous_after.is_some_and(|after| event.before != after) {
            return Err(ReliabilityError::ChainDiscontinuity);
        }
        previous_sequence = Some(event.sequence);
        previous_after = Some(event.after.clone());
    }
    Ok(())
}

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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuarantineEvidenceLog(Vec<QuarantineLifecycleEvent>);

impl QuarantineEvidenceLog {
    pub fn from_events(events: Vec<QuarantineLifecycleEvent>) -> Result<Self, ReliabilityError> {
        verify_quarantine_lifecycle_chain(&events)?;
        Ok(Self(events))
    }

    pub fn baseline(snapshot: QuarantineSnapshot) -> Result<Self, ReliabilityError> {
        Ok(Self(vec![QuarantineLifecycleEvent::new(
            0,
            snapshot.clone(),
            snapshot,
        )?]))
    }

    pub fn record(&mut self, snapshot: QuarantineSnapshot) -> Result<(), ReliabilityError> {
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
            .push(QuarantineLifecycleEvent::new(sequence, before, snapshot)?);
        verify_quarantine_lifecycle_chain(&self.0)
    }

    pub fn verify_for(&self, expected: &QuarantineSnapshot) -> Result<(), ReliabilityError> {
        verify_quarantine_lifecycle_events(&self.0, expected)
    }

    #[must_use]
    pub fn events(&self) -> &[QuarantineLifecycleEvent] {
        &self.0
    }
}

fn state_digest(
    snapshot: &QuarantineSnapshot,
) -> Result<statechronicle::ContentDigest, ReliabilityError> {
    let bytes = serde_json::to_vec(snapshot).map_err(ReliabilityError::Serialize)?;
    Ok(statechronicle::core::digest::hash_bytes(&bytes))
}

fn process_digest(
    operation: &OperationIdentity,
    sequence: u64,
    before: &QuarantineSnapshot,
    after: &QuarantineSnapshot,
) -> Result<PenelopeDigest, ReliabilityError> {
    let bytes = serde_json::to_vec(&(operation, sequence, before, after))
        .map_err(ReliabilityError::Serialize)?;
    Ok(PenelopeDigest::sha256(&bytes))
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
}
