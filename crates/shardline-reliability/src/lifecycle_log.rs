use serde::{Deserialize, Serialize};

use crate::{
    ReliabilityError,
    event::{LifecycleEvidenceEvent, verify_evidence_chain},
    states::EvidenceState,
};

/// Canonical append-only container for lifecycle evidence events.
///
/// Every lifecycle state machine uses the same verified event-chain storage
/// semantics; domain-specific code supplies only its typed state and event
/// construction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleEvidenceLog<S: EvidenceState>(Vec<LifecycleEvidenceEvent<S>>);

impl<S: EvidenceState> Default for LifecycleEvidenceLog<S> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<S: EvidenceState> LifecycleEvidenceLog<S> {
    /// Wraps persisted events after validating their complete chain.
    pub fn from_events(events: Vec<LifecycleEvidenceEvent<S>>) -> Result<Self, ReliabilityError> {
        verify_evidence_chain(&events)?;
        Ok(Self(events))
    }

    /// Appends one event and verifies the complete chain.
    pub fn append(&mut self, event: LifecycleEvidenceEvent<S>) -> Result<(), ReliabilityError> {
        self.0.push(event);
        let result = verify_evidence_chain(&self.0);
        if result.is_err() {
            self.0.pop();
        }
        result
    }

    /// Verifies all event digests, identity, ordering, and chain continuity.
    pub fn verify(&self) -> Result<(), ReliabilityError> {
        verify_evidence_chain(&self.0)
    }

    /// Returns the ordered evidence events.
    #[must_use]
    pub fn events(&self) -> &[LifecycleEvidenceEvent<S>] {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn events_mut(&mut self) -> &mut [LifecycleEvidenceEvent<S>] {
        &mut self.0
    }
}
