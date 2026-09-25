use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleEvidenceLog<S: EvidenceState> {
    events: Vec<LifecycleEvidenceEvent<S>>,
    head_only: bool,
}

impl<S> Serialize for LifecycleEvidenceLog<S>
where
    S: EvidenceState,
    LifecycleEvidenceEvent<S>: Serialize,
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

impl<'de, S> Deserialize<'de> for LifecycleEvidenceLog<S>
where
    S: EvidenceState + Deserialize<'de>,
    LifecycleEvidenceEvent<S>: Deserialize<'de>,
{
    fn deserialize<DeserializerT>(deserializer: DeserializerT) -> Result<Self, DeserializerT::Error>
    where
        DeserializerT: Deserializer<'de>,
    {
        Ok(Self {
            events: Vec::<LifecycleEvidenceEvent<S>>::deserialize(deserializer)?,
            head_only: false,
        })
    }
}

impl<S: EvidenceState> Default for LifecycleEvidenceLog<S> {
    fn default() -> Self {
        Self {
            events: Vec::new(),
            head_only: false,
        }
    }
}

impl<S: EvidenceState> LifecycleEvidenceLog<S> {
    /// Wraps persisted events after validating their complete chain.
    ///
    /// # Errors
    ///
    /// Returns an error when validation, integrity verification, or canonicalization fails.
    pub fn from_events(events: Vec<LifecycleEvidenceEvent<S>>) -> Result<Self, ReliabilityError> {
        verify_evidence_chain(&events)?;
        Ok(Self {
            events,
            head_only: false,
        })
    }

    /// Wraps one already-persisted head event without loading its historical
    /// prefix. Full-chain verification remains available to fsck and repair;
    /// normal mutations only need to validate the durable head boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when the head event's integrity digest is invalid.
    pub fn from_head(event: LifecycleEvidenceEvent<S>) -> Result<Self, ReliabilityError> {
        event.verify_integrity()?;
        Ok(Self {
            events: vec![event],
            head_only: true,
        })
    }

    /// Appends one event and verifies the complete chain.
    ///
    /// # Errors
    ///
    /// Returns an error when validation, integrity verification, or canonicalization fails.
    pub fn append(&mut self, event: LifecycleEvidenceEvent<S>) -> Result<(), ReliabilityError> {
        if self.head_only {
            let Some(previous) = self.events.last() else {
                return Err(ReliabilityError::OperationMismatch);
            };
            if event.operation != previous.operation
                || previous.sequence.checked_add(1) != Some(event.sequence)
                || event.before != previous.after
            {
                return Err(ReliabilityError::ChainDiscontinuity);
            }
            event.verify_integrity()?;
            self.events = vec![event];
            return Ok(());
        }
        self.events.push(event);
        let result = verify_evidence_chain(&self.events);
        if result.is_err() {
            self.events.pop();
        }
        result
    }

    /// Verifies all event digests, identity, ordering, and chain continuity.
    ///
    /// # Errors
    ///
    /// Returns an error when validation, integrity verification, or canonicalization fails.
    pub fn verify(&self) -> Result<(), ReliabilityError> {
        if self.head_only {
            self.events
                .last()
                .ok_or(ReliabilityError::OperationMismatch)?
                .verify_integrity()
        } else {
            verify_evidence_chain(&self.events)
        }
    }

    /// Returns the ordered evidence events.
    #[must_use]
    pub fn events(&self) -> &[LifecycleEvidenceEvent<S>] {
        &self.events
    }

    /// Returns whether this log intentionally contains only its durable head.
    #[must_use]
    pub const fn is_head_only(&self) -> bool {
        self.head_only
    }

    #[cfg(test)]
    pub(crate) fn events_mut(&mut self) -> &mut [LifecycleEvidenceEvent<S>] {
        &mut self.events
    }
}
