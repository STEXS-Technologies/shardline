use serde::Serialize;

use crate::{
    OperationIdentity, ReliabilityError,
    event::LifecycleEvidenceEvent,
    snapshot_event::{SnapshotEvidence, SnapshotEvidenceEvent},
    states::EvidenceState,
};

/// Common persistence metadata exposed by every integrity-checkable evidence event.
///
/// Storage adapters use this boundary to persist the journal key from the
/// typed event itself rather than carrying a parallel operation kind/id pair.
pub trait EvidenceEventMetadata: Serialize {
    /// Returns the integrity-bound operation identity for the event.
    fn operation_identity(&self) -> &OperationIdentity;

    /// Returns the integrity-bound sequence number for the event.
    fn sequence_number(&self) -> u64;

    /// Verifies the event before a storage adapter commits it.
    ///
    /// # Errors
    ///
    /// Returns an error when validation, integrity verification, or canonicalization fails.
    fn verify_integrity(&self) -> Result<(), ReliabilityError>;
}

impl<S: EvidenceState> EvidenceEventMetadata for LifecycleEvidenceEvent<S> {
    fn operation_identity(&self) -> &OperationIdentity {
        &self.operation
    }

    fn sequence_number(&self) -> u64 {
        self.sequence
    }

    fn verify_integrity(&self) -> Result<(), ReliabilityError> {
        LifecycleEvidenceEvent::verify_integrity(self)
    }
}

impl<S: SnapshotEvidence> EvidenceEventMetadata for SnapshotEvidenceEvent<S> {
    fn operation_identity(&self) -> &OperationIdentity {
        &self.operation
    }

    fn sequence_number(&self) -> u64 {
        self.sequence
    }

    fn verify_integrity(&self) -> Result<(), ReliabilityError> {
        SnapshotEvidenceEvent::verify_integrity(self)
    }
}
