use serde::Serialize;

use crate::{
    OperationIdentity,
    event::LifecycleEvidenceEvent,
    snapshot_event::{SnapshotEvidence, SnapshotEvidenceEvent},
    states::EvidenceState,
};

/// Common persistence metadata exposed by every authenticated evidence event.
///
/// Storage adapters use this boundary to persist the journal key from the
/// typed event itself rather than carrying a parallel operation kind/id pair.
pub trait EvidenceEventMetadata: Serialize {
    /// Returns the authenticated operation identity for the event.
    fn operation_identity(&self) -> &OperationIdentity;

    /// Returns the authenticated sequence number for the event.
    fn sequence_number(&self) -> u64;
}

impl<S: EvidenceState> EvidenceEventMetadata for LifecycleEvidenceEvent<S> {
    fn operation_identity(&self) -> &OperationIdentity {
        &self.operation
    }

    fn sequence_number(&self) -> u64 {
        self.sequence
    }
}

impl<S: SnapshotEvidence> EvidenceEventMetadata for SnapshotEvidenceEvent<S> {
    fn operation_identity(&self) -> &OperationIdentity {
        &self.operation
    }

    fn sequence_number(&self) -> u64 {
        self.sequence
    }
}
