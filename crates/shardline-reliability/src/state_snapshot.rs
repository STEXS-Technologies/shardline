use serde::{Deserialize, Serialize};

use crate::{OperationIdentity, ReliabilityError, snapshot_event::SnapshotEvidence};

/// Compact authenticated representation of a complete durable state.
///
/// Adapters hash their full typed state with StateChronicle and persist this
/// small snapshot in a shared `SnapshotEvidenceLog`. The adapter can therefore
/// verify large state (part maps, multipart cursors, hash state) without
/// duplicating the StateChronicle/Penelope envelope or storing the full state
/// in every evidence event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestSnapshot {
    pub operation: OperationIdentity,
    pub state_digest: statechronicle::ContentDigest,
}

impl DigestSnapshot {
    #[must_use]
    pub const fn new(
        operation: OperationIdentity,
        state_digest: statechronicle::ContentDigest,
    ) -> Self {
        Self {
            operation,
            state_digest,
        }
    }
}

impl SnapshotEvidence for DigestSnapshot {
    fn evidence_operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        Ok(self.operation.clone())
    }

    fn validate_evidence_transition(&self, after: &Self) -> Result<(), ReliabilityError> {
        if self.operation == after.operation {
            Ok(())
        } else {
            Err(ReliabilityError::OperationMismatch)
        }
    }
}
