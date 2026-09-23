use penelope_domain::ContentDigest as PenelopeDigest;
use serde::{Deserialize, Serialize};

use crate::{
    OperationIdentity, OperationKind, ReliabilityError,
    digest::{canonical_process_digest, canonical_state_digest},
    event_metadata::EvidenceEventMetadata,
};

const REPAIR_SNAPSHOT_SCHEMA: &str = "shardline.reliability.repair-snapshot.v1";

/// Frozen representation of an operator-approved reliability repair target.
///
/// This DTO is deliberately independent of the evolving repair orchestrator
/// types. Its fields and schema identifier are part of the durable digest
/// contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairSnapshotV1 {
    pub schema: String,
    pub subject: String,
    pub action: String,
    pub target: String,
}

impl RepairSnapshotV1 {
    pub fn new(
        subject: impl Into<String>,
        action: impl Into<String>,
        target: impl Into<String>,
    ) -> Result<Self, ReliabilityError> {
        let snapshot = Self {
            schema: REPAIR_SNAPSHOT_SCHEMA.to_owned(),
            subject: subject.into(),
            action: action.into(),
            target: target.into(),
        };
        if snapshot.subject.is_empty() {
            return Err(ReliabilityError::EmptyField("repair subject"));
        }
        if snapshot.action.is_empty() {
            return Err(ReliabilityError::EmptyField("repair action"));
        }
        if snapshot.target.is_empty() {
            return Err(ReliabilityError::EmptyField("repair target"));
        }
        Ok(snapshot)
    }
}

/// A typed, integrity-checked operator repair boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairEvidenceEvent {
    pub operation: OperationIdentity,
    pub sequence: u64,
    pub before: RepairSnapshotV1,
    pub after: RepairSnapshotV1,
    pub state_digest: statechronicle_core::digest::ContentDigest,
    pub process_digest: PenelopeDigest,
}

impl RepairEvidenceEvent {
    pub fn new(
        operation: OperationIdentity,
        sequence: u64,
        before: RepairSnapshotV1,
        after: RepairSnapshotV1,
    ) -> Result<Self, ReliabilityError> {
        if operation.kind != OperationKind::Repair {
            return Err(ReliabilityError::OperationMismatch);
        }
        let state_digest = canonical_state_digest(&after)?;
        let process_digest = canonical_process_digest(&operation, sequence, &before, &after)?;
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
        if self.operation.kind != OperationKind::Repair {
            return Err(ReliabilityError::OperationMismatch);
        }
        if self.state_digest != canonical_state_digest(&self.after)? {
            return Err(ReliabilityError::StateDigestMismatch);
        }
        if self.process_digest
            != canonical_process_digest(&self.operation, self.sequence, &self.before, &self.after)?
        {
            return Err(ReliabilityError::ProcessDigestMismatch);
        }
        Ok(())
    }
}

impl EvidenceEventMetadata for RepairEvidenceEvent {
    fn operation_identity(&self) -> &OperationIdentity {
        &self.operation
    }

    fn sequence_number(&self) -> u64 {
        self.sequence
    }

    fn verify_integrity(&self) -> Result<(), ReliabilityError> {
        Self::verify_integrity(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation() -> OperationIdentity {
        OperationIdentity::new("tenant", "repo", "repair-1", OperationKind::Repair).unwrap()
    }

    #[test]
    fn repair_events_are_verified_by_the_same_metadata_boundary() {
        let before = RepairSnapshotV1::new("operator", "inspect", "object-1").unwrap();
        let after = RepairSnapshotV1::new("operator", "baseline", "object-1").unwrap();
        let event = RepairEvidenceEvent::new(operation(), 0, before, after).unwrap();
        event.verify_integrity().unwrap();
        assert_eq!(event.operation_identity().kind, OperationKind::Repair);
    }
}
