use penelope::ContentDigest as PenelopeDigest;
use serde::{Deserialize, Serialize};

use crate::digest::{
    DigestEncoding, canonical_snapshot_digest, canonical_transition_process_digest, process_digest,
    state_digest,
};
use crate::{OperationIdentity, ReliabilityError};

/// Snapshot types that participate in the unified lifecycle evidence
/// protocol. Domain modules provide only identity and transition rules; the
/// StateChronicle/Penelope envelope and chain verifier live here once.
pub trait SnapshotEvidence: Clone + Eq + Serialize {
    fn evidence_operation(&self) -> Result<OperationIdentity, ReliabilityError>;

    fn validate_evidence_operation(
        &self,
        operation: &OperationIdentity,
    ) -> Result<(), ReliabilityError> {
        if &self.evidence_operation()? == operation {
            Ok(())
        } else {
            Err(ReliabilityError::OperationMismatch)
        }
    }

    fn validate_evidence_transition(&self, after: &Self) -> Result<(), ReliabilityError>;
}

/// Canonical evidence for a transition between two complete durable
/// snapshots. Provider, quarantine, and OCI records are aliases of this one
/// implementation, preserving their existing JSON field layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEvidenceEvent<S: SnapshotEvidence> {
    pub operation: OperationIdentity,
    pub sequence: u64,
    pub before: S,
    pub after: S,
    /// Encoding used for the authenticated digests. Missing on legacy JSON
    /// rows, which deserialize as [`DigestEncoding::LegacyJson`].
    #[serde(default)]
    pub digest_encoding: DigestEncoding,
    pub state_digest: statechronicle::ContentDigest,
    pub process_digest: PenelopeDigest,
}

impl<S: SnapshotEvidence> SnapshotEvidenceEvent<S> {
    pub fn new(sequence: u64, before: S, after: S) -> Result<Self, ReliabilityError> {
        before.validate_evidence_transition(&after)?;
        let operation = after.evidence_operation()?;
        let digest_encoding = DigestEncoding::CanonicalBcsV1;
        let state_digest = canonical_snapshot_digest(&after)?;
        let process_digest =
            canonical_transition_process_digest(&operation, sequence, &before, &after)?;
        Ok(Self {
            operation,
            sequence,
            before,
            after,
            digest_encoding,
            state_digest,
            process_digest,
        })
    }

    pub fn verify_integrity(&self) -> Result<(), ReliabilityError> {
        self.before.validate_evidence_transition(&self.after)?;
        self.after.validate_evidence_operation(&self.operation)?;
        if self.state_digest != state_digest(&self.after, self.digest_encoding)? {
            return Err(ReliabilityError::StateDigestMismatch);
        }
        if self.process_digest
            != process_digest(
                &self.operation,
                self.sequence,
                &self.before,
                &self.after,
                self.digest_encoding,
            )?
        {
            return Err(ReliabilityError::ProcessDigestMismatch);
        }
        Ok(())
    }
}

pub fn verify_snapshot_chain<S: SnapshotEvidence>(
    events: &[SnapshotEvidenceEvent<S>],
) -> Result<(), ReliabilityError> {
    let Some(first) = events.first() else {
        return Ok(());
    };
    if first.sequence != 0 || first.before != first.after {
        return Err(ReliabilityError::ChainDiscontinuity);
    }
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
