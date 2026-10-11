use super::{MaintenanceArtifactKind, MaintenanceArtifactProof, verify_maintenance_artifact};
use ed25519_dalek::VerifyingKey;
use penelope_core::task::{Digest, TaskExecutionEvent, TaskRecord};
use penelope_statechronicle::task::{
    CheckpointStateError, CheckpointStateEvidence, CheckpointStateVerifier, bind_checkpoint_state,
};
use statechronicle_domain::intent::KeyId;

/// Verifies real StateChronicle proofs before correlating Penelope checkpoints.
/// Supply bytes from the immutable journal blob and a deployment-trusted key.
pub struct CheckpointArtifactVerifier<'proof> {
    pub bytes: &'proof [u8],
    pub key: &'proof VerifyingKey,
    pub key_id: &'proof KeyId,
}

impl CheckpointStateVerifier for CheckpointArtifactVerifier<'_> {
    fn verify(&self, record: &TaskRecord) -> Result<CheckpointStateEvidence, CheckpointStateError> {
        let checkpoint = match record.event {
            TaskExecutionEvent::CheckpointCommitted { checkpoint }
            | TaskExecutionEvent::CheckpointMigrated { checkpoint, .. } => checkpoint,
            TaskExecutionEvent::IntentCommitted { .. }
            | TaskExecutionEvent::Started
            | TaskExecutionEvent::FaultObserved { .. }
            | TaskExecutionEvent::ReconciliationRequired
            | TaskExecutionEvent::Reconciled { .. }
            | TaskExecutionEvent::RecoveryPlanned { .. }
            | TaskExecutionEvent::Restarted
            | TaskExecutionEvent::Completed { .. }
            | TaskExecutionEvent::RecoveryExhausted => {
                return Err(CheckpointStateError::NotCheckpoint);
            }
        };
        if self.bytes.len() > 65536 {
            return Err(CheckpointStateError::VerificationFailed);
        }
        checkpoint
            .verify(self.bytes, 1)
            .map_err(|_error| CheckpointStateError::VerificationFailed)?;
        let proof = MaintenanceArtifactProof::from_json(self.bytes)
            .map_err(|_error| CheckpointStateError::VerificationFailed)?;
        let artifact = verify_maintenance_artifact(
            &proof,
            record.scope,
            record.generation,
            self.key,
            self.key_id,
        )
        .map_err(|_error| CheckpointStateError::VerificationFailed)?;
        if artifact.kind != MaintenanceArtifactKind::Checkpoint {
            return Err(CheckpointStateError::BindingMismatch);
        }
        bind_checkpoint_state(
            record,
            CheckpointStateEvidence {
                scope: artifact.scope,
                generation: artifact.generation,
                checkpoint,
                state_version: proof.event.after.version,
                state_digest: Digest(*proof.event.after.state_hash.as_bytes()),
                proof_digest: Digest::of(self.bytes),
            },
        )
    }
}
