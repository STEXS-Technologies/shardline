use crate::ReliabilityError;
use penelope_core::task::{Digest, TaskGeneration, TaskScope};
use serde::{Deserialize, Serialize};
use statechronicle_profile::StateProfile;

/// Immutable maintenance artifact role; wire tags are pinned by schema one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaintenanceArtifactKind {
    Checkpoint,
    Fsck,
    IndexRebuild,
    GarbageCollection,
    LifecycleRepair,
    StorageMigration,
}

/// Native artifact state. Large reports stay in immutable, digest-addressed files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceArtifact {
    pub schema_version: u32,
    pub scope: TaskScope,
    pub generation: TaskGeneration,
    pub step: u32,
    pub kind: MaintenanceArtifactKind,
    pub digest: Digest,
    pub bytes: u64,
}

impl MaintenanceArtifact {
    /// Verifies immutable materialized bytes with bounded memory.
    /// # Errors
    /// Rejects unavailable readers, length changes or an artifact digest mismatch.
    pub fn verify_materialized(
        &self,
        mut reader: impl std::io::Read,
    ) -> Result<(), ReliabilityError> {
        use sha2::{Digest as _, Sha256};
        let mut hash = Sha256::new();
        let mut count = 0_u64;
        let mut buffer = [0; 16384];
        loop {
            let length = reader
                .read(&mut buffer)
                .map_err(|error| ReliabilityError::Canonicalize(error.to_string()))?;
            if length == 0 {
                break;
            }
            count = count
                .checked_add(
                    u64::try_from(length)
                        .map_err(|error| ReliabilityError::Canonicalize(error.to_string()))?,
                )
                .ok_or(ReliabilityError::StateDigestMismatch)?;
            if count > self.bytes {
                return Err(ReliabilityError::StateDigestMismatch);
            }
            hash.update(
                buffer
                    .get(..length)
                    .ok_or(ReliabilityError::StateDigestMismatch)?,
            );
        }
        if count != self.bytes || Digest(hash.finalize().into()) != self.digest {
            return Err(ReliabilityError::StateDigestMismatch);
        }
        Ok(())
    }
}

pub(super) struct ArtifactProfile;
impl StateProfile for ArtifactProfile {
    type State = MaintenanceArtifact;
    type Command = MaintenanceArtifact;
    type Error = ReliabilityError;
    const DOMAIN_ID: &'static str = "shardline.maintenance-artifact";
    const SCHEMA_VERSION: u32 = 1;
    const MAX_PAYLOAD_BYTES: usize = 1024;
    fn operation(&self, _command: &Self::Command) -> &'static str {
        "shardline.maintenance.publish-artifact"
    }
    fn validate(
        &self,
        current: Option<&Self::State>,
        command: &Self::Command,
    ) -> Result<(), Self::Error> {
        if current.is_some() {
            return Err(ReliabilityError::StateMismatch);
        }
        self.validate_state(command)
    }
    fn apply(
        &self,
        current: Option<&Self::State>,
        command: &Self::Command,
    ) -> Result<Self::State, Self::Error> {
        self.validate(current, command)?;
        Ok(*command)
    }
    fn validate_state(&self, state: &Self::State) -> Result<(), Self::Error> {
        if state.schema_version != 1
            || state.generation.get() == 0
            || state.bytes == 0
            || state.digest == Digest([0; 32])
        {
            return Err(ReliabilityError::StateMismatch);
        }
        Ok(())
    }
}
