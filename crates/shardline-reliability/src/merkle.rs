//! Released public Merkle types backed by the current StateChronicle engine.
use crate::{
    EvidenceEventMetadata, ReliabilityError, compatibility::protocol_value, runtime_merkle,
};
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use statechronicle_domain_compat::{commit::Commit, event::Event, intent::KeyId, signed::Signed};

pub use runtime_merkle::RELIABILITY_MERKLE_SCHEMA_VERSION;

/// A deterministic StateChronicle commit derived from one Shardline evidence event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReliabilityMerkleCommit {
    /// Stable Shardline envelope schema version.
    #[serde(default)]
    pub schema_version: u8,
    /// Released StateChronicle commit type.
    pub body: Commit,
    /// Released StateChronicle event type.
    pub event: Event,
}

impl ReliabilityMerkleCommit {
    /// Signs with the current engine while preserving the released key and result types.
    ///
    /// # Errors
    /// Returns an error if protocol conversion or signing fails.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "Preserve the released signing API"
    )]
    pub fn sign(
        &self,
        signing_key: &SigningKey,
        key_id: KeyId,
    ) -> Result<Signed<Commit>, ReliabilityError> {
        let current: runtime_merkle::RuntimeReliabilityMerkleCommit = protocol_value(self)?;
        protocol_value(&current.sign(signing_key, protocol_value(&key_id)?)?)
    }
}

/// Builds a deterministic commitment with the current protocol engine.
///
/// # Errors
/// Returns an error if evidence validation or protocol conversion fails.
pub fn build_reliability_merkle_commit<T: EvidenceEventMetadata>(
    event: &T,
) -> Result<ReliabilityMerkleCommit, ReliabilityError> {
    build_reliability_merkle_commit_with_previous(event, None)
}

/// Builds a commitment linked to the preceding operation commitment.
///
/// # Errors
/// Returns an error if validation, sequence checking or protocol conversion fails.
pub fn build_reliability_merkle_commit_with_previous<T: EvidenceEventMetadata>(
    event: &T,
    previous: Option<&ReliabilityMerkleCommit>,
) -> Result<ReliabilityMerkleCommit, ReliabilityError> {
    let current_previous = previous.map(protocol_value).transpose()?;
    protocol_value(
        &runtime_merkle::build_reliability_merkle_commit_with_previous(
            event,
            current_previous.as_ref(),
        )?,
    )
}

/// Serializes a deterministic commitment for a durable evidence row.
///
/// # Errors
/// Returns an error if commitment construction or serialization fails.
pub fn reliability_merkle_commit_json<T: EvidenceEventMetadata>(
    event: &T,
) -> Result<serde_json::Value, ReliabilityError> {
    reliability_merkle_commit_json_with_previous(event, None)
}

/// Serializes a commitment linked to the preceding operation commitment.
///
/// # Errors
/// Returns an error if commitment construction or serialization fails.
pub fn reliability_merkle_commit_json_with_previous<T: EvidenceEventMetadata>(
    event: &T,
    previous: Option<&ReliabilityMerkleCommit>,
) -> Result<serde_json::Value, ReliabilityError> {
    Ok(serde_json::to_value(
        build_reliability_merkle_commit_with_previous(event, previous)?,
    )?)
}
