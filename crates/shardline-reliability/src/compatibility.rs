//! Preserve released Rust type identities while executing the current protocol.

use ed25519_dalek::VerifyingKey;
use serde::{Serialize, de::DeserializeOwned};
use statechronicle_domain_compat::{commit::Commit, signed::Signed};

use crate::ReliabilityError;

/// Convert through the versioned wire representation, never through layout or
/// unchecked casts. An incompatible protocol shape fails before publication.
pub(crate) fn protocol_value<T: Serialize, U: DeserializeOwned>(
    value: &T,
) -> Result<U, ReliabilityError> {
    Ok(serde_json::from_value(serde_json::to_value(value)?)?)
}

/// Verifies a released StateChronicle commit signature through the current protocol types.
///
/// # Errors
/// Returns an error when protocol conversion or signature verification fails.
pub(crate) fn verify_commit_signature(
    signed_commit: &Signed<Commit>,
    verifying_key: &VerifyingKey,
) -> Result<(), ReliabilityError> {
    let current_signed: statechronicle::domain::signed::Signed<
        statechronicle::domain::commit::Commit,
    > = protocol_value(signed_commit)?;
    let protocol_key = statechronicle_ed25519::VerifyingKey::from_bytes(&verifying_key.to_bytes())
        .map_err(|error| ReliabilityError::Merkle(error.to_string()))?;
    statechronicle::commit::sign::verify_commit(&current_signed, &protocol_key)
        .map_err(|error| ReliabilityError::Merkle(error.to_string()))
}
