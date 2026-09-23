use penelope::ContentDigest as PenelopeDigest;
use serde::Serialize;
use statechronicle::ContentDigest;

use crate::{OperationIdentity, ReliabilityError};

pub(crate) fn canonical_state_digest(state: &str) -> ContentDigest {
    statechronicle::core::digest::hash_bytes(state.as_bytes())
}

pub(crate) fn canonical_process_digest(
    operation: &OperationIdentity,
    sequence: u64,
    before: &str,
    after: &str,
) -> Result<PenelopeDigest, ReliabilityError> {
    let payload = serde_json::to_vec(&(operation, sequence, before, after))
        .map_err(ReliabilityError::Serialize)?;
    Ok(PenelopeDigest::sha256(&payload))
}

pub(crate) fn canonical_snapshot_digest<T: Serialize>(
    snapshot: &T,
) -> Result<ContentDigest, ReliabilityError> {
    let payload = serde_json::to_vec(snapshot).map_err(ReliabilityError::Serialize)?;
    Ok(statechronicle::core::digest::hash_bytes(&payload))
}

pub(crate) fn canonical_transition_process_digest<T: Serialize>(
    operation: &OperationIdentity,
    sequence: u64,
    before: &T,
    after: &T,
) -> Result<PenelopeDigest, ReliabilityError> {
    let payload = serde_json::to_vec(&(operation, sequence, before, after))
        .map_err(ReliabilityError::Serialize)?;
    Ok(PenelopeDigest::sha256(&payload))
}
