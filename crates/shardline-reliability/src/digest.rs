use penelope::ContentDigest as PenelopeDigest;
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
