use penelope::ContentDigest as PenelopeDigest;
use serde::{Deserialize, Serialize};
use statechronicle_core::digest::ContentDigest;

use crate::{OperationIdentity, ReliabilityError};

/// Encoding used for integrity-checkable evidence payloads.
///
/// `LegacyJson` is retained solely to verify evidence written before the
/// canonical BCS migration. All newly-created evidence uses BCS.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DigestEncoding {
    /// The pre-migration JSON byte representation.
    #[default]
    LegacyJson,
    /// StateChronicle/Penelope canonical BCS representation.
    CanonicalBcsV1,
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ReliabilityError> {
    statechronicle_core::canonicalize::canonicalize(value)
        .map_err(|error| ReliabilityError::Canonicalize(error.to_string()))
}

fn legacy_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ReliabilityError> {
    Ok(serde_json::to_vec(value)?)
}

pub(crate) fn state_digest<T: Serialize>(
    value: &T,
    encoding: DigestEncoding,
) -> Result<ContentDigest, ReliabilityError> {
    let bytes = match encoding {
        DigestEncoding::LegacyJson => legacy_bytes(value)?,
        DigestEncoding::CanonicalBcsV1 => canonical_bytes(value)?,
    };
    Ok(statechronicle_core::digest::hash_bytes(&bytes))
}

pub(crate) fn legacy_state_label_digest(state: &str) -> ContentDigest {
    statechronicle_core::digest::hash_bytes(state.as_bytes())
}

pub(crate) fn process_digest<T: Serialize, U: Serialize>(
    operation: &OperationIdentity,
    sequence: u64,
    before: &T,
    after: &U,
    encoding: DigestEncoding,
) -> Result<PenelopeDigest, ReliabilityError> {
    let payload = match encoding {
        DigestEncoding::LegacyJson => legacy_bytes(&(operation, sequence, before, after))?,
        DigestEncoding::CanonicalBcsV1 => canonical_bytes(&(operation, sequence, before, after))?,
    };
    Ok(PenelopeDigest::sha256(&payload))
}

pub fn canonical_state_digest<T: Serialize>(state: &T) -> Result<ContentDigest, ReliabilityError> {
    state_digest(state, DigestEncoding::CanonicalBcsV1)
}

pub(crate) fn canonical_process_digest<T: Serialize, U: Serialize>(
    operation: &OperationIdentity,
    sequence: u64,
    before: &T,
    after: &U,
) -> Result<PenelopeDigest, ReliabilityError> {
    process_digest(
        operation,
        sequence,
        before,
        after,
        DigestEncoding::CanonicalBcsV1,
    )
}

pub(crate) fn canonical_snapshot_digest<T: Serialize>(
    snapshot: &T,
) -> Result<ContentDigest, ReliabilityError> {
    canonical_state_digest(snapshot)
}

pub(crate) fn canonical_transition_process_digest<T: Serialize, U: Serialize>(
    operation: &OperationIdentity,
    sequence: u64,
    before: &T,
    after: &U,
) -> Result<PenelopeDigest, ReliabilityError> {
    canonical_process_digest(operation, sequence, before, after)
}
