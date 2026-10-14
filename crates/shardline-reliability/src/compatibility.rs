//! Preserve released Rust type identities while executing the current protocol.

use serde::{Serialize, de::DeserializeOwned};

use crate::ReliabilityError;

/// Convert through the versioned wire representation, never through layout or
/// unchecked casts. An incompatible protocol shape fails before publication.
pub(crate) fn protocol_value<T: Serialize, U: DeserializeOwned>(
    value: &T,
) -> Result<U, ReliabilityError> {
    Ok(serde_json::from_value(serde_json::to_value(value)?)?)
}
