//! Shared persisted Merkle journal records and chain operations.
//!
//! Filesystem adapters use this envelope so they do not invent independent
//! interpretations of StateChronicle commitments. The event payloads remain
//! the authoritative typed reliability events; the commitments are verified
//! against those exact payloads and their sequence-linked predecessor.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    EvidenceEventMetadata, OperationKind, ReliabilityError, ReliabilityMerkleCommit,
    build_persisted_merkle_commit_with_previous, persisted_event_sequence,
    reliability_merkle_commit_json_with_previous,
};
use serde::de::DeserializeOwned;

/// One append-only filesystem Merkle journal record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedMerkleJournalRecord {
    /// Resumable lifecycle events and their StateChronicle commitments.
    #[serde(default)]
    pub evidence: Vec<Value>,
    #[serde(default)]
    pub merkle_commits: Vec<Value>,
    /// Complete snapshot events and their StateChronicle commitments.
    #[serde(default)]
    pub snapshot_evidence: Vec<Value>,
    #[serde(default)]
    pub snapshot_merkle_commits: Vec<Value>,
}

/// Builds a sequence of linked commitments from typed persisted event JSON.
pub fn build_persisted_merkle_chain(
    operation_kind: OperationKind,
    events: &[Value],
) -> Result<Vec<Value>, ReliabilityError> {
    build_persisted_merkle_chain_with_previous(operation_kind, events, None)
}

/// Builds commitments for a newly appended suffix, linked to the prior head.
pub fn build_persisted_merkle_chain_with_previous(
    operation_kind: OperationKind,
    events: &[Value],
    previous: Option<&Value>,
) -> Result<Vec<Value>, ReliabilityError> {
    let mut previous = previous.cloned();
    let mut result = Vec::with_capacity(events.len());
    let mut expected_sequence = match previous.as_ref() {
        Some(commit) => Some(
            persisted_commit_sequence(commit)?
                .checked_add(1)
                .ok_or_else(|| ReliabilityError::Merkle("Merkle sequence overflow".into()))?,
        ),
        None => None,
    };

    for event in events {
        let sequence = persisted_event_sequence(operation_kind, event.clone())?;
        if expected_sequence.is_some_and(|expected| expected != sequence) {
            return Err(ReliabilityError::ChainDiscontinuity);
        }
        let commit = build_persisted_merkle_commit_with_previous(
            operation_kind,
            event.clone(),
            previous.clone(),
        )?;
        expected_sequence = Some(
            sequence
                .checked_add(1)
                .ok_or_else(|| ReliabilityError::Merkle("Merkle sequence overflow".into()))?,
        );
        previous = Some(commit.clone());
        result.push(commit);
    }
    Ok(result)
}

/// Verifies an entire persisted event/commit chain, including exact event
/// coverage, sequence continuity, and StateChronicle parent links.
pub fn verify_persisted_merkle_chain(
    operation_kind: OperationKind,
    events: &[Value],
    merkle_commits: &[Value],
) -> Result<(), ReliabilityError> {
    if events.len() != merkle_commits.len() {
        return Err(ReliabilityError::Merkle(
            "persisted Merkle event and commitment counts differ".into(),
        ));
    }
    for event in events {
        persisted_event_sequence(operation_kind, event.clone())?;
    }
    let commitments = merkle_commits.iter().cloned().map(Some).collect::<Vec<_>>();
    crate::verify_persisted_event_merkle_chain(operation_kind, events, &commitments)
}

/// Builds a Merkle chain for any typed reliability event, including complete
/// snapshot events whose operation kind intentionally shares a lifecycle
/// namespace but has a distinct durable JSON shape.
pub fn build_typed_merkle_chain<T: DeserializeOwned + EvidenceEventMetadata>(
    events: &[Value],
    previous: Option<&Value>,
) -> Result<Vec<Value>, ReliabilityError> {
    let mut previous = previous.cloned();
    let mut expected_sequence = match previous.as_ref() {
        Some(commit) => Some(
            persisted_commit_sequence(commit)?
                .checked_add(1)
                .ok_or_else(|| ReliabilityError::Merkle("Merkle sequence overflow".into()))?,
        ),
        None => None,
    };
    let mut result = Vec::with_capacity(events.len());
    for event_json in events {
        let event = serde_json::from_value::<T>(event_json.clone())?;
        let sequence = event.sequence_number();
        if expected_sequence.is_some_and(|expected| expected != sequence) {
            return Err(ReliabilityError::ChainDiscontinuity);
        }
        let previous_commit = previous
            .as_ref()
            .map(|value| serde_json::from_value::<ReliabilityMerkleCommit>(value.clone()))
            .transpose()?;
        let commit =
            reliability_merkle_commit_json_with_previous(&event, previous_commit.as_ref())?;
        expected_sequence = Some(
            sequence
                .checked_add(1)
                .ok_or_else(|| ReliabilityError::Merkle("Merkle sequence overflow".into()))?,
        );
        previous = Some(commit.clone());
        result.push(commit);
    }
    Ok(result)
}

/// Verifies a Merkle chain for any typed reliability event.
pub fn verify_typed_merkle_chain<T: DeserializeOwned + EvidenceEventMetadata>(
    events: &[Value],
    merkle_commits: &[Value],
) -> Result<(), ReliabilityError> {
    if events.len() != merkle_commits.len() {
        return Err(ReliabilityError::Merkle(
            "persisted Merkle event and commitment counts differ".into(),
        ));
    }
    let expected = build_typed_merkle_chain::<T>(events, None)?;
    for (observed, expected_json) in merkle_commits.iter().zip(expected) {
        let observed = serde_json::from_value::<ReliabilityMerkleCommit>(observed.clone())?;
        let expected = serde_json::from_value::<ReliabilityMerkleCommit>(expected_json)?;
        if observed.schema_version > crate::RELIABILITY_MERKLE_SCHEMA_VERSION
            || observed.body != expected.body
            || observed.event != expected.event
        {
            return Err(ReliabilityError::Merkle(
                "persisted Merkle commitment mismatch".into(),
            ));
        }
    }
    Ok(())
}

fn persisted_commit_sequence(commit: &Value) -> Result<u64, ReliabilityError> {
    commit
        .get("body")
        .and_then(|body| body.get("sequence"))
        .and_then(Value::as_u64)
        .ok_or_else(|| ReliabilityError::Merkle("persisted Merkle sequence is missing".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OperationKind, UploadLifecycleState, upload_lifecycle_event};

    #[test]
    fn persisted_merkle_chain_uses_the_declared_event_kind() {
        let first = upload_lifecycle_event(
            "tenant",
            "repository",
            "upload-merkle-journal",
            "object",
            "f".repeat(64),
            UploadLifecycleState::Created,
            UploadLifecycleState::Storing,
        )
        .unwrap();
        let event = serde_json::to_value(&first).unwrap();
        let commit =
            crate::build_persisted_merkle_commit(OperationKind::Upload, event.clone()).unwrap();

        verify_persisted_merkle_chain(OperationKind::Upload, &[event], &[commit]).unwrap();
    }
}
