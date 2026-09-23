use serde_json::Value;

use crate::{
    HubRefLifecycleEvent, LifecycleEvent, OciObjectLifecycleEvent, OciTagLifecycleEvent,
    OperationKind, ProviderLifecycleEvent, QuarantineLifecycleEvent, ReliabilityError,
    ReliabilityMerkleCommit, RepairEvidenceEvent, RetentionHoldLifecycleEvent,
    S3ObjectLifecycleEvent, StateTransitionEvent, WebhookDeliveryLifecycleEvent,
    reliability_merkle_commit_json_with_previous,
};

/// Verifies one persisted reliability event using the canonical domain decoder.
///
/// Database adapters persist a single envelope table, but the event payload is
/// intentionally typed by [`OperationKind`]. Keeping the discriminator-to-event
/// mapping here prevents each adapter or migration from developing its own
/// interpretation of integrity evidence.
pub fn verify_persisted_event(
    operation_kind: OperationKind,
    event_json: Value,
) -> Result<(), ReliabilityError> {
    match operation_kind {
        OperationKind::Upload => verify::<LifecycleEvent>(operation_kind, event_json),
        OperationKind::MetadataCommit => verify::<HubRefLifecycleEvent>(operation_kind, event_json),
        OperationKind::ResumableSession => {
            verify::<StateTransitionEvent>(operation_kind, event_json)
        }
        OperationKind::ProviderEvent => {
            verify::<ProviderLifecycleEvent>(operation_kind, event_json)
        }
        OperationKind::GarbageCollection => {
            verify::<QuarantineLifecycleEvent>(operation_kind, event_json)
        }
        OperationKind::Visibility => verify::<OciObjectLifecycleEvent>(operation_kind, event_json),
        OperationKind::OciTag => verify::<OciTagLifecycleEvent>(operation_kind, event_json),
        OperationKind::S3Object => verify::<S3ObjectLifecycleEvent>(operation_kind, event_json),
        OperationKind::RetentionHold => {
            verify::<RetentionHoldLifecycleEvent>(operation_kind, event_json)
        }
        OperationKind::WebhookDelivery => {
            verify::<WebhookDeliveryLifecycleEvent>(operation_kind, event_json)
        }
        OperationKind::Repair => verify::<RepairEvidenceEvent>(operation_kind, event_json),
    }
}

/// Builds the persisted Merkle commit for a typed envelope selected by its
/// durable operation discriminator.
pub fn build_persisted_merkle_commit(
    operation_kind: OperationKind,
    event_json: Value,
) -> Result<Value, ReliabilityError> {
    build_persisted_merkle_commit_with_previous(operation_kind, event_json, None)
}

/// Builds a persisted Merkle commitment linked to a previous commitment for
/// the same operation, when one exists.
pub fn build_persisted_merkle_commit_with_previous(
    operation_kind: OperationKind,
    event_json: Value,
    previous_json: Option<Value>,
) -> Result<Value, ReliabilityError> {
    let previous = previous_json
        .map(serde_json::from_value::<ReliabilityMerkleCommit>)
        .transpose()?;
    match operation_kind {
        OperationKind::Upload => build::<LifecycleEvent>(event_json, previous.as_ref()),
        OperationKind::MetadataCommit => {
            build::<HubRefLifecycleEvent>(event_json, previous.as_ref())
        }
        OperationKind::ResumableSession => {
            build::<StateTransitionEvent>(event_json, previous.as_ref())
        }
        OperationKind::ProviderEvent => {
            build::<ProviderLifecycleEvent>(event_json, previous.as_ref())
        }
        OperationKind::GarbageCollection => {
            build::<QuarantineLifecycleEvent>(event_json, previous.as_ref())
        }
        OperationKind::Visibility => {
            build::<OciObjectLifecycleEvent>(event_json, previous.as_ref())
        }
        OperationKind::OciTag => build::<OciTagLifecycleEvent>(event_json, previous.as_ref()),
        OperationKind::S3Object => build::<S3ObjectLifecycleEvent>(event_json, previous.as_ref()),
        OperationKind::RetentionHold => {
            build::<RetentionHoldLifecycleEvent>(event_json, previous.as_ref())
        }
        OperationKind::WebhookDelivery => {
            build::<WebhookDeliveryLifecycleEvent>(event_json, previous.as_ref())
        }
        OperationKind::Repair => build::<RepairEvidenceEvent>(event_json, previous.as_ref()),
    }
}

/// Verifies that a persisted Merkle body is the exact StateChronicle
/// commitment derived from its typed Shardline event.
pub fn verify_persisted_merkle_commit(
    operation_kind: OperationKind,
    event_json: Value,
    merkle_commit_json: Option<Value>,
) -> Result<(), ReliabilityError> {
    verify_persisted_merkle_commit_with_previous(
        operation_kind,
        event_json,
        merkle_commit_json,
        None,
    )
}

/// Verifies a persisted Merkle body and its StateChronicle parent link.
pub fn verify_persisted_merkle_commit_with_previous(
    operation_kind: OperationKind,
    event_json: Value,
    merkle_commit_json: Option<Value>,
    previous_json: Option<Value>,
) -> Result<(), ReliabilityError> {
    let observed = merkle_commit_json.ok_or_else(|| {
        ReliabilityError::Merkle(format!(
            "missing persisted Merkle commitment for {}",
            operation_kind.as_str()
        ))
    })?;
    let expected =
        build_persisted_merkle_commit_with_previous(operation_kind, event_json, previous_json)?;
    if observed != expected {
        return Err(ReliabilityError::Merkle(format!(
            "persisted Merkle commitment mismatch for {}",
            operation_kind.as_str()
        )));
    }
    Ok(())
}

fn verify<E>(operation_kind: OperationKind, event_json: Value) -> Result<(), ReliabilityError>
where
    E: serde::de::DeserializeOwned + crate::event_metadata::EvidenceEventMetadata,
{
    let event = serde_json::from_value::<E>(event_json)?;
    if event.operation_identity().kind != operation_kind {
        return Err(ReliabilityError::OperationMismatch);
    }
    event.verify_integrity()
}

fn build<E>(
    event_json: Value,
    previous: Option<&ReliabilityMerkleCommit>,
) -> Result<Value, ReliabilityError>
where
    E: serde::de::DeserializeOwned + crate::event_metadata::EvidenceEventMetadata,
{
    let event = serde_json::from_value::<E>(event_json)?;
    reliability_merkle_commit_json_with_previous(&event, previous)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{OperationIdentity, UploadLifecycleState, event::LifecycleEvidenceEvent};

    #[test]
    fn persisted_verifier_uses_the_declared_operation_kind() {
        let operation =
            OperationIdentity::new("tenant", "repository", "upload-1", OperationKind::Upload)
                .unwrap();
        let event = LifecycleEvidenceEvent::new(
            operation,
            0,
            UploadLifecycleState::Created,
            UploadLifecycleState::Created,
        )
        .unwrap();
        let json = serde_json::to_value(event).unwrap();
        verify_persisted_event(OperationKind::Upload, json.clone()).unwrap();
        assert!(verify_persisted_event(OperationKind::ProviderEvent, json).is_err());
    }

    #[test]
    fn persisted_merkle_builder_uses_the_declared_operation_kind() {
        let operation =
            OperationIdentity::new("tenant", "repository", "upload-2", OperationKind::Upload)
                .unwrap();
        let event = LifecycleEvidenceEvent::new(
            operation,
            0,
            UploadLifecycleState::Created,
            UploadLifecycleState::Created,
        )
        .unwrap();
        let json = serde_json::to_value(event).unwrap();
        let commit = build_persisted_merkle_commit(OperationKind::Upload, json).unwrap();
        assert_eq!(commit["body"]["event_count"], 1);
        assert!(build_persisted_merkle_commit(OperationKind::ProviderEvent, commit).is_err());
    }

    #[test]
    fn persisted_merkle_verifier_requires_the_previous_commitment() {
        let first = crate::upload_lifecycle_event(
            "tenant",
            "repository",
            "upload-chain",
            "object",
            "d".repeat(64),
            UploadLifecycleState::Created,
            UploadLifecycleState::Storing,
        )
        .unwrap();
        let second = crate::upload_lifecycle_event(
            "tenant",
            "repository",
            "upload-chain",
            "object",
            "d".repeat(64),
            UploadLifecycleState::Storing,
            UploadLifecycleState::Stored,
        )
        .unwrap();
        let first_json = serde_json::to_value(&first).unwrap();
        let second_json = serde_json::to_value(&second).unwrap();
        let first_commit =
            build_persisted_merkle_commit(OperationKind::Upload, first_json).unwrap();
        let second_commit = build_persisted_merkle_commit_with_previous(
            OperationKind::Upload,
            second_json.clone(),
            Some(first_commit.clone()),
        )
        .unwrap();

        let chained_commit = second_commit.clone();
        verify_persisted_merkle_commit_with_previous(
            OperationKind::Upload,
            second_json.clone(),
            Some(chained_commit.clone()),
            Some(first_commit),
        )
        .unwrap();
        assert!(
            verify_persisted_merkle_commit(
                OperationKind::Upload,
                second_json,
                Some(chained_commit)
            )
            .is_err()
        );
    }
}
