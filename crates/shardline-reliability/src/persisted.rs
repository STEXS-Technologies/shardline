use serde_json::Value;

use crate::{
    HubRefLifecycleEvent, LifecycleEvent, OciObjectLifecycleEvent, OciTagLifecycleEvent,
    OperationKind, ProviderLifecycleEvent, QuarantineLifecycleEvent, ReliabilityError,
    RetentionHoldLifecycleEvent, S3ObjectLifecycleEvent, StateTransitionEvent,
    WebhookDeliveryLifecycleEvent,
};

/// Verifies one persisted reliability event using the canonical domain decoder.
///
/// Database adapters persist a single envelope table, but the event payload is
/// intentionally typed by [`OperationKind`]. Keeping the discriminator-to-event
/// mapping here prevents each adapter or migration from developing its own
/// interpretation of authenticated evidence.
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
        OperationKind::Repair => Err(ReliabilityError::UnsupportedOperationKind(
            operation_kind.as_str(),
        )),
    }
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
}
