#![deny(unsafe_code)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::must_use_candidate)]

//! Compatibility-preserving reliability primitives for Shardline.
//!
//! The implementation is split by responsibility; this root only declares
//! modules and preserves the public compatibility surface through re-exports.

mod digest;
mod error;
mod event;
mod event_metadata;
mod lifecycle_log;
mod metadata_evidence;
mod oci_evidence;
mod oci_tag_evidence;
mod operation;
mod persisted;
mod provider_evidence;
mod quarantine_evidence;
mod retention_evidence;
mod s3_object_evidence;
mod session_evidence;
mod snapshot_event;
mod snapshot_log;
mod state_snapshot;
mod states;
mod webhook_evidence;

#[cfg(test)]
mod tests;

pub use digest::DigestEncoding;
pub use digest::canonical_state_digest;
pub use error::ReliabilityError;
pub use event::{
    LifecycleEvent, StateTransitionEvent, baseline_resumable_session_events,
    baseline_upload_lifecycle_events, resumable_session_event, upload_lifecycle_event,
    verify_lifecycle_chain, verify_lifecycle_chain_ends_at, verify_state_transition_chain,
    verify_state_transition_chain_ends_at, verify_upload_lifecycle_events,
};
pub use event_metadata::EvidenceEventMetadata;
pub use lifecycle_log::LifecycleEvidenceLog;
pub use metadata_evidence::{
    HubRefEvidenceLog, HubRefLifecycleEvent, HubRefSnapshot, MetadataCommitOperationId,
    verify_hub_ref_events,
};
pub use oci_evidence::{
    OciObjectEvidenceLog, OciObjectIdentity, OciObjectLifecycleEvent, OciObjectLifecycleState,
    OciObjectSnapshot, verify_oci_object_lifecycle_chain, verify_oci_object_lifecycle_events,
};
pub use oci_tag_evidence::{
    OciTagEvidenceLog, OciTagLifecycleEvent, OciTagOperationId, OciTagSnapshot,
    verify_oci_tag_events,
};
pub use operation::{OperationIdentity, OperationKind};
pub use penelope::ContentDigest as PenelopeDigest;
pub use persisted::verify_persisted_event;
pub use provider_evidence::{
    ProviderEvidenceLog, ProviderLifecycleEvent, ProviderLifecycleObservations,
    ProviderLifecycleSnapshot, ProviderRepositoryIdentity, verify_provider_lifecycle_chain,
    verify_provider_lifecycle_events,
};
pub use quarantine_evidence::{
    QuarantineEvidenceLog, QuarantineLifecycleEvent, QuarantineLifecycleState,
    QuarantineObjectIdentity, QuarantineSnapshot, verify_quarantine_lifecycle_chain,
    verify_quarantine_lifecycle_events,
};
pub use retention_evidence::{
    RetentionEvidenceLog, RetentionHoldLifecycleEvent, RetentionHoldLifecycleState,
    RetentionHoldSnapshot, RetentionObjectIdentity, verify_retention_hold_lifecycle_chain,
    verify_retention_hold_lifecycle_events,
};
pub use s3_object_evidence::{
    S3ObjectEvidenceLog, S3ObjectLifecycleEvent, S3ObjectOperationId, S3ObjectSnapshot,
    S3ObjectState, verify_s3_object_events,
};
pub use session_evidence::SessionEvidenceLog;
pub use session_evidence::verify_resumable_session_events;
pub use snapshot_event::SnapshotEvidence;
pub use snapshot_log::SnapshotEvidenceLog;
pub use state_snapshot::DigestSnapshot;
pub use statechronicle::ContentDigest as StateChronicleDigest;
pub use states::{ResumableLifecycleState, UploadLifecycleState};
pub use webhook_evidence::{
    WebhookDeliveryEvidenceLog, WebhookDeliveryIdentity, WebhookDeliveryLifecycleEvent,
    WebhookDeliveryLifecycleState, WebhookDeliveryOperationId, WebhookDeliverySnapshot,
    verify_webhook_delivery_chain, verify_webhook_delivery_events,
};
