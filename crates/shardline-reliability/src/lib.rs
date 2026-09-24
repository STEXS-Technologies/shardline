#![deny(unsafe_code)]

//! Compatibility-preserving reliability primitives for Shardline.
//!
//! The implementation is split by responsibility; this root only declares
//! modules and preserves the public compatibility surface through re-exports.

mod digest;
mod durable;
mod error;
mod event;
mod event_metadata;
mod lifecycle_log;
mod merkle;
mod merkle_journal;
mod metadata_evidence;
mod oci_evidence;
mod oci_tag_evidence;
mod operation;
mod persisted;
mod provider_evidence;
mod quarantine_evidence;
mod repair_evidence;
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
pub use durable::{
    DurableLifecycleStateV1, DurableLifecycleTransitionV1, DurableOperationIdentityV1,
    DurableSnapshotV1, DurableSnapshotV1Encoding,
};
pub use error::ReliabilityError;
pub use event::{
    LifecycleEvent, StateTransitionEvent, baseline_resumable_session_events,
    baseline_upload_lifecycle_events, resumable_session_event, upload_lifecycle_event,
    upload_lifecycle_identity, upload_operation_identity, verify_lifecycle_chain,
    verify_lifecycle_chain_ends_at, verify_state_transition_chain,
    verify_state_transition_chain_ends_at, verify_upload_lifecycle_events,
};
pub use event_metadata::EvidenceEventMetadata;
pub use lifecycle_log::LifecycleEvidenceLog;
pub use merkle::{
    RELIABILITY_MERKLE_SCHEMA_VERSION, ReliabilityMerkleCommit, build_reliability_merkle_commit,
    build_reliability_merkle_commit_with_previous, reliability_merkle_commit_json,
    reliability_merkle_commit_json_with_previous,
};
pub use merkle_journal::{
    PersistedMerkleJournalRecord, build_persisted_merkle_chain,
    build_persisted_merkle_chain_with_previous, build_typed_merkle_chain,
    verify_persisted_merkle_chain, verify_typed_merkle_chain,
};
pub use metadata_evidence::{
    HubRefEvidenceLog, HubRefLifecycleEvent, HubRefSnapshot, MetadataCommitOperationId,
    verify_hub_ref_events,
};
pub use oci_evidence::{
    OciObjectEvidenceLog, OciObjectIdentity, OciObjectLifecycleEvent, OciObjectLifecycleState,
    OciObjectOperationId, OciObjectSnapshot, verify_oci_object_lifecycle_chain,
    verify_oci_object_lifecycle_events,
};
pub use oci_tag_evidence::{
    OciTagEvidenceLog, OciTagLifecycleEvent, OciTagOperationId, OciTagSnapshot,
    verify_oci_tag_events,
};
pub use operation::{
    OperationIdentity, OperationKind, ResumableSessionSnapshotDomain,
    resumable_session_snapshot_identity,
};
pub use penelope_domain::ContentDigest as PenelopeDigest;
pub use persisted::{
    build_persisted_merkle_commit, build_persisted_merkle_commit_with_previous,
    persisted_event_identity, persisted_event_sequence, verify_persisted_event,
    verify_persisted_event_merkle_chain, verify_persisted_event_merkle_chain_with_sequences,
    verify_persisted_merkle_commit, verify_persisted_merkle_commit_with_previous,
};
pub use provider_evidence::{
    ProviderEvidenceLog, ProviderLifecycleEvent, ProviderLifecycleObservations,
    ProviderLifecycleSnapshot, ProviderRepositoryIdentity, ProviderRepositoryOperationId,
    verify_provider_lifecycle_chain, verify_provider_lifecycle_events,
};
pub use quarantine_evidence::{
    QuarantineEvidenceLog, QuarantineLifecycleEvent, QuarantineLifecycleState,
    QuarantineObjectIdentity, QuarantineSnapshot, verify_quarantine_lifecycle_chain,
    verify_quarantine_lifecycle_events,
};
pub use repair_evidence::{RepairEvidenceEvent, RepairSnapshotV1};
pub use retention_evidence::{
    RetentionEvidenceLog, RetentionHoldLifecycleEvent, RetentionHoldLifecycleState,
    RetentionHoldSnapshot, RetentionObjectIdentity, verify_retention_hold_lifecycle_chain,
    verify_retention_hold_lifecycle_events,
};
pub use s3_object_evidence::{
    S3ObjectEvidenceLog, S3ObjectLifecycleEvent, S3ObjectOperationId, S3ObjectSnapshot,
    S3ObjectState, verify_s3_object_events,
};
pub use session_evidence::{
    SessionEvidenceLog, verify_and_append_session_transition, verify_or_repair_session_evidence,
    verify_resumable_session_events, verify_session_evidence,
};
pub use snapshot_event::{SnapshotEvidence, SnapshotEvidenceEvent, verify_snapshot_event};
pub use snapshot_log::{
    SnapshotEvidenceLog, append_or_baseline_snapshot_evidence,
    verify_and_append_snapshot_transition, verify_or_repair_snapshot_evidence,
    verify_snapshot_evidence,
};
pub use state_snapshot::DigestSnapshot;
pub use statechronicle_core::digest::ContentDigest as StateChronicleDigest;
pub use states::{ResumableLifecycleState, UploadLifecycleState};
pub use webhook_evidence::{
    WebhookDeliveryEvidenceLog, WebhookDeliveryIdentity, WebhookDeliveryLifecycleEvent,
    WebhookDeliveryLifecycleState, WebhookDeliveryOperationId, WebhookDeliverySnapshot,
    verify_webhook_delivery_chain, verify_webhook_delivery_events,
};
