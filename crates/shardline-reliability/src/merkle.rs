//! StateChronicle Merkle commitments for Shardline reliability events.
//!
//! Every reliability event can be represented as a real StateChronicle commit
//! without changing Shardline's domain state machine. The synthetic typed
//! projection carries the canonical Shardline evidence digest, while the
//! StateChronicle commit carries the event Merkle root and sparse state root.
//! This keeps the two protocols coherent rather than maintaining a second
//! Shardline-specific Merkle interpretation.

use chrono::{DateTime, Utc};
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use statechronicle_commit::sign::sign_commit;
use statechronicle_commit::{
    batch::CommitBatch, builder::CommitBuilder, roots::compute_state_root,
};
use statechronicle_core::{canonicalize::canonicalize_and_digest, digest::ContentDigest};
use statechronicle_domain::{
    commit::{Commit, CommitScope, ProfileId},
    event::{Event, StateCommitment},
    ids::{CommitId, EventId, IntentId},
    intent::{KeyId, Operation},
    resource::ResourceId,
    resource_state::{ResourceState, UniqueAssetState},
    signed::Signed,
    status::Status,
    subject::SubjectId,
    tenant::TenantId,
};

use crate::{EvidenceEventMetadata, ReliabilityError, canonical_state_digest};

const PROFILE_ID: &str = "shardline.reliability.v1";
const EXECUTOR_ID: &str = "service:shardline.reliability";
const STATUS: &str = "evidence_committed";

/// A deterministic StateChronicle commit derived from one Shardline evidence
/// event. The body is independently verifiable through its event Merkle root
/// and sparse state root; signing is an explicit deployment boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReliabilityMerkleCommit {
    /// The StateChronicle commit body.
    pub body: Commit,
    /// The canonical synthetic event covered by `body.event_merkle_root`.
    pub event: Event,
}

impl ReliabilityMerkleCommit {
    /// Signs this commit with a deployment-provided Ed25519 key.
    pub fn sign(
        &self,
        signing_key: &SigningKey,
        key_id: KeyId,
    ) -> Result<Signed<Commit>, ReliabilityError> {
        sign_commit(&self.body, signing_key, key_id)
            .map_err(|error| ReliabilityError::Merkle(error.to_string()))
    }
}

/// Builds a deterministic StateChronicle commit for one typed Shardline event.
pub fn build_reliability_merkle_commit<T: EvidenceEventMetadata>(
    event: &T,
) -> Result<ReliabilityMerkleCommit, ReliabilityError> {
    event.verify_integrity()?;
    let operation = event.operation_identity();
    let event_digest = canonical_state_digest(event)?;
    let state = ResourceState::UniqueAsset(UniqueAssetState {
        owner: SubjectId(format!("shardline:{}", operation.operation_id)),
        status: Status::from_static(STATUS),
        trade_id: Some(String::from(event_digest.as_str())),
    });
    let state_hash = canonicalize_and_digest(&state)
        .map_err(|error| ReliabilityError::Canonicalize(error.to_string()))?;
    let commitment = StateCommitment {
        version: event.sequence_number(),
        state_hash,
        state: state.clone(),
    };
    let event_id = EventId::new(format!("evt_{}", digest_suffix(&event_digest)))
        .map_err(|error| ReliabilityError::Merkle(error.to_string()))?;
    // Evidence verification intentionally does not revalidate legacy object
    // hash spelling. Preserve that existing acceptance surface while still
    // committing the exact persisted identity bytes into the Merkle object.
    let intent_digest = canonicalize_and_digest(operation)
        .map_err(|error| ReliabilityError::Canonicalize(error.to_string()))?;
    let intent_id = IntentId::new(format!("int_{}", digest_suffix(&intent_digest)))
        .map_err(|error| ReliabilityError::Merkle(error.to_string()))?;
    let resource_id = ResourceId(format!(
        "shardline:{}:{}",
        operation.kind.as_str(),
        operation.operation_id
    ));
    let tenant = TenantId(operation.tenant.clone());
    let protocol_event = Event::new(
        tenant.clone(),
        event_id,
        intent_id,
        Operation::new(format!("shardline.reliability.{}", operation.kind.as_str()))
            .map_err(|error| ReliabilityError::Merkle(error.to_string()))?,
        resource_id,
        SubjectId(String::from(EXECUTOR_ID)),
        StateCommitment {
            version: commitment.version,
            state_hash: commitment.state_hash.clone(),
            state: state.clone(),
        },
        commitment,
        None,
        SubjectId(String::from(EXECUTOR_ID)),
        event_timestamp(event.sequence_number()),
    );
    let mut batch = CommitBatch::new(CommitScope::tenant(tenant));
    batch
        .add_event(protocol_event.clone())
        .map_err(|error| ReliabilityError::Merkle(error.to_string()))?;
    let empty_root =
        compute_state_root(&[]).map_err(|error| ReliabilityError::Merkle(error.to_string()))?;
    let previous_state_root = ContentDigest::new(*empty_root.as_bytes());
    let profile = ProfileId::new(String::from(PROFILE_ID))
        .map_err(|error| ReliabilityError::Merkle(error.to_string()))?;
    let builder = CommitBuilder::builder()
        .scope(batch.scope().clone())
        .sequence(event.sequence_number())
        .executor(SubjectId(String::from(EXECUTOR_ID)))
        .profile(profile)
        .created_at(event_timestamp(event.sequence_number()));
    let commit_digest =
        canonical_state_digest(&(operation, event.sequence_number(), event_digest))?;
    let commit_id = CommitId::new(format!("cmt_{}", digest_suffix(&commit_digest)))
        .map_err(|error| ReliabilityError::Merkle(error.to_string()))?;
    let body = builder
        .build(&batch, previous_state_root, &[], || Ok(commit_id))
        .map_err(|error| ReliabilityError::Merkle(error.to_string()))?;
    Ok(ReliabilityMerkleCommit {
        body,
        event: protocol_event,
    })
}

/// Serializes a Merkle commit for one durable evidence row.
pub fn reliability_merkle_commit_json<T: EvidenceEventMetadata>(
    event: &T,
) -> Result<serde_json::Value, ReliabilityError> {
    Ok(serde_json::to_value(build_reliability_merkle_commit(
        event,
    )?)?)
}

fn digest_suffix(digest: &ContentDigest) -> &str {
    digest
        .as_str()
        .strip_prefix("sha256:")
        .unwrap_or(digest.as_str())
}

fn event_timestamp(sequence: u64) -> DateTime<Utc> {
    DateTime::from_timestamp(i64::try_from(sequence).unwrap_or(i64::MAX), 0)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{OperationKind, UploadLifecycleState, upload_lifecycle_event};
    use ed25519_dalek::VerifyingKey;
    use statechronicle_commit::{roots::event_root, sign::verify_commit};

    #[test]
    fn builds_a_real_statechronicle_merkle_commit() {
        let evidence = upload_lifecycle_event(
            "tenant",
            "repo",
            "operation",
            "object",
            "a".repeat(64),
            UploadLifecycleState::Created,
            UploadLifecycleState::Storing,
        )
        .unwrap();
        assert_eq!(evidence.operation.kind, OperationKind::Upload);

        let commit = build_reliability_merkle_commit(&evidence).unwrap();
        assert_eq!(commit.body.event_count, 1);
        assert_eq!(
            commit.body.event_merkle_root,
            event_root(std::slice::from_ref(&commit.event)).unwrap()
        );
        assert!(commit.body.next_state_root != commit.body.previous_state_root);
    }

    #[test]
    fn signed_commit_round_trips_through_statechronicle_verifier() {
        let evidence = upload_lifecycle_event(
            "tenant",
            "repo",
            "operation",
            "object",
            "b".repeat(64),
            UploadLifecycleState::Created,
            UploadLifecycleState::Storing,
        )
        .unwrap();
        let commit = build_reliability_merkle_commit(&evidence).unwrap();
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let key_id = KeyId::new(String::from("shardline-test-key")).unwrap();
        let signed = commit.sign(&signing_key, key_id).unwrap();
        let verifying_key = VerifyingKey::from(&signing_key);
        verify_commit(&signed, &verifying_key).unwrap();
    }
}
