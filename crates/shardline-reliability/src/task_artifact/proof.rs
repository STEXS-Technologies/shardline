use super::profile::{ArtifactProfile, MaintenanceArtifact};
use crate::ReliabilityError;
use chrono::{DateTime, Utc};
use ed25519_dalek::{SigningKey, VerifyingKey};
use penelope::core::task::{TaskGeneration, TaskScope};
use serde::{Deserialize, Serialize};
use statechronicle::commit::{
    batch::CommitBatch,
    builder::CommitBuilder,
    roots::compute_state_root,
    sign::{sign_commit, verify_commit},
};
use statechronicle::core::{
    canonicalize::{canonicalize, canonicalize_and_digest},
    digest::ContentDigest,
};
use statechronicle::domain::{
    commit::{Commit, CommitScope, ProfileId},
    event::{Event, StateCommitment},
    ids::{CommitId, EventId, IntentId},
    intent::{Intent, KeyId, Nonce, Operation},
    resource::ResourceId,
    resource_state::ResourceState,
    signed::Signed,
    state_type::StateType,
    subject::SubjectId,
    tenant::TenantId,
};
use statechronicle::profile::StateProfile;
use statechronicle::profiles::{custom::TypedProfile, registry::ProfileRules};
use std::{collections::BTreeMap, sync::LazyLock};

const PROOF_SCHEMA: &str = "shardline.maintenance-artifact-proof.v1";
const EXECUTOR: &str = "service:shardline.maintenance";
static OPERATIONS: LazyLock<[Operation; 1]> = LazyLock::new(|| {
    [Operation(
        "shardline.maintenance.publish-artifact".to_owned(),
    )]
});
// The authenticated local producer signs immutable publications; there is no
// externally submitted intent ingress or separate TrustGrant grant in this port.
static PROFILE: LazyLock<TypedProfile<ArtifactProfile>> =
    LazyLock::new(|| TypedProfile::new(ArtifactProfile, &*OPERATIONS).with_optional_authority());

/// Genuine native Custom-state transition and its signed enclosing commit.
/// Trust the configured producer key, never a key supplied by the proof itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceArtifactProof {
    pub schema: String,
    pub artifact: MaintenanceArtifact,
    pub event: Event,
    pub commit: Signed<Commit>,
}

impl MaintenanceArtifactProof {
    /// Decodes the independently versioned proof within the task blob ceiling.
    /// # Errors
    /// Rejects oversized, malformed or unsupported-schema proof documents.
    pub fn from_json(bytes: &[u8]) -> Result<Self, ReliabilityError> {
        if bytes.len() > 65536 {
            return Err(ReliabilityError::Canonicalize(
                "native task proof exceeds 64 KiB".to_owned(),
            ));
        }
        let proof: Self = serde_json::from_slice(bytes)?;
        if proof.schema != PROOF_SCHEMA {
            return Err(ReliabilityError::StateMismatch);
        }
        Ok(proof)
    }

    /// Encodes a proof within the task blob ceiling, without unbounded buffering.
    /// # Errors
    /// Rejects serialization failures or documents exceeding 64 KiB.
    pub fn to_json(&self) -> Result<Vec<u8>, ReliabilityError> {
        super::json::encode(self)
    }
}

fn failure(error: impl std::fmt::Display) -> ReliabilityError {
    ReliabilityError::Merkle(error.to_string())
}

fn materialize(
    artifact: MaintenanceArtifact,
    created_at: DateTime<Utc>,
) -> Result<(Intent, Event, Commit), ReliabilityError> {
    ArtifactProfile.validate_state(&artifact)?;
    let identity = canonicalize_and_digest(&artifact).map_err(failure)?;
    let suffix = hex::encode(identity.as_bytes());
    let tenant = TenantId(format!(
        "shardline:task-tenant:{}",
        hex::encode(artifact.scope.tenant.digest().0)
    ));
    let resource = ResourceId(format!("shardline:task-artifact:{suffix}"));
    let operation = Operation("shardline.maintenance.publish-artifact".to_owned());
    let actor = SubjectId(EXECUTOR.to_owned());
    let inputs = BTreeMap::from([
        (
            "profile".to_owned(),
            serde_json::json!(ArtifactProfile::DOMAIN_ID),
        ),
        (
            "schema_version".to_owned(),
            serde_json::json!(ArtifactProfile::SCHEMA_VERSION),
        ),
        (
            "command".to_owned(),
            serde_json::json!(canonicalize(&artifact).map_err(failure)?),
        ),
    ]);
    let intent = Intent::builder()
        .tenant(tenant.clone())
        .intent_id(IntentId::new(format!("int_{suffix}")).map_err(failure)?)
        .operation(operation.clone())
        .actor(actor.clone())
        .resource(resource.clone())
        .state_type(StateType::Custom)
        .inputs(inputs.clone())
        .created_at(created_at)
        .nonce(Nonce::from_bytes(identity.as_bytes().to_vec()).map_err(failure)?)
        .build()
        .map_err(failure)?;
    let after = PROFILE.apply(None, &operation, &inputs).map_err(failure)?;
    let ResourceState::Custom(mut unborn) = after.clone() else {
        return Err(ReliabilityError::StateMismatch);
    };
    unborn.payload.clear();
    let before = ResourceState::Custom(unborn);
    let event_identity = canonicalize_and_digest(&(artifact, created_at)).map_err(failure)?;
    let event = Event::new(
        tenant.clone(),
        EventId::new(format!("evt_{}", hex::encode(event_identity.as_bytes()))).map_err(failure)?,
        intent.intent_id.clone(),
        operation,
        resource,
        actor.clone(),
        StateCommitment {
            version: 0,
            state_hash: canonicalize_and_digest(&before).map_err(failure)?,
            state: before,
        },
        StateCommitment {
            version: 1,
            state_hash: canonicalize_and_digest(&after).map_err(failure)?,
            state: after,
        },
        None,
        actor.clone(),
        created_at,
    );
    let mut batch = CommitBatch::new(CommitScope::tenant(tenant));
    batch.add_event(event.clone()).map_err(failure)?;
    let empty = compute_state_root(&[]).map_err(failure)?;
    let body = CommitBuilder::builder()
        .scope(batch.scope().clone())
        .sequence(1)
        .executor(actor)
        .profile(ProfileId::new(ArtifactProfile::DOMAIN_ID.to_owned()).map_err(failure)?)
        .created_at(created_at)
        .build(&batch, ContentDigest::new(*empty.as_bytes()), &[], || {
            CommitId::new(format!("cmt_{}", hex::encode(event_identity.as_bytes())))
                .map_err(Into::into)
        })
        .map_err(failure)?;
    Ok((intent, event, body))
}

/// Signs a bounded native artifact publication using the existing v2 key API.
/// The host must have materialized and independently hashed the immutable bytes.
/// # Errors
/// Rejects invalid artifact state, serialization, commitment or signing failures.
pub fn sign_maintenance_artifact(
    artifact: MaintenanceArtifact,
    created_at: DateTime<Utc>,
    key: &SigningKey,
    key_id: KeyId,
) -> Result<MaintenanceArtifactProof, ReliabilityError> {
    let (_intent, event, body) = materialize(artifact, created_at)?;
    let seed = zeroize::Zeroizing::new(key.to_bytes());
    let protocol_key = statechronicle_ed25519::SigningKey::from_bytes(&seed);
    let commit = sign_commit(&body, &protocol_key, key_id).map_err(failure)?;
    Ok(MaintenanceArtifactProof {
        schema: PROOF_SCHEMA.to_owned(),
        artifact,
        event,
        commit,
    })
}

/// Verifies signer trust, scope/generation, native reduction and complete roots.
/// A valid publication attests immutable bytes; it does not prove task completion,
/// current application freshness or the correctness of the maintenance algorithm.
/// Verify materialized bytes and the durable task record before reconciliation.
/// # Errors
/// Rejects signatures, trusted-key IDs, bindings, native states or roots that differ.
pub fn verify_maintenance_artifact(
    proof: &MaintenanceArtifactProof,
    scope: TaskScope,
    generation: TaskGeneration,
    key: &VerifyingKey,
    key_id: &KeyId,
) -> Result<MaintenanceArtifact, ReliabilityError> {
    proof.to_json()?;
    if proof.schema != PROOF_SCHEMA
        || proof.artifact.scope != scope
        || proof.artifact.generation != generation
        || &proof.commit.signature.key_id != key_id
    {
        return Err(ReliabilityError::OperationMismatch);
    }
    let protocol_key =
        statechronicle_ed25519::VerifyingKey::from_bytes(&key.to_bytes()).map_err(failure)?;
    verify_commit(&proof.commit, &protocol_key).map_err(failure)?;
    let (intent, event, body) = materialize(proof.artifact, proof.event.created_at)?;
    PROFILE
        .verify_transition(None, &intent, &proof.event)
        .map_err(failure)?;
    if proof.event != event || proof.commit.body != body {
        return Err(ReliabilityError::StateDigestMismatch);
    }
    if PROFILE
        .decode_state(&proof.event.after.state)
        .map_err(failure)?
        != proof.artifact
    {
        return Err(ReliabilityError::StateMismatch);
    }
    Ok(proof.artifact)
}
