#![cfg(feature = "task-journal")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::wildcard_enum_match_arm
)]
use chrono::{DateTime, Utc};
use ed25519_dalek::SigningKey;
use penelope_core::task::{
    BlobRef, Digest, TaskDefinitionFingerprint, TaskExecutionEvent, TaskGeneration, TaskId,
    TaskInputDigest, TaskRecord, TaskScope, TaskSequence, TaskTenantId, TaskTick,
};
use penelope_statechronicle::task::{CheckpointStateError, CheckpointStateVerifier};
use shardline_reliability::{
    CheckpointArtifactVerifier, MaintenanceArtifact, MaintenanceArtifactKind,
    MaintenanceArtifactProof, sign_maintenance_artifact, verify_maintenance_artifact,
};
use statechronicle_domain::{intent::KeyId, resource_state::ResourceState};

fn fixture(kind: MaintenanceArtifactKind) -> (MaintenanceArtifact, SigningKey, KeyId) {
    (
        MaintenanceArtifact {
            schema_version: 1,
            scope: TaskScope {
                tenant: TaskTenantId::from_digest(Digest::of(b"tenant")),
                task: TaskId::from_digest(Digest::of(b"task")),
                definition: TaskDefinitionFingerprint::from_digest(Digest::of(b"definition")),
                input: TaskInputDigest::from_digest(Digest::of(b"input")),
            },
            generation: TaskGeneration::new(1),
            step: 1,
            kind,
            digest: Digest::of(b"report"),
            bytes: 6,
        },
        SigningKey::from_bytes(&[7; 32]),
        KeyId("maintenance-fixture-key".to_owned()),
    )
}

fn signed(kind: MaintenanceArtifactKind) -> (MaintenanceArtifactProof, SigningKey, KeyId) {
    let (artifact, key, key_id) = fixture(kind);
    let timestamp: DateTime<Utc> = "2026-10-11T00:00:00Z".parse().unwrap();
    (
        sign_maintenance_artifact(artifact, timestamp, &key, key_id.clone()).unwrap(),
        key,
        key_id,
    )
}

#[test]
fn frozen_native_artifact_schema_remains_verifiable() {
    let proof = MaintenanceArtifactProof::from_json(include_bytes!(
        "fixtures/maintenance-artifact-v1.json"
    ))
    .unwrap();
    let (expected, key, key_id) = fixture(MaintenanceArtifactKind::Checkpoint);
    assert_eq!(
        verify_maintenance_artifact(
            &proof,
            expected.scope,
            expected.generation,
            &key.verifying_key(),
            &key_id
        )
        .unwrap(),
        expected
    );
    expected.verify_materialized(b"report".as_slice()).unwrap();
    assert_eq!(signed(MaintenanceArtifactKind::Checkpoint).0, proof);
}

#[test]
fn native_publication_contains_real_custom_state_and_verified_event_state_roots() {
    let (proof, key, key_id) = signed(MaintenanceArtifactKind::Fsck);
    let artifact = verify_maintenance_artifact(
        &proof,
        proof.artifact.scope,
        TaskGeneration::new(1),
        &key.verifying_key(),
        &key_id,
    )
    .unwrap();
    assert_eq!(artifact, proof.artifact);
    assert!(matches!(proof.event.after.state, ResourceState::Custom(_)));
    assert_eq!(proof.event.before.version, 0);
    assert_eq!(proof.event.after.version, 1);
    assert_eq!(
        proof.commit.body.profile.as_str(),
        "shardline.maintenance-artifact"
    );
    artifact.verify_materialized(b"report".as_slice()).unwrap();
    assert!(artifact.verify_materialized(b"wrong!".as_slice()).is_err());
    assert!(
        artifact
            .verify_materialized(b"report plus extra".as_slice())
            .is_err()
    );
    assert!(artifact.verify_materialized(b"short".as_slice()).is_err());
    assert_eq!(
        MaintenanceArtifactProof::from_json(&proof.to_json().unwrap()).unwrap(),
        proof
    );
}

#[test]
fn forged_roots_are_rejected_even_with_a_genuine_signature_over_the_forged_commit() {
    let (mut proof, key, key_id) = signed(MaintenanceArtifactKind::Fsck);
    proof.commit.body.next_state_root = statechronicle_core::digest::hash_bytes(b"forged state");
    let seed = zeroize::Zeroizing::new(key.to_bytes());
    let protocol_key = statechronicle_ed25519::SigningKey::from_bytes(&seed);
    proof.commit =
        statechronicle_commit::sign::sign_commit(&proof.commit.body, &protocol_key, key_id.clone())
            .unwrap();
    assert!(
        verify_maintenance_artifact(
            &proof,
            proof.artifact.scope,
            proof.artifact.generation,
            &key.verifying_key(),
            &key_id
        )
        .is_err()
    );
}

#[test]
fn trusted_key_scope_generation_and_native_payload_are_all_bound() {
    let (proof, key, key_id) = signed(MaintenanceArtifactKind::Fsck);
    let mut foreign = proof.artifact.scope;
    foreign.tenant = TaskTenantId::from_digest(Digest::of(b"other"));
    assert!(
        verify_maintenance_artifact(
            &proof,
            foreign,
            proof.artifact.generation,
            &key.verifying_key(),
            &key_id
        )
        .is_err()
    );
    assert!(
        verify_maintenance_artifact(
            &proof,
            proof.artifact.scope,
            TaskGeneration::new(2),
            &key.verifying_key(),
            &key_id
        )
        .is_err()
    );
    assert!(
        verify_maintenance_artifact(
            &proof,
            proof.artifact.scope,
            proof.artifact.generation,
            &SigningKey::from_bytes(&[8; 32]).verifying_key(),
            &key_id
        )
        .is_err()
    );
    assert!(
        verify_maintenance_artifact(
            &proof,
            proof.artifact.scope,
            proof.artifact.generation,
            &key.verifying_key(),
            &KeyId("other-key".to_owned())
        )
        .is_err()
    );
    let mut tampered = proof.clone();
    tampered.artifact.digest = Digest::of(b"rewritten artifact");
    assert!(
        verify_maintenance_artifact(
            &tampered,
            proof.artifact.scope,
            proof.artifact.generation,
            &key.verifying_key(),
            &key_id
        )
        .is_err()
    );
    let mut mutated_state = proof;
    if let ResourceState::Custom(custom) = &mut mutated_state.event.after.state {
        custom.schema_version = 2;
    }
    assert!(
        verify_maintenance_artifact(
            &mutated_state,
            mutated_state.artifact.scope,
            mutated_state.artifact.generation,
            &key.verifying_key(),
            &key_id
        )
        .is_err()
    );
}

#[test]
fn checkpoint_adapter_verifies_proof_before_correlating_exact_blob_and_record() {
    let (proof, key, key_id) = signed(MaintenanceArtifactKind::Checkpoint);
    let bytes = proof.to_json().unwrap();
    let checkpoint = BlobRef {
        digest: Digest::of(&bytes),
        bytes: u32::try_from(bytes.len()).unwrap(),
        schema: 1,
    };
    let record = TaskRecord {
        version: 1,
        scope: proof.artifact.scope,
        generation: proof.artifact.generation,
        sequence: TaskSequence::new(2),
        tick: TaskTick::new(5),
        event: TaskExecutionEvent::CheckpointCommitted { checkpoint },
    };
    let public_key = key.verifying_key();
    let verifier = CheckpointArtifactVerifier {
        bytes: &bytes,
        key: &public_key,
        key_id: &key_id,
    };
    let evidence = verifier.verify(&record).unwrap();
    assert_eq!(evidence.checkpoint, checkpoint);
    assert_eq!(evidence.state_version, 1);
    assert_eq!(
        evidence.state_digest,
        Digest(*proof.event.after.state_hash.as_bytes())
    );
    let mut foreign = record;
    foreign.generation = TaskGeneration::new(2);
    assert!(verifier.verify(&foreign).is_err());
    let mut not_checkpoint = record;
    not_checkpoint.event = TaskExecutionEvent::Started;
    assert_eq!(
        verifier.verify(&not_checkpoint),
        Err(CheckpointStateError::NotCheckpoint)
    );
    let mut wrong_blob = record;
    wrong_blob.event = TaskExecutionEvent::CheckpointCommitted {
        checkpoint: BlobRef {
            digest: Digest::of(b"other"),
            ..checkpoint
        },
    };
    assert!(verifier.verify(&wrong_blob).is_err());
}

#[test]
fn oversized_documents_and_unsupported_artifact_schemas_fail_closed() {
    let (mut proof, key, key_id) = signed(MaintenanceArtifactKind::Fsck);
    assert!(MaintenanceArtifactProof::from_json(&vec![b' '; 65537]).is_err());
    proof.event.actor.0 = "x".repeat(65537);
    assert!(proof.to_json().is_err());
    assert!(
        verify_maintenance_artifact(
            &proof,
            proof.artifact.scope,
            proof.artifact.generation,
            &key.verifying_key(),
            &key_id
        )
        .is_err()
    );
    let (mut artifact, schema_key, schema_key_id) = fixture(MaintenanceArtifactKind::Fsck);
    artifact.schema_version = 2;
    assert!(
        sign_maintenance_artifact(
            artifact,
            "2026-10-11T00:00:00Z".parse().unwrap(),
            &schema_key,
            schema_key_id
        )
        .is_err()
    );
}
