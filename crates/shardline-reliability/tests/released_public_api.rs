//! These annotations deliberately use the exact upstream types released by 1.11.3.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use ed25519_dalek::SigningKey;
use shardline_reliability::{
    DigestSnapshot, OperationIdentity, OperationKind, PenelopeDigest, ReliabilityMerkleCommit,
    StateChronicleDigest, UploadLifecycleState, build_reliability_merkle_commit,
    canonical_state_digest, upload_lifecycle_event,
};
use statechronicle_domain_compat::{commit::Commit, event::Event, intent::KeyId, signed::Signed};

#[test]
fn released_types_compile_and_current_engine_signatures_verify_with_released_verifier() {
    let evidence = upload_lifecycle_event(
        "tenant",
        "repository",
        "operation",
        "object",
        "a".repeat(64),
        UploadLifecycleState::Created,
        UploadLifecycleState::Storing,
    )
    .unwrap();
    let state: statechronicle_core_compat::digest::ContentDigest = evidence.state_digest.clone();
    let process: penelope_domain_compat::ContentDigest = evidence.process_digest;
    let state: StateChronicleDigest = state;
    let process: PenelopeDigest = process;
    assert_eq!(process, evidence.process_digest);
    let snapshot = DigestSnapshot::new(evidence.operation.clone(), state);
    let _: statechronicle_core_compat::digest::ContentDigest = snapshot.state_digest;

    let commit: ReliabilityMerkleCommit = build_reliability_merkle_commit(&evidence).unwrap();
    let _: &Commit = &commit.body;
    let _: &Event = &commit.event;
    let key = SigningKey::from_bytes(&[17; 32]);
    let signed: Signed<Commit> = commit
        .sign(&key, KeyId::new("compat-key".into()).unwrap())
        .unwrap();
    statechronicle_commit_compat::sign::verify_commit(&signed, &key.verifying_key()).unwrap();

    let runtime_signed: statechronicle_domain::signed::Signed<
        statechronicle_domain::commit::Commit,
    > = serde_json::from_value(serde_json::to_value(&signed).unwrap()).unwrap();
    let runtime_key =
        statechronicle_ed25519::VerifyingKey::from_bytes(&key.verifying_key().to_bytes()).unwrap();
    statechronicle_commit::sign::verify_commit(&runtime_signed, &runtime_key).unwrap();
}

#[test]
fn digest_bridge_preserves_current_canonical_bytes_and_released_return_types() {
    let operation =
        OperationIdentity::new("tenant", "repo", "operation", OperationKind::Upload).unwrap();
    let released: statechronicle_core_compat::digest::ContentDigest =
        canonical_state_digest(&operation).unwrap();
    let current = statechronicle_core::canonicalize::canonicalize_and_digest(&operation).unwrap();
    assert_eq!(released.as_str(), current.as_str());
    let _: statechronicle_core_compat::digest::ContentDigest = operation.content_digest().unwrap();
    let _: penelope_domain_compat::ContentDigest = operation.penelope_digest().unwrap();
}
