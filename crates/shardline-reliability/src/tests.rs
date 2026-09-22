#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;

#[test]
fn lifecycle_preserves_shardline_transitions() {
    assert!(UploadLifecycleState::Created.can_transition_to(UploadLifecycleState::Storing));
    assert!(
        UploadLifecycleState::MetadataCommitted.can_transition_to(UploadLifecycleState::Visible)
    );
    assert!(!UploadLifecycleState::Created.can_transition_to(UploadLifecycleState::Visible));
}

#[test]
fn identity_digest_is_stable_and_bounded() {
    let identity = OperationIdentity::new("tenant", "repo", "op-1", OperationKind::Upload)
        .unwrap()
        .with_object_key("chunks/aa/object");
    assert_eq!(
        identity.content_digest().unwrap(),
        identity.content_digest().unwrap()
    );
    assert_eq!(identity.penelope_digest().unwrap().0.len(), 32);
}

#[test]
fn lifecycle_event_correlates_statechronicle_and_penelope_digests() {
    let event = upload_lifecycle_event(
        "tenant",
        "repo",
        "op-1",
        "object",
        "hash",
        UploadLifecycleState::Created,
        UploadLifecycleState::Storing,
    )
    .unwrap();
    assert_eq!(event.state_digest.as_str().len(), 71);
    assert_eq!(event.process_digest.0.len(), 32);
    event.verify_integrity().unwrap();
    verify_lifecycle_chain(std::slice::from_ref(&event)).unwrap();
}

#[test]
fn generic_state_transition_chain_is_tamper_evident() {
    let operation = OperationIdentity::new(
        "tenant",
        "repository",
        "session-1",
        OperationKind::ResumableSession,
    )
    .unwrap()
    .with_object_key("objects/session-1");
    let first = StateTransitionEvent::new(operation.clone(), 1, "active", "completing").unwrap();
    let second = StateTransitionEvent::new(operation, 2, "completing", "completed").unwrap();
    verify_state_transition_chain(&[first.clone(), second]).unwrap();
    let mut tampered = first;
    tampered.before = "tampered".to_owned();
    assert!(matches!(
        tampered.verify_integrity(),
        Err(ReliabilityError::ProcessDigestMismatch)
    ));
}

#[test]
fn resumable_terminal_reuse_is_an_explicit_recovery_transition() {
    assert!(ResumableLifecycleState::Completed.can_transition_to(ResumableLifecycleState::Active));
    assert!(ResumableLifecycleState::Aborted.can_transition_to(ResumableLifecycleState::Active));
    assert!(ResumableLifecycleState::Expired.can_transition_to(ResumableLifecycleState::Active));
    assert!(
        !ResumableLifecycleState::Completing.can_transition_to(ResumableLifecycleState::Active)
    );
}

#[test]
fn baseline_events_are_replayable() {
    let upload = baseline_upload_lifecycle_events(
        "tenant",
        "repo",
        "upload-1",
        "object",
        "hash",
        UploadLifecycleState::Visible,
    )
    .unwrap();
    verify_lifecycle_chain(&upload).unwrap();
    let session = baseline_resumable_session_events(
        "scope",
        "session-1",
        "object",
        ResumableLifecycleState::Completed,
    )
    .unwrap();
    verify_state_transition_chain(&session).unwrap();
}
