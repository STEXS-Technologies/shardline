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
    let first = StateTransitionEvent::new(
        operation.clone(),
        1,
        ResumableLifecycleState::Active,
        ResumableLifecycleState::Completing,
    )
    .unwrap();
    let second = StateTransitionEvent::new(
        operation,
        2,
        ResumableLifecycleState::Completing,
        ResumableLifecycleState::Completed,
    )
    .unwrap();
    verify_state_transition_chain(&[first.clone(), second]).unwrap();
    let mut tampered = first;
    tampered.before = ResumableLifecycleState::Expired;
    assert!(matches!(
        tampered.verify_integrity(),
        Err(ReliabilityError::ProcessDigestMismatch)
    ));
}

#[test]
fn resumable_evidence_rejects_impossible_transitions() {
    let operation = OperationIdentity::new(
        "scope",
        "resumable",
        "session-invalid",
        OperationKind::ResumableSession,
    )
    .unwrap();
    let result = StateTransitionEvent::new(
        operation,
        1,
        ResumableLifecycleState::Active,
        ResumableLifecycleState::Completed,
    );
    assert!(matches!(
        result,
        Err(ReliabilityError::InvalidTransition { .. })
    ));
}

#[test]
fn resumable_evidence_keeps_existing_lowercase_json_spelling() {
    let event = resumable_session_event(
        "scope",
        "session-json",
        "object",
        1,
        ResumableLifecycleState::Active,
        ResumableLifecycleState::Completing,
    )
    .unwrap();
    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("\"before\":\"active\""));
    assert!(json.contains("\"after\":\"completing\""));
    let decoded: StateTransitionEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, event);
}

#[test]
fn terminal_state_must_match_the_verified_evidence_chain() {
    let events = baseline_upload_lifecycle_events(
        "tenant",
        "repo",
        "upload-terminal",
        "object",
        "hash",
        UploadLifecycleState::Visible,
    )
    .unwrap();
    assert!(verify_lifecycle_chain_ends_at(&events, UploadLifecycleState::Visible).is_ok());
    assert!(matches!(
        verify_lifecycle_chain_ends_at(&events, UploadLifecycleState::Stored),
        Err(ReliabilityError::StateMismatch)
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

#[test]
fn file_backed_session_evidence_is_replayable_and_tamper_evident() {
    let mut evidence = SessionEvidenceLog::new("s3", "session-1", "bucket/key").unwrap();
    evidence
        .record(
            "s3",
            "session-1",
            "bucket/key",
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        )
        .unwrap();
    evidence.verify().unwrap();
    assert_eq!(evidence.events().len(), 2);

    let mut tampered = evidence;
    tampered
        .events_mut()
        .get_mut(1)
        .expect("recorded evidence has baseline and mutation")
        .after = ResumableLifecycleState::Completed;
    assert!(matches!(
        tampered.verify(),
        Err(ReliabilityError::StateDigestMismatch) | Err(ReliabilityError::ProcessDigestMismatch)
    ));
}

#[test]
fn session_evidence_is_bound_to_one_identity() {
    let evidence = SessionEvidenceLog::new("scope", "session", "object").unwrap();

    evidence.verify_for("scope", "session", "object").unwrap();
    assert!(matches!(
        evidence.verify_for("other-scope", "session", "object"),
        Err(ReliabilityError::OperationMismatch)
    ));
    assert!(matches!(
        evidence.verify_for("scope", "other-session", "object"),
        Err(ReliabilityError::OperationMismatch)
    ));
    assert!(matches!(
        evidence.verify_for("scope", "session", "other-object"),
        Err(ReliabilityError::OperationMismatch)
    ));
}
