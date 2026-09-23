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
fn generic_lifecycle_log_is_shared_by_upload_and_resumable_evidence() {
    let upload_events = baseline_upload_lifecycle_events(
        "tenant",
        "repo",
        "op-1",
        "object",
        "hash",
        UploadLifecycleState::Visible,
    )
    .unwrap();
    let upload_log = LifecycleEvidenceLog::from_events(upload_events).unwrap();
    assert_eq!(upload_log.events().len(), 5);
    upload_log.verify().unwrap();

    let session_events = baseline_resumable_session_events(
        "repo",
        "session-1",
        "objects/target",
        ResumableLifecycleState::Completed,
    )
    .unwrap();
    let session_log = LifecycleEvidenceLog::from_events(session_events).unwrap();
    assert_eq!(
        session_log.events().last().unwrap().after,
        ResumableLifecycleState::Completed
    );
    session_log.verify().unwrap();
}

#[test]
fn generic_lifecycle_log_rolls_back_rejected_append() {
    let events = baseline_upload_lifecycle_events(
        "tenant",
        "repo",
        "op-1",
        "object",
        "hash",
        UploadLifecycleState::Storing,
    )
    .unwrap();
    let mut log = LifecycleEvidenceLog::from_events(events).unwrap();
    let original_len = log.events().len();
    let mut invalid = log.events().last().unwrap().clone();
    invalid.sequence = invalid.sequence.saturating_add(1);
    invalid.before = UploadLifecycleState::Visible;
    assert!(log.append(invalid).is_err());
    assert_eq!(log.events().len(), original_len);
    log.verify().unwrap();
}

#[test]
fn lifecycle_evidence_reads_legacy_json_digests_after_canonical_migration() {
    let operation = OperationIdentity::new("tenant", "repo", "legacy-op", OperationKind::Upload)
        .unwrap()
        .with_object_key("object")
        .with_content_sha256("a".repeat(64));
    let before = UploadLifecycleState::Created;
    let after = UploadLifecycleState::Storing;
    let state_digest = statechronicle::core::digest::hash_bytes(after.as_str().as_bytes());
    let process_bytes =
        serde_json::to_vec(&(&operation, 1_u64, before.as_str(), after.as_str())).unwrap();
    let process_digest = penelope::ContentDigest::sha256(&process_bytes);
    let legacy = serde_json::json!({
        "operation": operation,
        "sequence": 1,
        "before": before,
        "after": after,
        "state_digest": state_digest,
        "process_digest": process_digest,
    });
    let decoded: LifecycleEvent = serde_json::from_value(legacy).unwrap();

    assert_eq!(decoded.digest_encoding, DigestEncoding::LegacyJson);
    decoded.verify_integrity().unwrap();
    verify_lifecycle_chain(&[decoded]).unwrap();
}

#[test]
fn new_evidence_uses_canonical_bcs_digests() {
    let event = upload_lifecycle_event(
        "tenant",
        "repo",
        "canonical-op",
        "object",
        "a".repeat(64),
        UploadLifecycleState::Created,
        UploadLifecycleState::Storing,
    )
    .unwrap();

    assert_eq!(event.digest_encoding, DigestEncoding::CanonicalBcsV1);
    let legacy_state =
        statechronicle::core::digest::hash_bytes(UploadLifecycleState::Storing.as_str().as_bytes());
    assert_ne!(event.state_digest, legacy_state);
    event.verify_integrity().unwrap();
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
            | Err(ReliabilityError::InvalidTransition { .. })
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
fn upload_evidence_is_bound_to_identity_and_current_state() {
    let hash = "a".repeat(64);
    let events = baseline_upload_lifecycle_events(
        "tenant",
        "repo",
        "upload-verified",
        "object",
        &hash,
        UploadLifecycleState::Visible,
    )
    .unwrap();

    verify_upload_lifecycle_events(
        &events,
        "tenant",
        "repo",
        "upload-verified",
        "object",
        &hash,
        UploadLifecycleState::Visible,
    )
    .unwrap();
    assert!(matches!(
        verify_upload_lifecycle_events(
            &events,
            "tenant",
            "repo",
            "upload-verified",
            "other-object",
            &hash,
            UploadLifecycleState::Visible,
        ),
        Err(ReliabilityError::OperationMismatch)
    ));
    assert!(matches!(
        verify_upload_lifecycle_events(
            &events,
            "tenant",
            "repo",
            "upload-verified",
            "object",
            &hash,
            UploadLifecycleState::Stored,
        ),
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
fn lifecycle_verifiers_reject_terminal_only_journals() {
    let upload = baseline_upload_lifecycle_events(
        "tenant",
        "repo",
        "upload-partial",
        "object",
        "hash",
        UploadLifecycleState::Visible,
    )
    .unwrap();
    assert!(matches!(
        verify_upload_lifecycle_events(
            upload.get(1..).expect("terminal-only upload suffix"),
            "tenant",
            "repo",
            "upload-partial",
            "object",
            "hash",
            UploadLifecycleState::Visible,
        ),
        Err(ReliabilityError::ChainDiscontinuity)
    ));

    let session = baseline_resumable_session_events(
        "scope",
        "session-partial",
        "object",
        ResumableLifecycleState::Completed,
    )
    .unwrap();
    assert!(matches!(
        verify_resumable_session_events(
            session.get(1..).expect("terminal-only session suffix"),
            "scope",
            "session-partial",
            "object",
            ResumableLifecycleState::Completed,
        ),
        Err(ReliabilityError::ChainDiscontinuity)
    ));
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
        Err(ReliabilityError::StateDigestMismatch)
            | Err(ReliabilityError::ProcessDigestMismatch)
            | Err(ReliabilityError::InvalidTransition { .. })
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

fn provider_snapshot(revision: Option<&str>) -> ProviderLifecycleSnapshot {
    ProviderLifecycleSnapshot::from_parts(
        ProviderRepositoryIdentity::new("github", "team", "repo"),
        ProviderLifecycleObservations::new(
            Some(100),
            revision.map(ToOwned::to_owned).map(|_| 200),
            revision.map(ToOwned::to_owned),
            Some(300),
            Some(400),
            Some(500),
        ),
    )
    .unwrap()
}

#[test]
fn provider_evidence_binds_materialized_snapshot_and_digests() {
    let initial = provider_snapshot(Some("a"));
    let mut evidence = ProviderEvidenceLog::baseline(initial).unwrap();
    evidence.record(provider_snapshot(Some("b"))).unwrap();
    verify_provider_lifecycle_events(evidence.events(), &provider_snapshot(Some("b"))).unwrap();

    let mut tampered = evidence;
    tampered
        .events_mut()
        .get_mut(1)
        .expect("provider evidence has a transition")
        .after
        .last_pushed_revision = Some("forged".to_owned());
    assert!(matches!(
        tampered.verify_for(&provider_snapshot(Some("b"))),
        Err(ReliabilityError::StateDigestMismatch) | Err(ReliabilityError::ProcessDigestMismatch)
    ));
}
