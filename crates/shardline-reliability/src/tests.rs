#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use crate::snapshot_event::{SnapshotEvidenceEvent, verify_snapshot_chain, verify_snapshot_event};

#[test]
fn operation_kind_persisted_discriminators_round_trip() {
    for kind in [
        OperationKind::Upload,
        OperationKind::ResumableSession,
        OperationKind::MetadataCommit,
        OperationKind::OciTag,
        OperationKind::S3Object,
        OperationKind::Visibility,
        OperationKind::ProviderEvent,
        OperationKind::Repair,
        OperationKind::GarbageCollection,
        OperationKind::RetentionHold,
        OperationKind::WebhookDelivery,
    ] {
        assert_eq!(OperationKind::parse(kind.as_str()), Some(kind));
    }
    assert_eq!(OperationKind::parse("unknown"), None);
}

#[test]
fn lifecycle_preserves_shardline_transitions() {
    assert!(UploadLifecycleState::Created.can_transition_to(UploadLifecycleState::Storing));
    assert!(
        UploadLifecycleState::MetadataCommitted.can_transition_to(UploadLifecycleState::Visible)
    );
    assert!(!UploadLifecycleState::Created.can_transition_to(UploadLifecycleState::Visible));
}

#[test]
fn upload_lifecycle_committed_rank_is_canonical() {
    assert_eq!(UploadLifecycleState::Created.committed_rank(), 0);
    assert_eq!(UploadLifecycleState::Storing.committed_rank(), 1);
    assert_eq!(UploadLifecycleState::Stored.committed_rank(), 2);
    assert_eq!(UploadLifecycleState::MetadataCommitted.committed_rank(), 3);
    assert_eq!(UploadLifecycleState::Visible.committed_rank(), 4);
    assert_eq!(UploadLifecycleState::Failed.committed_rank(), 0);
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
    assert_eq!(
        event.operation,
        upload_operation_identity("tenant", "repo", "op-1", "object", "hash").unwrap()
    );
    assert_eq!(event.state_digest.as_str().len(), 71);
    assert_eq!(event.process_digest.0.len(), 32);
    event.verify_integrity().unwrap();
    verify_lifecycle_chain(std::slice::from_ref(&event)).unwrap();
}

#[test]
fn resumable_snapshot_domains_preserve_persisted_namespaces() {
    let expected = [
        (
            ResumableSessionSnapshotDomain::OciUpload,
            "oci-upload-session",
        ),
        (
            ResumableSessionSnapshotDomain::S3Multipart,
            "s3-multipart-session",
        ),
        (
            ResumableSessionSnapshotDomain::LfsPatch,
            "lfs-patch-session",
        ),
    ];
    for (domain, namespace) in expected {
        let identity =
            resumable_session_snapshot_identity(domain, "scope", "session", "key").unwrap();
        assert_eq!(identity.tenant, namespace);
        assert_eq!(identity.repository, "scope");
        assert_eq!(identity.operation_id, "session");
        assert_eq!(identity.object_key.as_deref(), Some("key"));
        assert_eq!(identity.kind, OperationKind::ResumableSession);
    }
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
fn lifecycle_chain_rejects_sequence_gaps() {
    let operation =
        upload_operation_identity("tenant", "repo", "op-gap", "object", "hash").unwrap();
    let baseline = LifecycleEvent::new(
        operation.clone(),
        0,
        UploadLifecycleState::Created,
        UploadLifecycleState::Created,
    )
    .unwrap();
    let skipped = LifecycleEvent::new(
        operation,
        2,
        UploadLifecycleState::Created,
        UploadLifecycleState::Storing,
    )
    .unwrap();
    assert!(matches!(
        verify_lifecycle_chain(&[baseline, skipped]),
        Err(ReliabilityError::ChainDiscontinuity)
    ));
}

#[test]
fn snapshot_chain_rejects_sequence_gaps() {
    let before = HubRefSnapshot::new("repo", "main", Some("sha-1".to_owned())).unwrap();
    let after = HubRefSnapshot::new("repo", "main", Some("sha-2".to_owned())).unwrap();
    let baseline = SnapshotEvidenceEvent::new(0, before.clone(), before.clone()).unwrap();
    let skipped = SnapshotEvidenceEvent::new(2, before, after).unwrap();
    assert!(matches!(
        verify_snapshot_chain(&[baseline, skipped]),
        Err(ReliabilityError::ChainDiscontinuity)
    ));
}

#[test]
fn latest_snapshot_event_verifier_checks_integrity_and_materialized_state() {
    let before = HubRefSnapshot::new("repo", "main", Some("sha-1".to_owned())).unwrap();
    let after = HubRefSnapshot::new("repo", "main", Some("sha-2".to_owned())).unwrap();
    let event = SnapshotEvidenceEvent::new(1, before, after.clone()).unwrap();
    verify_snapshot_event(&event, &after).unwrap();

    let wrong = HubRefSnapshot::new("repo", "main", Some("sha-3".to_owned())).unwrap();
    assert!(matches!(
        verify_snapshot_event(&event, &wrong),
        Err(ReliabilityError::StateMismatch)
    ));
}

#[test]
fn snapshot_evidence_helpers_share_baseline_repair_and_append_policy() {
    let initial = HubRefSnapshot::new("org/model", "main", Some("sha-1".to_owned())).unwrap();
    let (baseline, repaired) =
        verify_or_repair_snapshot_evidence(SnapshotEvidenceLog::default(), initial.clone())
            .unwrap();
    assert!(repaired);
    assert_eq!(baseline.events().len(), 1);
    baseline.verify_for(&initial).unwrap();

    let next = HubRefSnapshot::new("org/model", "main", Some("sha-2".to_owned())).unwrap();
    let appended = append_or_baseline_snapshot_evidence(baseline, next.clone()).unwrap();
    assert_eq!(appended.events().len(), 2);
    appended.verify_for(&next).unwrap();

    let wrong = HubRefSnapshot::new("org/model", "main", Some("sha-3".to_owned())).unwrap();
    assert!(matches!(
        verify_or_repair_snapshot_evidence(appended, wrong),
        Err(ReliabilityError::StateMismatch)
    ));
}

#[test]
fn snapshot_evidence_rejects_sequence_overflow() {
    let snapshot = HubRefSnapshot::new("repo", "main", None).unwrap();
    let mut log = SnapshotEvidenceLog::baseline(snapshot.clone()).unwrap();
    log.events_mut()[0].sequence = u64::MAX;
    assert!(matches!(
        log.record(snapshot),
        Err(ReliabilityError::ChainDiscontinuity)
    ));
}

#[test]
fn session_evidence_rejects_sequence_overflow() {
    let operation = OperationIdentity::new(
        "resumable-session",
        "scope",
        "session",
        OperationKind::ResumableSession,
    )
    .unwrap()
    .with_object_key("object");
    let event = StateTransitionEvent::new(
        operation,
        u64::MAX,
        ResumableLifecycleState::Active,
        ResumableLifecycleState::Active,
    )
    .unwrap();
    let mut log = SessionEvidenceLog::from_events(vec![event]).unwrap();
    assert!(matches!(
        log.record(
            "scope",
            "session",
            "object",
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Active,
        ),
        Err(ReliabilityError::ChainDiscontinuity)
    ));
}

#[test]
fn snapshot_transition_helper_verifies_before_and_appends_after() {
    let before = HubRefSnapshot::new("repo", "main", Some("sha-1".into())).unwrap();
    let after = HubRefSnapshot::new("repo", "main", Some("sha-2".into())).unwrap();
    let (evidence, baseline_was_missing) = verify_and_append_snapshot_transition(
        SnapshotEvidenceLog::default(),
        before,
        after.clone(),
    )
    .unwrap();
    assert!(baseline_was_missing);
    assert_eq!(evidence.events().len(), 2);
    evidence.verify_for(&after).unwrap();

    let wrong_before = HubRefSnapshot::new("repo", "main", Some("sha-x".into())).unwrap();
    assert!(matches!(
        verify_and_append_snapshot_transition(evidence, wrong_before, after),
        Err(ReliabilityError::StateMismatch)
    ));
}

#[test]
fn session_evidence_helper_shares_legacy_repair_and_identity_policy() {
    let (repaired, was_missing) = verify_or_repair_session_evidence(
        SessionEvidenceLog::default(),
        "scope",
        "session-1",
        "target",
    )
    .unwrap();
    assert!(was_missing);
    assert_eq!(repaired.events().len(), 1);
    repaired.verify_for("scope", "session-1", "target").unwrap();

    assert!(matches!(
        verify_or_repair_session_evidence(repaired, "scope", "session-1", "other-target"),
        Err(ReliabilityError::OperationMismatch)
    ));
}

#[test]
fn session_transition_helper_fences_expected_state_and_repairs_baseline() {
    let (evidence, was_missing) = verify_and_append_session_transition(
        SessionEvidenceLog::default(),
        "scope",
        "session-1",
        "target",
        ResumableLifecycleState::Active,
        ResumableLifecycleState::Completing,
    )
    .unwrap();
    assert!(was_missing);
    assert_eq!(evidence.events().len(), 2);
    assert_eq!(
        evidence.events().last().unwrap().after,
        ResumableLifecycleState::Completing
    );

    assert!(matches!(
        verify_and_append_session_transition(
            evidence,
            "scope",
            "session-1",
            "target",
            ResumableLifecycleState::Active,
            ResumableLifecycleState::Completed,
        ),
        Err(ReliabilityError::StateMismatch)
    ));
}

#[test]
fn lifecycle_evidence_reads_legacy_json_digests_after_canonical_migration() {
    let operation = OperationIdentity::new("tenant", "repo", "legacy-op", OperationKind::Upload)
        .unwrap()
        .with_object_key("object")
        .with_content_sha256("a".repeat(64));
    let before = UploadLifecycleState::Created;
    let after = UploadLifecycleState::Storing;
    let state_digest = statechronicle_core::digest::hash_bytes(after.as_str().as_bytes());
    let process_bytes =
        serde_json::to_vec(&(&operation, 1_u64, before.as_str(), after.as_str())).unwrap();
    let process_digest = penelope_domain::ContentDigest::sha256(&process_bytes);
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
        statechronicle_core::digest::hash_bytes(UploadLifecycleState::Storing.as_str().as_bytes());
    assert_ne!(event.state_digest, legacy_state);
    event.verify_integrity().unwrap();
}

#[test]
fn generic_state_transition_chain_detects_integrity_failures() {
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
fn file_backed_session_evidence_is_replayable_and_integrity_checked() {
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

#[test]
fn provider_operation_id_is_shared_by_typed_and_snapshot_paths() {
    let typed = ProviderRepositoryOperationId::new("github", "team", "repo");
    let snapshot_operation = provider_snapshot(None).evidence_operation().unwrap();

    assert_eq!(typed.as_str(), "github:team:repo");
    assert_eq!(snapshot_operation.operation_id, typed.as_str());
    assert_eq!(snapshot_operation.kind, OperationKind::ProviderEvent);
}
