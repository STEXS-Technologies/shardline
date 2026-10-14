#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use ed25519_dalek::{Signature, Verifier};
use serde::Deserialize;
use serde_json::Value;
use shardline_reliability::{
    LifecycleEvent, OperationKind, ReliabilityMerkleCommit, StateTransitionEvent,
    build_persisted_merkle_commit_with_previous, persisted_event_sequence, verify_lifecycle_chain,
    verify_persisted_event_merkle_chain_with_sequences, verify_state_transition_chain,
};
use statechronicle_domain_compat::intent::KeyId;

#[derive(Deserialize)]
struct HistoricalFixtures {
    producer: String,
    fixtures: Vec<HistoricalChain>,
}

#[derive(Deserialize)]
struct HistoricalChain {
    name: String,
    kind: OperationKind,
    events: Vec<Value>,
    commits: Vec<Value>,
    signatures: Vec<Value>,
}

fn fixtures() -> HistoricalFixtures {
    let mut fixtures: HistoricalFixtures =
        serde_json::from_str(include_str!("fixtures/deployment-evidence-0.1.0.json")).unwrap();
    fixtures.fixtures.push(
        serde_json::from_str(include_str!("fixtures/mixed-encoding-upload-0.1.0.json")).unwrap(),
    );
    fixtures
}

#[test]
fn historical_deployment_evidence_preserves_commitments_and_signatures() {
    let fixtures = fixtures();
    assert_eq!(
        fixtures.producer,
        "shardline-reliability 1.11.3; StateChronicle/Penelope 0.1.0"
    );
    assert_eq!(fixtures.fixtures.len(), 30);
    let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
    for chain in fixtures.fixtures {
        let sequences: Vec<_> = chain
            .events
            .iter()
            .map(|event| persisted_event_sequence(chain.kind, event.clone()).unwrap())
            .collect();
        let commits: Vec<_> = chain.commits.iter().cloned().map(Some).collect();
        verify_persisted_event_merkle_chain_with_sequences(
            chain.kind,
            &sequences,
            &chain.events,
            &commits,
        )
        .unwrap_or_else(|error| panic!("{}: {error}", chain.name));
        if chain.kind == OperationKind::Upload {
            let events: Vec<LifecycleEvent> = chain
                .events
                .iter()
                .cloned()
                .map(|value| serde_json::from_value(value).unwrap())
                .collect();
            verify_lifecycle_chain(&events).unwrap();
        } else if chain.kind == OperationKind::ResumableSession {
            let events: Vec<StateTransitionEvent> = chain
                .events
                .iter()
                .cloned()
                .map(|value| serde_json::from_value(value).unwrap())
                .collect();
            verify_state_transition_chain(&events).unwrap();
        }
        let mut previous = None;
        for ((event, expected), signature) in chain
            .events
            .iter()
            .zip(&chain.commits)
            .zip(&chain.signatures)
        {
            let generated = build_persisted_merkle_commit_with_previous(
                chain.kind,
                event.clone(),
                previous.clone(),
            )
            .unwrap();
            assert_eq!(&generated, expected, "{} commitment changed", chain.name);
            let commit: ReliabilityMerkleCommit =
                serde_json::from_value(generated.clone()).unwrap();
            let signed = commit
                .sign(
                    &key,
                    KeyId::new("deployment-compatibility-key".to_owned()).unwrap(),
                )
                .unwrap();
            let value = serde_json::to_value(&signed).unwrap();
            assert_eq!(
                &value["signature"], signature,
                "{} signature changed",
                chain.name
            );
            // Also verify with the public v2 key implementation, independently
            // of StateChronicle's v3 verifier.
            let bytes = statechronicle_core::canonicalize::canonicalize(&signed.body).unwrap();
            let detached_signature = Signature::from_bytes(signed.signature.sig.as_bytes());
            key.verifying_key()
                .verify(&bytes, &detached_signature)
                .unwrap();
            previous = Some(generated);
        }
    }
}

#[test]
fn historical_evidence_rejects_tampering_and_storage_sequence_rewrites() {
    for chain in fixtures().fixtures {
        let sequences: Vec<_> = chain
            .events
            .iter()
            .map(|event| persisted_event_sequence(chain.kind, event.clone()).unwrap())
            .collect();
        let mut commits: Vec<_> = chain.commits.iter().cloned().map(Some).collect();
        commits[1].as_mut().unwrap()["body"]["event_merkle_root"] =
            Value::String("tampered".to_owned());
        assert!(
            verify_persisted_event_merkle_chain_with_sequences(
                chain.kind,
                &sequences,
                &chain.events,
                &commits,
            )
            .is_err(),
            "{} accepted a rewritten root",
            chain.name
        );
        commits[1] = None;
        assert!(
            verify_persisted_event_merkle_chain_with_sequences(
                chain.kind,
                &sequences,
                &chain.events,
                &commits,
            )
            .is_err(),
            "{} accepted a missing commitment",
            chain.name
        );
        let valid_commits: Vec<_> = chain.commits.into_iter().map(Some).collect();
        let mut rewritten_sequences = sequences;
        rewritten_sequences[1] += 1;
        assert!(
            verify_persisted_event_merkle_chain_with_sequences(
                chain.kind,
                &rewritten_sequences,
                &chain.events,
                &valid_commits,
            )
            .is_err(),
            "{} accepted a rewritten row sequence",
            chain.name
        );
    }
}

#[test]
fn old_envelopes_without_schema_markers_remain_readable() {
    for chain in fixtures().fixtures {
        let mut events = chain.events;
        if chain.name.ends_with("/LegacyJson") {
            for event in &mut events {
                event.as_object_mut().unwrap().remove("digest_encoding");
            }
        }
        let sequences: Vec<_> = events
            .iter()
            .map(|event| persisted_event_sequence(chain.kind, event.clone()).unwrap())
            .collect();
        let commits: Vec<_> = chain
            .commits
            .into_iter()
            .map(|mut commit| {
                commit.as_object_mut().unwrap().remove("schema_version");
                Some(commit)
            })
            .collect();
        verify_persisted_event_merkle_chain_with_sequences(
            chain.kind, &sequences, &events, &commits,
        )
        .unwrap_or_else(|error| panic!("{}: {error}", chain.name));
    }
}
