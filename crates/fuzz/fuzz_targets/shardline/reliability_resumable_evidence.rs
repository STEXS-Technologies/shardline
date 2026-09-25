#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{
    OperationIdentity, OperationKind, PenelopeDigest, ResumableLifecycleState,
    StateTransitionEvent, verify_state_transition_chain,
};

const STATES: [ResumableLifecycleState; 5] = [
    ResumableLifecycleState::Active,
    ResumableLifecycleState::Completing,
    ResumableLifecycleState::Completed,
    ResumableLifecycleState::Aborted,
    ResumableLifecycleState::Expired,
];

fn state(byte: u8) -> ResumableLifecycleState {
    STATES
        .get(usize::from(byte))
        .copied()
        .unwrap_or(ResumableLifecycleState::Active)
}

fuzz_target!(|input: &[u8]| {
    let Ok(operation) = OperationIdentity::new(
        "fuzz-scope",
        "fuzz-repository",
        "fuzz-session",
        OperationKind::ResumableSession,
    ) else {
        return;
    };
    let mut current = ResumableLifecycleState::Active;
    let mut events = Vec::new();
    for byte in input.iter().take(128).copied() {
        let candidate = state(byte);
        if !current.can_transition_to(candidate) {
            continue;
        }
        // Invalid candidates are skipped, so the input index is not the
        // evidence sequence.  Allocate sequence numbers only for accepted
        // transitions or the fuzz target would manufacture a false chain-gap
        // finding for an otherwise valid state trace.
        let sequence = events.last().map_or(0, |event: &StateTransitionEvent| {
            event.sequence.saturating_add(1)
        });
        let Ok(event) = StateTransitionEvent::new(operation.clone(), sequence, current, candidate)
        else {
            return;
        };
        events.push(event);
        current = candidate;
    }
    assert!(verify_state_transition_chain(&events).is_ok());

    if let Some(event) = events.first_mut() {
        event.process_digest = PenelopeDigest::sha256(b"tampered");
        assert!(verify_state_transition_chain(&events).is_err());
    }
});
