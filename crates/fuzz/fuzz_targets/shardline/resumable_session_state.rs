#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_index::ResumableSessionState;

const STATES: [ResumableSessionState; 5] = [
    ResumableSessionState::Active,
    ResumableSessionState::Completing,
    ResumableSessionState::Completed,
    ResumableSessionState::Aborted,
    ResumableSessionState::Expired,
];

fn state(byte: u8) -> ResumableSessionState {
    STATES
        .get(usize::from(byte & 0b111))
        .copied()
        .unwrap_or(ResumableSessionState::Active)
}

fuzz_target!(|input: &[u8]| {
    let mut current = ResumableSessionState::Active;
    for &byte in input.iter().take(256) {
        let candidate = state(byte);
        let allowed = current.can_transition_to(candidate);
        if allowed {
            current = candidate;
        }
        assert!(current.can_transition_to(current));
        if current.is_terminal() {
            assert!(STATES.iter().all(|state| {
                current.can_transition_to(*state)
                    == (*state == current || *state == ResumableSessionState::Active)
            }));
        }
    }
});
