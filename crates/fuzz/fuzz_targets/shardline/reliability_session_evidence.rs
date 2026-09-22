#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{ResumableLifecycleState, SessionEvidenceLog};

const STATES: [ResumableLifecycleState; 5] = [
    ResumableLifecycleState::Active,
    ResumableLifecycleState::Completing,
    ResumableLifecycleState::Completed,
    ResumableLifecycleState::Aborted,
    ResumableLifecycleState::Expired,
];

fn state(byte: u8) -> ResumableLifecycleState {
    STATES
        .get(usize::from(byte) % STATES.len())
        .copied()
        .unwrap_or(ResumableLifecycleState::Active)
}

fuzz_target!(|input: &[u8]| {
    let Ok(mut evidence) = SessionEvidenceLog::new("fuzz-scope", "fuzz-session", "object") else {
        return;
    };
    let mut current = ResumableLifecycleState::Active;
    for byte in input.iter().take(128).copied() {
        let candidate = state(byte);
        if !current.can_transition_to(candidate) {
            continue;
        }
        if evidence
            .record("fuzz-scope", "fuzz-session", "object", current, candidate)
            .is_err()
        {
            return;
        }
        current = candidate;
    }
    evidence.verify().unwrap();
    let encoded = serde_json::to_vec(&evidence).unwrap();
    let decoded: SessionEvidenceLog = serde_json::from_slice(&encoded).unwrap();
    decoded.verify().unwrap();
});
