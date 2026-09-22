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
        .get(usize::from(byte))
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
    if evidence.verify().is_err() {
        return;
    }
    let Ok(encoded) = serde_json::to_vec(&evidence) else {
        return;
    };
    let Ok(decoded) = serde_json::from_slice::<SessionEvidenceLog>(&encoded) else {
        return;
    };
    if decoded.verify().is_err() {
        return;
    }
});
