#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{
    QuarantineEvidenceLog, QuarantineLifecycleState, QuarantineObjectIdentity, QuarantineSnapshot,
    verify_quarantine_lifecycle_events,
};

fn snapshot(state: QuarantineLifecycleState, length: u64) -> Option<QuarantineSnapshot> {
    QuarantineSnapshot::new(
        QuarantineObjectIdentity::new("aa/fuzz-object").ok()?,
        length,
        100,
        200,
        state,
    )
    .ok()
}

fuzz_target!(|input: &[u8]| {
    if let Ok(decoded) = serde_json::from_slice::<QuarantineEvidenceLog>(input)
        && let Some(expected) = snapshot(
            QuarantineLifecycleState::Active,
            u64::from(input.first().copied().unwrap_or_default()),
        )
    {
        drop(verify_quarantine_lifecycle_events(
            decoded.events(),
            &expected,
        ));
    }

    let Some(first) = snapshot(
        QuarantineLifecycleState::Active,
        u64::from(input.first().copied().unwrap_or_default()),
    ) else {
        return;
    };
    let Some(released) = snapshot(
        QuarantineLifecycleState::Released,
        u64::from(input.get(1).copied().unwrap_or_default()),
    ) else {
        return;
    };
    let Some(recovered) = snapshot(
        QuarantineLifecycleState::Active,
        u64::from(input.get(2).copied().unwrap_or_default()),
    ) else {
        return;
    };
    let Ok(mut evidence) = QuarantineEvidenceLog::baseline(first) else {
        return;
    };
    if evidence.record(released).is_err() || evidence.record(recovered.clone()).is_err() {
        return;
    }
    assert!(verify_quarantine_lifecycle_events(evidence.events(), &recovered).is_ok());

    let Ok(encoded) = serde_json::to_vec(&evidence) else {
        return;
    };
    let Ok(decoded) = serde_json::from_slice::<QuarantineEvidenceLog>(&encoded) else {
        return;
    };
    assert!(verify_quarantine_lifecycle_events(decoded.events(), &recovered).is_ok());
});
