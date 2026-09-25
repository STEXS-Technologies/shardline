#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{
    RetentionEvidenceLog, RetentionHoldLifecycleState, RetentionHoldSnapshot,
    RetentionObjectIdentity, verify_retention_hold_lifecycle_events,
};

fn snapshot(state: RetentionHoldLifecycleState, held_at: u64) -> Option<RetentionHoldSnapshot> {
    RetentionHoldSnapshot::new(
        RetentionObjectIdentity::new("aa/fuzz-retention").ok()?,
        "fuzz hold",
        held_at,
        Some(held_at.saturating_add(100)),
        state,
    )
    .ok()
}

fuzz_target!(|input: &[u8]| {
    if let Ok(decoded) = serde_json::from_slice::<RetentionEvidenceLog>(input)
        && let Some(expected) = snapshot(
            RetentionHoldLifecycleState::Active,
            u64::from(input.first().copied().unwrap_or_default()),
        )
    {
        drop(verify_retention_hold_lifecycle_events(
            decoded.events(),
            &expected,
        ));
    }

    let Some(active) = snapshot(
        RetentionHoldLifecycleState::Active,
        u64::from(input.first().copied().unwrap_or_default()),
    ) else {
        return;
    };
    let Some(released) = snapshot(
        RetentionHoldLifecycleState::Released,
        u64::from(input.get(1).copied().unwrap_or_default()),
    ) else {
        return;
    };
    let Some(recovered) = snapshot(
        RetentionHoldLifecycleState::Active,
        u64::from(input.get(2).copied().unwrap_or_default()),
    ) else {
        return;
    };
    let Ok(mut evidence) = RetentionEvidenceLog::baseline(active) else {
        return;
    };
    if evidence.record(released).is_err() || evidence.record(recovered.clone()).is_err() {
        return;
    }
    assert!(verify_retention_hold_lifecycle_events(evidence.events(), &recovered).is_ok());

    let Ok(encoded) = serde_json::to_vec(&evidence) else {
        return;
    };
    let Ok(decoded) = serde_json::from_slice::<RetentionEvidenceLog>(&encoded) else {
        return;
    };
    assert!(verify_retention_hold_lifecycle_events(decoded.events(), &recovered).is_ok());
});
