#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{
    OciObjectEvidenceLog, OciObjectIdentity, OciObjectLifecycleState, OciObjectSnapshot,
    verify_oci_object_lifecycle_events,
};

fn snapshot(state: OciObjectLifecycleState, deleted_at: Option<u64>) -> Option<OciObjectSnapshot> {
    OciObjectSnapshot::new(
        OciObjectIdentity::new("global", "fuzz/repository", "blob", "a".repeat(64)).ok()?,
        state,
        deleted_at,
    )
    .ok()
}

fuzz_target!(|input: &[u8]| {
    if let Ok(decoded) = serde_json::from_slice::<OciObjectEvidenceLog>(input) {
        if let Some(expected) = snapshot(OciObjectLifecycleState::Published, None) {
            drop(verify_oci_object_lifecycle_events(
                decoded.events(),
                &expected,
            ));
        }
    }

    let Some(published) = snapshot(OciObjectLifecycleState::Published, None) else {
        return;
    };
    let deleted_at = u64::from(input.first().copied().unwrap_or_default());
    let Some(deleted) = snapshot(OciObjectLifecycleState::Deleted, Some(deleted_at)) else {
        return;
    };
    let Some(reclaimed) = snapshot(OciObjectLifecycleState::Reclaimed, Some(deleted_at)) else {
        return;
    };
    let Ok(mut evidence) = OciObjectEvidenceLog::baseline(published) else {
        return;
    };
    if evidence.record(deleted).is_err() || evidence.record(reclaimed).is_err() {
        return;
    }
    let Some(expected) = snapshot(OciObjectLifecycleState::Reclaimed, Some(deleted_at)) else {
        return;
    };
    assert!(verify_oci_object_lifecycle_events(evidence.events(), &expected).is_ok());

    let Ok(encoded) = serde_json::to_vec(&evidence) else {
        return;
    };
    let Ok(decoded) = serde_json::from_slice::<OciObjectEvidenceLog>(&encoded) else {
        return;
    };
    assert!(verify_oci_object_lifecycle_events(decoded.events(), &expected).is_ok());
});
