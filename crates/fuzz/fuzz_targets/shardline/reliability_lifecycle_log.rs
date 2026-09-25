#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{
    LifecycleEvent, LifecycleEvidenceLog, UploadLifecycleState, baseline_upload_lifecycle_events,
};

fuzz_target!(|data: &[u8]| {
    // Persisted lifecycle logs are untrusted input. Construction must either
    // produce a fully verified chain or return an error without panicking.
    if let Ok(events) = serde_json::from_slice::<Vec<LifecycleEvent>>(data)
        && let Ok(log) = LifecycleEvidenceLog::from_events(events)
    {
        assert!(log.verify().is_ok());
    }

    let final_state = match data.first().copied().unwrap_or_default() % 4 {
        0 => UploadLifecycleState::Created,
        1 => UploadLifecycleState::Storing,
        2 => UploadLifecycleState::Stored,
        _ => UploadLifecycleState::Visible,
    };
    let Ok(events) = baseline_upload_lifecycle_events(
        "fuzz-tenant",
        "fuzz-repository",
        format!("operation-{}", data.len()),
        "fuzz-object",
        "a".repeat(64),
        final_state,
    ) else {
        return;
    };
    let Ok(log) = LifecycleEvidenceLog::from_events(events) else {
        return;
    };
    assert!(log.verify().is_ok());
});
