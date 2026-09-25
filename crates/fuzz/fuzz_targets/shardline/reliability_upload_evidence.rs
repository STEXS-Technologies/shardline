#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{
    LifecycleEvent, UploadLifecycleState, baseline_upload_lifecycle_events,
    verify_upload_lifecycle_events,
};

fuzz_target!(|data: &[u8]| {
    // Exercise hostile serialized evidence without assuming it is valid.
    if let Ok(events) = serde_json::from_slice::<Vec<LifecycleEvent>>(data) {
        drop(verify_upload_lifecycle_events(
            &events,
            "fuzz-tenant",
            "fuzz-repository",
            "fuzz-operation",
            "fuzz-object",
            &"a".repeat(64),
            UploadLifecycleState::Visible,
        ));
    }

    let final_state = match data.first().copied().unwrap_or_default() % 6 {
        0 => UploadLifecycleState::Created,
        1 => UploadLifecycleState::Storing,
        2 => UploadLifecycleState::Stored,
        3 => UploadLifecycleState::MetadataCommitted,
        4 => UploadLifecycleState::Visible,
        _ => UploadLifecycleState::Failed,
    };
    let operation_id = format!("fuzz-operation-{}", data.len());
    let object_key = format!("fuzz-object-{}", data.len());
    let hash = "a".repeat(64);
    let Ok(events) = baseline_upload_lifecycle_events(
        "fuzz-tenant",
        "fuzz-repository",
        &operation_id,
        &object_key,
        &hash,
        final_state,
    ) else {
        return;
    };
    assert!(
        verify_upload_lifecycle_events(
            &events,
            "fuzz-tenant",
            "fuzz-repository",
            &operation_id,
            &object_key,
            &hash,
            final_state,
        )
        .is_ok()
    );

    let Ok(encoded) = serde_json::to_vec(&events) else {
        return;
    };
    let Ok(decoded) = serde_json::from_slice::<Vec<LifecycleEvent>>(&encoded) else {
        return;
    };
    assert!(
        verify_upload_lifecycle_events(
            &decoded,
            "fuzz-tenant",
            "fuzz-repository",
            &operation_id,
            &object_key,
            &hash,
            final_state,
        )
        .is_ok()
    );
});
