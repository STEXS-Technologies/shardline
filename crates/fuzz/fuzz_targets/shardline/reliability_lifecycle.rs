#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{
    LifecycleEvent, OperationIdentity, OperationKind, ReliabilityError, UploadLifecycleState,
    verify_lifecycle_chain,
};

const CONTENT_SHA256: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

const fn next_state(state: UploadLifecycleState, byte: u8) -> UploadLifecycleState {
    match state {
        UploadLifecycleState::Created => match byte % 3 {
            0 => UploadLifecycleState::Created,
            1 => UploadLifecycleState::Storing,
            _ => UploadLifecycleState::Failed,
        },
        UploadLifecycleState::Storing => match byte % 3 {
            0 => UploadLifecycleState::Storing,
            1 => UploadLifecycleState::Stored,
            _ => UploadLifecycleState::Failed,
        },
        UploadLifecycleState::Stored => match byte % 3 {
            0 => UploadLifecycleState::Stored,
            1 => UploadLifecycleState::MetadataCommitted,
            _ => UploadLifecycleState::Failed,
        },
        UploadLifecycleState::MetadataCommitted => match byte % 3 {
            0 => UploadLifecycleState::MetadataCommitted,
            1 => UploadLifecycleState::Visible,
            _ => UploadLifecycleState::Failed,
        },
        UploadLifecycleState::Visible | UploadLifecycleState::Failed => state,
    }
}

fuzz_target!(|input: &[u8]| {
    let operation_id = if input.is_empty() {
        "fuzz-operation".to_owned()
    } else {
        input
            .iter()
            .take(64)
            .map(|byte| char::from(b'a'.wrapping_add(byte % 26)))
            .collect()
    };
    let Ok(operation) = OperationIdentity::new(
        "fuzz-tenant",
        "fuzz-repository",
        operation_id,
        OperationKind::Upload,
    ) else {
        return;
    };
    let operation = operation
        .with_object_key("objects/fuzz")
        .with_content_sha256(CONTENT_SHA256);

    let mut state = UploadLifecycleState::Created;
    let mut events = Vec::new();
    for (index, byte) in input.iter().take(128).copied().enumerate() {
        let next = next_state(state, byte);
        let Some(sequence) = u64::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1))
        else {
            return;
        };
        let Ok(event) = LifecycleEvent::new(operation.clone(), sequence, state, next) else {
            return;
        };
        events.push(event);
        state = next;
    }
    assert!(verify_lifecycle_chain(&events).is_ok());

    if let Some(event) = events.first_mut() {
        event.process_digest = penelope::ContentDigest::sha256(b"tampered");
        assert!(matches!(
            verify_lifecycle_chain(&events),
            Err(ReliabilityError::ProcessDigestMismatch)
        ));
    }
});
