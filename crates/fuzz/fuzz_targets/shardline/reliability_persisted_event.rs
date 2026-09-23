#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{OperationKind, verify_persisted_event};

fn operation_kind(byte: u8) -> OperationKind {
    match byte % 9 {
        0 => OperationKind::Upload,
        1 => OperationKind::ResumableSession,
        2 => OperationKind::MetadataCommit,
        3 => OperationKind::Visibility,
        4 => OperationKind::ProviderEvent,
        5 => OperationKind::Repair,
        6 => OperationKind::GarbageCollection,
        7 => OperationKind::RetentionHold,
        _ => OperationKind::WebhookDelivery,
    }
}

fuzz_target!(|input: &[u8]| {
    let Some((&kind_byte, payload)) = input.split_first() else {
        return;
    };
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return;
    };
    drop(verify_persisted_event(operation_kind(kind_byte), value));
});
