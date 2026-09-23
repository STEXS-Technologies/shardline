#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_reliability::{
    OperationKind, build_persisted_merkle_commit, build_persisted_merkle_commit_with_previous,
    verify_persisted_merkle_commit_with_previous,
};

const fn operation_kind(byte: u8) -> OperationKind {
    match byte % 11 {
        0 => OperationKind::Upload,
        1 => OperationKind::ResumableSession,
        2 => OperationKind::MetadataCommit,
        3 => OperationKind::OciTag,
        4 => OperationKind::S3Object,
        5 => OperationKind::Visibility,
        6 => OperationKind::ProviderEvent,
        7 => OperationKind::Repair,
        8 => OperationKind::GarbageCollection,
        9 => OperationKind::RetentionHold,
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
    let kind = operation_kind(kind_byte);
    let Ok(first) = build_persisted_merkle_commit(kind, value.clone()) else {
        return;
    };
    let second =
        build_persisted_merkle_commit_with_previous(kind, value.clone(), Some(first.clone())).ok();
    drop(verify_persisted_merkle_commit_with_previous(
        kind,
        value,
        second,
        Some(first),
    ));
});
