#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_protocol::ShardlineHash;
use shardline_storage::{
    LocalObjectStore, LocalPublishBoundary, LocalPublishFault, ObjectBody, ObjectIntegrity,
    ObjectKey, arm_fault,
};

const MAX_BODY_BYTES: usize = 16 * 1024;

fuzz_target!(|data: (u8, u8, Vec<u8>, Vec<u8>)| {
    let (boundary_selector, fault_selector, mut previous, mut replacement) = data;
    previous.truncate(MAX_BODY_BYTES);
    replacement.truncate(MAX_BODY_BYTES);
    if previous.is_empty() {
        previous.push(0x11);
    }
    if replacement.is_empty() {
        replacement.push(0x22);
    }

    let Ok(sandbox) = tempfile::tempdir() else {
        return;
    };
    let root = sandbox.path().join("objects");
    let Ok(store) = LocalObjectStore::new(root) else {
        return;
    };
    let Ok(key) = ObjectKey::parse("faults/atomic-object") else {
        return;
    };
    let previous_integrity = integrity(&previous);
    assert!(
        store
            .put_overwrite(&key, ObjectBody::from_slice(&previous), &previous_integrity,)
            .is_ok()
    );

    let boundary = boundary_from_selector(boundary_selector);
    let fault = fault_from_selector(fault_selector);
    let _armed = arm_fault(store.path_for_key(&key), boundary, fault);
    let replacement_integrity = integrity(&replacement);
    let result = store.put_overwrite(
        &key,
        ObjectBody::from_slice(&replacement),
        &replacement_integrity,
    );
    let stored = std::fs::read(store.path_for_key(&key));
    assert!(stored.is_ok());
    let Ok(stored) = stored else {
        return;
    };

    assert!(
        stored == previous || stored == replacement,
        "injected local publication fault exposed torn bytes"
    );
    if result.is_ok() {
        assert_eq!(stored, replacement);
    } else if matches!(
        boundary,
        LocalPublishBoundary::BeforeTemporaryWrite
            | LocalPublishBoundary::DuringTemporaryWrite
            | LocalPublishBoundary::BeforeTemporarySync
            | LocalPublishBoundary::AfterTemporaryDurable
    ) {
        assert_eq!(stored, previous);
    } else {
        assert_eq!(stored, replacement);
    }
});

fn integrity(bytes: &[u8]) -> ObjectIntegrity {
    let hash = ShardlineHash::from_bytes(*blake3::hash(bytes).as_bytes());
    ObjectIntegrity::new(hash, u64::try_from(bytes.len()).unwrap_or(u64::MAX))
}

const fn boundary_from_selector(selector: u8) -> LocalPublishBoundary {
    match selector % 7 {
        0 => LocalPublishBoundary::BeforeTemporaryWrite,
        1 => LocalPublishBoundary::DuringTemporaryWrite,
        2 => LocalPublishBoundary::BeforeTemporarySync,
        3 => LocalPublishBoundary::AfterTemporaryDurable,
        4 => LocalPublishBoundary::AfterInstall,
        5 => LocalPublishBoundary::BeforeParentSync,
        _ => LocalPublishBoundary::AfterParentDurable,
    }
}

const fn fault_from_selector(selector: u8) -> LocalPublishFault {
    match selector % 5 {
        0 => LocalPublishFault::Interrupted,
        1 => LocalPublishFault::OutOfSpace,
        2 => LocalPublishFault::InputOutput,
        3 => LocalPublishFault::PartialWrite,
        _ => LocalPublishFault::SyncFailure,
    }
}
