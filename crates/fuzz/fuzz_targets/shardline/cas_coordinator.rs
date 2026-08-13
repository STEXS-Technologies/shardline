#![no_main]
#![allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::let_underscore_untyped,
    clippy::shadow_unrelated
)]

use std::num::NonZeroU64;

use libfuzzer_sys::fuzz_target;
use shardline_cas::{CasCoordinator, CasLimits, ObjectReachability, paths::FUZZ_NAMESPACE_PREFIX};
use shardline_storage::SyncObjectStoreBridge;

use shardline_index::{AsyncIndexStore, MemoryIndexStore, StoredObjectId};
use shardline_protocol::ShardlineHash;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome};

/// Maximum body bytes accepted by the fuzz coordinator.
const MAX_BODY_BYTES: u64 = 4096;

fuzz_target!(|data: Vec<u8>| {
    // Build a fresh tokio runtime per iteration; a platform that cannot create
    // one is a setup failure, not a fuzz finding, so bail rather than panic.
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(_) => return,
    };

    let index = MemoryIndexStore::default();
    let object_store = SyncObjectStoreBridge::new(MemoryObjectStore::default());
    let limits = CasLimits::new(
        NonZeroU64::new(MAX_BODY_BYTES).unwrap_or(NonZeroU64::MIN),
        NonZeroU64::new(MAX_BODY_BYTES).unwrap_or(NonZeroU64::MIN),
        NonZeroU64::new(MAX_BODY_BYTES).unwrap_or(NonZeroU64::MIN),
    );
    let coordinator = CasCoordinator::new(index, object_store, (), limits);

    // Use the fuzzed data as the body.
    let body = data;
    let hash = ShardlineHash::from_bytes(*blake3::hash(&body).as_bytes());
    let body_len = u64::try_from(body.len()).unwrap_or(u64::MAX);
    let integrity = ObjectIntegrity::new(hash, body_len);

    let key = match ObjectKey::parse(&format!("{FUZZ_NAMESPACE_PREFIX}test-blob")) {
        Ok(key) => key,
        Err(_) => return,
    };

    // Test 1: store_content_addressed_blob observes size limits.
    let outcome =
        runtime.block_on(coordinator.store_content_addressed_blob(&key, &integrity, body.clone()));

    if body_len > MAX_BODY_BYTES {
        // Must be rejected.
        assert!(
            outcome.is_err(),
            "body {body_len} bytes exceeds max {MAX_BODY_BYTES} but was accepted"
        );
        return;
    }

    // For valid-sized bodies, the store should succeed. An empty body can
    // legitimately fail (nothing to address); that is a valid fuzz outcome, not
    // a defect, so we return rather than asserting success.
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(_) => {
            assert!(
                body.is_empty(),
                "store_content_addressed_blob failed for {body_len}-byte non-empty body"
            );
            return;
        }
    };

    // Test 2: First store is Inserted.
    assert_eq!(outcome, PutOutcome::Inserted);

    // Test 3: Idempotent — storing the same blob again returns AlreadyExists.
    let second = runtime.block_on(coordinator.store_content_addressed_blob(&key, &integrity, body));
    assert!(
        matches!(second, Ok(PutOutcome::AlreadyExists)),
        "idempotent store should return AlreadyExists, got {second:?}"
    );

    // Test 4: is_object_reachable returns true once the object is registered
    // in the index. store_content_addressed_blob writes only the object store;
    // production flows also register content-addressed objects in the index so
    // reachability (an index lookup) can resolve them — mirror that here.
    let object_id = StoredObjectId::new(hash);
    runtime
        .block_on(AsyncIndexStore::insert_object(coordinator.index(), &object_id))
        .expect("index registration should succeed");
    let reachable = runtime.block_on(ObjectReachability::is_object_reachable(
        &coordinator,
        &object_id,
    ));
    assert!(
        matches!(reachable, Ok(true)),
        "stored object should be reachable, got {reachable:?}"
    );
});

// ── In-memory object store for fuzz testing ──────────────────────────

use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
struct MemoryObjectStore {
    objects: std::sync::Arc<std::sync::Mutex<HashMap<ObjectKey, Vec<u8>>>>,
}

impl ObjectStore for MemoryObjectStore {
    type Error = MemoryObjectError;

    fn put_if_absent(
        &self,
        key: &ObjectKey,
        body: ObjectBody<'_>,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, Self::Error> {
        let body_len =
            u64::try_from(body.as_slice().len()).map_err(|_err| MemoryObjectError::Integrity)?;
        if body_len != integrity.length() {
            return Err(MemoryObjectError::Integrity);
        }

        let mut objects = self
            .objects
            .lock()
            .map_err(|_err| MemoryObjectError::Integrity)?;
        if let Some(existing) = objects.get(key) {
            if existing.as_slice() == body.as_slice() {
                return Ok(PutOutcome::AlreadyExists);
            }
            return Err(MemoryObjectError::Integrity);
        }

        objects.insert(key.clone(), body.as_slice().to_vec());
        Ok(PutOutcome::Inserted)
    }

    fn read_range(
        &self,
        key: &ObjectKey,
        range: shardline_protocol::ByteRange,
    ) -> Result<Vec<u8>, Self::Error> {
        let objects = self
            .objects
            .lock()
            .map_err(|_err| MemoryObjectError::Integrity)?;
        let object = objects.get(key).ok_or(MemoryObjectError::Missing)?;
        let start = usize::try_from(range.start()).map_err(|_err| MemoryObjectError::Missing)?;
        let length = range.len().ok_or(MemoryObjectError::Missing)?;
        let length = usize::try_from(length).map_err(|_err| MemoryObjectError::Missing)?;

        Ok(object.iter().copied().skip(start).take(length).collect())
    }

    fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
        let objects = self
            .objects
            .lock()
            .map_err(|_err| MemoryObjectError::Integrity)?;
        Ok(objects.contains_key(key))
    }

    fn metadata(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<shardline_storage::ObjectMetadata>, Self::Error> {
        let objects = self
            .objects
            .lock()
            .map_err(|_err| MemoryObjectError::Integrity)?;
        Ok(objects.get(key).map(|object| {
            shardline_storage::ObjectMetadata::new(
                key.clone(),
                u64::try_from(object.len()).unwrap_or(u64::MAX),
                None,
            )
        }))
    }

    fn list_prefix(
        &self,
        prefix: &shardline_storage::ObjectPrefix,
    ) -> Result<Vec<shardline_storage::ObjectMetadata>, Self::Error> {
        let objects = self
            .objects
            .lock()
            .map_err(|_err| MemoryObjectError::Integrity)?;
        let mut result: Vec<_> = objects
            .iter()
            .filter(|(key, _)| key.as_str().starts_with(prefix.as_str()))
            .map(|(key, object)| {
                shardline_storage::ObjectMetadata::new(
                    key.clone(),
                    u64::try_from(object.len()).unwrap_or(u64::MAX),
                    None,
                )
            })
            .collect();
        result.sort_by(|a, b| a.key().as_str().cmp(b.key().as_str()));
        Ok(result)
    }

    fn delete_if_present(
        &self,
        key: &ObjectKey,
    ) -> Result<shardline_storage::DeleteOutcome, Self::Error> {
        let mut objects = self
            .objects
            .lock()
            .map_err(|_err| MemoryObjectError::Integrity)?;
        if objects.remove(key).is_some() {
            Ok(shardline_storage::DeleteOutcome::Deleted)
        } else {
            Ok(shardline_storage::DeleteOutcome::NotFound)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemoryObjectError {
    Integrity,
    Missing,
}

impl std::fmt::Display for MemoryObjectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integrity => write!(f, "integrity mismatch"),
            Self::Missing => write!(f, "object missing"),
        }
    }
}

impl std::error::Error for MemoryObjectError {}
