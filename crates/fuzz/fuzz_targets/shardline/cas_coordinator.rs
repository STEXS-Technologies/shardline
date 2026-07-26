#![no_main]
#![allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::let_underscore_untyped,
    clippy::shadow_unrelated
)]

use std::{
    num::NonZeroU64,
    sync::LazyLock,
};

use libfuzzer_sys::fuzz_target;
use shardline_cas::{CasCoordinator, CasLimits, ObjectReachability, paths::FUZZ_NAMESPACE_PREFIX};
use shardline_storage::SyncObjectStoreBridge;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Runtime::new().expect("failed to create tokio runtime for fuzz target")
});
use shardline_index::{MemoryIndexStore, StoredObjectId};
use shardline_protocol::ShardlineHash;
use shardline_storage::{
    ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome,
};

/// Maximum body bytes accepted by the fuzz coordinator.
const MAX_BODY_BYTES: u64 = 4096;

fuzz_target!(|data: Vec<u8>| {
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

    let key = ObjectKey::parse(&format!("{FUZZ_NAMESPACE_PREFIX}test-blob"))
        .expect("static key is valid");

    // Test 1: store_content_addressed_blob observes size limits.
    let outcome = RUNTIME.block_on(coordinator.store_content_addressed_blob(
        &key,
        &integrity,
        body.clone(),
    ));

    if body_len > MAX_BODY_BYTES {
        // Must be rejected.
        assert!(
            outcome.is_err(),
            "body {body_len} bytes exceeds max {MAX_BODY_BYTES} but was accepted"
        );
        return;
    }

    // For valid-sized bodies, the store should succeed.
    let outcome = match outcome {
        Ok(o) => o,
        Err(e) => {
            // Allowed transient errors.
            if body.is_empty() && matches!(&e, shardline_cas::CasError::Overflow) {
                return;
            }
            panic!("store_content_addressed_blob failed for {body_len}-byte body: {e:?}");
        }
    };

    // Test 2: First store is Inserted.
    assert_eq!(outcome, PutOutcome::Inserted);

    // Test 3: Idempotent — storing the same blob again returns AlreadyExists.
    let second = RUNTIME.block_on(coordinator.store_content_addressed_blob(
        &key,
        &integrity,
        body.clone(),
    ));
    assert!(
        matches!(second, Ok(PutOutcome::AlreadyExists)),
        "idempotent store should return AlreadyExists, got {second:?}"
    );

    // Test 4: is_object_reachable returns true for the stored object.
    let object_id = StoredObjectId::new(hash);
    let reachable = RUNTIME.block_on(ObjectReachability::is_object_reachable(&coordinator, &object_id));
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
        let body_len = u64::try_from(body.as_slice().len()).map_err(|_| MemoryObjectError::Integrity)?;
        if body_len != integrity.length() {
            return Err(MemoryObjectError::Integrity);
        }

        let mut objects = self.objects.lock().map_err(|_| MemoryObjectError::Integrity)?;
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
        let objects = self.objects.lock().map_err(|_| MemoryObjectError::Integrity)?;
        let object = objects.get(key).ok_or(MemoryObjectError::Missing)?;
        let start = usize::try_from(range.start()).map_err(|_| MemoryObjectError::Missing)?;
        let length = range.len().ok_or(MemoryObjectError::Missing)?;
        let length = usize::try_from(length).map_err(|_| MemoryObjectError::Missing)?;

        Ok(object.iter().copied().skip(start).take(length).collect())
    }

    fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
        let objects = self.objects.lock().map_err(|_| MemoryObjectError::Integrity)?;
        Ok(objects.contains_key(key))
    }

    fn metadata(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<shardline_storage::ObjectMetadata>, Self::Error> {
        let objects = self.objects.lock().map_err(|_| MemoryObjectError::Integrity)?;
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
        let objects = self.objects.lock().map_err(|_| MemoryObjectError::Integrity)?;
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

    fn copy_if_absent(
        &self,
        source: &ObjectKey,
        destination: &ObjectKey,
    ) -> Result<shardline_storage::PutOutcome, Self::Error> {
        use std::collections::hash_map::Entry;
        let mut objects = self.objects.lock().map_err(|_| MemoryObjectError::Integrity)?;
        let value = objects.get(source).ok_or(MemoryObjectError::Missing)?.clone();
        match objects.entry(destination.clone()) {
            Entry::Occupied(_) => Ok(shardline_storage::PutOutcome::AlreadyExists),
            Entry::Vacant(entry) => {
                entry.insert(value);
                Ok(shardline_storage::PutOutcome::Inserted)
            }
        }
    }

    fn delete_if_present(
        &self,
        key: &ObjectKey,
    ) -> Result<shardline_storage::DeleteOutcome, Self::Error> {
        let mut objects = self.objects.lock().map_err(|_| MemoryObjectError::Integrity)?;
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
