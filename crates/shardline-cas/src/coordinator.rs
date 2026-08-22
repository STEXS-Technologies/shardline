use shardline_index::{AsyncIndexStore, StoredObjectId};
use shardline_storage::{AsyncObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, PutOutcome};

use crate::reachability::ObjectReachability;
use crate::{
    CasError, CasLimits, UploadLifecycleBoundary, fault_injection::upload_lifecycle_failpoint,
};

#[derive(Debug)]
pub struct CasCoordinator<I, O, R> {
    index: I,
    object_store: O,
    record_store: R,
    limits: CasLimits,
}

impl<I, O, R> CasCoordinator<I, O, R> {
    pub const fn new(index: I, object_store: O, record_store: R, limits: CasLimits) -> Self {
        Self {
            index,
            object_store,
            record_store,
            limits,
        }
    }

    pub const fn index(&self) -> &I {
        &self.index
    }

    pub const fn object_store(&self) -> &O {
        &self.object_store
    }

    pub const fn record_store(&self) -> &R {
        &self.record_store
    }

    pub const fn limits(&self) -> CasLimits {
        self.limits
    }
}

impl<I, O, R> CasCoordinator<I, O, R>
where
    I: AsyncIndexStore + Send + Sync,
    <I as AsyncIndexStore>::Error: std::error::Error + Send + Sync + 'static,
    O: AsyncObjectStore + Clone + 'static,
    <O as AsyncObjectStore>::Error: std::error::Error + Send + Sync + 'static,
{
    /// # Errors
    ///
    /// Returns CasError when the body exceeds the configured limit or the
    /// object store rejects the write.
    pub async fn store_content_addressed_blob(
        &self,
        key: &ObjectKey,
        integrity: &ObjectIntegrity,
        body: Vec<u8>,
    ) -> Result<PutOutcome, CasError> {
        let body_len = u64::try_from(body.len()).map_err(|_err| CasError::Overflow)?;
        if body_len > self.limits.max_object_bytes().get() {
            return Err(CasError::BodyTooLarge {
                actual: body_len,
                max: self.limits.max_object_bytes().get(),
            });
        }
        self.object_store
            .put_if_absent(key, ObjectBody::from_vec(body), integrity)
            .await
            .map_err(CasError::from_object_store)
    }
}

impl<I, O, R> CasCoordinator<I, O, R>
where
    I: shardline_index::UploadIntentStore + Send + Sync,
    <I as shardline_index::UploadIntentStore>::Error: std::error::Error + Send + Sync + 'static,
{
    /// Executes upload work within the coordinator-owned durable intent lifecycle.
    ///
    /// The configured index is the only store used for the lifecycle, preventing
    /// intent creation and state transitions from being split across stores.
    /// A work error deliberately leaves the intent at its last durable in-flight
    /// state: external commits can succeed even when their acknowledgement is
    /// lost, so only reconciliation after inspecting durable state may classify
    /// an attempt as terminally failed.
    ///
    /// # Errors
    ///
    /// Returns the caller's work error, or an error converted from [`CasError`]
    /// when an intent persistence boundary fails.
    pub async fn with_upload_intent<F, Fut, T, E>(
        &self,
        intent: &shardline_index::UploadIntent,
        work: F,
    ) -> Result<T, E>
    where
        E: From<CasError>,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        self.begin_upload(intent).await.map_err(E::from)?;
        upload_lifecycle_failpoint(
            intent.intent_id(),
            UploadLifecycleBoundary::AfterIntentCreated,
        )
        .map_err(E::from)?;
        let current = self
            .index
            .intent_by_id(intent.intent_id())
            .await
            .map_err(CasError::from_record)
            .map_err(E::from)?
            .ok_or_else(|| E::from(CasError::InvalidUploadTransition))?;

        // Content-addressed upload retries are idempotent. Once an intent is
        // visible, repeat the storage work only to obtain the protocol response;
        // never reopen the completed durable lifecycle.
        if current.state() == shardline_index::UploadIntentState::Visible {
            return work().await;
        }
        // A terminal `Failed` intent cannot be resumed. Any other in-flight state
        // (Created / Storing / Stored / MetadataCommitted) is tolerated so that a
        // concurrent duplicate caller for the same intent does not spuriously
        // fail; the forward transitions below are themselves idempotent.
        if current.state() == shardline_index::UploadIntentState::Failed {
            return Err(E::from(CasError::InvalidUploadTransition));
        }
        self.transition_upload(
            intent.intent_id(),
            shardline_index::UploadIntentState::Storing,
        )
        .await
        .map_err(E::from)?;
        upload_lifecycle_failpoint(intent.intent_id(), UploadLifecycleBoundary::AfterStoring)
            .map_err(E::from)?;

        match work().await {
            Ok(result) => {
                upload_lifecycle_failpoint(
                    intent.intent_id(),
                    UploadLifecycleBoundary::AfterObjectWork,
                )
                .map_err(E::from)?;
                self.transition_upload(
                    intent.intent_id(),
                    shardline_index::UploadIntentState::Stored,
                )
                .await
                .map_err(E::from)?;
                upload_lifecycle_failpoint(
                    intent.intent_id(),
                    UploadLifecycleBoundary::AfterStored,
                )
                .map_err(E::from)?;
                self.transition_upload(
                    intent.intent_id(),
                    shardline_index::UploadIntentState::MetadataCommitted,
                )
                .await
                .map_err(E::from)?;
                upload_lifecycle_failpoint(
                    intent.intent_id(),
                    UploadLifecycleBoundary::AfterMetadataCommitted,
                )
                .map_err(E::from)?;
                self.transition_upload(
                    intent.intent_id(),
                    shardline_index::UploadIntentState::Visible,
                )
                .await
                .map_err(E::from)?;
                upload_lifecycle_failpoint(
                    intent.intent_id(),
                    UploadLifecycleBoundary::AfterVisible,
                )
                .map_err(E::from)?;
                Ok(result)
            }
            // A returned error does not establish whether an object-store or
            // metadata commit happened before its acknowledgement was lost.
            // Keep the intent at its last durable in-flight boundary so an
            // idempotent retry or startup reconciliation can inspect reality.
            // Marking it terminal here would make an ambiguous success
            // unrecoverable and could let one failed duplicate poison a
            // concurrent winner.
            Err(error) => Err(error),
        }
    }

    /// Creates a durable upload intent before any object-store mutation.
    ///
    /// # Errors
    ///
    /// Returns CasError when the intent cannot be persisted.
    pub async fn begin_upload(
        &self,
        intent: &shardline_index::UploadIntent,
    ) -> Result<(), CasError> {
        self.index
            .create_intent(intent)
            .await
            .map_err(map_upload_intent_store_error)?;
        let stored = self
            .index
            .intent_by_id(intent.intent_id())
            .await
            .map_err(CasError::from_record)?;
        if stored.is_some_and(|stored| stored.has_same_identity(intent)) {
            Ok(())
        } else {
            Err(CasError::InvalidUploadTransition)
        }
    }

    /// Records one validated persistence boundary for an upload.
    ///
    /// Transitions are idempotent and tolerant of a concurrent duplicate caller:
    /// if the intent is already at or beyond the target state in the committed
    /// chain, the boundary is considered already reached and no error is raised.
    ///
    /// # Errors
    ///
    /// Returns CasError when the transition is invalid, missing, or cannot be
    /// persisted.
    pub async fn transition_upload(
        &self,
        intent_id: &str,
        next: shardline_index::UploadIntentState,
    ) -> Result<(), CasError> {
        let current = self
            .index
            .intent_by_id(intent_id)
            .await
            .map_err(CasError::from_record)?;
        let Some(current) = current else {
            return Err(CasError::InvalidUploadTransition);
        };
        if current.state() == shardline_index::UploadIntentState::Failed {
            return if next == shardline_index::UploadIntentState::Failed {
                Ok(())
            } else {
                Err(CasError::InvalidUploadTransition)
            };
        }
        // Already at or past the target in the committed chain: a concurrent
        // duplicate caller advanced it, so the boundary is effectively reached.
        if next != shardline_index::UploadIntentState::Failed
            && committed_state_rank(current.state()) >= committed_state_rank(next)
        {
            return Ok(());
        }
        let transitioned = self
            .index
            .transition_intent(intent_id, next)
            .await
            .map_err(CasError::from_record)?;
        if transitioned {
            return Ok(());
        }
        // The store's atomic read raced with a concurrent duplicate caller and
        // observed a state ahead of `next`, so it declined the backward-looking
        // conditional update. Re-check: if the intent is now at or past the
        // target, the boundary is effectively already reached.
        let after = self
            .index
            .intent_by_id(intent_id)
            .await
            .map_err(CasError::from_record)?;
        match after {
            Some(intent) if intent.state() == shardline_index::UploadIntentState::Failed => {
                if next == shardline_index::UploadIntentState::Failed {
                    Ok(())
                } else {
                    Err(CasError::InvalidUploadTransition)
                }
            }
            Some(intent)
                if next != shardline_index::UploadIntentState::Failed
                    && committed_state_rank(intent.state()) >= committed_state_rank(next) =>
            {
                Ok(())
            }
            _ => Err(CasError::InvalidUploadTransition),
        }
    }
}

fn map_upload_intent_store_error(error: impl std::error::Error + 'static) -> CasError {
    let message = error.to_string();
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = source {
        if current
            .downcast_ref::<shardline_index::UploadIntentConflictError>()
            .is_some()
        {
            return CasError::InvalidUploadTransition;
        }
        source = current.source();
    }
    CasError::Record(message)
}

/// Returns the order of an upload-intent state along the committed chain,
/// used to detect "already at-or-past" for idempotent forward transitions.
/// `Failed` is terminal and out of the chain (rank `u8::MAX`).
const fn committed_state_rank(state: shardline_index::UploadIntentState) -> u8 {
    match state {
        shardline_index::UploadIntentState::Created => 0,
        shardline_index::UploadIntentState::Storing => 1,
        shardline_index::UploadIntentState::Stored => 2,
        shardline_index::UploadIntentState::MetadataCommitted => 3,
        shardline_index::UploadIntentState::Visible => 4,
        shardline_index::UploadIntentState::Failed => 0,
    }
}

#[async_trait::async_trait]
impl<I, O, R> ObjectReachability for CasCoordinator<I, O, R>
where
    I: AsyncIndexStore + Send + Sync,
    <I as AsyncIndexStore>::Error: std::error::Error + Send + Sync + 'static,
    O: Sync,
    R: Sync,
{
    async fn is_object_reachable(&self, object_id: &StoredObjectId) -> Result<bool, CasError> {
        let object_id = *object_id;
        AsyncIndexStore::contains_object(&self.index, &object_id)
            .await
            .map_err(CasError::from_index)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use shardline_index::{MemoryIndexStore, UploadIntent, UploadIntentState, UploadIntentStore};
    use shardline_protocol::ByteRange;
    use shardline_storage::{
        AsyncObjectStore, DeleteOutcome, LocalObjectStore, ObjectBody, ObjectIntegrity, ObjectKey,
        ObjectMetadata, ObjectPrefix, PutOutcome, SyncObjectStoreBridge,
    };
    use std::{
        num::NonZeroU64,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use super::CasCoordinator;
    use crate::{CasError, CasLimits, UploadLifecycleBoundary, arm_upload_interruption};

    #[derive(Debug, PartialEq, Eq)]
    struct IndexProbe;
    #[derive(Debug, PartialEq, Eq)]
    struct ObjectStoreProbe;
    #[derive(Debug, PartialEq, Eq)]
    struct RecordStoreProbe;

    #[derive(Debug, thiserror::Error)]
    enum LostAcknowledgementError {
        #[error("object-store acknowledgement was lost after commit")]
        LostAfterCommit,
        #[error("inner object-store operation failed: {0}")]
        Inner(String),
    }

    #[derive(Clone)]
    struct LostPutAcknowledgementStore {
        inner: SyncObjectStoreBridge<LocalObjectStore>,
        lose_next_put_acknowledgement: Arc<AtomicBool>,
    }

    impl LostPutAcknowledgementStore {
        fn new(inner: LocalObjectStore) -> Self {
            Self {
                inner: SyncObjectStoreBridge::new(inner),
                lose_next_put_acknowledgement: Arc::new(AtomicBool::new(true)),
            }
        }
    }

    #[async_trait::async_trait]
    impl AsyncObjectStore for LostPutAcknowledgementStore {
        type Error = LostAcknowledgementError;

        async fn put_if_absent(
            &self,
            key: &ObjectKey,
            body: ObjectBody<'_>,
            integrity: &ObjectIntegrity,
        ) -> Result<PutOutcome, Self::Error> {
            let outcome = self
                .inner
                .put_if_absent(key, body, integrity)
                .await
                .map_err(|error| LostAcknowledgementError::Inner(error.to_string()))?;
            if self
                .lose_next_put_acknowledgement
                .swap(false, Ordering::SeqCst)
            {
                return Err(LostAcknowledgementError::LostAfterCommit);
            }
            Ok(outcome)
        }

        async fn read_range(
            &self,
            key: &ObjectKey,
            range: ByteRange,
        ) -> Result<Vec<u8>, Self::Error> {
            self.inner
                .read_range(key, range)
                .await
                .map_err(|error| LostAcknowledgementError::Inner(error.to_string()))
        }

        async fn contains(&self, key: &ObjectKey) -> Result<bool, Self::Error> {
            self.inner
                .contains(key)
                .await
                .map_err(|error| LostAcknowledgementError::Inner(error.to_string()))
        }

        async fn metadata(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>, Self::Error> {
            self.inner
                .metadata(key)
                .await
                .map_err(|error| LostAcknowledgementError::Inner(error.to_string()))
        }

        async fn list_prefix(
            &self,
            prefix: &ObjectPrefix,
        ) -> Result<Vec<ObjectMetadata>, Self::Error> {
            self.inner
                .list_prefix(prefix)
                .await
                .map_err(|error| LostAcknowledgementError::Inner(error.to_string()))
        }

        async fn delete_if_present(&self, key: &ObjectKey) -> Result<DeleteOutcome, Self::Error> {
            self.inner
                .delete_if_present(key)
                .await
                .map_err(|error| LostAcknowledgementError::Inner(error.to_string()))
        }
    }

    #[test]
    fn coordinator_keeps_adapters_and_limits() {
        let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MAX, NonZeroU64::MIN);
        let c = CasCoordinator::new(IndexProbe, ObjectStoreProbe, RecordStoreProbe, limits);
        assert_eq!(c.index(), &IndexProbe);
        assert_eq!(c.object_store(), &ObjectStoreProbe);
        assert_eq!(c.record_store(), &RecordStoreProbe);
        assert_eq!(c.limits(), limits);
    }

    #[test]
    fn coordinator_debug_format() {
        let limits = CasLimits::new(
            NonZeroU64::new(1).unwrap(),
            NonZeroU64::new(2).unwrap(),
            NonZeroU64::new(3).unwrap(),
        );
        let c = CasCoordinator::new(IndexProbe, ObjectStoreProbe, RecordStoreProbe, limits);
        let debug = format!("{c:?}");
        assert!(debug.contains("CasCoordinator"));
        assert!(debug.contains("RecordStoreProbe"));
    }

    #[test]
    fn coordinator_with_different_types() {
        let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MIN, NonZeroU64::MIN);
        let c = CasCoordinator::new(
            42_usize,
            String::from("store"),
            String::from("records"),
            limits,
        );
        assert_eq!(c.index(), &42_usize);
        assert_eq!(c.object_store(), &"store");
        assert_eq!(c.record_store(), &"records");
        assert_eq!(c.limits(), limits);
    }

    #[test]
    fn coordinator_limits_independent() {
        let a = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MIN, NonZeroU64::MIN);
        let b = CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX);
        assert_ne!(
            CasCoordinator::new((), (), (), a).limits(),
            CasCoordinator::new((), (), (), b).limits()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn content_addressed_retry_recovers_from_lost_put_acknowledgement() {
        let storage = tempfile::tempdir().unwrap();
        let object_store = LostPutAcknowledgementStore::new(
            LocalObjectStore::new(storage.path().join("objects")).unwrap(),
        );
        let coordinator = CasCoordinator::new(
            MemoryIndexStore::new(),
            object_store.clone(),
            (),
            CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX),
        );
        let key = ObjectKey::parse("objects/lost-ack").unwrap();
        let body = b"durably accepted before response loss".to_vec();
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(&body).as_bytes()),
            u64::try_from(body.len()).unwrap(),
        );

        let first = coordinator
            .store_content_addressed_blob(&key, &integrity, body.clone())
            .await;
        assert!(matches!(first, Err(CasError::ObjectStore(_))));
        assert!(object_store.contains(&key).await.unwrap());

        let retry = coordinator
            .store_content_addressed_blob(&key, &integrity, body.clone())
            .await;
        assert_eq!(retry, Ok(PutOutcome::AlreadyExists));
        let end = u64::try_from(body.len()).unwrap().saturating_sub(1);
        assert_eq!(
            object_store
                .read_range(&key, ByteRange::new(0, end).unwrap())
                .await
                .unwrap(),
            body
        );
    }

    #[test]
    fn with_upload_intent_success_transitions_to_visible() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let index = MemoryIndexStore::new();
        let object_store = SyncObjectStoreBridge::new(
            LocalObjectStore::new(storage.path().join("objects")).unwrap(),
        );
        let limits = CasLimits::new(
            NonZeroU64::new(100).unwrap(),
            NonZeroU64::new(100).unwrap(),
            NonZeroU64::new(100).unwrap(),
        );
        let coordinator = CasCoordinator::new(index.clone(), object_store, (), limits);

        let intent = UploadIntent::new(
            "success-intent".to_owned(),
            "objects/test".to_owned(),
            "abcdef".to_owned(),
            42,
        );

        let result: Result<i32, CasError> =
            rt.block_on(coordinator.with_upload_intent(&intent, || async { Ok(42) }));

        assert_eq!(result, Ok(42));

        let stored = rt
            .block_on(index.intent_by_id("success-intent"))
            .unwrap()
            .unwrap();
        assert_eq!(stored.state(), UploadIntentState::Visible);
    }

    #[test]
    fn with_upload_intent_failure_remains_recoverable() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let index = MemoryIndexStore::new();
        let object_store = SyncObjectStoreBridge::new(
            LocalObjectStore::new(storage.path().join("objects")).unwrap(),
        );
        let limits = CasLimits::new(
            NonZeroU64::new(100).unwrap(),
            NonZeroU64::new(100).unwrap(),
            NonZeroU64::new(100).unwrap(),
        );
        let coordinator = CasCoordinator::new(index.clone(), object_store, (), limits);

        let intent = UploadIntent::new(
            "failure-intent".to_owned(),
            "objects/test".to_owned(),
            "abcdef".to_owned(),
            42,
        );

        let result: Result<i32, CasError> = rt.block_on(
            coordinator.with_upload_intent(&intent, || async { Err(CasError::Overflow) }),
        );

        assert_eq!(result, Err(CasError::Overflow));

        let stored = rt
            .block_on(index.intent_by_id("failure-intent"))
            .unwrap()
            .unwrap();
        assert_eq!(stored.state(), UploadIntentState::Storing);
    }

    #[test]
    fn with_upload_intent_retry_after_ambiguous_error_can_complete() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let index = MemoryIndexStore::new();
        let coordinator = CasCoordinator::new(
            index.clone(),
            (),
            (),
            CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX),
        );
        let intent = UploadIntent::new(
            "ambiguous-intent".to_owned(),
            "objects/test".to_owned(),
            "abcdef".to_owned(),
            42,
        );

        let first: Result<i32, CasError> =
            rt.block_on(coordinator.with_upload_intent(&intent, || async {
                Err(CasError::ObjectStore("lost acknowledgement".to_owned()))
            }));
        let retry: Result<i32, CasError> =
            rt.block_on(coordinator.with_upload_intent(&intent, || async { Ok(42) }));

        assert!(matches!(first, Err(CasError::ObjectStore(_))));
        assert_eq!(retry, Ok(42));
        let stored = rt
            .block_on(index.intent_by_id(intent.intent_id()))
            .unwrap()
            .unwrap();
        assert_eq!(stored.state(), UploadIntentState::Visible);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_duplicate_does_not_poison_visible_winner() {
        let index = MemoryIndexStore::new();
        let coordinator = CasCoordinator::new(
            index.clone(),
            (),
            (),
            CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX),
        );
        let intent = UploadIntent::new(
            "concurrent-ambiguous-intent".to_owned(),
            "objects/test".to_owned(),
            "abcdef".to_owned(),
            42,
        );

        let (failed, winner): (Result<i32, CasError>, Result<i32, CasError>) = tokio::join!(
            coordinator.with_upload_intent(&intent, || async {
                tokio::task::yield_now().await;
                Err(CasError::ObjectStore("lost acknowledgement".to_owned()))
            }),
            coordinator.with_upload_intent(&intent, || async { Ok(42) }),
        );

        assert!(matches!(failed, Err(CasError::ObjectStore(_))));
        assert_eq!(winner, Ok(42));
        let stored = index
            .intent_by_id(intent.intent_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state(), UploadIntentState::Visible);
    }

    fn upload_lifecycle_boundary_strategy() -> impl Strategy<Value = UploadLifecycleBoundary> {
        prop_oneof![
            Just(UploadLifecycleBoundary::AfterIntentCreated),
            Just(UploadLifecycleBoundary::AfterStoring),
            Just(UploadLifecycleBoundary::AfterObjectWork),
            Just(UploadLifecycleBoundary::AfterStored),
            Just(UploadLifecycleBoundary::AfterMetadataCommitted),
            Just(UploadLifecycleBoundary::AfterVisible),
        ]
    }

    fn durable_state_at_boundary(boundary: UploadLifecycleBoundary) -> UploadIntentState {
        match boundary {
            UploadLifecycleBoundary::AfterIntentCreated => UploadIntentState::Created,
            UploadLifecycleBoundary::AfterStoring | UploadLifecycleBoundary::AfterObjectWork => {
                UploadIntentState::Storing
            }
            UploadLifecycleBoundary::AfterStored => UploadIntentState::Stored,
            UploadLifecycleBoundary::AfterMetadataCommitted => UploadIntentState::MetadataCommitted,
            UploadLifecycleBoundary::AfterVisible => UploadIntentState::Visible,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn every_upload_lifecycle_interruption_is_recoverable(
            boundary in upload_lifecycle_boundary_strategy(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let index = MemoryIndexStore::new();
            let coordinator = CasCoordinator::new(
                index.clone(),
                (),
                (),
                CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX),
            );
            let intent_id = format!("fault-{boundary:?}");
            let intent = UploadIntent::new(
                intent_id.clone(),
                "objects/test".to_owned(),
                "abcdef".to_owned(),
                42,
            );
            let work_calls = Arc::new(AtomicUsize::new(0));
            let guard = arm_upload_interruption(intent_id.clone(), boundary);
            let first_calls = Arc::clone(&work_calls);
            let first: Result<i32, CasError> = rt.block_on(
                coordinator.with_upload_intent(&intent, move || async move {
                    first_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(42)
                }),
            );

            prop_assert_eq!(
                first,
                Err(CasError::InjectedUploadInterruption { boundary }),
            );
            let durable = rt
                .block_on(index.intent_by_id(&intent_id))
                .unwrap()
                .unwrap();
            prop_assert_eq!(durable.state(), durable_state_at_boundary(boundary));

            drop(guard);
            let retry_calls = Arc::clone(&work_calls);
            let retry: Result<i32, CasError> = rt.block_on(
                coordinator.with_upload_intent(&intent, move || async move {
                    retry_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(42)
                }),
            );

            prop_assert_eq!(retry, Ok(42));
            let recovered = rt
                .block_on(index.intent_by_id(&intent_id))
                .unwrap()
                .unwrap();
            prop_assert_eq!(recovered.state(), UploadIntentState::Visible);
            let expected_work_calls = match boundary {
                UploadLifecycleBoundary::AfterIntentCreated
                | UploadLifecycleBoundary::AfterStoring => 1,
                UploadLifecycleBoundary::AfterObjectWork
                | UploadLifecycleBoundary::AfterStored
                | UploadLifecycleBoundary::AfterMetadataCommitted
                | UploadLifecycleBoundary::AfterVisible => 2,
            };
            prop_assert_eq!(work_calls.load(Ordering::SeqCst), expected_work_calls);
        }
    }

    #[test]
    fn with_upload_intent_retry_does_not_reopen_visible_intent() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let index = MemoryIndexStore::new();
        let coordinator = CasCoordinator::new(
            index.clone(),
            (),
            (),
            CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX),
        );
        let intent = UploadIntent::new(
            "visible-intent".to_owned(),
            "objects/test".to_owned(),
            "abcdef".to_owned(),
            42,
        );

        let first: Result<i32, CasError> =
            rt.block_on(coordinator.with_upload_intent(&intent, || async { Ok(42) }));
        let retry: Result<i32, CasError> =
            rt.block_on(coordinator.with_upload_intent(&intent, || async { Ok(42) }));

        assert_eq!(first, Ok(42));
        assert_eq!(retry, Ok(42));
        let stored = rt
            .block_on(index.intent_by_id("visible-intent"))
            .unwrap()
            .unwrap();
        assert_eq!(stored.state(), UploadIntentState::Visible);
    }

    #[test]
    fn concurrent_with_upload_intent_same_intent_both_ok() {
        use std::sync::Arc;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let index = MemoryIndexStore::new();
        let coordinator = Arc::new(CasCoordinator::new(
            index.clone(),
            (),
            (),
            CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX),
        ));
        let intent = UploadIntent::new(
            "same-intent".to_owned(),
            "objects/test".to_owned(),
            "abcdef".to_owned(),
            42,
        );
        let handle = rt.handle().clone();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let mut handles: Vec<std::thread::JoinHandle<Result<i32, CasError>>> = Vec::new();
        for _ in 0..2 {
            let coordinator = Arc::clone(&coordinator);
            let intent = intent.clone();
            let barrier = Arc::clone(&barrier);
            let handle = handle.clone();
            handles.push(std::thread::spawn(move || {
                handle.block_on(async {
                    barrier.wait().await;
                    coordinator
                        .with_upload_intent(&intent, || async { Ok(42_i32) })
                        .await
                })
            }));
        }
        for h in handles {
            assert_eq!(
                h.join().unwrap(),
                Ok(42),
                "a concurrent caller for the same intent must not get a spurious \
                 InvalidUploadTransition (5xx)"
            );
        }
        let stored = rt
            .block_on(index.intent_by_id("same-intent"))
            .unwrap()
            .unwrap();
        assert_eq!(stored.state(), UploadIntentState::Visible);
    }

    #[test]
    fn begin_upload_rejects_conflicting_idempotency_key() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let index = MemoryIndexStore::new();
        let coordinator = CasCoordinator::new(
            index,
            (),
            (),
            CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX),
        );
        let original = UploadIntent::new(
            "collision".to_owned(),
            "objects/a".to_owned(),
            "hash-a".to_owned(),
            1,
        );
        let conflicting = UploadIntent::new(
            "collision".to_owned(),
            "objects/b".to_owned(),
            "hash-b".to_owned(),
            2,
        );
        rt.block_on(coordinator.begin_upload(&original)).unwrap();
        assert_eq!(
            rt.block_on(coordinator.begin_upload(&conflicting)),
            Err(CasError::InvalidUploadTransition)
        );
    }

    #[test]
    fn failed_intent_cannot_resume_success_chain() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let index = MemoryIndexStore::new();
        let coordinator = CasCoordinator::new(
            index.clone(),
            (),
            (),
            CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX),
        );
        let intent = UploadIntent::new(
            "terminal-failure".to_owned(),
            "objects/a".to_owned(),
            "hash-a".to_owned(),
            1,
        );
        rt.block_on(coordinator.begin_upload(&intent)).unwrap();
        rt.block_on(coordinator.transition_upload(intent.intent_id(), UploadIntentState::Failed))
            .unwrap();
        assert_eq!(
            rt.block_on(
                coordinator.transition_upload(intent.intent_id(), UploadIntentState::Storing,)
            ),
            Err(CasError::InvalidUploadTransition)
        );
        assert_eq!(
            rt.block_on(index.intent_by_id(intent.intent_id()))
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Failed
        );
    }
}
