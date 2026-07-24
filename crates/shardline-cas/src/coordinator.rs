use shardline_index::{AsyncIndexStore, StoredObjectId};
use shardline_storage::{
    AsyncObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, PutOutcome,
};

use crate::{CasError, CasLimits};
use crate::reachability::ObjectReachability;

#[derive(Debug)]
pub struct CasCoordinator<I, O, R> {
    index: I,
    object_store: O,
    record_store: R,
    limits: CasLimits,
}

impl<I, O, R> CasCoordinator<I, O, R> {
    pub const fn new(index: I, object_store: O, record_store: R, limits: CasLimits) -> Self {
        Self { index, object_store, record_store, limits }
    }

    pub const fn index(&self) -> &I { &self.index }

    pub const fn object_store(&self) -> &O { &self.object_store }

    pub const fn record_store(&self) -> &R { &self.record_store }

    pub const fn limits(&self) -> CasLimits { self.limits }
}

impl<I, O, R> CasCoordinator<I, O, R>
where
    I: AsyncIndexStore + Send + Sync,
    <I as AsyncIndexStore>::Error: std::error::Error + Send + Sync + 'static,
    O: AsyncObjectStore + Clone + 'static,
    <O as AsyncObjectStore>::Error: std::error::Error + Send + Sync + 'static,
{
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

    pub async fn with_upload_intent<U, F, Fut, T>(
        &self,
        intent_store: &U,
        intent: &shardline_index::UploadIntent,
        work: F,
    ) -> Result<T, CasError>
    where
        U: shardline_index::UploadIntentStore,
        U::Error: std::error::Error + Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, CasError>>,
    {
        intent_store
            .create_intent(intent)
            .await
            .map_err(|e| CasError::from_record(e))?;
        match work().await {
            Ok(result) => {
                intent_store
                    .transition_intent(intent.intent_id(), shardline_index::UploadIntentState::Visible)
                    .await
                    .map_err(|e| CasError::from_record(e))?;
                Ok(result)
            }
            Err(e) => {
                intent_store
                    .transition_intent(intent.intent_id(), shardline_index::UploadIntentState::Failed)
                    .await
                    .ok();
                Err(e)
            }
        }
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
    use std::num::NonZeroU64;
    use super::CasCoordinator;
    use crate::CasLimits;

    #[derive(Debug, PartialEq, Eq)]
    struct IndexProbe;
    #[derive(Debug, PartialEq, Eq)]
    struct ObjectStoreProbe;
    #[derive(Debug, PartialEq, Eq)]
    struct RecordStoreProbe;

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
        let limits = CasLimits::new(NonZeroU64::new(1).unwrap(), NonZeroU64::new(2).unwrap(), NonZeroU64::new(3).unwrap());
        let c = CasCoordinator::new(IndexProbe, ObjectStoreProbe, RecordStoreProbe, limits);
        let debug = format!("{c:?}");
        assert!(debug.contains("CasCoordinator"));
        assert!(debug.contains("RecordStoreProbe"));
    }

    #[test]
    fn coordinator_with_different_types() {
        let limits = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MIN, NonZeroU64::MIN);
        let c = CasCoordinator::new(42_usize, String::from("store"), String::from("records"), limits);
        assert_eq!(c.index(), &42_usize);
        assert_eq!(c.object_store(), &"store");
        assert_eq!(c.record_store(), &"records");
        assert_eq!(c.limits(), limits);
    }

    #[test]
    fn coordinator_limits_independent() {
        let a = CasLimits::new(NonZeroU64::MIN, NonZeroU64::MIN, NonZeroU64::MIN);
        let b = CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX);
        assert_ne!(CasCoordinator::new((), (), (), a).limits(), CasCoordinator::new((), (), (), b).limits());
    }
}
