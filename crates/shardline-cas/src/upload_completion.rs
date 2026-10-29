//! Complete already-durable domain work without redundant metadata commits.

use shardline_index::{UploadIntentState, UploadIntentStore};
use shardline_reliability::upload_lifecycle_event;

use crate::{CasCoordinator, CasError};

impl<I, O, R> CasCoordinator<I, O, R>
where
    I: UploadIntentStore,
    I::Error: std::error::Error + Send + Sync + 'static,
{
    pub(crate) async fn complete_upload_in_scope(
        &self,
        tenant: &str,
        repository: &str,
        intent_id: &str,
    ) -> Result<(), CasError> {
        // A compatibility adapter or older process may expose intermediate
        // commits. Each declined batch can race with one of the three forward
        // completion states; resnapshot rather than reject a valid duplicate.
        for _ in 0..3 {
            let current = self
                .index()
                .intent_by_id(intent_id)
                .await
                .map_err(CasError::from_record)?
                .ok_or(CasError::InvalidUploadTransition)?;
            if current.state() == UploadIntentState::Failed {
                return Err(CasError::InvalidUploadTransition);
            }
            if current.state() == UploadIntentState::Visible {
                return Ok(());
            }
            // The caller invokes this only after its object AND domain metadata
            // work returns success. Preserve the same ordered evidence events; no
            // external work or newly asserted durability sits between these states.
            let mut previous = current.state();
            let mut transitions = Vec::with_capacity(3);
            for next in [
                UploadIntentState::Stored,
                UploadIntentState::MetadataCommitted,
                UploadIntentState::Visible,
            ] {
                if current.state().committed_rank() >= next.committed_rank() {
                    continue;
                }
                let event = upload_lifecycle_event(
                    tenant,
                    repository,
                    intent_id,
                    current.object_key(),
                    current.object_hash(),
                    previous,
                    next,
                )
                .map_err(|error| CasError::Record(error.to_string()))?;
                transitions.push((next, event));
                previous = next;
            }
            if self
                .index()
                .transition_intent_batch_with_events(intent_id, &transitions)
                .await
                .map_err(CasError::from_record)?
            {
                return Ok(());
            }
        }
        // Another identical caller may already have finished this batch.
        match self
            .index()
            .intent_by_id(intent_id)
            .await
            .map_err(CasError::from_record)?
        {
            Some(intent) if intent.state() == UploadIntentState::Visible => Ok(()),
            _ => Err(CasError::InvalidUploadTransition),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CasLimits;
    use shardline_index::{LocalIndexStore, UploadIntent, UploadIntentStore};
    use std::{num::NonZeroU64, sync::Arc};

    struct InterleavedCompletion {
        inner: shardline_index::MemoryIndexStore,
        advance_once: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl UploadIntentStore for InterleavedCompletion {
        type Error = <shardline_index::MemoryIndexStore as UploadIntentStore>::Error;

        async fn create_intent(&self, intent: &UploadIntent) -> Result<(), Self::Error> {
            self.inner.create_intent(intent).await
        }
        async fn transition_intent(
            &self,
            id: &str,
            next: UploadIntentState,
        ) -> Result<bool, Self::Error> {
            self.inner.transition_intent(id, next).await
        }
        async fn intent_by_id(&self, id: &str) -> Result<Option<UploadIntent>, Self::Error> {
            self.inner.intent_by_id(id).await
        }
        async fn intents_by_state(
            &self,
            state: UploadIntentState,
        ) -> Result<Vec<UploadIntent>, Self::Error> {
            self.inner.intents_by_state(state).await
        }
        async fn stale_intents(
            &self,
            state: UploadIntentState,
            age: std::time::Duration,
        ) -> Result<Vec<UploadIntent>, Self::Error> {
            self.inner.stale_intents(state, age).await
        }
        async fn transition_intent_batch_with_events(
            &self,
            id: &str,
            events: &[(UploadIntentState, shardline_reliability::LifecycleEvent)],
        ) -> Result<bool, Self::Error> {
            if self
                .advance_once
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                // An older process commits Stored between the coordinator's
                // snapshot and the batch's lock acquisition, then stops.
                assert!(
                    self.inner
                        .transition_intent(id, UploadIntentState::Stored)
                        .await?
                );
                return Ok(false);
            }
            self.inner
                .transition_intent_batch_with_events(id, events)
                .await
        }
    }

    #[tokio::test]
    async fn completion_resumes_after_a_legacy_writer_advances_one_boundary() {
        let index = shardline_index::MemoryIndexStore::new();
        let intent = UploadIntent::new(
            "mixed-completion".to_owned(),
            "objects/mixed".to_owned(),
            "a".repeat(64),
            42,
        );
        index.create_intent(&intent).await.unwrap();
        assert!(
            index
                .transition_intent(intent.intent_id(), UploadIntentState::Storing)
                .await
                .unwrap()
        );
        let coordinator = CasCoordinator::new(
            InterleavedCompletion {
                inner: index.clone(),
                advance_once: std::sync::atomic::AtomicBool::new(true),
            },
            (),
            (),
            CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX),
        );
        coordinator
            .complete_upload_in_scope("shardline", "default", intent.intent_id())
            .await
            .unwrap();
        assert_eq!(
            index
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Visible
        );
        assert_eq!(
            index
                .reliability_events(intent.intent_id())
                .await
                .unwrap()
                .len(),
            5
        );
    }

    #[tokio::test]
    async fn concurrent_sqlite_completion_is_idempotent_and_preserves_evidence() {
        let root = tempfile::tempdir().unwrap();
        let index = LocalIndexStore::new(root.path().to_owned()).unwrap();
        let coordinator = Arc::new(CasCoordinator::new(
            index.clone(),
            (),
            (),
            CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX),
        ));
        let intent = UploadIntent::new(
            "xorb-v2-completion-race".to_owned(),
            "objects/completion-race".to_owned(),
            "a".repeat(64),
            42,
        );
        let barrier = Arc::new(tokio::sync::Barrier::new(16));
        let mut writers = Vec::new();
        for _ in 0..16 {
            let coordinator = coordinator.clone();
            let intent = intent.clone();
            let barrier = barrier.clone();
            writers.push(tokio::spawn(async move {
                coordinator
                    .with_upload_intent(&intent, || async move {
                        barrier.wait().await;
                        Ok::<_, CasError>(())
                    })
                    .await
            }));
        }
        for writer in writers {
            writer.await.unwrap().unwrap();
        }
        assert_eq!(
            index
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Visible
        );
        assert_eq!(
            index
                .reliability_events(intent.intent_id())
                .await
                .unwrap()
                .len(),
            5
        );
        index.verify_reliability_events().unwrap();
    }
}
