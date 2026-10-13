//! Bind coordinator progress to the request's original database session.
use crate::maintenance_barrier::RequestSession;
use shardline_index::{
    PostgresIndexStore, PostgresMetadataStoreError, UploadIntent, UploadIntentState,
    UploadIntentStore,
};
use shardline_reliability::LifecycleEvent;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub(super) struct RequestUploadIntentStore {
    index: PostgresIndexStore,
    identity: Arc<()>,
    lost: AtomicBool,
}

impl RequestUploadIntentStore {
    pub(super) const fn new(index: PostgresIndexStore, identity: Arc<()>) -> Self {
        Self {
            index,
            identity,
            lost: AtomicBool::new(false),
        }
    }

    pub(super) fn owner_lost(&self) -> bool {
        self.lost.load(Ordering::Relaxed)
    }

    fn missing_owner(&self) -> PostgresMetadataStoreError {
        self.lost.store(true, Ordering::Relaxed);
        sqlx::Error::PoolClosed.into()
    }

    async fn observe_failure<T: Sync>(
        &self,
        connection: &mut sqlx::PgConnection,
        result: &Result<T, PostgresMetadataStoreError>,
    ) {
        if result.is_err() && sqlx::query("SELECT 1").execute(connection).await.is_err() {
            self.lost.store(true, Ordering::Relaxed);
        }
    }
}

#[async_trait::async_trait]
impl UploadIntentStore for RequestUploadIntentStore {
    type Error = PostgresMetadataStoreError;

    async fn create_intent(&self, intent: &UploadIntent) -> Result<(), Self::Error> {
        self.create_intent_scoped(intent, "shardline", "default")
            .await
    }

    async fn create_intent_scoped(
        &self,
        intent: &UploadIntent,
        tenant: &str,
        repository: &str,
    ) -> Result<(), Self::Error> {
        if let Some(mut owner) = RequestSession::connection_for(&self.identity).await {
            let connection = owner.as_mut().ok_or_else(|| self.missing_owner())?;
            let result = PostgresIndexStore::create_upload_intent_scoped_on_connection(
                connection, intent, tenant, repository,
            )
            .await;
            self.observe_failure(connection, &result).await;
            result
        } else {
            self.index
                .create_intent_scoped(intent, tenant, repository)
                .await
        }
    }

    async fn intent_by_id(&self, id: &str) -> Result<Option<UploadIntent>, Self::Error> {
        if let Some(mut owner) = RequestSession::connection_for(&self.identity).await {
            let connection = owner.as_mut().ok_or_else(|| self.missing_owner())?;
            let result =
                PostgresIndexStore::upload_intent_by_id_on_connection(connection, id).await;
            self.observe_failure(connection, &result).await;
            result
        } else {
            self.index.intent_by_id(id).await
        }
    }

    async fn transition_intent(
        &self,
        id: &str,
        next: UploadIntentState,
    ) -> Result<bool, Self::Error> {
        if RequestSession::connection_for(&self.identity)
            .await
            .is_none()
        {
            return self.index.transition_intent(id, next).await;
        }
        let Some(current) = self.intent_by_id(id).await? else {
            return Ok(false);
        };
        if current.state() == next {
            return Ok(true);
        }
        if !current.state().can_transition_to(next) {
            return Ok(false);
        }
        let events = self.index.reliability_events(id).await?;
        let head = events
            .last()
            .ok_or(PostgresMetadataStoreError::Reliability(
                shardline_reliability::ReliabilityError::OperationMismatch,
            ))?;
        let event = shardline_reliability::upload_lifecycle_event(
            &head.operation.tenant,
            &head.operation.repository,
            id,
            current.object_key(),
            current.object_hash(),
            current.state(),
            next,
        )?;
        self.transition_intent_with_event(id, next, &event).await
    }

    async fn transition_intent_with_event(
        &self,
        id: &str,
        next: UploadIntentState,
        event: &LifecycleEvent,
    ) -> Result<bool, Self::Error> {
        if let Some(mut owner) = RequestSession::connection_for(&self.identity).await {
            let connection = owner.as_mut().ok_or_else(|| self.missing_owner())?;
            let result = PostgresIndexStore::transition_upload_intent_with_event_on_connection(
                connection, id, next, event,
            )
            .await;
            self.observe_failure(connection, &result).await;
            result
        } else {
            self.index
                .transition_intent_with_event(id, next, event)
                .await
        }
    }

    async fn intents_by_state(
        &self,
        state: UploadIntentState,
    ) -> Result<Vec<UploadIntent>, Self::Error> {
        self.index.intents_by_state(state).await
    }
    async fn stale_intents(
        &self,
        state: UploadIntentState,
        older_than: Duration,
    ) -> Result<Vec<UploadIntent>, Self::Error> {
        self.index.stale_intents(state, older_than).await
    }
    async fn reliability_events(&self, id: &str) -> Result<Vec<LifecycleEvent>, Self::Error> {
        self.index.reliability_events(id).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::test_support::postgres_fixture::PostgresFixture;

    #[tokio::test]
    async fn lost_request_owner_cannot_create_read_or_advance_upload_intents() {
        let Some(fixture) = PostgresFixture::new().await else {
            return;
        };
        let backend = fixture.postgres();
        let index = fixture.index();
        let intent = UploadIntent::new(
            "owned-upload".to_owned(),
            "xorbs/owned".to_owned(),
            "owned-hash".to_owned(),
            12,
        );
        index.create_intent(&intent).await.unwrap();
        let before = index.reliability_events(intent.intent_id()).await.unwrap();
        let event = shardline_reliability::upload_lifecycle_event(
            "shardline",
            "default",
            intent.intent_id(),
            intent.object_key(),
            intent.object_hash(),
            UploadIntentState::Created,
            UploadIntentState::Storing,
        )
        .unwrap();
        let session = RequestSession::acquire(
            &backend.gc_barrier_pool,
            backend.request_session_identity.clone(),
        )
        .await
        .unwrap();
        session
            .scope(async {
                let mut owner = RequestSession::connection_for(&backend.request_session_identity)
                    .await
                    .unwrap();
                let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut **owner.as_mut().unwrap())
                    .await
                    .unwrap();
                drop(owner);
                assert!(
                    sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                        .bind(pid)
                        .fetch_one(&fixture.admin)
                        .await
                        .unwrap()
                );
                // Independent adapters must each detect the dead original session.
                let store = || {
                    RequestUploadIntentStore::new(
                        index.clone(),
                        backend.request_session_identity.clone(),
                    )
                };
                let creation = store();
                let other = UploadIntent::new(
                    "rejected-upload".to_owned(),
                    "xorbs/other".to_owned(),
                    "other-hash".to_owned(),
                    13,
                );
                assert!(creation.create_intent(&other).await.is_err());
                assert!(creation.owner_lost());
                let reading = store();
                assert!(reading.intent_by_id(intent.intent_id()).await.is_err());
                assert!(reading.owner_lost());
                let transition = store();
                assert!(
                    transition
                        .transition_intent_with_event(
                            intent.intent_id(),
                            UploadIntentState::Storing,
                            &event
                        )
                        .await
                        .is_err()
                );
                assert!(transition.owner_lost());
            })
            .await;
        session.finish().await;
        assert!(
            index
                .intent_by_id("rejected-upload")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            index
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Created
        );
        assert_eq!(
            serde_json::to_value(index.reliability_events(intent.intent_id()).await.unwrap())
                .unwrap(),
            serde_json::to_value(before).unwrap()
        );
        let fresh = RequestSession::acquire(
            &backend.gc_barrier_pool,
            backend.request_session_identity.clone(),
        )
        .await
        .unwrap();
        fresh
            .scope(async {
                let store = RequestUploadIntentStore::new(
                    index.clone(),
                    backend.request_session_identity.clone(),
                );
                assert!(
                    store
                        .transition_intent_with_event(
                            intent.intent_id(),
                            UploadIntentState::Storing,
                            &event
                        )
                        .await
                        .unwrap()
                );
                assert!(!store.owner_lost());
                assert_eq!(
                    store
                        .intent_by_id(intent.intent_id())
                        .await
                        .unwrap()
                        .unwrap()
                        .state(),
                    UploadIntentState::Storing
                );
            })
            .await;
        fresh.finish().await;
        fixture.finish().await;
    }
}
