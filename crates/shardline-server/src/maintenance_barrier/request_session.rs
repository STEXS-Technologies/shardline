use std::sync::Arc;

use sqlx::{PgPool, Postgres, pool::PoolConnection};

use super::GC_WRITE_BARRIER_KEY;
use crate::ServerError;

#[cfg(test)]
mod tests;

tokio::task_local! {
    static REQUEST_SESSION: RequestSession;
}

#[derive(Clone)]
/// One request's non-replaceable lock-owning session. Resource publications
/// borrow this connection rather than acquiring an independent session. The
/// backend identity prevents a nested call to another backend borrowing it.
pub(crate) struct RequestSession {
    backend: Arc<()>,
    connection: Arc<tokio::sync::Mutex<RequestConnection>>,
}

pub(crate) struct RequestConnection {
    connection: Option<PoolConnection<Postgres>>,
    // A mapped resource guard may outlive its request. Retain admission with
    // the connection state until the final borrower releases ownership.
    _slot: Option<tokio::sync::OwnedSemaphorePermit>,
}

pub(crate) type RequestConnectionGuard =
    tokio::sync::OwnedMappedMutexGuard<RequestConnection, Option<PoolConnection<Postgres>>>;

impl RequestSession {
    /// Public embedded uploads establish ownership automatically. Nested calls
    /// retain the existing owner, including its lost-connection state.
    #[cfg(test)]
    pub(crate) async fn scope_if_needed<T>(
        pool: &PgPool,
        backend: &Arc<()>,
        future: impl std::future::Future<Output = Result<T, ServerError>>,
    ) -> Result<T, ServerError> {
        Self::scope_inner(pool, backend, None, future).await
    }

    pub(crate) async fn scope_bounded_if_needed<T>(
        pool: &PgPool,
        backend: &Arc<()>,
        slots: &Arc<tokio::sync::Semaphore>,
        future: impl std::future::Future<Output = Result<T, ServerError>>,
    ) -> Result<T, ServerError> {
        Self::scope_inner(pool, backend, Some(slots), future).await
    }

    async fn scope_inner<T>(
        pool: &PgPool,
        backend: &Arc<()>,
        slots: Option<&Arc<tokio::sync::Semaphore>>,
        future: impl std::future::Future<Output = Result<T, ServerError>>,
    ) -> Result<T, ServerError> {
        let already_owned = REQUEST_SESSION
            .try_with(|session| Arc::ptr_eq(backend, &session.backend))
            .unwrap_or(false);
        if already_owned {
            Self::verify_for(backend).await?;
            return future.await;
        }
        let session = Self::acquire_inner(pool, backend.clone(), slots).await?;
        let result = session.scope(future).await;
        session.finish().await;
        result
    }

    /// Rejects a lost request session without acquiring a replacement connection.
    pub(crate) async fn verify_for(backend: &Arc<()>) -> Result<(), ServerError> {
        if let Some(mut owner) = Self::connection_for(backend).await {
            let connection = owner.as_mut().ok_or(ServerError::StaleResourceFence)?;
            sqlx::query("SELECT 1")
                .execute(&mut **connection)
                .await
                .map_err(|_error| ServerError::StaleResourceFence)?;
        }
        Ok(())
    }

    pub(crate) async fn connection_for(backend: &Arc<()>) -> Option<RequestConnectionGuard> {
        let session = REQUEST_SESSION
            .try_with(|session| Arc::ptr_eq(backend, &session.backend).then(|| session.clone()))
            .ok()
            .flatten()?;
        Some(tokio::sync::OwnedMutexGuard::map(
            session.connection.lock_owned().await,
            |owner| &mut owner.connection,
        ))
    }
    #[cfg(test)]
    pub(crate) async fn acquire(pool: &PgPool, backend: Arc<()>) -> Result<Self, ServerError> {
        Self::acquire_inner(pool, backend, None).await
    }

    pub(crate) async fn acquire_bounded(
        pool: &PgPool,
        backend: Arc<()>,
        slots: &Arc<tokio::sync::Semaphore>,
    ) -> Result<Self, ServerError> {
        Self::acquire_inner(pool, backend, Some(slots)).await
    }

    async fn acquire_inner(
        pool: &PgPool,
        backend: Arc<()>,
        slots: Option<&Arc<tokio::sync::Semaphore>>,
    ) -> Result<Self, ServerError> {
        // Admission and checkout share the existing timeout. A queued owner
        // must not occupy metadata headroom before it receives admission.
        let (mut connection, slot) =
            tokio::time::timeout(pool.options().get_acquire_timeout(), async {
                let slot = match slots {
                    Some(slots) => Some(
                        Arc::clone(slots)
                            .acquire_owned()
                            .await
                            .map_err(|_error| sqlx::Error::PoolClosed)?,
                    ),
                    None => None,
                };
                Ok::<_, sqlx::Error>((pool.acquire().await?, slot))
            })
            .await
            .unwrap_or(Err(sqlx::Error::PoolTimedOut))
            .map_err(|error| {
                if matches!(error, sqlx::Error::PoolTimedOut) {
                    ServerError::WorkQueueSaturated
                } else {
                    shardline_index::PostgresMetadataStoreError::from(error).into()
                }
            })?;
        connection.close_on_drop();
        sqlx::query("SELECT pg_advisory_lock_shared($1)")
            .bind(GC_WRITE_BARRIER_KEY)
            .execute(&mut *connection)
            .await
            .map_err(shardline_index::PostgresMetadataStoreError::from)?;
        Ok(Self {
            backend,
            connection: Arc::new(tokio::sync::Mutex::new(RequestConnection {
                connection: Some(connection),
                _slot: slot,
            })),
        })
    }

    pub(crate) async fn scope<F: std::future::Future>(&self, future: F) -> F::Output {
        REQUEST_SESSION.scope(self.clone(), future).await
    }

    pub(crate) async fn finish(self) {
        if let Some(mut connection) = self.connection.lock().await.connection.take()
            && sqlx::query("SELECT pg_advisory_unlock_all()")
                .execute(&mut *connection)
                .await
                .is_ok()
        {
            connection.return_to_pool().await;
        }
    }
}
