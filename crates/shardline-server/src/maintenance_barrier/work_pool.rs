use sqlx::PgPool;

/// Closes a work pool when its operation exits or is cancelled.
/// Borrowed connections retain their original session locks until released;
/// closing a pool does not authorize competing work to bypass those locks.
pub(crate) struct WorkPoolCleanup(PgPool);

impl WorkPoolCleanup {
    pub(crate) fn new(pool: &PgPool) -> Self {
        Self(pool.clone())
    }
}

impl Drop for WorkPoolCleanup {
    fn drop(&mut self) {
        close_work_pool(&self.0);
    }
}

pub(crate) fn close_work_pool(pool: &PgPool) {
    // Closing is synchronous; awaiting completes physical connection cleanup.
    drop(pool.close());
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        let pool = pool.clone();
        runtime.spawn(async move { pool.close().await });
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test(flavor = "multi_thread")]
    #[allow(clippy::unwrap_used)]
    async fn cancellation_keeps_borrowed_connection_owned_until_release() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let owner_url = url.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let owner = tokio::spawn(async move {
            let pool = super::super::postgres_exclusive_work_pool(&owner_url)
                .await
                .unwrap();
            let _cleanup = super::WorkPoolCleanup::new(&pool);
            let connection = pool.acquire().await.unwrap();
            sender.send((pool.clone(), connection)).unwrap();
            std::future::pending::<()>().await;
        });
        let (retained, mut connection) = receiver.await.unwrap();
        let mut waiter =
            tokio::spawn(async move { super::super::postgres_exclusive_work_pool(&url).await });
        owner.abort();
        assert!(owner.await.unwrap_err().is_cancelled());
        assert!(retained.is_closed());
        assert!(matches!(
            retained.acquire().await,
            Err(sqlx::Error::PoolClosed)
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut waiter)
                .await
                .is_err(),
            "replacement must not bypass the borrowed original owner"
        );
        let owns_lock: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND granted)")
            .fetch_one(&mut *connection).await.unwrap();
        assert!(owns_lock);
        drop(connection);
        let replacement = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        replacement.close().await;
        retained.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[allow(clippy::unwrap_used)]
    async fn cancelled_operation_releases_postgres_ownership_with_retained_handles() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let owner_url = url.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let owner = tokio::spawn(async move {
            let pool = super::super::postgres_exclusive_work_pool(&owner_url)
                .await
                .unwrap();
            let _cleanup = super::WorkPoolCleanup::new(&pool);
            sender.send(pool.clone()).unwrap();
            std::future::pending::<()>().await;
        });
        let retained = receiver.await.unwrap();
        let waiter_url = url.clone();
        let mut waiter =
            tokio::spawn(
                async move { super::super::postgres_exclusive_work_pool(&waiter_url).await },
            );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut waiter)
                .await
                .is_err(),
            "the active owner must still exclude a competing operation"
        );
        owner.abort();
        assert!(owner.await.unwrap_err().is_cancelled());
        assert!(retained.is_closed());
        assert!(matches!(
            retained.acquire().await,
            Err(sqlx::Error::PoolClosed)
        ));
        let replacement = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        replacement.close().await;
        retained.close().await;
    }

    #[tokio::test]
    #[allow(clippy::panic_in_result_fn)] // Assertions distinguish cleanup failure from SQL errors.
    async fn operation_exit_closes_retained_handles() -> Result<(), sqlx::Error> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused_cleanup_fixture")?;
        let retained = pool.clone();
        let cleanup = super::WorkPoolCleanup::new(&pool);
        drop(pool);
        drop(cleanup);
        assert!(retained.is_closed());
        assert!(matches!(
            retained.acquire().await,
            Err(sqlx::Error::PoolClosed)
        ));
        Ok(())
    }
}
