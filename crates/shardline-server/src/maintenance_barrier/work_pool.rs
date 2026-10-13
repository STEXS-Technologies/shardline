use sqlx::PgPool;

/// Closes every handle to a work pool when its operation exits or is cancelled.
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
