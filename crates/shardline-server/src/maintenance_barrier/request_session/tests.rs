use super::*;
use std::time::Duration;

#[tokio::test]
async fn saturated_request_admission_preserves_metadata_progress() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let pool = crate::postgres_backend::connect_postgres_metadata_pool(&url, 14).unwrap();
    let slots = Arc::new(tokio::sync::Semaphore::new(10));
    let identity = Arc::new(());
    let mut owners = Vec::new();
    for _ in 0..10 {
        owners.push(
            RequestSession::acquire_bounded(&pool, Arc::clone(&identity), &slots)
                .await
                .unwrap(),
        );
    }
    let waiting = RequestSession::acquire_bounded(&pool, Arc::clone(&identity), &slots);
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut waiting)
            .await
            .is_err()
    );

    // Hold all four metadata slots simultaneously while every request slot is
    // occupied. Checkout alone would not prove that queries can make progress.
    let mut metadata = Vec::new();
    for _ in 0..4 {
        let mut connection = tokio::time::timeout(Duration::from_secs(2), pool.acquire())
            .await
            .unwrap()
            .unwrap();
        let value: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(value, 1);
        metadata.push(connection);
    }
    owners.pop().unwrap().finish().await;
    let next = tokio::time::timeout(Duration::from_secs(2), &mut waiting)
        .await
        .unwrap()
        .unwrap();
    next.scope(RequestSession::verify_for(&identity))
        .await
        .unwrap();
    next.finish().await;
    for owner in owners {
        owner.finish().await;
    }
    drop(metadata);
    assert_eq!(slots.available_permits(), 10);
    pool.close().await;
}

#[tokio::test]
async fn borrowed_owner_connection_retains_admission_after_request_cancellation() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let pool = crate::postgres_backend::connect_postgres_metadata_pool(&url, 2).unwrap();
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let identity = Arc::new(());
    let owner = RequestSession::acquire_bounded(&pool, Arc::clone(&identity), &slots)
        .await
        .unwrap();
    let mut borrowed = owner
        .scope(RequestSession::connection_for(&identity))
        .await
        .unwrap();
    drop(owner);
    let waiting = RequestSession::acquire_bounded(&pool, Arc::clone(&identity), &slots);
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut waiting)
            .await
            .is_err()
    );
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut **borrowed.as_mut().unwrap())
        .await
        .unwrap();
    drop(borrowed);
    let next = tokio::time::timeout(Duration::from_secs(2), &mut waiting)
        .await
        .unwrap()
        .unwrap();
    next.scope(async {
        let mut connection = RequestSession::connection_for(&identity).await.unwrap();
        let next_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut **connection.as_mut().unwrap())
            .await
            .unwrap();
        assert_ne!(pid, next_pid, "cancelled lock owner must be closed");
    })
    .await;
    next.finish().await;
    assert_eq!(slots.available_permits(), 1);
    pool.close().await;
}
