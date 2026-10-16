#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Public uploads must participate in the same GC barrier as HTTP uploads.

use crate::test_support::postgres_fixture::PostgresFixture;
use std::time::Duration;

#[tokio::test]
async fn cancelled_embedded_upload_scope_releases_gc_ownership() {
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: public upload ownership requires DATABASE_URL");
        return;
    };
    let pool = fixture.postgres().gc_barrier_pool.clone();
    let identity = fixture.postgres().request_session_identity.clone();
    let (admitted, admission) = tokio::sync::oneshot::channel();
    let upload = tokio::spawn(async move {
        crate::maintenance_barrier::RequestSession::scope_if_needed(&pool, &identity, async {
            admitted.send(()).unwrap();
            std::future::pending::<Result<(), crate::ServerError>>().await
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(10), admission)
        .await
        .unwrap()
        .unwrap();
    {
        let exclusive = crate::maintenance_barrier::acquire_postgres_exclusive(&fixture.admin);
        tokio::pin!(exclusive);
        assert!(
            tokio::time::timeout(Duration::from_millis(250), &mut exclusive)
                .await
                .is_err(),
            "active embedded work must exclude GC"
        );
        upload.abort();
        assert!(upload.await.unwrap_err().is_cancelled());
        let replacement = tokio::time::timeout(Duration::from_secs(10), exclusive)
            .await
            .unwrap()
            .unwrap();
        drop(replacement);
    }
    fixture.finish().await;
}

#[tokio::test]
async fn public_uploads_reject_lost_existing_owner_without_replacement() {
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: public upload ownership requires DATABASE_URL");
        return;
    };
    let backend = fixture.postgres();
    let session = crate::maintenance_barrier::RequestSession::acquire(
        &backend.gc_barrier_pool,
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    session
        .scope(async {
            let mut owner = crate::maintenance_barrier::RequestSession::connection_for(
                &backend.request_session_identity,
            )
            .await
            .unwrap();
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut **owner.as_mut().unwrap())
                .await
                .unwrap();
            drop(owner);
            let killed: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
                .bind(pid)
                .fetch_one(&fixture.admin)
                .await
                .unwrap();
            assert!(killed);
            let (_, canonical, hash) = crate::test_fixtures::footerless_xorb().unwrap();
            let xorb = tokio::time::timeout(
                Duration::from_secs(10),
                backend.upload_xorb(&hash, canonical),
            )
            .await
            .unwrap();
            assert!(matches!(xorb, Err(crate::ServerError::StaleResourceFence)));
            let file = tokio::time::timeout(
                Duration::from_secs(10),
                backend.upload_file(
                    "lost-library-owner",
                    axum::body::Bytes::from_static(b"body"),
                    None,
                ),
            )
            .await
            .unwrap();
            assert!(matches!(file, Err(crate::ServerError::StaleResourceFence)));
        })
        .await;
    session.finish().await;
    assert!(matches!(
        backend.read_record("lost-library-owner", None, None).await,
        Err(crate::ServerError::NotFound)
    ));
    fixture.finish().await;
}

#[tokio::test]
async fn public_xorb_upload_waits_for_exclusive_gc_without_caller_session() {
    let Some(fixture) = PostgresFixture::new().await else {
        eprintln!("SKIPPED: public upload ownership requires DATABASE_URL");
        return;
    };
    {
        let (_, canonical, hash) = crate::test_fixtures::footerless_xorb().unwrap();
        let barrier = crate::maintenance_barrier::acquire_postgres_exclusive(&fixture.admin)
            .await
            .unwrap();
        let upload = fixture.postgres().upload_xorb(&hash, canonical);
        tokio::pin!(upload);
        let while_gc_owns_storage =
            tokio::time::timeout(Duration::from_millis(250), &mut upload).await;
        drop(barrier);
        assert!(
            while_gc_owns_storage.is_err(),
            "public upload completed while exclusive GC owned the storage barrier"
        );
        tokio::time::timeout(Duration::from_secs(10), upload)
            .await
            .unwrap()
            .unwrap();
    }
    fixture.finish().await;
}
