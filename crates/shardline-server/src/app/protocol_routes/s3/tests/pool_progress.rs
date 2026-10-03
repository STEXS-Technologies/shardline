use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_s3_writers_and_gc_progress_with_one_connection_per_pool() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let tmp = TempDir::new().unwrap();
    let object_store =
        crate::object_store::ServerObjectStore::local(tmp.path().join("objects")).unwrap();
    let mut state = build_postgres_state(&url, tmp.path(), object_store)
        .await
        .unwrap();
    let unique_state = Arc::get_mut(&mut state).unwrap();
    let crate::ServerBackend::Postgres(backend) = &mut unique_state.backend else {
        unreachable!();
    };
    *backend = backend.clone().with_single_connection_pools(&url).unwrap();
    let work = backend.index_store().pool().clone();
    let app = s3_router(state.clone()).layer(axum::middleware::from_fn_with_state(
        state,
        crate::app::gc_write_barrier_middleware,
    ));
    let gc_pool = crate::postgres_backend::connect_postgres_metadata_pool(&url, 1).unwrap();
    let gc = crate::maintenance_barrier::acquire_postgres_exclusive(&gc_pool)
        .await
        .unwrap();
    let token = mint_token(TokenScope::Write, "pool-s3", NAME);
    let mut writers = Vec::new();
    for index in 0..16 {
        let app = app.clone();
        let auth = sigv4_auth(&token);
        writers.push(tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/pool-s3.models/progress/{index}"))
                    .header(header::AUTHORIZATION, auth)
                    .body(Body::from(format!("payload-{index}")))
                    .unwrap(),
            )
            .await
            .unwrap()
        }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        writers.iter().all(|writer| !writer.is_finished()),
        "GC must exclude all S3 mutations"
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sqlx::query("SELECT 1").execute(&work),
    )
    .await
    .unwrap()
    .unwrap();
    drop(gc);
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        for writer in writers {
            let response = writer.await.unwrap();
            let status = response.status();
            let body = body_bytes(response).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "S3 upload failed: {}",
                String::from_utf8_lossy(&body)
            );
        }
    })
    .await
    .expect("all nested GC/resource/index uploads must complete");
    let token = mint_token(TokenScope::Read, "pool-s3", NAME);
    for index in 0..16 {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/pool-s3.models/progress/{index}"))
                    .header(header::AUTHORIZATION, sigv4_auth(&token))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_bytes(response).await,
            format!("payload-{index}").as_bytes()
        );
    }
}
