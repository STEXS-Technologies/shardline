//! Exercise authorization-to-storage domain propagation through both HTTP frontends.
use super::*;
use sha2::Sha256;
use shardline_index::DeduplicationDomain;
use shardline_server_core::protocol_support::protocol_object_file_id;

async fn assert_http_domains(state: Arc<AppState>) {
    let lfs = Router::new()
        .route(
            "/v1/lfs/objects/{oid}",
            put(crate::app::protocol_routes::lfs::lfs_put_object),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            crate::app::gc_write_barrier_middleware,
        ))
        .with_state(state.clone());
    let app = s3_router(state.clone()).merge(lfs);
    let scope = RepositoryScope::new(RepositoryProvider::Generic, OWNER, NAME, None).unwrap();
    let namespace = crate::protocol_support::scope_namespace(Some(&scope));
    let mut random = 0x1234_5678_90ab_cdef_u64;
    let original: Vec<u8> = (0..1024 * 1024)
        .map(|_| {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random as u8
        })
        .collect();
    let response = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/domain-source"),
            original.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&body_bytes(response).await)
    );
    let source_key = shardline_s3_adapter::s3_object_key(&namespace, "domain-source").unwrap();
    let source = state
        .backend
        .protocol_file_record(&protocol_object_file_id(&source_key))
        .await
        .unwrap();
    assert_eq!(
        source.reuse_domain(),
        DeduplicationDomain::from_scope(Some(&scope))
    );

    for (index, (owner, name, reuse)) in [
        (OWNER, NAME, true),
        (OWNER, "other", false),
        ("other", NAME, false),
    ]
    .into_iter()
    .enumerate()
    {
        let domain = RepositoryScope::new(RepositoryProvider::Generic, owner, name, None).unwrap();
        let mut edited = original.clone();
        edited[512 * 1024..512 * 1024 + 4096].fill(u8::try_from(index + 1).unwrap());
        let oid = hex::encode(Sha256::digest(&edited));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", mint_token(TokenScope::Write, owner, name)),
                    )
                    .body(Body::from(edited))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "{} {}",
            response.status(),
            String::from_utf8_lossy(&body_bytes(response).await)
        );
        let key = shardline_storage::ObjectKey::parse(&format!(
            "protocols/lfs/{}/objects/{oid}",
            crate::protocol_support::scope_namespace(Some(&domain))
        ))
        .unwrap();
        let record = state
            .backend
            .protocol_file_record(&protocol_object_file_id(&key))
            .await
            .unwrap();
        assert_eq!(
            record.reuse_domain(),
            DeduplicationDomain::from_scope(Some(&domain))
        );
        assert_eq!(
            record
                .chunks
                .iter()
                .any(|term| source.chunks.iter().any(|prior| prior.hash == term.hash)),
            reuse
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_http_s3_and_lfs_preserve_deduplication_domains() {
    let (state, _root) = build_test_state().await;
    assert_http_domains(state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_http_s3_and_lfs_preserve_deduplication_domains() {
    let Some((state, _root)) = build_postgres_test_state().await else {
        return;
    };
    assert_http_domains(state).await;
}
