use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_revision_register_delete_never_leaves_dangling_path() {
    let (app, tmp) = build_app(false).await;
    let id = file_id(91);
    write_record(tmp.path(), &id, 100, None).await;
    for i in 0..100 {
        let rev = format!("race{i}");
        let register_app = app.clone();
        let delete_app = app.clone();
        let reg_uri = path_url_for_rev(&rev, "victim.txt");
        let del_uri = format!("/api/{PROVIDER}/{OWNER}/{REPO}/revisions/{rev}");
        let (reg, del) = tokio::join!(
            register_app.oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(reg_uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({"fileId":id}).to_string()))
                    .unwrap()
            ),
            async {
                tokio::time::sleep(std::time::Duration::from_micros((i % 20) * 250)).await;
                delete_app
                    .oneshot(
                        Request::builder()
                            .method("DELETE")
                            .uri(del_uri)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
            }
        );
        assert!(reg.unwrap().status().is_success());
        assert!(del.unwrap().status().is_success());
        let store = shardline_index::LocalIndexStore::open(tmp.path().to_path_buf());
        let key = shardline_index::RepoKey::new(PROVIDER, OWNER, REPO);
        let tree_key = shardline_index::TreeKey::new(PROVIDER, OWNER, REPO, &rev);
        let revision = shardline_index::TreeStore::revision(&store, &key, &rev)
            .await
            .unwrap();
        let entry = shardline_index::TreeStore::tree_entry(&store, &tree_key, "victim.txt")
            .await
            .unwrap();
        assert!(
            revision.is_some() || entry.is_none(),
            "dangling HTTP tree at iteration {i}: revision absent while path present"
        );
    }
}

#[tokio::test]
async fn metadata_revision_conflict_preserves_attributes() {
    let (app, tmp) = build_app(false).await;
    let store = shardline_index::LocalIndexStore::open(tmp.path().to_path_buf());
    let key = shardline_index::RepoKey::new(PROVIDER, OWNER, REPO);
    let original = shardline_index::RevisionRecord {
        provider: PROVIDER.to_owned(),
        owner: OWNER.to_owned(),
        repo: REPO.to_owned(),
        revision: REV.to_owned(),
        created_at_unix_seconds: 11,
        updated_at_unix_seconds: 22,
    };
    assert!(
        shardline_index::TreeStore::create_revision_if_absent(&store, &original)
            .await
            .unwrap()
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/{PROVIDER}/{OWNER}/{REPO}/revisions/{REV}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        shardline_index::TreeStore::revision(&store, &key, REV)
            .await
            .unwrap()
            .unwrap(),
        original
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_metadata_revision_register_delete_never_leaves_dangling_path() {
    let Some(database_url) = std::env::var("SHARDLINE_TREE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let tmp = TempDir::new().unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:0".to_owned(),
        tmp.path().to_path_buf(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_frontends([ServerFrontend::Xet])
    .unwrap()
    .with_index_postgres_url(database_url.clone())
    .unwrap();
    let app = crate::app::router(config).await.unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    let store = shardline_index::PostgresIndexStore::new(pool.clone());
    let record_store = shardline_index::PostgresRecordStore::new(pool.clone());
    let id = file_id(92);
    RecordMutation::write_latest_record(
        &record_store,
        &FileRecord {
            file_id: id.clone(),
            content_hash: String::new(),
            total_bytes: 100,
            chunk_size: 65536,
            storage_repr: StorageRepresentation::WholeFileV1,
            repository_scope: None,
            chunks: vec![],
        },
    )
    .await
    .unwrap();
    for i in 0..100 {
        let rev = format!("pg-race-{i}");
        let (registered, deleted) = tokio::join!(
            app.clone().oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(path_url_for_rev(&rev, "file"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({"fileId":id}).to_string()))
                    .unwrap()
            ),
            async {
                tokio::time::sleep(std::time::Duration::from_micros((i % 20) * 250)).await;
                app.clone()
                    .oneshot(
                        Request::builder()
                            .method("DELETE")
                            .uri(format!("/api/{PROVIDER}/{OWNER}/{REPO}/revisions/{rev}"))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
            }
        );
        assert!(registered.unwrap().status().is_success());
        assert!(deleted.unwrap().status().is_success());
        assert!(
            shardline_index::TreeStore::revision(
                &store,
                &shardline_index::RepoKey::new(PROVIDER, OWNER, REPO),
                &rev
            )
            .await
            .unwrap()
            .is_some()
                || shardline_index::TreeStore::tree_entry(
                    &store,
                    &shardline_index::TreeKey::new(PROVIDER, OWNER, REPO, &rev),
                    "file"
                )
                .await
                .unwrap()
                .is_none()
        );
    }
    pool.close().await;
}

async fn assert_http_revision_creation_capacity(app: Router, owner: &str) {
    let responses = futures_util::future::join_all((0..16).map(|i| {
        let request_app = app.clone();
        async move {
            request_app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/{PROVIDER}/{owner}/{REPO}/revisions/quota{i}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
        }
    }))
    .await;
    let mut created_names = Vec::new();
    for response in responses {
        let status = response.status();
        let body = get_body(response).await;
        if status == StatusCode::OK {
            created_names.push(body["name"].as_str().unwrap().to_owned());
        } else {
            assert_eq!(
                status,
                StatusCode::CONFLICT,
                "unexpected creation response: {body}"
            );
            assert_eq!(
                body["error"],
                "revision registry is full for this repository"
            );
        }
    }
    assert_eq!(
        created_names.len(),
        3,
        "concurrent creation must enforce the configured cap"
    );
    let list_url = format!("/api/{PROVIDER}/{owner}/{REPO}/revisions");
    let before = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&list_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(before.status(), StatusCode::OK);
    let before = get_body(before).await;
    assert_eq!(before["revisions"].as_array().unwrap().len(), 3);
    let conflict = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/api/{PROVIDER}/{owner}/{REPO}/revisions/{}",
                    created_names[0]
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    assert_eq!(get_body(conflict).await["error"], "revision already exists");
    let after = app
        .oneshot(
            Request::builder()
                .uri(&list_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(after.status(), StatusCode::OK);
    assert_eq!(
        get_body(after).await,
        before,
        "quota rejection/conflict must not mutate metadata"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_revision_creation_enforces_capacity_under_concurrent_http() {
    let (app, _tmp) = build_app_with_cap(false, NonZeroUsize::new(3).unwrap()).await;
    assert_http_revision_creation_capacity(app, "quota-owner").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_metadata_revision_creation_enforces_capacity_under_concurrent_http() {
    let Some(database_url) = std::env::var("SHARDLINE_TREE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let tmp = TempDir::new().unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:0".to_owned(),
        tmp.path().to_path_buf(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_frontends([ServerFrontend::Xet])
    .unwrap()
    .with_max_revisions_per_repo(NonZeroUsize::new(3).unwrap())
    .unwrap()
    .with_index_postgres_url(database_url)
    .unwrap();
    assert_http_revision_creation_capacity(
        crate::app::router(config).await.unwrap(),
        "quota-owner",
    )
    .await;
}
