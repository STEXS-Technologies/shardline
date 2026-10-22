use super::*;
use shardline_storage::ObjectStore as _;

mod crash;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oci_physical_reclamation_preserves_live_referrers() {
    exercise(None, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable PostgreSQL DATABASE_URL and TEST_S3_* storage"]
async fn minio_oci_physical_reclamation_preserves_live_referrers() {
    let database = std::env::var("DATABASE_URL").unwrap();
    let admin = sqlx::PgPool::connect(&database).await.unwrap();
    let schema = format!(
        "oci_reclamation_{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
        .execute(&admin)
        .await
        .unwrap();
    let isolated = connection_options(
        &database,
        &format!("-csearch_path={schema} -capplication_name={schema}"),
    );
    let storage = S3ObjectStoreConfig::new(
        std::env::var("TEST_S3_BUCKET").unwrap(),
        "us-east-1".to_owned(),
    )
    .with_endpoint(Some(std::env::var("TEST_S3_ENDPOINT").unwrap()))
    .with_allow_http(true)
    .with_credentials(
        Some(shardline_protocol::SecretString::new(
            std::env::var("TEST_S3_ACCESS_KEY").unwrap(),
        )),
        Some(shardline_protocol::SecretString::new(
            std::env::var("TEST_S3_SECRET_KEY").unwrap(),
        )),
        None,
    )
    .with_key_prefix(Some(&schema));
    exercise(Some(&isolated), Some(storage)).await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA \"{schema}\" CASCADE"
    )))
    .execute(&admin)
    .await
    .unwrap();
    admin.close().await;
}

async fn exercise(database: Option<&str>, storage: Option<S3ObjectStoreConfig>) {
    let root = tempfile::tempdir().unwrap();
    let fresh = tempfile::tempdir().unwrap();
    let server = start_with_storage(
        root.path(),
        ServerFrontend::Oci,
        false,
        database,
        storage.clone(),
    )
    .await;
    let client = Client::new();
    let repository = "reclamation/artifacts";
    let subject_doc = json!({"schemaVersion":2,"mediaType":INDEX,"manifests":[]});
    let subject = publish(&client, &server.base, repository, &subject_doc).await;
    let artifact = |kind: &str| json!({"schemaVersion":2,"mediaType":INDEX,"manifests":[],"artifactType":kind,"subject":{"mediaType":INDEX,"digest":subject,"size":serde_json::to_vec(&subject_doc).unwrap().len()}});
    let removed_doc = artifact("application/example.removed");
    let live_doc = artifact("application/example.live");
    let removed = publish(&client, &server.base, repository, &removed_doc).await;
    let live = publish(&client, &server.base, repository, &live_doc).await;
    let objects = storage.clone().map_or_else(
        || shardline_server_core::ServerObjectStore::local(root.path().join("chunks")).unwrap(),
        |config| shardline_server_core::ServerObjectStore::s3(config).unwrap(),
    );
    let physical = |digest: &str| {
        shardline_oci_adapter::oci_manifest_key_from_namespace(
            repository,
            digest.strip_prefix("sha256:").unwrap(),
            "global",
        )
        .unwrap()
    };
    assert!(objects.contains(&physical(&removed)).unwrap());
    let response = client
        .delete(format!(
            "{}/v2/{repository}/manifests/{removed}",
            server.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert!(
        objects.contains(&physical(&removed)).unwrap(),
        "logical deletion must retain physical bytes until GC"
    );
    let mut config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1".to_owned(),
        root.path().to_owned(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_frontends(vec![ServerFrontend::Oci])
    .unwrap();
    if let Some(database) = database {
        config = config.with_index_postgres_url(database.to_owned()).unwrap();
    }
    if let Some(storage) = storage.clone() {
        config = config.with_object_storage(ObjectStorageAdapter::S3, Some(storage));
    }
    let options = shardline_server::LocalGcOptions {
        mark: true,
        sweep: true,
        retention_seconds: 0,
        max_revisions_per_repo: None,
    };
    if let (Some(database), Some(storage)) = (database, storage.as_ref()) {
        let companion = shardline_oci_adapter::oci_manifest_media_type_key_from_namespace(
            repository,
            removed.strip_prefix("sha256:").unwrap(),
            "global",
        )
        .unwrap();
        crash::interrupt_after_physical_delete(
            database,
            storage,
            root.path(),
            &removed,
            crash::Boundary::BeforeCompanionDelete(&companion),
            None,
        )
        .await;
        assert!(!objects.contains(&physical(&removed)).unwrap());
        assert!(
            objects.contains(&companion).unwrap(),
            "first crash must precede companion deletion"
        );
        crash::interrupt_after_physical_delete(
            database,
            storage,
            root.path(),
            &removed,
            crash::Boundary::BeforeMetadataCommit,
            None,
        )
        .await;
        assert!(!objects.contains(&physical(&removed)).unwrap());
        assert!(!objects.contains(&companion).unwrap());
    }
    let report = shardline_server::run_gc(config.clone(), options)
        .await
        .unwrap();
    assert_eq!(report.reclaimed_oci_tombstones, 1);
    if let Some(database) = database {
        let pool = sqlx::PgPool::connect(database).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM shardline_oci_manifest_subjects_v1 WHERE scope_namespace='global' AND repository=$1 AND manifest_digest=$2")
            .bind(repository).bind(removed.strip_prefix("sha256:").unwrap()).fetch_one(&pool).await.unwrap();
        assert_eq!(
            count, 0,
            "completed reclamation must remove its optional lookup mapping when permitted"
        );
        pool.close().await;
    }
    assert!(!objects.contains(&physical(&removed)).unwrap());
    for digest in [&subject, &live] {
        assert!(objects.contains(&physical(digest)).unwrap());
    }
    assert_eq!(
        shardline_server::run_gc(config.clone(), options)
            .await
            .unwrap()
            .reclaimed_oci_tombstones,
        0
    );
    server.stop().await;
    let next_root = if storage.is_some() {
        fresh.path()
    } else {
        root.path()
    };
    let server = start_with_storage(
        next_root,
        ServerFrontend::Oci,
        false,
        database,
        storage.clone(),
    )
    .await;
    let response = client
        .get(format!(
            "{}/v2/{repository}/referrers/{subject}",
            server.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let listing: Value = response.json().await.unwrap();
    assert_eq!(listing["manifests"].as_array().unwrap().len(), 1);
    assert_eq!(listing["manifests"][0]["digest"], live);
    for (digest, document) in [(&subject, &subject_doc), (&live, &live_doc)] {
        let response = client
            .get(format!(
                "{}/v2/{repository}/manifests/{digest}",
                server.base
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.bytes().await.unwrap().as_ref(),
            serde_json::to_vec(document).unwrap()
        );
    }
    assert_eq!(
        publish(&client, &server.base, repository, &removed_doc).await,
        removed
    );
    assert!(objects.contains(&physical(&removed)).unwrap());
    if let (Some(database), Some(storage)) = (database, storage.as_ref()) {
        let url = format!("{}/v2/{repository}/manifests/{removed}", server.base);
        assert_eq!(
            client.delete(&url).send().await.unwrap().status(),
            StatusCode::ACCEPTED
        );
        let replacement = client
            .put(&url)
            .header("content-type", INDEX)
            .body(serde_json::to_vec(&removed_doc).unwrap());
        crash::interrupt_after_physical_delete(
            database,
            storage,
            next_root,
            &removed,
            crash::Boundary::BeforeMetadataCommit,
            Some(replacement),
        )
        .await;
        assert!(objects.contains(&physical(&removed)).unwrap());
        assert_eq!(
            shardline_server::run_gc(config, options)
                .await
                .unwrap()
                .reclaimed_oci_tombstones,
            0
        );
        let response = client.get(&url).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.bytes().await.unwrap().as_ref(),
            serde_json::to_vec(&removed_doc).unwrap()
        );
    }
    server.stop().await;
}
