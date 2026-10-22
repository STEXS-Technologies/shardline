//! Fixture-only mutations of derived lookup metadata; domain records stay intact.
use super::*;

mod reclamation;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires PostgreSQL DATABASE_URL and disposable TEST_S3_* storage"]
async fn minio_oci_referrers_survive_replica_replacement() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
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
    .with_key_prefix(Some(&format!(
        "oci-index-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    )));
    exercise_referrers(Some(&database), Some(storage)).await;
}

fn connection_options(database: &str, extra: &str) -> String {
    let mut url = url::Url::parse(database).unwrap();
    let options = url
        .query_pairs()
        .find(|(key, _)| key == "options")
        .map_or_else(String::new, |(_, value)| value.into_owned());
    let retained: Vec<_> = url
        .query_pairs()
        .filter(|(key, _)| key != "options")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    url.set_query(None);
    url.query_pairs_mut()
        .extend_pairs(retained)
        .append_pair("options", &format!("{options} {extra}"));
    url.into()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable PostgreSQL DATABASE_URL with role administration"]
async fn postgres_readonly_referrers_preserve_legacy_discovery_without_index_writes() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    let admin = sqlx::PgPool::connect(&database).await.unwrap();
    let suffix = format!(
        "{}_{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    let schema = format!("oci_readonly_{suffix}");
    let role = format!("oci_reader_{suffix}");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
        .execute(&admin)
        .await
        .unwrap();
    let writer_url = connection_options(&database, &format!("-csearch_path={schema}"));
    let root = tempfile::tempdir().unwrap();
    let writer = start(root.path(), ServerFrontend::Oci, false, Some(&writer_url)).await;
    let client = Client::new();
    let repository = "readonly/referrers";
    let subject = format!("sha256:{}", "a".repeat(64));
    let mut expected = Vec::new();
    for artifact in ["application/example.first", "application/example.second"] {
        let document = json!({"schemaVersion":2,"mediaType":INDEX,"manifests":[],"artifactType":artifact,"subject":{"digest":subject,"mediaType":INDEX,"size":1}});
        let digest = publish(&client, &writer.base, repository, &document).await;
        evict(root.path(), Some(&writer_url), repository, &digest).await;
        expected.push(digest);
    }
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE \"{role}\" NOLOGIN;
        GRANT USAGE ON SCHEMA \"{schema}\" TO \"{role}\";
        GRANT SELECT ON ALL TABLES IN SCHEMA \"{schema}\" TO \"{role}\";"
    )))
    .execute(&admin)
    .await
    .unwrap();
    let reader_url = connection_options(
        &writer_url,
        &format!("-crole={role} -cdefault_transaction_read_only=on"),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let config = ServerConfig::new(
        listener.local_addr().unwrap(),
        base.clone(),
        root.path().to_owned(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_frontends(vec![ServerFrontend::Oci])
    .unwrap()
    .with_reconstruction_cache_disabled()
    .with_index_postgres_url(reader_url.clone())
    .unwrap();
    let (router, background) = app::router_with_background_tasks(config).await.unwrap();
    let reader = Runtime {
        base,
        task: tokio::spawn(async move { axum::serve(listener, router).await }),
        background: Some(background),
    };
    let probe = sqlx::PgPool::connect(&reader_url).await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>("SELECT current_setting('transaction_read_only')='on' AND NOT has_table_privilege(current_user,'shardline_oci_manifest_subjects_v1','INSERT')")
        .fetch_one(&probe).await.unwrap());
    probe.close().await;
    let url = format!("{}/v2/{repository}/referrers/{subject}", reader.base);
    let response = client.get(&url).query(&[("n", "1")]).send().await.unwrap();
    let status = response.status();
    let next = response
        .headers()
        .get("link")
        .map(|value| value.to_str().unwrap().to_owned());
    let body = response.text().await.unwrap();
    let mut actual = Vec::new();
    if status == StatusCode::OK {
        let first: Value = serde_json::from_str(&body).unwrap();
        actual.push(first["manifests"][0]["digest"].as_str().unwrap().to_owned());
        let next = next.unwrap();
        let path = next.split_once('<').unwrap().1.split_once('>').unwrap().0;
        let second: Value = client
            .get(format!("{}{path}", reader.base))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        actual.push(
            second["manifests"][0]["digest"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    reader.stop().await;
    let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM \"{schema}\".shardline_oci_manifest_subjects_v1 WHERE repository=$1"
    )))
    .bind(repository)
    .fetch_one(&admin)
    .await
    .unwrap();
    // A writable deployment without the optional owner upgrade must retain
    // both publication and authoritative discovery, without runtime DDL.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TABLE \"{schema}\".shardline_oci_manifest_subjects_v1"
    )))
    .execute(&admin)
    .await
    .unwrap();
    let third = json!({"schemaVersion":2,"mediaType":INDEX,"manifests":[],"artifactType":"application/example.third","subject":{"digest":subject,"mediaType":INDEX,"size":1}});
    publish(&client, &writer.base, repository, &third).await;
    let legacy_response = client
        .get(format!(
            "{}/v2/{repository}/referrers/{subject}",
            writer.base
        ))
        .send()
        .await
        .unwrap();
    let legacy_status = legacy_response.status();
    let legacy_body: Value = legacy_response.json().await.unwrap();
    let absent: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NULL")
        .bind(format!("{schema}.shardline_oci_manifest_subjects_v1"))
        .fetch_one(&admin)
        .await
        .unwrap();
    writer.stop().await;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP OWNED BY \"{role}\"; DROP ROLE \"{role}\"; DROP SCHEMA \"{schema}\" CASCADE;"
    )))
    .execute(&admin)
    .await
    .unwrap();
    admin.close().await;
    assert_eq!(
        status,
        StatusCode::OK,
        "read-only legacy discovery failed: {body}"
    );
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
    assert_eq!(
        count, 0,
        "read-only discovery must not backfill lookup metadata"
    );
    assert_eq!(legacy_status, StatusCode::OK);
    assert_eq!(legacy_body["manifests"].as_array().unwrap().len(), 3);
    assert!(
        absent,
        "ordinary requests must not create optional PostgreSQL tables"
    );
}

pub(super) async fn orphan(root: &Path, database: Option<&str>, repository: &str, subject: &str) {
    let entry = shardline_index::OciManifestSubject {
        scope_namespace: "global".to_owned(),
        repository: repository.to_owned(),
        digest_hex: "c".repeat(64),
        subject_digest_hex: Some(subject.strip_prefix("sha256:").unwrap().to_owned()),
    };
    if let Some(database) = database {
        let pool = sqlx::PgPool::connect(database).await.unwrap();
        shardline_index::PostgresIndexStore::new(pool.clone())
            .retain_oci_manifest_subject(&entry)
            .await
            .unwrap();
        pool.close().await;
    } else {
        let root = root.to_owned();
        tokio::task::spawn_blocking(move || {
            shardline_index::LocalIndexStore::open(root)
                .retain_oci_manifest_subject(&entry)
                .unwrap()
        })
        .await
        .unwrap();
    }
}

pub(super) async fn evict(root: &Path, database: Option<&str>, repository: &str, digest: &str) {
    let digest = digest.strip_prefix("sha256:").unwrap();
    if let Some(database) = database {
        let pool = sqlx::PgPool::connect(database).await.unwrap();
        let changed = sqlx::query("DELETE FROM shardline_oci_manifest_subjects_v1 WHERE repository=$1 AND manifest_digest=$2")
            .bind(repository).bind(digest).execute(&pool).await.unwrap().rows_affected();
        assert_eq!(changed, 1);
        pool.close().await;
    } else {
        let root = root.to_owned();
        let repository = repository.to_owned();
        let digest = digest.to_owned();
        tokio::task::spawn_blocking(move || {
            let connection = rusqlite::Connection::open(root.join("metadata.sqlite3")).unwrap();
            assert_eq!(connection.execute("DELETE FROM shardline_oci_manifest_subjects_v1 WHERE repository=?1 AND manifest_digest=?2",
                rusqlite::params![repository, digest]).unwrap(), 1);
        }).await.unwrap();
    }
}

pub(super) async fn replace_commitment(
    root: &Path,
    database: Option<&str>,
    repository: &str,
    digest: &str,
    replacement: &str,
) -> String {
    let digest = digest.strip_prefix("sha256:").unwrap();
    if let Some(database) = database {
        let pool = sqlx::PgPool::connect(database).await.unwrap();
        let previous: String = sqlx::query_scalar("SELECT commitment FROM shardline_oci_manifest_subjects_v1 WHERE repository=$1 AND manifest_digest=$2")
            .bind(repository).bind(digest).fetch_one(&pool).await.unwrap();
        let changed = sqlx::query("UPDATE shardline_oci_manifest_subjects_v1 SET commitment=$3 WHERE repository=$1 AND manifest_digest=$2")
            .bind(repository).bind(digest).bind(replacement).execute(&pool).await.unwrap().rows_affected();
        assert_eq!(changed, 1);
        pool.close().await;
        previous
    } else {
        let root = root.to_owned();
        let repository = repository.to_owned();
        let digest = digest.to_owned();
        let replacement = replacement.to_owned();
        tokio::task::spawn_blocking(move || {
            let connection = rusqlite::Connection::open(root.join("metadata.sqlite3")).unwrap();
            let previous: String = connection.query_row("SELECT commitment FROM shardline_oci_manifest_subjects_v1 WHERE repository=?1 AND manifest_digest=?2",
                rusqlite::params![repository, digest], |row| row.get(0)).unwrap();
            assert_eq!(connection.execute("UPDATE shardline_oci_manifest_subjects_v1 SET commitment=?3 WHERE repository=?1 AND manifest_digest=?2",
                rusqlite::params![repository, digest, replacement]).unwrap(), 1);
            previous
        }).await.unwrap()
    }
}
