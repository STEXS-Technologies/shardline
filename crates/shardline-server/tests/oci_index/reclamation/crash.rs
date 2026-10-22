//! A separate test process is killed after S3 acknowledgement and before commit.
use super::*;
use std::process::{Command, Stdio};

const APPLICATION: &str = "oci_gc_crash_worker";

pub(super) enum Boundary<'key> {
    BeforeCompanionDelete(&'key shardline_storage::ObjectKey),
    BeforeMetadataCommit,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "internal subprocess fixture; invoked by MinIO reclamation parent"]
async fn minio_gc_reclamation_worker() {
    let root = std::env::var("SHARDLINE_GC_CRASH_ROOT").expect("parent must supply worker root");
    let database = std::env::var("DATABASE_URL").unwrap();
    let prefix = std::env::var("SHARDLINE_GC_CRASH_PREFIX").unwrap();
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
    .with_key_prefix(Some(&prefix));
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1".to_owned(),
        root.into(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_frontends(vec![ServerFrontend::Oci])
    .unwrap()
    .with_index_postgres_url(database)
    .unwrap()
    .with_object_storage(ObjectStorageAdapter::S3, Some(storage));
    shardline_server::run_gc(
        config,
        shardline_server::LocalGcOptions {
            mark: true,
            sweep: true,
            retention_seconds: 0,
            max_revisions_per_repo: None,
        },
    )
    .await
    .unwrap();
    panic!("worker completed before parent interrupted the metadata commit");
}

pub(super) async fn interrupt_after_physical_delete(
    database: &str,
    storage: &S3ObjectStoreConfig,
    root: &Path,
    digest: &str,
    boundary: Boundary<'_>,
    publication: Option<reqwest::RequestBuilder>,
) {
    let pool = sqlx::PgPool::connect(database).await.unwrap();
    let application = format!(
        "{APPLICATION}_{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    let digest_hex = digest.strip_prefix("sha256:").unwrap();
    assert!(digest_hex.bytes().all(|byte| byte.is_ascii_hexdigit()));
    // PostgreSQL's numeric-to-bigint cast rounds the deletion timestamp;
    // GC's Unix-second clock floors it. Wait for actual retention eligibility
    // rather than assuming a newly acknowledged delete is already eligible.
    let mut eligible = false;
    for _ in 0..100 {
        eligible = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM shardline_oci_object_tombstones
             WHERE digest_hex=$1 AND object_kind='manifest'
             AND deleted_at_unix_seconds <= floor(extract(epoch from clock_timestamp()))::bigint)",
        )
        .bind(digest_hex)
        .fetch_one(&pool)
        .await
        .unwrap();
        if eligible {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(eligible, "target tombstone never became retention-eligible");
    let (table, operation, condition) = match boundary {
        Boundary::BeforeMetadataCommit => (
            "shardline_oci_object_tombstones",
            "DELETE",
            format!("OLD.digest_hex='{digest_hex}'"),
        ),
        Boundary::BeforeCompanionDelete(key) => (
            "shardline_resource_fences",
            "INSERT",
            format!(
                "NEW.domain='maintenance-remote-delete' AND NEW.resource='{}'",
                key.as_str().replace('\'', "''")
            ),
        ),
    };
    // This private-schema trigger only pauses the child session. Normal server
    // requests and recovery use their usual transactions and storage guards.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE FUNCTION oci_gc_crash_pause() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN IF {condition} AND current_setting('application_name')='{application}'
         THEN PERFORM pg_sleep(120); END IF; IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END $$;
         CREATE TRIGGER zz_oci_gc_crash_pause BEFORE {operation} ON {table}
         FOR EACH ROW EXECUTE FUNCTION oci_gc_crash_pause();"
    ))).execute(&pool).await.unwrap();
    let mut worker_database = url::Url::parse(database).unwrap();
    worker_database
        .query_pairs_mut()
        .append_pair("application_name", &application);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "oci_index::reclamation::crash::minio_gc_reclamation_worker",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env("DATABASE_URL", worker_database.as_str())
        .env("SHARDLINE_GC_CRASH_ROOT", root)
        .env("SHARDLINE_GC_CRASH_PREFIX", storage.key_prefix().unwrap())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut paused = None;
    for _ in 0..300 {
        paused = sqlx::query_scalar::<_, i32>(
            "SELECT pid FROM pg_stat_activity WHERE application_name=$1 AND wait_event='PgSleep' LIMIT 1"
        ).bind(&application).fetch_optional(&pool).await.unwrap();
        if paused.is_some() || child.try_wait().unwrap().is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let publication = publication.map(|request| tokio::spawn(async move { request.send().await }));
    let mut writer_waiting = publication.is_none();
    if publication.is_some() && paused.is_some() {
        for _ in 0..100 {
            writer_waiting = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
                 WHERE l.locktype='advisory' AND NOT l.granted AND l.mode='ShareLock'
                 AND a.application_name=current_schema()
                 AND l.objsubid=1 AND l.classid::bigint=(x'5348415244474301'::bit(64)::bigint >> 32)
                 AND l.objid::bigint=(x'5348415244474301'::bit(64)::bigint & 4294967295))",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if writer_waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
    let _kill_result = child.kill();
    let exit = child.wait().unwrap();
    // Abort the sleeping backend after killing its client, without waiting for
    // the artificial pause timer. Its metadata transaction must roll back.
    if let Some(pid) = paused {
        sqlx::query("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP TRIGGER zz_oci_gc_crash_pause ON {table}; DROP FUNCTION oci_gc_crash_pause()"
    )))
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;
    if let Some(publication) = publication {
        let response = tokio::time::timeout(std::time::Duration::from_secs(15), publication)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::CREATED,
            "{}",
            response.text().await.unwrap()
        );
    }
    assert!(
        writer_waiting,
        "HTTP publication did not wait on the GC barrier"
    );
    assert!(
        paused.is_some(),
        "GC child never reached the post-delete metadata boundary"
    );
    assert!(!exit.success());
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        assert_eq!(exit.signal(), Some(9), "worker must exit through SIGKILL");
    }
}
