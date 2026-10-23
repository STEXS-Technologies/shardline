//! A provider-accepted OCI GC delete with a lost reply must remain unresolved.
use super::*;
use shardline_server::{LocalGcOptions, ObjectStorageAdapter, ServerConfig, ServerFrontend};
use std::num::NonZeroUsize;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_deploy_oci_gc_lost_delete_ack_stays_pending_and_blocks_republish() {
    let drill = "drill_deploy_oci_gc_lost_delete_ack_stays_pending_and_blocks_republish";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} - shardline binary not found; run `cargo build -p shardline`");
        return;
    };
    migrate_chaos_postgres(&stack.pg_url).await;

    let proxy = S3FaultProxy::start_disarmed(&stack.s3_endpoint, S3ProxyFault::DropAcceptedDelete)
        .expect("start S3 accepted-delete acknowledgement fault proxy");
    let endpoint = proxy.endpoint().to_owned();
    let direct_storage = S3ObjectStoreConfig::new(OBJECT_BUCKET.to_owned(), "us-east-1".to_owned())
        .with_endpoint(Some(stack.s3_endpoint.clone()))
        .with_allow_http(true)
        .with_credentials(
            Some(shardline_protocol::SecretString::new(
                "shardline".to_owned(),
            )),
            Some(shardline_protocol::SecretString::new(
                "shardline-dev-password".to_owned(),
            )),
            None,
        );
    let faulted_storage = direct_storage.clone().with_endpoint(Some(endpoint.clone()));

    let root = TempDir::new().unwrap();
    let mut server = DeploymentServer::spawn(
        &binary,
        &[
            ("SHARDLINE_SERVER_FRONTENDS", "oci"),
            ("SHARDLINE_S3_ENDPOINT", &endpoint),
        ],
        root.path(),
    );
    server.wait_ready(Duration::from_secs(10)).await;

    let token = mint_token("drill", "drill", TokenScope::Write);
    let base = server.base_url();
    let repository = "drill/drill";
    let manifest = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}"#;
    let digest = format!("sha256:{}", sha256_hex(manifest));
    let manifest_url = format!("{base}/v2/{repository}/manifests/latest");
    let published = reqwest::Client::new()
        .put(&manifest_url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.index.v1+json")
        .body(manifest.to_vec())
        .send()
        .await
        .expect("publish OCI manifest");
    assert_eq!(published.status().as_u16(), 201, "manifest published");
    assert_eq!(
        published
            .headers()
            .get("docker-content-digest")
            .and_then(|value| value.to_str().ok()),
        Some(digest.as_str()),
        "server returns the digest used for the tombstone"
    );

    let direct_objects = ServerObjectStore::s3(direct_storage).unwrap();
    let deleted = reqwest::Client::new()
        .delete(format!("{base}/v2/{repository}/manifests/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("logically delete OCI manifest");
    assert_eq!(deleted.status().as_u16(), 202, "tombstone recorded");

    let pool = sqlx::PgPool::connect(&stack.pg_url).await.unwrap();
    let digest_hex = digest.strip_prefix("sha256:").unwrap();
    let scope_namespace: String = sqlx::query_scalar(
        "SELECT scope_namespace FROM shardline_oci_object_tombstones
         WHERE digest_hex = $1 AND object_kind = 'manifest'",
    )
    .bind(digest_hex)
    .fetch_one(&pool)
    .await
    .expect("read authoritative OCI object scope");
    let physical_key = shardline_oci_adapter::oci_manifest_key_from_namespace(
        repository,
        digest_hex,
        &scope_namespace,
    )
    .unwrap();
    assert!(
        direct_objects.contains(&physical_key).unwrap(),
        "physical OCI manifest should still exist before GC"
    );

    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        base.clone(),
        root.path().join("data"),
        NonZeroUsize::new(CHUNK_SIZE).unwrap(),
    )
    .with_server_frontends(vec![ServerFrontend::Oci])
    .unwrap()
    .with_index_postgres_url(stack.pg_url.clone())
    .unwrap()
    .with_object_storage(ObjectStorageAdapter::S3, Some(faulted_storage));
    let options = LocalGcOptions {
        mark: true,
        sweep: true,
        retention_seconds: 0,
        max_revisions_per_repo: None,
    };

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let eligible: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM shardline_oci_object_tombstones
                 WHERE digest_hex = $1 AND object_kind = 'manifest'
                   AND deleted_at_unix_seconds <= floor(extract(epoch FROM clock_timestamp()))::bigint)",
            )
            .bind(digest_hex)
            .fetch_one(&pool)
            .await
            .expect("inspect manifest tombstone eligibility");
            if eligible {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("manifest tombstone becomes eligible for zero-retention test");

    proxy.arm();
    assert!(
        shardline_server::run_gc(config.clone(), options)
            .await
            .is_err(),
        "GC must report the lost provider acknowledgement as an unresolved failure"
    );
    assert!(
        proxy.fault_was_injected(),
        "MinIO accepted the DELETE before the proxy dropped its reply"
    );
    assert!(
        !direct_objects.contains(&physical_key).unwrap(),
        "the test fault is specifically a lost reply after provider deletion"
    );

    let tombstone_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM shardline_oci_object_tombstones
         WHERE digest_hex = $1 AND object_kind = 'manifest'",
    )
    .bind(digest_hex)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(tombstone_count, 1, "tombstone remains authoritative");
    let pending: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM shardline_resource_fences
         WHERE domain = 'maintenance-remote-delete' AND resource = $1)",
    )
    .bind(physical_key.as_str())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(pending, "unacknowledged remote delete remains fenced");

    let blocked_republish = reqwest::Client::new()
        .put(&manifest_url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.index.v1+json")
        .body(manifest.to_vec())
        .send()
        .await
        .expect("attempt publication while delete outcome is uncertain");
    assert!(
        !blocked_republish.status().is_success(),
        "new content must not be published at a key with an unresolved delete"
    );
    assert!(
        !direct_objects.contains(&physical_key).unwrap(),
        "a rejected publication must not recreate bytes while the earlier delete may still finish"
    );

    assert!(
        shardline_server::run_gc(config, options).await.is_err(),
        "a later sweep must not blindly replay an unprovable delete"
    );
    let retry_tombstone_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM shardline_oci_object_tombstones
         WHERE digest_hex = $1 AND object_kind = 'manifest'",
    )
    .bind(digest_hex)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(retry_tombstone_count, 1);
    pool.close().await;
    server.kill_hard();
    eprintln!(
        "chaos({drill}): PASS - MinIO deleted the object but its lost reply kept the tombstone and remote-delete fence pending; GC did not replay"
    );
}
