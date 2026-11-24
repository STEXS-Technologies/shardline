#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::{num::NonZeroUsize, time::Duration};

use ed25519_dalek::{SigningKey, VerifyingKey};
use shardline_index::HubRecoveryMode;
use shardline_protocol::{
    RepositoryProvider, RepositoryScope, SecretString, TokenClaims, TokenScope,
};
use shardline_reliability::{
    HubRefBatchArchiveCheckpointPin, HubRefBatchArchiveRecordV1, MAX_HUB_REF_ARCHIVE_RECORD_BYTES,
};
use shardline_server_core::{AuthProvider, ServerObjectStore, auth::LocalHmacProvider};
use shardline_storage::{
    ObjectIntegrity, ObjectKey, ObjectPrefix, ObjectStore, S3ObjectStoreConfig,
};
use shardline_test_support::{DockerLocalStack, S3RawConfig};
use sqlx::{PgPool, Row};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle, time::sleep};

use super::{
    DeploymentMode, RouterBackgroundTasks, ServerConfig, ServerFrontend, ServerRole,
    router_with_background_tasks,
};
use crate::{ObjectStorageAdapter, config::HubRecoveryRuntimeConfig};

const DEPLOYMENT_ID: &str = "hub-shadow-e2e";
const KEY_ID: &str = "hub-shadow-e2e-key";
const SIGNING_SEED: [u8; 32] = [0x5a; 32];
const TOKEN_KEY: &[u8] = b"hub-shadow-e2e-token-key-32-bytes-long";
const REPOSITORY: &str = "shadow-e2e/model";

#[derive(Debug)]
struct ArchivedEvent {
    digest: String,
    event: serde_json::Value,
}

struct RunningServer {
    base_url: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: JoinHandle<Result<(), std::io::Error>>,
    background: RouterBackgroundTasks,
}

impl RunningServer {
    async fn start(config: ServerConfig) -> Self {
        let (router, background) = router_with_background_tasks(config).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, shutdown_signal) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = shutdown_signal.await;
                })
                .await
        });
        Self {
            base_url: format!("http://{address}"),
            shutdown: Some(shutdown),
            task,
            background,
        }
    }

    async fn stop(self) {
        let Self {
            shutdown,
            task,
            background,
            ..
        } = self;
        let _ = shutdown.unwrap().send(());
        task.await.unwrap().unwrap();
        background.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable PostgreSQL and MinIO containers"]
async fn postgres_minio_shadow_ndjson_receipts_survive_router_restart() {
    let stack = DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()
        .expect("start disposable PostgreSQL and MinIO")
        .expect("Docker must be available for this qualification");
    let database_url = format!("{}?sslmode=disable", stack.postgres_url().unwrap());
    let pool = PgPool::connect(&database_url).await.unwrap();
    crate::apply_database_migrations(&pool).await.unwrap();

    let namespace = stack.unique_s3_key_prefix("hub-shadow-e2e");
    let payload = s3_config(
        stack
            .s3_raw_config(Some(&format!("{namespace}/payload")))
            .unwrap(),
    );
    let archive = s3_config(
        stack
            .s3_raw_config(Some(&format!("{namespace}/archive")))
            .unwrap(),
    );
    let temp = TempDir::new().unwrap();
    let token = write_token();
    let archive_store = ServerObjectStore::s3(archive.clone()).unwrap();

    let config = || {
        ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:8080".to_owned(),
            temp.path().to_path_buf(),
            NonZeroUsize::new(65_536).unwrap(),
        )
        .with_server_frontends(vec![ServerFrontend::Hub])
        .unwrap()
        .with_server_role(ServerRole::All)
        .with_deployment_mode(DeploymentMode::Insecure)
        .with_token_signing_key(TOKEN_KEY.to_vec())
        .unwrap()
        .with_index_postgres_url(database_url.clone())
        .unwrap()
        .with_object_storage(ObjectStorageAdapter::S3, Some(payload.clone()))
        .with_hub_recovery(
            HubRecoveryMode::Shadow,
            Some(HubRecoveryRuntimeConfig {
                deployment_id: DEPLOYMENT_ID.to_owned(),
                key_id: KEY_ID.to_owned(),
                signing_key: shardline_protocol::SecretBytes::new(SIGNING_SEED.to_vec()),
                archive_s3: archive.clone(),
            }),
        )
    };

    let first_server = RunningServer::start(config()).await;
    let client = reqwest::Client::new();
    let repository_token = format!("Bearer {token}");
    let create = client
        .post(format!("{}/api/repos/create", first_server.base_url))
        .header("authorization", &repository_token)
        .json(&serde_json::json!({
            "type": "model",
            "name": REPOSITORY,
            "private": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(create.status(), axum::http::StatusCode::CREATED);

    let first = publish_ndjson(
        &client,
        &first_server.base_url,
        &repository_token,
        "first version",
        "",
        "first.txt",
        b"first bytes",
    )
    .await;
    let first_event = wait_for_archived_event(&pool, REPOSITORY, 1).await;
    let first_manifest = verify_manifest_reference(&archive_store, &first_event.event).await;
    assert_eq!(first_manifest.deployment_id, DEPLOYMENT_ID);
    assert_eq!(first_manifest.revision.sha, first["commit_id"]);
    assert_eq!(first_manifest.files[0].path, "first.txt");

    first_server.stop().await;

    let second_server = RunningServer::start(config()).await;
    let second = publish_ndjson(
        &client,
        &second_server.base_url,
        &repository_token,
        "second version",
        first["commit_id"].as_str().unwrap(),
        "second.txt",
        b"second bytes",
    )
    .await;
    let second_event = wait_for_archived_event(&pool, REPOSITORY, 2).await;
    let second_manifest = verify_manifest_reference(&archive_store, &second_event.event).await;
    assert_eq!(second_manifest.revision.sha, second["commit_id"]);
    assert_eq!(second_manifest.files.len(), 2);

    let first_state = &first_event.event["after"];
    let pin = HubRefBatchArchiveCheckpointPin::new(
        first_state["deployment_id"].as_str().unwrap(),
        first_state["provider_id"].as_str().unwrap(),
        first_state["tenant_id"].as_str().unwrap(),
        first_state["repository_id"].as_str().unwrap(),
        1,
        first_event.digest.clone(),
    )
    .unwrap();
    let signing_key = SigningKey::from_bytes(&SIGNING_SEED);
    let verifying_key = VerifyingKey::from(&signing_key);
    let first_bytes = read_receipt_bytes(&archive_store, 1, &first_event.digest);
    let (_first_record, anchor) = HubRefBatchArchiveRecordV1::decode_pinned_anchor(
        &first_bytes,
        &verifying_key,
        &pin,
        &first_event.digest,
    )
    .unwrap();

    let second_bytes = read_receipt_bytes(&archive_store, 2, &second_event.digest);
    let (second_record, next_anchor) =
        HubRefBatchArchiveRecordV1::decode_with_previous(&second_bytes, &verifying_key, &anchor)
            .unwrap();
    assert_eq!(second_record.event.sequence, 2);
    assert_eq!(next_anchor.sequence(), 2);
    assert_eq!(
        second_event.event["after"]["manifest_refs"][0]["revision_sha"],
        second["commit_id"]
    );

    second_server.stop().await;
    pool.close().await;
    drop(stack);
}

fn s3_config(raw: S3RawConfig) -> S3ObjectStoreConfig {
    S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_credentials(
            raw.access_key.map(SecretString::new),
            raw.secret_key.map(SecretString::new),
            raw.session_token.map(SecretString::new),
        )
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_allow_http(raw.allow_http)
}

fn write_token() -> String {
    let repository = RepositoryScope::new(
        RepositoryProvider::Generic,
        "shadow-e2e",
        "model",
        Some("main"),
    )
    .unwrap();
    let claims = TokenClaims::new(
        "shardline",
        "shadow-e2e",
        TokenScope::Write,
        repository,
        u64::MAX,
    )
    .unwrap();
    LocalHmacProvider::new(TOKEN_KEY)
        .unwrap()
        .mint_token(&claims)
        .unwrap()
}

async fn publish_ndjson(
    client: &reqwest::Client,
    base_url: &str,
    authorization: &str,
    message: &str,
    parent_commit: &str,
    path: &str,
    contents: &[u8],
) -> serde_json::Value {
    use base64::Engine;

    let encoded = base64::engine::general_purpose::STANDARD.encode(contents);
    let body = format!(
        "{{\"header\":{{\"message\":{message:?},\"parentCommit\":{parent_commit:?}}}}}\n{{\"file\":{{\"path\":{path:?},\"content\":{encoded:?}}}}}"
    );
    let response = client
        .post(format!("{base_url}/api/models/{REPOSITORY}/commit/main"))
        .header("authorization", authorization)
        .header("content-type", "application/x-ndjson")
        .body(body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.bytes().await.unwrap();
    assert_eq!(
        status,
        axum::http::StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap()
}

async fn wait_for_archived_event(
    pool: &PgPool,
    repository_id: &str,
    sequence: i64,
) -> ArchivedEvent {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(row) = sqlx::query(
                "SELECT event_json, archive_digest, archived_at_unix_seconds
             FROM shardline_hub_recovery_outbox
             WHERE repo_id = $1 AND domain_sequence = $2",
            )
            .bind(repository_id)
            .bind(sequence)
            .fetch_optional(pool)
            .await
            .unwrap()
                && let Some(digest) = row.try_get::<Option<String>, _>("archive_digest").unwrap()
                && row
                    .try_get::<Option<i64>, _>("archived_at_unix_seconds")
                    .unwrap()
                    .is_some()
            {
                return ArchivedEvent {
                    digest,
                    event: row.try_get("event_json").unwrap(),
                };
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("worker did not archive expected sequence")
}

async fn verify_manifest_reference(
    archive: &ServerObjectStore,
    event: &serde_json::Value,
) -> shardline_index::HubTreeVersionManifestV1 {
    let reference = &event["after"]["manifest_refs"][0];
    let key = ObjectKey::parse(reference["object_key"].as_str().unwrap()).unwrap();
    let (bytes, _length) = archive
        .read_object_if_present(&key, 8 * 1024 * 1024)
        .unwrap()
        .expect("manifest object should exist in MinIO");
    assert_eq!(
        ObjectIntegrity::for_bytes(&bytes).hash().hex_string(),
        reference["digest_blake3"].as_str().unwrap()
    );
    let manifest = shardline_index::HubTreeVersionManifestV1::decode(&bytes).unwrap();
    assert_eq!(manifest.deployment_id, DEPLOYMENT_ID);
    manifest
}

fn read_receipt_bytes(archive: &ServerObjectStore, sequence: u64, digest: &str) -> Vec<u8> {
    let suffix = format!("{sequence:020}-{digest}.json");
    let matching_keys: Vec<_> = archive
        .list_prefix(&ObjectPrefix::parse("recovery/v1").unwrap())
        .unwrap()
        .into_iter()
        .filter(|metadata| metadata.key().as_str().ends_with(&suffix))
        .map(|metadata| metadata.key().clone())
        .collect();
    assert_eq!(
        matching_keys.len(),
        1,
        "expected exactly one matching receipt"
    );
    let key = &matching_keys[0];
    archive
        .read_object_if_present(
            key,
            u64::try_from(MAX_HUB_REF_ARCHIVE_RECORD_BYTES).unwrap(),
        )
        .unwrap()
        .expect("linked receipt should exist in MinIO")
        .0
}
