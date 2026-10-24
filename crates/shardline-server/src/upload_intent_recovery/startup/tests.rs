//! End-to-end recovery and lifetime regression fixtures.
#![allow(clippy::panic, clippy::unreachable)]
use crate::{ServerBackend, ServerConfig, ServerRole, object_store::ServerObjectStore};
use axum::{
    body::{Body, Bytes},
    http::{Request, StatusCode},
};
use sha2::Digest;
use shardline_index::{
    FileRecord, LocalIndexStore, UploadIntent, UploadIntentState, UploadIntentStore,
};
use std::{
    future::Future,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tower::ServiceExt;

const LARGE_FILE_BYTES: usize = 68_157_440;

async fn publish(backend: &ServerBackend, id: &str, bytes: Bytes) -> FileRecord {
    match backend {
        ServerBackend::Local(backend) => {
            backend.upload_file(id, bytes, None).await.unwrap();
        }
        ServerBackend::Postgres(backend) => {
            backend.upload_file(id, bytes, None).await.unwrap();
        }
    }
    backend.protocol_file_record(id).await.unwrap()
}

fn config(root: &std::path::Path) -> ServerConfig {
    ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.to_path_buf(),
        NonZeroUsize::new(65_536).unwrap(),
    )
    .with_token_signing_key(b"0123456789abcdef0123456789abcdef".to_vec())
    .unwrap()
}

async fn seed<S: UploadIntentStore>(
    index: &S,
    id: String,
    record: &FileRecord,
    digest: &str,
) -> UploadIntent
where
    S::Error: std::fmt::Debug,
{
    let intent = UploadIntent::new(
        id,
        format!("record/{}", record.file_id),
        digest.to_owned(),
        record.total_bytes,
    );
    index.create_intent(&intent).await.unwrap();
    for state in [UploadIntentState::Storing, UploadIntentState::Stored] {
        assert!(
            index
                .transition_intent(intent.intent_id(), state)
                .await
                .unwrap()
        );
    }
    intent
}

fn hold_read(
    config: &ServerConfig,
    record: &FileRecord,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    std::sync::mpsc::Sender<()>,
) {
    let objects = ServerObjectStore::local(config.root_dir().join("chunks")).unwrap();
    let ServerObjectStore::Local(local) = objects else {
        unreachable!()
    };
    let xorb = crate::xet_adapter::xorb_object_key(&record.chunks[0].hash).unwrap();
    let key = if local.path_for_key(&xorb).is_file() {
        xorb
    } else {
        crate::chunk_store::chunk_object_key(&record.chunks[0].hash).unwrap()
    };
    assert!(local.path_for_key(&key).is_file());
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    crate::object_store::set_before_local_object_read_hook(local.path_for_key(&key), move || {
        let _ignored = entered_tx.send(());
        let _ignored = released.recv_timeout(Duration::from_secs(15));
    });
    (entered, release)
}

async fn wait_visible<S: UploadIntentStore>(index: &S, intent: &UploadIntent)
where
    S::Error: std::fmt::Debug,
{
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if index
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state()
                == UploadIntentState::Visible
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        index
            .reliability_events(intent.intent_id())
            .await
            .unwrap()
            .len(),
        5
    );
}

async fn exercise_router<S, F, Fut>(
    config: ServerConfig,
    index: &S,
    record: FileRecord,
    digest: &str,
    age: F,
) where
    S: UploadIntentStore,
    S::Error: std::fmt::Debug,
    F: Fn(String) -> Fut,
    Fut: Future<Output = ()>,
{
    for role in [ServerRole::All, ServerRole::Api, ServerRole::Transfer] {
        let intent = seed(
            index,
            format!("file-v2-startup-{}", role.as_str()),
            &record,
            digest,
        )
        .await;
        age(intent.intent_id().to_owned()).await;
        let (entered, release) = hold_read(&config, &record);
        let mut building = tokio::spawn(crate::app::router(config.clone().with_server_role(role)));
        let app = match tokio::time::timeout(Duration::from_secs(5), &mut building).await {
            Ok(result) => result.unwrap().unwrap(),
            Err(error) => {
                building.abort();
                let _ignored = release.send(());
                panic!("router awaited file bytes before readiness: {error}");
            }
        };
        let entered_result = tokio::time::timeout(Duration::from_secs(5), entered).await;
        if entered_result.is_err() {
            let _ignored = release.send(());
            let events = index.reliability_events(intent.intent_id()).await.unwrap();
            let pool = crate::admission::BoundedPool::new(NonZeroUsize::MIN);
            let verified = super::super::file_record_matches_intent(
                &intent,
                &events,
                record.clone(),
                ServerObjectStore::local(config.root_dir().join("chunks")).unwrap(),
                pool,
                &super::RecoveryPass::Complete,
            )
            .await;
            panic!(
                "recovery did not reach held read: role={role:?}, standalone verification={verified:?}, first={:?}, chunks={}",
                record.chunks.first(),
                record.chunks.len()
            );
        }
        entered_result.unwrap().unwrap();
        assert_eq!(
            index
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Stored
        );
        let ready = tokio::time::timeout(
            Duration::from_secs(3),
            app.clone().oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(ready.status(), StatusCode::OK);
        assert_eq!(
            index
                .reliability_events(intent.intent_id())
                .await
                .unwrap()
                .len(),
            3
        );
        release.send(()).unwrap();
        wait_visible(index, &intent).await;
        drop(app);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_router_serves_while_large_file_recovery_is_held_for_every_role() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let backend = ServerBackend::from_config(&config).await.unwrap();
    let payload = Bytes::from(vec![42_u8; LARGE_FILE_BYTES]);
    let digest = hex::encode(sha2::Sha256::digest(&payload));
    let record = publish(&backend, "large-startup-file", payload).await;
    let index = LocalIndexStore::new(root.path().to_path_buf()).unwrap();
    let database_path = root.path().join("metadata.sqlite3");
    exercise_router(config, &index, record, &digest, move |id| {
        let path = database_path.clone();
        async move { rusqlite::Connection::open(path).unwrap().execute("UPDATE shardline_upload_intents SET created_at_unix_seconds=unixepoch()-60 WHERE intent_id=?1", [id]).unwrap(); }
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_router_serves_while_large_file_recovery_is_held_for_every_role() {
    let Some(fixture) = crate::test_support::postgres_fixture::PostgresFixture::new().await else {
        eprintln!("SKIPPED: large-file startup fixture requires DATABASE_URL");
        return;
    };
    fixture.migrate().await;
    fixture
        .index()
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let payload = Bytes::from(vec![42_u8; LARGE_FILE_BYTES]);
    let digest = hex::encode(sha2::Sha256::digest(&payload));
    let record = publish(&fixture.backend, "large-startup-file", payload).await;
    let admin = fixture.admin.clone();
    let schema = fixture.schema.clone();
    exercise_router(fixture.config.clone().with_token_signing_key(b"0123456789abcdef0123456789abcdef".to_vec()).unwrap(), fixture.index(), record, &digest, move |id| {
        let admin = admin.clone(); let schema = schema.clone();
        async move { sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {schema}.shardline_upload_intents SET created_at=now()-interval '1 minute' WHERE intent_id=$1"))).bind(id).execute(&admin).await.unwrap(); }
    }).await;
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standalone_backend_constructor_keeps_eager_verified_recovery() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let backend = ServerBackend::from_config(&config).await.unwrap();
    let payload = Bytes::from_static(b"eager standalone recovery");
    let digest = hex::encode(sha2::Sha256::digest(&payload));
    let record = publish(&backend, "eager-file", payload).await;
    let index = LocalIndexStore::new(root.path().to_path_buf()).unwrap();
    let intent = seed(
        &index,
        "file-v2-eager-recovery".to_owned(),
        &record,
        &digest,
    )
    .await;
    rusqlite::Connection::open(root.path().join("metadata.sqlite3"))
        .unwrap()
        .execute(
            "UPDATE shardline_upload_intents SET created_at_unix_seconds=unixepoch()-60",
            [],
        )
        .unwrap();
    let (entered, release) = hold_read(&config, &record);
    let building = tokio::spawn(async move { ServerBackend::from_config(&config).await });
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .unwrap()
        .unwrap();
    assert!(!building.is_finished());
    assert_eq!(
        index
            .intent_by_id(intent.intent_id())
            .await
            .unwrap()
            .unwrap()
            .state(),
        UploadIntentState::Stored
    );
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), building)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    wait_visible(&index, &intent).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_file_verification_preserves_evidence_and_router_readiness() {
    for corrupt_storage in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let backend = ServerBackend::from_config(&config).await.unwrap();
        let payload = Bytes::from_static(b"unconfirmed recovery bytes");
        let digest = if corrupt_storage {
            hex::encode(sha2::Sha256::digest(&payload))
        } else {
            hex::encode(sha2::Sha256::digest(b"different publication"))
        };
        let record = publish(&backend, "unconfirmed-file", payload).await;
        let index = LocalIndexStore::new(root.path().to_path_buf()).unwrap();
        let intent = seed(
            &index,
            "file-v2-unconfirmed-recovery".to_owned(),
            &record,
            &digest,
        )
        .await;
        rusqlite::Connection::open(root.path().join("metadata.sqlite3"))
            .unwrap()
            .execute(
                "UPDATE shardline_upload_intents SET created_at_unix_seconds=unixepoch()-60",
                [],
            )
            .unwrap();
        if corrupt_storage {
            let objects = ServerObjectStore::local(config.root_dir().join("chunks")).unwrap();
            let ServerObjectStore::Local(local) = objects else {
                unreachable!()
            };
            let key = crate::xet_adapter::xorb_object_key(&record.chunks[0].hash).unwrap();
            let path = local.path_for_key(&key);
            assert!(path.is_file());
            std::fs::write(path, b"invalid serialized container").unwrap();
        }
        let mut guard = super::RecoveryGuard::start(backend, Arc::new(AtomicBool::new(false)));
        tokio::time::timeout(Duration::from_secs(5), &mut guard.task)
            .await
            .unwrap()
            .unwrap();
        let app = crate::app::router(config).await.unwrap();
        let response = tokio::time::timeout(
            Duration::from_secs(3),
            app.clone().oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            index
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Stored
        );
        assert_eq!(
            index
                .reliability_events(intent.intent_id())
                .await
                .unwrap()
                .len(),
            3
        );
        drop(app);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn router_lifetime_or_shutdown_stops_file_verification_without_false_completion() {
    for abort in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let backend = ServerBackend::from_config(&config).await.unwrap();
        let payload = Bytes::from_static(b"interrupted recovery bytes");
        let digest = hex::encode(sha2::Sha256::digest(&payload));
        let record = publish(&backend, "interrupted-file", payload).await;
        let index = LocalIndexStore::new(root.path().to_path_buf()).unwrap();
        let intent = seed(
            &index,
            "file-v2-interrupted-recovery".to_owned(),
            &record,
            &digest,
        )
        .await;
        rusqlite::Connection::open(root.path().join("metadata.sqlite3"))
            .unwrap()
            .execute(
                "UPDATE shardline_upload_intents SET created_at_unix_seconds=unixepoch()-60",
                [],
            )
            .unwrap();
        let stopped = Arc::new(AtomicBool::new(false));
        let (entered, release) = hold_read(&config, &record);
        let mut guard = super::RecoveryGuard::start(backend.clone(), stopped.clone());
        let task = guard.task.abort_handle();
        tokio::time::timeout(Duration::from_secs(5), entered)
            .await
            .unwrap()
            .unwrap();
        if abort {
            drop(guard);
            release.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while !task.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        } else {
            stopped.store(true, Ordering::Relaxed);
            release.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(5), &mut guard.task)
                .await
                .unwrap()
                .unwrap();
            drop(guard);
        }
        assert_eq!(
            index
                .intent_by_id(intent.intent_id())
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Stored
        );
        assert_eq!(
            index
                .reliability_events(intent.intent_id())
                .await
                .unwrap()
                .len(),
            3
        );
        let resumed = super::RecoveryGuard::start(backend, Arc::new(AtomicBool::new(false)));
        wait_visible(&index, &intent).await;
        drop(resumed);
    }
}
