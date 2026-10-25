//! GC process loss while authenticating the only remaining native member source.
use super::*;
use shardline_index::{
    FileChunkRecord, FileRecord, PostgresRecordStore, RecordMutation, RecordTraversal,
    StorageRepresentation,
};
use shardline_storage::AsyncObjectStore;
use shardline_xet_core::{
    merklehash::{compute_data_hash, xorb_hash},
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

struct Process(std::process::Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _kill = self.0.kill();
        let _wait = self.0.wait();
    }
}

async fn source(objects: &ServerObjectStore, body: &[u8]) -> String {
    let hash = compute_data_hash(body);
    let tail = b"independent native source member";
    let tail_hash = compute_data_hash(tail);
    let length = body.len() as u64;
    let total = length + tail.len() as u64;
    let identity = xorb_hash(&[(hash, length), (tail_hash, tail.len() as u64)]);
    let bytes = serialized_xorb_object_from_components(
        &identity,
        [body, tail.as_slice()].concat(),
        vec![(hash, length), (tail_hash, total)],
        CompressionScheme::None,
    )
    .unwrap()
    .serialized_data;
    shardline_xet_adapter::store_uploaded_xorb_bytes(objects, &identity.hex(), &bytes)
        .await
        .unwrap();
    identity.hex()
}

fn gc(binary: &Path, root: &Path, database: &str, environment: &[(&str, String)]) -> Command {
    let mut command = Command::new(binary);
    command
        .env_clear()
        .current_dir(root)
        .args(["--config"])
        .arg(root.join("empty-config.toml"))
        .args(["gc", "--root"])
        .arg(root)
        .env("SHARDLINE_INDEX_POSTGRES_URL", database)
        .env("SHARDLINE_SERVER_FRONTENDS", "xet")
        .env("TMPDIR", std::env::temp_dir());
    for (name, value) in environment {
        command.env(name, value);
    }
    command
}

async fn finish(mut command: Command) {
    let output = tokio::task::spawn_blocking(move || {
        shardline_test_support::native_client_output(&mut command, Duration::from_secs(60))
    })
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "GC exited with {} (fixture output omitted)",
        output.status
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_source_gc_sigkill_preserves_live_bytes_and_expired_orphan_recovery() {
    use std::os::unix::process::ExitStatusExt;
    let binary = resolve_shardline_binary().expect("actual GC binary required");
    let services = tokio::task::spawn_blocking(|| {
        shardline_test_support::DockerLocalStack::builder()
            .with_postgres()
            .with_minio()
            .start()
            .expect("native GC services")
            .expect("native GC requires Docker")
    })
    .await
    .unwrap();
    let database = services.postgres_url().unwrap();
    let pool = sqlx::PgPool::connect(&database).await.unwrap();
    shardline_server::apply_database_migrations(&pool)
        .await
        .unwrap();
    let prefix = services.unique_s3_key_prefix("source-gc-sigkill");
    let raw = services.s3_raw_config(Some(&prefix)).unwrap();
    let endpoint = raw.endpoint.clone().unwrap();
    let storage = S3ObjectStoreConfig::new(raw.bucket.clone(), raw.region.clone())
        .with_endpoint(raw.endpoint.clone())
        .with_allow_http(true)
        .with_key_prefix(Some(&prefix))
        .with_credentials(
            raw.access_key.clone().map(SecretString::new),
            raw.secret_key.clone().map(SecretString::new),
            None,
        );
    let objects = ServerObjectStore::s3(storage.clone()).unwrap();
    let body = vec![0x35_u8; 128 * 1024];
    let live = source(&objects, &body).await;
    let orphan = source(&objects, b"retention-aged orphan").await;
    let member = compute_data_hash(&body).hex();
    let member_key = ObjectKey::parse(&format!("{}/{}", &member[..2], member)).unwrap();
    AsyncObjectStore::delete_if_present(&objects, &member_key)
        .await
        .unwrap();
    let records = PostgresRecordStore::new(pool.clone());
    records
        .write_version_record(&FileRecord {
            file_id: "protected-historical-member".to_owned(),
            content_hash: member.clone(),
            total_bytes: body.len() as u64,
            chunk_size: body.len() as u64,
            storage_repr: StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: member.clone(),
                offset: 0,
                length: body.len() as u64,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 0,
            }],
        })
        .await
        .unwrap();
    assert!(
        records
            .native_chunk_reference_page(None)
            .await
            .unwrap()
            .is_empty()
    );
    let environment = vec![
        ("SHARDLINE_OBJECT_STORAGE_ADAPTER", "s3".to_owned()),
        ("SHARDLINE_S3_BUCKET", raw.bucket),
        ("SHARDLINE_S3_REGION", raw.region),
        ("SHARDLINE_S3_ENDPOINT", endpoint.clone()),
        ("SHARDLINE_S3_ACCESS_KEY_ID", raw.access_key.unwrap()),
        ("SHARDLINE_S3_SECRET_ACCESS_KEY", raw.secret_key.unwrap()),
        ("SHARDLINE_S3_KEY_PREFIX", prefix),
        ("SHARDLINE_S3_ALLOW_HTTP", "true".to_owned()),
    ];
    let root = TempDir::new().unwrap();
    std::fs::write(root.path().join("empty-config.toml"), "").unwrap();
    let live_key = shardline_xet_adapter::xorb_object_key(&live).unwrap();
    let orphan_key = shardline_xet_adapter::xorb_object_key(&orphan).unwrap();
    // The public CLI deliberately enforces at least one hour when creating
    // quarantine entries. Seed a short, genuinely aged fixture through the
    // production server API; do not change clocks or stored deadlines.
    let config = shardline_server::ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.path().to_owned(),
        std::num::NonZeroUsize::new(128).unwrap(),
    )
    .with_index_postgres_url(database.clone())
    .unwrap()
    .with_object_storage(shardline_server::ObjectStorageAdapter::S3, Some(storage));
    shardline_server::run_gc_diagnostics(config, shardline_server::LocalGcOptions::mark_only(5))
        .await
        .unwrap();
    let mut early = gc(&binary, root.path(), &database, &environment);
    early.arg("--sweep");
    finish(early).await;
    assert!(
        AsyncObjectStore::metadata(&objects, &orphan_key)
            .await
            .unwrap()
            .is_some()
    );
    tokio::time::sleep(Duration::from_secs(5)).await;
    let proxy = S3FaultProxy::start(
        &endpoint,
        S3ProxyFault::DelaySuccessfulGetBodyForPath {
            suffix: live_key.as_str().to_owned(),
            delay: Duration::from_secs(5),
        },
    )
    .unwrap();
    let mut interrupted = gc(&binary, root.path(), &database, &environment);
    interrupted
        .arg("--sweep")
        .env("SHARDLINE_S3_ENDPOINT", proxy.endpoint())
        .stdout(Stdio::from(
            std::fs::File::create(root.path().join("interrupted.stdout")).unwrap(),
        ))
        .stderr(Stdio::from(
            std::fs::File::create(root.path().join("interrupted.stderr")).unwrap(),
        ));
    let mut child = Process(interrupted.spawn().unwrap());
    tokio::time::timeout(Duration::from_secs(30), async {
        while !proxy.fault_was_injected() {
            if let Some(status) = child.0.try_wait().unwrap() {
                let diagnostics = TempDir::new().unwrap();
                for name in ["interrupted.stdout", "interrupted.stderr"] {
                    std::fs::copy(root.path().join(name), diagnostics.path().join(name)).unwrap();
                }
                panic!(
                    "GC exited before source read ({status}); private diagnostics: {}",
                    diagnostics.keep().display()
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("GC must authenticate the missing-copy source");
    child.0.kill().unwrap();
    assert_eq!(child.0.wait().unwrap().signal(), Some(9));
    assert!(
        AsyncObjectStore::metadata(&objects, &live_key)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        AsyncObjectStore::metadata(&objects, &orphan_key)
            .await
            .unwrap()
            .is_some(),
        "source authentication must complete before destructive sweep"
    );
    let mut resumed = gc(&binary, root.path(), &database, &environment);
    resumed.arg("--sweep");
    finish(resumed).await;
    assert!(
        AsyncObjectStore::metadata(&objects, &live_key)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        AsyncObjectStore::metadata(&objects, &orphan_key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        AsyncObjectStore::metadata(&objects, &member_key)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        shardline_xet_adapter::read_verified_xorb_chunk(&objects, &live, &member)
            .unwrap()
            .unwrap(),
        body
    );
    pool.close().await;
}
