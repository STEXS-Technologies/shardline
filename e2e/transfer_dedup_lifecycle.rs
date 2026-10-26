//! Actual HTTP dedup lookups across replicas and a durable unresolved delete.

use super::*;
use shardline_index::PostgresIndexStore;
use shardline_server::{ObjectStorageAdapter, ServerObjectStore};
use shardline_storage::{
    AsyncObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, S3ObjectStoreConfig,
};
use std::path::Path;

type Error = Box<dyn std::error::Error>;

struct Runtime {
    base: String,
    task: tokio::task::JoinHandle<Result<(), shardline_server::ServerError>>,
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Runtime {
    async fn start(
        root: &Path,
        database: &str,
        objects: S3ObjectStoreConfig,
    ) -> Result<Self, Error> {
        Self::start_with_objects(root, database, Some(objects)).await
    }

    async fn start_with_objects(
        root: &Path,
        database: &str,
        objects: Option<S3ObjectStoreConfig>,
    ) -> Result<Self, Error> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let base = format!("http://{address}");
        let mut config = ServerConfig::new(
            address,
            base.clone(),
            root.to_owned(),
            NonZeroUsize::new(65536).ok_or("invalid chunk size")?,
        )
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())?
        .with_index_postgres_url(database.to_owned())?;
        if let Some(objects) = objects {
            config = config.with_object_storage(ObjectStorageAdapter::S3, Some(objects));
        }
        let runtime = Self {
            base,
            task: spawn(async move { serve_with_listener(config, listener).await }),
        };
        wait_for_health(&runtime.base).await?;
        Ok(runtime)
    }

    async fn stop(mut self) {
        self.task.abort();
        let _joined = (&mut self.task).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker PostgreSQL; objects use a shared filesystem"]
async fn native_delta_delete_fencing_survives_replica_restart() -> Result<(), Error> {
    qualify_delta_delete_fencing(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO"]
async fn native_delta_remote_delete_fencing_survives_replica_restart() -> Result<(), Error> {
    qualify_delta_delete_fencing(true).await
}

async fn qualify_delta_delete_fencing(remote: bool) -> Result<(), Error> {
    let stack = tokio::task::spawn_blocking(move || {
        let builder = shardline_test_support::DockerLocalStack::builder().with_postgres();
        if remote {
            builder.with_minio().start()
        } else {
            builder.start()
        }
    })
    .await??
    .ok_or("Docker PostgreSQL is required")?;
    let database = stack.postgres_url().ok_or("PostgreSQL is required")?;
    let owner = sqlx::PgPool::connect(&database).await?;
    shardline_server::apply_database_migrations(&owner).await?;
    let root = tempfile::tempdir()?;
    let peer_root = tempfile::tempdir()?;
    let objects = if remote {
        let raw = stack
            .s3_raw_config(Some("delta-lifecycle"))
            .ok_or("MinIO is required")?;
        Some(
            S3ObjectStoreConfig::new(raw.bucket, raw.region)
                .with_endpoint(raw.endpoint)
                .with_allow_http(raw.allow_http)
                .with_key_prefix(raw.key_prefix.as_deref())
                .with_credentials(
                    raw.access_key.map(shardline_protocol::SecretString::new),
                    raw.secret_key.map(shardline_protocol::SecretString::new),
                    None,
                ),
        )
    } else {
        None
    };
    let storage = if let Some(config) = &objects {
        ServerObjectStore::s3(config.clone())?
    } else {
        ServerObjectStore::local(root.path().join("chunks"))?
    };
    // Filesystem objects need a shared root; remote replicas must not depend
    // on another replica's staging or cache files.
    let second_root = if remote {
        peer_root.path()
    } else {
        root.path()
    };
    let first = Runtime::start_with_objects(root.path(), &database, objects.clone()).await?;
    let second = Runtime::start_with_objects(second_root, &database, objects.clone()).await?;
    let write = bearer_token(
        "owner",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    )?;
    let read = bearer_token(
        "owner",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    )?;
    let hidden = bearer_token(
        "other",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "other",
        "assets",
        Some("main"),
    )?;
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let content = b"aaaa";
    let (xorb, hash) = single_chunk_xorb(content);
    client
        .post(format!("{}/v1/xorbs/default/{hash}", first.base))
        .bearer_auth(&write)
        .body(xorb)
        .send()
        .await?
        .error_for_status()?;
    let (stable, stable_hash) = single_chunk_xorb(b"bbbb");
    client
        .post(format!("{}/v1/xorbs/default/{stable_hash}", first.base))
        .bearer_auth(&write)
        .body(stable)
        .send()
        .await?
        .error_for_status()?;
    let (shard, file) = single_file_shard(&[(content, &hash), (b"bbbb", &stable_hash)]);
    client
        .post(format!("{}/v1/shards", first.base))
        .bearer_auth(&write)
        .body(shard)
        .send()
        .await?
        .error_for_status()?;
    let mut proof = None;
    for runtime in [&first, &second] {
        let response = delta_lookup(&client, runtime, &read, &file).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = response.json().await?;
        assert_eq!(body["fileSize"], 8);
        assert!(body["hashRanges"][0].is_null());
        assert!(body["hashRanges"][1].is_object());
        proof = Some(body);
    }
    let expected = xet_core_structures::merklehash::file_hash(&[
        (
            xet_core_structures::merklehash::compute_data_hash(b"cccc"),
            4,
        ),
        (
            xet_core_structures::merklehash::compute_data_hash(b"bbbb"),
            4,
        ),
    ]);
    super::transfer_delta_upload::qualify(
        &second.base,
        &read,
        &write,
        &stable_hash,
        expected.as_bytes().try_into().unwrap(),
        proof.as_ref().ok_or("missing proof")?,
    )
    .await;
    // Older metadata without a descriptor must recover through the verified source.
    sqlx::query("DELETE FROM shardline_xet_xorbs")
        .execute(&owner)
        .await?;
    assert_eq!(
        delta_lookup(&client, &second, &read, &file).await?.status(),
        StatusCode::OK
    );
    let key = ObjectKey::parse(&format!(
        "xorbs/default/{}/{hash}.xorb",
        hash.get(..2).ok_or("invalid hash")?
    ))?;
    let index = PostgresIndexStore::new(owner.clone());
    let pending = index.begin_remote_delete(&key).await?;
    first.stop().await;
    let takeover_root = tempfile::tempdir()?;
    let restart_root = if remote {
        takeover_root.path()
    } else {
        root.path()
    };
    let restarted = Runtime::start_with_objects(restart_root, &database, objects).await?;
    for runtime in [&restarted, &second] {
        let blocked = delta_lookup(&client, runtime, &read, &file).await?;
        assert_eq!(blocked.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            blocked
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
        assert_eq!(
            delta_lookup(&client, runtime, &hidden, &file)
                .await?
                .status(),
            StatusCode::NOT_FOUND
        );
    }
    // This fixture never issued a DELETE; settling its marker is unambiguous.
    assert!(index.finish_remote_delete(&pending).await?);
    for runtime in [&restarted, &second] {
        assert_eq!(
            delta_lookup(&client, runtime, &read, &file).await?.status(),
            StatusCode::OK
        );
    }
    AsyncObjectStore::delete_if_present(&storage, &key).await?;
    for runtime in [&restarted, &second] {
        assert_eq!(
            delta_lookup(&client, runtime, &read, &file).await?.status(),
            StatusCode::NOT_FOUND
        );
    }
    restarted.stop().await;
    second.stop().await;
    owner.close().await;
    Ok(())
}

async fn delta_lookup(
    client: &Client,
    runtime: &Runtime,
    token: &str,
    file: &str,
) -> Result<reqwest::Response, Error> {
    Ok(client
        .get(format!("{}/v2/file-chunk-hashes/{file}", runtime.base))
        .bearer_auth(token)
        .header("X-Range-Dirty", "bytes=0-1")
        .send()
        .await?)
}

async fn lookup(
    client: &Client,
    runtime: &Runtime,
    token: &str,
    namespace: &str,
    member: &str,
) -> Result<reqwest::Response, Error> {
    Ok(client
        .get(format!("{}/v1/chunks/{namespace}/{member}", runtime.base))
        .bearer_auth(token)
        .send()
        .await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO"]
async fn native_dedup_delete_fencing_survives_replica_restart() -> Result<(), Error> {
    let stack = tokio::task::spawn_blocking(|| {
        shardline_test_support::DockerLocalStack::builder()
            .with_postgres()
            .with_minio()
            .start()
    })
    .await??
    .ok_or("Docker PostgreSQL and MinIO are required")?;
    let database = stack.postgres_url().ok_or("PostgreSQL is required")?;
    let owner = sqlx::PgPool::connect(&database).await?;
    shardline_server::apply_database_migrations(&owner).await?;
    let prefix = stack.unique_s3_key_prefix("dedup-lifecycle");
    let raw = stack
        .s3_raw_config(Some(&prefix))
        .ok_or("MinIO is required")?;
    let objects = S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_allow_http(raw.allow_http)
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            None,
        );
    let storage = ServerObjectStore::s3(objects.clone())?;
    let first_root = tempfile::tempdir()?;
    let second_root = tempfile::tempdir()?;
    let first = Runtime::start(first_root.path(), &database, objects.clone()).await?;
    let second = Runtime::start(second_root.path(), &database, objects.clone()).await?;
    let write = bearer_token(
        "owner",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    )?;
    let read = bearer_token(
        "owner",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    )?;
    let hidden = bearer_token(
        "other",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "other",
        "assets",
        Some("main"),
    )?;
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let content = b"dedup must not advertise an unresolved destructive attempt";
    let (xorb, hash) = single_chunk_xorb(content);
    client
        .post(format!("{}/v1/xorbs/default/{hash}", first.base))
        .bearer_auth(&write)
        .body(xorb)
        .send()
        .await?
        .error_for_status()?;
    let (shard, _file) = single_file_shard(&[(content, &hash)]);
    client
        .post(format!("{}/v1/shards", first.base))
        .bearer_auth(&write)
        .body(shard)
        .send()
        .await?
        .error_for_status()?;
    let member_hash = shardline_xet_core::merklehash::compute_data_hash(content);
    let member = member_hash.hex();
    let xorb_hash = shardline_xet_core::MerkleHash::from(
        *shardline_index::parse_xet_hash_hex(&hash)?.as_bytes(),
    );
    for runtime in [&first, &second] {
        for namespace in ["default", "default-merkledb"] {
            let response = lookup(&client, runtime, &read, namespace, &member).await?;
            assert_eq!(response.status(), StatusCode::OK);
            super::transfer_xorb_ranges::assert_scoped_dedup_reply(
                &response.bytes().await?,
                member_hash,
                shardline_xet_core::merklehash::compute_data_hash(b"unrelated"),
                xorb_hash,
                0,
            )?;
        }
    }
    let key = ObjectKey::parse(&format!(
        "xorbs/default/{}/{hash}.xorb",
        hash.get(..2).ok_or("invalid fixture hash")?,
    ))?;
    let index = PostgresIndexStore::new(owner.clone());
    let pending = index.begin_remote_delete(&key).await?;
    assert!(AsyncObjectStore::metadata(&storage, &key).await?.is_some());
    first.stop().await;
    let restarted = Runtime::start(first_root.path(), &database, objects).await?;
    for runtime in [&restarted, &second] {
        for namespace in ["default", "default-merkledb"] {
            let blocked = lookup(&client, runtime, &read, namespace, &member).await?;
            assert_eq!(blocked.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(
                blocked
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok()),
                Some("1")
            );
            // A pending delete must not become a cross-repository existence oracle.
            assert_eq!(
                lookup(&client, runtime, &hidden, namespace, &member)
                    .await?
                    .status(),
                StatusCode::NOT_FOUND,
            );
        }
    }
    // No remote DELETE was issued in this fixture: settle the abandoned marker.
    assert!(index.finish_remote_delete(&pending).await?);
    for runtime in [&restarted, &second] {
        for namespace in ["default", "default-merkledb"] {
            assert_eq!(
                lookup(&client, runtime, &read, namespace, &member)
                    .await?
                    .status(),
                StatusCode::OK
            );
        }
    }
    // Corruption in another repository's source must not change a hidden
    // member's response from 404 into a source-validation error.
    let damaged_store = storage.clone();
    let damaged_key = key.clone();
    tokio::task::spawn_blocking(move || {
        let bytes = b"damaged";
        damaged_store.put_overwrite(
            &damaged_key,
            ObjectBody::from_slice(bytes),
            &ObjectIntegrity::new(chunk_hash(bytes), 7),
        )
    })
    .await??;
    for runtime in [&restarted, &second] {
        for namespace in ["default", "default-merkledb"] {
            assert_eq!(
                lookup(&client, runtime, &hidden, namespace, &member)
                    .await?
                    .status(),
                StatusCode::NOT_FOUND,
            );
        }
    }
    // Inject a missing source while retaining discovery metadata. Neither
    // replica may advertise it or treat the lookup as a malformed request.
    AsyncObjectStore::delete_if_present(&storage, &key).await?;
    for runtime in [&restarted, &second] {
        for namespace in ["default", "default-merkledb"] {
            assert_eq!(
                lookup(&client, runtime, &read, namespace, &member)
                    .await?
                    .status(),
                StatusCode::NOT_FOUND
            );
        }
    }
    restarted.stop().await;
    second.stop().await;
    owner.close().await;
    Ok(())
}
