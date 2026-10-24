//! Released-binary qualification of local metadata, objects and backup recovery.

use super::*;
use shardline_index::{LocalRecordStore, XetFileIdentity};

mod creation_policy;
mod native_xet;
mod postgres;

struct LocalServer {
    child: std::process::Child,
    base: String,
    log: NamedTempFile,
    _config: NamedTempFile,
}

impl LocalServer {
    fn spawn(binary: &Path, root: &Path) -> Self {
        Self::spawn_with_postgres(binary, root, None)
    }

    fn spawn_with_postgres(binary: &Path, root: &Path, postgres: Option<&str>) -> Self {
        Self::spawn_with_frontends(binary, root, postgres, "s3,lfs")
    }

    fn spawn_with_frontends(
        binary: &Path,
        root: &Path,
        postgres: Option<&str>,
        frontends: &str,
    ) -> Self {
        Self::spawn_with_storage(binary, root, postgres, frontends, &[])
    }

    fn spawn_with_storage(
        binary: &Path,
        root: &Path,
        postgres: Option<&str>,
        frontends: &str,
        storage_env: &[(&str, String)],
    ) -> Self {
        let port = free_tcp_port().expect("local upgrade port");
        let log = NamedTempFile::new().unwrap();
        let config = NamedTempFile::new().unwrap();
        let base = format!("http://127.0.0.1:{port}");
        let mut command = Command::new(binary);
        command.env_clear();
        if let Some(url) = postgres {
            command.env("SHARDLINE_INDEX_POSTGRES_URL", url);
        }
        command
            .arg("--config")
            .arg(config.path())
            .arg("serve")
            .env("SHARDLINE_ROOT_DIR", root)
            // env_clear must not silently move large native staging back onto
            // a quota-limited /tmp when the drill selected disk-backed scratch.
            .env("TMPDIR", std::env::temp_dir())
            .env("SHARDLINE_BIND_ADDR", format!("127.0.0.1:{port}"))
            .env("SHARDLINE_PUBLIC_BASE_URL", &base)
            .env("SHARDLINE_OBJECT_STORAGE_ADAPTER", "local")
            .env("SHARDLINE_SERVER_ROLE", "all")
            .env("SHARDLINE_SERVER_FRONTENDS", frontends)
            .env("SHARDLINE_AUTH_PROVIDER", "local")
            .env(
                "SHARDLINE_TOKEN_SIGNING_KEY",
                std::str::from_utf8(TEST_SIGNING_KEY).unwrap(),
            )
            .env("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER", "memory")
            .stdout(Stdio::from(log.reopen().unwrap()))
            .stderr(Stdio::from(log.reopen().unwrap()));
        for (name, value) in storage_env {
            command.env(name, value);
        }
        let child = command.spawn().unwrap();
        Self {
            child,
            base,
            log,
            _config: config,
        }
    }

    fn logs(&self) -> String {
        std::fs::read_to_string(self.log.path()).unwrap()
    }

    async fn ready(&mut self) {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        for _ in 0..150 {
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "startup failed: {}",
                self.logs()
            );
            if client
                .get(format!("{}/healthz", self.base))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!("local startup timeout: {}", self.logs());
    }

    async fn rejected(&mut self, diagnostic: &str) {
        for _ in 0..150 {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(!status.success());
                assert!(self.logs().contains(diagnostic), "{}", self.logs());
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!(
            "incompatible binary did not reject startup: {}",
            self.logs()
        );
    }
}

impl Drop for LocalServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking() {
            eprintln!("upgrade server {} diagnostics:\n{}", self.base, self.logs());
        }
    }
}

fn migrate(binary: &Path, root: &Path) {
    let config = NamedTempFile::new().unwrap();
    let output = Command::new(binary)
        .env_clear()
        .arg("--config")
        .arg(config.path())
        .args(["db", "migrate", "local-up", "--root"])
        .arg(root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "migration: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn copy_offline_root(source: &Path, destination: &Path) {
    std::fs::create_dir_all(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        let kind = entry.file_type().unwrap();
        if kind.is_dir() {
            copy_offline_root(&entry.path(), &target);
        } else {
            assert!(kind.is_file(), "backup fixture must not follow symlinks");
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn released_local_rolling_upgrade_preserves_live_writes_and_old_restart() {
    rolling_upgrade(None).await;
}

async fn rolling_upgrade(postgres: Option<&str>) {
    let Some(old) = resolve_n_minus_one_binary("local rolling protocol upgrade") else {
        return;
    };
    let current = resolve_shardline_binary().expect("current binary required");
    let root = TempDir::new().unwrap();
    let token = mint_token("drill", "drill", TokenScope::Write);
    migrate_metadata(&old, root.path(), postgres).await;
    let mut previous = LocalServer::spawn_with_postgres(&old, root.path(), postgres);
    previous.ready().await;
    let old_base = previous.base.clone();
    let writer_token = token.clone();
    let (started, first_acknowledged) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(async move {
        let mut started = Some(started);
        for index in 0..32_u8 {
            let bytes = vec![index; 65_537];
            let response =
                s3_put(&old_base, &writer_token, &format!("rolling-{index}"), bytes).await;
            assert!(
                response.status().is_success(),
                "live write {index}: {}: {}",
                response.status(),
                response.text().await.unwrap()
            );
            if let Some(started) = started.take() {
                started.send(()).unwrap();
            }
        }
    });
    first_acknowledged.await.unwrap();
    migrate_metadata(&current, root.path(), postgres).await;
    let mut upgraded = LocalServer::spawn_with_postgres(&current, root.path(), postgres);
    upgraded.ready().await;
    writer.await.unwrap();
    for index in 0..32_u8 {
        let key = format!("rolling-{index}");
        let bytes = vec![index; 65_537];
        assert_s3_bytes(&previous.base, &token, &key, &bytes, "old live reader").await;
        assert_s3_bytes(&upgraded.base, &token, &key, &bytes, "new live reader").await;
    }
    let bytes = vec![91; 131_079];
    assert!(
        s3_put(&upgraded.base, &token, "new-writer", bytes.clone())
            .await
            .status()
            .is_success()
    );
    assert_s3_bytes(
        &previous.base,
        &token,
        "new-writer",
        &bytes,
        "old reader of new write",
    )
    .await;
    let checksummed_bytes = b"checksummed write during rolling upgrade";
    let checksum = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        Sha256::digest(checksummed_bytes),
    );
    let checksummed_write = reqwest::Client::new()
        .put(format!("{}/{BUCKET}/checksummed-write", upgraded.base))
        .bearer_auth(&token)
        .header("x-amz-checksum-sha256", checksum)
        .body(checksummed_bytes.to_vec())
        .send()
        .await
        .unwrap();
    assert!(checksummed_write.status().is_success());
    // 1.12.1 does not know the checksum metadata key, but the storage record
    // and object bytes remain readable through the old process.
    assert_s3_bytes(
        &previous.base,
        &token,
        "checksummed-write",
        checksummed_bytes,
        "old reader of checksummed write",
    )
    .await;
    let tagged_bytes = b"tagged object during rolling upgrade";
    let tagged_write = reqwest::Client::new()
        .put(format!("{}/{BUCKET}/tagged-rollout", upgraded.base))
        .bearer_auth(&token)
        .header("x-amz-tagging", "rollout=active")
        .body(tagged_bytes.to_vec())
        .send()
        .await
        .unwrap();
    assert!(tagged_write.status().is_success());
    assert_s3_bytes(
        &previous.base,
        &token,
        "tagged-rollout",
        tagged_bytes,
        "old reader of newly tagged object",
    )
    .await;
    let old_tagging_route = reqwest::Client::new()
        .get(format!("{}/{BUCKET}/tagged-rollout?tagging", previous.base))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        old_tagging_route.status(),
        reqwest::StatusCode::NOT_IMPLEMENTED
    );

    // The released writer has no tagging support. A same-key PutObject without
    // an x-amz-tagging header replaces the object's tag set with the empty set,
    // matching S3 semantics. Deployments must keep tagged-key mutations on the
    // upgraded replicas until the old writer is drained.
    let old_replacement = b"old writer replacement without tags";
    assert!(
        s3_put(
            &previous.base,
            &token,
            "tagged-rollout",
            old_replacement.to_vec(),
        )
        .await
        .status()
        .is_success()
    );
    let tags_after_old_write = reqwest::Client::new()
        .get(format!("{}/{BUCKET}/tagged-rollout?tagging", upgraded.base))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(tags_after_old_write.status(), reqwest::StatusCode::OK);
    let tags_after_old_write = tags_after_old_write.text().await.unwrap();
    assert!(tags_after_old_write.contains("<TagSet></TagSet>"));
    drop(previous);
    // A rollout must tolerate an old replica restarting, without deleting
    // migration records, downgrading SQL, or discarding newly acknowledged data.
    let mut restarted = LocalServer::spawn_with_postgres(&old, root.path(), postgres);
    restarted.ready().await;
    assert_s3_bytes(
        &restarted.base,
        &token,
        "new-writer",
        &bytes,
        "old replica restart",
    )
    .await;
    assert_s3_bytes(
        &restarted.base,
        &token,
        "checksummed-write",
        checksummed_bytes,
        "old restart reader of checksummed write",
    )
    .await;
}

async fn migrate_metadata(binary: &Path, root: &Path, postgres: Option<&str>) {
    let binary = binary.to_path_buf();
    let root = root.to_path_buf();
    let postgres = postgres.map(str::to_owned);
    tokio::task::spawn_blocking(move || {
        if let Some(url) = postgres {
            let output = Command::new(binary)
                .env_clear()
                .args(["db", "migrate", "up", "--database-url", &url])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "migration: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        } else {
            migrate(&binary, &root);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn released_local_upgrade_preserves_objects_and_protocol_state() {
    let Some(old) = resolve_n_minus_one_binary("local protocol upgrade") else {
        return;
    };
    let current =
        resolve_shardline_binary().expect("current binary required with released baseline");
    let root = TempDir::new().unwrap();
    let backup = TempDir::new().unwrap();
    let token = mint_token("drill", "drill", TokenScope::Write);
    let old_bytes = vec![17; 131_079];
    let new_bytes = vec![29; 196_619];
    migrate(&old, root.path());
    let mut server = LocalServer::spawn(&old, root.path());
    server.ready().await;
    assert!(
        s3_put(&server.base, &token, "before", old_bytes.clone())
            .await
            .status()
            .is_success()
    );
    drop(server);
    // Copy metadata AND objects only after the old process has closed them.
    copy_offline_root(root.path(), backup.path());
    let mut unmigrated = LocalServer::spawn(&current, root.path());
    unmigrated.rejected("frontend schema is incompatible").await;
    drop(unmigrated);
    migrate(&current, root.path());
    let mut server = LocalServer::spawn(&current, root.path());
    server.ready().await;
    assert_s3_bytes(&server.base, &token, "before", &old_bytes, "local upgrade").await;
    assert!(
        s3_put(&server.base, &token, "after", new_bytes.clone())
            .await
            .status()
            .is_success()
    );
    let client = reqwest::Client::new();
    let lock = client
        .post(format!("{}/v1/lfs/locks", server.base))
        .bearer_auth(&token)
        .json(&serde_json::json!({"path":"models/weights.bin"}))
        .send()
        .await
        .unwrap();
    assert_eq!(lock.status(), reqwest::StatusCode::CREATED);
    let lock: serde_json::Value = lock.json().await.unwrap();
    assert!(lock["lock"]["id"].as_str().is_some_and(|id| !id.is_empty()));
    drop(server);
    // A table-persistence fixture; real native Xet uploads are tested separately.
    let identity = XetFileIdentity {
        repository_scope: RepositoryScope::new(
            RepositoryProvider::Generic,
            "drill",
            "drill",
            Some("main"),
        )
        .unwrap(),
        sha256: "a".repeat(64),
        file_id: "b".repeat(64),
        total_bytes: 42,
    };
    let store = LocalRecordStore::new(root.path().to_path_buf()).unwrap();
    store
        .commit_native_shard_metadata_with_identities(&[], &[], std::slice::from_ref(&identity))
        .await
        .unwrap();
    drop(store);
    let mut server = LocalServer::spawn(&current, root.path());
    server.ready().await;
    assert_s3_bytes(&server.base, &token, "before", &old_bytes, "local restart").await;
    assert_s3_bytes(&server.base, &token, "after", &new_bytes, "local restart").await;
    let locks: serde_json::Value = client
        .get(format!("{}/v1/lfs/locks", server.base))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(locks["locks"][0]["id"], lock["lock"]["id"]);
    let duplicate = client
        .post(format!("{}/v1/lfs/locks", server.base))
        .bearer_auth(&token)
        .json(&serde_json::json!({"path":"models/weights.bin"}))
        .send()
        .await
        .unwrap();
    assert_eq!(duplicate.status(), reqwest::StatusCode::CONFLICT);
    let store = LocalRecordStore::new(root.path().to_path_buf()).unwrap();
    assert_eq!(
        store
            .xet_file_identity(&identity.repository_scope, &identity.sha256)
            .unwrap(),
        Some(identity)
    );
    drop(store);
    drop(server);
    let mut previous = LocalServer::spawn(&old, root.path());
    previous.ready().await;
    assert_s3_bytes(
        &previous.base,
        &token,
        "before",
        &old_bytes,
        "old replica restart",
    )
    .await;
    assert_s3_bytes(
        &previous.base,
        &token,
        "after",
        &new_bytes,
        "old replica restart",
    )
    .await;
    drop(previous);
    let mut restored = LocalServer::spawn(&old, backup.path());
    restored.ready().await;
    assert_s3_bytes(
        &restored.base,
        &token,
        "before",
        &old_bytes,
        "backup restore",
    )
    .await;
    assert_eq!(
        s3_get(&restored.base, &token, "after").await.status(),
        reqwest::StatusCode::NOT_FOUND
    );
    drop(restored);
    // Restoration was isolated: acknowledged post-backup data still exists here.
    let mut retained = LocalServer::spawn(&current, root.path());
    retained.ready().await;
    assert_s3_bytes(
        &retained.base,
        &token,
        "after",
        &new_bytes,
        "retained upgraded deployment",
    )
    .await;
    let retained_locks: serde_json::Value = client
        .get(format!("{}/v1/lfs/locks", retained.base))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(retained_locks["locks"][0]["id"], lock["lock"]["id"]);
}
