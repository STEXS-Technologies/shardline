//! Released-binary qualification of local metadata, objects and backup recovery.

use super::*;
use shardline_index::{LocalRecordStore, XetFileIdentity};

struct LocalServer {
    child: std::process::Child,
    base: String,
    log: NamedTempFile,
    _config: NamedTempFile,
}

impl LocalServer {
    fn spawn(binary: &Path, root: &Path) -> Self {
        let port = free_tcp_port().expect("local upgrade port");
        let log = NamedTempFile::new().unwrap();
        let config = NamedTempFile::new().unwrap();
        let base = format!("http://127.0.0.1:{port}");
        let child = Command::new(binary)
            .env_clear()
            .arg("--config")
            .arg(config.path())
            .arg("serve")
            .env("SHARDLINE_ROOT_DIR", root)
            .env("SHARDLINE_BIND_ADDR", format!("127.0.0.1:{port}"))
            .env("SHARDLINE_PUBLIC_BASE_URL", &base)
            .env("SHARDLINE_OBJECT_STORAGE_ADAPTER", "local")
            .env("SHARDLINE_SERVER_ROLE", "all")
            .env("SHARDLINE_SERVER_FRONTENDS", "s3,lfs")
            .env("SHARDLINE_AUTH_PROVIDER", "local")
            .env(
                "SHARDLINE_TOKEN_SIGNING_KEY",
                std::str::from_utf8(TEST_SIGNING_KEY).unwrap(),
            )
            .env("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER", "memory")
            .stdout(Stdio::from(log.reopen().unwrap()))
            .stderr(Stdio::from(log.reopen().unwrap()))
            .spawn()
            .unwrap();
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
    unmigrated.rejected("schema is stale").await;
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
    let mut incompatible = LocalServer::spawn(&old, root.path());
    incompatible.rejected("unknown migration").await;
    drop(incompatible);
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
}
