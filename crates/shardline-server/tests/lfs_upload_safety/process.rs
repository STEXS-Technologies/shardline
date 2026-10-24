//! Kill the actual HTTP-serving process during a basic LFS request.
use super::*;
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const CHILD: &str = "lfs_upload_safety::process::lfs_process_child";
mod split;

struct OwnedProcess(Child);

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        let _kill = self.0.kill();
        let _wait = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "internal subprocess fixture; invoked by LFS process parent"]
async fn lfs_process_child() {
    let root = std::env::var_os("LFS_PROCESS_ROOT").expect("parent fixture root");
    let frontend = match std::env::var("LFS_PROCESS_FRONTEND").unwrap().as_str() {
        "hub" => ServerFrontend::Hub,
        "lfs" => ServerFrontend::Lfs,
        _ => panic!("invalid parent frontend"),
    };
    let database = std::env::var("LFS_PROCESS_DATABASE").ok();
    let storage = database.as_ref().map(|_| {
        S3ObjectStoreConfig::new(
            std::env::var("LFS_PROCESS_BUCKET").unwrap(),
            "us-east-1".to_owned(),
        )
        .with_endpoint(Some(std::env::var("LFS_PROCESS_ENDPOINT").unwrap()))
        .with_allow_http(true)
        .with_key_prefix(Some(&std::env::var("LFS_PROCESS_PREFIX").unwrap()))
        .with_credentials(
            Some(shardline_protocol::SecretString::new(
                std::env::var("LFS_PROCESS_ACCESS").unwrap(),
            )),
            Some(shardline_protocol::SecretString::new(
                std::env::var("LFS_PROCESS_SECRET").unwrap(),
            )),
            None,
        )
    });
    let public = std::env::var("LFS_PROCESS_PUBLIC").ok();
    let role = std::env::var("LFS_PROCESS_ROLE")
        .ok()
        .map(|value| match value.as_str() {
            "api" => shardline_server::ServerRole::Api,
            "transfer" => shardline_server::ServerRole::Transfer,
            _ => panic!("invalid parent role"),
        });
    let routing = role.map(|role| (role, public.as_deref().expect("split public URL")));
    let server = start_routed(
        Path::new(&root),
        frontend,
        true,
        database.as_deref(),
        storage,
        None,
        routing,
    )
    .await;
    let temporary = Path::new(&root).join("process-ready.tmp");
    std::fs::write(&temporary, &server.base).unwrap();
    std::fs::rename(temporary, Path::new(&root).join("process-ready")).unwrap();
    std::future::pending::<()>().await;
}

async fn launch(
    root: &Path,
    frontend: &str,
    environment: &[(&str, String)],
) -> (OwnedProcess, String) {
    let ready = root.join("process-ready");
    if ready.exists() {
        std::fs::remove_file(&ready).unwrap();
    }
    let mut child = OwnedProcess(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD, "--ignored"])
            .env("LFS_PROCESS_ROOT", root)
            .env("LFS_PROCESS_FRONTEND", frontend)
            .envs(environment.iter().map(|(name, value)| (*name, value)))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let base = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "LFS child exited before readiness"
            );
            if let Ok(base) = std::fs::read_to_string(&ready)
                && !base.is_empty()
            {
                break base;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("LFS child readiness deadline");
    (child, base)
}

#[tokio::test]
async fn lfs_process_kill_preserves_retry_and_acknowledged_bytes() {
    exercise(&[]).await;
}

async fn exercise(environment: &[(&str, String)]) {
    for (frontend, prefix) in [("lfs", "/v1/lfs/objects"), ("hub", "/lfs/objects")] {
        let root = tempfile::tempdir().unwrap();
        let credential = token("writer", "process-safety", TokenScope::Write);
        let payload = vec![37_u8; 2 * 1024 * 1024];
        let oid = hex::encode(Sha256::digest(&payload));
        let path = format!("{prefix}/{oid}");
        let (mut child, base) = launch(root.path(), frontend, environment).await;
        let address = base.strip_prefix("http://").unwrap();
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket.write_all(format!("PUT {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {credential}\r\nContent-Length: {}\r\n\r\n", payload.len()).as_bytes()).await.unwrap();
        // Leave the request unfinished. This qualifies termination during the
        // wire upload, not a particular persistence boundary inside ingestion.
        socket.write_all(&payload[..1024 * 1024]).await.unwrap();
        child.0.kill().unwrap();
        assert_eq!(child.0.wait().unwrap().signal(), Some(9));
        drop(socket);
        let recovery_root = tempfile::tempdir().unwrap();
        let retry_root = if environment.is_empty() {
            root.path()
        } else {
            recovery_root.path()
        };
        let (mut child, base) = launch(retry_root, frontend, environment).await;
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap();
        let url = format!("{base}{path}");
        assert_eq!(
            client
                .get(&url)
                .bearer_auth(&credential)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert!(
            client
                .put(&url)
                .bearer_auth(&credential)
                .body(payload.clone())
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        let replica_root = tempfile::tempdir().unwrap();
        let replica = if environment.is_empty() {
            None
        } else {
            Some(launch(replica_root.path(), frontend, environment).await)
        };
        child.0.kill().unwrap();
        assert_eq!(child.0.wait().unwrap().signal(), Some(9));
        if let Some((_replica, replica_base)) = &replica {
            let response = client
                .get(format!("{replica_base}{path}"))
                .bearer_auth(&credential)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.bytes().await.unwrap().as_ref(), payload.as_slice());
        }
        let (_child, base) = launch(root.path(), frontend, environment).await;
        let downloaded = client
            .get(format!("{base}{path}"))
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(downloaded.status(), StatusCode::OK);
        assert_eq!(
            downloaded.bytes().await.unwrap().as_ref(),
            payload.as_slice()
        );
    }
}

#[cfg(feature = "docker")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO"]
async fn lfs_process_kill_postgres_minio() {
    let services = tokio::task::spawn_blocking(|| {
        shardline_test_support::DockerLocalStack::builder()
            .with_postgres()
            .with_minio()
            .start()
            .expect("start disposable LFS process backends")
            .expect("Docker required for LFS process qualification")
    })
    .await
    .unwrap();
    let prefix = services.unique_s3_key_prefix("lfs-process");
    let raw = services.s3_raw_config(Some(&prefix)).unwrap();
    let environment = [
        ("LFS_PROCESS_DATABASE", services.postgres_url().unwrap()),
        ("LFS_PROCESS_BUCKET", raw.bucket),
        ("LFS_PROCESS_ENDPOINT", raw.endpoint.unwrap()),
        ("LFS_PROCESS_PREFIX", prefix),
        ("LFS_PROCESS_ACCESS", raw.access_key.unwrap()),
        ("LFS_PROCESS_SECRET", raw.secret_key.unwrap()),
    ];
    exercise(&environment).await;
    split::exercise(&environment).await;
}
