//! Kill the actual HTTP-serving process during a basic LFS request.
use super::*;
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const CHILD: &str = "lfs_upload_safety::process::lfs_process_child";

struct OwnedProcess(Child);

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        let _kill = self.0.kill();
        let _wait = self.0.wait();
    }
}

#[tokio::test]
#[ignore = "internal subprocess fixture; invoked by LFS process parent"]
async fn lfs_process_child() {
    let root = std::env::var_os("LFS_PROCESS_ROOT").expect("parent fixture root");
    let frontend = match std::env::var("LFS_PROCESS_FRONTEND").unwrap().as_str() {
        "hub" => ServerFrontend::Hub,
        "lfs" => ServerFrontend::Lfs,
        _ => panic!("invalid parent frontend"),
    };
    let server = start(Path::new(&root), frontend, true, None).await;
    let temporary = Path::new(&root).join("process-ready.tmp");
    std::fs::write(&temporary, &server.base).unwrap();
    std::fs::rename(temporary, Path::new(&root).join("process-ready")).unwrap();
    std::future::pending::<()>().await;
}

async fn launch(root: &Path, frontend: &str) -> (OwnedProcess, String) {
    let ready = root.join("process-ready");
    if ready.exists() {
        std::fs::remove_file(&ready).unwrap();
    }
    let mut child = OwnedProcess(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD, "--ignored"])
            .env("LFS_PROCESS_ROOT", root)
            .env("LFS_PROCESS_FRONTEND", frontend)
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
    for (frontend, prefix) in [("lfs", "/v1/lfs/objects"), ("hub", "/lfs/objects")] {
        let root = tempfile::tempdir().unwrap();
        let credential = token("writer", "process-safety", TokenScope::Write);
        let payload = vec![37_u8; 2 * 1024 * 1024];
        let oid = hex::encode(Sha256::digest(&payload));
        let path = format!("{prefix}/{oid}");
        let (mut child, base) = launch(root.path(), frontend).await;
        let address = base.strip_prefix("http://").unwrap();
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket.write_all(format!("PUT {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {credential}\r\nContent-Length: {}\r\n\r\n", payload.len()).as_bytes()).await.unwrap();
        // Leave the request unfinished. This qualifies termination during the
        // wire upload, not a particular persistence boundary inside ingestion.
        socket.write_all(&payload[..1024 * 1024]).await.unwrap();
        child.0.kill().unwrap();
        assert_eq!(child.0.wait().unwrap().signal(), Some(9));
        drop(socket);
        let (mut child, base) = launch(root.path(), frontend).await;
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
        child.0.kill().unwrap();
        assert_eq!(child.0.wait().unwrap().signal(), Some(9));
        let (_child, base) = launch(root.path(), frontend).await;
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
