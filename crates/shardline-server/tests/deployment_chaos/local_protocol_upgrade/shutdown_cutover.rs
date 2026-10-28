//! Real process drain across released/candidate binaries, without upload replay.

use super::*;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admitted_upload_survives_sigterm_upgrade_and_rollback() {
    tokio::time::timeout(Duration::from_secs(90), exercise(None, &[]))
        .await
        .expect("released-process drain/upgrade/rollback exceeded its deadline");
}

#[cfg(feature = "docker")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_admitted_upload_survives_sigterm_upgrade_and_rollback() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let services = shardline_test_support::DockerLocalStack::builder()
            .with_postgres()
            .with_minio()
            .start()
            .expect("remote drain services")
            .expect("remote drain qualification requires Docker");
        let postgres = services.postgres_url().expect("PostgreSQL fixture");
        let prefix = services.unique_s3_key_prefix("sigterm-predecessor");
        let s3 = services
            .s3_raw_config(Some(&prefix))
            .expect("MinIO fixture");
        let storage_env = vec![
            ("SHARDLINE_OBJECT_STORAGE_ADAPTER", "s3".to_owned()),
            ("SHARDLINE_S3_BUCKET", s3.bucket),
            ("SHARDLINE_S3_REGION", s3.region),
            ("SHARDLINE_S3_ENDPOINT", s3.endpoint.unwrap()),
            ("SHARDLINE_S3_ACCESS_KEY_ID", s3.access_key.unwrap()),
            ("SHARDLINE_S3_SECRET_ACCESS_KEY", s3.secret_key.unwrap()),
            ("SHARDLINE_S3_KEY_PREFIX", prefix),
            ("SHARDLINE_S3_ALLOW_HTTP", "true".to_owned()),
        ];
        exercise(Some(&postgres), &storage_env).await;
    })
    .await
    .expect("remote released-process drain/upgrade/rollback exceeded its deadline");
}

async fn exercise(postgres: Option<&str>, storage_env: &[(&str, String)]) {
    let Some(old) = resolve_n_minus_one_binary("admitted upload graceful upgrade") else {
        return;
    };
    let current = resolve_shardline_binary().expect("current binary required");
    let root = TempDir::new().unwrap();
    let token = mint_token("drill", "drill", TokenScope::Write);
    migrate_metadata(&old, root.path(), postgres).await;
    let mut predecessor =
        LocalServer::spawn_with_storage(&old, root.path(), postgres, "s3,lfs", storage_env);
    predecessor.ready().await;
    let original = drain_upload(&mut predecessor, &token, 0x37).await;
    drop(predecessor);

    migrate_metadata(&current, root.path(), postgres).await;
    let mut candidate =
        LocalServer::spawn_with_storage(&current, root.path(), postgres, "s3,lfs", storage_env);
    candidate.ready().await;
    verify(&candidate, &token, &original).await;
    let added = drain_upload(&mut candidate, &token, 0x63).await;
    drop(candidate);

    let mut rollback =
        LocalServer::spawn_with_storage(&old, root.path(), postgres, "s3,lfs", storage_env);
    rollback.ready().await;
    // No retry may recreate missing data before either post-cutover read.
    verify(&rollback, &token, &original).await;
    verify(&rollback, &token, &added).await;
}

async fn drain_upload(server: &mut LocalServer, token: &str, seed: u8) -> (String, Vec<u8>) {
    let payload = vec![seed; 1024 * 1024];
    let oid = format!("{:x}", Sha256::digest(&payload));
    let path = format!("/v1/lfs/objects/{oid}");
    let address = server.base.strip_prefix("http://").unwrap();
    let mut connection = BufReader::new(tokio::net::TcpStream::connect(address).await.unwrap());
    connection.get_mut().write_all(format!(
        "PUT {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
        payload.len(),
    ).as_bytes()).await.unwrap();
    let mut status = String::new();
    connection.read_line(&mut status).await.unwrap();
    assert!(status.starts_with("HTTP/1.1 100"), "admission: {status}");
    loop {
        let mut line = String::new();
        connection.read_line(&mut line).await.unwrap();
        if line == "\r\n" {
            break;
        }
        assert!(!line.is_empty(), "truncated Continue response");
    }

    // Signal the actual child, not a library shutdown future. Its PID remains
    // owned and unreaped, so this cannot target a subsequently reused PID.
    let signal = shardline_test_support::native_client_output(
        Command::new("/bin/kill").args(["-TERM", &server.child.id().to_string()]),
        Duration::from_secs(5),
    )
    .unwrap();
    assert!(signal.status.success(), "SIGTERM delivery failed");
    loop {
        assert!(
            server.child.try_wait().unwrap().is_none(),
            "process exited with an admitted upload unfinished: {}",
            server.logs()
        );
        if tokio::net::TcpStream::connect(address).await.is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    connection.get_mut().write_all(&payload).await.unwrap();
    let mut response = Vec::new();
    connection.read_to_end(&mut response).await.unwrap();
    let status =
        std::str::from_utf8(response.split(|byte| *byte == b'\n').next().unwrap()).unwrap();
    assert!(
        status.starts_with("HTTP/1.1 200") || status.starts_with("HTTP/1.1 201"),
        "draining upload failed: {status}; {}",
        server.logs()
    );
    loop {
        if let Some(status) = server.child.try_wait().unwrap() {
            assert!(
                status.success(),
                "ungraceful exit: {status}; {}",
                server.logs()
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (path, payload)
}

async fn verify(server: &LocalServer, token: &str, uploaded: &(String, Vec<u8>)) {
    let bytes = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
        .get(format!("{}{}", server.base, uploaded.0))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), uploaded.1.as_slice());
}
