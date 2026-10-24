//! Released and candidate processes read both generations of packed S3 data.
use super::*;

async fn verify(server: &LocalServer, token: &str, key: &str, expected: &[u8]) {
    assert_s3_bytes(&server.base, token, key, expected, "packed range upgrade").await;
    for start in [0, expected.len() / 2, expected.len() - 4096] {
        let end = start + 4095;
        let response = reqwest::Client::new()
            .get(format!("{}/{BUCKET}/{key}", server.base))
            .bearer_auth(token)
            .header("Range", format!("bytes={start}-{end}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers()["content-range"],
            format!("bytes {start}-{end}/{}", expected.len())
        );
        assert_eq!(
            response.bytes().await.unwrap().as_ref(),
            &expected[start..=end]
        );
    }
}

#[tokio::test]
async fn released_remote_packed_ranges_survive_upgrade_and_rollback() {
    let old = resolve_n_minus_one_binary("packed range upgrade")
        .expect("released predecessor required for remote packed range qualification");
    let current = resolve_shardline_binary().expect("candidate binary required");
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()
        .expect("packed migration services")
        .expect("packed migration requires Docker");
    let postgres = services.postgres_url().unwrap();
    let prefix = services.unique_s3_key_prefix("packed-range-predecessor");
    let storage = services.s3_raw_config(Some(&prefix)).unwrap();
    let storage_env = vec![
        ("SHARDLINE_OBJECT_STORAGE_ADAPTER", "s3".to_owned()),
        ("SHARDLINE_S3_BUCKET", storage.bucket),
        ("SHARDLINE_S3_REGION", storage.region),
        ("SHARDLINE_S3_ENDPOINT", storage.endpoint.unwrap()),
        ("SHARDLINE_S3_ACCESS_KEY_ID", storage.access_key.unwrap()),
        (
            "SHARDLINE_S3_SECRET_ACCESS_KEY",
            storage.secret_key.unwrap(),
        ),
        ("SHARDLINE_S3_KEY_PREFIX", prefix),
        ("SHARDLINE_S3_ALLOW_HTTP", "true".to_owned()),
    ];
    let root = TempDir::new().unwrap();
    let token = mint_token("drill", "drill", TokenScope::Write);
    let mut state = 748973_u64;
    let payload: Vec<u8> = (0..4 * 1024 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    let candidate_payload: Vec<u8> = payload.iter().map(|byte| byte ^ 0xa5).collect();
    for (binary, migrate_first, write_key) in [
        (&old, true, Some("released-packed")),
        (&current, true, Some("candidate-packed")),
        (&old, false, None),
        (&current, false, None),
    ] {
        if migrate_first {
            migrate_metadata(binary, root.path(), Some(&postgres)).await;
        }
        let mut server = LocalServer::spawn_with_storage(
            binary,
            root.path(),
            Some(&postgres),
            "s3",
            &storage_env,
        );
        server.ready().await;
        if let Some(key) = write_key {
            let bytes = if key == "released-packed" {
                &payload
            } else {
                &candidate_payload
            };
            assert!(
                s3_put(&server.base, &token, key, bytes.clone())
                    .await
                    .status()
                    .is_success()
            );
        }
        verify(&server, &token, "released-packed", &payload).await;
        if write_key != Some("released-packed") {
            verify(&server, &token, "candidate-packed", &candidate_payload).await;
        }
        if write_key == Some("candidate-packed") {
            tokio::join!(
                maintenance::gc_before_rollback(
                    &current,
                    root.path(),
                    Some(&postgres),
                    &storage_env,
                    "s3"
                ),
                verify(&server, &token, "released-packed", &payload),
                verify(&server, &token, "candidate-packed", &candidate_payload),
            );
            verify(&server, &token, "released-packed", &payload).await;
            verify(&server, &token, "candidate-packed", &candidate_payload).await;
        }
        drop(server);
    }
}
