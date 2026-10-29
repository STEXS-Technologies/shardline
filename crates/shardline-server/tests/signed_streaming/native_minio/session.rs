use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MINIO_SIGNER_BIN"]
async fn native_minio_signed_streaming_session_credentials() {
    exercise_session(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MINIO_SIGNER_BIN and disposable DATABASE_URL"]
async fn native_minio_signed_streaming_session_credentials_on_postgres() {
    let database = std::env::var("DATABASE_URL").unwrap();
    exercise_session(Some(&database)).await;
}

async fn exercise_session(database: Option<&str>) {
    let binary = std::env::var("MINIO_SIGNER_BIN").unwrap();
    let root = tempfile::tempdir().unwrap();
    let credential = token("writer", "aws-resume", TokenScope::Write);
    let identity = signing_identity(credential.clone())
        .with_session_token(shardline_protocol::SecretString::new(
            "native-session-token".to_owned(),
        ))
        .unwrap();
    let server = start_configured(
        root.path(),
        ServerFrontend::S3,
        true,
        database,
        None,
        Some(identity),
    )
    .await;
    let url = format!("{}/aws-resume.locks/native-session.bin", server.base);
    let client = Client::new();
    for trailer in [false, true] {
        for session in ["native-session-token", "", "substituted-token"] {
            let mut command = std::process::Command::new(&binary);
            command.args(["--session-token", session]);
            if trailer {
                command.arg("--trailer");
            }
            if session != "native-session-token" {
                command.arg("--expect-denied");
            }
            command.arg(&url);
            let output = tokio::task::spawn_blocking(move || {
                shardline_test_support::native_client_output(
                    &mut command,
                    std::time::Duration::from_secs(180),
                )
                .unwrap()
            })
            .await
            .unwrap();
            assert!(
                output.status.success(),
                "native session signer failed: {}",
                output.status
            );
            let response = client
                .get(&url)
                .bearer_auth(&credential)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.bytes().await.unwrap().as_ref(),
                b"native MinIO signed streaming payload\n".repeat(5000)
            );
        }
    }
    server.stop().await;
}
