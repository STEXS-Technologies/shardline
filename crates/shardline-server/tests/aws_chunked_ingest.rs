//! Actual HTTP framing and publication regressions. Signatures/checksums are
//! framing controls only: this suite does not assert cryptographic verification.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use shardline_protocol::{
    RepositoryProvider, RepositoryScope, TokenClaims, TokenScope, TokenSigner,
};
use shardline_server::{DeploymentMode, ServerConfig, ServerFrontend, app};
use std::{num::NonZeroUsize, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const VALID: &[u8] = b"3\r\nnew\r\n0\r\n\r\n";

async fn run_ingest(database_url: Option<&str>) {
    let root = tempfile::tempdir().unwrap();
    let key = b"0123456789abcdef0123456789abcdef";
    let mut config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".into(),
        root.path().into(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_frontends([ServerFrontend::S3])
    .unwrap()
    .with_deployment_mode(DeploymentMode::Insecure)
    .with_token_signing_key(key.to_vec())
    .unwrap();
    if let Some(url) = database_url {
        let pool = sqlx::PgPool::connect(url).await.unwrap();
        shardline_server::apply_database_migrations(&pool)
            .await
            .unwrap();
        pool.close().await;
        config = config.with_index_postgres_url(url.to_owned()).unwrap();
    }
    let name = format!(
        "ingest-{}",
        root.path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .trim_start_matches('.')
            .to_ascii_lowercase()
    );
    let token = TokenSigner::new(key)
        .unwrap()
        .sign(
            &TokenClaims::new(
                "shardline",
                "audit",
                TokenScope::Write,
                RepositoryScope::new(RepositoryProvider::Generic, "audit", &name, None).unwrap(),
                u64::MAX,
            )
            .unwrap(),
        )
        .unwrap();
    let router = app::router(config).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let base = format!("http://{address}/audit.{name}");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _shutdown_result = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .put(&base)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let object = format!("{base}/object");
    // Each failure starts with committed old bytes and ends with a valid retry.
    let long_line = format!("3;chunk-signature={}\r\nnew\r\n0\r\n\r\n", "a".repeat(2048));
    let cases: Vec<(&str, Vec<u8>, Vec<&str>)> = vec![
        (
            "missing-final-crlf",
            b"3\r\nnew\r\n0\r\n".to_vec(),
            vec!["3"],
        ),
        ("trailing-garbage", [VALID, b"junk"].concat(), vec!["3"]),
        ("missing-zero", b"3\r\nnew\r\n".to_vec(), vec!["3"]),
        ("truncated-data", b"5\r\nnew".to_vec(), vec!["5"]),
        ("short-declaration", VALID.to_vec(), vec!["2"]),
        ("long-declaration", VALID.to_vec(), vec!["4"]),
        ("missing-declaration", VALID.to_vec(), vec![]),
        ("invalid-declaration", VALID.to_vec(), vec!["invalid"]),
        ("signed-declaration", VALID.to_vec(), vec!["+3"]),
        ("negative-declaration", VALID.to_vec(), vec!["-3"]),
        (
            "overflow-declaration",
            VALID.to_vec(),
            vec!["184467440737095516160"],
        ),
        ("duplicate-equal", VALID.to_vec(), vec!["3", "3"]),
        ("duplicate-first-valid", VALID.to_vec(), vec!["3", "4"]),
        ("duplicate-last-valid", VALID.to_vec(), vec!["4", "3"]),
        ("long-line", long_line.as_bytes().to_vec(), vec!["3"]),
    ];
    for (case_name, wire, lengths) in cases {
        assert_eq!(
            client
                .put(&object)
                .bearer_auth(&token)
                .body("old")
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("content-encoding", "aws-chunked".parse().unwrap());
        for length in lengths {
            headers.append("x-amz-decoded-content-length", length.parse().unwrap());
        }
        let rejected = client
            .put(&object)
            .bearer_auth(&token)
            .headers(headers)
            .body(wire)
            .send()
            .await
            .unwrap();
        let expected_status = if matches!(
            case_name,
            "missing-final-crlf"
                | "trailing-garbage"
                | "missing-zero"
                | "truncated-data"
                | "short-declaration"
                | "long-declaration"
                | "long-line"
        ) {
            500 // Existing ServerError::Io mapping for decoder failures.
        } else {
            400 // Header validation errors are InvalidArgument.
        };
        assert_eq!(
            rejected.status(),
            expected_status,
            "{case_name}: {}",
            rejected.text().await.unwrap()
        );
        assert_eq!(
            client
                .get(&object)
                .bearer_auth(&token)
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "old",
            "{case_name}"
        );
        let retry = client
            .put(&object)
            .bearer_auth(&token)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", "3")
            .body(VALID)
            .send()
            .await
            .unwrap();
        assert_eq!(retry.status(), 200, "retry after {case_name}");
        assert_eq!(
            client
                .get(&object)
                .bearer_auth(&token)
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "new"
        );
    }
    // A size line exceeding the bound is also rejected across HTTP body frames.
    assert_eq!(
        client
            .put(&object)
            .bearer_auth(&token)
            .body("old")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let pieces = long_line
        .as_bytes()
        .chunks(1024)
        .map(|piece| Ok::<_, std::io::Error>(piece.to_vec()))
        .collect::<Vec<_>>();
    let split_response = client
        .put(&object)
        .bearer_auth(&token)
        .header("content-encoding", "aws-chunked")
        .header("x-amz-decoded-content-length", "3")
        .body(reqwest::Body::wrap_stream(futures_util::stream::iter(
            pieces,
        )))
        .send()
        .await
        .unwrap();
    assert_eq!(split_response.status(), 500);
    assert_eq!(
        client
            .get(&object)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        "old"
    );
    // Both detection paths and checksum/signature trailer framing are accepted.
    for (control_name, wire, marker) in [
        ("unsigned", VALID, "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
        ("signed", b"3;chunk-signature=aa\r\nnew\r\n0;chunk-signature=bb\r\n\r\n".as_slice(), "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
        ("trailer", b"3\r\nnew\r\n0\r\nx-amz-checksum-crc32: fake\n\r\nx-amz-trailer-signature: fake\r\n\r\n".as_slice(), "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
    ] {
        let mut control = client.put(&object).bearer_auth(&token)
            .header("x-amz-content-sha256", marker).header("x-amz-decoded-content-length", "3");
        if control_name != "signed" { control = control.header("content-encoding", "aws-chunked"); }
        let accepted = control.body(wire).send().await.unwrap();
        assert_eq!(accepted.status(), 200, "{control_name}: {}", accepted.text().await.unwrap());
        assert_eq!(client.get(&object).bearer_auth(&token).send().await.unwrap().bytes().await.unwrap(), "new");
    }
    // The decoder must consume HTTP EOF rather than publish at its own zero chunk.
    assert_eq!(
        client
            .put(&object)
            .bearer_auth(&token)
            .body("old")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let mut tcp = tokio::net::TcpStream::connect(address).await.unwrap();
    let raw_headers = format!(
        "PUT /audit.{name}/object HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\nContent-Encoding: aws-chunked\r\nX-Amz-Decoded-Content-Length: 3\r\nContent-Length: 100\r\nConnection: close\r\n\r\n"
    );
    tcp.write_all(raw_headers.as_bytes()).await.unwrap();
    tcp.write_all(VALID).await.unwrap();
    tcp.shutdown().await.unwrap();
    let mut raw_response = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), tcp.read_to_end(&mut raw_response))
        .await
        .unwrap()
        .unwrap();
    assert!(!String::from_utf8_lossy(&raw_response).starts_with("HTTP/1.1 200"));
    assert_eq!(
        client
            .get(&object)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        "old"
    );
    // UploadPart failures must retain the previously acknowledged part.
    let multipart = format!("{base}/multipart");
    let initiation = client
        .post(format!("{multipart}?uploads"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(initiation.status(), 200);
    let init_xml = initiation.text().await.unwrap();
    let id = init_xml
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let part = format!("{multipart}?uploadId={id}&partNumber=1");
    let initial_part = client
        .put(&part)
        .bearer_auth(&token)
        .body("old")
        .send()
        .await
        .unwrap();
    assert_eq!(initial_part.status(), 200);
    let old_etag = initial_part
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let rejected_part = client
        .put(&part)
        .bearer_auth(&token)
        .header("content-encoding", "aws-chunked")
        .header("x-amz-decoded-content-length", "2")
        .body(VALID)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected_part.status(), 500);
    let old_completion = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{old_etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let completed_old = client
        .post(format!("{multipart}?uploadId={id}"))
        .bearer_auth(&token)
        .body(old_completion)
        .send()
        .await
        .unwrap();
    assert_eq!(
        completed_old.status(),
        200,
        "prior part must remain completable: {}",
        completed_old.text().await.unwrap()
    );
    assert_eq!(
        client
            .get(&multipart)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        "old"
    );

    // A fresh session proves a failed replacement can then be retried and
    // completed with the new part, without depending on unsupported ListParts.
    let retry_initiation = client
        .post(format!("{multipart}?uploads"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(retry_initiation.status(), 200);
    let retry_xml = retry_initiation.text().await.unwrap();
    let retry_id = retry_xml
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let retry_part_url = format!("{multipart}?uploadId={retry_id}&partNumber=1");
    assert_eq!(
        client
            .put(&retry_part_url)
            .bearer_auth(&token)
            .body("old")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let retry_rejected = client
        .put(&retry_part_url)
        .bearer_auth(&token)
        .header("content-encoding", "aws-chunked")
        .header("x-amz-decoded-content-length", "2")
        .body(VALID)
        .send()
        .await
        .unwrap();
    assert_eq!(retry_rejected.status(), 500);
    let retried_part = client
        .put(&retry_part_url)
        .bearer_auth(&token)
        .header("content-encoding", "aws-chunked")
        .header("x-amz-decoded-content-length", "3")
        .body(VALID)
        .send()
        .await
        .unwrap();
    assert_eq!(retried_part.status(), 200);
    let new_etag = retried_part
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap();
    assert_ne!(new_etag, old_etag);
    let new_completion = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{new_etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let completed_new = client
        .post(format!("{multipart}?uploadId={retry_id}"))
        .bearer_auth(&token)
        .body(new_completion)
        .send()
        .await
        .unwrap();
    assert_eq!(
        completed_new.status(),
        200,
        "valid retry must complete: {}",
        completed_new.text().await.unwrap()
    );
    assert_eq!(
        client
            .get(&multipart)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        "new"
    );
    let _shutdown_result = shutdown_tx.send(());
    server.await.unwrap();
}
#[tokio::test]
async fn sqlite_aws_chunked_requires_complete_framing_and_declared_length() {
    run_ingest(None).await;
}
#[tokio::test]
async fn postgres_aws_chunked_requires_complete_framing_and_declared_length() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping PostgreSQL: DATABASE_URL unset");
        return;
    };
    run_ingest(Some(&url)).await;
}
