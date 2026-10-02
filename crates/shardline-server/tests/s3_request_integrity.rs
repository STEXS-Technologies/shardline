//! Actual loopback HTTP regressions for caller-supplied S3 request integrity.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use
)]
use std::num::NonZeroUsize;

use base64::Engine;
use md5::{Digest, Md5};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{ServerConfig, ServerFrontend, ServerRole, app};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn checksum(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(Md5::digest(bytes))
}

fn upload_id(xml: &str) -> &str {
    xml.split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0
}

fn completion(etag: &str) -> String {
    format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    )
}

async fn assert_request_integrity(database_url: Option<&str>) {
    let signing_key = b"0123456789abcdef0123456789abcdef";
    let root = tempfile::TempDir::new().unwrap();
    let mut config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        root.path().to_path_buf(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends(vec![ServerFrontend::S3])
    .unwrap()
    .with_token_signing_key(signing_key.to_vec())
    .unwrap()
    .with_reconstruction_cache_disabled();
    if let Some(url) = database_url {
        let pool = sqlx::PgPool::connect(url).await.unwrap();
        shardline_server::apply_database_migrations(&pool)
            .await
            .unwrap();
        pool.close().await;
        config = config.with_index_postgres_url(url.to_owned()).unwrap();
    }
    let router = app::router(config).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let base = format!("http://{address}");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    let provider = LocalHmacProvider::new(signing_key).unwrap();
    let repository =
        RepositoryScope::new(RepositoryProvider::Generic, "md5-audit", "isolated", None).unwrap();
    let claims = TokenClaims::new(
        "shardline",
        "md5-audit",
        TokenScope::Write,
        repository,
        u64::MAX,
    )
    .unwrap();
    let token = provider.mint_token(&claims).unwrap();
    let client = reqwest::Client::new();
    let bucket = format!("{base}/md5-audit.isolated");
    let object = format!("{bucket}/object");
    let wrong = "AAAAAAAAAAAAAAAAAAAAAA==";
    let response = client
        .put(&object)
        .bearer_auth(&token)
        .header("content-md5", checksum(b"original"))
        .body("original")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    for (md5, code) in [(wrong, "BadDigest"), ("malformed", "InvalidDigest")] {
        let response = client
            .put(&object)
            .bearer_auth(&token)
            .header("content-md5", md5)
            .body("must-not-land")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        assert!(response.text().await.unwrap().contains(code));
        let bytes = client
            .get(&object)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&bytes[..], b"original");
    }

    let framed = "8;chunk-signature=deadbeef\r\noriginal\r\n0;chunk-signature=deadbeef\r\n\r\n";
    for (digest, status) in [(checksum(b"original"), 200), (wrong.to_owned(), 400)] {
        let response = client
            .put(&object)
            .bearer_auth(&token)
            .header("content-md5", digest)
            .header("content-encoding", "aws-chunked")
            .header("x-amz-decoded-content-length", "8")
            .body(framed)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            status,
            "Content-MD5 must hash decoded bytes"
        );
        let bytes = client
            .get(&object)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&bytes[..], b"original");
    }

    let multipart = format!("{bucket}/multipart");
    let response = client
        .post(format!("{multipart}?uploads"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let xml = response.text().await.unwrap();
    let id = upload_id(&xml);
    let part_url = format!("{multipart}?uploadId={id}&partNumber=1");
    let response = client
        .put(&part_url)
        .bearer_auth(&token)
        .header("content-md5", checksum(b"original-part"))
        .body("original-part")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let original_etag = response
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let framed =
        "d;chunk-signature=deadbeef\r\noriginal-part\r\n0;chunk-signature=deadbeef\r\n\r\n";
    let response = client
        .put(&part_url)
        .bearer_auth(&token)
        .header("content-md5", checksum(b"original-part"))
        .header("content-encoding", "aws-chunked")
        .header("x-amz-decoded-content-length", "13")
        .body(framed)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers().get("etag").unwrap().to_str().unwrap(),
        original_etag
    );
    let response = client
        .put(&part_url)
        .bearer_auth(&token)
        .header("content-md5", wrong)
        .body("bad-part")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    assert!(response.text().await.unwrap().contains("BadDigest"));

    // Half-close a genuine HTTP connection before the declared body finishes.
    // The acknowledged original part must remain usable after this failure.
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let request = format!(
        "PUT /md5-audit.isolated/multipart?uploadId={id}&partNumber=1 HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\nContent-Length: 100\r\n\r\npartial"
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.shutdown().await.unwrap();
    let mut response_bytes = Vec::new();
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        stream.read_to_end(&mut response_bytes),
    )
    .await;

    let xml = completion(&original_etag);
    let response = client
        .post(format!("{multipart}?uploadId={id}"))
        .bearer_auth(&token)
        .header("content-md5", wrong)
        .body(xml.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    assert!(response.text().await.unwrap().contains("BadDigest"));
    let response = client
        .post(format!("{multipart}?uploadId={id}"))
        .bearer_auth(&token)
        .header("content-md5", checksum(xml.as_bytes()))
        .body(xml)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "failed part/XML checksum must not consume the session"
    );
    assert_eq!(
        &client
            .get(&multipart)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()[..],
        b"original-part"
    );

    // Changed part bytes receive a changed ETag; stale completion stays retryable.
    let response = client
        .post(format!("{multipart}?uploads"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    let xml = response.text().await.unwrap();
    let id = upload_id(&xml);
    let part_url = format!("{multipart}?uploadId={id}&partNumber=1");
    let response = client
        .put(&part_url)
        .bearer_auth(&token)
        .body("version-A")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let etag_a = response
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let response = client
        .put(&part_url)
        .bearer_auth(&token)
        .body("version-B")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let etag_b = response
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert_ne!(etag_a, etag_b);
    let response = client
        .post(format!("{multipart}?uploadId={id}"))
        .bearer_auth(&token)
        .body(completion(&etag_a))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    assert!(response.text().await.unwrap().contains("InvalidPart"));
    let response = client
        .post(format!("{multipart}?uploadId={id}"))
        .bearer_auth(&token)
        .body(completion(&etag_b))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        &client
            .get(&multipart)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()[..],
        b"version-B"
    );

    let delete_xml = "<Delete><Object><Key>object</Key></Object></Delete>";
    let response = client
        .post(format!("{bucket}?delete"))
        .bearer_auth(&token)
        .header("content-md5", wrong)
        .body(delete_xml)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    assert!(response.text().await.unwrap().contains("BadDigest"));
    let bytes = client
        .get(&object)
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&bytes[..], b"original");
    let response = client
        .post(format!("{bucket}?delete"))
        .bearer_auth(&token)
        .header("content-md5", checksum(delete_xml.as_bytes()))
        .body(delete_xml)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        client
            .get(&object)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_request_checksums_and_interrupted_part_overwrites_preserve_acknowledged_state() {
    assert_request_integrity(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_postgres_request_checksums_preserve_acknowledged_state() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return;
    };
    assert_request_integrity(Some(&url)).await;
}
