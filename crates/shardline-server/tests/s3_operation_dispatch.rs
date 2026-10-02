//! Actual loopback regressions for unsupported and ambiguous S3 dispatch.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::shadow_unrelated
)]
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{ServerConfig, ServerFrontend, ServerRole, app};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use std::num::NonZeroUsize;

#[tokio::test]
async fn unsupported_and_ambiguous_operations_preserve_data() {
    let key = b"0123456789abcdef0123456789abcdef";
    let tmp = tempfile::TempDir::new().unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends(vec![ServerFrontend::S3])
    .unwrap()
    .with_token_signing_key(key.to_vec())
    .unwrap()
    .with_reconstruction_cache_disabled();
    let router = app::router(config).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _shutdown = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    let provider = LocalHmacProvider::new(key).unwrap();
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
    let source = format!("{bucket}/source");
    assert_eq!(
        client
            .put(&source)
            .bearer_auth(&token)
            .body("copy-source-bytes")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let multipart = format!("{bucket}/copied-part");
    let created = client
        .post(format!("{multipart}?uploads"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    let xml = created.text().await.unwrap();
    let id = xml
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let part = format!("{multipart}?uploadId={id}&partNumber=1");
    let original = client
        .put(&part)
        .bearer_auth(&token)
        .body("acknowledged-original-part")
        .send()
        .await
        .unwrap();
    assert_eq!(original.status(), 200);
    let original_etag = original
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let copy = client
        .put(&part)
        .bearer_auth(&token)
        .header("x-amz-copy-source", "/md5-audit.isolated/source")
        .send()
        .await
        .unwrap();
    let status = copy.status();
    let etag = original_etag.clone();
    let response_xml = copy.text().await.unwrap();
    println!(
        "UploadPartCopy existing nonempty source + empty request body: status={status}, original_etag={original_etag}, acknowledged_etag={etag}, response={response_xml:?}"
    );
    assert_eq!(status, 501);
    assert!(response_xml.contains("NotImplemented"));
    let completion = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let complete = client
        .post(format!("{multipart}?uploadId={id}"))
        .bearer_auth(&token)
        .body(completion)
        .send()
        .await
        .unwrap();
    assert_eq!(complete.status(), 200);
    let stored = client
        .get(&multipart)
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    println!("Copied-part completion: status=200, final_bytes={stored:?}");
    assert_eq!(&stored[..], b"acknowledged-original-part");

    let mixed = format!("{bucket}/mixed-query");
    let created = client
        .post(format!("{mixed}?uploads&acl"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    let status = created.status();
    let xml = created.text().await.unwrap();
    println!(
        "POST ?uploads&acl: status={status}, created_session={}",
        xml.contains("<UploadId>")
    );
    assert_eq!(status, 501);
    assert!(!xml.contains("<UploadId>"));
    let created = client
        .post(format!("{mixed}?uploads&extension=accepted"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    let xml = created.text().await.unwrap();
    let id = xml
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let uploaded = client
        .put(format!("{mixed}?uploadId={id}&partNumber=1&tagging"))
        .bearer_auth(&token)
        .body("unexpectedly-stored")
        .send()
        .await
        .unwrap();
    let status = uploaded.status();
    println!(
        "PUT ?uploadId&partNumber&tagging: status={status}, etag={:?}",
        uploaded.headers().get("etag")
    );
    assert_eq!(status, 501);
    let aborted = client
        .delete(format!("{mixed}?uploadId={id}&tagging"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    let status = aborted.status();
    let after = client
        .put(format!("{mixed}?uploadId={id}&partNumber=2"))
        .bearer_auth(&token)
        .body("next-part")
        .send()
        .await
        .unwrap();
    println!(
        "DELETE ?uploadId&tagging: status={status}, subsequent valid UploadPart={}",
        after.status()
    );
    assert_eq!(status, 501);
    assert_eq!(after.status(), 200);
    let rejected = client
        .put(format!("{mixed}?uploadId={id}&partNumber=2&partNumber=3"))
        .bearer_auth(&token)
        .body("ignored")
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 501);
    let rejected = client
        .post(format!("{mixed}?uploadId={id}&uploads"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 501);
    let destination = format!("{bucket}/malformed-copy");
    assert_eq!(
        client
            .put(&destination)
            .bearer_auth(&token)
            .body("prior-destination")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let malformed = reqwest::header::HeaderValue::from_bytes(&[0x80]).unwrap();
    let response = client
        .put(&destination)
        .bearer_auth(&token)
        .header("x-amz-copy-source", malformed)
        .body("literal-request-body")
        .send()
        .await
        .unwrap();
    let status = response.status();
    let response_body = response.text().await.unwrap();
    let stored = client
        .get(&destination)
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    println!(
        "CopyObject non-ASCII source field: status={status}, response={response_body:?}, destination={stored:?}"
    );
    assert_eq!(status, 400);
    assert_eq!(&stored[..], b"prior-destination");
    let response = client
        .put(&destination)
        .bearer_auth(&token)
        .header("x-amz-copy-source", "/md5-audit.isolated/source")
        .header("x-amz-copy-source", "/md5-audit.isolated/does-not-exist")
        .send()
        .await
        .unwrap();
    let status = response.status();
    let response_body = response.text().await.unwrap();
    let stored = client
        .get(&destination)
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    println!(
        "CopyObject repeated conflicting source fields: status={status}, response={response_body:?}, destination={stored:?}"
    );
    assert_eq!(status, 400);
    assert_eq!(&stored[..], b"prior-destination");
    let valid = client
        .put(&destination)
        .bearer_auth(&token)
        .header("x-amz-copy-source", "/md5-audit.isolated/source")
        .send()
        .await
        .unwrap();
    assert_eq!(valid.status(), 200);
    let stored = client
        .get(&destination)
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&stored[..], b"copy-source-bytes");
    let _shutdown = shutdown_tx.send(());
    server.await.unwrap();
}
