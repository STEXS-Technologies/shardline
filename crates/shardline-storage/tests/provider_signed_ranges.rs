//! Provider presigning evaluation: a shared object must never get a whole-object grant.
#![cfg(feature = "s3")]
#![allow(clippy::unwrap_used)]
use object_store::{
    ObjectStoreExt,
    aws::AmazonS3Builder,
    path::Path,
    signer::{Method, SignedUrlOptions, Signer, Url},
};
use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

fn fetch(url: &Url, range: Option<&str>) -> (u16, Vec<u8>) {
    let host = url.host_str().unwrap();
    let port = url.port_or_known_default().unwrap();
    let mut socket = TcpStream::connect((host, port)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let target = format!("{}?{}", url.path(), url.query().unwrap());
    write!(
        socket,
        "GET {target} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n"
    )
    .unwrap();
    if let Some(range) = range {
        write!(socket, "Range: {range}\r\n").unwrap();
    }
    socket.write_all(b"\r\n").unwrap();
    let mut response = Vec::new();
    socket.take(65536).read_to_end(&mut response).unwrap();
    let header_end = response
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .unwrap();
    let headers = std::str::from_utf8(response.get(..header_end).unwrap()).unwrap();
    let status = headers.split_whitespace().nth(1).unwrap().parse().unwrap();
    (
        status,
        response
            .get(header_end.checked_add(4).unwrap()..)
            .unwrap()
            .to_vec(),
    )
}

#[tokio::test]
#[ignore = "requires owned Docker/MinIO; does not qualify public cloud providers or Xet URL routing"]
async fn provider_presigned_get_enforces_exact_signed_range() {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_minio()
        .start()
        .unwrap()
        .unwrap();
    let raw = services.s3_raw_config(None).unwrap();
    let store = AmazonS3Builder::new()
        .with_bucket_name(raw.bucket)
        .with_region(raw.region)
        .with_endpoint(raw.endpoint.unwrap())
        .with_allow_http(true)
        .with_access_key_id(raw.access_key.unwrap())
        .with_secret_access_key(raw.secret_key.unwrap())
        .build()
        .unwrap();
    let key = Path::from("shared-container");
    store
        .put(
            &key,
            bytes::Bytes::from_static(b"AAAAprivate-other-tenant").into(),
        )
        .await
        .unwrap();
    let options = SignedUrlOptions::default().with_signed_header(
        http::header::RANGE,
        http::HeaderValue::from_static("bytes=0-3"),
    );
    let signed = store
        .signed_url_opts(Method::GET, &key, Duration::from_secs(30), &options)
        .await
        .unwrap();
    // No bearer credentials: the signature alone must constrain disclosure.
    assert_eq!(fetch(&signed, Some("bytes=0-3")), (206, b"AAAA".to_vec()));
    for range in [
        None,
        Some("bytes=0-22"),
        Some("bytes=4-22"),
        Some("bytes=-4"),
    ] {
        assert_eq!(
            fetch(&signed, range).0,
            if range.is_none() { 400 } else { 403 },
            "changed/omitted range escaped its grant"
        );
    }
    let mut changed = signed.clone();
    changed.set_path(&format!("{}/different", signed.path()));
    assert_eq!(fetch(&changed, Some("bytes=0-3")).0, 403);
    // Positive control: ordinary presigning grants the whole shared object and
    // is therefore unsuitable as a substitute for a scoped Xet transfer URL.
    let unscoped = store
        .signed_url(Method::GET, &key, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(
        fetch(&unscoped, None),
        (200, b"AAAAprivate-other-tenant".to_vec())
    );
    let expired = store
        .signed_url_opts(Method::GET, &key, Duration::from_secs(1), &options)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(fetch(&expired, Some("bytes=0-3")).0, 403);
    let refreshed = store
        .signed_url_opts(Method::GET, &key, Duration::from_secs(30), &options)
        .await
        .unwrap();
    assert_eq!(
        fetch(&refreshed, Some("bytes=0-3")),
        (206, b"AAAA".to_vec())
    );
}
