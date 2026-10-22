//! End-to-end signed-chunk publication checks with an independent test signer.
use super::*;
use hmac::{Hmac, Mac};

mod native_minio;

fn mac(key: &[u8], bytes: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(bytes);
    mac.finalize().into_bytes().to_vec()
}

fn request(client: &Client, url: &str, payload: &[u8]) -> (reqwest::RequestBuilder, Vec<u8>) {
    request_with_trailer(client, url, payload, None)
}

fn request_with_trailer(
    client: &Client,
    url: &str,
    payload: &[u8],
    checksum: Option<&str>,
) -> (reqwest::RequestBuilder, Vec<u8>) {
    let url = reqwest::Url::parse(url).unwrap();
    let now = chrono::Utc::now();
    let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let host = format!("{}:{}", url.host_str().unwrap(), url.port().unwrap());
    let marker = if checksum.is_some() {
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
    } else {
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
    };
    let signed = if checksum.is_some() {
        "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer"
    } else {
        "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length"
    };
    let trailer_header = if checksum.is_some() {
        "x-amz-trailer:x-amz-checksum-sha256\n"
    } else {
        ""
    };
    let canonical = format!(
        "PUT\n{}\n\ncontent-encoding:aws-chunked\nhost:{host}\nx-amz-content-sha256:{marker}\nx-amz-date:{timestamp}\nx-amz-decoded-content-length:{}\n{trailer_header}\n{signed}\n{marker}",
        url.path(),
        payload.len()
    );
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let mut key = b"AWS4legacy-token-bridge".to_vec();
    for value in [&date, "us-east-1", "s3", "aws4_request"] {
        key = mac(&key, value.as_bytes());
    }
    let seed = hex::encode(mac(&key, to_sign.as_bytes()));
    let mut previous = seed.clone();
    let mut wire = Vec::new();
    for chunk in payload.chunks(4096).chain(std::iter::once(&[][..])) {
        let to_sign = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{scope}\n{previous}\n{}\n{}",
            hex::encode(Sha256::digest(b"")),
            hex::encode(Sha256::digest(chunk))
        );
        previous = hex::encode(mac(&key, to_sign.as_bytes()));
        wire.extend_from_slice(
            format!("{:x};chunk-signature={previous}\r\n", chunk.len()).as_bytes(),
        );
        wire.extend_from_slice(chunk);
        wire.extend_from_slice(b"\r\n");
    }
    if let Some(checksum) = checksum {
        let canonical = format!("x-amz-checksum-sha256:{checksum}\n");
        let to_sign = format!(
            "AWS4-HMAC-SHA256-TRAILER\n{timestamp}\n{scope}\n{previous}\n{}",
            hex::encode(Sha256::digest(canonical.as_bytes()))
        );
        let signature = hex::encode(mac(&key, to_sign.as_bytes()));
        wire.extend_from_slice(
            format!(
                "x-amz-checksum-sha256:{checksum}\r\nx-amz-trailer-signature:{signature}\r\n\r\n"
            )
            .as_bytes(),
        );
    }
    let mut request = client.put(url)
        .header("content-encoding", "aws-chunked")
        .header("x-amz-date", timestamp)
        .header("x-amz-content-sha256", marker)
        .header("x-amz-decoded-content-length", payload.len())
        .header("authorization", format!("AWS4-HMAC-SHA256 Credential=SIGNINGKEY/{scope},SignedHeaders={signed},Signature={seed}"));
    if checksum.is_some() {
        request = request.header("x-amz-trailer", "x-amz-checksum-sha256");
    }
    (request, wire)
}

#[tokio::test]
async fn signed_stream_failure_preserves_published_object() {
    let root = tempfile::tempdir().unwrap();
    let credential = token("writer", "aws-resume", TokenScope::Write);
    let server = start_configured(
        root.path(),
        ServerFrontend::S3,
        true,
        None,
        None,
        Some(signing_identity(credential.clone())),
    )
    .await;
    let client = Client::new();
    let url = format!("{}/aws-resume.locks/stream.bin", server.base);
    let payload = vec![b'a'; 10000];
    let (builder, wire) = request(&client, &url, &payload);
    let response = builder.body(wire).send().await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    for failure in 0..4 {
        let (builder, mut wire) = request(&client, &url, b"replacement bytes");
        match failure {
            0 => {
                let offset = wire.windows(2).position(|bytes| bytes == b"\r\n").unwrap() + 2;
                wire[offset] ^= 1;
            }
            1 => {
                wire.truncate(wire.len() - 2);
            }
            2 => {
                let end = wire
                    .windows(4)
                    .position(|bytes| bytes == b"\r\n0;")
                    .unwrap()
                    + 2;
                wire.truncate(end);
            }
            _ => wire.push(b'x'),
        }
        let response = builder.body(wire).send().await.unwrap();
        assert!(
            !response.status().is_success(),
            "invalid stream {failure} published"
        );
        let response = client
            .get(&url)
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap().as_ref(), payload);
    }
    server.stop().await;
}

#[tokio::test]
async fn signed_trailer_failures_preserve_published_object() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let root = tempfile::tempdir().unwrap();
    let credential = token("writer", "aws-resume", TokenScope::Write);
    let server = start_configured(
        root.path(),
        ServerFrontend::S3,
        true,
        None,
        None,
        Some(signing_identity(credential.clone())),
    )
    .await;
    let client = Client::new();
    let url = format!("{}/aws-resume.locks/trailer.bin", server.base);
    let payload = vec![b'a'; 10000];
    let checksum = STANDARD.encode(Sha256::digest(&payload));
    let (builder, wire) = request_with_trailer(&client, &url, &payload, Some(&checksum));
    let response = builder.body(wire).send().await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    for failure in 0..4 {
        let replacement = b"replacement content";
        let checksum = if failure == 0 {
            STANDARD.encode([0u8; 32])
        } else {
            STANDARD.encode(Sha256::digest(replacement))
        };
        let (builder, mut wire) = request_with_trailer(&client, &url, replacement, Some(&checksum));
        match failure {
            0 => {} // Correct MAC over an incorrect checksum must still fail.
            1 => {
                wire.truncate(wire.len() - 2);
            }
            2 => {
                let offset = wire.len() - 5;
                wire[offset] = if wire[offset] == b'0' { b'1' } else { b'0' };
            }
            _ => wire.push(b'x'),
        }
        assert!(
            !builder
                .body(wire)
                .send()
                .await
                .unwrap()
                .status()
                .is_success(),
            "invalid trailer {failure} published"
        );
        let read = client
            .get(&url)
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(read.status(), StatusCode::OK);
        assert_eq!(read.bytes().await.unwrap().as_ref(), payload);
    }
    server.stop().await;
}
