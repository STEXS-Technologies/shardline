use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha256;

#[tokio::test]
#[ignore = "requires BOTOCORE_PYTHON with boto3"]
async fn native_boto3_presigned_post_uploads_and_reads_object() {
    let root = tempfile::tempdir().unwrap();
    let credential = token("writer", "aws-resume", TokenScope::Write);
    let server = start_configured(
        root.path(),
        ServerFrontend::S3,
        true,
        None,
        None,
        Some(signing_identity(credential)),
    )
    .await;
    let endpoint = server.base.clone();
    let python = std::env::var("BOTOCORE_PYTHON").expect("BOTOCORE_PYTHON must be configured");
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(python)
            .arg("-c")
            .arg(
                r#"
import base64, hashlib, sys, urllib.request
import boto3
from botocore.config import Config

endpoint = sys.argv[1]
bucket = 'aws-resume.locks'
payload = b'native boto3 browser form upload'
checksum = base64.b64encode(hashlib.sha256(payload).digest()).decode()
client = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
    aws_access_key_id='SIGNINGKEY', aws_secret_access_key='legacy-token-bridge',
    config=Config(signature_version='s3v4'))
post = client.generate_presigned_post(
    Bucket=bucket,
    Key='uploads/${filename}',
    Fields={'success_action_status': '201', 'Content-Type': 'application/x-shardline-model',
        'x-amz-meta-model': 'release-1',
        'x-amz-checksum-sha256': checksum, 'x-ignore-client-note': 'ignored'},
    Conditions=[
        {'bucket': bucket}, ['starts-with', '$key', 'uploads/'],
        {'success_action_status': '201'}, {'Content-Type': 'application/x-shardline-model'},
        {'x-amz-meta-model': 'release-1'},
        {'x-amz-checksum-sha256': checksum},
        ['content-length-range', 1, 1024],
    ],
    ExpiresIn=300,
)
boundary = 'shardline-boto3-post-boundary'
body = bytearray()
for name, value in post['fields'].items():
    body.extend(f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode())
body.extend(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="weights.bin"\r\nContent-Type: application/octet-stream\r\n\r\n'.encode())
body.extend(payload)
body.extend(f'\r\n--{boundary}--\r\n'.encode())
request = urllib.request.Request(post['url'], data=bytes(body), method='POST',
    headers={'Content-Type': f'multipart/form-data; boundary={boundary}'})
with urllib.request.urlopen(request) as response:
    assert response.status == 201, (response.status, response.read())
    assert b'<Key>uploads/weights.bin</Key>' in response.read()
assert client.get_object(Bucket=bucket, Key='uploads/weights.bin')['Body'].read() == payload
assert client.head_object(Bucket=bucket, Key='uploads/weights.bin')['ContentType'] == 'application/x-shardline-model'
"#,
            )
            .arg(&endpoint)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "native boto3 PostObject failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn signed_browser_post_object_expands_filename_and_checks_policy_before_publish() {
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
    let url = format!("{}/aws-resume.locks", server.base);
    let file = b"browser form upload bytes";
    let response = post_object_form(&client, &url, file, false, false, false, false).await;
    let status = response.status();
    let xml = response.text().await.unwrap();
    assert_eq!(status, StatusCode::CREATED, "{xml}");
    assert!(xml.contains("<Key>uploads/weights model.bin</Key>"));
    assert!(xml.contains(&format!(
        "<Location>{}/aws-resume.locks/uploads/weights%20model.bin</Location>",
        server.base
    )));

    let object_url = format!("{url}/uploads/weights%20model.bin");
    let response = signed_request(&client, reqwest::Method::GET, &object_url, b"")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/x-shardline-model"
    );
    assert_eq!(response.bytes().await.unwrap().as_ref(), file);

    let response = client
        .get(&object_url)
        .bearer_auth(&credential)
        .header("x-amz-checksum-mode", "ENABLED")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-amz-checksum-sha256")
            .unwrap()
            .to_str()
            .unwrap(),
        STANDARD.encode(Sha256::digest(file))
    );
    assert_eq!(
        response
            .headers()
            .get("x-amz-meta-model")
            .unwrap()
            .to_str()
            .unwrap(),
        "release-1"
    );
    assert_eq!(response.bytes().await.unwrap().as_ref(), file);

    // A validly shaped form with a damaged policy signature must not replace
    // the already-published object.
    let response = post_object_form(
        &client,
        &url,
        b"tampered request",
        true,
        false,
        false,
        false,
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = post_object_form(&client, &url, b"wrong key", false, true, false, false).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response =
        post_object_form(&client, &url, b"bad checksum", false, false, true, false).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = post_object_form(&client, &url, b"too large", false, false, false, true).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = signed_request(&client, reqwest::Method::GET, &object_url, b"")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), file);
}

async fn post_object_form(
    client: &Client,
    url: &str,
    file: &[u8],
    corrupt_signature: bool,
    invalid_key: bool,
    invalid_checksum: bool,
    too_large_for_policy: bool,
) -> reqwest::Response {
    let now = chrono::Utc::now();
    let date = now.format("%Y%m%d").to_string();
    let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
    let credential = format!("SIGNINGKEY/{date}/us-east-1/s3/aws4_request");
    let checksum = if invalid_checksum {
        STANDARD.encode(Sha256::digest(b"not the request body"))
    } else {
        STANDARD.encode(Sha256::digest(file))
    };
    let content_md5 = STANDARD.encode(md5::Md5::digest(file));
    let policy_max = if too_large_for_policy { 4 } else { 1024 };
    let policy = STANDARD.encode(
        serde_json::to_vec(&json!({
            "expiration": now
                .checked_add_signed(chrono::Duration::minutes(5))
                .unwrap()
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            "conditions": [
                { "bucket": "aws-resume.locks" },
                ["starts-with", "$key", "uploads/"],
                { "x-amz-algorithm": "AWS4-HMAC-SHA256" },
                { "x-amz-credential": credential },
                { "x-amz-date": timestamp },
                { "Content-Type": "application/x-shardline-model" },
                { "x-amz-meta-model": "release-1" },
                { "x-amz-checksum-sha256": checksum },
                { "Content-MD5": content_md5 },
                { "success_action_status": "201" },
                ["content-length-range", 1, policy_max]
            ]
        }))
        .unwrap(),
    );
    let mut signature = sign_post_policy(&policy, &date);
    if corrupt_signature {
        signature.replace_range(0..1, if signature.starts_with('0') { "1" } else { "0" });
    }
    let key = if invalid_key {
        "other/${filename}"
    } else {
        "uploads/${filename}"
    };
    let fields = [
        ("key", key.to_owned()),
        ("Content-Type", "application/x-shardline-model".to_owned()),
        ("x-amz-meta-model", "release-1".to_owned()),
        ("x-amz-checksum-sha256", checksum),
        ("Content-MD5", content_md5),
        ("success_action_status", "201".to_owned()),
        ("x-amz-algorithm", "AWS4-HMAC-SHA256".to_owned()),
        ("x-amz-credential", credential),
        ("x-amz-date", timestamp),
        ("policy", policy),
        ("x-amz-signature", signature),
    ];
    let boundary = "shardline-post-object-test-boundary";
    let mut body = Vec::new();
    for (name, value) in fields {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .as_bytes(),
        );
    }
    body.extend_from_slice(
        format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"folder/weights model.bin\"\r\nContent-Type: application/octet-stream\r\n\r\n").as_bytes(),
    );
    body.extend_from_slice(file);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    client
        .post(url)
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .unwrap()
}

fn sign_post_policy(policy: &str, date: &str) -> String {
    let mut key = b"AWS4legacy-token-bridge".to_vec();
    for part in [date.as_bytes(), b"us-east-1", b"s3", b"aws4_request"] {
        let mut mac = Hmac::<Sha256>::new_from_slice(&key).unwrap();
        mac.update(part);
        key = mac.finalize().into_bytes().to_vec();
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).unwrap();
    mac.update(policy.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}
