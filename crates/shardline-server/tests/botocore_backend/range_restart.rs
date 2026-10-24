//! Stock signed-client reads after recreating API and metadata/storage clients.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires pinned botocore and disposable Docker PostgreSQL/MinIO"]
async fn native_small_ranges_with_owned_postgres_and_minio_survive_server_restart() {
    let (services, database, storage) = fixture("botocore-range-restart").await;
    let endpoint = services.s3_raw_config(None).unwrap().endpoint.unwrap();
    let proxy = shardline_test_support::S3FaultProxy::start_disarmed(
        &endpoint,
        shardline_test_support::S3ProxyFault::ServiceUnavailableOnce,
    )
    .unwrap();
    let storage = storage.with_endpoint(Some(proxy.endpoint().to_owned()));
    let root = tempfile::tempdir().unwrap();
    let mut seed = 748973_u64;
    let payload: Vec<u8> = (0..4 * 1024 * 1024)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        })
        .collect();
    let source = root.path().join("source.bin");
    std::fs::write(&source, &payload).unwrap();
    let credential = token("writer", "range-restart", TokenScope::Write);
    for upload in [true, false] {
        // A fresh local root on restart proves metadata and object references
        // come from PostgreSQL/S3, rather than an upload-local cache.
        let server_root = tempfile::tempdir().unwrap();
        let server = start_configured(
            server_root.path(),
            ServerFrontend::S3,
            true,
            Some(&database),
            Some(storage.clone()),
            Some(signing_identity(credential.clone())),
        )
        .await;
        let python = std::env::var("BOTOCORE_PYTHON").expect("pinned native SDK required");
        let base = server.base.clone();
        let source = source.clone();
        if !upload {
            // Separate first-request startup/recovery traffic from steady-state
            // data-path accounting, while checking the initial restart read too.
            let warm = Client::new()
                .get(format!("{base}/range-restart.locks/ranges.bin"))
                .bearer_auth(&credential)
                .send()
                .await
                .unwrap();
            assert_eq!(warm.status(), StatusCode::OK);
            assert_eq!(warm.bytes().await.unwrap().as_ref(), payload.as_slice());
        }
        let before = proxy.get_requests();
        let before_bytes = proxy.get_response_bytes();
        let before_lengths = proxy.get_response_lengths().len();
        let result = tokio::task::spawn_blocking(move || {
            std::process::Command::new(python).arg("-c").arg(r#"
import hashlib, sys
import botocore.session
from botocore.config import Config
from botocore.exceptions import ClientError
c = botocore.session.get_session().create_client('s3', endpoint_url=sys.argv[1],
    region_name='us-east-1', aws_access_key_id='SIGNINGKEY',
    aws_secret_access_key='legacy-token-bridge', config=Config(signature_version='s3v4', retries={'max_attempts': 0}))
payload = open(sys.argv[2], 'rb').read()
args = {'Bucket': 'range-restart.locks', 'Key': 'ranges.bin'}
if sys.argv[3] == 'upload':
    c.put_object(**args, Body=payload, ChecksumAlgorithm='SHA256')
full = c.get_object(**args)['Body'].read()
assert hashlib.sha256(full).digest() == hashlib.sha256(payload).digest()
for start in (0, len(payload)//2, len(payload)-4096):
    end = start + 4095
    response = c.get_object(**args, Range=f'bytes={start}-{end}')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 206
    assert response['ContentRange'] == f'bytes {start}-{end}/{len(payload)}'
    assert response['ContentLength'] == 4096
    body = response['Body'].read()
    assert body == payload[start:end+1], f'range_mismatch start={start} length={len(body)}'
try:
    c.get_object(**args, Range=f'bytes={len(payload)}-')
    raise AssertionError('unsatisfiable range accepted')
except ClientError as error:
    assert error.response['ResponseMetadata']['HTTPStatusCode'] == 416
"#).arg(base).arg(source).arg(if upload { "upload" } else { "read" }).output().unwrap()
        }).await.unwrap();
        if !result.status.success()
            && let Some(path) = std::env::var_os("SHARDLINE_NATIVE_CLIENT_FAILURE_LOG")
        {
            std::fs::write(path, &result.stderr).unwrap();
        }
        assert!(
            result.status.success(),
            "stock SDK range/restart workflow failed (client output kept private)"
        );
        if !upload {
            let requests = proxy.get_requests() - before;
            let bytes = proxy.get_response_bytes() - before_bytes;
            eprintln!(
                "backend GET body lengths {:?}",
                &proxy.get_response_lengths()[before_lengths..]
            );
            assert_eq!(
                requests, 10,
                "one full GET and three authenticated three-GET ranges"
            );
            assert!(
                bytes < 5 * 1024 * 1024,
                "range reads must not fetch the full container again"
            );
        }
        server.stop().await;
    }
}
