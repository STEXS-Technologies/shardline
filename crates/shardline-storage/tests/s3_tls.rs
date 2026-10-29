#![cfg(feature = "s3")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{process::Command, time::Duration};

use shardline_protocol::{ByteRange, SecretString, ShardlineHash};
use shardline_storage::{
    ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, S3ObjectStore, S3ObjectStoreConfig,
};
use shardline_test_support::{DockerLocalStack, HttpsProxyFixture, native_client_output};

#[test]
fn s3_https_trusted_root_roundtrip() {
    let stack = DockerLocalStack::builder()
        .with_minio_tls()
        .start()
        .unwrap()
        .expect("Docker is required for S3 TLS qualification");
    let raw = stack.s3_raw_config(Some("tls-client")).unwrap();
    let unrelated = DockerLocalStack::builder()
        .with_minio_tls()
        .start()
        .unwrap()
        .expect("Docker is required for independent CA qualification");
    let mismatched = HttpsProxyFixture::start_with_mismatched_hostname().unwrap();
    for (ca, endpoint, reject) in [
        (
            stack.minio_tls_ca_cert_path().unwrap(),
            raw.endpoint.as_ref().unwrap().as_str(),
            "false",
        ),
        (
            unrelated.minio_tls_ca_cert_path().unwrap(),
            raw.endpoint.as_ref().unwrap().as_str(),
            "true",
        ),
        (mismatched.ca_cert_path(), mismatched.origin(), "hostname"),
    ] {
        let output = native_client_output(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "s3_https_child", "--ignored"])
                .env("SSL_CERT_FILE", ca)
                .env("SHARDLINE_TLS_TEST_REJECT", reject)
                .env("SHARDLINE_TLS_TEST_ENDPOINT", endpoint)
                .env("SHARDLINE_TLS_TEST_BUCKET", &raw.bucket)
                .env("SHARDLINE_TLS_TEST_REGION", &raw.region)
                .env(
                    "SHARDLINE_TLS_TEST_ACCESS",
                    raw.access_key.as_ref().unwrap(),
                )
                .env(
                    "SHARDLINE_TLS_TEST_SECRET",
                    raw.secret_key.as_ref().unwrap(),
                ),
            Duration::from_secs(90),
        )
        .unwrap();
        assert!(
            output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),
            "isolated HTTPS storage qualification failed (reject={reject})"
        );
    }
}

// A separate process loads trust before constructing any HTTP/TLS client.
#[test]
#[ignore = "invoked by the TLS parent with an isolated trust environment"]
fn s3_https_child() {
    let value = |name| std::env::var(name).unwrap();
    let config = S3ObjectStoreConfig::new(
        value("SHARDLINE_TLS_TEST_BUCKET"),
        value("SHARDLINE_TLS_TEST_REGION"),
    )
    .with_endpoint(Some(value("SHARDLINE_TLS_TEST_ENDPOINT")))
    .with_credentials(
        Some(SecretString::new(value("SHARDLINE_TLS_TEST_ACCESS"))),
        Some(SecretString::new(value("SHARDLINE_TLS_TEST_SECRET"))),
        None,
    )
    .with_allow_http(false);
    let store = S3ObjectStore::new(config).unwrap();
    let body = vec![73_u8; 131072];
    let key = ObjectKey::parse("tls/verified").unwrap();
    let rejection = value("SHARDLINE_TLS_TEST_REJECT");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    if rejection != "false" {
        let error = store
            .metadata(&key)
            .expect_err("invalid CA or hostname must be rejected");
        let diagnostic = format!("{error:?}").to_lowercase();
        assert!(
            diagnostic.contains("certificate") || diagnostic.contains("unknownissuer"),
            "failure must identify certificate validation"
        );
        if rejection == "hostname" {
            assert!(
                diagnostic.contains("notvalidforname")
                    || diagnostic.contains("not valid for name")
                    || diagnostic.contains("hostname"),
                "trusted CA with wrong hostname must fail name validation"
            );
        }
        let async_error = runtime
            .block_on(shardline_storage::AsyncObjectStore::metadata(&store, &key))
            .expect_err("async storage must reject invalid CA or hostname");
        let async_diagnostic = format!("{async_error:?}").to_lowercase();
        assert!(
            async_diagnostic.contains("certificate") || async_diagnostic.contains("unknownissuer")
        );
        if rejection == "hostname" {
            assert!(
                async_diagnostic.contains("notvalidforname")
                    || async_diagnostic.contains("not valid for name")
                    || async_diagnostic.contains("hostname")
            );
        }
        return;
    }
    let integrity = ObjectIntegrity::new(
        ShardlineHash::from_bytes(*blake3::hash(&body).as_bytes()),
        body.len() as u64,
    );
    assert!(
        store
            .put_if_absent(&key, ObjectBody::from_slice(&body), &integrity)
            .is_ok()
    );
    assert_eq!(
        store.metadata(&key).unwrap().unwrap().length(),
        body.len() as u64
    );
    assert_eq!(
        store
            .read_range(&key, ByteRange::new(0, body.len() as u64 - 1).unwrap())
            .unwrap(),
        body
    );
    assert_eq!(
        store
            .read_range(&key, ByteRange::new(100, 199).unwrap())
            .unwrap(),
        vec![73_u8; 100]
    );
    assert!(store.delete_if_present(&key).is_ok());
    assert!(!store.contains(&key).unwrap());
    runtime.block_on(async {
        use shardline_storage::AsyncObjectStore;

        let async_key = ObjectKey::parse("tls/async-verified").unwrap();
        AsyncObjectStore::put_if_absent(
            &store,
            &async_key,
            ObjectBody::from_slice(&body),
            &integrity,
        )
        .await
        .unwrap();
        assert_eq!(
            AsyncObjectStore::metadata(&store, &async_key)
                .await
                .unwrap()
                .unwrap()
                .length(),
            body.len() as u64
        );
        assert_eq!(
            AsyncObjectStore::read_range(
                &store,
                &async_key,
                ByteRange::new(0, body.len() as u64 - 1).unwrap(),
            )
            .await
            .unwrap(),
            body
        );
        assert_eq!(
            AsyncObjectStore::read_range(&store, &async_key, ByteRange::new(100, 199).unwrap())
                .await
                .unwrap(),
            vec![73_u8; 100]
        );
        AsyncObjectStore::delete_if_present(&store, &async_key)
            .await
            .unwrap();
        assert!(
            !AsyncObjectStore::contains(&store, &async_key)
                .await
                .unwrap()
        );
    });
}
