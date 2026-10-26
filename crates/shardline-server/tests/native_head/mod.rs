//! Denied native HEAD requests must not discover private provider objects.
mod revocation;
use std::{num::NonZeroUsize, time::Duration};

use reqwest::{Client, StatusCode};
use sha2::{Digest, Sha256};
use shardline_protocol::{
    RepositoryProvider, RepositoryScope, SecretString, TokenClaims, TokenScope,
};
use shardline_server::{ObjectStorageAdapter, ServerConfig, ServerFrontend};
use shardline_server_core::{AuthProvider, LocalHmacProvider};
use shardline_storage::S3ObjectStoreConfig;
use shardline_test_support::{DockerLocalStack, S3FaultProxy, S3ProxyFault};
use shardline_xet_core::{
    merklehash::{MerkleHash, compute_data_hash, file_hash},
    metadata_shard::{
        file_structs::{
            FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt, MDBFileInfo,
        },
        shard_in_memory::MDBInMemoryShard,
    },
};
use tokio::net::TcpListener;

const KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

struct ServerCleanup(tokio::task::AbortHandle);

impl Drop for ServerCleanup {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Docker MinIO"]
async fn scoped_head_authorizes_before_provider_metadata_with_local_records() {
    qualify(false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO"]
async fn scoped_head_authorizes_before_provider_metadata_with_postgres_records() {
    qualify(true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Docker PostgreSQL/MinIO and one million metadata records"]
async fn scoped_reads_recheck_membership_after_million_record_revocation() {
    tokio::time::timeout(Duration::from_secs(1200), qualify(true, true))
        .await
        .expect("million-record HTTP qualification exceeded its deadline");
}

fn token(repository: &str, permission: TokenScope) -> String {
    let scope =
        RepositoryScope::new(RepositoryProvider::Generic, "team", repository, None).unwrap();
    LocalHmacProvider::new(KEY)
        .unwrap()
        .mint_token(
            &TokenClaims::new("shardline", "head-reader", permission, scope, u64::MAX).unwrap(),
        )
        .unwrap()
}

async fn denied_reads(client: &Client, url: &str, credential: &str, proxy: &S3FaultProxy) {
    let before_heads = proxy.head_requests();
    let before_gets = proxy.get_requests();
    for (method, range) in [
        (reqwest::Method::HEAD, None),
        (reqwest::Method::GET, None),
        (reqwest::Method::GET, Some("bytes=0-0")),
        (reqwest::Method::GET, Some("invalid")),
    ] {
        let target = if method == reqwest::Method::GET {
            url.replace("/v1/xorbs/", "/transfer/xorb/")
        } else {
            url.to_owned()
        };
        let mut request = client.request(method, target);
        if let Some(range) = range {
            request = request.header(reqwest::header::RANGE, range);
        }
        let response = request.bearer_auth(credential).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(!response.headers().contains_key(reqwest::header::LOCATION));
    }
    assert_eq!(
        proxy.head_requests(),
        before_heads,
        "denied reads must not inspect provider metadata"
    );
    assert_eq!(
        proxy.get_requests(),
        before_gets,
        "denied reads must not read provider bytes"
    );
}

async fn qualify(postgres: bool, million: bool) {
    let services = tokio::task::spawn_blocking(move || {
        let builder = DockerLocalStack::builder().with_minio();
        let builder = if postgres {
            builder.with_postgres()
        } else {
            builder
        };
        builder
            .start()
            .unwrap()
            .expect("native HEAD qualification requires Docker")
    })
    .await
    .unwrap();
    let raw = services
        .s3_raw_config(Some(&services.unique_s3_key_prefix("native-head-auth")))
        .unwrap();
    let proxy = S3FaultProxy::start_disarmed(
        raw.endpoint.as_deref().unwrap(),
        S3ProxyFault::ServiceUnavailableOnce,
    )
    .unwrap();
    let storage = S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(Some(proxy.endpoint().to_owned()))
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_allow_http(true)
        .with_credentials(
            raw.access_key.map(SecretString::new),
            raw.secret_key.map(SecretString::new),
            None,
        );
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let base = format!("http://{address}");
    let mut config = ServerConfig::new(
        address,
        base.clone(),
        root.path().to_owned(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_server_frontends(vec![ServerFrontend::Xet])
    .unwrap()
    .with_deployment_mode(shardline_server::DeploymentMode::Insecure)
    .with_token_signing_key(KEY.to_vec())
    .unwrap()
    .with_reconstruction_cache_disabled()
    .with_object_storage(ObjectStorageAdapter::S3, Some(storage.clone()));
    if postgres {
        let database = services.postgres_url().unwrap();
        shardline_server::run_database_migration(&shardline_server::DatabaseMigrationOptions::new(
            database.clone(),
            shardline_server::DatabaseMigrationCommand::Up { steps: None },
        ))
        .await
        .unwrap();
        config = config.with_index_postgres_url(database).unwrap();
    }
    let router = shardline_server::app::router(config).await.unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let _server_cleanup = ServerCleanup(server.abort_handle());
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let owner = token("owner", TokenScope::Write);
    let other = token("other", TokenScope::Read);
    let data = vec![b'h'; 64 * 1024];
    let length = u64::try_from(data.len()).unwrap();
    let (hash, bytes, info) = super::container(&data);
    let url = format!("{base}/v1/xorbs/default/{}", hash.hex());
    let uploaded = client
        .post(&url)
        .bearer_auth(&owner)
        .body(bytes.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::OK);
    // An uploaded object without a published file is not a read grant.
    denied_reads(&client, &url, &owner, &proxy).await;
    let mut shard = MDBInMemoryShard::default();
    shard.add_xorb_block(info).unwrap();
    shard
        .add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(
                file_hash(&[(compute_data_hash(&data), length)]),
                1,
                false,
                true,
            ),
            segments: vec![FileDataSequenceEntry::new(hash, length, 0, 1)],
            verification: Vec::new(),
            metadata_ext: Some(FileMetadataExt::new(
                MerkleHash::from_hex(&format!("{:x}", Sha256::digest(&data))).unwrap(),
            )),
        })
        .unwrap();
    let published = client
        .post(format!("{base}/v1/shards"))
        .bearer_auth(&owner)
        .body(shard.to_bytes().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(published.status(), StatusCode::OK);
    if million {
        revocation::populate(services.postgres_url().unwrap().as_str()).await;
        for _ in 0..32 {
            assert_eq!(
                client
                    .head(&url)
                    .bearer_auth(&owner)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::OK
            );
        }
    }
    denied_reads(&client, &url, &other, &proxy).await;
    denied_reads(
        &client,
        &format!("{base}/v1/xorbs/default/{}", "f".repeat(64)),
        &owner,
        &proxy,
    )
    .await;
    let before_heads = proxy.head_requests();
    let before_gets = proxy.get_requests();
    let response = client.head(&url).bearer_auth(&owner).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    // HEAD has no response body; inspect the advertised object length rather
    // than reqwest's zero-length decoded body size.
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap(),
        u64::try_from(bytes.len()).unwrap()
    );
    assert_eq!(proxy.head_requests().checked_sub(before_heads), Some(1));
    assert_eq!(proxy.get_requests(), before_gets);
    revocation::Read {
        client: &client,
        url: &url,
        credential: &owner,
        proxy: &proxy,
        bytes: &bytes,
    }
    .qualify(root.path(), services.postgres_url().as_deref(), million)
    .await;
    let objects = shardline_server_core::ServerObjectStore::s3(storage).unwrap();
    let key = shardline_xet_adapter::xorb_object_key(&hash.hex()).unwrap();
    assert!(
        shardline_storage::AsyncObjectStore::metadata(&objects, &key)
            .await
            .unwrap()
            .is_some(),
        "revocation must not rely on deleting the provider object"
    );
    assert!(!proxy.fault_was_injected());
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
