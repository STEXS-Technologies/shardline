#![allow(clippy::indexing_slicing, clippy::panic_in_result_fn)]

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::{
    error::Error as StdError,
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use reqwest::{
    Client, StatusCode,
    header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, LOCATION},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, spawn, task::JoinHandle};

use super::{
    bearer_token, serve_with_listener, single_chunk_xorb, single_file_shard, wait_for_health,
};
use crate::{
    FileReconstructionResponse, ServerConfig, ServerError, ServerFrontend,
    bazel_http_adapter::{BazelCacheKind, bazel_cache_object_key},
    lfs_adapter::lfs_object_key,
    local_backend::chunk_hash,
    object_store::ServerObjectStore,
    oci_adapter::{oci_blob_key, oci_manifest_key, oci_manifest_media_type_key},
    protocol_support::shared_sha256_object_key,
};
use shardline_protocol::{RepositoryProvider, TokenScope};
use shardline_storage::{ObjectBody, ObjectIntegrity};

struct ProtocolFrontendRuntime {
    storage: tempfile::TempDir,
    base_url: String,
    server: JoinHandle<Result<(), ServerError>>,
}

struct ProtocolSharedRootRuntime {
    base_url: String,
    server: JoinHandle<Result<(), ServerError>>,
}

impl ProtocolFrontendRuntime {
    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn storage_path(&self) -> &Path {
        self.storage.path()
    }
}

impl Drop for ProtocolFrontendRuntime {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl ProtocolSharedRootRuntime {
    fn base_url(&self) -> &str {
        &self.base_url
    }
}

impl Drop for ProtocolSharedRootRuntime {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn start_protocol_runtime(
    frontends: &[ServerFrontend],
) -> Result<ProtocolFrontendRuntime, Box<dyn StdError>> {
    start_protocol_runtime_with_max_request_body(
        frontends,
        NonZeroUsize::new(67_108_864).unwrap_or(NonZeroUsize::MIN),
    )
    .await
}

async fn start_protocol_runtime_with_max_request_body(
    frontends: &[ServerFrontend],
    max_request_body_bytes: NonZeroUsize,
) -> Result<ProtocolFrontendRuntime, Box<dyn StdError>> {
    let storage = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_max_request_body_bytes(max_request_body_bytes)
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_server_frontends(frontends.iter().copied())?;
    let server = spawn(async move { serve_with_listener(config, listener).await });
    wait_for_health(&base_url).await?;

    Ok(ProtocolFrontendRuntime {
        storage,
        base_url,
        server,
    })
}

async fn start_protocol_runtime_with_oci_limits(
    frontends: &[ServerFrontend],
    oci_upload_session_ttl_seconds: NonZeroU64,
    oci_upload_max_active_sessions: NonZeroUsize,
) -> Result<ProtocolFrontendRuntime, Box<dyn StdError>> {
    let storage = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_oci_upload_session_ttl_seconds(oci_upload_session_ttl_seconds)
    .with_oci_upload_max_active_sessions(oci_upload_max_active_sessions)
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_server_frontends(frontends.iter().copied())?;
    let server = spawn(async move { serve_with_listener(config, listener).await });
    wait_for_health(&base_url).await?;

    Ok(ProtocolFrontendRuntime {
        storage,
        base_url,
        server,
    })
}

async fn start_protocol_runtime_with_oci_token_limits(
    frontends: &[ServerFrontend],
    oci_registry_token_ttl_seconds: NonZeroU64,
    oci_registry_token_max_in_flight_requests: NonZeroUsize,
) -> Result<ProtocolFrontendRuntime, Box<dyn StdError>> {
    let storage = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_oci_registry_token_ttl_seconds(oci_registry_token_ttl_seconds)
    .with_oci_registry_token_max_in_flight_requests(oci_registry_token_max_in_flight_requests)
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_server_frontends(frontends.iter().copied())?;
    let server = spawn(async move { serve_with_listener(config, listener).await });
    wait_for_health(&base_url).await?;

    Ok(ProtocolFrontendRuntime {
        storage,
        base_url,
        server,
    })
}

async fn start_protocol_runtime_on_shared_root(
    frontends: &[ServerFrontend],
    root_dir: &Path,
    configure: impl FnOnce(ServerConfig) -> ServerConfig,
) -> Result<ProtocolSharedRootRuntime, Box<dyn StdError>> {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = configure(ServerConfig::new(
        addr,
        base_url.clone(),
        root_dir.to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    ))
    .with_token_signing_key(b"signing-key".to_vec())?
    .with_server_frontends(frontends.iter().copied())?;
    let server = spawn(async move { serve_with_listener(config, listener).await });
    wait_for_health(&base_url).await?;

    Ok(ProtocolSharedRootRuntime { base_url, server })
}

fn scoped_token(scope: TokenScope, owner: &str, repo: &str) -> Result<String, Box<dyn StdError>> {
    bearer_token(
        "protocol-user-1",
        scope,
        RepositoryProvider::GitHub,
        owner,
        repo,
        Some("main"),
    )
}

fn scoped_repository(
    owner: &str,
    repo: &str,
) -> Result<shardline_protocol::RepositoryScope, Box<dyn StdError>> {
    Ok(shardline_protocol::RepositoryScope::new(
        RepositoryProvider::GitHub,
        owner,
        repo,
        Some("main"),
    )?)
}

fn seed_oci_blob(
    runtime: &ProtocolFrontendRuntime,
    repository: &str,
    repository_scope: &shardline_protocol::RepositoryScope,
    bytes: &[u8],
) -> Result<String, Box<dyn StdError>> {
    let digest_hex = hex::encode(Sha256::digest(bytes));
    let object_key = oci_blob_key(repository, &digest_hex, Some(repository_scope))?;
    let object_store = ServerObjectStore::local(runtime.storage_path().join("chunks"))?;
    let integrity = ObjectIntegrity::new(chunk_hash(bytes), u64::try_from(bytes.len())?);
    let _stored =
        object_store.put_if_absent(&object_key, ObjectBody::from_slice(bytes), &integrity)?;
    Ok(digest_hex)
}

fn seed_oci_manifest(
    runtime: &ProtocolFrontendRuntime,
    repository: &str,
    repository_scope: &shardline_protocol::RepositoryScope,
    media_type: &str,
    bytes: &[u8],
) -> Result<String, Box<dyn StdError>> {
    let digest_hex = hex::encode(Sha256::digest(bytes));
    let object_store = ServerObjectStore::local(runtime.storage_path().join("chunks"))?;

    let manifest_key = oci_manifest_key(repository, &digest_hex, Some(repository_scope))?;
    let manifest_integrity = ObjectIntegrity::new(chunk_hash(bytes), u64::try_from(bytes.len())?);
    let _stored_manifest = object_store.put_if_absent(
        &manifest_key,
        ObjectBody::from_slice(bytes),
        &manifest_integrity,
    )?;

    let media_type_key =
        oci_manifest_media_type_key(repository, &digest_hex, Some(repository_scope))?;
    let media_type_bytes = media_type.as_bytes();
    let media_type_integrity = ObjectIntegrity::new(
        chunk_hash(media_type_bytes),
        u64::try_from(media_type_bytes.len())?,
    );
    let _stored_media_type = object_store.put_if_absent(
        &media_type_key,
        ObjectBody::from_slice(media_type_bytes),
        &media_type_integrity,
    )?;

    Ok(digest_hex)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_frontend_round_trip_and_repository_scoping() -> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Lfs]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let other_read_token = scoped_token(TokenScope::Read, "team", "other-assets")?;
    let bytes = b"large-file-content";
    let oid = hex::encode(Sha256::digest(bytes));

    let batch_upload = client
        .post(format!("{}/v1/lfs/objects/batch", runtime.base_url()))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/json")
        .json(&json!({
            "operation": "upload",
            "transfers": ["basic"],
            "objects": [{ "oid": oid, "size": bytes.len() }],
        }))
        .send()
        .await?;
    assert_eq!(batch_upload.status(), StatusCode::OK);
    assert_eq!(
        batch_upload
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/vnd.git-lfs+json")
    );
    let batch_upload = batch_upload.json::<Value>().await?;
    assert_eq!(batch_upload["transfer"], "basic");
    assert_eq!(
        batch_upload["objects"][0]["actions"]["upload"]["href"],
        format!("{}/v1/lfs/objects/{oid}", runtime.base_url())
    );

    let upload = client
        .put(format!("{}/v1/lfs/objects/{oid}", runtime.base_url()))
        .bearer_auth(&write_token)
        .body(bytes.as_slice().to_vec())
        .send()
        .await?;
    assert_eq!(upload.status(), StatusCode::OK);

    let head = client
        .head(format!("{}/v1/lfs/objects/{oid}", runtime.base_url()))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(
        head.headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some("18")
    );

    let batch_download = client
        .post(format!("{}/v1/lfs/objects/batch", runtime.base_url()))
        .bearer_auth(&read_token)
        .header(CONTENT_TYPE, "application/json")
        .json(&json!({
            "operation": "download",
            "objects": [{ "oid": oid, "size": bytes.len() }],
        }))
        .send()
        .await?;
    assert_eq!(batch_download.status(), StatusCode::OK);
    let batch_download = batch_download.json::<Value>().await?;
    assert_eq!(
        batch_download["objects"][0]["actions"]["download"]["href"],
        format!("{}/v1/lfs/objects/{oid}", runtime.base_url())
    );

    let download = client
        .get(format!("{}/v1/lfs/objects/{oid}", runtime.base_url()))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(download.status(), StatusCode::OK);
    assert_eq!(
        download
            .headers()
            .get("Docker-Content-Digest")
            .and_then(|value| value.to_str().ok()),
        Some(format!("sha256:{oid}").as_str())
    );
    assert_eq!(download.bytes().await?.as_ref(), bytes);

    let wrong_scope = client
        .get(format!("{}/v1/lfs/objects/{oid}", runtime.base_url()))
        .bearer_auth(&other_read_token)
        .send()
        .await?;
    assert_eq!(wrong_scope.status(), StatusCode::NOT_FOUND);

    let missing_auth = client
        .get(format!("{}/v1/lfs/objects/{oid}", runtime.base_url()))
        .send()
        .await?;
    assert_eq!(missing_auth.status(), StatusCode::UNAUTHORIZED);

    let mismatched_upload = client
        .put(format!("{}/v1/lfs/objects/{oid}", runtime.base_url()))
        .bearer_auth(&write_token)
        .body("wrong-body".as_bytes().to_vec())
        .send()
        .await?;
    assert_eq!(mismatched_upload.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_http_frontend_round_trip_and_hash_validation() -> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::BazelHttp]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let other_read_token = scoped_token(TokenScope::Read, "team", "other-assets")?;

    let ac_hash = "a".repeat(64);
    let ac_body = b"{\"exitCode\":0}";
    let ac_put = client
        .put(format!(
            "{}/v1/bazel/cache/ac/{ac_hash}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .body(ac_body.as_slice().to_vec())
        .send()
        .await?;
    assert_eq!(ac_put.status(), StatusCode::NO_CONTENT);

    let ac_get = client
        .get(format!(
            "{}/v1/bazel/cache/ac/{ac_hash}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(ac_get.status(), StatusCode::OK);
    assert_eq!(ac_get.bytes().await?.as_ref(), ac_body);

    let cas_body = b"compiled-artifact";
    let cas_hash = hex::encode(Sha256::digest(cas_body));
    let cas_put = client
        .put(format!(
            "{}/v1/bazel/cache/cas/{cas_hash}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .body(cas_body.as_slice().to_vec())
        .send()
        .await?;
    assert_eq!(cas_put.status(), StatusCode::NO_CONTENT);

    let cas_get = client
        .get(format!(
            "{}/v1/bazel/cache/cas/{cas_hash}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(cas_get.status(), StatusCode::OK);
    assert_eq!(cas_get.bytes().await?.as_ref(), cas_body);

    let wrong_scope = client
        .get(format!(
            "{}/v1/bazel/cache/cas/{cas_hash}",
            runtime.base_url()
        ))
        .bearer_auth(&other_read_token)
        .send()
        .await?;
    assert_eq!(wrong_scope.status(), StatusCode::NOT_FOUND);

    let missing_auth = client
        .get(format!(
            "{}/v1/bazel/cache/ac/{ac_hash}",
            runtime.base_url()
        ))
        .send()
        .await?;
    assert_eq!(missing_auth.status(), StatusCode::UNAUTHORIZED);

    let mismatched_cas = client
        .put(format!(
            "{}/v1/bazel/cache/cas/{cas_hash}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .body("wrong-artifact".as_bytes().to_vec())
        .send()
        .await?;
    assert_eq!(mismatched_cas.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_frontend_rejects_excessive_batch_cardinality() -> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Lfs]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let objects = (0..=1024)
        .map(|index| {
            json!({
                "oid": format!("{index:064x}"),
                "size": 1,
            })
        })
        .collect::<Vec<_>>();

    let response = client
        .post(format!("{}/v1/lfs/objects/batch", runtime.base_url()))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/json")
        .json(&json!({
            "operation": "upload",
            "transfers": ["basic"],
            "objects": objects,
        }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_frontend_rejects_unsupported_transfer_adapters() -> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Lfs]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;

    let response = client
        .post(format!("{}/v1/lfs/objects/batch", runtime.base_url()))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/json")
        .json(&json!({
            "operation": "upload",
            "transfers": ["tus.io", "ssh"],
            "objects": [{
                "oid": format!("{:064x}", 1),
                "size": 1
            }],
        }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = response.json::<Value>().await?;
    assert_eq!(body["message"], "unsupported transfer adapter");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_frontend_batch_reports_stored_object_size() -> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Lfs]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let bytes = b"authoritative-lfs-size";
    let oid = hex::encode(Sha256::digest(bytes));

    let upload = client
        .put(format!("{}/v1/lfs/objects/{oid}", runtime.base_url()))
        .bearer_auth(&write_token)
        .body(bytes.as_slice().to_vec())
        .send()
        .await?;
    assert_eq!(upload.status(), StatusCode::OK);

    let upload_batch = client
        .post(format!("{}/v1/lfs/objects/batch", runtime.base_url()))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/json")
        .json(&json!({
            "operation": "upload",
            "objects": [{ "oid": oid, "size": 1 }],
        }))
        .send()
        .await?;
    assert_eq!(upload_batch.status(), StatusCode::OK);
    let upload_batch = upload_batch.json::<Value>().await?;
    assert_eq!(upload_batch["objects"][0]["size"], bytes.len());
    assert!(upload_batch["objects"][0]["actions"].is_null());

    let download_batch = client
        .post(format!("{}/v1/lfs/objects/batch", runtime.base_url()))
        .bearer_auth(&read_token)
        .header(CONTENT_TYPE, "application/json")
        .json(&json!({
            "operation": "download",
            "objects": [{ "oid": oid, "size": 1 }],
        }))
        .send()
        .await?;
    assert_eq!(download_batch.status(), StatusCode::OK);
    let download_batch = download_batch.json::<Value>().await?;
    assert_eq!(download_batch["objects"][0]["size"], bytes.len());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_frontends_share_digest_addressed_storage_and_keep_xet_working()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[
        ServerFrontend::Xet,
        ServerFrontend::Lfs,
        ServerFrontend::BazelHttp,
        ServerFrontend::Oci,
    ])
    .await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let repository_scope = scoped_repository("team", "assets")?;

    let shared_bytes = b"shared-frontends-payload";
    let digest_hex = hex::encode(Sha256::digest(shared_bytes));

    let lfs_upload = client
        .put(format!(
            "{}/v1/lfs/objects/{digest_hex}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .body(shared_bytes.as_slice().to_vec())
        .send()
        .await?;
    assert_eq!(lfs_upload.status(), StatusCode::OK);

    let bazel_upload = client
        .put(format!(
            "{}/v1/bazel/cache/cas/{digest_hex}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .body(shared_bytes.as_slice().to_vec())
        .send()
        .await?;
    assert_eq!(bazel_upload.status(), StatusCode::NO_CONTENT);

    let oci_upload = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads?digest=sha256:{digest_hex}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .body(shared_bytes.as_slice().to_vec())
        .send()
        .await?;
    assert_eq!(oci_upload.status(), StatusCode::CREATED);

    let lfs_download = client
        .get(format!(
            "{}/v1/lfs/objects/{digest_hex}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(lfs_download.status(), StatusCode::OK);
    assert_eq!(lfs_download.bytes().await?.as_ref(), shared_bytes);

    let bazel_download = client
        .get(format!(
            "{}/v1/bazel/cache/cas/{digest_hex}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(bazel_download.status(), StatusCode::OK);
    assert_eq!(bazel_download.bytes().await?.as_ref(), shared_bytes);

    let oci_download = client
        .get(format!(
            "{}/v2/team/assets/blobs/sha256:{digest_hex}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(oci_download.status(), StatusCode::OK);
    assert_eq!(oci_download.bytes().await?.as_ref(), shared_bytes);

    let (first_xorb, first_hash) = single_chunk_xorb(b"abcd");
    let (second_xorb, second_hash) = single_chunk_xorb(b"efgh");
    for (xorb_body, xorb_hash) in [
        (first_xorb, first_hash.as_str()),
        (second_xorb, second_hash.as_str()),
    ] {
        let uploaded = client
            .post(format!(
                "{}/v1/xorbs/default/{xorb_hash}",
                runtime.base_url()
            ))
            .bearer_auth(&write_token)
            .body(xorb_body)
            .send()
            .await?;
        assert!(uploaded.status().is_success());
    }
    let (shard_body, file_hash) =
        single_file_shard(&[(b"abcd", &first_hash), (b"efgh", &second_hash)]);
    let shard_upload = client
        .post(format!("{}/v1/shards", runtime.base_url()))
        .bearer_auth(&write_token)
        .body(shard_body)
        .send()
        .await?;
    assert!(shard_upload.status().is_success());

    let reconstruction = client
        .get(format!(
            "{}/v1/reconstructions/{file_hash}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(reconstruction.status(), StatusCode::OK);
    let reconstruction = reconstruction.json::<FileReconstructionResponse>().await?;
    assert_eq!(reconstruction.terms.len(), 2);
    assert!(reconstruction.fetch_info.contains_key(&first_hash));
    assert!(reconstruction.fetch_info.contains_key(&second_hash));

    let object_store = ServerObjectStore::local(runtime.storage_path().join("chunks"))?;
    let lfs_key = lfs_object_key(&digest_hex, Some(&repository_scope))?;
    let bazel_key =
        bazel_cache_object_key(BazelCacheKind::Cas, &digest_hex, Some(&repository_scope))?;
    let oci_key = oci_blob_key("team/assets", &digest_hex, Some(&repository_scope))?;
    let shared_key = shared_sha256_object_key(&digest_hex)?;
    let lfs_path = object_store
        .local_path_for_key(&lfs_key)
        .ok_or("expected local object path for lfs object")?;
    let bazel_path = object_store
        .local_path_for_key(&bazel_key)
        .ok_or("expected local object path for bazel object")?;
    let oci_path = object_store
        .local_path_for_key(&oci_key)
        .ok_or("expected local object path for oci object")?;
    let shared_path = object_store
        .local_path_for_key(&shared_key)
        .ok_or("expected local object path for shared object")?;

    #[cfg(unix)]
    {
        let lfs_metadata = fs::metadata(&lfs_path)?;
        let bazel_metadata = fs::metadata(&bazel_path)?;
        let oci_metadata = fs::metadata(&oci_path)?;
        let shared_metadata = fs::metadata(&shared_path)?;
        assert_eq!(lfs_metadata.ino(), bazel_metadata.ino());
        assert_eq!(lfs_metadata.ino(), oci_metadata.ino());
        assert_eq!(lfs_metadata.ino(), shared_metadata.ino());
        assert!(shared_metadata.nlink() >= 4);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_blob_manifest_and_tag_round_trip() -> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let other_read_token = scoped_token(TokenScope::Read, "team", "other-assets")?;

    let root = client
        .get(format!("{}/v2/", runtime.base_url()))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(root.status(), StatusCode::OK);
    assert_eq!(
        root.headers()
            .get("Docker-Distribution-API-Version")
            .and_then(|value| value.to_str().ok()),
        Some("registry/2.0")
    );

    let blob_bytes = br#"{"architecture":"amd64","os":"linux"}"#;
    let blob_digest = hex::encode(Sha256::digest(blob_bytes));
    let blob_upload = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads?digest=sha256:{blob_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .body(blob_bytes.as_slice().to_vec())
        .send()
        .await?;
    assert_eq!(blob_upload.status(), StatusCode::CREATED);
    assert_eq!(
        blob_upload
            .headers()
            .get("Docker-Content-Digest")
            .and_then(|value| value.to_str().ok()),
        Some(format!("sha256:{blob_digest}").as_str())
    );

    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{blob_digest}"),
            "size": blob_bytes.len(),
        },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest)?;
    let manifest_digest = hex::encode(Sha256::digest(&manifest_bytes));
    let manifest_put = client
        .put(format!(
            "{}/v2/team/assets/manifests/v1?tag=stable",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes.clone())
        .send()
        .await?;
    assert_eq!(manifest_put.status(), StatusCode::CREATED);
    assert_eq!(
        manifest_put
            .headers()
            .get("Docker-Content-Digest")
            .and_then(|value| value.to_str().ok()),
        Some(format!("sha256:{manifest_digest}").as_str())
    );

    let manifest_head = client
        .head(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(manifest_head.status(), StatusCode::OK);
    assert_eq!(
        manifest_head
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/vnd.oci.image.manifest.v1+json")
    );

    let manifest_get = client
        .get(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(manifest_get.status(), StatusCode::OK);
    assert_eq!(
        manifest_get.bytes().await?.as_ref(),
        manifest_bytes.as_slice()
    );

    let tags = client
        .get(format!("{}/v2/team/assets/tags/list", runtime.base_url()))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(tags.status(), StatusCode::OK);
    let tags = tags.json::<Value>().await?;
    assert_eq!(tags["name"], "team/assets");
    assert_eq!(tags["tags"], json!(["stable", "v1"]));

    let blob_get = client
        .get(format!(
            "{}/v2/team/assets/blobs/sha256:{blob_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(blob_get.status(), StatusCode::OK);
    assert_eq!(blob_get.bytes().await?.as_ref(), blob_bytes);

    let wrong_scope = client
        .get(format!(
            "{}/v2/team/assets/blobs/sha256:{blob_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&other_read_token)
        .send()
        .await?;
    assert_eq!(wrong_scope.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_auth_challenges_missing_tokens_and_forbids_read_only_writes()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;

    let missing_root_auth = client
        .get(format!("{}/v2/", runtime.base_url()))
        .send()
        .await?;
    assert_eq!(missing_root_auth.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        missing_root_auth
            .headers()
            .get("WWW-Authenticate")
            .and_then(|value| value.to_str().ok()),
        Some(
            format!(
                "Bearer realm=\"{}/v2/token\",service=\"shardline\"",
                runtime.base_url()
            )
            .as_str()
        )
    );

    let insufficient_scope = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(insufficient_scope.status(), StatusCode::FORBIDDEN);
    assert!(
        insufficient_scope
            .headers()
            .get("WWW-Authenticate")
            .is_none()
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_registry_token_exchange_supports_basic_client_credentials()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;

    let blob_bytes = br#"{"architecture":"amd64","os":"linux"}"#;
    let blob_digest = hex::encode(Sha256::digest(blob_bytes));
    let blob_upload = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads?digest=sha256:{blob_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .body(blob_bytes.as_slice().to_vec())
        .send()
        .await?;
    assert_eq!(blob_upload.status(), StatusCode::CREATED);

    let manifest = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{blob_digest}"),
            "size": blob_bytes.len(),
        },
        "layers": [],
    }))?;
    let manifest_put = client
        .put(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(manifest)
        .send()
        .await?;
    assert_eq!(manifest_put.status(), StatusCode::CREATED);

    let missing_auth = client
        .get(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .send()
        .await?;
    assert_eq!(missing_auth.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        missing_auth
            .headers()
            .get("WWW-Authenticate")
            .and_then(|value| value.to_str().ok()),
        Some(
            format!(
                "Bearer realm=\"{}/v2/token\",service=\"shardline\",scope=\"repository:team/assets:pull\"",
                runtime.base_url()
            )
            .as_str()
        )
    );

    let basic_credentials = BASE64_STANDARD.encode(format!("builder:{read_token}"));
    let exchanged = client
        .get(format!(
            "{}/v2/token?service=shardline&scope=repository:team/assets:pull&account=protocol-user-1",
            runtime.base_url()
        ))
        .header(AUTHORIZATION, format!("Basic {basic_credentials}"))
        .send()
        .await?;
    assert_eq!(exchanged.status(), StatusCode::OK);
    let exchanged = exchanged.json::<Value>().await?;
    let registry_token = exchanged["token"].as_str().ok_or("missing token")?;
    assert_eq!(exchanged["access_token"], exchanged["token"]);

    let fetched = client
        .get(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .bearer_auth(registry_token)
        .send()
        .await?;
    assert_eq!(fetched.status(), StatusCode::OK);

    let denied_push = client
        .get(format!(
            "{}/v2/token?service=shardline&scope=repository:team/assets:pull,push&account=protocol-user-1",
            runtime.base_url()
        ))
        .header(AUTHORIZATION, format!("Basic {basic_credentials}"))
        .send()
        .await?;
    assert_eq!(denied_push.status(), StatusCode::FORBIDDEN);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_registry_token_exchange_rejects_duplicate_service_or_oversized_query_values()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let basic_credentials = BASE64_STANDARD.encode(format!("stexs:{read_token}"));

    let duplicate_service = client
        .get(format!(
            "{}/v2/token?service=shardline&service=shardline&scope=repository:team/assets:pull",
            runtime.base_url()
        ))
        .header(AUTHORIZATION, format!("Basic {basic_credentials}"))
        .send()
        .await?;
    assert_eq!(duplicate_service.status(), StatusCode::BAD_REQUEST);

    let oversized_scope = client
        .get(format!(
            "{}/v2/token?service=shardline&scope={}",
            runtime.base_url(),
            "a".repeat(2_048)
        ))
        .header(AUTHORIZATION, format!("Basic {basic_credentials}"))
        .send()
        .await?;
    assert_eq!(oversized_scope.status(), StatusCode::URI_TOO_LONG);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_registry_token_exchange_merges_repeated_scope_values_for_one_repository()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let basic_credentials = BASE64_STANDARD.encode(format!("stexs:{write_token}"));

    let exchanged = client
        .get(format!(
            "{}/v2/token?service=shardline&scope=repository:team/assets:pull&scope=repository:team/assets:pull,push",
            runtime.base_url()
        ))
        .header(AUTHORIZATION, format!("Basic {basic_credentials}"))
        .send()
        .await?;
    assert_eq!(exchanged.status(), StatusCode::OK);

    let exchanged = exchanged.json::<Value>().await?;
    let registry_token = exchanged["token"].as_str().ok_or("missing token")?;
    let write_probe = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads",
            runtime.base_url()
        ))
        .bearer_auth(registry_token)
        .send()
        .await?;
    assert_eq!(write_probe.status(), StatusCode::ACCEPTED);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_registry_token_exchange_uses_dedicated_ttl_and_reports_metrics()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime_with_oci_token_limits(
        &[ServerFrontend::Oci],
        NonZeroU64::new(1).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .await?;
    let client = Client::new();
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let basic_credentials = BASE64_STANDARD.encode(format!("stexs:{read_token}"));

    let exchanged = client
        .get(format!(
            "{}/v2/token?service=shardline&scope=repository:team/assets:pull",
            runtime.base_url()
        ))
        .header(AUTHORIZATION, format!("Basic {basic_credentials}"))
        .send()
        .await?;
    assert_eq!(exchanged.status(), StatusCode::OK);
    let exchanged = exchanged.json::<Value>().await?;
    let expires_in = exchanged["expires_in"]
        .as_u64()
        .ok_or("missing expires_in")?;
    assert!(expires_in <= 1);

    let metrics = client
        .get(format!("{}/metrics", runtime.base_url()))
        .send()
        .await?;
    assert_eq!(metrics.status(), StatusCode::OK);
    let metrics = metrics.text().await?;
    assert!(metrics.contains("shardline_oci_registry_token_ttl_seconds 1"));
    assert!(metrics.contains("shardline_oci_registry_token_max_in_flight_requests 4"));
    assert!(metrics.contains("shardline_oci_registry_token_requests_total 1"));
    assert!(metrics.contains("shardline_oci_registry_token_rate_limited_total 0"));
    assert!(metrics.contains("shardline_oci_registry_token_active_requests 0"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_manifest_get_honors_accept_headers() -> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;

    let blob_bytes = br#"{"architecture":"amd64","os":"linux"}"#;
    let blob_digest = hex::encode(Sha256::digest(blob_bytes));
    let blob_upload = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads?digest=sha256:{blob_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .body(blob_bytes.as_slice().to_vec())
        .send()
        .await?;
    assert_eq!(blob_upload.status(), StatusCode::CREATED);

    let manifest = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{blob_digest}"),
            "size": blob_bytes.len(),
        },
        "layers": [],
    }))?;
    let manifest_put = client
        .put(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(manifest)
        .send()
        .await?;
    assert_eq!(manifest_put.status(), StatusCode::CREATED);

    let exact_accept = client
        .get(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .header(ACCEPT, "application/vnd.oci.image.manifest.v1+json")
        .send()
        .await?;
    assert_eq!(exact_accept.status(), StatusCode::OK);

    let wildcard_accept = client
        .head(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .header(ACCEPT, "application/*")
        .send()
        .await?;
    assert_eq!(wildcard_accept.status(), StatusCode::OK);

    let incompatible_accept = client
        .get(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .header(ACCEPT, "application/vnd.oci.image.index.v1+json")
        .send()
        .await?;
    assert_eq!(incompatible_accept.status(), StatusCode::NOT_ACCEPTABLE);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_blob_head_range_and_manifest_digest_resolution_work()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;

    let blob_bytes = b"abcdefghij";
    let blob_digest = hex::encode(Sha256::digest(blob_bytes));
    let blob_upload = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads?digest=sha256:{blob_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .body(blob_bytes.as_slice().to_vec())
        .send()
        .await?;
    assert_eq!(blob_upload.status(), StatusCode::CREATED);

    let blob_head = client
        .head(format!(
            "{}/v2/team/assets/blobs/sha256:{blob_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(blob_head.status(), StatusCode::OK);
    assert_eq!(
        blob_head
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()),
        Some("10")
    );
    assert_eq!(
        blob_head
            .headers()
            .get("Docker-Content-Digest")
            .and_then(|value| value.to_str().ok()),
        Some(format!("sha256:{blob_digest}").as_str())
    );

    let blob_range = client
        .get(format!(
            "{}/v2/team/assets/blobs/sha256:{blob_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .header("Range", "bytes=2-5")
        .send()
        .await?;
    assert_eq!(blob_range.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        blob_range
            .headers()
            .get("Content-Range")
            .and_then(|value| value.to_str().ok()),
        Some("bytes 2-5/10")
    );
    assert_eq!(blob_range.bytes().await?.as_ref(), b"cdef");

    let manifest_bytes = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{blob_digest}"),
            "size": blob_bytes.len(),
        },
        "layers": [],
    }))?;
    let manifest_digest = hex::encode(Sha256::digest(&manifest_bytes));
    let manifest_put = client
        .put(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes.clone())
        .send()
        .await?;
    assert_eq!(manifest_put.status(), StatusCode::CREATED);

    let manifest_head = client
        .head(format!(
            "{}/v2/team/assets/manifests/sha256:{manifest_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(manifest_head.status(), StatusCode::OK);
    assert_eq!(
        manifest_head
            .headers()
            .get("Docker-Content-Digest")
            .and_then(|value| value.to_str().ok()),
        Some(format!("sha256:{manifest_digest}").as_str())
    );

    let manifest_get = client
        .get(format!(
            "{}/v2/team/assets/manifests/sha256:{manifest_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(manifest_get.status(), StatusCode::OK);
    assert_eq!(
        manifest_get.bytes().await?.as_ref(),
        manifest_bytes.as_slice()
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_index_manifest_round_trip_preserves_media_type()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let repository_scope = scoped_repository("team", "assets")?;

    let child_manifest_a = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{}", "a".repeat(64)),
            "size": 11,
        },
        "layers": [],
    }))?;
    let child_manifest_b = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{}", "b".repeat(64)),
            "size": 12,
        },
        "layers": [],
    }))?;
    let child_manifest_a_digest = seed_oci_manifest(
        &runtime,
        "team/assets",
        &repository_scope,
        "application/vnd.oci.image.manifest.v1+json",
        &child_manifest_a,
    )?;
    let child_manifest_b_digest = seed_oci_manifest(
        &runtime,
        "team/assets",
        &repository_scope,
        "application/vnd.oci.image.manifest.v1+json",
        &child_manifest_b,
    )?;

    let index_bytes = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": format!("sha256:{child_manifest_a_digest}"),
            "size": 123,
            "platform": {
                "architecture": "amd64",
                "os": "linux",
            },
        }, {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": format!("sha256:{child_manifest_b_digest}"),
            "size": 456,
            "platform": {
                "architecture": "arm64",
                "os": "linux",
            },
        }],
    }))?;
    let index_digest = hex::encode(Sha256::digest(&index_bytes));

    let put = client
        .put(format!(
            "{}/v2/team/assets/manifests/multi",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/vnd.oci.image.index.v1+json")
        .body(index_bytes.clone())
        .send()
        .await?;
    assert_eq!(put.status(), StatusCode::CREATED);
    assert_eq!(
        put.headers()
            .get("Docker-Content-Digest")
            .and_then(|value| value.to_str().ok()),
        Some(format!("sha256:{index_digest}").as_str())
    );

    let head = client
        .head(format!(
            "{}/v2/team/assets/manifests/multi",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(
        head.headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/vnd.oci.image.index.v1+json")
    );

    let get = client
        .get(format!(
            "{}/v2/team/assets/manifests/sha256:{index_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/vnd.oci.image.index.v1+json")
    );
    assert_eq!(get.bytes().await?.as_ref(), index_bytes.as_slice());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_rejects_manifests_with_missing_blob_references()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let manifest_bytes = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": 17,
            "digest": format!("sha256:{}", "1".repeat(64)),
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar",
            "size": 23,
            "digest": format!("sha256:{}", "2".repeat(64)),
        }],
    }))?;

    let response = client
        .put(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_rejects_indexes_with_missing_child_manifests() -> Result<(), Box<dyn StdError>>
{
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let index_bytes = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "size": 91,
            "digest": format!("sha256:{}", "3".repeat(64)),
            "platform": {
                "architecture": "amd64",
                "os": "linux",
            },
        }],
    }))?;

    let response = client
        .put(format!(
            "{}/v2/team/assets/manifests/multi",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/vnd.oci.image.index.v1+json")
        .body(index_bytes)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_rejects_unsupported_or_mismatched_manifest_media_types()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let repository_scope = scoped_repository("team", "assets")?;
    let config_bytes = br#"{"architecture":"amd64"}"#;
    let layer_bytes = b"layer-bytes";
    let config_digest = seed_oci_blob(&runtime, "team/assets", &repository_scope, config_bytes)?;
    let layer_digest = seed_oci_blob(&runtime, "team/assets", &repository_scope, layer_bytes)?;
    let mismatched_manifest = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": config_bytes.len(),
            "digest": format!("sha256:{config_digest}"),
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar",
            "size": layer_bytes.len(),
            "digest": format!("sha256:{layer_digest}"),
        }],
    }))?;

    let mismatched = client
        .put(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(mismatched_manifest)
        .send()
        .await?;
    assert_eq!(mismatched.status(), StatusCode::BAD_REQUEST);

    let unsupported = client
        .put(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "text/plain")
        .body(br#"{"schemaVersion":2}"#.to_vec())
        .send()
        .await?;
    assert_eq!(unsupported.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_mounts_existing_blob_into_nested_repository() -> Result<(), Box<dyn StdError>>
{
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let repository_scope = scoped_repository("team", "assets")?;
    let source_bytes = b"mounted blob";
    let digest_hex = seed_oci_blob(
        &runtime,
        "team/assets/source",
        &repository_scope,
        source_bytes,
    )?;

    let mount = client
        .post(format!(
            "{}/v2/team/assets/target/blobs/uploads?mount=sha256:{digest_hex}&from=team/assets/source",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(mount.status(), StatusCode::CREATED);
    assert_eq!(
        mount
            .headers()
            .get("Docker-Content-Digest")
            .and_then(|value| value.to_str().ok()),
        Some(format!("sha256:{digest_hex}").as_str())
    );

    let mounted_blob = client
        .get(format!(
            "{}/v2/team/assets/target/blobs/sha256:{digest_hex}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(mounted_blob.status(), StatusCode::OK);
    assert_eq!(mounted_blob.bytes().await?.as_ref(), source_bytes);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_tags_list_is_paginated_and_validates_inputs() -> Result<(), Box<dyn StdError>>
{
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;
    let repository_scope = scoped_repository("team", "assets")?;

    for tag in ["v3", "v1", "v2"] {
        let config_bytes = tag.as_bytes();
        let config_digest =
            seed_oci_blob(&runtime, "team/assets", &repository_scope, config_bytes)?;
        let manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": format!("sha256:{config_digest}"),
                "size": config_bytes.len(),
            },
            "layers": [],
        }))?;
        let response = client
            .put(format!(
                "{}/v2/team/assets/manifests/{tag}",
                runtime.base_url()
            ))
            .bearer_auth(&write_token)
            .header(CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
            .body(manifest)
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    let first_page = client
        .get(format!(
            "{}/v2/team/assets/tags/list?n=2",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(first_page.status(), StatusCode::OK);
    assert_eq!(
        first_page
            .headers()
            .get("link")
            .and_then(|value| value.to_str().ok()),
        Some("</v2/team/assets/tags/list?n=2&last=v2>; rel=\"next\"")
    );
    let first_page = first_page.json::<Value>().await?;
    assert_eq!(first_page["tags"], json!(["v1", "v2"]));

    let second_page = client
        .get(format!(
            "{}/v2/team/assets/tags/list?n=2&last=v2",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(second_page.status(), StatusCode::OK);
    assert!(second_page.headers().get("link").is_none());
    let second_page = second_page.json::<Value>().await?;
    assert_eq!(second_page["tags"], json!(["v3"]));

    let invalid_page_size = client
        .get(format!(
            "{}/v2/team/assets/tags/list?n=0",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(invalid_page_size.status(), StatusCode::BAD_REQUEST);

    let invalid_last = client
        .get(format!(
            "{}/v2/team/assets/tags/list?last=bad/tag",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(invalid_last.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_chunked_upload_and_invalid_tag_rejection() -> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;

    let start_upload = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(start_upload.status(), StatusCode::ACCEPTED);
    let upload_location = start_upload
        .headers()
        .get(LOCATION)
        .and_then(|value| value.to_str().ok())
        .ok_or("missing upload location")?;
    let upload_url = format!("{}{}", runtime.base_url(), upload_location);

    let patch = client
        .patch(&upload_url)
        .bearer_auth(&write_token)
        .header(CONTENT_RANGE, "0-3")
        .body(b"abcd".to_vec())
        .send()
        .await?;
    assert_eq!(patch.status(), StatusCode::ACCEPTED);

    let status = client
        .get(&upload_url)
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(status.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        status
            .headers()
            .get("range")
            .and_then(|value| value.to_str().ok()),
        Some("0-3")
    );

    let blob_bytes = b"abcdefgh";
    let blob_digest = hex::encode(Sha256::digest(blob_bytes));
    let complete = client
        .put(format!("{upload_url}?digest=sha256:{blob_digest}"))
        .bearer_auth(&write_token)
        .header(CONTENT_RANGE, "4-7")
        .body(b"efgh".to_vec())
        .send()
        .await?;
    assert_eq!(complete.status(), StatusCode::CREATED);

    let blob_get = client
        .get(format!(
            "{}/v2/team/assets/blobs/sha256:{blob_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(blob_get.status(), StatusCode::OK);
    assert_eq!(blob_get.bytes().await?.as_ref(), blob_bytes);

    let manifest = json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{blob_digest}"),
            "size": blob_bytes.len(),
        },
        "layers": [],
    });
    let invalid_tag = client
        .put(format!(
            "{}/v2/team/assets/manifests/v2?tag=bad/tag",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(serde_json::to_vec(&manifest)?)
        .send()
        .await?;
    assert_eq!(invalid_tag.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_delete_upload_session_cancels_pending_blob() -> Result<(), Box<dyn StdError>>
{
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;

    let start_upload = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(start_upload.status(), StatusCode::ACCEPTED);
    let upload_location = start_upload
        .headers()
        .get(LOCATION)
        .and_then(|value| value.to_str().ok())
        .ok_or("missing upload location")?;
    let upload_url = format!("{}{}", runtime.base_url(), upload_location);

    let patch = client
        .patch(&upload_url)
        .bearer_auth(&write_token)
        .body(b"abcd".to_vec())
        .send()
        .await?;
    assert_eq!(patch.status(), StatusCode::ACCEPTED);

    let delete = client
        .delete(&upload_url)
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);

    let status = client
        .get(&upload_url)
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(status.status(), StatusCode::NOT_FOUND);

    let finalize = client
        .put(format!(
            "{upload_url}?digest=sha256:{}",
            hex::encode(Sha256::digest(b"abcdefgh"))
        ))
        .bearer_auth(&write_token)
        .body(b"efgh".to_vec())
        .send()
        .await?;
    assert_eq!(finalize.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_delete_manifest_by_digest_removes_tags_but_leaves_blobs()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;

    let blob_bytes = br#"{"architecture":"amd64","os":"linux"}"#;
    let blob_digest = hex::encode(Sha256::digest(blob_bytes));
    let blob_upload = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads?digest=sha256:{blob_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .body(blob_bytes.as_slice().to_vec())
        .send()
        .await?;
    assert_eq!(blob_upload.status(), StatusCode::CREATED);

    let manifest_bytes = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{blob_digest}"),
            "size": blob_bytes.len(),
        },
        "layers": [],
    }))?;
    let manifest_digest = hex::encode(Sha256::digest(&manifest_bytes));
    let manifest_put = client
        .put(format!(
            "{}/v2/team/assets/manifests/v1?tag=stable",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes)
        .send()
        .await?;
    assert_eq!(manifest_put.status(), StatusCode::CREATED);

    let delete_by_tag = client
        .delete(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(delete_by_tag.status(), StatusCode::BAD_REQUEST);

    let delete = client
        .delete(format!(
            "{}/v2/team/assets/manifests/sha256:{manifest_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(delete.status(), StatusCode::ACCEPTED);

    let manifest_by_digest = client
        .get(format!(
            "{}/v2/team/assets/manifests/sha256:{manifest_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(manifest_by_digest.status(), StatusCode::NOT_FOUND);

    let manifest_by_tag = client
        .get(format!(
            "{}/v2/team/assets/manifests/v1",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(manifest_by_tag.status(), StatusCode::NOT_FOUND);

    let tags = client
        .get(format!("{}/v2/team/assets/tags/list", runtime.base_url()))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(tags.status(), StatusCode::OK);
    let tags = tags.json::<Value>().await?;
    assert_eq!(tags["tags"], json!([]));

    let blob = client
        .get(format!(
            "{}/v2/team/assets/blobs/sha256:{blob_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(blob.status(), StatusCode::OK);
    assert_eq!(blob.bytes().await?.as_ref(), blob_bytes);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_retagging_preserves_latest_manifest_when_old_digest_is_deleted()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;

    async fn upload_manifest(
        client: &Client,
        runtime: &ProtocolFrontendRuntime,
        write_token: &str,
        blob_bytes: &[u8],
    ) -> Result<String, Box<dyn StdError>> {
        let blob_digest = hex::encode(Sha256::digest(blob_bytes));
        let blob_upload = client
            .post(format!(
                "{}/v2/team/assets/blobs/uploads?digest=sha256:{blob_digest}",
                runtime.base_url()
            ))
            .bearer_auth(write_token)
            .body(blob_bytes.to_vec())
            .send()
            .await?;
        assert_eq!(blob_upload.status(), StatusCode::CREATED);

        let manifest_bytes = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": format!("sha256:{blob_digest}"),
                "size": blob_bytes.len(),
            },
            "layers": [],
        }))?;
        let manifest_digest = hex::encode(Sha256::digest(&manifest_bytes));
        let manifest_put = client
            .put(format!(
                "{}/v2/team/assets/manifests/latest",
                runtime.base_url()
            ))
            .bearer_auth(write_token)
            .header(CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
            .body(manifest_bytes)
            .send()
            .await?;
        assert_eq!(manifest_put.status(), StatusCode::CREATED);

        Ok(manifest_digest)
    }

    let first_digest =
        upload_manifest(&client, &runtime, &write_token, br#"{"version":1}"#).await?;
    let second_digest =
        upload_manifest(&client, &runtime, &write_token, br#"{"version":2}"#).await?;
    assert_ne!(first_digest, second_digest);

    let delete_first = client
        .delete(format!(
            "{}/v2/team/assets/manifests/sha256:{first_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(delete_first.status(), StatusCode::ACCEPTED);

    let latest = client
        .get(format!(
            "{}/v2/team/assets/manifests/latest",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(latest.status(), StatusCode::OK);
    assert_eq!(
        latest
            .headers()
            .get("Docker-Content-Digest")
            .and_then(|value| value.to_str().ok()),
        Some(format!("sha256:{second_digest}").as_str())
    );

    let delete_second = client
        .delete(format!(
            "{}/v2/team/assets/manifests/sha256:{second_digest}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(delete_second.status(), StatusCode::ACCEPTED);

    let missing = client
        .get(format!(
            "{}/v2/team/assets/manifests/latest",
            runtime.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_rejects_manifest_digest_reference_mismatch() -> Result<(), Box<dyn StdError>>
{
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let manifest_bytes = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{}", "2".repeat(64)),
            "size": 0,
        },
        "layers": [],
    }))?;

    let response = client
        .put(format!(
            "{}/v2/team/assets/manifests/sha256:{}",
            runtime.base_url(),
            "1".repeat(64)
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_rejects_cross_scope_upload_session_access() -> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let other_write_token = bearer_token(
        "protocol-user-2",
        TokenScope::Write,
        RepositoryProvider::Generic,
        "team",
        "assets",
        Some("main"),
    )?;

    let start_upload = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(start_upload.status(), StatusCode::ACCEPTED);
    let upload_location = start_upload
        .headers()
        .get(LOCATION)
        .and_then(|value| value.to_str().ok())
        .ok_or("missing upload location")?;
    let upload_url = format!("{}{}", runtime.base_url(), upload_location);

    let patch = client
        .patch(&upload_url)
        .bearer_auth(&other_write_token)
        .body(b"abcd".to_vec())
        .send()
        .await?;
    assert_eq!(patch.status(), StatusCode::NOT_FOUND);

    let status = client
        .get(&upload_url)
        .bearer_auth(&other_write_token)
        .send()
        .await?;
    assert_eq!(status.status(), StatusCode::NOT_FOUND);

    let delete = client
        .delete(&upload_url)
        .bearer_auth(&other_write_token)
        .send()
        .await?;
    assert_eq!(delete.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_rejects_repository_paths_outside_token_scope() -> Result<(), Box<dyn StdError>>
{
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;

    let create_upload = client
        .post(format!(
            "{}/v2/team/other/blobs/uploads",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(create_upload.status(), StatusCode::NOT_FOUND);

    let tags = client
        .get(format!("{}/v2/team/other/tags/list", runtime.base_url()))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(tags.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_rejects_excessive_manifest_tag_aliases() -> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let manifest = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{}", "1".repeat(64)),
            "size": 0,
        },
        "layers": [],
    }))?;
    let query = (0..129)
        .map(|index| format!("tag=t{index}"))
        .collect::<Vec<_>>()
        .join("&");

    let response = client
        .put(format!(
            "{}/v2/team/assets/manifests/v1?{query}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(manifest)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_rejects_oversized_query_strings() -> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime(&[ServerFrontend::Oci]).await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let oversized_from = "a".repeat(16_385);

    let response = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads?mount=sha256:{}&from={oversized_from}",
            runtime.base_url(),
            "1".repeat(64)
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::URI_TOO_LONG);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_rejects_cumulative_chunked_upload_growth_past_limit()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime_with_max_request_body(
        &[ServerFrontend::Oci],
        NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
    )
    .await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;

    let start_upload = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(start_upload.status(), StatusCode::ACCEPTED);
    let upload_location = start_upload
        .headers()
        .get(LOCATION)
        .and_then(|value| value.to_str().ok())
        .ok_or("missing upload location")?;
    let upload_url = format!("{}{}", runtime.base_url(), upload_location);

    let first_patch = client
        .patch(&upload_url)
        .bearer_auth(&write_token)
        .header(CONTENT_RANGE, "0-7")
        .body(b"abcdefgh".to_vec())
        .send()
        .await?;
    assert_eq!(first_patch.status(), StatusCode::ACCEPTED);

    let second_patch = client
        .patch(&upload_url)
        .bearer_auth(&write_token)
        .body(b"i".to_vec())
        .send()
        .await?;
    assert_eq!(second_patch.status(), StatusCode::PAYLOAD_TOO_LARGE);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_mount_copy_is_not_limited_by_request_body_budget()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime_with_max_request_body(
        &[ServerFrontend::Oci],
        NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
    )
    .await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let repository_scope = scoped_repository("team", "assets")?;
    let digest_hex = seed_oci_blob(
        &runtime,
        "team/assets/source",
        &repository_scope,
        b"012345678",
    )?;

    let response = client
        .post(format!(
            "{}/v2/team/assets/target/blobs/uploads?mount=sha256:{digest_hex}&from=team/assets/source",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let expected_digest_header = format!("sha256:{digest_hex}");
    assert_eq!(
        response
            .headers()
            .get("Docker-Content-Digest")
            .and_then(|value| value.to_str().ok()),
        Some(expected_digest_header.as_str())
    );

    let mounted = client
        .get(format!(
            "{}/v2/team/assets/target/blobs/sha256:{digest_hex}",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(mounted.status(), StatusCode::OK);
    assert_eq!(mounted.bytes().await?.as_ref(), b"012345678");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_rejects_excessive_live_upload_sessions() -> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime_with_oci_limits(
        &[ServerFrontend::Oci],
        NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(1).unwrap_or(NonZeroUsize::MIN),
    )
    .await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;

    let first = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(first.status(), StatusCode::ACCEPTED);

    let second = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_reclaims_orphaned_upload_sessions_with_missing_body()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime_with_oci_limits(
        &[ServerFrontend::Oci],
        NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(1).unwrap_or(NonZeroUsize::MIN),
    )
    .await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;

    let first = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(first.status(), StatusCode::ACCEPTED);
    let upload_location = first
        .headers()
        .get(LOCATION)
        .and_then(|value| value.to_str().ok())
        .ok_or("missing upload location")?;
    let session_id = upload_location
        .rsplit('/')
        .next()
        .ok_or("missing upload session id")?;
    fs::remove_file(
        runtime
            .storage_path()
            .join("oci-uploads")
            .join(format!("{session_id}.bin")),
    )?;

    let second = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(second.status(), StatusCode::ACCEPTED);

    let stale = client
        .get(format!("{}{}", runtime.base_url(), upload_location))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(stale.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_expires_idle_upload_sessions() -> Result<(), Box<dyn StdError>> {
    let runtime = start_protocol_runtime_with_oci_limits(
        &[ServerFrontend::Oci],
        NonZeroU64::new(1).unwrap_or(NonZeroU64::MIN),
        NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
    )
    .await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;

    let start_upload = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads",
            runtime.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(start_upload.status(), StatusCode::ACCEPTED);
    let upload_location = start_upload
        .headers()
        .get(LOCATION)
        .and_then(|value| value.to_str().ok())
        .ok_or("missing upload location")?;
    let upload_url = format!("{}{}", runtime.base_url(), upload_location);

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let status = client
        .get(&upload_url)
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(status.status(), StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_shared_root_upload_sessions_work_across_runtime_instances()
-> Result<(), Box<dyn StdError>> {
    let shared_root = tempfile::tempdir()?;
    let first = start_protocol_runtime_on_shared_root(
        &[ServerFrontend::Oci],
        shared_root.path(),
        |config| config,
    )
    .await?;
    let second = start_protocol_runtime_on_shared_root(
        &[ServerFrontend::Oci],
        shared_root.path(),
        |config| config,
    )
    .await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;

    let start_upload = client
        .post(format!("{}/v2/team/assets/blobs/uploads", first.base_url()))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(start_upload.status(), StatusCode::ACCEPTED);
    let upload_location = start_upload
        .headers()
        .get(LOCATION)
        .and_then(|value| value.to_str().ok())
        .ok_or("missing upload location")?;
    let upload_url = format!("{}{}", second.base_url(), upload_location);

    let patch = client
        .patch(&upload_url)
        .bearer_auth(&write_token)
        .header(CONTENT_RANGE, "0-3")
        .body(b"abcd".to_vec())
        .send()
        .await?;
    assert_eq!(patch.status(), StatusCode::ACCEPTED);

    let blob_bytes = b"abcdefgh";
    let blob_digest = hex::encode(Sha256::digest(blob_bytes));
    let finalize = client
        .put(format!(
            "{}{}?digest=sha256:{blob_digest}",
            first.base_url(),
            upload_location
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_RANGE, "4-7")
        .body(b"efgh".to_vec())
        .send()
        .await?;
    assert_eq!(finalize.status(), StatusCode::CREATED);

    let blob = client
        .get(format!(
            "{}/v2/team/assets/blobs/sha256:{blob_digest}",
            second.base_url()
        ))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(blob.status(), StatusCode::OK);
    assert_eq!(blob.bytes().await?.as_ref(), blob_bytes);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_shared_root_upload_session_limits_span_runtime_instances()
-> Result<(), Box<dyn StdError>> {
    let shared_root = tempfile::tempdir()?;
    let first = start_protocol_runtime_on_shared_root(
        &[ServerFrontend::Oci],
        shared_root.path(),
        |config| {
            config
                .with_oci_upload_session_ttl_seconds(NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN))
                .with_oci_upload_max_active_sessions(
                    NonZeroUsize::new(1).unwrap_or(NonZeroUsize::MIN),
                )
        },
    )
    .await?;
    let second = start_protocol_runtime_on_shared_root(
        &[ServerFrontend::Oci],
        shared_root.path(),
        |config| {
            config
                .with_oci_upload_session_ttl_seconds(NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN))
                .with_oci_upload_max_active_sessions(
                    NonZeroUsize::new(1).unwrap_or(NonZeroUsize::MIN),
                )
        },
    )
    .await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;

    let first_create = client
        .post(format!("{}/v2/team/assets/blobs/uploads", first.base_url()))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(first_create.status(), StatusCode::ACCEPTED);

    let second_create = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads",
            second.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(second_create.status(), StatusCode::TOO_MANY_REQUESTS);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_frontend_shared_root_manifest_deletes_are_visible_across_runtime_instances()
-> Result<(), Box<dyn StdError>> {
    let shared_root = tempfile::tempdir()?;
    let first = start_protocol_runtime_on_shared_root(
        &[ServerFrontend::Oci],
        shared_root.path(),
        |config| config,
    )
    .await?;
    let second = start_protocol_runtime_on_shared_root(
        &[ServerFrontend::Oci],
        shared_root.path(),
        |config| config,
    )
    .await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write, "team", "assets")?;
    let read_token = scoped_token(TokenScope::Read, "team", "assets")?;

    let blob_bytes = br#"{"architecture":"amd64","os":"linux"}"#;
    let blob_digest = hex::encode(Sha256::digest(blob_bytes));
    let blob_upload = client
        .post(format!(
            "{}/v2/team/assets/blobs/uploads?digest=sha256:{blob_digest}",
            first.base_url()
        ))
        .bearer_auth(&write_token)
        .body(blob_bytes.as_slice().to_vec())
        .send()
        .await?;
    assert_eq!(blob_upload.status(), StatusCode::CREATED);

    let manifest_bytes = serde_json::to_vec(&json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": format!("sha256:{blob_digest}"),
            "size": blob_bytes.len(),
        },
        "layers": [],
    }))?;
    let manifest_digest = hex::encode(Sha256::digest(&manifest_bytes));
    let manifest_put = client
        .put(format!(
            "{}/v2/team/assets/manifests/v1?tag=stable",
            first.base_url()
        ))
        .bearer_auth(&write_token)
        .header(CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes)
        .send()
        .await?;
    assert_eq!(manifest_put.status(), StatusCode::CREATED);

    let visible_on_second = client
        .get(format!("{}/v2/team/assets/manifests/v1", second.base_url()))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(visible_on_second.status(), StatusCode::OK);

    let delete = client
        .delete(format!(
            "{}/v2/team/assets/manifests/sha256:{manifest_digest}",
            second.base_url()
        ))
        .bearer_auth(&write_token)
        .send()
        .await?;
    assert_eq!(delete.status(), StatusCode::ACCEPTED);

    let missing_on_first = client
        .get(format!("{}/v2/team/assets/manifests/v1", first.base_url()))
        .bearer_auth(&read_token)
        .send()
        .await?;
    assert_eq!(missing_on_first.status(), StatusCode::NOT_FOUND);

    Ok(())
}
