use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use axum::body::{Bytes, to_bytes};
use reqwest::{
    Client, StatusCode,
    header::{CONTENT_TYPE, RANGE},
};
use shardline_index::xet_hash_hex_string;
use shardline_protocol::{RepositoryProvider, TokenScope};
use tokio::{net::TcpListener, spawn, sync::Semaphore, time::timeout};

use super::{
    AppState, STREAM_READ_BUFFER_BYTES, acquire_chunk_transfer_permit, bearer_token, chunk_hash,
    clear_repository_reference_probe_filter, full_byte_stream_response,
    lock_repository_reference_probe_test, repository_reference_probe_count,
    reset_repository_reference_probe_count_for_hash, serve_with_listener, single_chunk_xorb,
    single_file_shard, test_byte_stream, wait_for_health,
};
use crate::{
    FileReconstructionResponse, LocalBackend, ReadyResponse, ServerConfig, ServerRole,
    XorbUploadResponse, app::ProtocolMetrics, backend::ServerBackend,
    reconstruction_cache::ReconstructionCacheService, transfer_limiter::TransferLimiter,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_xorb_and_shard_routes_register_reconstruction() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());
    let write_token = bearer_token(
        "provider-user-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    let read_token = bearer_token(
        "provider-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    assert!(write_token.is_ok());
    assert!(read_token.is_ok());
    let (Ok(write_token), Ok(read_token)) = (write_token, read_token) else {
        server.abort();
        return;
    };

    let (first, first_hash) = single_chunk_xorb(b"aaaa");
    let (second, second_hash) = single_chunk_xorb(b"bbbb");
    let (shard, file_hash) = single_file_shard(&[(b"aaaa", &first_hash), (b"bbbb", &second_hash)]);

    let first_upload = Client::new()
        .post(format!("{base_url}/v1/xorbs/default/{first_hash}"))
        .bearer_auth(&write_token)
        .body(first.clone())
        .send()
        .await;
    let second_upload = Client::new()
        .post(format!("{base_url}/v1/xorbs/default/{second_hash}"))
        .bearer_auth(&write_token)
        .body(second.clone())
        .send()
        .await;

    assert!(first_upload.is_ok());
    assert!(second_upload.is_ok());
    let (Ok(first_upload), Ok(second_upload)) = (first_upload, second_upload) else {
        server.abort();
        return;
    };
    assert!(first_upload.status().is_success());
    assert!(second_upload.status().is_success());
    let first_upload = first_upload.json::<XorbUploadResponse>().await;
    let second_upload = second_upload.json::<XorbUploadResponse>().await;
    assert!(first_upload.is_ok());
    assert!(second_upload.is_ok());

    let shard_upload = Client::new()
        .post(format!("{base_url}/v1/shards"))
        .bearer_auth(&write_token)
        .body(shard.clone())
        .send()
        .await;
    assert!(shard_upload.is_ok());
    let Ok(shard_upload) = shard_upload else {
        server.abort();
        return;
    };
    assert!(shard_upload.status().is_success());

    let reconstruction = Client::new()
        .get(format!("{base_url}/v1/reconstructions/{file_hash}"))
        .bearer_auth(&read_token)
        .send()
        .await;
    assert!(reconstruction.is_ok());
    let Ok(reconstruction) = reconstruction else {
        server.abort();
        return;
    };
    assert!(reconstruction.status().is_success());
    let reconstruction = reconstruction.json::<FileReconstructionResponse>().await;
    assert!(reconstruction.is_ok());
    let Ok(reconstruction) = reconstruction else {
        server.abort();
        return;
    };

    assert_eq!(reconstruction.offset_into_first_range, 0);
    assert_eq!(reconstruction.terms.len(), 2);
    assert_eq!(
        reconstruction
            .terms
            .first()
            .map(|term| term.unpacked_length),
        Some(4)
    );
    assert_eq!(
        reconstruction
            .fetch_info
            .get(&first_hash)
            .and_then(|entries| entries.first())
            .map(|entry| entry.url.clone()),
        Some(format!("{base_url}/transfer/xorb/default/{first_hash}"))
    );
    assert_eq!(
        reconstruction
            .fetch_info
            .get(&first_hash)
            .and_then(|entries| entries.first())
            .map(|entry| entry.url_range.end),
        Some(11)
    );

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_route_applies_requested_range_and_rejects_unsatisfiable_ranges() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let public_base_url = format!("http://{addr}");
    let chunk_size = NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN);
    let config = ServerConfig::new(
        addr,
        public_base_url.clone(),
        storage.path().to_path_buf(),
        chunk_size,
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = public_base_url;
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());
    let write_token = bearer_token(
        "provider-user-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    let read_token = bearer_token(
        "provider-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    assert!(write_token.is_ok());
    assert!(read_token.is_ok());
    let (Ok(write_token), Ok(read_token)) = (write_token, read_token) else {
        server.abort();
        return;
    };

    let (first_xorb, first_hash) = single_chunk_xorb(b"abcd");
    let (second_xorb, second_hash) = single_chunk_xorb(b"efgh");
    let (third_xorb, third_hash) = single_chunk_xorb(b"ijkl");
    for (xorb_body, xorb_hash) in [
        (first_xorb, first_hash.as_str()),
        (second_xorb, second_hash.as_str()),
        (third_xorb, third_hash.as_str()),
    ] {
        let uploaded = Client::new()
            .post(format!("{base_url}/v1/xorbs/default/{xorb_hash}"))
            .bearer_auth(&write_token)
            .body(xorb_body)
            .send()
            .await;
        assert!(uploaded.is_ok());
        let Ok(uploaded) = uploaded else {
            server.abort();
            return;
        };
        assert!(uploaded.status().is_success());
    }
    let (shard_body, file_hash) = single_file_shard(&[
        (b"abcd", &first_hash),
        (b"efgh", &second_hash),
        (b"ijkl", &third_hash),
    ]);
    let uploaded_shard = Client::new()
        .post(format!("{base_url}/v1/shards"))
        .bearer_auth(&write_token)
        .body(shard_body)
        .send()
        .await;
    assert!(uploaded_shard.is_ok());
    let Ok(uploaded_shard) = uploaded_shard else {
        server.abort();
        return;
    };
    assert!(uploaded_shard.status().is_success());

    let ranged = Client::new()
        .get(format!("{base_url}/v1/reconstructions/{file_hash}"))
        .bearer_auth(&read_token)
        .header(RANGE, "bytes=2-9")
        .send()
        .await;
    assert!(ranged.is_ok());
    let Ok(ranged) = ranged else {
        server.abort();
        return;
    };
    assert_eq!(ranged.status(), StatusCode::OK);
    let ranged = ranged.json::<FileReconstructionResponse>().await;
    assert!(ranged.is_ok());
    let Ok(ranged) = ranged else {
        server.abort();
        return;
    };
    assert_eq!(ranged.offset_into_first_range, 2);
    assert_eq!(ranged.terms.len(), 3);

    let unsatisfiable = Client::new()
        .get(format!("{base_url}/v1/reconstructions/{file_hash}"))
        .bearer_auth(&read_token)
        .header(RANGE, "bytes=20-30")
        .send()
        .await;
    assert!(unsatisfiable.is_ok());
    let Ok(unsatisfiable) = unsatisfiable else {
        server.abort();
        return;
    };
    assert_eq!(unsatisfiable.status(), StatusCode::RANGE_NOT_SATISFIABLE);

    let malformed = Client::new()
        .get(format!("{base_url}/v1/reconstructions/{file_hash}"))
        .bearer_auth(&read_token)
        .header(RANGE, "items=0-1")
        .send()
        .await;
    assert!(malformed.is_ok());
    let Ok(malformed) = malformed else {
        server.abort();
        return;
    };
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_hash_path_routes_reject_non_xet_hashes() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());
    let read_token = bearer_token(
        "provider-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    assert!(read_token.is_ok());
    let Ok(read_token) = read_token else {
        server.abort();
        return;
    };

    let reconstruction = Client::new()
        .get(format!("{base_url}/v1/reconstructions/asset.bin"))
        .bearer_auth(&read_token)
        .send()
        .await;
    assert!(reconstruction.is_ok());
    let Ok(reconstruction) = reconstruction else {
        server.abort();
        return;
    };
    assert_eq!(reconstruction.status(), StatusCode::BAD_REQUEST);

    let valid_file_id = "0".repeat(64);
    let invalid_content_hash = Client::new()
        .get(format!(
            "{base_url}/v1/reconstructions/{valid_file_id}?content_hash=asset.bin"
        ))
        .bearer_auth(&read_token)
        .send()
        .await;
    assert!(invalid_content_hash.is_ok());
    let Ok(invalid_content_hash) = invalid_content_hash else {
        server.abort();
        return;
    };
    assert_eq!(invalid_content_hash.status(), StatusCode::BAD_REQUEST);

    let uppercase_content_hash = Client::new()
        .get(format!(
            "{base_url}/v2/reconstructions/{valid_file_id}?content_hash={}",
            "A".repeat(64)
        ))
        .bearer_auth(&read_token)
        .send()
        .await;
    assert!(uppercase_content_hash.is_ok());
    let Ok(uppercase_content_hash) = uppercase_content_hash else {
        server.abort();
        return;
    };
    assert_eq!(uppercase_content_hash.status(), StatusCode::BAD_REQUEST);

    let xorb = Client::new()
        .head(format!("{base_url}/v1/xorbs/default/{}", "A".repeat(64)))
        .bearer_auth(&read_token)
        .send()
        .await;
    assert!(xorb.is_ok());
    let Ok(xorb) = xorb else {
        server.abort();
        return;
    };
    assert_eq!(xorb.status(), StatusCode::BAD_REQUEST);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xorb_transfer_route_requires_range_and_serves_partial_content() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());
    let write_token = bearer_token(
        "provider-user-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    let read_token = bearer_token(
        "provider-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    assert!(write_token.is_ok());
    assert!(read_token.is_ok());
    let (Ok(write_token), Ok(read_token)) = (write_token, read_token) else {
        server.abort();
        return;
    };

    let (body, xorb_hash) = single_chunk_xorb(b"abcdefghijkl");
    let (shard_body, _file_hash) = single_file_shard(&[(b"abcdefghijkl", &xorb_hash)]);
    let upload = Client::new()
        .post(format!("{base_url}/v1/xorbs/default/{xorb_hash}"))
        .bearer_auth(&write_token)
        .body(body.clone())
        .send()
        .await;
    assert!(upload.is_ok());
    let Ok(upload) = upload else {
        server.abort();
        return;
    };
    assert!(upload.status().is_success());
    let missing_hash = "a".repeat(64);

    let unauthorized_existing_head = Client::new()
        .head(format!("{base_url}/v1/xorbs/default/{xorb_hash}"))
        .send()
        .await;
    assert!(unauthorized_existing_head.is_ok());
    let Ok(unauthorized_existing_head) = unauthorized_existing_head else {
        server.abort();
        return;
    };
    assert_eq!(
        unauthorized_existing_head.status(),
        StatusCode::UNAUTHORIZED
    );

    let unauthorized_missing_head = Client::new()
        .head(format!("{base_url}/v1/xorbs/default/{missing_hash}"))
        .send()
        .await;
    assert!(unauthorized_missing_head.is_ok());
    let Ok(unauthorized_missing_head) = unauthorized_missing_head else {
        server.abort();
        return;
    };
    assert_eq!(unauthorized_missing_head.status(), StatusCode::UNAUTHORIZED);

    let unauthorized_invalid_head = Client::new()
        .head(format!("{base_url}/v1/xorbs/default/{}", "A".repeat(64)))
        .send()
        .await;
    assert!(unauthorized_invalid_head.is_ok());
    let Ok(unauthorized_invalid_head) = unauthorized_invalid_head else {
        server.abort();
        return;
    };
    assert_eq!(unauthorized_invalid_head.status(), StatusCode::UNAUTHORIZED);

    let unauthorized_existing_transfer = Client::new()
        .get(format!("{base_url}/transfer/xorb/default/{xorb_hash}"))
        .header(RANGE, "bytes=0-0")
        .send()
        .await;
    assert!(unauthorized_existing_transfer.is_ok());
    let Ok(unauthorized_existing_transfer) = unauthorized_existing_transfer else {
        server.abort();
        return;
    };
    assert_eq!(
        unauthorized_existing_transfer.status(),
        StatusCode::UNAUTHORIZED
    );

    let unauthorized_missing_transfer = Client::new()
        .get(format!("{base_url}/transfer/xorb/default/{missing_hash}"))
        .header(RANGE, "bytes=0-0")
        .send()
        .await;
    assert!(unauthorized_missing_transfer.is_ok());
    let Ok(unauthorized_missing_transfer) = unauthorized_missing_transfer else {
        server.abort();
        return;
    };
    assert_eq!(
        unauthorized_missing_transfer.status(),
        StatusCode::UNAUTHORIZED
    );

    let unauthorized_invalid_prefix_transfer = Client::new()
        .get(format!("{base_url}/transfer/xorb/other/{xorb_hash}"))
        .header(RANGE, "bytes=0-0")
        .send()
        .await;
    assert!(unauthorized_invalid_prefix_transfer.is_ok());
    let Ok(unauthorized_invalid_prefix_transfer) = unauthorized_invalid_prefix_transfer else {
        server.abort();
        return;
    };
    assert_eq!(
        unauthorized_invalid_prefix_transfer.status(),
        StatusCode::UNAUTHORIZED
    );

    let unauthorized_invalid_hash_transfer = Client::new()
        .get(format!(
            "{base_url}/transfer/xorb/default/{}",
            "A".repeat(64)
        ))
        .header(RANGE, "bytes=0-0")
        .send()
        .await;
    assert!(unauthorized_invalid_hash_transfer.is_ok());
    let Ok(unauthorized_invalid_hash_transfer) = unauthorized_invalid_hash_transfer else {
        server.abort();
        return;
    };
    assert_eq!(
        unauthorized_invalid_hash_transfer.status(),
        StatusCode::UNAUTHORIZED
    );

    let shard_upload = Client::new()
        .post(format!("{base_url}/v1/shards"))
        .bearer_auth(&write_token)
        .body(shard_body)
        .send()
        .await;
    assert!(shard_upload.is_ok());
    let Ok(shard_upload) = shard_upload else {
        server.abort();
        return;
    };
    assert!(shard_upload.status().is_success());

    let missing_range = Client::new()
        .get(format!("{base_url}/transfer/xorb/default/{xorb_hash}"))
        .bearer_auth(&read_token)
        .send()
        .await;
    assert!(missing_range.is_ok());
    let Ok(missing_range) = missing_range else {
        server.abort();
        return;
    };
    assert_eq!(missing_range.status(), StatusCode::BAD_REQUEST);

    let invalid_prefix = Client::new()
        .get(format!("{base_url}/transfer/xorb/other/{xorb_hash}"))
        .bearer_auth(&read_token)
        .header(RANGE, "bytes=2-7")
        .send()
        .await;
    assert!(invalid_prefix.is_ok());
    let Ok(invalid_prefix) = invalid_prefix else {
        server.abort();
        return;
    };
    assert_eq!(invalid_prefix.status(), StatusCode::BAD_REQUEST);

    let ranged = Client::new()
        .get(format!("{base_url}/transfer/xorb/default/{xorb_hash}"))
        .bearer_auth(&read_token)
        .header(RANGE, "bytes=2-7")
        .send()
        .await;
    assert!(ranged.is_ok());
    let Ok(ranged) = ranged else {
        server.abort();
        return;
    };
    assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
    let accept_ranges = ranged.headers().get("accept-ranges");
    assert_eq!(
        accept_ranges.and_then(|value| value.to_str().ok()),
        Some("bytes")
    );
    let content_range = ranged.headers().get("content-range");
    let expected_content_range = format!("bytes 2-7/{}", body.len());
    assert_eq!(
        content_range.and_then(|value| value.to_str().ok()),
        Some(expected_content_range.as_str())
    );
    let ranged = ranged.bytes().await;
    assert!(ranged.is_ok());
    let Ok(ranged) = ranged else {
        server.abort();
        return;
    };
    assert_eq!(body.get(2..8), Some(ranged.as_ref()));

    let open_ended = Client::new()
        .get(format!("{base_url}/transfer/xorb/default/{xorb_hash}"))
        .bearer_auth(&read_token)
        .header(RANGE, "bytes=2-")
        .send()
        .await;
    assert!(open_ended.is_ok());
    let Ok(open_ended) = open_ended else {
        server.abort();
        return;
    };
    assert_eq!(open_ended.status(), StatusCode::PARTIAL_CONTENT);
    let open_ended_content_range = open_ended.headers().get("content-range");
    let open_ended_expected = format!("bytes 2-{}/{}", body.len() - 1, body.len());
    assert_eq!(
        open_ended_content_range.and_then(|value| value.to_str().ok()),
        Some(open_ended_expected.as_str())
    );
    let open_ended = open_ended.bytes().await;
    assert!(open_ended.is_ok());
    let Ok(open_ended) = open_ended else {
        server.abort();
        return;
    };
    assert_eq!(body.get(2..), Some(open_ended.as_ref()));

    let oversized = Client::new()
        .get(format!("{base_url}/transfer/xorb/default/{xorb_hash}"))
        .bearer_auth(&read_token)
        .header(RANGE, "bytes=2-999999")
        .send()
        .await;
    assert!(oversized.is_ok());
    let Ok(oversized) = oversized else {
        server.abort();
        return;
    };
    assert_eq!(oversized.status(), StatusCode::PARTIAL_CONTENT);
    let oversized_content_range = oversized.headers().get("content-range");
    assert_eq!(
        oversized_content_range.and_then(|value| value.to_str().ok()),
        Some(open_ended_expected.as_str())
    );
    let oversized = oversized.bytes().await;
    assert!(oversized.is_ok());
    let Ok(oversized) = oversized else {
        server.abort();
        return;
    };
    assert_eq!(body.get(2..), Some(oversized.as_ref()));

    let unsatisfiable_header = format!("bytes={}-{}", body.len(), body.len());
    let unsatisfiable = Client::new()
        .get(format!("{base_url}/transfer/xorb/default/{xorb_hash}"))
        .bearer_auth(&read_token)
        .header(RANGE, unsatisfiable_header)
        .send()
        .await;
    assert!(unsatisfiable.is_ok());
    let Ok(unsatisfiable) = unsatisfiable else {
        server.abort();
        return;
    };
    assert_eq!(unsatisfiable.status(), StatusCode::RANGE_NOT_SATISFIABLE);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xorb_routes_reject_missing_hashes_before_repository_reference_scan() {
    let _probe_guard = lock_repository_reference_probe_test().await;
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());
    let read_token = bearer_token(
        "provider-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    assert!(read_token.is_ok());
    let Ok(read_token) = read_token else {
        server.abort();
        return;
    };
    let missing_hash = "a".repeat(64);

    reset_repository_reference_probe_count_for_hash(&missing_hash);
    let head = Client::new()
        .head(format!("{base_url}/v1/xorbs/default/{missing_hash}"))
        .bearer_auth(&read_token)
        .send()
        .await;
    assert!(head.is_ok());
    let Ok(head) = head else {
        server.abort();
        return;
    };
    assert_eq!(head.status(), StatusCode::NOT_FOUND);
    assert_eq!(repository_reference_probe_count(), 0);

    reset_repository_reference_probe_count_for_hash(&missing_hash);
    let transfer = Client::new()
        .get(format!("{base_url}/transfer/xorb/default/{missing_hash}"))
        .bearer_auth(&read_token)
        .header(RANGE, "bytes=0-0")
        .send()
        .await;
    assert!(transfer.is_ok());
    let Ok(transfer) = transfer else {
        server.abort();
        return;
    };
    assert_eq!(transfer.status(), StatusCode::NOT_FOUND);
    assert_eq!(repository_reference_probe_count(), 0);
    clear_repository_reference_probe_filter();

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunk_routes_reject_missing_hashes_before_repository_reference_scan() {
    let _probe_guard = lock_repository_reference_probe_test().await;
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());
    let read_token = bearer_token(
        "provider-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    assert!(read_token.is_ok());
    let Ok(read_token) = read_token else {
        server.abort();
        return;
    };
    let missing_hash = "b".repeat(64);

    reset_repository_reference_probe_count_for_hash(&missing_hash);
    let default_route = Client::new()
        .get(format!("{base_url}/v1/chunks/default/{missing_hash}"))
        .bearer_auth(&read_token)
        .send()
        .await;
    assert!(default_route.is_ok());
    let Ok(default_route) = default_route else {
        server.abort();
        return;
    };
    assert_eq!(default_route.status(), StatusCode::NOT_FOUND);
    assert_eq!(repository_reference_probe_count(), 0);

    reset_repository_reference_probe_count_for_hash(&missing_hash);
    let merkledb_route = Client::new()
        .get(format!(
            "{base_url}/v1/chunks/default-merkledb/{missing_hash}"
        ))
        .bearer_auth(&read_token)
        .send()
        .await;
    assert!(merkledb_route.is_ok());
    let Ok(merkledb_route) = merkledb_route else {
        server.abort();
        return;
    };
    assert_eq!(merkledb_route.status(), StatusCode::NOT_FOUND);
    assert_eq!(repository_reference_probe_count(), 0);
    clear_repository_reference_probe_filter();

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunk_transfer_permit_uses_stored_chunk_length() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let chunk_size = NonZeroUsize::new(4).map_or(NonZeroUsize::MIN, |value| value);
    let backend = LocalBackend::new(
        storage.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
    )
    .await;
    assert!(backend.is_ok());
    let Ok(backend) = backend else {
        return;
    };
    let (body, hash) = single_chunk_xorb(b"abcdefgh");
    let upload = backend.upload_xorb(&hash, body).await;
    assert!(upload.is_ok());
    let reconstruction_cache = ReconstructionCacheService::from_config(&ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://127.0.0.1:8080".to_owned(),
        storage.path().to_path_buf(),
        chunk_size,
    ));
    assert!(reconstruction_cache.is_ok());
    let Ok(reconstruction_cache) = reconstruction_cache else {
        return;
    };
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        "http://127.0.0.1:8080".to_owned(),
        storage.path().to_path_buf(),
        chunk_size,
    );
    let state = AppState {
        config,
        role: ServerRole::All,
        backend: ServerBackend::Local(backend),
        auth: None,
        provider_tokens: None,
        reconstruction_cache,
        transfer_limiter: TransferLimiter::new(chunk_size, NonZeroUsize::MIN),
        oci_registry_token_limiter: Arc::new(Semaphore::new(1)),
        protocol_metrics: ProtocolMetrics::default(),
    };

    let first = acquire_chunk_transfer_permit(&state, &hash).await;
    assert!(first.is_ok());
    let Ok(first) = first else {
        return;
    };

    let blocked = timeout(
        Duration::from_millis(50),
        acquire_chunk_transfer_permit(&state, &hash),
    )
    .await;
    assert!(blocked.is_err());

    drop(first);

    let released = timeout(
        Duration::from_secs(1),
        acquire_chunk_transfer_permit(&state, &hash),
    )
    .await;
    assert!(released.is_ok());
    let Ok(released) = released else {
        return;
    };
    assert!(released.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_transfer_response_streams_exact_expected_bytes() {
    let chunk_size = NonZeroUsize::new(4).map_or(NonZeroUsize::MIN, |value| value);
    let limiter = TransferLimiter::new(chunk_size, NonZeroUsize::MIN);
    let response = full_byte_stream_response(
        test_byte_stream(vec![
            Ok(Bytes::from_static(b"abc")),
            Ok(Bytes::from_static(b"def")),
        ]),
        limiter,
        6,
    );

    let body = to_bytes(response.into_body(), 16).await;
    assert!(body.is_ok());
    let Ok(body) = body else {
        return;
    };
    assert_eq!(body.as_ref(), b"abcdef");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_transfer_response_rejects_short_stream() {
    let chunk_size = NonZeroUsize::new(4).map_or(NonZeroUsize::MIN, |value| value);
    let limiter = TransferLimiter::new(chunk_size, NonZeroUsize::MIN);
    let response = full_byte_stream_response(
        test_byte_stream(vec![Ok(Bytes::from_static(b"abc"))]),
        limiter,
        4,
    );

    let body = to_bytes(response.into_body(), 16).await;
    assert!(body.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_transfer_response_rejects_oversized_frame() {
    let chunk_size = NonZeroUsize::new(4).map_or(NonZeroUsize::MIN, |value| value);
    let limiter = TransferLimiter::new(chunk_size, NonZeroUsize::MIN);
    let response = full_byte_stream_response(
        test_byte_stream(vec![Ok(Bytes::from_static(b"abcde"))]),
        limiter,
        4,
    );

    let body = to_bytes(response.into_body(), 16).await;
    assert!(body.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_transfer_response_accepts_frame_larger_than_stream_buffer() {
    let chunk_size = NonZeroUsize::new(4).map_or(NonZeroUsize::MIN, |value| value);
    let limiter = TransferLimiter::new(chunk_size, NonZeroUsize::MIN);
    let total_length_u64 = STREAM_READ_BUFFER_BYTES.saturating_add(1);
    let total_length = usize::try_from(total_length_u64).unwrap_or(0);
    let response = full_byte_stream_response(
        test_byte_stream(vec![Ok(Bytes::from(vec![b'x'; total_length]))]),
        limiter,
        total_length_u64,
    );

    let body = to_bytes(response.into_body(), total_length.saturating_add(16)).await;
    assert!(body.is_ok());
    let Ok(body) = body else {
        return;
    };
    assert_eq!(body.len(), total_length);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_route_boots_with_postgres_metadata_config() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_index_postgres_url("postgres://shardline:change-me@localhost:5432/shardline".to_owned())
    .and_then(|config| config.with_token_signing_key(b"signing-key".to_vec()));
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");

    let response = wait_for_health(&base_url).await;
    assert!(response.is_ok());

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readiness_route_reports_local_backend_for_initialized_storage() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());

    let response = Client::new().get(format!("{base_url}/readyz")).send().await;
    assert!(response.is_ok());
    let Ok(response) = response else {
        server.abort();
        return;
    };
    assert_eq!(response.status(), StatusCode::OK);
    let ready = response.json::<ReadyResponse>().await;
    assert!(ready.is_ok());
    let Ok(ready) = ready else {
        server.abort();
        return;
    };
    assert_eq!(ready.status, "ok");
    assert_eq!(ready.server_role, "all");
    assert_eq!(ready.server_frontends, vec!["xet".to_owned()]);
    assert_eq!(ready.metadata_backend, "local");
    assert_eq!(ready.object_backend, "local");
    assert_eq!(ready.cache_backend, "memory");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_route_reports_runtime_configuration() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).unwrap_or(NonZeroUsize::MIN),
    )
    .with_server_role(ServerRole::Api)
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());

    let response = Client::new()
        .get(format!("{base_url}/metrics"))
        .send()
        .await;
    assert!(response.is_ok());
    let Ok(response) = response else {
        server.abort();
        return;
    };
    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response.headers().get(CONTENT_TYPE);
    assert!(content_type.is_some());
    let content_type = content_type.and_then(|value| value.to_str().ok());
    assert_eq!(
        content_type,
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    let body = response.text().await;
    assert!(body.is_ok());
    let Ok(body) = body else {
        server.abort();
        return;
    };
    assert!(body.contains("shardline_up 1"));
    assert!(
            body.contains(
                "shardline_server_info{role=\"api\",frontends=\"xet\",metadata_backend=\"local\",object_backend=\"local\",cache_backend=\"memory\"} 1"
            )
        );
    assert!(body.contains("shardline_auth_enabled 1"));
    assert!(body.contains("shardline_provider_tokens_enabled 0"));
    assert!(body.contains("shardline_metrics_auth_enabled 0"));
    assert!(body.contains("shardline_oci_registry_token_ttl_seconds 300"));
    assert!(body.contains("shardline_oci_registry_token_max_in_flight_requests 64"));
    assert!(body.contains("shardline_oci_registry_token_requests_total 0"));
    assert!(body.contains("shardline_oci_registry_token_rate_limited_total 0"));
    assert!(body.contains("shardline_oci_registry_token_active_requests 0"));

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_route_can_require_static_bearer_token() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(65_536).unwrap_or(NonZeroUsize::MIN),
    )
    .with_server_role(ServerRole::Api)
    .with_token_signing_key(b"signing-key".to_vec())
    .and_then(|config| config.with_metrics_token(b"metrics-token".to_vec()));
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());

    let client = Client::new();
    let missing = client.get(format!("{base_url}/metrics")).send().await;
    assert!(missing.is_ok());
    let Ok(missing) = missing else {
        server.abort();
        return;
    };
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    let accepted = client
        .get(format!("{base_url}/metrics"))
        .bearer_auth("metrics-token")
        .send()
        .await;
    assert!(accepted.is_ok());
    let Ok(accepted) = accepted else {
        server.abort();
        return;
    };
    assert_eq!(accepted.status(), StatusCode::OK);
    let body = accepted.text().await;
    assert!(body.is_ok());
    let Ok(body) = body else {
        server.abort();
        return;
    };
    assert!(body.contains("shardline_metrics_auth_enabled 1"));

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_route_rejects_missing_xorbs() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());
    let write_token = bearer_token(
        "provider-user-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    assert!(write_token.is_ok());
    let Ok(write_token) = write_token else {
        server.abort();
        return;
    };

    let (_missing_xorb, missing_hash) = single_chunk_xorb(b"missing");
    let (shard, _file_hash) = single_file_shard(&[(b"missing", &missing_hash)]);
    let response = Client::new()
        .post(format!("{base_url}/v1/shards"))
        .bearer_auth(&write_token)
        .body(shard)
        .send()
        .await;

    assert!(response.is_ok());
    let Ok(response) = response else {
        server.abort();
        return;
    };
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routes_require_bearer_token_when_auth_is_enabled() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());

    let response = Client::new()
        .get(format!("{base_url}/v1/reconstructions/asset.bin"))
        .send()
        .await;

    assert!(response.is_ok());
    let Ok(response) = response else {
        server.abort();
        return;
    };
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_routes_reject_read_only_tokens() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());

    let token = bearer_token(
        "provider-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    assert!(token.is_ok());
    let Ok(token) = token else {
        server.abort();
        return;
    };
    let response = Client::new()
        .post(format!(
            "{base_url}/v1/xorbs/default/{}",
            xet_hash_hex_string(chunk_hash(b"aaaa"))
        ))
        .bearer_auth(token)
        .body("aaaa".as_bytes().to_vec())
        .send()
        .await;

    assert!(response.is_ok());
    let Ok(response) = response else {
        server.abort();
        return;
    };
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routes_accept_matching_scope_tokens() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());

    let write_token = bearer_token(
        "provider-user-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    let read_token = bearer_token(
        "provider-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    assert!(write_token.is_ok());
    assert!(read_token.is_ok());
    let (Ok(write_token), Ok(read_token)) = (write_token, read_token) else {
        server.abort();
        return;
    };
    let (xorb_body, xorb_hash) = single_chunk_xorb(b"aaaa");
    let (shard_body, file_hash) = single_file_shard(&[(b"aaaa", &xorb_hash)]);

    let upload = Client::new()
        .post(format!("{base_url}/v1/xorbs/default/{xorb_hash}"))
        .bearer_auth(&write_token)
        .body(xorb_body)
        .send()
        .await;
    assert!(upload.is_ok());
    let Ok(upload) = upload else {
        server.abort();
        return;
    };
    assert!(upload.status().is_success());

    let shard = Client::new()
        .post(format!("{base_url}/v1/shards"))
        .bearer_auth(&write_token)
        .body(shard_body.clone())
        .send()
        .await;
    assert!(shard.is_ok());
    let Ok(shard) = shard else {
        server.abort();
        return;
    };
    assert!(shard.status().is_success());

    let reconstruction = Client::new()
        .get(format!("{base_url}/v1/reconstructions/{file_hash}"))
        .bearer_auth(&read_token)
        .send()
        .await;
    assert!(reconstruction.is_ok());
    let Ok(reconstruction) = reconstruction else {
        server.abort();
        return;
    };
    assert!(reconstruction.status().is_success());

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_rejects_bodies_over_configured_limit() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let max_request_body_bytes = NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN);
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_max_request_body_bytes(max_request_body_bytes)
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());
    let write_token = bearer_token(
        "provider-user-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    assert!(write_token.is_ok());
    let Ok(write_token) = write_token else {
        server.abort();
        return;
    };

    let response = Client::new()
        .post(format!("{base_url}/v1/shards"))
        .bearer_auth(&write_token)
        .body(b"abcde".to_vec())
        .send()
        .await;
    assert!(response.is_ok());
    let Ok(response) = response else {
        server.abort();
        return;
    };
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_route_requires_auth_when_token_auth_is_configured() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());

    let response = Client::new()
        .get(format!("{base_url}/v1/stats"))
        .send()
        .await;
    assert!(response.is_ok());
    let Ok(response) = response else {
        server.abort();
        return;
    };
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_chunk_route_requires_file_version_context() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());

    let write_token = bearer_token(
        "provider-user-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    let read_token = bearer_token(
        "provider-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    );
    let other_read_token = bearer_token(
        "provider-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "other-team",
        "assets",
        Some("main"),
    );
    assert!(write_token.is_ok());
    assert!(read_token.is_ok());
    assert!(other_read_token.is_ok());
    let (Ok(write_token), Ok(read_token), Ok(other_read_token)) =
        (write_token, read_token, other_read_token)
    else {
        server.abort();
        return;
    };
    let (xorb_body, xorb_hash) = single_chunk_xorb(b"aaaa");
    let (shard_body, _file_hash) = single_file_shard(&[(b"aaaa", &xorb_hash)]);
    let missing_hash = "0".repeat(64);

    let missing_context_for_absent_hash = Client::new()
        .get(format!(
            "{base_url}/v1/chunks/default-merkledb/{missing_hash}"
        ))
        .bearer_auth(&read_token)
        .send()
        .await;
    assert!(missing_context_for_absent_hash.is_ok());
    let Ok(missing_context_for_absent_hash) = missing_context_for_absent_hash else {
        server.abort();
        return;
    };
    assert_eq!(
        missing_context_for_absent_hash.status(),
        StatusCode::NOT_FOUND
    );

    let upload = Client::new()
        .post(format!("{base_url}/v1/xorbs/default/{xorb_hash}"))
        .bearer_auth(&write_token)
        .body(xorb_body)
        .send()
        .await;
    assert!(upload.is_ok());
    let shard = Client::new()
        .post(format!("{base_url}/v1/shards"))
        .bearer_auth(&write_token)
        .body(shard_body.clone())
        .send()
        .await;
    assert!(shard.is_ok());

    let chunk = Client::new()
        .get(format!("{base_url}/v1/chunks/default-merkledb/{xorb_hash}"))
        .bearer_auth(&read_token)
        .send()
        .await;
    assert!(chunk.is_ok());
    let Ok(chunk) = chunk else {
        server.abort();
        return;
    };
    assert_eq!(chunk.status(), StatusCode::OK);
    let chunk_body = chunk.bytes().await;
    assert!(chunk_body.is_ok());
    let Ok(chunk_body) = chunk_body else {
        server.abort();
        return;
    };
    assert_eq!(chunk_body, shard_body);

    let alias_chunk = Client::new()
        .get(format!("{base_url}/v1/chunks/default/{xorb_hash}"))
        .bearer_auth(&read_token)
        .send()
        .await;
    assert!(alias_chunk.is_ok());
    let Ok(alias_chunk) = alias_chunk else {
        server.abort();
        return;
    };
    assert_eq!(alias_chunk.status(), StatusCode::OK);
    let alias_chunk_body = alias_chunk.bytes().await;
    assert!(alias_chunk_body.is_ok());
    let Ok(alias_chunk_body) = alias_chunk_body else {
        server.abort();
        return;
    };
    assert_eq!(alias_chunk_body, chunk_body);

    let scoped_miss = Client::new()
        .get(format!("{base_url}/v1/chunks/default/{xorb_hash}"))
        .bearer_auth(&other_read_token)
        .send()
        .await;
    assert!(scoped_miss.is_ok());
    let Ok(scoped_miss) = scoped_miss else {
        server.abort();
        return;
    };
    assert_eq!(scoped_miss.status(), StatusCode::NOT_FOUND);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_transfer_urls_work_and_stay_repository_scoped() {
    let storage = tempfile::tempdir();
    assert!(storage.is_ok());
    let Ok(storage) = storage else {
        return;
    };
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await;
    assert!(listener.is_ok());
    let Ok(listener) = listener else {
        return;
    };
    let addr = listener.local_addr();
    assert!(addr.is_ok());
    let Ok(addr) = addr else {
        return;
    };
    let config = ServerConfig::new(
        addr,
        format!("http://{addr}"),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"signing-key".to_vec());
    assert!(config.is_ok());
    let Ok(config) = config else {
        return;
    };
    let server = spawn(async move { serve_with_listener(config, listener).await });
    let base_url = format!("http://{addr}");
    let health = wait_for_health(&base_url).await;
    assert!(health.is_ok());

    let left_write = bearer_token(
        "provider-user-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "team-a",
        "assets",
        Some("main"),
    );
    let left_read = bearer_token(
        "provider-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team-a",
        "assets",
        Some("main"),
    );
    let right_read = bearer_token(
        "provider-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team-b",
        "assets",
        Some("main"),
    );
    assert!(left_write.is_ok());
    assert!(left_read.is_ok());
    assert!(right_read.is_ok());
    let (Ok(left_write), Ok(left_read), Ok(right_read)) = (left_write, left_read, right_read)
    else {
        server.abort();
        return;
    };
    let (xorb_body, xorb_hash) = single_chunk_xorb(b"aaaa");
    let (shard_body, file_hash) = single_file_shard(&[(b"aaaa", &xorb_hash)]);

    let upload = Client::new()
        .post(format!("{base_url}/v1/xorbs/default/{xorb_hash}"))
        .bearer_auth(&left_write)
        .body(xorb_body.clone())
        .send()
        .await;
    assert!(upload.is_ok());
    let shard = Client::new()
        .post(format!("{base_url}/v1/shards"))
        .bearer_auth(&left_write)
        .body(shard_body)
        .send()
        .await;
    assert!(shard.is_ok());

    let reconstruction = Client::new()
        .get(format!("{base_url}/v1/reconstructions/{file_hash}"))
        .bearer_auth(&left_read)
        .send()
        .await;
    assert!(reconstruction.is_ok());
    let Ok(reconstruction) = reconstruction else {
        server.abort();
        return;
    };
    assert!(reconstruction.status().is_success());
    let reconstruction = reconstruction.json::<FileReconstructionResponse>().await;
    assert!(reconstruction.is_ok());
    let Ok(reconstruction) = reconstruction else {
        server.abort();
        return;
    };
    let fetch_entry = reconstruction
        .fetch_info
        .get(&xorb_hash)
        .and_then(|entries| entries.first())
        .cloned();
    assert!(fetch_entry.is_some());
    let Some(fetch_entry) = fetch_entry else {
        server.abort();
        return;
    };
    assert!(fetch_entry.url.contains("/transfer/xorb/default/"));

    let range_header = format!(
        "bytes={}-{}",
        fetch_entry.url_range.start, fetch_entry.url_range.end
    );
    let left_xorb = Client::new()
        .get(&fetch_entry.url)
        .bearer_auth(&left_read)
        .header(RANGE, &range_header)
        .send()
        .await;
    let right_xorb = Client::new()
        .get(&fetch_entry.url)
        .bearer_auth(&right_read)
        .header(RANGE, &range_header)
        .send()
        .await;
    assert!(left_xorb.is_ok());
    assert!(right_xorb.is_ok());
    let (Ok(left_xorb), Ok(right_xorb)) = (left_xorb, right_xorb) else {
        server.abort();
        return;
    };
    assert_eq!(left_xorb.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(right_xorb.status(), StatusCode::NOT_FOUND);
    let left_xorb = left_xorb.bytes().await;
    assert!(left_xorb.is_ok());
    let Ok(left_xorb) = left_xorb else {
        server.abort();
        return;
    };
    let expected_start = usize::try_from(fetch_entry.url_range.start).unwrap_or(0);
    let expected_end = usize::try_from(fetch_entry.url_range.end).unwrap_or(0);
    let expected_end_exclusive = expected_end.checked_add(1);
    assert!(expected_end_exclusive.is_some());
    let Some(expected_end_exclusive) = expected_end_exclusive else {
        server.abort();
        return;
    };
    assert_eq!(
        xorb_body.get(expected_start..expected_end_exclusive),
        Some(left_xorb.as_ref())
    );

    server.abort();
}
