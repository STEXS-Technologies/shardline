//! Request-level coverage for reusing a fully verified source observation.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use shardline_index::{
    FileChunkRecord, FileRecord, MemoryIndexStore, MemoryRecordStore, RecordMutation,
    StorageRepresentation,
};
use shardline_protocol::SecretString;
use shardline_storage::S3ObjectStoreConfig;
use shardline_xet_core::xorb_object::{
    CompressionScheme, SerializedXorbObject, XorbObject,
    xorb_format_test_utils::{ChunkSize, build_raw_xorb},
};

use super::*;

#[test]
fn packed_reachability_reuses_verified_get_and_preserves_missing_and_error_results() {
    assert_verified_reachability(3, 256);
}

#[test]
fn spilled_packed_reachability_rejects_corruption_before_exposing_members() {
    // Exceed the fixed in-memory verification budget, exercising the same
    // source checks after the remote body spills to private scratch storage.
    assert_verified_reachability(48, 64 * 1024);
}

fn assert_verified_reachability(num_chunks: u32, chunk_size: u32) {
    let serialized = SerializedXorbObject::from_xorb_with_compression(
        build_raw_xorb(num_chunks, ChunkSize::Fixed(chunk_size)),
        CompressionScheme::None,
        true,
    )
    .unwrap();
    let hash = serialized.hash.hex();
    let key = shardline_xet_adapter::xorb_object_key(&hash).unwrap();
    let info =
        XorbObject::deserialize(&mut std::io::Cursor::new(&serialized.serialized_data)).unwrap();
    let record = FileRecord {
        file_id: "packed-file".to_owned(),
        content_hash: hash.clone(),
        total_bytes: serialized.raw_num_bytes,
        chunk_size: serialized.raw_num_bytes,
        storage_repr: StorageRepresentation::XorbCdcV1,
        repository_scope: None,
        chunks: vec![FileChunkRecord {
            hash,
            offset: 0,
            length: serialized.raw_num_bytes,
            range_start: 0,
            range_end: serialized.num_chunks as u64,
            packed_start: 0,
            packed_end: serialized.footer_start.unwrap(),
        }],
    };
    record.validate_reconstruction_plan().unwrap();

    for (status, damaged) in [(200, false), (404, false), (403, false), (200, true)] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let server_stop = Arc::clone(&stop);
        let source_path = format!("/assets/{}", key.as_str());
        let mut body = serialized.serialized_data.clone();
        if damaged {
            // Damage the final member so an implementation cannot publish
            // earlier members before it discovers a late validation failure.
            let payload_end = usize::try_from(serialized.footer_start.unwrap()).unwrap();
            *body.get_mut(payload_end.checked_sub(1).unwrap()).unwrap() ^= 1;
        }
        let server = std::thread::spawn(move || {
            let deadline = Instant::now().checked_add(Duration::from_secs(10)).unwrap();
            let mut source_heads = 0_usize;
            let mut source_gets = 0_usize;
            let mut sidecar_heads = 0_usize;
            while !server_stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                let mut stream = match listener.accept() {
                    Ok((stream, _)) => stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let count = stream.read(&mut buffer).unwrap();
                    assert!(
                        count > 0 && request.len() < 16384,
                        "bounded HTTP request headers"
                    );
                    request.extend_from_slice(buffer.get(..count).unwrap());
                }
                let request = String::from_utf8(request).unwrap();
                let source_get = request.starts_with(&format!("GET {source_path} "));
                let source_head = request.starts_with(&format!("HEAD {source_path} "));
                let sidecar_head = request.starts_with("HEAD /assets/_xorb_chunks/");
                assert!(
                    source_get || source_head || sidecar_head,
                    "unexpected storage request"
                );
                source_gets = source_gets.checked_add(usize::from(source_get)).unwrap();
                source_heads = source_heads.checked_add(usize::from(source_head)).unwrap();
                sidecar_heads = sidecar_heads
                    .checked_add(usize::from(sidecar_head))
                    .unwrap();
                if source_get && status == 200 {
                    write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"fixture\"\r\nLast-Modified: Mon, 12 Oct 2026 00:00:00 GMT\r\nConnection: close\r\n\r\n", body.len()).unwrap();
                    stream.write_all(&body).unwrap();
                } else {
                    let status = if sidecar_head { 404 } else { status };
                    let reason = if status == 403 {
                        "Forbidden"
                    } else {
                        "Not Found"
                    };
                    write!(stream, "HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                }
            }
            Ok((source_heads, source_gets, sidecar_heads))
        });
        let objects = ServerObjectStore::s3(
            S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
                .with_endpoint(Some(format!("http://{address}")))
                .with_allow_http(true)
                .with_credentials(
                    Some(SecretString::from_secret("fixture-access")),
                    Some(SecretString::from_secret("fixture-secret")),
                    None,
                ),
        )
        .unwrap();
        let records = MemoryRecordStore::new();
        let mut reachability = ReachabilityAccumulator::default();
        let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
            records.write_version_record(&record).await.unwrap();
            records.write_latest_record(&record).await.unwrap();
            collect_referenced_object_keys(
                &records,
                &MemoryIndexStore::new(),
                &objects,
                &[ServerFrontend::Xet],
                &mut reachability,
            )
            .await
        });
        stop.store(true, Ordering::Relaxed);
        let (heads, gets, sidecars) = server.join().unwrap().unwrap();
        assert_eq!(heads, 0, "full verified GET replaces the source HEAD");
        assert_eq!(
            gets, 1,
            "latest and immutable history share one source observation"
        );
        if status == 403 || damaged {
            assert!(
                result.is_err(),
                "unreadable or corrupt sources must stop collection"
            );
            for member in &info.info.chunk_hashes {
                assert!(
                    !reachability
                        .live_dedupe_chunk_hashes
                        .contains(&member.hex()),
                    "no unverified members escape"
                );
            }
        } else {
            result.unwrap();
            assert_eq!(
                sidecars, 1,
                "unchanged optional sidecar probe is shared across history"
            );
            assert_eq!(reachability.scanned_records, 2);
            assert_eq!(
                reachability.referenced_object_keys.contains(key.as_str()),
                status == 200
            );
            assert_eq!(
                reachability
                    .missing_optional_object_keys
                    .contains(key.as_str()),
                status == 404
            );
            for member in &info.info.chunk_hashes {
                let member = chunk_object_key(&member.hex()).unwrap();
                assert_eq!(
                    reachability
                        .referenced_object_keys
                        .contains(member.as_str()),
                    status == 200
                );
            }
        }
    }
}
