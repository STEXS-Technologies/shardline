#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::{
    io::{BufRead, BufReader, ErrorKind, Write},
    net::TcpListener,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use shardline_protocol::SecretString;
use shardline_server_core::{ServerObjectStore, ServerObjectStoreError};
use shardline_storage::S3ObjectStoreConfig;

use super::{
    GC_CLOCK_BOOT_OBSERVATION_KEY, GcBootObservation, GcClockAnchor, LAST_GC_CLOCK_ANCHOR_KEY,
    read_gc_clock_anchor,
};
use crate::GcError;

struct Reply {
    key: &'static str,
    body: Option<Vec<u8>>,
    declared_length: Option<u64>,
}

fn reply(key: &'static str, body: &[u8]) -> Reply {
    Reply {
        key,
        body: Some(body.to_vec()),
        declared_length: None,
    }
}

fn s3_clock_fixture(replies: Vec<Reply>) -> (ServerObjectStore, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let server = thread::spawn(move || {
        for reply in replies {
            let deadline = Instant::now().checked_add(Duration::from_secs(10)).unwrap();
            let mut connection = loop {
                match listener.accept() {
                    Ok((connection, _peer)) => break connection,
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "missing clock-record request");
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("clock fixture accept failed: {error}"),
                }
            };
            connection
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut reader = BufReader::new(connection.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            assert_eq!(line, format!("GET /fixture/{} HTTP/1.1\r\n", reply.key));
            loop {
                line.clear();
                assert!(reader.read_line(&mut line).unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
                assert!(!line.to_ascii_lowercase().starts_with("range:"));
            }
            if let Some(body) = reply.body {
                let length = reply.declared_length.unwrap_or(body.len() as u64);
                write!(
                    connection,
                    "HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nETag: \"fixture\"\r\nLast-Modified: Wed, 01 Oct 2025 00:00:00 GMT\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                // Oversized-response tests can close before consuming a body.
                let _write_result = connection.write_all(&body);
            } else {
                connection
                    .write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
            }
        }
    });
    let config = S3ObjectStoreConfig::new("fixture".to_owned(), "us-east-1".to_owned())
        .with_endpoint(Some(format!("http://{address}")))
        .with_allow_http(true)
        .with_credentials(
            Some(SecretString::from_secret("fixture")),
            Some(SecretString::from_secret("fixture")),
            None,
        );
    (ServerObjectStore::s3(config).unwrap(), server)
}

#[test]
fn clock_records_use_two_full_gets_without_head_requests() {
    let boot = GcBootObservation {
        boot_id: "fixture-boot".to_owned(),
        uptime_seconds: 100,
    };
    let sidecar = serde_json::to_vec(&GcClockAnchor {
        wall_seconds: 2_000_000_000,
        boot: Some(boot.clone()),
    })
    .unwrap();
    let (store, server) = s3_clock_fixture(vec![
        reply(LAST_GC_CLOCK_ANCHOR_KEY, b"2000000000"),
        reply(GC_CLOCK_BOOT_OBSERVATION_KEY, &sidecar),
    ]);
    let result = read_gc_clock_anchor(&store);
    server.join().unwrap();
    let anchor = result.unwrap().unwrap();
    assert_eq!(anchor.wall_seconds, 2_000_000_000);
    assert_eq!(anchor.boot, Some(boot));
}

#[test]
fn missing_clock_anchor_uses_one_get_and_does_not_read_sidecar() {
    let (store, server) = s3_clock_fixture(vec![Reply {
        key: LAST_GC_CLOCK_ANCHOR_KEY,
        body: None,
        declared_length: None,
    }]);
    let result = read_gc_clock_anchor(&store);
    server.join().unwrap();
    assert!(result.unwrap().is_none());
}

#[test]
fn truncated_clock_anchor_propagates_storage_failure() {
    let (store, server) = s3_clock_fixture(vec![Reply {
        declared_length: Some(20),
        ..reply(LAST_GC_CLOCK_ANCHOR_KEY, b"2000000000")
    }]);
    let result = read_gc_clock_anchor(&store);
    server.join().unwrap();
    assert!(matches!(result, Err(GcError::ObjectStore(_))));
}

#[test]
fn oversized_clock_anchor_preserves_the_existing_overflow_error() {
    let (store, server) = s3_clock_fixture(vec![Reply {
        declared_length: Some(64 * 1024 * 1024 + 1),
        ..reply(LAST_GC_CLOCK_ANCHOR_KEY, b"2000000000")
    }]);
    let result = read_gc_clock_anchor(&store);
    server.join().unwrap();
    assert!(matches!(
        result,
        Err(GcError::ObjectStore(ServerObjectStoreError::Overflow))
    ));
}

#[test]
fn truncated_sidecar_preserves_the_scalar_without_elapsed_time_trust() {
    let (store, server) = s3_clock_fixture(vec![
        reply(LAST_GC_CLOCK_ANCHOR_KEY, b"2000000000"),
        Reply {
            declared_length: Some(20),
            ..reply(GC_CLOCK_BOOT_OBSERVATION_KEY, b"short")
        },
    ]);
    let result = read_gc_clock_anchor(&store);
    server.join().unwrap();
    let anchor = result.unwrap().unwrap();
    assert_eq!(anchor.wall_seconds, 2_000_000_000);
    assert!(anchor.boot.is_none());
}
