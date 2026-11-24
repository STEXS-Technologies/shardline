//! Provider responses cannot change the byte budget of a requested range.
use super::*;
use crate::{AsyncObjectStore, ObjectStore};
use std::io::{Read, Write};
use std::time::{Duration, Instant};

#[tokio::test]
async fn ranged_get_rejects_short_and_oversized_bodies() {
    exercise(ReadMode::Asynchronous).await;
}

#[tokio::test]
async fn synchronous_ranged_get_rejects_short_and_oversized_bodies() {
    exercise(ReadMode::Synchronous).await;
}

#[tokio::test]
async fn suffix_get_uses_one_request_and_rejects_short_and_oversized_bodies() {
    exercise(ReadMode::Suffix).await;
}

#[tokio::test]
async fn suffix_distinguishes_missing_forbidden_and_zero_length_reads() {
    for (status, length) in [(404, 0), (404, 3), (403, 0), (403, 3), (200, 0)] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let (mut socket, request) = receive_single_request(&listener)?;
            let method = if length == 0 { "head " } else { "get " };
            assert!(request.starts_with(method));
            let size = if status == 200 { 10 } else { 0 };
            write!(
                socket,
                "HTTP/1.1 {status} Fixture\r\nContent-Length: {size}\r\nETag: \"fixture\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n"
            )?;
            Ok::<(), std::io::Error>(())
        });
        let store = synchronous_runtime_fixture(format!("http://{address}"));
        let key = ObjectKey::parse("chunks/suffix-status").unwrap();
        let result = AsyncObjectStore::read_suffix(&store, &key, length).await;
        server.join().unwrap().unwrap();
        match status {
            404 => assert!(result.unwrap().is_none()),
            200 => assert_eq!(result.unwrap(), Some((Vec::new(), 10))),
            _ => assert!(result.is_err()),
        }
    }
}

#[derive(Clone, Copy)]
enum ReadMode {
    Synchronous,
    Asynchronous,
    Suffix,
}

fn receive_single_request(
    listener: &std::net::TcpListener,
) -> Result<(std::net::TcpStream, String), std::io::Error> {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .ok_or_else(|| std::io::Error::other("request deadline overflow"))?;
    let mut socket = loop {
        match listener.accept() {
            Ok((socket, _)) => break socket,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(error),
        }
    };
    socket.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = socket.read(&mut buffer)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "incomplete HTTP request",
            ));
        }
        request.extend_from_slice(&buffer[..read]);
    }
    let request = String::from_utf8(request)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?
        .to_ascii_lowercase();
    Ok((socket, request))
}

#[tokio::test]
async fn snapshot_ranges_bind_every_get_to_the_original_generation() {
    // The provider deliberately ignores If-Match in several cases. Client-side
    // metadata and exact-body validation must still reject the response.
    for (etag, version, total, returned_range, body, accepted) in [
        ("original", "v1", 10, "0-2/10", "abc", true),
        ("changed", "v1", 10, "0-2/10", "abc", false),
        ("original", "v2", 10, "0-2/10", "abc", false),
        ("original", "v1", 11, "0-2/11", "abc", false),
        ("original", "v1", 10, "1-3/10", "abc", false),
        ("original", "v1", 10, "0-2/10", "ab", false),
        ("original", "v1", 10, "0-2/10", "abcd", false),
    ] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let provider = std::thread::spawn(move || {
            for step in 0..2 {
                let (mut socket, request) = receive_single_request(&listener)?;
                if step == 0 {
                    assert!(request.contains("range: bytes=-3\r\n"));
                    write!(socket, "HTTP/1.1 206 Partial Content\r\nContent-Length: 3\r\nContent-Range: bytes 7-9/10\r\nETag: \"original\"\r\nx-amz-version-id: v1\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\nxyz").unwrap();
                } else {
                    assert!(request.contains("range: bytes=0-2\r\n"));
                    assert!(request.contains("if-match: \"original\"\r\n"));
                    assert!(request.lines().next().unwrap().contains("versionid=v1"));
                    write!(socket, "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {returned_range}\r\nETag: \"{etag}\"\r\nx-amz-version-id: {version}\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
                }
            }
            Ok::<(), std::io::Error>(())
        });
        let store = synchronous_runtime_fixture(format!("http://{address}"));
        let key = ObjectKey::parse("chunks/snapshot").unwrap();
        let (suffix, snapshot) = store.read_suffix_snapshot(&key, 3).await.unwrap().unwrap();
        assert_eq!(suffix, b"xyz");
        assert_eq!(snapshot.length(), 10);
        // A rejected local range must not add another provider request.
        assert!(
            snapshot
                .read_range(ByteRange::new(9, 10).unwrap())
                .await
                .is_err()
        );
        let result = snapshot.read_range(ByteRange::new(0, 2).unwrap()).await;
        provider.join().unwrap().unwrap();
        assert_eq!(
            result.is_ok(),
            accepted,
            "etag={etag}, version={version}, total={total}, range={returned_range}, body={body}"
        );
        if accepted {
            assert_eq!(result.unwrap(), b"abc");
        }
    }
}

#[tokio::test]
async fn snapshot_suffix_rejects_missing_weak_wildcard_and_unquoted_etags() {
    for etag in [None, Some("W/\"weak\""), Some("*"), Some("unquoted")] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let provider = std::thread::spawn(move || {
            let (mut socket, request) = receive_single_request(&listener)?;
            assert!(request.contains("range: bytes=-3\r\n"));
            let etag = etag.map_or_else(String::new, |value| format!("ETag: {value}\r\n"));
            write!(
                socket,
                "HTTP/1.1 206 Partial Content\r\nContent-Length: 3\r\nContent-Range: bytes 7-9/10\r\n{etag}Last-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\nxyz"
            )?;
            Ok::<(), std::io::Error>(())
        });
        let store = synchronous_runtime_fixture(format!("http://{address}"));
        let key = ObjectKey::parse("chunks/no-strong-etag").unwrap();
        let result = store.read_suffix_snapshot(&key, 3).await;
        provider.join().unwrap().unwrap();
        assert!(
            matches!(result, Err(S3ObjectStoreError::SnapshotIdentityUnavailable)),
            "etag={etag:?}"
        );
    }
}

async fn exercise(mode: ReadMode) {
    for length in [3_usize, 2, 4] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let (mut socket, headers) = receive_single_request(&listener)?;
            assert!(headers.starts_with("get /assets/chunks/range-budget "));
            let (requested, returned) = if matches!(mode, ReadMode::Suffix) {
                ("bytes=-3", "7-9/10")
            } else {
                ("bytes=0-2", "0-2/10")
            };
            assert!(headers.contains(&format!("range: {requested}\r\n")));
            write!(socket, "HTTP/1.1 206 Partial Content\r\nContent-Length: {length}\r\nContent-Range: bytes {returned}\r\nETag: \"fixture\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n").unwrap();
            socket.write_all(&vec![b'x'; length]).unwrap();
            Ok::<(), std::io::Error>(())
        });
        let store = synchronous_runtime_fixture(format!("http://{address}"));
        let key = ObjectKey::parse("chunks/range-budget").unwrap();
        let result = match mode {
            ReadMode::Synchronous => {
                ObjectStore::read_range(&store, &key, ByteRange::new(0, 2).unwrap())
            }
            ReadMode::Suffix => AsyncObjectStore::read_suffix(&store, &key, 3)
                .await
                .map(|found| {
                    let (bytes, total) = found.expect("source exists");
                    assert_eq!(total, 10);
                    bytes
                }),
            ReadMode::Asynchronous => {
                AsyncObjectStore::read_range(&store, &key, ByteRange::new(0, 2).unwrap()).await
            }
        };
        server.join().unwrap().unwrap();
        if length == 3 {
            assert_eq!(result.unwrap(), b"xxx");
        } else {
            assert!(
                result.is_err(),
                "provider returned {length} bytes for a three-byte range"
            );
        }
    }
}
