//! Provider responses cannot change the byte budget of a requested range.
use super::*;
use crate::AsyncObjectStore;
use std::io::{Read, Write};
use std::time::{Duration, Instant};

#[tokio::test]
async fn ranged_get_rejects_short_and_oversized_bodies() {
    exercise(false).await;
}

#[tokio::test]
async fn suffix_get_uses_one_request_and_rejects_short_and_oversized_bodies() {
    exercise(true).await;
}

#[tokio::test]
async fn suffix_distinguishes_missing_forbidden_and_zero_length_reads() {
    for (status, length) in [(404, 0), (404, 3), (403, 0), (403, 3), (200, 0)] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let deadline = Instant::now().checked_add(Duration::from_secs(5)).unwrap();
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => return Err(error),
                }
            };
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut buffer).unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buffer[..read]);
            }
            let method: &[u8] = if length == 0 { b"HEAD " } else { b"GET " };
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

async fn exercise(suffix: bool) {
    for length in [3_usize, 2, 4] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let deadline = Instant::now().checked_add(Duration::from_secs(5)).unwrap();
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => return Err(error),
                }
            };
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut buffer).unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buffer[..read]);
            }
            assert!(request.starts_with(b"GET /assets/chunks/range-budget "));
            let headers = String::from_utf8(request).unwrap().to_ascii_lowercase();
            let (requested, returned) = if suffix {
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
        let result = if suffix {
            AsyncObjectStore::read_suffix(&store, &key, 3)
                .await
                .map(|found| {
                    let (bytes, total) = found.expect("source exists");
                    assert_eq!(total, 10);
                    bytes
                })
        } else {
            AsyncObjectStore::read_range(&store, &key, ByteRange::new(0, 2).unwrap()).await
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
