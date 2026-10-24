//! Provider responses cannot change the byte budget of a requested range.
use super::*;
use crate::AsyncObjectStore;
use std::io::{Read, Write};
use std::time::{Duration, Instant};

#[tokio::test]
async fn ranged_get_rejects_short_and_oversized_bodies() {
    for length in [3_usize, 2, 4] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
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
            assert!(headers.contains("range: bytes=0-2\r\n"));
            write!(socket, "HTTP/1.1 206 Partial Content\r\nContent-Length: {length}\r\nContent-Range: bytes 0-2/10\r\nETag: \"fixture\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n").unwrap();
            socket.write_all(&vec![b'x'; length]).unwrap();
            Ok::<(), std::io::Error>(())
        });
        let store = synchronous_runtime_fixture(format!("http://{address}"));
        let key = ObjectKey::parse("chunks/range-budget").unwrap();
        let result =
            AsyncObjectStore::read_range(&store, &key, ByteRange::new(0, 2).unwrap()).await;
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
