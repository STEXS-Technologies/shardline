use std::{
    io::{Read, Write},
    net::TcpListener,
    time::Duration,
};

use shardline_protocol::SecretString;

use super::{S3ObjectStore, S3ObjectStoreConfig};

#[test]
#[allow(clippy::panic)]
fn temporary_cleanup_lists_only_reserved_subtree_within_tenant() {
    for tenant in [None, Some("tenant-a")] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(std::time::Instant::now() < deadline);
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("accept failed: {error}"),
                }
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            while !request.windows(4).any(|value| value == b"\r\n\r\n") {
                let length = stream.read(&mut buffer).unwrap();
                assert!(length > 0);
                request.extend_from_slice(&buffer[..length]);
            }
            let body = "<ListBucketResult><Name>fixture</Name><IsTruncated>false</IsTruncated></ListBucketResult>";
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
            String::from_utf8(request).unwrap()
        });
        let store = S3ObjectStore::new(
            S3ObjectStoreConfig::new("fixture".to_owned(), "us-east-1".to_owned())
                .with_endpoint(Some(format!("http://{address}")))
                .with_allow_http(true)
                .with_key_prefix(tenant)
                .with_credentials(
                    Some(SecretString::from_secret("fixture-access")),
                    Some(SecretString::from_secret("fixture-secret")),
                    None,
                ),
        )
        .unwrap();
        assert_eq!(store.sweep_stale_temp_keys(0).unwrap(), (0, 0));
        let request = worker.join().unwrap();
        let target = request
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap();
        let prefix = target
            .split_once('?')
            .unwrap()
            .1
            .split('&')
            .find_map(|part| part.strip_prefix("prefix="))
            .unwrap();
        let expected =
            tenant.map_or_else(|| "__tmp/".to_owned(), |value| format!("{value}/__tmp/"));
        assert_eq!(prefix.replace("%2F", "/").replace("%2f", "/"), expected);
    }
}
