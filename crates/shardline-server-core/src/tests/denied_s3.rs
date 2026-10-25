//! An owned loopback denial, independent of AWS credentials and connectivity.
use super::*;
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

pub(super) struct DeniedS3 {
    pub(super) store: ServerObjectStore,
    stopped: Arc<AtomicBool>,
    server: Option<JoinHandle<()>>,
}

impl DeniedS3 {
    pub(super) fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let stopped = Arc::new(AtomicBool::new(false));
        let stopping = stopped.clone();
        let server = thread::spawn(move || {
            while !stopping.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut connection, _)) => {
                        connection
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        connection
                            .set_write_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        let mut reader = BufReader::new(connection.try_clone().unwrap());
                        let mut line = String::new();
                        let mut length = 0;
                        loop {
                            line.clear();
                            assert!(reader.read_line(&mut line).unwrap() > 0);
                            if line == "\r\n" {
                                break;
                            }
                            if let Some(value) =
                                line.to_ascii_lowercase().strip_prefix("content-length:")
                            {
                                length = value.trim().parse::<u64>().unwrap();
                            }
                        }
                        std::io::copy(&mut reader.take(length), &mut std::io::sink()).unwrap();
                        connection.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("loopback S3 fixture failed: {error}"),
                }
            }
        });
        let config = S3ObjectStoreConfig::new("test-bucket".to_owned(), "us-east-1".to_owned())
            .with_endpoint(Some(format!("http://{address}")))
            .with_allow_http(true)
            .with_credentials(
                Some(shardline_protocol::SecretString::from_secret("fixture")),
                Some(shardline_protocol::SecretString::from_secret("fixture")),
                None,
            );
        Self {
            store: ServerObjectStore::s3(config).unwrap(),
            stopped,
            server: Some(server),
        }
    }
}

impl Drop for DeniedS3 {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(server) = self.server.take() {
            assert!(server.join().is_ok(), "loopback S3 fixture panicked");
        }
    }
}
