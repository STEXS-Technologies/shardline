//! Exercise payload ownership through the actual S3 HTTP transport, without
//! conflating transport cancellation with durable provider acknowledgement.
use std::{io::Read as _, net::TcpListener, time::Duration};

use bytes::Bytes;
use shardline_protocol::{SecretString, ShardlineHash};
use tokio::sync::Semaphore;

use super::{S3ObjectStore, S3ObjectStoreConfig};
use crate::{AsyncObjectStore as _, ObjectBody, ObjectIntegrity, ObjectKey};

struct AdmittedPayload {
    bytes: Vec<u8>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl AsRef<[u8]> for AdmittedPayload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_s3_upload_retains_owner_under_network_backpressure() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (release, stop_reading) = std::sync::mpsc::channel();
    let (started, headers_received) = tokio::sync::oneshot::channel();
    let peer = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut header = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !header.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
            let read = socket.read(&mut buffer).unwrap();
            assert!(read > 0);
            header.extend_from_slice(&buffer[..read]);
            assert!(header.len() < 64 * 1024);
        }
        assert!(header.starts_with(b"PUT "));
        started.send(()).unwrap();
        // Do not consume the body: a large payload cannot disappear into a
        // successful response while the ownership assertion runs.
        let _release = stop_reading.recv_timeout(Duration::from_secs(5));
        drop(socket);
    });
    let store = S3ObjectStore::new(
        S3ObjectStoreConfig::new("fixture".to_owned(), "us-east-1".to_owned())
            .with_endpoint(Some(format!("http://{address}")))
            .with_allow_http(true)
            .with_credentials(
                Some(SecretString::from_secret("fixture-access")),
                Some(SecretString::from_secret("fixture-secret")),
                None,
            ),
    )
    .unwrap();
    let permits = std::sync::Arc::new(Semaphore::new(1));
    let payload = vec![0x5a; 32 * 1024 * 1024];
    let integrity = ObjectIntegrity::new(
        ShardlineHash::from_bytes(*blake3::hash(&payload).as_bytes()),
        payload.len() as u64,
    );
    let bytes = Bytes::from_owner(AdmittedPayload {
        bytes: payload,
        _permit: permits.clone().acquire_owned().await.unwrap(),
    });
    let writer = tokio::spawn(async move {
        store
            .put_if_absent(
                &ObjectKey::parse("admitted").unwrap(),
                ObjectBody::from_bytes(bytes),
                &integrity,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), headers_received)
        .await
        .unwrap()
        .unwrap();
    assert!(
        permits.try_acquire().is_err(),
        "transport must retain original payload ownership"
    );
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    // A cancelled write can still have reached the provider. Require bounded
    // resource cleanup, never infer that cancellation undid a remote effect.
    release.send(()).unwrap();
    peer.join().unwrap();
    let _released = tokio::time::timeout(Duration::from_secs(5), permits.acquire())
        .await
        .unwrap()
        .unwrap();
}
