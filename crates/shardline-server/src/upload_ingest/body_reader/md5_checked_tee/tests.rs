use axum::body::{Body, Bytes};
use futures_util::stream;
use std::num::NonZeroUsize;

use super::*;
use crate::upload_ingest::body_reader::{RequestBodyReader, read_body_to_bytes};

#[tokio::test]
async fn checked_tee_retains_matching_etag_for_segmented_and_empty_bodies() {
    for bytes in [b"hello".as_slice(), b"".as_slice()] {
        for frame in 1..=7 {
            let hasher = Arc::new(Mutex::new(Md5::new()));
            let expected: [u8; 16] = Md5::digest(bytes).into();
            let chunks: Vec<_> = bytes
                .chunks(frame)
                .map(|part| Ok(Bytes::copy_from_slice(part)))
                .collect();
            let mut reader = RequestBodyReader::from_stream(stream::iter(chunks))
                .with_md5_tee_and_expected(hasher.clone(), Some(expected));
            assert_eq!(read_body_to_bytes(&mut reader).await.unwrap(), bytes);
            let etag: [u8; 16] = hasher.lock().unwrap().clone().finalize().into();
            assert_eq!(etag, expected);
            assert!(reader.next_bytes().await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn checked_tee_rejects_mismatch_at_eof_and_preserves_stream_limits() {
    let hasher = Arc::new(Mutex::new(Md5::new()));
    let mut reader = RequestBodyReader::from_bytes(Bytes::from_static(b"actual"))
        .with_md5_tee_and_expected(hasher, Some(Md5::digest(b"wrong").into()));
    assert!(matches!(
        read_body_to_bytes(&mut reader).await,
        Err(ServerError::RequestBodyMd5Mismatch)
    ));
    assert!(reader.next_bytes().await.unwrap().is_none());

    let mut failed = RequestBodyReader::from_stream(stream::iter([
        Err(ServerError::RequestBodyTooLarge),
        Ok(Bytes::from_static(b"ignored")),
    ]))
    .with_md5_tee_and_expected(
        Arc::new(Mutex::new(Md5::new())),
        Some(Md5::digest(b"ignored").into()),
    );
    assert!(matches!(
        failed.next_bytes().await,
        Err(ServerError::RequestBodyTooLarge)
    ));
    assert!(failed.next_bytes().await.unwrap().is_none());

    let body = Body::from_stream(stream::iter([
        Ok::<_, std::io::Error>(Bytes::from_static(b"123")),
        Ok(Bytes::from_static(b"45")),
    ]));
    let mut capped = RequestBodyReader::from_body(body, NonZeroUsize::new(4).unwrap())
        .unwrap()
        .with_md5_tee_and_expected(
            Arc::new(Mutex::new(Md5::new())),
            Some(Md5::digest(b"12345").into()),
        );
    assert!(matches!(
        read_body_to_bytes(&mut capped).await,
        Err(ServerError::RequestBodyTooLarge)
    ));
}
