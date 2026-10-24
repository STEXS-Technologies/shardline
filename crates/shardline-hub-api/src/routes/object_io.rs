//! Bounded and streaming object access for Hub handlers.
//!
//! The storage adapters expose range reads, not whole-object reads.  Keep that
//! boundary visible here: previews consume a bounded prefix and downloads emit
//! fixed-size ranges as an HTTP body stream.
use std::{io, sync::Arc};

use axum::body::Body;
use bytes::Bytes;
use futures_util::stream;
use shardline_protocol::ByteRange;
use shardline_server_core::{ServerObjectStore, ServerObjectStoreError};
use shardline_storage::{ObjectKey, ObjectStore};

/// Range size used for Hub reads.  This bounds one storage response and one
/// resident body chunk; it is deliberately independent of file size.
pub(crate) const OBJECT_STREAM_CHUNK_BYTES: u64 = 64 * 1024;

/// Preserve a safe storage category and operation in internal errors. Never
/// emit external HTTP errors, whose messages may contain signed URLs.
pub(crate) fn storage_failure(
    operation: &'static str,
    error: &ServerObjectStoreError,
) -> crate::error::HubApiError {
    use std::error::Error;
    use std::fmt::Write as _;
    let mut detail = match error {
        ServerObjectStoreError::S3(cause) => cause.to_string(),
        ServerObjectStoreError::NotFound
        | ServerObjectStoreError::Overflow
        | ServerObjectStoreError::InvalidContentHash
        | ServerObjectStoreError::StoredObjectLengthMismatch
        | ServerObjectStoreError::Local(_)
        | ServerObjectStoreError::Io(_)
        | ServerObjectStoreError::NumericConversion(_) => error.to_string(),
    };
    if let ServerObjectStoreError::S3(shardline_storage::S3ObjectStoreError::External(provider)) =
        error
        && let Some(code) = provider_error_code(&provider.to_string())
    {
        write!(detail, " (provider {code})").unwrap_or_default();
    }
    let mut cause: &(dyn Error + 'static) = error;
    for _ in 0..16 {
        if let Some(io_error) = cause.downcast_ref::<io::Error>() {
            write!(detail, " (I/O {:?})", io_error.kind()).unwrap_or_default();
        }
        if let Some(http_error) = cause.downcast_ref::<reqwest::Error>() {
            if http_error.is_timeout() {
                detail.push_str(" (HTTP timeout)");
            } else if http_error.is_connect() {
                detail.push_str(" (HTTP connection failure)");
            }
            if let Some(status) = http_error.status() {
                write!(detail, " (HTTP {})", status.as_u16()).unwrap_or_default();
            }
        }
        let Some(source) = cause.source() else { break };
        cause = source;
    }
    crate::error::HubApiError::CasError(format!("{operation}: {detail}"))
}

// The provider library's status/body error type is private. Retain only known
// S3 XML codes, never an arbitrary response body or request URL.
fn provider_error_code(message: &str) -> Option<&'static str> {
    [
        "XMinioStorageFull",
        "SlowDown",
        "RequestTimeout",
        "InternalError",
        "ServiceUnavailable",
        "SignatureDoesNotMatch",
        "InvalidAccessKeyId",
        "AccessDenied",
        "NoSuchBucket",
        "NoSuchKey",
        "BadDigest",
        "InvalidRequest",
        "AuthorizationHeaderMalformed",
        "InvalidRange",
        "NotImplemented",
    ]
    .into_iter()
    .find(|code| message.contains(&format!("<Code>{code}</Code>")))
}

/// Reads at most `limit` bytes from the beginning of an object.
///
/// This is intended for metadata/front-matter and bounded dataset previews. It
/// never asks the backend for a range larger than the limit and never allocates
/// based on the complete object length.
pub(crate) fn read_object_prefix(
    store: &ServerObjectStore,
    key: &ObjectKey,
    object_length: u64,
    limit: usize,
) -> Result<Vec<u8>, ServerObjectStoreError> {
    let target = object_length.min(limit as u64);
    let mut output = Vec::with_capacity(target as usize);
    let mut offset = 0_u64;
    while offset < target {
        let end_exclusive = offset
            .checked_add(OBJECT_STREAM_CHUNK_BYTES)
            .ok_or(ServerObjectStoreError::Overflow)?
            .min(target);
        let end = end_exclusive
            .checked_sub(1)
            .ok_or(ServerObjectStoreError::Overflow)?;
        let range =
            ByteRange::new(offset, end).map_err(|_error| ServerObjectStoreError::Overflow)?;
        let chunk = store.read_range(key, range)?;
        if chunk.is_empty() {
            return Err(ServerObjectStoreError::StoredObjectLengthMismatch);
        }
        let expected_u64 = end
            .checked_sub(offset)
            .and_then(|value| value.checked_add(1))
            .ok_or(ServerObjectStoreError::Overflow)?;
        let expected =
            usize::try_from(expected_u64).map_err(|_error| ServerObjectStoreError::Overflow)?;
        if chunk.len() != expected {
            return Err(ServerObjectStoreError::StoredObjectLengthMismatch);
        }
        output.extend_from_slice(&chunk);
        offset = end_exclusive;
    }
    Ok(output)
}

/// Builds an HTTP body that reads an object through fixed-size ranged reads.
///
/// Storage access is moved to Tokio's blocking pool because the current local
/// and S3 adapters expose synchronous range reads through `ServerObjectStore`.
pub(crate) fn stream_object(store: &ServerObjectStore, key: ObjectKey, object_length: u64) -> Body {
    stream_object_range(store, key, object_length, 0, object_length).unwrap_or_else(|_error| {
        Body::from_stream(stream::once(async {
            Err::<Bytes, _>(io::Error::other("invalid full-object stream range"))
        }))
    })
}

/// Streams a bounded byte interval of an object using fixed-size backend reads.
/// `start` is inclusive and `length` is the number of bytes to return.
pub(crate) fn stream_object_range(
    store: &ServerObjectStore,
    key: ObjectKey,
    object_length: u64,
    start: u64,
    length: u64,
) -> Result<Body, ServerObjectStoreError> {
    let limit_exclusive = start
        .checked_add(length)
        .filter(|end| start <= object_length && *end <= object_length)
        .ok_or(ServerObjectStoreError::Overflow)?;
    if length == 0 {
        return Ok(Body::empty());
    }
    let initial = (Arc::new(store.clone()), key, start, limit_exclusive);
    let stream = stream::try_unfold(
        initial,
        move |(store_state, key_state, offset, end_exclusive)| async move {
            if offset >= end_exclusive {
                return Ok(None);
            }
            let chunk_end_exclusive = offset
                .checked_add(OBJECT_STREAM_CHUNK_BYTES)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "object range overflow")
                })?
                .min(end_exclusive);
            let end = chunk_end_exclusive.checked_sub(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "object range overflow")
            })?;
            let range = ByteRange::new(offset, end).map_err(|_error| {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid object range")
            })?;
            let expected_u64 = end
                .checked_sub(offset)
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "object range overflow")
                })?;
            let expected = usize::try_from(expected_u64).map_err(|_error| {
                io::Error::new(io::ErrorKind::InvalidInput, "object range overflow")
            })?;
            let store_for_read = Arc::clone(&store_state);
            let key_for_read = key_state.clone();
            let chunk = tokio::task::spawn_blocking(move || {
                store_for_read
                    .read_range(&key_for_read, range)
                    .map_err(|error| io::Error::other(error.to_string()))
            })
            .await
            .map_err(|error| io::Error::other(error.to_string()))??;
            if chunk.len() != expected {
                return Err(io::Error::other(
                    "object range returned an unexpected length",
                ));
            }
            Ok(Some((
                Bytes::from(chunk),
                (store_state, key_state, chunk_end_exclusive, end_exclusive),
            )))
        },
    );
    Ok(Body::from_stream(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use shardline_protocol::ShardlineHash;
    use shardline_storage::{ObjectBody, ObjectIntegrity};

    #[test]
    fn storage_diagnostics_keep_operation_and_category_without_source_secrets() {
        assert_eq!(
            provider_error_code("URL?token=secret <Code>XMinioStorageFull</Code>"),
            Some("XMinioStorageFull")
        );
        assert_eq!(provider_error_code("<Code>secret</Code>"), None);
        let message = storage_failure(
            "archive write",
            &ServerObjectStoreError::S3(shardline_storage::S3ObjectStoreError::Io(io::Error::new(
                io::ErrorKind::StorageFull,
                "secret signed URL and credentials",
            ))),
        )
        .to_string();
        assert!(message.contains("archive write"));
        assert!(message.contains("StorageFull"));
        assert!(!message.contains("secret"));
        assert!(!message.contains("credentials"));
        let request_error = reqwest::Client::new()
            .get("http://[invalid/?token=secret")
            .build()
            .expect_err("malformed URL");
        let message = storage_failure(
            "read",
            &ServerObjectStoreError::Io(io::Error::other(request_error)),
        )
        .to_string();
        assert!(!message.contains("token="));
        assert!(!message.contains("secret"));
    }

    fn local_store() -> (tempfile::TempDir, ServerObjectStore) {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = ServerObjectStore::local(temp.path().join("objects")).expect("local store");
        (temp, store)
    }

    fn put(store: &ServerObjectStore, key: &ObjectKey, bytes: &[u8]) {
        let integrity = ObjectIntegrity::new(
            ShardlineHash::from_bytes(*blake3::hash(bytes).as_bytes()),
            bytes.len() as u64,
        );
        store
            .put_overwrite(key, ObjectBody::from_slice(bytes), &integrity)
            .expect("put object");
    }

    #[test]
    fn prefix_read_is_bounded_to_requested_bytes() {
        let (_temp, store) = local_store();
        let key = ObjectKey::parse("hub/large.bin").expect("key");
        let bytes = vec![0xA5; (OBJECT_STREAM_CHUNK_BYTES as usize * 3) + 17];
        put(&store, &key, &bytes);

        let prefix =
            read_object_prefix(&store, &key, bytes.len() as u64, 100_003).expect("prefix read");
        assert_eq!(prefix.len(), 100_003);
        assert_eq!(prefix, bytes[..100_003]);
    }

    #[test]
    fn prefix_read_allows_empty_object_and_zero_limit() {
        let (_temp, store) = local_store();
        let key = ObjectKey::parse("hub/empty.bin").expect("key");
        put(&store, &key, b"");
        assert!(read_object_prefix(&store, &key, 0, 128).unwrap().is_empty());
        assert!(read_object_prefix(&store, &key, 1, 0).unwrap().is_empty());
    }

    #[test]
    fn prefix_read_reports_missing_object() {
        let (_temp, store) = local_store();
        let key = ObjectKey::parse("hub/missing.bin").expect("key");
        assert!(read_object_prefix(&store, &key, 1, 1).is_err());
    }

    #[tokio::test]
    async fn stream_surfaces_mid_object_storage_failure_and_recovers() {
        let (_temp, store) = local_store();
        let key = ObjectKey::parse("hub/recover.bin").expect("key");
        let bytes = vec![0x3C; (OBJECT_STREAM_CHUNK_BYTES as usize * 2) + 11];
        put(&store, &key, &bytes);
        let path = store.local_path_for_key(&key).expect("local path");

        let mut body = stream_object(&store, key.clone(), bytes.len() as u64);
        let first = body
            .frame()
            .await
            .expect("first frame")
            .expect("first body frame");
        assert_eq!(
            first.into_data().expect("first data").len(),
            OBJECT_STREAM_CHUNK_BYTES as usize
        );

        std::fs::remove_file(&path).expect("inject object loss");
        let failure = body
            .frame()
            .await
            .expect("stream frame")
            .expect_err("expected read failure");
        assert!(!failure.to_string().is_empty());

        // Recovery is a fresh stream after the durable object is restored.
        put(&store, &key, &bytes);
        let mut recovered = stream_object(&store, key, bytes.len() as u64);
        let mut recovered_bytes = Vec::new();
        while let Some(frame) = recovered.frame().await {
            recovered_bytes.extend_from_slice(
                &frame
                    .expect("recovered frame")
                    .into_data()
                    .expect("recovered data"),
            );
        }
        assert_eq!(recovered_bytes, bytes);
    }

    #[tokio::test]
    async fn stream_empty_object_completes_without_storage_read() {
        let (_temp, store) = local_store();
        let key = ObjectKey::parse("hub/empty-stream.bin").expect("key");
        let mut body = stream_object(&store, key, 0);
        assert!(body.frame().await.is_none());
    }

    #[tokio::test]
    async fn stream_small_object_returns_exact_bytes_then_end() {
        let (_temp, store) = local_store();
        let key = ObjectKey::parse("hub/small-stream.bin").expect("key");
        put(&store, &key, b"small");
        let mut body = stream_object(&store, key, 5);
        let frame = body
            .frame()
            .await
            .expect("data frame")
            .expect("valid frame");
        assert_eq!(
            frame.into_data().expect("data"),
            Bytes::from_static(b"small")
        );
        assert!(body.frame().await.is_none());
    }

    #[tokio::test]
    async fn stream_range_returns_exact_interval_across_backend_read_boundaries() {
        let (_temp, store) = local_store();
        let key = ObjectKey::parse("hub/ranged-stream.bin").expect("key");
        let bytes: Vec<u8> = (0..(OBJECT_STREAM_CHUNK_BYTES as usize * 3 + 19))
            .map(|index| (index % 251) as u8)
            .collect();
        put(&store, &key, &bytes);
        let start = 37_u64;
        let length = OBJECT_STREAM_CHUNK_BYTES * 2 + 11;
        let body = stream_object_range(&store, key, bytes.len() as u64, start, length)
            .expect("valid range");
        let selected = body.collect().await.expect("range stream").to_bytes();
        let start = usize::try_from(start).expect("start fits usize");
        let end = start + usize::try_from(length).expect("length fits usize");
        assert_eq!(selected.as_ref(), &bytes[start..end]);
    }

    #[tokio::test]
    async fn blackhole_stream_fails_without_buffering_or_panicking() {
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("hub/missing.bin").expect("key");
        let mut body = stream_object(&store, key, 1);
        let frame = body.frame().await.expect("stream frame");
        assert!(frame.is_err(), "blackhole read must be surfaced to caller");
    }
}
