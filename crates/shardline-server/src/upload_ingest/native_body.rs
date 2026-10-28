//! Memory-only native ingress, admitted before consuming request bytes.
use super::RequestBodyReader;
use crate::{ServerError, admission::BoundedPool};
use tokio::sync::OwnedSemaphorePermit;

pub(crate) struct NativeBody {
    pub bytes: Vec<u8>,
    pub admission: OwnedSemaphorePermit,
}

enum VerificationInput {
    Frame(axum::body::Bytes),
    Complete,
}

pub(crate) async fn read_native_body(
    reader: &mut RequestBodyReader,
) -> Result<NativeBody, ServerError> {
    let admission = super::buffer_admission::acquire().await?;
    let bytes = reader.read_bounded_memory().await?;
    Ok(NativeBody { bytes, admission })
}

/// One admitted verifier consumes a one-frame queue while the async reader
/// receives subsequent bytes. Only an explicit, successfully checked EOF can
/// produce a prepared publication; disconnect/cancellation closes the queue.
pub(crate) async fn prepare_native_xorb(
    reader: &mut RequestBodyReader,
    hash: &str,
    pool: &BoundedPool,
) -> Result<
    (
        crate::upload_identity::PreparedNativeXorb,
        u64,
        crate::upload_identity::NativeUploadResources,
    ),
    ServerError,
> {
    let admission = super::buffer_admission::acquire().await?;
    let worker = pool
        .acquire()
        .await
        .map_err(|error| ServerError::Io(std::io::Error::other(error)))?;
    let maximum = reader.bounded_memory_limit()?;
    let mut state = shardline_xet_adapter::IncrementalXorbMemory::new(hash, maximum)?;
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<VerificationInput>(1);
    let receive = async move {
        while let Some(bytes) = reader.next_bytes().await? {
            sender
                .send(VerificationInput::Frame(bytes))
                .await
                .map_err(|_error| {
                    ServerError::Io(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "native verifier stopped",
                    ))
                })?;
        }
        sender
            .send(VerificationInput::Complete)
            .await
            .map_err(|_error| {
                ServerError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "native verifier stopped",
                ))
            })?;
        Ok::<(), ServerError>(())
    };
    let verify = crate::admission::cancellable_work_with_permit(worker, move |cancel| {
        loop {
            cancel.check()?;
            match receiver.blocking_recv() {
                Some(VerificationInput::Frame(bytes)) => {
                    state = state.push(&bytes, &|| cancel.check_io())?;
                }
                Some(VerificationInput::Complete) => {
                    let prepared = state.finish(&|| cancel.check_io())?;
                    return Ok((prepared, admission));
                }
                None => {
                    return Err(ServerError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "native body ended without verified EOF",
                    )));
                }
            }
        }
    });
    tokio::pin!(receive, verify);
    // Stop receiving immediately when verification fails, even if the client
    // leaves its stream open. Reader errors cancel running verification.
    let ((prepared, retained_admission), retained_worker) = tokio::select! {
        verified = &mut verify => {
            let verified = verified?;
            receive.await?;
            verified
        }
        received = &mut receive => {
            if let Err(error) = received {
                if matches!(&error, ServerError::Io(source) if source.kind() == std::io::ErrorKind::BrokenPipe) {
                    return Err(verify.await.err().unwrap_or(error));
                }
                return Err(error);
            }
            verify.await?
        }
    };
    let length = prepared.length();
    Ok((prepared, length, (retained_admission, retained_worker)))
}

#[cfg(test)]
async fn read_with_admission(
    reader: &mut RequestBodyReader,
    pool: &BoundedPool,
) -> Result<NativeBody, ServerError> {
    let admission = pool
        .acquire()
        .await
        .map_err(|error| ServerError::Io(std::io::Error::other(error)))?;
    let bytes = reader.read_bounded_memory().await?;
    Ok(NativeBody { bytes, admission })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;
    use std::num::NonZeroUsize;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[tokio::test]
    async fn malformed_member_fails_before_client_eof() {
        let pool = BoundedPool::new(NonZeroUsize::MIN);
        let stream = futures_util::StreamExt::chain(
            futures_util::stream::once(async { Ok(Bytes::from_static(&[255; 8])) }),
            futures_util::stream::pending(),
        );
        let mut reader = RequestBodyReader::from_stream(stream);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            prepare_native_xorb(&mut reader, &"00".repeat(32), &pool),
        )
        .await;
        assert!(
            result
                .expect("must reject without waiting for EOF")
                .is_err()
        );
        let _released = tokio::time::timeout(std::time::Duration::from_secs(5), pool.acquire())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn incremental_reader_preserves_body_limit_error() {
        let pool = BoundedPool::new(NonZeroUsize::MIN);
        let stream =
            futures_util::stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"too large"))]);
        let mut reader = RequestBodyReader::from_body(
            axum::body::Body::from_stream(stream),
            NonZeroUsize::new(3).unwrap(),
        )
        .unwrap();
        let result = prepare_native_xorb(&mut reader, &"00".repeat(32), &pool).await;
        assert!(matches!(result, Err(ServerError::RequestBodyTooLarge)));
        let _released = tokio::time::timeout(std::time::Duration::from_secs(5), pool.acquire())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn cancelled_incremental_reader_unblocks_waiting_worker() {
        let pool = BoundedPool::new(NonZeroUsize::MIN);
        let owned_pool = pool.clone();
        let (started, waiting) = tokio::sync::oneshot::channel();
        let stream = futures_util::StreamExt::chain(
            futures_util::stream::once(async move {
                started.send(()).unwrap();
                Ok(Bytes::from_static(b"\0"))
            }),
            futures_util::stream::pending(),
        );
        let caller = tokio::spawn(async move {
            let mut reader = RequestBodyReader::from_stream(stream);
            prepare_native_xorb(&mut reader, &"00".repeat(32), &owned_pool).await
        });
        waiting.await.unwrap();
        assert!(pool.try_acquire().is_none());
        caller.abort();
        assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
        let _released = tokio::time::timeout(std::time::Duration::from_secs(5), pool.acquire())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn admission_precedes_body_consumption_and_cancellation_releases_it() {
        let pool = BoundedPool::new(NonZeroUsize::MIN);
        let held = pool.acquire().await.unwrap();
        let consumed = Arc::new(AtomicUsize::new(0));
        let observed = consumed.clone();
        let mut reader = RequestBodyReader::from_stream(futures_util::stream::once(async move {
            observed.fetch_add(1, Ordering::Relaxed);
            Ok(Bytes::from_static(b"native"))
        }));
        let mut waiting = Box::pin(read_with_admission(&mut reader, &pool));
        assert!(futures_util::poll!(&mut waiting).is_pending());
        assert_eq!(consumed.load(Ordering::Relaxed), 0);
        drop(waiting);
        assert!(pool.try_acquire().is_none());
        drop(held);
        let body = read_with_admission(&mut reader, &pool).await.unwrap();
        assert_eq!(body.bytes, b"native");
        assert!(pool.try_acquire().is_none());
        drop(body);
        assert!(pool.try_acquire().is_some());
    }

    #[tokio::test]
    async fn interrupted_body_releases_memory_admission() {
        let pool = BoundedPool::new(NonZeroUsize::MIN);
        let mut reader = RequestBodyReader::from_stream(futures_util::stream::iter([
            Ok(Bytes::from_static(b"partial")),
            Err(ServerError::Io(std::io::Error::other("interrupted body"))),
        ]));
        assert!(read_with_admission(&mut reader, &pool).await.is_err());
        assert!(pool.try_acquire().is_some());
    }

    #[tokio::test]
    async fn cancellation_during_body_read_reclaims_capacity() {
        let pool = BoundedPool::new(NonZeroUsize::MIN);
        let stream = futures_util::StreamExt::chain(
            futures_util::stream::once(async { Ok(Bytes::from(vec![0x5a; 1024 * 1024])) }),
            futures_util::stream::pending(),
        );
        let mut reader = RequestBodyReader::from_stream(stream);
        let mut collecting = Box::pin(read_with_admission(&mut reader, &pool));
        assert!(futures_util::poll!(&mut collecting).is_pending());
        assert!(pool.try_acquire().is_none());
        drop(collecting);
        assert!(pool.try_acquire().is_some());
    }

    #[tokio::test]
    async fn oversized_chunked_body_releases_capacity() {
        let pool = BoundedPool::new(NonZeroUsize::MIN);
        let stream = futures_util::stream::iter([
            Ok::<_, std::io::Error>(Bytes::from_static(b"ab")),
            Ok(Bytes::from_static(b"cd")),
        ]);
        let mut reader = RequestBodyReader::from_body(
            axum::body::Body::from_stream(stream),
            NonZeroUsize::new(3).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            read_with_admission(&mut reader, &pool).await,
            Err(ServerError::RequestBodyTooLarge)
        ));
        assert!(pool.try_acquire().is_some());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn large_native_ingress_performs_no_file_writes() {
        fn writes() -> u64 {
            std::fs::read_to_string("/proc/thread-self/io")
                .unwrap()
                .lines()
                .find_map(|line| {
                    line.strip_prefix("wchar: ")
                        .map(|value| value.parse().unwrap())
                })
                .unwrap()
        }
        let pool = BoundedPool::new(NonZeroUsize::MIN);
        let bytes = vec![0x5a; 8 * 1024 * 1024];
        let input = bytes.clone();
        let frames = (0..1024).map(move |index| {
            let start = index * 8192;
            Ok(Bytes::copy_from_slice(&input[start..start + 8192]))
        });
        let mut reader = RequestBodyReader::from_stream(futures_util::stream::iter(frames));
        let before = writes();
        let body = read_with_admission(&mut reader, &pool).await.unwrap();
        assert_eq!(writes(), before, "native ingress wrote a staging file");
        assert_eq!(body.bytes, bytes);
    }
}
