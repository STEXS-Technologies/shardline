//! Admit request buffering with the same quota retained by Git pack workers.
use crate::error::HubApiError;
use axum::body::{Body, Bytes, to_bytes};
use tokio::sync::{Semaphore, SemaphorePermit};

pub(super) async fn read_request(
    body: Body,
    limit: usize,
    operation: &str,
) -> Result<(Bytes, SemaphorePermit<'static>), HubApiError> {
    read_admitted(body, limit, operation, &super::identity::IDENTITY_ADMISSION).await
}

async fn read_admitted(
    body: Body,
    limit: usize,
    operation: &str,
    admission: &'static Semaphore,
) -> Result<(Bytes, SemaphorePermit<'static>), HubApiError> {
    let permit = admission
        .acquire()
        .await
        .map_err(|error| HubApiError::CasError(error.to_string()))?;
    let bytes = to_bytes(body, limit).await.map_err(|error| {
        HubApiError::BadRequest(format!("{operation} request too large: {error}"))
    })?;
    Ok((bytes, permit))
}

pub(super) fn response_body(bytes: Vec<u8>, permit: SemaphorePermit<'static>) -> Body {
    // Keep the pack allocation and its admission slot together until HTTP
    // consumes or drops the body. Small frames bound transport handoff size.
    Body::from_stream(futures_util::stream::unfold(
        (Bytes::from(bytes), permit),
        |(mut bytes, permit)| async move {
            if bytes.is_empty() {
                return None;
            }
            let frame = bytes.split_to(bytes.len().min(65536));
            Some((Ok::<_, std::io::Error>(frame), (bytes, permit)))
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[tokio::test]
    async fn request_body_is_not_polled_before_admission() {
        let admission = Box::leak(Box::new(Semaphore::new(1)));
        let held = admission.acquire().await.unwrap();
        let polled = Arc::new(AtomicBool::new(false));
        let observed = polled.clone();
        let body = Body::from_stream(futures_util::stream::once(async move {
            observed.store(true, Ordering::SeqCst);
            Ok::<_, std::io::Error>(Bytes::from_static(b"pack"))
        }));
        let mut reading = Box::pin(read_admitted(body, 4, "test", admission));
        assert!(futures_util::poll!(&mut reading).is_pending());
        assert!(!polled.load(Ordering::SeqCst));
        drop(held);
        let (bytes, permit) = reading.await.unwrap();
        assert_eq!(bytes.as_ref(), b"pack");
        assert!(polled.load(Ordering::SeqCst));
        assert!(admission.try_acquire().is_err());
        drop(permit);
        assert_eq!(admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn oversized_body_releases_admission() {
        let admission = Box::leak(Box::new(Semaphore::new(1)));
        assert!(
            read_admitted(Body::from("oversized"), 4, "test", admission)
                .await
                .is_err()
        );
        assert_eq!(admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn cancelled_body_read_releases_admission() {
        let admission = Box::leak(Box::new(Semaphore::new(1)));
        let body =
            Body::from_stream(futures_util::stream::pending::<Result<Bytes, std::io::Error>>());
        let mut reading = Box::pin(read_admitted(body, 4, "test", admission));
        assert!(futures_util::poll!(&mut reading).is_pending());
        assert_eq!(admission.available_permits(), 0);
        drop(reading);
        assert_eq!(admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn response_retains_admission_until_drained_or_cancelled() {
        let admission = Box::leak(Box::new(Semaphore::new(1)));
        for cancel in [false, true] {
            let permit = admission.acquire().await.unwrap();
            let expected = vec![42; 131073];
            let mut stream = response_body(expected.clone(), permit).into_data_stream();
            assert_eq!(admission.available_permits(), 0);
            let first = stream.next().await.unwrap().unwrap();
            assert_eq!(first.len(), 65536);
            assert_eq!(admission.available_permits(), 0);
            if cancel {
                drop(stream);
            } else {
                let mut received = first.to_vec();
                while let Some(frame) = stream.next().await {
                    let frame = frame.unwrap();
                    assert!(frame.len() <= 65536);
                    received.extend_from_slice(&frame);
                }
                assert_eq!(received, expected);
            }
            assert_eq!(admission.available_permits(), 1);
        }
    }
}
