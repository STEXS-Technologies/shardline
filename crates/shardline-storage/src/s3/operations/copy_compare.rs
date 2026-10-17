//! Compare complete remote snapshots without buffering complete objects.
use bytes::Bytes;
use futures_util::StreamExt;
use object_store::{GetOptions, ObjectStore};

use super::{S3ByteStream, S3ObjectStore, S3ObjectStoreError, stream_payload_for_range};
use crate::{ObjectKey, PutOutcome};

pub(super) fn compare_existing_copy(
    store: &S3ObjectStore,
    source: &ObjectKey,
    destination: &ObjectKey,
    expected_length: u64,
) -> Result<PutOutcome, S3ObjectStoreError> {
    let source = store.location_for_key(source)?;
    let destination = store.location_for_key(destination)?;
    store.block_on_result(async {
        let mut streams = Vec::with_capacity(2);
        // Inspect the destination first: absent/conflicting destinations need
        // no source GET. The GET supplies the metadata of that exact snapshot.
        for location in [&destination, &source] {
            let result = store
                .inner
                .get_opts(
                    location,
                    GetOptions::new().with_range(Some(0..expected_length)),
                )
                .await
                .map_err(|error| {
                    if location == &destination
                        && matches!(error, object_store::Error::NotFound { .. })
                    {
                        S3ObjectStoreError::ExistingObjectConflict
                    } else {
                        S3ObjectStoreError::External(error)
                    }
                })?;
            if result.meta.size != expected_length {
                return Err(S3ObjectStoreError::ExistingObjectConflict);
            }
            streams.push(stream_payload_for_range(result, 0..expected_length)?);
        }
        let source_stream = streams
            .pop()
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        let destination_stream = streams
            .pop()
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        compare_streams(source_stream, destination_stream, expected_length).await?;
        Ok(PutOutcome::AlreadyExists)
    })
}

async fn next_frame(stream: &mut S3ByteStream) -> Result<Option<Bytes>, S3ObjectStoreError> {
    while let Some(frame) = stream.next().await {
        let frame = frame?;
        if !frame.is_empty() {
            return Ok(Some(frame));
        }
    }
    Ok(None)
}

async fn compare_streams(
    mut source: S3ByteStream,
    mut destination: S3ByteStream,
    expected_length: u64,
) -> Result<(), S3ObjectStoreError> {
    let mut left = Bytes::new();
    let mut right = Bytes::new();
    let mut compared = 0_u64;
    loop {
        if left.is_empty() {
            left = next_frame(&mut source).await?.unwrap_or_default();
        }
        if right.is_empty() {
            right = next_frame(&mut destination).await?.unwrap_or_default();
        }
        if left.is_empty() || right.is_empty() {
            return if left.is_empty() && right.is_empty() && compared == expected_length {
                Ok(())
            } else {
                Err(S3ObjectStoreError::ExistingObjectConflict)
            };
        }
        let length = left.len().min(right.len());
        compared = compared
            .checked_add(
                u64::try_from(length)
                    .map_err(|_error| S3ObjectStoreError::ExistingObjectConflict)?,
            )
            .filter(|total| *total <= expected_length)
            .ok_or(S3ObjectStoreError::ExistingObjectConflict)?;
        if left.split_to(length) != right.split_to(length) {
            return Err(S3ObjectStoreError::ExistingObjectConflict);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frames(parts: &[&'static [u8]]) -> S3ByteStream {
        let mut frames = Vec::with_capacity(parts.len());
        for part in parts {
            frames.push(Ok(Bytes::from_static(part)));
        }
        Box::pin(futures_util::stream::iter(frames))
    }

    #[tokio::test]
    async fn compares_independently_segmented_frames() {
        assert!(
            compare_streams(
                frames(&[b"", b"abc", b"d", b"ef", b""]),
                frames(&[b"a", b"", b"bcdef"]),
                6
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn rejects_mismatches_and_incomplete_or_excess_bodies() {
        for (left, right, length) in [
            (b"abcdef".as_slice(), b"abcdeg".as_slice(), 6),
            (b"abcdef".as_slice(), b"xbcdef".as_slice(), 6),
            (b"abcde".as_slice(), b"abcdef".as_slice(), 6),
            (b"abcdef".as_slice(), b"abcde".as_slice(), 6),
            (b"abcde".as_slice(), b"abcde".as_slice(), 6),
            (b"abcdefg".as_slice(), b"abcdefg".as_slice(), 6),
        ] {
            assert!(
                compare_streams(frames(&[left]), frames(&[right]), length)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn rejects_transport_failure_after_matching_bytes() {
        let failure = || {
            Box::pin(futures_util::stream::iter(vec![
                Ok(Bytes::from_static(b"abc")),
                Err(S3ObjectStoreError::ExistingObjectConflict),
            ])) as S3ByteStream
        };
        assert!(
            compare_streams(failure(), frames(&[b"abc"]), 3)
                .await
                .is_err()
        );
        assert!(
            compare_streams(frames(&[b"abc"]), failure(), 3)
                .await
                .is_err()
        );
    }
}
