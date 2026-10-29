//! Bounded anchored reads of immutable local upload parts.
use bytes::Bytes;
use futures_util::{Stream, stream};
use shardline_protocol::ByteRange;
use shardline_storage::ObjectKey;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::{ServerObjectStore, open_local_object_for_read};
use crate::{ServerError, error::ObjectStoreError, upload_ingest::RequestBodyReader};

pub(super) async fn stream(
    store: &ServerObjectStore,
    key: &ObjectKey,
    object_length: u64,
    range: ByteRange,
) -> Result<impl Stream<Item = Result<Bytes, ServerError>> + Send + 'static + use<>, ServerError> {
    let mut file = open_local_object_for_read(store, key, object_length)
        .await?
        .ok_or(ServerError::NotFound)?;
    if range.end_inclusive() >= object_length {
        return Err(ServerError::ObjectStore(
            ObjectStoreError::StoredLengthMismatch,
        ));
    }
    let length = range.len().ok_or(ServerError::Overflow)?;
    file.seek(std::io::SeekFrom::Start(range.start())).await?;
    let reader = RequestBodyReader::from_reader_chain(vec![file.take(length)], 64 * 1024);
    Ok(stream::try_unfold(
        (reader, length),
        |(mut reader, remaining)| async move {
            match reader.next_bytes().await? {
                Some(bytes) => {
                    let remaining = remaining
                        .checked_sub(u64::try_from(bytes.len())?)
                        .ok_or(ServerError::Overflow)?;
                    Ok(Some((bytes, (reader, remaining))))
                }
                None if remaining == 0 => Ok(None),
                None => Err(ServerError::ObjectStore(
                    ObjectStoreError::StoredLengthMismatch,
                )),
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_index::{ResumablePartRange, ResumableSessionPart};
    use std::num::NonZeroU64;

    #[tokio::test]
    async fn local_overlay_stream_matches_generation_order_and_bounds_frames() {
        let root = tempfile::tempdir().unwrap();
        let store = ServerObjectStore::local(root.path()).unwrap();
        let total = 3 * 64 * 1024 + 17;
        let mut expected = vec![0; total];
        let mut parts = Vec::new();
        for generation in 1..=32_usize {
            let start = generation * 997 % (total / 2);
            let length = if generation == 1 {
                total / 2
            } else {
                128 + generation * 17
            };
            let bytes = vec![u8::try_from(generation).unwrap(); length];
            let key = ObjectKey::parse(&format!(
                "staging/lfs/{generation}/{}",
                blake3::hash(&bytes).to_hex()
            ))
            .unwrap();
            let path = root.path().join(key.as_str());
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, &bytes).unwrap();
            expected[start..start + length].copy_from_slice(&bytes);
            parts.push(
                ResumableSessionPart::new(
                    NonZeroU64::new(generation as u64).unwrap(),
                    NonZeroU64::new(generation as u64).unwrap(),
                    key.as_str().to_owned(),
                    length as u64,
                    None,
                )
                .with_range(
                    ResumablePartRange::new(start as u64, (start + length) as u64).unwrap(),
                ),
            );
        }
        // Publication order is the generation, not caller enumeration order.
        parts.reverse();
        let mut reader = super::super::lfs_parts_reader(&store, &parts, total as u64)
            .await
            .unwrap();
        let mut observed = Vec::new();
        while let Some(bytes) = reader.next_bytes().await.unwrap() {
            assert!(bytes.len() <= 1024 * 1024);
            observed.extend_from_slice(&bytes);
        }
        assert_eq!(observed, expected);
    }

    #[tokio::test]
    async fn local_range_rejects_wrong_source_length_and_stops_at_selected_end() {
        let root = tempfile::tempdir().unwrap();
        let store = ServerObjectStore::local(root.path()).unwrap();
        let key = ObjectKey::parse("part").unwrap();
        std::fs::write(root.path().join("part"), b"prefixPAYLOADsuffix").unwrap();
        assert!(
            stream(&store, &key, 1, ByteRange::new(6, 12).unwrap())
                .await
                .is_err()
        );
        let mut selected = Box::pin(
            stream(&store, &key, 19, ByteRange::new(6, 12).unwrap())
                .await
                .unwrap(),
        );
        use futures_util::StreamExt;
        assert_eq!(selected.next().await.unwrap().unwrap(), &b"PAYLOAD"[..]);
        assert!(selected.next().await.is_none());
    }
}
