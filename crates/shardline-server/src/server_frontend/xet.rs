use axum::body::Bytes;
use futures_util::stream;
use shardline_index::{FileChunkRecord, parse_xet_hash_hex};
use shardline_storage::ObjectKey;
use tokio::sync::mpsc;

mod terms;

#[cfg(test)]
mod pressure;

use crate::{
    InvalidSerializedShardError, ServerError,
    object_store::ServerObjectStore,
    xet_adapter::{
        XetAdapterError, XorbVisitError, try_for_each_serialized_xorb_chunk_trusted,
        validate_serialized_xorb, visit_stored_xorb_chunk_hashes,
        xorb_hash_from_object_key_if_present, xorb_object_key,
    },
};

pub(crate) fn map_xorb_visit_error_server(error: XorbVisitError<ServerError>) -> ServerError {
    match error {
        XorbVisitError::Parse(error) => ServerError::from(error),
        XorbVisitError::Visitor(error) => error,
    }
}

pub(super) fn push_optional_chunk_container_key(
    object_keys: &mut Vec<ObjectKey>,
    chunk_hash: &str,
) -> Result<(), ServerError> {
    let object_key = xorb_object_key(chunk_hash)?;
    if !object_keys.contains(&object_key) {
        object_keys.push(object_key);
    }
    Ok(())
}

pub(super) fn referenced_term_object_key(term_hash: &str) -> Result<ObjectKey, ServerError> {
    Ok(xorb_object_key(term_hash)?)
}

pub(super) fn owns_protocol_object(key: &ObjectKey) -> Result<bool, ServerError> {
    Ok(xorb_hash_from_object_key_if_present(key)?.is_some())
}

pub(super) fn visit_protocol_object_member_chunks<Visitor>(
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    mut visitor: Visitor,
) -> Result<(), ServerError>
where
    Visitor: FnMut(String) -> Result<(), ServerError>,
{
    let mut result = Ok(());
    visit_stored_xorb_chunk_hashes(object_store, object_key, |chunk_hash_hex| {
        match visitor(chunk_hash_hex) {
            Ok(()) => Ok(()),
            Err(e) => {
                result = Err(e);
                Err(XetAdapterError::NotFound)
            }
        }
    })?;
    result
}

pub(super) fn append_referenced_term_bytes(
    object_store: &ServerObjectStore,
    term: &FileChunkRecord,
    output: &mut Vec<u8>,
) -> Result<(), ServerError> {
    if term.range_end <= term.range_start {
        return Err(InvalidSerializedShardError::NativeXetTermEmptyOrInvertedChunkRange.into());
    }

    let xorb_key = xorb_object_key(&term.hash)?;
    // Local objects retain their verified file handle without a scratch copy.
    // A zero memory budget keeps remote materialization streamed to disk.
    let Some((mut reader, _length)) =
        object_store.materialize_bounded_object_if_present(&xorb_key, 0)?
    else {
        return Err(ServerError::MissingReferencedXorb);
    };
    let expected_hash = parse_xet_hash_hex(&term.hash)?;
    let validated = validate_serialized_xorb(&mut reader, expected_hash)?;
    terms::validate_length(term, &validated)?;
    let range_start = usize::try_from(term.range_start)?;
    let range_end = usize::try_from(term.range_end)?;
    if range_end > validated.chunks().len() {
        return Err(InvalidSerializedShardError::NativeXetTermRangeExceededXorbChunkCount.into());
    }

    let mut chunk_index = 0_usize;
    try_for_each_serialized_xorb_chunk_trusted(&mut reader, &validated, |decoded_chunk| {
        if chunk_index >= range_start && chunk_index < range_end {
            terms::validate_member(&decoded_chunk)?;
            output.extend_from_slice(decoded_chunk.data());
        }
        chunk_index = chunk_index.checked_add(1).ok_or(ServerError::Overflow)?;
        Ok::<(), ServerError>(())
    })
    .map_err(map_xorb_visit_error_server)
}

struct ReferencedFileWorker(tokio::task::JoinHandle<()>);

impl Drop for ReferencedFileWorker {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Streams a file record stored as Xet Xorb terms with a bounded channel between
/// the blocking Xorb decoder and the HTTP response consumer.
pub(crate) fn referenced_file_record_byte_stream(
    object_store: ServerObjectStore,
    record: shardline_index::FileRecord,
    pool: crate::admission::BoundedPool,
) -> Result<crate::download_stream::ServerByteStream, ServerError> {
    if record.storage_layout() != shardline_index::FileRecordStorageLayout::ReferencedObjectTerms {
        return Err(ServerError::ObjectStore(
            crate::error::ObjectStoreError::StoredLengthMismatch,
        ));
    }
    record.validate_reconstruction_plan()?;

    let mut expected_offset = 0_u64;
    for term in &record.chunks {
        if term.offset != expected_offset {
            return Err(ServerError::ObjectStore(
                crate::error::ObjectStoreError::StoredLengthMismatch,
            ));
        }
        expected_offset = expected_offset
            .checked_add(term.length)
            .ok_or(ServerError::Overflow)?;
    }
    if expected_offset != record.total_bytes {
        return Err(ServerError::ObjectStore(
            crate::error::ObjectStoreError::StoredLengthMismatch,
        ));
    }

    let (sender, receiver) = mpsc::channel::<Result<Bytes, ServerError>>(2);
    let worker = ReferencedFileWorker(tokio::spawn(async move {
        let blocking_sender = sender.clone();
        let result = crate::admission::blocking_work(&pool, move || {
            for term in record.chunks {
                stream_referenced_term(&object_store, &term, &blocking_sender)?;
            }
            Ok(())
        })
        .await;
        if let Err(error) = result {
            let _ignored = sender.send(Err(error)).await;
        }
    }));

    Ok(Box::pin(stream::unfold(
        (receiver, Some(worker)),
        |(mut receiver, mut worker)| async move {
            if let Some(item) = receiver.recv().await {
                return Some((item, (receiver, worker)));
            }
            if let Some(mut worker) = worker.take()
                && let Err(error) = (&mut worker.0).await
            {
                return Some((Err(ServerError::BlockingTask(error)), (receiver, None)));
            }
            None
        },
    )))
}

fn stream_referenced_term(
    object_store: &ServerObjectStore,
    term: &FileChunkRecord,
    sender: &mpsc::Sender<Result<Bytes, ServerError>>,
) -> Result<(), ServerError> {
    if term.range_end <= term.range_start {
        return Err(InvalidSerializedShardError::NativeXetTermEmptyOrInvertedChunkRange.into());
    }
    let xorb_key = xorb_object_key(&term.hash)?;
    let checkpoint = || {
        if sender.is_closed() {
            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "native file response was cancelled",
            ))
        } else {
            Ok(())
        }
    };
    checkpoint()?;
    if let Some(path) = object_store.local_path_for_key(&xorb_key) {
        crate::object_store::run_before_local_object_read_hook(&path);
    }
    let Some((mut materialized, _length)) = object_store
        .materialize_bounded_object_if_present_with_checkpoint(&xorb_key, 0, &checkpoint)?
    else {
        return Err(ServerError::MissingReferencedXorb);
    };
    let mut reader = crate::download_stream::checkpoint_reader::CheckpointReader {
        reader: &mut materialized,
        checkpoint: &checkpoint,
    };
    let expected_hash = parse_xet_hash_hex(&term.hash)?;
    let validated = validate_serialized_xorb(&mut reader, expected_hash)?;
    terms::validate_length(term, &validated)?;
    let range_start = usize::try_from(term.range_start)?;
    let range_end = usize::try_from(term.range_end)?;
    if range_end > validated.chunks().len() {
        return Err(InvalidSerializedShardError::NativeXetTermRangeExceededXorbChunkCount.into());
    }

    let mut chunk_index = 0_usize;
    let mut emitted_bytes = 0_u64;
    try_for_each_serialized_xorb_chunk_trusted(&mut reader, &validated, |decoded_chunk| {
        if chunk_index >= range_start && chunk_index < range_end {
            terms::validate_member(&decoded_chunk)?;
            let bytes = decoded_chunk.data();
            emitted_bytes = emitted_bytes
                .checked_add(
                    u64::try_from(bytes.len())
                        .map_err(|_conversion_error| ServerError::Overflow)?,
                )
                .ok_or(ServerError::Overflow)?;
            sender
                .blocking_send(Ok(Bytes::copy_from_slice(bytes)))
                .map_err(|_closed| ServerError::NotFound)?;
        }
        chunk_index = chunk_index.checked_add(1).ok_or(ServerError::Overflow)?;
        Ok::<(), ServerError>(())
    })
    .map_err(map_xorb_visit_error_server)?;

    if emitted_bytes != term.length {
        return Err(ServerError::ObjectStore(
            crate::error::ObjectStoreError::StoredLengthMismatch,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use shardline_storage::ObjectKey;

    use super::*;
    use crate::xet_adapter::{XorbParseError, XorbVisitError};

    fn selected_term_fixture(
        length: u64,
    ) -> (tempfile::TempDir, ServerObjectStore, FileChunkRecord) {
        let root = tempfile::tempdir().unwrap();
        let store = ServerObjectStore::local(root.path()).unwrap();
        let packed = crate::upload_ingest::xorb_packer::pack_chunks_into_xorb(&[
            (b"first chunk".to_vec(), 0),
            (b"second chunk".to_vec(), 11),
        ])
        .unwrap();
        let key = xorb_object_key(&packed.xorb_hash_hex).unwrap();
        store
            .put_overwrite(
                &key,
                shardline_storage::ObjectBody::Borrowed(&packed.serialized),
                &shardline_storage::ObjectIntegrity::new(
                    crate::local_backend::chunk_hash(&packed.serialized),
                    packed.serialized.len() as u64,
                ),
            )
            .unwrap();
        let term = FileChunkRecord {
            hash: packed.xorb_hash_hex,
            offset: 0,
            length,
            range_start: 1,
            range_end: 2,
            packed_start: 0,
            packed_end: packed.serialized.len() as u64,
        };
        (root, store, term)
    }

    #[test]
    fn referenced_term_rejects_wrong_length_before_appending_bytes() {
        let (_root, store, term) = selected_term_fixture(11);
        let mut output = b"unchanged".to_vec();
        assert!(append_referenced_term_bytes(&store, &term, &mut output).is_err());
        assert_eq!(output, b"unchanged");
    }

    #[tokio::test]
    async fn dropping_worker_removes_waiter_before_capacity_is_released() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        let pool = crate::admission::BoundedPool::new(std::num::NonZeroUsize::new(1).unwrap());
        let occupied = pool.acquire().await.unwrap();
        let waiting_pool = pool.clone();
        let resumed = Arc::new(AtomicBool::new(false));
        let worker_resumed = resumed.clone();
        let (started, waiting) = tokio::sync::oneshot::channel();
        let worker = ReferencedFileWorker(tokio::spawn(async move {
            started.send(()).unwrap();
            let _permit = waiting_pool.acquire().await.unwrap();
            worker_resumed.store(true, Ordering::SeqCst);
        }));
        let aborted = worker.0.abort_handle();
        waiting.await.unwrap();
        drop(worker);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !aborted.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued producer must exit while capacity remains occupied");
        assert_eq!(pool.available_permits(), 0);
        drop(occupied);
        assert_eq!(pool.available_permits(), pool.capacity());
        assert!(!resumed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn dropped_native_stream_cancels_admission_wait_without_reading_source() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        let (_root, store, term) = selected_term_fixture(12);
        let pool = crate::admission::BoundedPool::new(std::num::NonZeroUsize::new(1).unwrap());
        let occupied = pool.acquire().await.unwrap();
        let read = Arc::new(AtomicBool::new(false));
        let observed = read.clone();
        let path = store
            .local_path_for_key(&xorb_object_key(&term.hash).unwrap())
            .unwrap();
        crate::object_store::set_before_local_object_read_hook(path, move || {
            observed.store(true, Ordering::SeqCst);
        });
        let record = shardline_index::FileRecord {
            file_id: "a".repeat(64),
            content_hash: "c".repeat(64),
            total_bytes: 12,
            chunk_size: 0,
            storage_repr: shardline_index::StorageRepresentation::WholeFileV1,
            repository_scope: None,
            chunks: vec![term],
        };
        let stream = referenced_file_record_byte_stream(store, record, pool.clone()).unwrap();
        // On this single-thread runtime the producer runs until its admission wait.
        tokio::task::yield_now().await;
        drop(stream);
        tokio::task::yield_now().await;
        drop(occupied);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            crate::admission::blocking_work(&pool, || Ok(())),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            !read.load(Ordering::SeqCst),
            "cancelled queued work must not fetch a source"
        );
        assert_eq!(pool.available_permits(), pool.capacity());
    }

    #[test]
    fn referenced_term_rejects_wrong_length_before_sending_bytes() {
        let (_root, store, term) = selected_term_fixture(11);
        let (sender, mut receiver) = mpsc::channel(2);
        assert!(stream_referenced_term(&store, &term, &sender).is_err());
        assert!(
            receiver.try_recv().is_err(),
            "invalid term must emit no successful frame"
        );
    }

    #[test]
    fn referenced_term_preserves_valid_selected_bytes_in_both_paths() {
        let (_root, store, term) = selected_term_fixture(12);
        let mut output = Vec::new();
        append_referenced_term_bytes(&store, &term, &mut output).unwrap();
        assert_eq!(output, b"second chunk");
        let (sender, mut receiver) = mpsc::channel(2);
        stream_referenced_term(&store, &term, &sender).unwrap();
        assert_eq!(
            receiver.try_recv().unwrap().unwrap().as_ref(),
            b"second chunk"
        );
        assert!(receiver.try_recv().is_err());
    }

    // -----------------------------------------------------------------------
    // map_xorb_visit_error_server
    // -----------------------------------------------------------------------

    #[test]
    fn map_xorb_visit_error_parse_hash_mismatch() {
        let err = XorbVisitError::Parse(XorbParseError::HashMismatch);
        let result = map_xorb_visit_error_server(err);
        assert!(matches!(result, ServerError::XorbHashMismatch));
    }

    #[test]
    fn map_xorb_visit_error_parse_other_error() {
        // Non-HashMismatch parse errors map to InvalidSerializedXorb
        let io_err = std::io::Error::other("test io");
        let err = XorbVisitError::Parse(XorbParseError::Io(io_err));
        let result = map_xorb_visit_error_server(err);
        assert!(matches!(result, ServerError::InvalidSerializedXorb));
    }

    #[test]
    fn map_xorb_visit_error_visitor_passthrough() {
        let err = XorbVisitError::Visitor(ServerError::NotFound);
        let result = map_xorb_visit_error_server(err);
        assert!(matches!(result, ServerError::NotFound));
    }

    #[test]
    fn map_xorb_visit_error_visitor_other_error() {
        let err = XorbVisitError::Visitor(ServerError::Overflow);
        let result = map_xorb_visit_error_server(err);
        assert!(matches!(result, ServerError::Overflow));
    }

    // -----------------------------------------------------------------------
    // push_optional_chunk_container_key
    // -----------------------------------------------------------------------

    #[test]
    fn push_optional_chunk_key_valid_hash() {
        let hash = "ab".repeat(32);
        let mut keys = Vec::new();
        let result = push_optional_chunk_container_key(&mut keys, &hash);
        assert!(result.is_ok());
        assert_eq!(keys.len(), 1);
        let expected_key = xorb_object_key(&hash).unwrap();
        assert_eq!(keys[0], expected_key);
    }

    #[test]
    fn push_optional_chunk_key_deduplicates() {
        let hash = "cd".repeat(32);
        let mut keys = Vec::new();
        push_optional_chunk_container_key(&mut keys, &hash).unwrap();
        push_optional_chunk_container_key(&mut keys, &hash).unwrap();
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn push_optional_chunk_key_rejects_invalid_hash() {
        let mut keys = Vec::new();
        let result = push_optional_chunk_container_key(&mut keys, "not-a-hex-hash");
        assert!(result.is_err());
    }

    #[test]
    fn push_optional_chunk_key_rejects_uppercase_hash() {
        let mut keys = Vec::new();
        let result = push_optional_chunk_container_key(&mut keys, &"AA".repeat(32));
        assert!(result.is_err());
    }

    #[test]
    fn push_optional_chunk_key_rejects_short_hash() {
        let mut keys = Vec::new();
        let result = push_optional_chunk_container_key(&mut keys, "abc");
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // referenced_term_object_key
    // -----------------------------------------------------------------------

    #[test]
    fn referenced_term_object_key_valid_hash() {
        let hash = "ef".repeat(32);
        let result = referenced_term_object_key(&hash);
        assert!(result.is_ok());
        let expected = xorb_object_key(&hash).unwrap();
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn referenced_term_object_key_invalid_hash() {
        let result = referenced_term_object_key("invalid");
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // owns_protocol_object
    // -----------------------------------------------------------------------

    #[test]
    fn owns_protocol_object_with_xorb_key() {
        let hash = "01".repeat(32);
        let key = xorb_object_key(&hash).unwrap();
        let result = owns_protocol_object(&key);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn owns_protocol_object_with_non_xorb_key() {
        let key = ObjectKey::parse("shards/ab/some.shard").unwrap();
        let result = owns_protocol_object(&key);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn owns_protocol_object_with_chunk_key() {
        let key = ObjectKey::parse("ab/abcdef1234567890").unwrap();
        let result = owns_protocol_object(&key);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn owns_protocol_object_with_random_key() {
        let key = ObjectKey::parse("some/arbitrary/path").unwrap();
        let result = owns_protocol_object(&key);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    // -----------------------------------------------------------------------
    // append_referenced_term_bytes — early error paths
    // -----------------------------------------------------------------------

    #[test]
    fn referenced_term_reads_validate_the_container_before_appending_selected_bytes() {
        use shardline_storage::{ObjectBody, ObjectIntegrity};

        let root = tempfile::tempdir().unwrap();
        let store = ServerObjectStore::local(root.path()).unwrap();
        let packed = crate::upload_ingest::xorb_packer::pack_chunks_into_xorb(&[
            (b"first chunk".to_vec(), 0),
            (b"second chunk".to_vec(), 11),
        ])
        .unwrap();
        let key = xorb_object_key(&packed.xorb_hash_hex).unwrap();
        let term = FileChunkRecord {
            hash: packed.xorb_hash_hex,
            offset: 0,
            length: 12,
            range_start: 1,
            range_end: 2,
            packed_start: 0,
            packed_end: packed.serialized.len() as u64,
        };
        let write = |bytes: &[u8]| {
            store
                .put_overwrite(
                    &key,
                    ObjectBody::Borrowed(bytes),
                    &ObjectIntegrity::new(
                        crate::local_backend::chunk_hash(bytes),
                        bytes.len() as u64,
                    ),
                )
                .unwrap();
        };
        write(&packed.serialized);
        let mut output = b"prefix:".to_vec();
        append_referenced_term_bytes(&store, &term, &mut output).unwrap();
        assert_eq!(output, b"prefix:second chunk");

        let mut corrupt = packed.serialized;
        *corrupt.first_mut().unwrap() ^= 0xff;
        write(&corrupt);
        let mut output = b"unchanged".to_vec();
        assert!(append_referenced_term_bytes(&store, &term, &mut output).is_err());
        assert_eq!(output, b"unchanged");
    }

    #[test]
    fn append_referenced_term_bytes_empty_range_returns_error() {
        let store = crate::object_store::ServerObjectStore::blackhole();
        let term = FileChunkRecord {
            hash: "ab".repeat(32),
            offset: 0,
            length: 1024,
            range_start: 10,
            range_end: 10, // empty range: range_end == range_start
            packed_start: 0,
            packed_end: 1024,
        };
        let mut output = Vec::new();
        let result = append_referenced_term_bytes(&store, &term, &mut output);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            ServerError::InvalidSerializedShard(
                crate::InvalidSerializedShardError::NativeXetTermEmptyOrInvertedChunkRange
            )
        ));
    }

    #[test]
    fn append_referenced_term_bytes_inverted_range_returns_error() {
        let store = crate::object_store::ServerObjectStore::blackhole();
        let term = FileChunkRecord {
            hash: "ab".repeat(32),
            offset: 0,
            length: 1024,
            range_start: 20,
            range_end: 10, // inverted: end < start
            packed_start: 0,
            packed_end: 1024,
        };
        let mut output = Vec::new();
        let result = append_referenced_term_bytes(&store, &term, &mut output);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            ServerError::InvalidSerializedShard(
                crate::InvalidSerializedShardError::NativeXetTermEmptyOrInvertedChunkRange
            )
        ));
    }

    // -----------------------------------------------------------------------
    // visit_protocol_object_member_chunks
    // -----------------------------------------------------------------------

    #[test]
    fn visit_protocol_object_member_chunks_returns_ok_without_visitor_error() {
        // Using a non-xorb key with blackhole store; owns_protocol_object returns
        // false for non-xorb keys, so visit_stored_xorb_chunk_hashes is not called.
        let store = crate::object_store::ServerObjectStore::blackhole();
        let key = ObjectKey::parse("some/random/key").unwrap();
        let mut visited = false;
        let result = visit_protocol_object_member_chunks(&store, &key, |_hash| {
            visited = true;
            Err(ServerError::Overflow)
        });
        // The key is not a protocol object (not under xorbs namespace), so
        // visit_stored_xorb_chunk_hashes is never called and the function returns Ok.
        assert!(result.is_ok());
        assert!(!visited);
    }

    // -----------------------------------------------------------------------
    // owns_protocol_object — key with xorb namespace
    // -----------------------------------------------------------------------

    #[test]
    fn owns_protocol_object_returns_false_for_invalid_hash_key() {
        let key = ObjectKey::parse("aa/short").unwrap();
        let result = owns_protocol_object(&key);
        assert!(result.is_ok());
        // The key doesn't match xorb pattern
        assert!(!result.unwrap());
    }
}
