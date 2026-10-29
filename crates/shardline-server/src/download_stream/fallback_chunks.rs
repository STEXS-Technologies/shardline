//! Sequential, authenticated legacy chunk reads with bounded stream-local reuse.
use axum::body::Bytes;
use futures_util::{TryStreamExt, stream};
use shardline_index::xet_hash_hex_string;
use shardline_protocol::ByteRange;
use shardline_storage::ObjectKey;

use super::{ServerByteStream, object_byte_stream};
use crate::{
    ServerError, error::ObjectStoreError, local_backend::chunk_hash,
    object_store::ServerObjectStore,
};

const MAX_REUSED_CHUNK_BYTES: usize = 64 * 1024;

pub(super) struct Term {
    pub key: ObjectKey,
    pub storage_length: u64,
    pub raw_length: u64,
    pub expected_hash: String,
    pub range: ByteRange,
    pub compressed: bool,
}

struct VerifiedChunk {
    key: ObjectKey,
    storage_length: u64,
    raw_length: u64,
    expected_hash: String,
    compressed: bool,
    bytes: Bytes,
}

impl VerifiedChunk {
    fn matches(&self, term: &Term) -> bool {
        self.key == term.key
            && self.storage_length == term.storage_length
            && self.raw_length == term.raw_length
            && self.expected_hash == term.expected_hash
            && self.compressed == term.compressed
    }
}

pub(super) fn byte_stream(objects: ServerObjectStore, terms: Vec<Term>) -> ServerByteStream {
    Box::pin(stream::try_unfold(
        (terms.into_iter(), None::<VerifiedChunk>),
        move |(mut terms, previous)| {
            let objects = objects.clone();
            async move {
                let Some(term) = terms.next() else {
                    return Ok(None);
                };
                let data = match previous {
                    Some(previous) if previous.matches(&term) => previous.bytes,
                    _ => read_verified(objects, &term).await?,
                };
                let start = usize::try_from(term.range.start())?;
                let end = usize::try_from(term.range.end_inclusive())?;
                let selected = data.get(start..=end).ok_or(ServerError::Overflow)?;
                let complete = start == 0 && selected.len() == data.len();
                // Share a complete emitted buffer, rather than retaining an extra
                // allocation. Partial reads keep their previous small output and
                // do not pin a complete decoded chunk for a later term.
                let output = if complete {
                    data.clone()
                } else {
                    Bytes::copy_from_slice(selected)
                };
                let next_verified = if complete && data.len() <= MAX_REUSED_CHUNK_BYTES {
                    Some(VerifiedChunk {
                        key: term.key,
                        storage_length: term.storage_length,
                        raw_length: term.raw_length,
                        expected_hash: term.expected_hash,
                        compressed: term.compressed,
                        bytes: data,
                    })
                } else {
                    None
                };
                Ok::<_, ServerError>(Some((output, (terms, next_verified))))
            }
        },
    ))
}

async fn read_verified(objects: ServerObjectStore, term: &Term) -> Result<Bytes, ServerError> {
    let compressed = object_byte_stream(objects, term.key.clone(), term.storage_length)
        .await?
        .try_fold(Vec::new(), |mut acc, chunk| async move {
            acc.extend_from_slice(&chunk);
            Ok(acc)
        })
        .await?;
    let data = if term.compressed {
        const MAX_DECOMPRESSED_CHUNK: u64 = 2 * 1024 * 1024;
        let length = compressed
            .first_chunk::<4>()
            .map(|header| u64::from(u32::from_le_bytes(*header)))
            .unwrap_or(u64::MAX);
        if length > MAX_DECOMPRESSED_CHUNK {
            return Err(ServerError::Overflow);
        }
        lz4_flex::decompress_size_prepended(&compressed).map_err(|error| {
            tracing::warn!(compressed_len = compressed.len(), %error, "failed to decompress chunk");
            ServerError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
        })?
    } else {
        compressed
    };
    let actual = xet_hash_hex_string(chunk_hash(&data));
    if actual != term.expected_hash {
        tracing::warn!(expected = %term.expected_hash, %actual, chunk_len = data.len(), "chunk integrity mismatch");
        return Err(ServerError::ObjectStore(
            ObjectStoreError::StoredLengthMismatch,
        ));
    }
    // Do not retain spare collection capacity behind a small emitted buffer.
    Ok(Bytes::from(data.into_boxed_slice()))
}
