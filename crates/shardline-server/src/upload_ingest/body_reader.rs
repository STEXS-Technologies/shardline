use std::{num::NonZeroUsize, pin::Pin};

use axum::body::{Body, Bytes, HttpBody};
use futures_util::stream::{self, Stream, StreamExt};

use crate::{ServerError, overflow::checked_add};

type BodyChunkResult = Result<Bytes, ServerError>;
type BoxedBodyStream = Pin<Box<dyn Stream<Item = BodyChunkResult> + Send>>;

pub(super) enum ChunkBuffer {
    Pooled(Bytes),
    Shared(Bytes),
}

impl ChunkBuffer {
    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Pooled(bytes) => bytes.as_ref(),
            Self::Shared(bytes) => bytes.as_ref(),
        }
    }

    pub(super) const fn len(&self) -> usize {
        match self {
            Self::Pooled(bytes) => bytes.len(),
            Self::Shared(bytes) => bytes.len(),
        }
    }
}

/// Bounded streaming reader for request bodies.
pub(crate) struct RequestBodyReader {
    stream: BoxedBodyStream,
    max_bytes: Option<u64>,
    expected_total_bytes: Option<usize>,
    read_bytes: u64,
}

impl RequestBodyReader {
    /// Creates a reader over an HTTP request body with a maximum accepted byte count.
    pub(crate) fn from_body(body: Body, max_bytes: NonZeroUsize) -> Result<Self, ServerError> {
        let size_hint = body.size_hint();
        let expected_total_bytes = size_hint
            .exact()
            .or_else(|| size_hint.upper())
            .map(usize::try_from)
            .transpose()?;
        if expected_total_bytes.is_some_and(|expected| expected > max_bytes.get()) {
            return Err(ServerError::RequestBodyTooLarge);
        }
        Ok(Self {
            stream: Box::pin(
                body.into_data_stream()
                    .map(|chunk| chunk.map_err(ServerError::RequestBodyRead)),
            ),
            max_bytes: Some(u64::try_from(max_bytes.get())?),
            expected_total_bytes,
            read_bytes: 0,
        })
    }

    /// Creates a reader over in-memory bytes for tests and non-HTTP callers.
    pub(crate) fn from_bytes(bytes: Bytes) -> Self {
        Self {
            stream: Box::pin(stream::once(async move { Ok(bytes) })),
            max_bytes: None,
            expected_total_bytes: None,
            read_bytes: 0,
        }
    }

    /// Creates a reader over an internal byte stream.
    pub(crate) fn from_stream(
        stream: impl Stream<Item = Result<Bytes, ServerError>> + Send + 'static,
    ) -> Self {
        Self {
            stream: Box::pin(stream),
            max_bytes: None,
            expected_total_bytes: None,
            read_bytes: 0,
        }
    }

    /// Reads the next body chunk while enforcing the configured total byte limit.
    pub(crate) async fn next_bytes(&mut self) -> Result<Option<Bytes>, ServerError> {
        let Some(chunk) = self.stream.next().await else {
            return Ok(None);
        };
        let chunk = chunk?;
        let chunk_len = u64::try_from(chunk.len())?;
        self.read_bytes = checked_add(self.read_bytes, chunk_len)?;
        if self
            .max_bytes
            .is_some_and(|max_bytes| self.read_bytes > max_bytes)
        {
            return Err(ServerError::RequestBodyTooLarge);
        }

        Ok(Some(chunk))
    }
}

/// Reads a bounded request body into memory without server-side disk staging.
pub(crate) async fn read_body_to_bytes(
    reader: &mut RequestBodyReader,
) -> Result<Vec<u8>, ServerError> {
    let mut body = reader
        .expected_total_bytes
        .map_or_else(Vec::new, Vec::with_capacity);
    while let Some(bytes) = reader.next_bytes().await? {
        let expected_len = body
            .len()
            .checked_add(bytes.len())
            .ok_or(ServerError::Overflow)?;
        body.try_reserve(bytes.len())
            .map_err(|_error| ServerError::RequestBodyTooLarge)?;
        body.extend_from_slice(&bytes);
        debug_assert_eq!(body.len(), expected_len);
    }

    Ok(body)
}

#[cfg(test)]
mod tests {
    use axum::body::Bytes;

    use super::{ChunkBuffer, RequestBodyReader, read_body_to_bytes};

    // ------------------------------------------------------------------
    // ChunkBuffer
    // ------------------------------------------------------------------

    #[test]
    fn pooled_chunk_buffer_as_slice_returns_bytes() {
        let buf = ChunkBuffer::Pooled(Bytes::from_static(b"hello"));
        assert_eq!(buf.as_slice(), b"hello");
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn shared_chunk_buffer_as_slice_returns_bytes() {
        let buf = ChunkBuffer::Shared(Bytes::from_static(b"world"));
        assert_eq!(buf.as_slice(), b"world");
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn pooled_chunk_buffer_empty() {
        let buf = ChunkBuffer::Pooled(Bytes::new());
        assert_eq!(buf.as_slice(), b"");
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn shared_chunk_buffer_empty() {
        let buf = ChunkBuffer::Shared(Bytes::new());
        assert_eq!(buf.as_slice(), b"");
        assert_eq!(buf.len(), 0);
    }

    // ------------------------------------------------------------------
    // RequestBodyReader::from_bytes
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn from_bytes_returns_content_on_first_call() {
        let mut reader = RequestBodyReader::from_bytes(Bytes::from_static(b"hello"));
        let chunk = reader.next_bytes().await.unwrap();
        assert!(chunk.is_some());
        assert_eq!(chunk.unwrap().as_ref(), b"hello");
    }

    #[tokio::test]
    async fn from_bytes_returns_none_on_second_call() {
        let mut reader = RequestBodyReader::from_bytes(Bytes::from_static(b"hello"));
        let _first = reader.next_bytes().await.unwrap();
        let second = reader.next_bytes().await.unwrap();
        assert!(second.is_none());
    }

    #[tokio::test]
    async fn from_bytes_empty_body_returns_empty_chunk_then_none() {
        // `from_bytes` wraps data in `stream::once`, so even an empty Bytes
        // yields one `Some(Bytes::new())` before the stream ends.
        let mut reader = RequestBodyReader::from_bytes(Bytes::new());
        let result = reader.next_bytes().await.unwrap();
        assert!(result.is_some());
        assert!(result.unwrap().is_empty());
        assert!(reader.next_bytes().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn from_bytes_large_content() {
        let data = vec![0xAB_u8; 65536];
        let bytes = Bytes::from(data.clone());
        let mut reader = RequestBodyReader::from_bytes(bytes);

        let chunk = reader.next_bytes().await.unwrap();
        assert!(chunk.is_some());
        assert_eq!(chunk.unwrap().as_ref(), data.as_slice());

        // Stream exhausted.
        assert!(reader.next_bytes().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn from_bytes_multiple_chunks_are_none_after_exhaustion() {
        // from_bytes produces a single-chunk stream
        let mut reader = RequestBodyReader::from_bytes(Bytes::from_static(b"only one chunk"));
        assert!(reader.next_bytes().await.unwrap().is_some());
        assert!(reader.next_bytes().await.unwrap().is_none());
        assert!(reader.next_bytes().await.unwrap().is_none());
    }

    // ------------------------------------------------------------------
    // read_body_to_bytes
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn read_body_to_bytes_returns_full_content() {
        let mut reader = RequestBodyReader::from_bytes(Bytes::from_static(b"hello world"));
        let body = read_body_to_bytes(&mut reader).await.unwrap();
        assert_eq!(body, b"hello world");
    }

    #[tokio::test]
    async fn read_body_to_bytes_empty_body() {
        let mut reader = RequestBodyReader::from_bytes(Bytes::new());
        let body = read_body_to_bytes(&mut reader).await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn read_body_to_bytes_large_content() {
        let data = vec![0xCD_u8; 100_000];
        let mut reader = RequestBodyReader::from_bytes(Bytes::from(data.clone()));
        let body = read_body_to_bytes(&mut reader).await.unwrap();
        assert_eq!(body, data.as_slice());
    }

    // ------------------------------------------------------------------
    // RequestBodyReader::from_body size-limit path
    // ------------------------------------------------------------------

    #[test]
    fn from_body_rejects_body_exceeding_size_hint() {
        use std::num::NonZeroUsize;

        use axum::body::Body;

        let body = Body::from(vec![0u8; 100]);
        let max_bytes = NonZeroUsize::new(50).unwrap();
        let result = RequestBodyReader::from_body(body, max_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn from_body_accepts_body_within_size_hint() {
        use std::num::NonZeroUsize;

        use axum::body::Body;

        let body = Body::from(vec![0u8; 50]);
        let max_bytes = NonZeroUsize::new(100).unwrap();
        let result = RequestBodyReader::from_body(body, max_bytes);
        assert!(result.is_ok());
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn read_body_to_bytes_idempotent_with_empty_reader() {
        let mut reader = RequestBodyReader::from_bytes(Bytes::new());
        let body1 = read_body_to_bytes(&mut reader).await.unwrap();
        let body2 = read_body_to_bytes(&mut reader).await.unwrap();
        assert!(body1.is_empty());
        assert!(body2.is_empty());
    }

    #[test]
    fn chunk_buffer_len_matches_as_slice() {
        let data = b"chunk data for testing";
        let pooled = ChunkBuffer::Pooled(Bytes::from_static(data));
        let shared = ChunkBuffer::Shared(Bytes::from_static(data));
        assert_eq!(pooled.len(), pooled.as_slice().len());
        assert_eq!(shared.len(), shared.as_slice().len());
        assert_eq!(pooled.len(), shared.len());
    }

    // ------------------------------------------------------------------
    // Runtime body-size enforcement via from_body
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn from_body_enforces_runtime_limit() {
        use std::num::NonZeroUsize;

        use axum::body::Body;
        use futures_util::stream;

        // Create a streaming body with no upper size hint so the initial
        // `from_body` check passes, then have the stream yield enough bytes
        // to exceed the runtime limit.
        let chunks: Vec<Result<Bytes, axum::Error>> = vec![
            Ok(Bytes::from(vec![0u8; 30])),
            Ok(Bytes::from(vec![0u8; 30])),
        ];
        let body = Body::from_stream(stream::iter(chunks));
        let max_bytes = NonZeroUsize::new(40).unwrap();
        let mut reader = RequestBodyReader::from_body(body, max_bytes).unwrap();

        // First read succeeds (30 bytes < 40)
        let first = reader.next_bytes().await.unwrap();
        assert!(first.is_some());

        // Second read puts us at 60 > 40 → error
        let second = reader.next_bytes().await;
        assert!(matches!(
            second,
            Err(crate::ServerError::RequestBodyTooLarge)
        ));
    }

    #[tokio::test]
    async fn from_body_stream_within_limit_succeeds() {
        use std::num::NonZeroUsize;

        use axum::body::Body;
        use futures_util::stream;

        let chunks: Vec<Result<Bytes, axum::Error>> = vec![
            Ok(Bytes::from(vec![0u8; 20])),
            Ok(Bytes::from(vec![0u8; 20])),
        ];
        let body = Body::from_stream(stream::iter(chunks));
        let max_bytes = NonZeroUsize::new(40).unwrap(); // exactly at limit
        let mut reader = RequestBodyReader::from_body(body, max_bytes).unwrap();

        let first = reader.next_bytes().await.unwrap();
        assert!(first.is_some());
        let second = reader.next_bytes().await.unwrap();
        assert!(second.is_some());
        let third = reader.next_bytes().await.unwrap();
        assert!(third.is_none());
    }
}
