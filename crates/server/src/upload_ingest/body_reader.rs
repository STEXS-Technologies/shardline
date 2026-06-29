use std::{
    num::NonZeroUsize,
    pin::Pin,
};

use axum::{
    Error as AxumError,
    body::{Body, Bytes, HttpBody},
};
use futures_util::stream::{self, Stream, StreamExt};

use crate::{
    ServerError,
    overflow::checked_add,
};

type BodyChunkResult = Result<Bytes, AxumError>;
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
            stream: Box::pin(body.into_data_stream()),
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

    /// Returns the expected total body size when the transport declared one.
    pub(crate) const fn expected_total_bytes(&self) -> Option<usize> {
        self.expected_total_bytes
    }

    /// Reads the next body chunk while enforcing the configured total byte limit.
    pub(crate) async fn next_bytes(&mut self) -> Result<Option<Bytes>, ServerError> {
        let Some(chunk) = self.stream.next().await else {
            return Ok(None);
        };
        let chunk = chunk.map_err(ServerError::RequestBodyRead)?;
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
