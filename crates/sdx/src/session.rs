//! Download sessions for the sdx CAS read path (M2a / M2b1).
//!
//! [`DownloadSession`] downloads a file (or a byte range of a file) by its
//! 64-hex `file_id` — the library core is file_id-addressed; path resolution
//! arrives with the §2.5 server metadata endpoints in M5
//! (`docs/SDX_PLAN.md` §4.3). Downloads are sequential and unbuffered-to-disk:
//! the reconstructed bytes are assembled in memory and written to `dest`.
//!
//! M2b1 adds the pull-based streaming surface (`download_stream`,
//! `download_unordered_stream`, `download_to_writer`, `download_bytes`),
//! mirroring upstream `xet-data`'s `FileReconstructor` (`docs/SDX_PLAN.md`
//! §4.4.1).

use std::{
    ops::{Range, RangeInclusive},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;

use crate::{
    auth::TokenService,
    cache::ChunkCache,
    error::SdxError,
    hash::parse_xet_hash_hex,
    reconstruction,
    stream::{
        BufferSemaphore, DownloadStream, FileReconstructor, StreamContext, StreamLimits,
        UnorderedDownloadStream,
    },
    transfer::{ByteRange, TransferClient},
};

/// Shared state between a [`DownloadSession`] and its owning [`crate::XetClient`].
pub(crate) struct DownloadSessionInner {
    pub(crate) transfer: TransferClient,
    pub(crate) tokens: TokenService,
    pub(crate) api_base: String,
    pub(crate) buffer_semaphore: Arc<BufferSemaphore>,
    pub(crate) active_downloads: Arc<AtomicU64>,
    pub(crate) download_permits: Arc<tokio::sync::Semaphore>,
    pub(crate) limits: StreamLimits,
    /// Optional on-disk chunk cache (M2b2), shared by all streams.
    pub(crate) chunk_cache: Option<Arc<ChunkCache>>,
    /// Count of ranged xorb transfer requests (for observability/E2E).
    pub(crate) xorb_fetch_count: Arc<AtomicU64>,
    /// CDC target chunk size for uploads (M3b), default 64 KiB.
    pub(crate) upload_chunk_size: usize,
}

impl DownloadSessionInner {
    /// Builds the streaming pipeline context shared by this session, resolving
    /// the process-global blocking runtime lazily.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the dedicated blocking runtime cannot be
    /// started.
    pub(crate) fn stream_context(&self) -> Result<StreamContext, SdxError> {
        Ok(StreamContext {
            transfer: self.transfer.clone(),
            api_base: self.api_base.clone(),
            buffer_semaphore: self.buffer_semaphore.clone(),
            active_downloads: self.active_downloads.clone(),
            download_permits: self.download_permits.clone(),
            limits: self.limits.clone(),
            chunk_cache: self.chunk_cache.clone(),
            xorb_fetch_count: self.xorb_fetch_count.clone(),
            #[cfg(not(target_family = "wasm"))]
            blocking_runtime: crate::stream::global_blocking_runtime()?,
        })
    }

    /// Builds a [`FileReconstructor`] for `file_id`, validating the identifier
    /// and resolving the read token.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when `file_id` is malformed, `range` is inverted
    /// (`start > end`), token issuance fails, or the blocking runtime cannot
    /// be resolved.
    pub(crate) async fn build_reconstructor(
        &self,
        file_id: &str,
        range: Option<Range<u64>>,
    ) -> Result<FileReconstructor, SdxError> {
        // Validate the file identifier before spending a token issuance.
        let _ = parse_xet_hash_hex(file_id)?;
        if let Some(range) = &range
            && range.start > range.end
        {
            return Err(SdxError::InvalidByteRange {
                start: range.start,
                end: range.end,
            });
        }
        let token = self.tokens.read_token().await?;
        let mut reconstructor =
            FileReconstructor::new(self.stream_context()?, file_id.to_owned(), token.token);
        if let Some(range) = range {
            reconstructor = reconstructor.with_byte_range(range);
        }
        Ok(reconstructor)
    }

    /// Returns the number of ranged xorb transfer requests issued so far.
    #[must_use]
    pub(crate) fn xorb_fetch_count(&self) -> u64 {
        self.xorb_fetch_count.load(Ordering::Relaxed)
    }
}

/// Sequential download session over one repository.
///
/// Clone is cheap: sessions share the underlying HTTP client and token service.
#[derive(Clone)]
pub struct DownloadSession {
    pub(crate) inner: Arc<DownloadSessionInner>,
}

impl DownloadSession {
    /// Downloads the file identified by `file_id` (64 lowercase hex
    /// characters) to `dest`, returning the number of bytes written.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when `file_id` is malformed, token issuance fails,
    /// the reconstruction or xorb fetch fails, or the file cannot be written.
    pub async fn download_file(&self, file_id: &str, dest: &Path) -> Result<u64, SdxError> {
        self.download(file_id, dest, None).await
    }

    /// Downloads the inclusive byte range `range` of the file identified by
    /// `file_id` to `dest`, returning the number of bytes written.
    ///
    /// Range ends are inclusive per the Xet reconstruction contract
    /// (`docs/PROTOCOL_CONFORMANCE.md` "Range Semantics").
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when `file_id` or the range is invalid, token
    /// issuance fails, the reconstruction or xorb fetch fails, the range is
    /// past the end of the file, or the file cannot be written.
    pub async fn download_range(
        &self,
        file_id: &str,
        range: RangeInclusive<u64>,
        dest: &Path,
    ) -> Result<u64, SdxError> {
        let start = *range.start();
        let end = *range.end();
        if start > end {
            return Err(SdxError::InvalidByteRange { start, end });
        }
        self.download(file_id, dest, Some(ByteRange::new(start, end)))
            .await
    }

    /// Returns a pull-based streaming download of `file_id`.
    ///
    /// `range` is an end-exclusive byte range; `None` means the full file. The
    /// returned [`DownloadStream`] yields `Bytes` chunks via
    /// [`DownloadStream::next`] / [`DownloadStream::blocking_next`] — the
    /// consumer forwards chunks to a socket/stdout and the file is never
    /// buffered whole. Memory is bounded by the client's byte-denominated
    /// buffer semaphore (see [`crate::XetClientBuilder::with_buffer_semaphore`]).
    ///
    /// The background reconstruction task is spawned paused and auto-starts on
    /// the first `next()`/`blocking_next()`; dropping the stream cancels it.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when `file_id` is malformed, token issuance fails,
    /// or `range` is inverted (`start > end`). Reconstruction/fetch errors
    /// surface on the stream's `next()` calls.
    pub async fn download_stream(
        &self,
        file_id: &str,
        range: Option<Range<u64>>,
    ) -> Result<DownloadStream, SdxError> {
        let reconstructor = self.build_reconstructor(file_id, range).await?;
        Ok(reconstructor.reconstruct_to_stream())
    }

    /// Returns a pull-based streaming download of `file_id` that yields
    /// `(file_offset, Bytes)` pairs in completion order.
    ///
    /// Progress probes: [`UnorderedDownloadStream::total_bytes_expected`],
    /// [`UnorderedDownloadStream::bytes_in_progress`], and
    /// [`UnorderedDownloadStream::bytes_completed`].
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when `file_id` is malformed, token issuance fails,
    /// or `range` is inverted (`start > end`).
    pub async fn download_unordered_stream(
        &self,
        file_id: &str,
        range: Option<Range<u64>>,
    ) -> Result<UnorderedDownloadStream, SdxError> {
        let reconstructor = self.build_reconstructor(file_id, range).await?;
        Ok(reconstructor.reconstruct_to_unordered_stream())
    }

    /// Downloads `file_id` into the `std::io::Write` sink `writer` (a file,
    /// stdout, ...), running it on a `spawn_blocking` thread and returning the
    /// number of bytes written.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when `file_id` is malformed, token issuance fails,
    /// a fetch or reconstruction fails, or the writer fails.
    pub async fn download_to_writer<W: std::io::Write + Send + 'static>(
        &self,
        file_id: &str,
        writer: W,
    ) -> Result<u64, SdxError> {
        let reconstructor = self.build_reconstructor(file_id, None).await?;
        reconstructor.reconstruct_to_writer(writer).await
    }

    /// Downloads the whole file into a single [`Bytes`] (in-memory convenience
    /// path; fine for modest files).
    ///
    /// Equivalent to draining [`download_stream`](Self::download_stream) with
    /// `range = None`.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when `file_id` is malformed, token issuance fails,
    /// or a fetch/reconstruction fails.
    pub async fn download_bytes(&self, file_id: &str) -> Result<Bytes, SdxError> {
        let mut stream = self.download_stream(file_id, None).await?;
        let mut out = bytes::BytesMut::new();
        while let Some(chunk) = stream.next().await? {
            out.extend_from_slice(&chunk);
        }
        Ok(out.freeze())
    }

    /// Builds a [`FileReconstructor`] for `file_id`, validating the identifier
    /// and resolving the read token.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when `file_id` is malformed or token issuance fails.
    async fn build_reconstructor(
        &self,
        file_id: &str,
        range: Option<Range<u64>>,
    ) -> Result<FileReconstructor, SdxError> {
        self.inner.build_reconstructor(file_id, range).await
    }

    async fn download(
        &self,
        file_id: &str,
        dest: &Path,
        range: Option<ByteRange>,
    ) -> Result<u64, SdxError> {
        parse_xet_hash_hex(file_id)?;
        let token = self.inner.tokens.read_token().await?;
        let file = reconstruction::reconstruct(
            &self.inner.transfer,
            &self.inner.api_base,
            &token.token,
            file_id,
            range,
        )
        .await?;
        tokio::fs::write(dest, &file.data).await?;
        Ok(u64::try_from(file.data.len()).unwrap_or(u64::MAX))
    }
}
