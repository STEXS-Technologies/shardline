//! Download sessions for the sdx CAS read path (M2a).
//!
//! [`DownloadSession`] downloads a file (or a byte range of a file) by its
//! 64-hex `file_id` — the library core is file_id-addressed; path resolution
//! arrives with the §2.5 server metadata endpoints in M5
//! (`docs/SDX_PLAN.md` §4.3). Downloads are sequential and unbuffered-to-disk:
//! the reconstructed bytes are assembled in memory and written to `dest`.

use std::{ops::RangeInclusive, path::Path, sync::Arc};

use crate::{
    auth::TokenService,
    error::SdxError,
    hash::parse_xet_hash_hex,
    reconstruction,
    transfer::{ByteRange, TransferClient},
};

/// Shared state between a [`DownloadSession`] and its owning [`crate::XetClient`].
pub(crate) struct DownloadSessionInner {
    pub(crate) transfer: TransferClient,
    pub(crate) tokens: TokenService,
    pub(crate) api_base: String,
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
