use std::borrow::Cow;
use std::fmt::Display;
use std::io::{Cursor, Read, Write, copy};
use std::str::FromStr;

use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use xet_core_structures::xorb_object::byte_grouping::bg4::{bg4_regroup, bg4_split};

use crate::error::CoreError;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum CompressionScheme {
    /// No compression: payloads are stored as-is.
    None = 0,
    /// LZ4 frame compression.
    LZ4 = 1,
    /// Byte-grouping (4-way interleave) followed by LZ4.
    ByteGrouping4LZ4 = 2,
    /// Pick a concrete scheme automatically (`resolve_for_data` currently
    /// resolves to [`CompressionScheme::LZ4`]).
    #[default]
    Auto = 99,
}

impl Display for CompressionScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", Into::<&str>::into(self))
    }
}

impl From<&CompressionScheme> for &'static str {
    fn from(value: &CompressionScheme) -> Self {
        match value {
            CompressionScheme::Auto => "auto",
            CompressionScheme::LZ4 => "lz4",
            CompressionScheme::ByteGrouping4LZ4 => "bg4-lz4",
            CompressionScheme::None => "none",
        }
    }
}

impl From<CompressionScheme> for &'static str {
    fn from(value: CompressionScheme) -> Self {
        From::from(&value)
    }
}

impl TryFrom<u8> for CompressionScheme {
    type Error = CoreError;

    fn try_from(value: u8) -> std::result::Result<Self, CoreError> {
        match value {
            0 => Ok(CompressionScheme::None),
            1 => Ok(CompressionScheme::LZ4),
            2 => Ok(CompressionScheme::ByteGrouping4LZ4),
            99 => Ok(CompressionScheme::Auto),
            _ => Err(CoreError::MalformedData(format!(
                "cannot convert value {value} to CompressionScheme"
            ))),
        }
    }
}

impl FromStr for CompressionScheme {
    type Err = CoreError;

    /// Parses a compression scheme name.
    ///
    /// Accepts `auto` (and empty), `none`, `lz4`, and `bg4-lz4`,
    /// case-insensitively and with surrounding whitespace.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_xet_core::xorb_object::CompressionScheme;
    ///
    /// assert_eq!("lz4".parse::<CompressionScheme>()?, CompressionScheme::LZ4);
    /// assert_eq!(" BG4-LZ4 ".parse::<CompressionScheme>()?, CompressionScheme::ByteGrouping4LZ4);
    /// assert_eq!("".parse::<CompressionScheme>()?, CompressionScheme::Auto);
    /// assert!("gzip".parse::<CompressionScheme>().is_err());
    /// # Ok::<(), shardline_xet_core::CoreError>(())
    /// ```
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "" | "auto" => Ok(CompressionScheme::Auto),
            "none" => Ok(CompressionScheme::None),
            "lz4" => Ok(CompressionScheme::LZ4),
            "bg4-lz4" => Ok(CompressionScheme::ByteGrouping4LZ4),
            _ => Err(CoreError::MalformedData(format!(
                "Invalid compression scheme '{s}'. Valid values are: auto, none, lz4, bg4-lz4."
            ))),
        }
    }
}

impl CompressionScheme {
    pub fn resolve_for_data(&self, _data: &[u8]) -> Self {
        if *self == CompressionScheme::Auto {
            CompressionScheme::LZ4
        } else {
            *self
        }
    }

    /// Compresses a byte slice under this scheme.
    ///
    /// `None` returns the input borrowed (no copy); `Auto` resolves to a
    /// concrete scheme first.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_xet_core::xorb_object::CompressionScheme;
    ///
    /// let data = b"compressible payload compressible payload";
    /// let scheme = CompressionScheme::LZ4;
    ///
    /// let compressed = scheme.compress_from_slice(data)?;
    /// let restored = scheme.decompress_from_slice(&compressed)?;
    /// assert_eq!(&*restored, data);
    /// # Ok::<(), shardline_xet_core::CoreError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`CoreError`] when the underlying compression library fails.
    pub fn compress_from_slice<'a>(&self, data: &'a [u8]) -> Result<Cow<'a, [u8]>, CoreError> {
        Ok(match self {
            CompressionScheme::Auto => {
                return self.resolve_for_data(data).compress_from_slice(data);
            }
            CompressionScheme::None => data.into(),
            CompressionScheme::LZ4 => lz4_compress_from_slice(data).map(Cow::from)?,
            CompressionScheme::ByteGrouping4LZ4 => {
                bg4_lz4_compress_from_slice(data).map(Cow::from)?
            }
        })
    }

    pub fn decompress_from_slice<'a>(&self, data: &'a [u8]) -> Result<Cow<'a, [u8]>, CoreError> {
        Ok(match self {
            CompressionScheme::Auto => {
                return Err(CoreError::MalformedData(
                    "Cannot decompress with Auto scheme".to_string(),
                ));
            }
            CompressionScheme::None => data.into(),
            CompressionScheme::LZ4 => lz4_decompress_from_slice(data).map(Cow::from)?,
            CompressionScheme::ByteGrouping4LZ4 => {
                bg4_lz4_decompress_from_slice(data).map(Cow::from)?
            }
        })
    }

    pub fn decompress_from_reader<R: Read, W: Write>(
        &self,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<u64, CoreError> {
        Ok(match self {
            CompressionScheme::Auto => {
                return Err(CoreError::MalformedData(
                    "Cannot decompress with Auto scheme".to_string(),
                ));
            }
            CompressionScheme::None => copy(reader, writer)?,
            CompressionScheme::LZ4 => lz4_decompress_from_reader(reader, writer)?,
            CompressionScheme::ByteGrouping4LZ4 => bg4_lz4_decompress_from_reader(reader, writer)?,
        })
    }

    /// Decompresses `reader` into `writer` while enforcing a `declared_len`
    /// ceiling on the decompressed output.
    ///
    /// The limit is enforced DURING decompression, not after: the write side is
    /// capped so a chunk whose header lies about its `uncompressed_length` (or a
    /// crafted compressed frame that expands far beyond the declaration) aborts
    /// as soon as the declared size is exceeded, instead of growing an unbounded
    /// buffer first. Returns the number of bytes written to `writer`.
    pub fn decompress_from_reader_bounded<R: Read, W: Write>(
        &self,
        reader: &mut R,
        writer: &mut W,
        declared_len: u64,
    ) -> Result<u64, CoreError> {
        match self {
            CompressionScheme::Auto => Err(CoreError::MalformedData(
                "Cannot decompress with Auto scheme".to_string(),
            )),
            CompressionScheme::None => copy_limited(reader, writer, declared_len),
            CompressionScheme::LZ4 => {
                lz4_decompress_from_reader_limited(reader, writer, declared_len)
            }
            CompressionScheme::ByteGrouping4LZ4 => {
                bg4_lz4_decompress_from_reader_limited(reader, writer, declared_len)
            }
        }
    }

    pub fn choose_from_data(_data: &[u8]) -> Self {
        CompressionScheme::LZ4
    }
}

/// Marker substring used by the write-side decompression ceiling so limit
/// errors can be re-mapped to [`CoreError::MalformedData`] at the boundary.
pub(crate) const DECOMPRESSION_LIMIT_EXCEEDED: &str =
    "decompressed chunk exceeded the declared uncompressed length";

/// Writer wrapper that fails once `limit` bytes have been written.
///
/// Used to enforce a chunk header's declared `uncompressed_length` during
/// decompression so a lying header cannot drive unbounded output/allocation.
pub(crate) struct LimitedWriter<W: Write> {
    inner: W,
    written: u64,
    limit: u64,
}

impl<W: Write> LimitedWriter<W> {
    pub(crate) fn new(inner: W, limit: u64) -> Self {
        Self {
            inner,
            written: 0,
            limit,
        }
    }
}

impl<W: Write> Write for LimitedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let remaining = self.limit.saturating_sub(self.written);
        if u64::try_from(buf.len()).unwrap_or(u64::MAX) > remaining {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                DECOMPRESSION_LIMIT_EXCEEDED,
            ));
        }
        let written = self.inner.write(buf)?;
        self.written = self.written.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn map_limit_io_error(error: std::io::Error) -> CoreError {
    if error.to_string().contains(DECOMPRESSION_LIMIT_EXCEEDED) {
        return CoreError::MalformedData(DECOMPRESSION_LIMIT_EXCEEDED.to_string());
    }
    CoreError::Io(error)
}

fn copy_limited<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    declared_len: u64,
) -> Result<u64, CoreError> {
    let mut limited = LimitedWriter::new(writer, declared_len);
    copy(reader, &mut limited).map_err(map_limit_io_error)
}

pub fn lz4_compress_from_slice(data: &[u8]) -> Result<Vec<u8>, CoreError> {
    let mut enc = FrameEncoder::new(Vec::new());
    enc.write_all(data)?;
    Ok(enc.finish()?)
}

pub fn lz4_decompress_from_slice(data: &[u8]) -> Result<Vec<u8>, CoreError> {
    let mut dest = vec![];
    lz4_decompress_from_reader(&mut Cursor::new(data), &mut dest)?;
    Ok(dest)
}

fn lz4_decompress_from_reader<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<u64, CoreError> {
    let mut dec = FrameDecoder::new(reader);
    Ok(copy(&mut dec, writer)?)
}

fn lz4_decompress_from_reader_limited<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    declared_len: u64,
) -> Result<u64, CoreError> {
    let mut dec = FrameDecoder::new(reader);
    let mut limited = LimitedWriter::new(writer, declared_len);
    copy(&mut dec, &mut limited).map_err(map_limit_io_error)
}

fn bg4_lz4_compress_from_slice(data: &[u8]) -> Result<Vec<u8>, CoreError> {
    let grouped = bg4_split(data);
    let mut enc = FrameEncoder::new(Vec::new());
    enc.write_all(&grouped)?;
    Ok(enc.finish()?)
}

fn bg4_lz4_decompress_from_slice(data: &[u8]) -> Result<Vec<u8>, CoreError> {
    let mut dest = vec![];
    bg4_lz4_decompress_from_reader(&mut Cursor::new(data), &mut dest)?;
    Ok(dest)
}

fn bg4_lz4_decompress_from_reader<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
) -> Result<u64, CoreError> {
    let mut grouped = vec![];
    FrameDecoder::new(reader).read_to_end(&mut grouped)?;
    let regrouped = bg4_regroup(&grouped);
    writer.write_all(&regrouped)?;
    Ok(regrouped.len() as u64)
}

fn bg4_lz4_decompress_from_reader_limited<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    declared_len: u64,
) -> Result<u64, CoreError> {
    // BG4 regrouping is length-preserving: the intermediate `grouped` buffer is
    // exactly the size of the final output, so cap the frame read at
    // `declared_len + 1` to bound intermediate allocation as well as the final
    // writer output. A frame that produces more than `declared_len` bytes is
    // rejected as malformed.
    let mut grouped = vec![];
    FrameDecoder::new(reader)
        .take(declared_len.saturating_add(1))
        .read_to_end(&mut grouped)?;
    if u64::try_from(grouped.len()).unwrap_or(u64::MAX) > declared_len {
        return Err(CoreError::MalformedData(
            DECOMPRESSION_LIMIT_EXCEEDED.to_string(),
        ));
    }
    let regrouped = bg4_regroup(&grouped);
    writer.write_all(&regrouped)?;
    Ok(regrouped.len() as u64)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::io::Cursor;

    use proptest::prelude::*;

    use super::*;

    #[test]
    fn from_and_into_static_str_covers_all_variants() {
        for scheme in [
            CompressionScheme::None,
            CompressionScheme::LZ4,
            CompressionScheme::ByteGrouping4LZ4,
            CompressionScheme::Auto,
        ] {
            let s: &'static str = scheme.into();
            assert!(!s.is_empty());
            let s_ref: &'static str = (&scheme).into();
            assert_eq!(s, s_ref);
        }
    }

    #[test]
    fn try_from_u8_all_valid_values() {
        for (val, expected) in [
            (0u8, CompressionScheme::None),
            (1, CompressionScheme::LZ4),
            (2, CompressionScheme::ByteGrouping4LZ4),
            (99, CompressionScheme::Auto),
        ] {
            assert_eq!(CompressionScheme::try_from(val).unwrap(), expected);
        }
    }

    #[test]
    fn try_from_u8_invalid_values() {
        for val in [3u8, 4, 98, 100, 200, 255] {
            assert!(CompressionScheme::try_from(val).is_err());
        }
    }

    #[test]
    fn from_str_all_valid_variants() {
        for (s, expected) in [
            ("", CompressionScheme::Auto),
            ("auto", CompressionScheme::Auto),
            ("AUTO", CompressionScheme::Auto),
            (" Auto ", CompressionScheme::Auto),
            ("none", CompressionScheme::None),
            ("None", CompressionScheme::None),
            ("lz4", CompressionScheme::LZ4),
            ("LZ4", CompressionScheme::LZ4),
            ("bg4-lz4", CompressionScheme::ByteGrouping4LZ4),
            ("BG4-LZ4", CompressionScheme::ByteGrouping4LZ4),
        ] {
            assert_eq!(
                s.parse::<CompressionScheme>().unwrap(),
                expected,
                "failed for input: {s:?}"
            );
        }
    }

    #[test]
    fn from_str_invalid_values() {
        for s in ["gzip", "zstd", "lz5", "lz4-hc", "bg4", "unknown"] {
            assert!(
                s.parse::<CompressionScheme>().is_err(),
                "should fail for: {s}"
            );
        }
    }

    #[test]
    fn display_fmt_all_variants() {
        assert_eq!(format!("{}", CompressionScheme::None), "none");
        assert_eq!(format!("{}", CompressionScheme::LZ4), "lz4");
        assert_eq!(
            format!("{}", CompressionScheme::ByteGrouping4LZ4),
            "bg4-lz4"
        );
        assert_eq!(format!("{}", CompressionScheme::Auto), "auto");
    }

    #[test]
    fn resolve_for_data_all_variants() {
        // Auto resolves to LZ4
        assert_eq!(
            CompressionScheme::Auto.resolve_for_data(b"data"),
            CompressionScheme::LZ4
        );
        // Other schemes pass through
        assert_eq!(
            CompressionScheme::None.resolve_for_data(b"data"),
            CompressionScheme::None
        );
        assert_eq!(
            CompressionScheme::LZ4.resolve_for_data(b"data"),
            CompressionScheme::LZ4
        );
        assert_eq!(
            CompressionScheme::ByteGrouping4LZ4.resolve_for_data(b"data"),
            CompressionScheme::ByteGrouping4LZ4
        );
    }

    #[test]
    fn compress_and_decompress_all_non_auto() {
        let data = b"Hello, this is a comprehensive test of all compression schemes!";
        for scheme in &[
            CompressionScheme::None,
            CompressionScheme::LZ4,
            CompressionScheme::ByteGrouping4LZ4,
        ] {
            let compressed = scheme.compress_from_slice(data).unwrap();
            let decompressed = scheme.decompress_from_slice(&compressed).unwrap();
            assert_eq!(&*decompressed, data, "roundtrip failed for {scheme:?}");
        }
    }

    #[test]
    fn compress_auto_delegates_and_is_lz4_decompressible() {
        let data = b"Auto compression delegation test data";
        let compressed = CompressionScheme::Auto.compress_from_slice(data).unwrap();
        let decompressed = CompressionScheme::LZ4
            .decompress_from_slice(&compressed)
            .unwrap();
        assert_eq!(&*decompressed, data);
    }

    #[test]
    fn decompress_auto_errors() {
        let result = CompressionScheme::Auto.decompress_from_slice(b"garbage");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::MalformedData(_)));
    }

    #[test]
    fn decompress_from_reader_all_non_auto() {
        let data = b"Reader-based decompression across all schemes";
        for scheme in &[
            CompressionScheme::None,
            CompressionScheme::LZ4,
            CompressionScheme::ByteGrouping4LZ4,
        ] {
            let compressed = scheme.compress_from_slice(data).unwrap();
            let mut reader = Cursor::new(&*compressed);
            let mut writer = Vec::new();
            let n = scheme
                .decompress_from_reader(&mut reader, &mut writer)
                .unwrap();
            assert_eq!(n, data.len() as u64);
            assert_eq!(
                &writer, data,
                "decompress_from_reader failed for {scheme:?}"
            );
        }
    }

    #[test]
    fn decompress_from_reader_auto_errors() {
        let mut reader = Cursor::new(b"data");
        let mut writer = Vec::new();
        let result = CompressionScheme::Auto.decompress_from_reader(&mut reader, &mut writer);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::MalformedData(_)));
    }

    #[test]
    fn decompress_empty_data() {
        let compressed = CompressionScheme::LZ4.compress_from_slice(b"").unwrap();
        let decompressed = CompressionScheme::LZ4
            .decompress_from_slice(&compressed)
            .unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn lz4_compress_decompress_functions_direct() {
        let data = b"Direct lz4 function call test";
        let compressed = lz4_compress_from_slice(data).unwrap();
        let decompressed = lz4_decompress_from_slice(&compressed).unwrap();
        assert_eq!(&decompressed, data);
    }

    #[test]
    fn compress_none_uses_cow_borrowed() {
        let data = b"no compression test data";
        let compressed = CompressionScheme::None.compress_from_slice(data).unwrap();
        // None scheme returns borrowed Cow
        assert!(matches!(compressed, Cow::Borrowed(_)));
    }

    #[test]
    fn compress_large_data_uses_lz4_compression() {
        // Large enough that LZ4 compression actually helps
        let data = vec![0xABu8; 10000];
        let compressed = CompressionScheme::LZ4.compress_from_slice(&data).unwrap();
        assert!(
            compressed.len() < data.len(),
            "LZ4 should compress repetitive data"
        );
    }

    #[test]
    fn choose_from_data_always_returns_lz4() {
        assert_eq!(
            CompressionScheme::choose_from_data(b""),
            CompressionScheme::LZ4
        );
        assert_eq!(
            CompressionScheme::choose_from_data(b"data"),
            CompressionScheme::LZ4
        );
    }

    #[test]
    fn default_is_auto() {
        assert_eq!(CompressionScheme::default(), CompressionScheme::Auto);
    }

    // --- BG4-specific regression tests ---

    #[test]
    fn bg4_roundtrip_small_payload() {
        let data = b"Hello, this tests BG4 byte-interleaving compression!";
        let compressed = CompressionScheme::ByteGrouping4LZ4
            .compress_from_slice(data)
            .unwrap();
        let decompressed = CompressionScheme::ByteGrouping4LZ4
            .decompress_from_slice(&compressed)
            .unwrap();
        assert_eq!(&*decompressed, data);
    }

    #[test]
    fn bg4_roundtrip_empty() {
        let data: &[u8] = b"";
        let compressed = CompressionScheme::ByteGrouping4LZ4
            .compress_from_slice(data)
            .unwrap();
        let decompressed = CompressionScheme::ByteGrouping4LZ4
            .decompress_from_slice(&compressed)
            .unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn bg4_roundtrip_single_byte() {
        let data = &[42u8];
        let compressed = CompressionScheme::ByteGrouping4LZ4
            .compress_from_slice(data)
            .unwrap();
        let decompressed = CompressionScheme::ByteGrouping4LZ4
            .decompress_from_slice(&compressed)
            .unwrap();
        assert_eq!(&*decompressed, data);
    }

    #[test]
    fn bg4_roundtrip_not_multiple_of_4() {
        // Test various sizes that don't divide evenly by 4
        for n in [1, 2, 3, 5, 7, 13, 65535, 65537, 131073] {
            let data: Vec<u8> = (0..n).map(|i| (i % 256) as u8).collect();
            let compressed = CompressionScheme::ByteGrouping4LZ4
                .compress_from_slice(&data)
                .unwrap();
            let decompressed = CompressionScheme::ByteGrouping4LZ4
                .decompress_from_slice(&compressed)
                .unwrap();
            assert_eq!(*decompressed, data, "BG4 roundtrip failed for size {n}");
        }
    }

    #[test]
    fn bg4_roundtrip_chunk_sized_payload() {
        // Typical chunk size used in shardline (64 KiB)
        let data: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();
        let compressed = CompressionScheme::ByteGrouping4LZ4
            .compress_from_slice(&data)
            .unwrap();
        let decompressed = CompressionScheme::ByteGrouping4LZ4
            .decompress_from_slice(&compressed)
            .unwrap();
        assert_eq!(*decompressed, data);
    }

    #[test]
    fn bg4_roundtrip_large_repetitive_data() {
        // 1 MiB of repeated pattern — BG4 should compress well
        let data: Vec<u8> = (0..1_048_576).map(|i| ((i * 7 + 3) % 256) as u8).collect();
        let compressed = CompressionScheme::ByteGrouping4LZ4
            .compress_from_slice(&data)
            .unwrap();
        let decompressed = CompressionScheme::ByteGrouping4LZ4
            .decompress_from_slice(&compressed)
            .unwrap();
        assert_eq!(*decompressed, data);
        // Verify BG4 actually compresses
        assert!(
            compressed.len() < data.len(),
            "BG4-LZ4 should compress repetitive data"
        );
    }

    #[test]
    fn bg4_roundtrip_random_data() {
        // 256 KiB of pseudo-random data
        let data: Vec<u8> = (0u32..262144)
            .map(|i| (i.wrapping_mul(31).wrapping_add(7)) as u8)
            .collect();
        let compressed = CompressionScheme::ByteGrouping4LZ4
            .compress_from_slice(&data)
            .unwrap();
        let decompressed = CompressionScheme::ByteGrouping4LZ4
            .decompress_from_slice(&compressed)
            .unwrap();
        assert_eq!(*decompressed, data);
    }

    #[test]
    fn bg4_roundtrip_reader_based() {
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        let compressed = CompressionScheme::ByteGrouping4LZ4
            .compress_from_slice(&data)
            .unwrap();
        let mut reader = Cursor::new(&*compressed);
        let mut writer = Vec::new();
        let n = CompressionScheme::ByteGrouping4LZ4
            .decompress_from_reader(&mut reader, &mut writer)
            .unwrap();
        assert_eq!(n, data.len() as u64);
        assert_eq!(writer, data);
    }

    #[test]
    fn bg4_produces_different_output_than_plain_lz4() {
        // BG4 interleaving should produce a different compressed output than plain LZ4
        let data = b"abcdefgh";
        let bg4_compressed = CompressionScheme::ByteGrouping4LZ4
            .compress_from_slice(data)
            .unwrap();
        let lz4_compressed = CompressionScheme::LZ4.compress_from_slice(data).unwrap();
        // The compressed bytes should differ (BG4 interleaves before LZ4)
        assert_ne!(
            *bg4_compressed, *lz4_compressed,
            "BG4 and plain LZ4 should produce different compressed output"
        );
        // But both should decompress to the same original
        assert_eq!(
            *CompressionScheme::ByteGrouping4LZ4
                .decompress_from_slice(&bg4_compressed)
                .unwrap(),
            *CompressionScheme::LZ4
                .decompress_from_slice(&lz4_compressed)
                .unwrap()
        );
    }

    #[test]
    fn bg4_cross_compat_with_upstream_split_regroup() {
        // Verify our BG4 matches upstream xet-core-structures split/regroup
        let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let grouped = bg4_split(&data);
        let regrouped = bg4_regroup(&grouped);
        assert_eq!(
            regrouped, data,
            "bg4_split→bg4_regroup roundtrip must match upstream"
        );
    }

    // --- Fuzz-target-style proptest tests for BG4 decompression safety ---

    proptest! {
        /// Feed arbitrary byte slices to bg4_regroup and verify it never panics.
        /// bg4_regroup is a pure byte-interleaving function — it should handle any input gracefully.
        #[test]
        fn bg4_regroup_arbitrary_bytes_never_panics(data in proptest::collection::vec(any::<u8>(), 0..8192)) {
            let _result = bg4_regroup(&data);
            // Any output (correct or garbage) is acceptable; we only care about no panic.
        }
    }

    proptest! {
        /// Feed arbitrary byte slices to bg4_lz4_decompress_from_slice and verify it never panics.
        /// The decompressor should return Err on invalid data rather than crashing.
        #[test]
        fn bg4_lz4_decompress_arbitrary_bytes_never_panics(data in proptest::collection::vec(any::<u8>(), 0..8192)) {
            let _result = bg4_lz4_decompress_from_slice(&data);
            // Both Ok and Err are acceptable; we only care about no panic.
        }
    }

    proptest! {
        /// Compress then decompress arbitrary data and verify a perfect roundtrip.
        /// This is a stronger correctness check: any data that survives compress+decompress
        /// must come back byte-for-byte identical.
        #[test]
        fn bg4_compress_roundtrip_arbitrary_data(data in proptest::collection::vec(any::<u8>(), 0..16384)) {
            let compressed = bg4_lz4_compress_from_slice(&data)
                .expect("compression of arbitrary data should not fail");
            let decompressed = bg4_lz4_decompress_from_slice(&compressed)
                .expect("decompression of our own compressed data should not fail");
            assert_eq!(decompressed, data, "BG4 compress→decompress roundtrip must preserve data");
        }
    }

    // -------------------------------------------------------------------------
    // BG4 regression tests — safety/corner-case coverage
    // -------------------------------------------------------------------------

    use xet_core_structures::xorb_object::byte_grouping::bg4::bg4_regroup as upstream_bg4_regroup;

    #[test]
    fn bg4_decompress_corrupted_lz4_frame() {
        // Compress valid data with BG4, then truncate the LZ4 frame so it's
        // malformed.  Decompression must not panic.  It may return an error
        // (preferred) or decompress to wrong data — either is acceptable as
        // long as it doesn't crash.
        let data = b"Hello, this is a test of BG4 compression with valid data!";
        let compressed = bg4_lz4_compress_from_slice(data).unwrap();

        // Truncate at several positions inside the LZ4 frame header / body
        for &cut in &[1usize, 4, 8, 16, 32, compressed.len() / 2] {
            let truncated = &compressed[..cut.min(compressed.len())];
            let result = bg4_lz4_decompress_from_slice(truncated);
            match result {
                Err(_) => {} // expected — truncated data should error
                Ok(decompressed) => {
                    // If the truncated frame happens to decompress (e.g. just
                    // the magic header), the output must NOT match the original.
                    assert_ne!(
                        &*decompressed, data,
                        "truncated BG4 data must not roundtrip correctly (cut={cut})"
                    );
                }
            }
        }
    }

    #[test]
    fn bg4_decompress_plain_lz4_as_bg4() {
        // Compress with plain LZ4, then try to decompress using the BG4 path.
        // Must not panic. May return error or wrong data.
        let data = b"Data compressed with plain LZ4, NOT BG4.";
        let lz4_compressed = lz4_compress_from_slice(data).unwrap();

        let result = bg4_lz4_decompress_from_slice(&lz4_compressed);
        match result {
            Err(_) => {} // expected
            Ok(decompressed) => {
                // Plain LZ4 data decompresses fine via LZ4, but bg4_regroup
                // on non-interleaved data produces garbage.
                assert_ne!(
                    decompressed, data,
                    "plain LZ4 data decompressed via BG4 path must not match original"
                );
            }
        }
    }

    #[test]
    fn bg4_decompress_none_data_as_bg4() {
        // Feed raw uncompressed bytes to the BG4 decompressor.  Must not panic.
        let raw = b"These bytes have never been compressed.";
        let result = bg4_lz4_decompress_from_slice(raw);
        match result {
            Err(_) => {} // expected
            Ok(decompressed) => {
                assert_ne!(
                    decompressed, raw,
                    "uncompressed data decompressed via BG4 path must not match original"
                );
            }
        }
    }

    #[test]
    fn bg4_regroup_panic_safety() {
        // Call upstream bg4_regroup with truly arbitrary byte sequences (not the
        // output of bg4_split).  The function must never panic.
        let inputs: &[&[u8]] = &[
            b"",
            b"a",
            b"ab",
            b"abc",
            b"abcd",
            b"abcde",
            &[0u8; 7],
            &[0xFFu8; 100],
            &(0..200).map(|i| i as u8).collect::<Vec<_>>(),
        ];
        for input in inputs {
            // Should never panic regardless of input
            let _result = upstream_bg4_regroup(input);
        }
        // If we reach here, no panic occurred
        // If we reach here, bg4_regroup did not panic on any input
    }

    #[test]
    fn lz4_decompress_bg4_data_as_lz4() {
        // Compress with BG4, then try to decompress via the plain LZ4 path.
        // Must not panic. May return error or wrong data.
        let data = b"BG4 compressed payload fed to plain LZ4 decompressor.";
        let bg4_compressed = bg4_lz4_compress_from_slice(data).unwrap();

        let result = lz4_decompress_from_slice(&bg4_compressed);
        match result {
            Err(_) => {} // expected — LZ4 frame decoder rejects the data
            Ok(decompressed) => {
                // If the LZ4 frame happens to be valid, the content will be
                // the interleaved bytes (not the original message).
                assert_ne!(
                    decompressed, data,
                    "plain LZ4 decompressor must not roundtrip BG4 data"
                );
            }
        }
    }

    #[test]
    fn bg4_decompress_truncated_compressed() {
        // Compress with BG4, then truncate the output at various lengths.
        // Must not panic. May return error or wrong data.
        let data: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
        let compressed = bg4_lz4_compress_from_slice(&data).unwrap();
        let len = compressed.len();

        for &cut in &[
            len / 2,  // 50 %
            len / 10, // 10 %
            1usize,   // 1 byte
            0usize,   // 0 bytes (empty)
        ] {
            let cut = cut.min(len);
            let truncated = &compressed[..cut];
            let result = bg4_lz4_decompress_from_slice(truncated);
            match result {
                Err(_) => {} // expected
                Ok(decompressed) => {
                    assert_ne!(
                        decompressed, data,
                        "truncated BG4 data must not roundtrip correctly (cut={cut}/{len})"
                    );
                }
            }
        }
    }

    #[test]
    fn bg4_compress_and_decompress_max_chunk_size() {
        // 16 MiB — the maximum chunk size in the shardline chunking layer.
        const SIZE: usize = 16 * 1024 * 1024; // 16 MiB
        let data: Vec<u8> = (0..SIZE).map(|i| (i % 256) as u8).collect();
        let compressed = bg4_lz4_compress_from_slice(&data).unwrap();
        let decompressed = bg4_lz4_decompress_from_slice(&compressed).unwrap();
        assert_eq!(
            decompressed.len(),
            data.len(),
            "BG4 roundtrip at max chunk size: length mismatch"
        );
        // Compare in chunks to keep peak memory reasonable
        for chunk in decompressed.chunks(1024 * 1024) {
            let offset = chunk.as_ptr() as usize - decompressed.as_ptr() as usize;
            assert_eq!(
                chunk,
                &data[offset..offset + chunk.len()],
                "BG4 roundtrip mismatch at offset {offset}"
            );
        }
    }

    #[test]
    fn bg4_compress_tiny_data_expands() {
        // Very small inputs (1, 2, 3 bytes) must not panic and must
        // roundtrip correctly.
        for &n in &[1usize, 2, 3] {
            let data: Vec<u8> = (0..n).map(|i| (i % 256) as u8).collect();
            let compressed = bg4_lz4_compress_from_slice(&data).unwrap();
            let decompressed = bg4_lz4_decompress_from_slice(&compressed).unwrap();
            assert_eq!(
                decompressed, data,
                "BG4 roundtrip failed for {n}-byte input"
            );
        }
    }
}
