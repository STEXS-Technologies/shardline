use std::borrow::Cow;
use std::fmt::Display;
use std::io::{Cursor, Read, Write, copy};
use std::str::FromStr;

use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use xet_core_structures::xorb_object::byte_grouping::bg4::{bg4_split, bg4_regroup};

use crate::error::CoreError;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum CompressionScheme {
    None = 0,
    LZ4 = 1,
    ByteGrouping4LZ4 = 2,
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

    pub fn compress_from_slice<'a>(&self, data: &'a [u8]) -> Result<Cow<'a, [u8]>, CoreError> {
        Ok(match self {
            CompressionScheme::Auto => {
                return self.resolve_for_data(data).compress_from_slice(data);
            }
            CompressionScheme::None => data.into(),
            CompressionScheme::LZ4 => lz4_compress_from_slice(data).map(Cow::from)?,
            CompressionScheme::ByteGrouping4LZ4 => bg4_lz4_compress_from_slice(data).map(Cow::from)?,
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

    pub fn choose_from_data(_data: &[u8]) -> Self {
        CompressionScheme::LZ4
    }
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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::io::Cursor;

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
}
