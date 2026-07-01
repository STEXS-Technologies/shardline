use std::borrow::Cow;
use std::fmt::Display;
use std::io::{Cursor, Read, Write, copy};
use std::str::FromStr;

use lz4_flex::frame::{FrameDecoder, FrameEncoder};

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
            CompressionScheme::Auto => return self.resolve_for_data(data).compress_from_slice(data),
            CompressionScheme::None => data.into(),
            CompressionScheme::LZ4 => lz4_compress_from_slice(data).map(Cow::from)?,
            CompressionScheme::ByteGrouping4LZ4 => lz4_compress_from_slice(data).map(Cow::from)?,
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
            CompressionScheme::ByteGrouping4LZ4 => lz4_decompress_from_slice(data).map(Cow::from)?,
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
            CompressionScheme::ByteGrouping4LZ4 => lz4_decompress_from_reader(reader, writer)?,
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
