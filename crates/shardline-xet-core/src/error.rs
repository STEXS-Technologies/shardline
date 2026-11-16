use std::convert::Infallible;

use thiserror::Error;
use tracing::warn;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum CoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("{0}")]
    Other(String),

    #[error("Shard version error: {0}")]
    ShardVersion(String),

    #[error("Invalid shard: {0}")]
    InvalidShard(String),

    #[error("Invalid range")]
    InvalidRange,

    #[error("Invalid arguments")]
    InvalidArguments,

    #[error("Malformed data: {0}")]
    MalformedData(String),

    #[error("Hash mismatch")]
    HashMismatch,

    #[error("Compression error: {0}")]
    CompressionError(#[from] lz4_flex::frame::Error),

    #[error("Hash parsing error: {0}")]
    HashParsing(#[from] Infallible),

    #[error("Chunk header parse error")]
    ChunkHeaderParse,
}

impl PartialEq for CoreError {
    fn eq(&self, other: &CoreError) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl CoreError {
    pub fn other(inner: impl ToString) -> Self {
        Self::Other(inner.to_string())
    }

    pub fn invalid_shard(inner: impl ToString) -> Self {
        Self::InvalidShard(inner.to_string())
    }
}

pub trait Validate<T> {
    fn ok_for_format_error(self) -> std::result::Result<Option<T>, CoreError>;
}

impl<T> Validate<T> for std::result::Result<T, CoreError> {
    fn ok_for_format_error(self) -> std::result::Result<Option<T>, CoreError> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(CoreError::MalformedData(e)) => {
                warn!("XORB Validation: {e}");
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
}

pub type Result<T> = std::result::Result<T, CoreError>;

use crate::merklehash::{DataHashBytesParseError, DataHashHexParseError};

impl From<DataHashHexParseError> for CoreError {
    fn from(_: DataHashHexParseError) -> Self {
        CoreError::Other("Invalid hex input for DataHash".to_string())
    }
}

impl From<DataHashBytesParseError> for CoreError {
    fn from(_: DataHashBytesParseError) -> Self {
        CoreError::Other("Invalid bytes input for DataHash".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_variants() {
        let cases = [
            (
                CoreError::Io(std::io::Error::other("lost file")),
                "I/O error: lost file",
            ),
            (
                CoreError::InternalError("oops".into()),
                "Internal error: oops",
            ),
            (CoreError::Other("msg".into()), "msg"),
            (
                CoreError::ShardVersion("bad".into()),
                "Shard version error: bad",
            ),
            (CoreError::InvalidShard("bad".into()), "Invalid shard: bad"),
            (CoreError::InvalidRange, "Invalid range"),
            (CoreError::InvalidArguments, "Invalid arguments"),
            (
                CoreError::MalformedData("garbage".into()),
                "Malformed data: garbage",
            ),
            (CoreError::HashMismatch, "Hash mismatch"),
            (CoreError::ChunkHeaderParse, "Chunk header parse error"),
        ];
        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected, "{error:?}");
        }
        let source = lz4_flex::frame::Error::WrongMagicNumber;
        let expected = format!("Compression error: {source}");
        assert_eq!(CoreError::CompressionError(source).to_string(), expected);
    }

    #[test]
    fn partial_eq_same_discriminant() {
        let a = CoreError::Other("foo".into());
        let b = CoreError::Other("bar".into());
        assert_eq!(a, b);
    }

    #[test]
    fn partial_eq_different_discriminant() {
        let a = CoreError::Other("foo".into());
        let b = CoreError::InvalidRange;
        assert_ne!(a, b);
    }

    #[test]
    fn constructors() {
        let e = CoreError::other("custom");
        assert_eq!(format!("{e}"), "custom");

        let e = CoreError::invalid_shard("custom");
        assert_eq!(format!("{e}"), "Invalid shard: custom");
    }

    #[test]
    fn from_io() {
        let e: CoreError = std::io::Error::other("test").into();
        let CoreError::Io(source) = e else {
            panic!("lost I/O error")
        };
        assert_eq!(source.kind(), std::io::ErrorKind::Other);
        assert_eq!(source.to_string(), "test");
    }

    #[test]
    fn from_lz4() {
        let e: CoreError = lz4_flex::frame::Error::WrongMagicNumber.into();
        assert!(matches!(
            e,
            CoreError::CompressionError(lz4_flex::frame::Error::WrongMagicNumber)
        ));
    }

    #[test]
    fn from_hex_parse() {
        use crate::merklehash::DataHashHexParseError;
        let e: CoreError = DataHashHexParseError.into();
        assert!(matches!(e, CoreError::Other(_)));
        assert_eq!(format!("{e}"), "Invalid hex input for DataHash");
    }

    #[test]
    fn from_bytes_parse() {
        use crate::merklehash::DataHashBytesParseError;
        let e: CoreError = DataHashBytesParseError.into();
        assert!(matches!(e, CoreError::Other(_)));
        assert_eq!(format!("{e}"), "Invalid bytes input for DataHash");
    }

    #[test]
    fn validate_ok() {
        let r: std::result::Result<u32, CoreError> = Ok(42);
        assert_eq!(r.ok_for_format_error().unwrap(), Some(42));
    }

    #[test]
    fn validate_malformed() {
        let r: std::result::Result<u32, CoreError> = Err(CoreError::MalformedData("skip".into()));
        assert!(r.ok_for_format_error().unwrap().is_none());
    }

    #[test]
    fn validate_other_err() {
        let r: std::result::Result<u32, CoreError> = Err(CoreError::Other("fatal".into()));
        assert!(
            matches!(r.ok_for_format_error(), Err(CoreError::Other(message)) if message == "fatal")
        );
    }

    #[test]
    fn validate_io_err() {
        let r: std::result::Result<u32, CoreError> =
            Err(CoreError::Io(std::io::Error::other("eek")));
        assert!(
            matches!(r.ok_for_format_error(), Err(CoreError::Io(source)) if source.to_string() == "eek")
        );
    }
}
