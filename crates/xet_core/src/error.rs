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

impl From<crate::merklehash::DataHashHexParseError> for CoreError {
    fn from(_: crate::merklehash::DataHashHexParseError) -> Self {
        CoreError::Other("Invalid hex input for DataHash".to_string())
    }
}

impl From<crate::merklehash::DataHashBytesParseError> for CoreError {
    fn from(_: crate::merklehash::DataHashBytesParseError) -> Self {
        CoreError::Other("Invalid bytes input for DataHash".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper that constructs every variant and checks Display output.
    fn check_display(variant: CoreError, expected_substr: &str) {
        let s = format!("{variant}");
        assert!(
            s.contains(expected_substr),
            "Display for {variant:?} should contain '{expected_substr}', got: {s}"
        );
    }

    #[test]
    fn display_io() {
        let err = CoreError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "lost file",
        ));
        check_display(err, "I/O error");
    }

    #[test]
    fn display_internal() {
        check_display(CoreError::InternalError("oops".into()), "Internal error");
    }

    #[test]
    fn display_other() {
        check_display(CoreError::Other("msg".into()), "msg");
    }

    #[test]
    fn display_shard_version() {
        check_display(CoreError::ShardVersion("bad".into()), "Shard version");
    }

    #[test]
    fn display_invalid_shard() {
        check_display(CoreError::InvalidShard("bad".into()), "Invalid shard");
    }

    #[test]
    fn display_invalid_range() {
        check_display(CoreError::InvalidRange, "Invalid range");
    }

    #[test]
    fn display_invalid_arguments() {
        check_display(CoreError::InvalidArguments, "Invalid arguments");
    }

    #[test]
    fn display_malformed_data() {
        check_display(CoreError::MalformedData("garbage".into()), "Malformed data");
    }

    #[test]
    fn display_hash_mismatch() {
        check_display(CoreError::HashMismatch, "Hash mismatch");
    }

    #[test]
    fn display_compression_error() {
        let err = CoreError::CompressionError(lz4_flex::frame::Error::WrongMagicNumber);
        check_display(err, "Compression error");
    }

    #[test]
    fn display_chunk_header_parse() {
        check_display(CoreError::ChunkHeaderParse, "Chunk header parse");
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
        let e: CoreError = std::io::Error::new(std::io::ErrorKind::Other, "test").into();
        assert!(matches!(e, CoreError::Io(_)));
    }

    #[test]
    fn from_lz4() {
        let e: CoreError = lz4_flex::frame::Error::WrongMagicNumber.into();
        assert!(matches!(e, CoreError::CompressionError(_)));
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
        assert!(r.ok_for_format_error().is_err());
    }

    #[test]
    fn validate_io_err() {
        let r: std::result::Result<u32, CoreError> = Err(CoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "eek",
        )));
        assert!(r.ok_for_format_error().is_err());
    }
}
