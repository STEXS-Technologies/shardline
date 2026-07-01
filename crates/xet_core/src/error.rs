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
