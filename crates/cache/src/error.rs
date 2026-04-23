use std::num::TryFromIntError;

use thiserror::Error;

/// Reconstruction cache adapter failure.
#[derive(Debug, Error)]
pub enum ReconstructionCacheError {
    /// The configured Redis URL was empty.
    #[error("reconstruction cache redis url must not be empty")]
    EmptyRedisUrl,
    /// Redis client initialization or operations failed.
    #[error("reconstruction cache redis operation failed")]
    Redis(#[from] redis::RedisError),
    /// Numeric conversion exceeded supported bounds.
    #[error("reconstruction cache numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
    /// The adapter reported a generic operational failure.
    #[error("reconstruction cache adapter operation failed")]
    Operation,
}
