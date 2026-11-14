use std::num::TryFromIntError;

use thiserror::Error;

/// Reconstruction cache adapter failure.
#[derive(Debug, Error)]
pub enum ReconstructionCacheError {
    /// The configured Redis URL was empty.
    #[error("reconstruction cache redis url must not be empty")]
    EmptyRedisUrl,
    /// Only one half of a Redis mTLS identity was configured.
    #[error("redis TLS client certificate and key must be configured together")]
    IncompleteRedisTlsClientIdentity,
    /// Redis client initialization or operations failed.
    #[error("reconstruction cache redis operation failed")]
    Redis(#[from] redis::RedisError),
    /// A Redis operation exceeded its configured latency bound.
    #[error("reconstruction cache redis operation timed out")]
    RedisTimeout,
    /// A stale loader attempted to mutate a reservation owned by another caller.
    #[error("reconstruction cache loading reservation is no longer owned by this operation")]
    LostLoadingReservation,
    /// The Redis operation timeout was zero.
    #[error("reconstruction cache redis operation timeout must be greater than zero")]
    InvalidRedisOperationTimeout,
    /// The Redis TTL cannot be represented as a signed millisecond expiration.
    #[error("reconstruction cache redis TTL exceeds the supported expiration range")]
    InvalidRedisTtl,
    /// Numeric conversion exceeded supported bounds.
    #[error("reconstruction cache numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
    /// The adapter reported a generic operational failure.
    #[error("reconstruction cache adapter operation failed")]
    Operation,
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[test]
    fn plain_errors_have_expected_messages_and_no_source() {
        for (error, expected) in [
            (
                ReconstructionCacheError::EmptyRedisUrl,
                "reconstruction cache redis url must not be empty",
            ),
            (
                ReconstructionCacheError::Operation,
                "reconstruction cache adapter operation failed",
            ),
            (
                ReconstructionCacheError::IncompleteRedisTlsClientIdentity,
                "redis TLS client certificate and key must be configured together",
            ),
            (
                ReconstructionCacheError::RedisTimeout,
                "reconstruction cache redis operation timed out",
            ),
            (
                ReconstructionCacheError::InvalidRedisOperationTimeout,
                "reconstruction cache redis operation timeout must be greater than zero",
            ),
        ] {
            assert_eq!(error.to_string(), expected);
            assert!(error.source().is_none());
            assert!(!format!("{error:?}").is_empty());
        }
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn redis_conversion_preserves_source_and_display() {
        for message in ["inner source", "test error"] {
            let source = redis::RedisError::from((redis::ErrorKind::Io, message));
            let error: ReconstructionCacheError = source.into();
            assert_eq!(
                error.to_string(),
                "reconstruction cache redis operation failed"
            );
            assert!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<redis::RedisError>()
                    .is_some()
            );
            assert!(matches!(error, ReconstructionCacheError::Redis(_)));
        }
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn numeric_conversion_preserves_source_and_display() {
        let source = i64::try_from(u64::MAX).unwrap_err();
        let error: ReconstructionCacheError = source.into();
        assert_eq!(
            error.to_string(),
            "reconstruction cache numeric conversion exceeded supported bounds"
        );
        assert!(
            error
                .source()
                .unwrap()
                .downcast_ref::<TryFromIntError>()
                .is_some()
        );
        assert!(matches!(
            error,
            ReconstructionCacheError::NumericConversion(_)
        ));
    }
}
