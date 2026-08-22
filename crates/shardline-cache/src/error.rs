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
    /// The Redis operation timeout was zero.
    #[error("reconstruction cache redis operation timeout must be greater than zero")]
    InvalidRedisOperationTimeout,
    /// Numeric conversion exceeded supported bounds.
    #[error("reconstruction cache numeric conversion exceeded supported bounds")]
    NumericConversion(#[from] TryFromIntError),
    /// The adapter reported a generic operational failure.
    #[error("reconstruction cache adapter operation failed")]
    Operation,
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;
    use std::mem::discriminant;

    use super::*;

    #[allow(clippy::unwrap_used, clippy::shadow_unrelated)]
    #[test]
    fn error_source_returns_inner_error() {
        // EmptyRedisUrl has no source
        assert!(ReconstructionCacheError::EmptyRedisUrl.source().is_none());

        // Operation has no source
        assert!(ReconstructionCacheError::Operation.source().is_none());
        assert!(
            ReconstructionCacheError::IncompleteRedisTlsClientIdentity
                .source()
                .is_none()
        );
        assert!(ReconstructionCacheError::RedisTimeout.source().is_none());
        assert!(
            ReconstructionCacheError::InvalidRedisOperationTimeout
                .source()
                .is_none()
        );

        // Redis wraps a redis::RedisError
        let redis_err = redis::RedisError::from((redis::ErrorKind::Io, "inner source"));
        let err = ReconstructionCacheError::Redis(redis_err);
        let source = err.source();
        assert!(source.is_some());
        assert!(
            source
                .unwrap()
                .downcast_ref::<redis::RedisError>()
                .is_some()
        );

        // NumericConversion wraps a TryFromIntError
        let int_err = i64::try_from(u64::MAX).unwrap_err();
        let err = ReconstructionCacheError::NumericConversion(int_err);
        let source = err.source();
        assert!(source.is_some());
        assert!(source.unwrap().downcast_ref::<TryFromIntError>().is_some());
    }

    #[test]
    fn display_empty_redis_url() {
        let err = ReconstructionCacheError::EmptyRedisUrl;
        assert_eq!(
            err.to_string(),
            "reconstruction cache redis url must not be empty"
        );
    }

    #[test]
    fn display_operation() {
        let err = ReconstructionCacheError::Operation;
        assert_eq!(
            err.to_string(),
            "reconstruction cache adapter operation failed"
        );
    }

    #[test]
    fn display_incomplete_redis_tls_client_identity() {
        let err = ReconstructionCacheError::IncompleteRedisTlsClientIdentity;
        assert_eq!(
            err.to_string(),
            "redis TLS client certificate and key must be configured together"
        );
    }

    #[test]
    fn display_redis() {
        let redis_err = redis::RedisError::from((redis::ErrorKind::Io, "test error"));
        let err = ReconstructionCacheError::Redis(redis_err);
        assert_eq!(
            err.to_string(),
            "reconstruction cache redis operation failed"
        );
    }

    #[test]
    fn display_redis_timeout() {
        assert_eq!(
            ReconstructionCacheError::RedisTimeout.to_string(),
            "reconstruction cache redis operation timed out"
        );
        assert_eq!(
            ReconstructionCacheError::InvalidRedisOperationTimeout.to_string(),
            "reconstruction cache redis operation timeout must be greater than zero"
        );
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn display_numeric_conversion() {
        let int_err = i64::try_from(u64::MAX).unwrap_err();
        let err = ReconstructionCacheError::NumericConversion(int_err);
        assert_eq!(
            err.to_string(),
            "reconstruction cache numeric conversion exceeded supported bounds"
        );
    }

    #[test]
    fn from_redis_error() {
        let redis_err = redis::RedisError::from((redis::ErrorKind::Io, "test error"));
        let err: ReconstructionCacheError = redis_err.into();
        assert!(matches!(err, ReconstructionCacheError::Redis(_)));
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn from_try_from_int_error() {
        let int_err = i64::try_from(u64::MAX).unwrap_err();
        let err: ReconstructionCacheError = int_err.into();
        assert!(matches!(
            err,
            ReconstructionCacheError::NumericConversion(_)
        ));
    }

    #[test]
    fn debug_output_non_empty() {
        let err = ReconstructionCacheError::EmptyRedisUrl;
        let debug = format!("{err:?}");
        assert!(!debug.is_empty());
    }

    #[test]
    fn discriminant_matches_same_variant() {
        let a = ReconstructionCacheError::EmptyRedisUrl;
        let b = ReconstructionCacheError::EmptyRedisUrl;
        assert_eq!(discriminant(&a), discriminant(&b));
    }

    #[test]
    fn discriminant_differs_between_variants() {
        let a = ReconstructionCacheError::EmptyRedisUrl;
        let b = ReconstructionCacheError::Operation;
        assert_ne!(discriminant(&a), discriminant(&b));
    }
}
