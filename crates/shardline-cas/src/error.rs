use std::fmt;

/// CAS coordinator error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasError {
    /// Object body exceeds the configured maximum.
    BodyTooLarge {
        /// Actual body size in bytes.
        actual: u64,
        /// Configured maximum in bytes.
        max: u64,
    },
    /// ObjectStore operation failed.
    ObjectStore(String),
    /// Index operation failed (reachability, reconstruction, lifecycle).
    Index(String),
    /// RecordStore operation failed.
    Record(String),
    /// Numeric overflow.
    Overflow,
    /// Internal error.
    Internal(String),
}

impl fmt::Display for CasError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BodyTooLarge { actual, max } => {
                write!(f, "body too large: {actual} bytes exceeds max {max} bytes")
            }
            Self::ObjectStore(msg) => write!(f, "object store error: {msg}"),
            Self::Index(msg) => write!(f, "index error: {msg}"),
            Self::Record(msg) => write!(f, "record store error: {msg}"),
            Self::Overflow => write!(f, "numeric overflow"),
            Self::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl std::error::Error for CasError {}

impl CasError {
    /// Creates an `ObjectStore` error from any display-able error type.
    pub fn from_object_store<E: fmt::Display>(e: E) -> Self {
        Self::ObjectStore(e.to_string())
    }

    /// Creates an `Index` error from any display-able error type.
    pub fn from_index<E: fmt::Display>(e: E) -> Self {
        Self::Index(e.to_string())
    }

    /// Creates a `Record` error from any display-able error type.
    pub fn from_record<E: fmt::Display>(e: E) -> Self {
        Self::Record(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cas_error_body_too_large_display() {
        let err = CasError::BodyTooLarge { actual: 100, max: 50 };
        assert_eq!(err.to_string(), "body too large: 100 bytes exceeds max 50 bytes");
    }

    #[test]
    fn cas_error_object_store_display() {
        let err = CasError::ObjectStore("s3 error".to_owned());
        assert_eq!(err.to_string(), "object store error: s3 error");
    }

    #[test]
    fn cas_error_index_display() {
        let err = CasError::Index("not found".to_owned());
        assert_eq!(err.to_string(), "index error: not found");
    }

    #[test]
    fn cas_error_record_display() {
        let err = CasError::Record("commit failed".to_owned());
        assert_eq!(err.to_string(), "record store error: commit failed");
    }

    #[test]
    fn cas_error_overflow_display() {
        let err = CasError::Overflow;
        assert_eq!(err.to_string(), "numeric overflow");
    }

    #[test]
    fn cas_error_internal_display() {
        let err = CasError::Internal("join error".to_owned());
        assert_eq!(err.to_string(), "internal error: join error");
    }

    #[test]
    fn cas_error_is_std_error() {
        fn takes_error(_: &dyn std::error::Error) {}
        takes_error(&CasError::Overflow);
        takes_error(&CasError::ObjectStore("msg".to_owned()));
    }

    #[test]
    fn cas_error_from_object_store() {
        let err = CasError::from_object_store("disk full");
        assert!(matches!(err, CasError::ObjectStore(_)));
        assert_eq!(err.to_string(), "object store error: disk full");
    }

    #[test]
    fn cas_error_from_index() {
        let err = CasError::from_index("key missing");
        assert!(matches!(err, CasError::Index(_)));
    }

    #[test]
    fn cas_error_from_record() {
        let err = CasError::from_record("txn failed");
        assert!(matches!(err, CasError::Record(_)));
    }
}
