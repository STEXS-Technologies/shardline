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
    /// A persisted upload intent is missing or cannot move to the requested state.
    InvalidUploadTransition,
    /// A deterministic test interrupted the upload lifecycle at a typed boundary.
    InjectedUploadInterruption {
        /// Boundary reached immediately before the interruption.
        boundary: crate::UploadLifecycleBoundary,
    },
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
            Self::InvalidUploadTransition => write!(f, "invalid upload intent state transition"),
            Self::InjectedUploadInterruption { boundary } => {
                write!(f, "injected upload interruption at {boundary:?}")
            }
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
    fn cas_errors_preserve_messages_and_standard_error_contract() {
        for (error, expected) in [
            (
                CasError::BodyTooLarge {
                    actual: 100,
                    max: 50,
                },
                "body too large: 100 bytes exceeds max 50 bytes",
            ),
            (
                CasError::ObjectStore("s3 error".to_owned()),
                "object store error: s3 error",
            ),
            (
                CasError::Index("not found".to_owned()),
                "index error: not found",
            ),
            (
                CasError::Record("commit failed".to_owned()),
                "record store error: commit failed",
            ),
            (
                CasError::InvalidUploadTransition,
                "invalid upload intent state transition",
            ),
            (CasError::Overflow, "numeric overflow"),
            (
                CasError::InjectedUploadInterruption {
                    boundary: crate::UploadLifecycleBoundary::AfterMetadataCommitted,
                },
                "injected upload interruption at AfterMetadataCommitted",
            ),
            (
                CasError::Internal("join error".to_owned()),
                "internal error: join error",
            ),
        ] {
            let error: &dyn std::error::Error = &error;
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn cas_error_conversions_preserve_category_and_message() {
        for (error, expected) in [
            (
                CasError::from_object_store("disk full"),
                CasError::ObjectStore("disk full".to_owned()),
            ),
            (
                CasError::from_index("key missing"),
                CasError::Index("key missing".to_owned()),
            ),
            (
                CasError::from_record("txn failed"),
                CasError::Record("txn failed".to_owned()),
            ),
        ] {
            assert_eq!(error, expected);
        }
        assert_eq!(
            CasError::from_object_store("disk full").to_string(),
            "object store error: disk full"
        );
    }
}
