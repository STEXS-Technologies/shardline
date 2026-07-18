use std::fmt::Display;

use thiserror::Error;

#[derive(Debug, Error)]
#[error("{message}")]
pub(crate) struct LocalSqliteInvariantError {
    message: String,
}

impl LocalSqliteInvariantError {
    #[must_use]
    pub(crate) fn new(message: impl Display) -> Self {
        Self {
            message: message.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invariant_error_display_formats_message() {
        let err = LocalSqliteInvariantError::new("something went wrong");
        assert_eq!(err.to_string(), "something went wrong");
    }

    #[test]
    fn invariant_error_new_with_different_types() {
        let from_str = LocalSqliteInvariantError::new("hello");
        let from_string = LocalSqliteInvariantError::new(String::from("hello"));
        let from_int = LocalSqliteInvariantError::new(42);

        assert_eq!(from_str.to_string(), "hello");
        assert_eq!(from_string.to_string(), "hello");
        assert_eq!(from_int.to_string(), "42");
    }

    #[test]
    fn invariant_error_debug_output() {
        let err = LocalSqliteInvariantError::new("test");
        let debug = format!("{err:?}");
        assert!(debug.contains("LocalSqliteInvariantError"));
        assert!(debug.contains("test"));
    }
}
