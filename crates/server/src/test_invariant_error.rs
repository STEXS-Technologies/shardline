use std::fmt::Display;

use thiserror::Error;

#[derive(Debug, Error)]
#[error("{message}")]
pub struct ServerTestInvariantError {
    message: String,
}

impl ServerTestInvariantError {
    #[must_use]
    pub fn new(message: impl Display) -> Self {
        Self {
            message: message.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_test_invariant_error_display() {
        let err = ServerTestInvariantError::new("something went wrong");
        assert_eq!(err.to_string(), "something went wrong");
    }

    #[test]
    fn server_test_invariant_error_debug() {
        let err = ServerTestInvariantError::new("test message");
        let debug = format!("{err:?}");
        assert!(debug.contains("ServerTestInvariantError"));
        assert!(debug.contains("test message"));
    }

    #[test]
    fn server_test_invariant_error_empty_message() {
        let err = ServerTestInvariantError::new("");
        assert_eq!(err.to_string(), "");
    }
}
