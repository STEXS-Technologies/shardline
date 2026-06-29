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
