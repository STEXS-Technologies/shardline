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
