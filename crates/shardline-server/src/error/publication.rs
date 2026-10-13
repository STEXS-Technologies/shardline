//! Internal retryable publication errors, without changing public error enums.

use super::ServerError;

#[derive(Debug, thiserror::Error)]
#[error("prepared upload content is no longer available; retry the upload")]
struct ContentUnavailable;

pub(crate) fn content_unavailable() -> ServerError {
    ServerError::Io(std::io::Error::other(ContentUnavailable))
}

pub(super) fn is_content_unavailable(error: &std::io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.is::<ContentUnavailable>())
}

pub(super) fn io_message(error: &std::io::Error) -> &'static str {
    if is_content_unavailable(error) {
        "prepared upload content is no longer available; retry the upload"
    } else {
        "local storage operation failed"
    }
}
