//! Internal retryable publication errors, without changing public error enums.

use super::ServerError;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
struct ContentUnavailable {
    message: &'static str,
}

pub(crate) fn content_unavailable() -> ServerError {
    ServerError::Io(content_unavailable_io())
}

pub(crate) fn content_unavailable_io() -> std::io::Error {
    std::io::Error::other(ContentUnavailable {
        message: "prepared upload content is no longer available; retry the upload",
    })
}

pub(crate) fn native_content_unavailable_io() -> std::io::Error {
    std::io::Error::other(ContentUnavailable {
        message: "native shard backing content is no longer available; upload missing backing content again, then retry the shard",
    })
}

pub(super) fn is_content_unavailable(error: &std::io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.is::<ContentUnavailable>())
}

pub(super) fn io_message(error: &std::io::Error) -> &'static str {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<ContentUnavailable>())
        .map_or("local storage operation failed", |source| source.message)
}
