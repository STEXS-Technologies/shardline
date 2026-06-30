use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

/// Hub API error type.
#[derive(Debug, Error)]
pub enum HubApiError {
    /// IO error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization or deserialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Resource not found.
    #[error("not found")]
    NotFound,

    /// Unauthorized access.
    #[error("unauthorized")]
    Unauthorized,

    /// Forbidden access.
    #[error("forbidden")]
    Forbidden,

    /// Resource conflict.
    #[error("conflict")]
    Conflict,

    /// Request payload too large.
    #[error("payload too large")]
    PayloadTooLarge,

    /// Invalid authentication token.
    #[error("invalid token")]
    InvalidToken,

    /// Repository not found.
    #[error("repository not found")]
    RepoNotFound,

    /// Revision not found.
    #[error("revision not found")]
    RevisionNotFound,

    /// Invalid path in request.
    #[error("invalid path: {0}")]
    PathValidation(String),

    /// CAS (content-addressable storage) error.
    #[error("cas error: {0}")]
    CasError(String),

    /// Optimistic concurrency conflict.
    #[error("optimistic concurrency conflict")]
    OptimisticConcurrency,

    /// Pkt-line encoding error.
    #[error("protocol error: {0}")]
    PktLine(#[from] crate::git::pktline::PktLineError),

    /// Pack file generation error.
    #[error("pack error: {0}")]
    Pack(#[from] crate::git::pack::PackError),
}

impl IntoResponse for HubApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Io(_) | Self::Json(_) | Self::CasError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::NotFound | Self::RepoNotFound | Self::RevisionNotFound => {
                StatusCode::NOT_FOUND
            }
            Self::Unauthorized | Self::InvalidToken => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::Conflict | Self::OptimisticConcurrency => StatusCode::CONFLICT,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::PathValidation(_) => StatusCode::BAD_REQUEST,
            Self::PktLine(_) | Self::Pack(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = ErrorBody {
            error: self.to_string(),
        };
        (status, Json(body)).into_response()
    }
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}
