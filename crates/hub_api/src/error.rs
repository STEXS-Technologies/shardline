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



    /// Pkt-line encoding error.
    #[error("protocol error: {0}")]
    PktLine(#[from] crate::git::pktline::PktLineError),

    /// Pack file generation error.
    #[error("pack error: {0}")]
    Pack(#[from] crate::git::pack::PackError),
}

impl IntoResponse for HubApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Self::Io(_) | Self::Json(_) | Self::CasError(_) | Self::PktLine(_) | Self::Pack(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_owned())
            }
            Self::NotFound | Self::RepoNotFound | Self::RevisionNotFound => {
                (StatusCode::NOT_FOUND, self.to_string())
            }
            Self::Unauthorized | Self::InvalidToken => (StatusCode::UNAUTHORIZED, self.to_string()),
            Self::Forbidden => (StatusCode::FORBIDDEN, self.to_string()),
            Self::PathValidation(_) => (StatusCode::BAD_REQUEST, self.to_string()),
        };
        let body = ErrorBody { error: message };
        (status, Json(body)).into_response()
    }
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}
