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
            Self::Io(_) | Self::Json(_) | Self::CasError(_) | Self::PktLine(_) | Self::Pack(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_owned(),
            ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    fn status_and_body(error: HubApiError) -> (StatusCode, String) {
        let response = error.into_response();
        let status = response.status();
        let body = tokio::runtime::Runtime::new().unwrap().block_on(async {
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            String::from_utf8(bytes.to_vec()).unwrap()
        });
        (status, body)
    }

    #[test]
    fn not_found_maps_to_404() {
        let (status, body) = status_and_body(HubApiError::NotFound);
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.contains("not found"));
    }

    #[test]
    fn repo_not_found_maps_to_404() {
        let (status, body) = status_and_body(HubApiError::RepoNotFound);
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.contains("repository not found"));
    }

    #[test]
    fn revision_not_found_maps_to_404() {
        let (status, body) = status_and_body(HubApiError::RevisionNotFound);
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.contains("revision not found"));
    }

    #[test]
    fn unauthorized_maps_to_401() {
        let (status, body) = status_and_body(HubApiError::Unauthorized);
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(body.contains("unauthorized"));
    }

    #[test]
    fn invalid_token_maps_to_401() {
        let (status, body) = status_and_body(HubApiError::InvalidToken);
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(body.contains("invalid token"));
    }

    #[test]
    fn forbidden_maps_to_403() {
        let (status, body) = status_and_body(HubApiError::Forbidden);
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(body.contains("forbidden"));
    }

    #[test]
    fn path_validation_maps_to_400() {
        let (status, body) = status_and_body(HubApiError::PathValidation("bad path".into()));
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("invalid path: bad path"));
    }

    #[test]
    fn io_error_maps_to_500() {
        let io_err = std::io::Error::other("disk failure");
        let (status, body) = status_and_body(HubApiError::Io(io_err));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("internal error"));
    }

    #[test]
    fn json_error_maps_to_500() {
        let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let (status, body) = status_and_body(HubApiError::Json(json_err));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("internal error"));
    }

    #[test]
    fn cas_error_maps_to_500() {
        let (status, body) = status_and_body(HubApiError::CasError("oops".into()));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("internal error"));
    }

    #[test]
    fn error_display_messages() {
        assert_eq!(HubApiError::NotFound.to_string(), "not found");
        assert_eq!(HubApiError::Unauthorized.to_string(), "unauthorized");
        assert_eq!(HubApiError::Forbidden.to_string(), "forbidden");
        assert_eq!(HubApiError::InvalidToken.to_string(), "invalid token");
        assert_eq!(
            HubApiError::RepoNotFound.to_string(),
            "repository not found"
        );
        assert_eq!(
            HubApiError::RevisionNotFound.to_string(),
            "revision not found"
        );
        assert_eq!(
            HubApiError::PathValidation("x".into()).to_string(),
            "invalid path: x"
        );
        assert_eq!(
            HubApiError::CasError("y".into()).to_string(),
            "cas error: y"
        );
    }
}
