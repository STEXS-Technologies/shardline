use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

use crate::git::{pack::PackError, pktline::PktLineError};

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

    /// Conflict (e.g. parent commit mismatch).
    #[error("conflict: {0}")]
    Conflict(String),

    /// Invalid authentication token.
    #[error("invalid token")]
    InvalidToken,

    /// The token signing key is misconfigured.
    #[error("token signing key is misconfigured: {0}")]
    SigningKeyError(String),

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
    PktLine(#[from] PktLineError),

    /// Pack file generation error.
    #[error("pack error: {0}")]
    Pack(#[from] PackError),
}

impl IntoResponse for HubApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Self::Io(_)
            | Self::Json(_)
            | Self::CasError(_)
            | Self::PktLine(_)
            | Self::Pack(_)
            | Self::SigningKeyError(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_owned(),
            ),
            Self::NotFound | Self::RepoNotFound | Self::RevisionNotFound => {
                (StatusCode::NOT_FOUND, self.to_string())
            }
            Self::Unauthorized | Self::InvalidToken => (StatusCode::UNAUTHORIZED, self.to_string()),
            Self::Forbidden => (StatusCode::FORBIDDEN, self.to_string()),
            Self::Conflict(_) => (StatusCode::CONFLICT, self.to_string()),
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

    // -----------------------------------------------------------------------
    // Status code mappings
    // -----------------------------------------------------------------------

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
    fn conflict_maps_to_409() {
        let (status, body) = status_and_body(HubApiError::Conflict("parent mismatch".into()));
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(body.contains("conflict: parent mismatch"));
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
    fn signing_key_error_maps_to_500() {
        let (status, body) = status_and_body(HubApiError::SigningKeyError("misconfig".into()));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("internal error"));
    }

    #[test]
    fn pkt_line_error_maps_to_500() {
        let err = HubApiError::from(crate::git::pktline::PktLineError::PayloadTooLarge {
            size: 100,
            max: 50,
        });
        let (status, body) = status_and_body(err);
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("internal error"));
    }

    #[test]
    fn pack_error_maps_to_500() {
        let err = HubApiError::from(crate::git::pack::PackError::TooManyObjects);
        let (status, body) = status_and_body(err);
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("internal error"));
    }

    // -----------------------------------------------------------------------
    // Display implementations
    // -----------------------------------------------------------------------

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
        assert_eq!(
            HubApiError::Conflict("parent mismatch".into()).to_string(),
            "conflict: parent mismatch"
        );
        assert_eq!(
            HubApiError::SigningKeyError("key not found".into()).to_string(),
            "token signing key is misconfigured: key not found"
        );
        assert!(
            HubApiError::Io(std::io::Error::other("disk full"))
                .to_string()
                .contains("io error: disk full")
        );
        assert!(
            HubApiError::Json(serde_json::from_str::<()>("bad").unwrap_err())
                .to_string()
                .contains("json error")
        );
    }

    #[test]
    fn io_error_display_includes_detail() {
        let err = HubApiError::Io(std::io::Error::other("disk full"));
        let msg = err.to_string();
        assert!(msg.contains("io error"), "expected io error, got: {msg}");
        assert!(msg.contains("disk full"), "expected detail, got: {msg}");
    }

    #[test]
    fn json_error_display_includes_detail() {
        let json_err = serde_json::from_str::<serde_json::Value>("{broken").unwrap_err();
        let err = HubApiError::Json(json_err);
        let msg = err.to_string();
        assert!(
            msg.contains("json error"),
            "expected json error, got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // From conversions
    // -----------------------------------------------------------------------

    #[test]
    fn from_pkt_line_error() {
        let err: HubApiError =
            crate::git::pktline::PktLineError::PayloadTooLarge { size: 100, max: 50 }.into();
        assert!(matches!(err, HubApiError::PktLine(_)));
    }

    #[test]
    fn from_pack_error() {
        let err: HubApiError = crate::git::pack::PackError::TooManyObjects.into();
        assert!(matches!(err, HubApiError::Pack(_)));
    }

    #[test]
    fn from_io_error() {
        let err: HubApiError = std::io::Error::other("fail").into();
        assert!(matches!(err, HubApiError::Io(_)));
    }

    #[test]
    fn from_json_error() {
        let json_err = serde_json::from_str::<()>("bad").unwrap_err();
        let err: HubApiError = json_err.into();
        assert!(matches!(err, HubApiError::Json(_)));
    }

    #[test]
    fn debug_format_includes_variant_names() {
        let mut cases: Vec<(HubApiError, &str)> = vec![
            (HubApiError::NotFound, "NotFound"),
            (HubApiError::Unauthorized, "Unauthorized"),
            (HubApiError::Forbidden, "Forbidden"),
            (HubApiError::InvalidToken, "InvalidToken"),
            (HubApiError::RepoNotFound, "RepoNotFound"),
            (HubApiError::RevisionNotFound, "RevisionNotFound"),
            (HubApiError::PathValidation("x".into()), "PathValidation"),
            (HubApiError::CasError("y".into()), "CasError"),
            (HubApiError::Conflict("z".into()), "Conflict"),
            (HubApiError::SigningKeyError("k".into()), "SigningKeyError"),
        ];
        // Add variants using From conversions
        cases.push((std::io::Error::other("io").into(), "Io"));
        cases.push((
            serde_json::from_str::<()>("bad").unwrap_err().into(),
            "Json",
        ));
        cases.push((
            crate::git::pktline::PktLineError::PayloadTooLarge { size: 100, max: 50 }.into(),
            "PktLine",
        ));
        cases.push((crate::git::pack::PackError::TooManyObjects.into(), "Pack"));
        for (error, expected_variant) in &cases {
            let debug = format!("{error:?}");
            assert!(
                debug.contains(expected_variant),
                "expected Debug output to contain '{expected_variant}', got: {debug}"
            );
        }
    }
}
