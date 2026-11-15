use axum::{
    Json,
    http::{
        HeaderValue,
        header::{RETRY_AFTER, WWW_AUTHENTICATE},
    },
    response::{IntoResponse, Response},
};

use super::ServerError;

/// OCI Distribution-spec formatted error wrapper.
///
/// Wraps [`ServerError`] and implements [`IntoResponse`] using the OCI error
/// envelope format mandated by the OCI Distribution specification:
///
/// ```json
/// { "errors": [{ "code": "...", "message": "...", "detail": null }] }
/// ```
#[derive(Debug)]
pub(crate) struct OciError(pub ServerError);

impl From<ServerError> for OciError {
    fn from(error: ServerError) -> Self {
        Self(error)
    }
}

pub(crate) fn protocol_error_response(
    status: axum::http::StatusCode,
    code: &'static str,
    message: &str,
) -> Response {
    (
        status,
        Json(serde_json::json!({"errors":[{"code":code,"message":message,"detail":null}]})),
    )
        .into_response()
}

impl IntoResponse for OciError {
    fn into_response(self) -> Response {
        let (code, challenge) = if let ServerError::UnauthorizedChallenge(value) = &self.0 {
            ("UNAUTHORIZED", HeaderValue::from_str(value).ok())
        } else {
            let challenge = matches!(
                self.0,
                ServerError::MissingAuthorization
                    | ServerError::InvalidAuthorizationHeader
                    | ServerError::InvalidToken(_)
            )
            .then(|| HeaderValue::from_static("Bearer realm=\"shardline\""));
            (self.0.oci_error_code(), challenge)
        };
        let mut response = protocol_error_response(self.0.status_code(), code, &self.0.to_string());
        if let Some(challenge) = challenge {
            response.headers_mut().insert(WWW_AUTHENTICATE, challenge);
        }
        if self.0.is_retryable_publication() {
            response
                .headers_mut()
                .insert(RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}
