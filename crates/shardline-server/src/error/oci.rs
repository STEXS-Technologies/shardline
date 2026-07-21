use axum::{
    Json,
    http::{HeaderValue, header::WWW_AUTHENTICATE},
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

impl IntoResponse for OciError {
    fn into_response(self) -> Response {
        let status = self.0.status_code();
        let should_attach_default_challenge = matches!(
            self.0,
            ServerError::MissingAuthorization
                | ServerError::InvalidAuthorizationHeader
                | ServerError::InvalidToken(_)
        );
        let custom_challenge = if let ServerError::UnauthorizedChallenge(custom_header) = &self.0 {
            Some(custom_header.as_str())
        } else {
            None
        };
        let body = Json(serde_json::json!({
            "errors": [{
                "code": self.0.oci_error_code(),
                "message": self.0.to_string(),
                "detail": null
            }]
        }));
        let mut response = (status, body).into_response();
        if let Some(custom_header) = custom_challenge {
            if let Ok(header_value) = HeaderValue::from_str(custom_header) {
                response
                    .headers_mut()
                    .insert(WWW_AUTHENTICATE, header_value);
            }
        } else if should_attach_default_challenge {
            response.headers_mut().insert(
                WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"shardline\""),
            );
        }
        response
    }
}
