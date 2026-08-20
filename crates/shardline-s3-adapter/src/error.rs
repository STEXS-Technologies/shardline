use axum::{
    body::Body,
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};

use crate::types::S3ErrorBody;

/// A ready-to-send S3 error envelope.
///
/// Serializes to the S3 XML `<Error>` body with the matching HTTP status code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Error {
    /// The S3 error code, for example `NoSuchKey`.
    pub code: &'static str,
    /// Human-readable error message.
    pub message: String,
    /// The HTTP status code for the error response.
    pub status: StatusCode,
}

impl S3Error {
    /// The object did not exist.
    #[must_use]
    pub fn no_such_key(key: &str) -> Self {
        let message = if key.is_empty() {
            "The specified key does not exist.".to_owned()
        } else {
            format!("The specified key does not exist: {key}")
        };
        Self {
            code: "NoSuchKey",
            message,
            status: StatusCode::NOT_FOUND,
        }
    }

    /// The bucket did not exist (or could not be decoded).
    #[must_use]
    pub fn no_such_bucket(bucket: &str) -> Self {
        Self {
            code: "NoSuchBucket",
            message: format!("The specified bucket does not exist: {bucket}"),
            status: StatusCode::NOT_FOUND,
        }
    }

    /// The caller lacked permission for the request.
    #[must_use]
    pub fn access_denied() -> Self {
        Self {
            code: "AccessDenied",
            message: "Access Denied".to_owned(),
            status: StatusCode::FORBIDDEN,
        }
    }

    /// The requested byte range could not be satisfied.
    #[must_use]
    pub fn invalid_range() -> Self {
        Self {
            code: "InvalidRange",
            message: "The requested range is not satisfiable".to_owned(),
            status: StatusCode::RANGE_NOT_SATISFIABLE,
        }
    }

    /// The referenced multipart upload session did not exist.
    #[must_use]
    pub fn no_such_upload() -> Self {
        Self {
            code: "NoSuchUpload",
            message: "The specified multipart upload does not exist.".to_owned(),
            status: StatusCode::NOT_FOUND,
        }
    }

    /// An uploaded part was invalid.
    #[must_use]
    pub fn invalid_part() -> Self {
        Self {
            code: "InvalidPart",
            message: "One or more of the specified parts could not be found.".to_owned(),
            status: StatusCode::BAD_REQUEST,
        }
    }

    /// The upload body was smaller than the minimum allowed size.
    #[must_use]
    pub fn entity_too_small() -> Self {
        Self {
            code: "EntityTooSmall",
            message: "Your proposed upload is smaller than the minimum allowed size".to_owned(),
            status: StatusCode::BAD_REQUEST,
        }
    }

    /// The upload exceeded the maximum allowed size (S3's `EntityTooLarge`,
    /// `400` — distinct from the HTTP-level `413` body-limit envelope).
    #[must_use]
    pub fn entity_too_large() -> Self {
        Self {
            code: "EntityTooLarge",
            message: "Your proposed upload exceeds the maximum allowed size".to_owned(),
            status: StatusCode::BAD_REQUEST,
        }
    }

    /// The operation is recognized but not implemented by the S3 frontend.
    #[must_use]
    pub fn not_implemented() -> Self {
        Self {
            code: "NotImplemented",
            message: "A header you provided implies functionality that is not implemented."
                .to_owned(),
            status: StatusCode::NOT_IMPLEMENTED,
        }
    }

    /// An internal server failure.
    #[must_use]
    pub fn internal() -> Self {
        Self {
            code: "InternalError",
            message: "We encountered an internal error. Please try again.".to_owned(),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// The server is temporarily overloaded (an internal resource cap was hit,
    /// for example the maximum number of active multipart upload sessions).
    #[must_use]
    pub fn slow_down() -> Self {
        Self {
            code: "SlowDown",
            message: "Please reduce your request rate.".to_owned(),
            status: StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// A request parameter was invalid.
    #[must_use]
    pub fn invalid_argument(message: &str) -> Self {
        Self {
            code: "InvalidArgument",
            message: message.to_owned(),
            status: StatusCode::BAD_REQUEST,
        }
    }

    /// The request body XML did not validate against the S3 published schema
    /// (`MalformedXML`, `400`). AWS S3 uses this for `DeleteObjects` bodies
    /// that list no keys, more than `MAX_S3_DELETE_KEYS` keys, or otherwise
    /// violate the published schema.
    #[must_use]
    pub fn malformed_xml() -> Self {
        Self {
            code: "MalformedXML",
            message: "The XML you provided was not well-formed or did not validate against our \
                      published schema."
                .to_owned(),
            status: StatusCode::BAD_REQUEST,
        }
    }

    /// A conditional request (`If-Match` / `If-None-Match`) failed against the
    /// stored object (`PreconditionFailed`, `412`).
    #[must_use]
    pub fn precondition_failed() -> Self {
        Self {
            code: "PreconditionFailed",
            message: "At least one of the pre-conditions you specified did not hold.".to_owned(),
            status: StatusCode::PRECONDITION_FAILED,
        }
    }

    /// Returns this error with a replaced message (keeps the code and status).
    #[must_use]
    pub fn with_message(mut self, message: String) -> Self {
        self.message = message;
        self
    }

    /// Builds the S3 error envelope for a coarse server-error class.
    ///
    /// The concrete `From<shardline_server::error::ServerError>` impl lives in
    /// the `shardline-server` crate (where `ServerError` is defined, following
    /// the `shardline-oci-adapter` precedent); it classifies `ServerError`
    /// variants into [`S3ErrorClass`] and delegates here.
    #[must_use]
    pub fn from_class(class: S3ErrorClass) -> Self {
        match class {
            S3ErrorClass::RangeNotSatisfiable => Self::invalid_range(),
            S3ErrorClass::NotFound => Self::no_such_key(""),
            S3ErrorClass::AccessDenied => Self::access_denied(),
            S3ErrorClass::Internal => Self::internal(),
        }
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let body = S3ErrorBody {
            code: self.code.to_owned(),
            message: self.message,
            key: None,
            request_id: None,
        };
        let mut response = Response::new(Body::from(body.to_xml()));
        *response.status_mut() = self.status;
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
        response
    }
}

/// Coarse server-error classification used by the S3 frontend to translate a
/// server-layer failure into an S3 error envelope.
///
/// The `shardline-server` crate maps its `ServerError` variants onto these
/// classes (see [`S3ErrorClassify`]) so the S3 adapter stays free of a
/// dependency on the server crate (which would be a dependency cycle once the
/// frontend routes are wired into `shardline-server`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3ErrorClass {
    /// The requested byte range could not be satisfied
    /// (`ServerError::RangeNotSatisfiable`).
    RangeNotSatisfiable,
    /// The requested object or bucket did not exist (`ServerError::NotFound`).
    NotFound,
    /// The caller lacked permission (`ServerError` authorization variants).
    AccessDenied,
    /// Any other server failure.
    Internal,
}

/// Classifies a server-layer error into an [`S3ErrorClass`].
///
/// # Examples
///
/// The `shardline-server` crate implements this for `ServerError`:
///
/// ```ignore
/// impl S3ErrorClassify for ServerError {
///     fn s3_class(&self) -> S3ErrorClass {
///         match self {
///             Self::RangeNotSatisfiable => S3ErrorClass::RangeNotSatisfiable,
///             Self::NotFound => S3ErrorClass::NotFound,
///             Self::MissingAuthorization
///             | Self::InvalidAuthorizationHeader
///             | Self::InvalidToken(_)
///             | Self::InsufficientScope
///             | Self::ProviderDenied => S3ErrorClass::AccessDenied,
///             _ => S3ErrorClass::Internal,
///         }
///     }
/// }
/// ```
pub trait S3ErrorClassify {
    /// Returns the S3 error class for this server error.
    fn s3_class(&self) -> S3ErrorClass;
}

impl<E: S3ErrorClassify> From<E> for S3Error {
    fn from(value: E) -> Self {
        Self::from_class(value.s3_class())
    }
}

impl From<crate::multipart::S3SessionError> for S3Error {
    fn from(value: crate::multipart::S3SessionError) -> Self {
        match value {
            crate::multipart::S3SessionError::NotFound => Self::no_such_upload(),
            crate::multipart::S3SessionError::TooManySessions => Self::slow_down(),
            crate::multipart::S3SessionError::TooManyPartFiles => Self::slow_down(),
            crate::multipart::S3SessionError::MissingPart(_) => Self::invalid_part(),
            crate::multipart::S3SessionError::InvalidPartNumber => {
                Self::invalid_argument("partNumber must be between 1 and 10000")
            }
            crate::multipart::S3SessionError::SessionQuotaExceeded
            | crate::multipart::S3SessionError::AggregateQuotaExceeded => Self::entity_too_large(),
            crate::multipart::S3SessionError::Io(error) => {
                Self::internal().with_message(error.to_string())
            }
            crate::multipart::S3SessionError::Json(error) => {
                Self::internal().with_message(error.to_string())
            }
            crate::multipart::S3SessionError::InvalidUploadId => {
                Self::internal().with_message("invalid upload session id".to_owned())
            }
            crate::multipart::S3SessionError::Overflow => {
                Self::internal().with_message("upload session overflow".to_owned())
            }
            crate::multipart::S3SessionError::BlockingTask(error) => {
                Self::internal().with_message(error.to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::arithmetic_side_effects,
        clippy::option_if_let_else,
        clippy::unreachable,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use
    )]

    use super::*;

    #[test]
    fn no_such_key_constructor_maps_to_404() {
        let error = S3Error::no_such_key("data/model.pt");
        assert_eq!(error.code, "NoSuchKey");
        assert_eq!(error.status, StatusCode::NOT_FOUND);
        assert!(error.message.contains("data/model.pt"));
    }

    #[test]
    fn no_such_bucket_constructor_maps_to_404() {
        let error = S3Error::no_such_bucket("acme.models");
        assert_eq!(error.code, "NoSuchBucket");
        assert_eq!(error.status, StatusCode::NOT_FOUND);
        assert!(error.message.contains("acme.models"));
    }

    #[test]
    fn access_denied_constructor_maps_to_403() {
        let error = S3Error::access_denied();
        assert_eq!(error.code, "AccessDenied");
        assert_eq!(error.status, StatusCode::FORBIDDEN);
    }

    #[test]
    fn invalid_range_constructor_maps_to_416() {
        let error = S3Error::invalid_range();
        assert_eq!(error.code, "InvalidRange");
        assert_eq!(error.status, StatusCode::RANGE_NOT_SATISFIABLE);
    }

    #[test]
    fn no_such_upload_constructor_maps_to_404() {
        let error = S3Error::no_such_upload();
        assert_eq!(error.code, "NoSuchUpload");
        assert_eq!(error.status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn invalid_part_constructor_maps_to_400() {
        let error = S3Error::invalid_part();
        assert_eq!(error.code, "InvalidPart");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn entity_too_small_constructor_maps_to_400() {
        let error = S3Error::entity_too_small();
        assert_eq!(error.code, "EntityTooSmall");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn not_implemented_constructor_maps_to_501() {
        let error = S3Error::not_implemented();
        assert_eq!(error.code, "NotImplemented");
        assert_eq!(error.status, StatusCode::NOT_IMPLEMENTED);
    }

    #[test]
    fn internal_constructor_maps_to_500() {
        let error = S3Error::internal();
        assert_eq!(error.code, "InternalError");
        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn malformed_xml_constructor_maps_to_400() {
        let error = S3Error::malformed_xml();
        assert_eq!(error.code, "MalformedXML");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(
            error.message.contains("published schema"),
            "{}",
            error.message
        );
    }

    #[test]
    fn precondition_failed_constructor_maps_to_412() {
        let error = S3Error::precondition_failed();
        assert_eq!(error.code, "PreconditionFailed");
        assert_eq!(error.status, StatusCode::PRECONDITION_FAILED);
    }

    #[tokio::test]
    async fn s3_error_into_response_emits_xml_with_status() {
        let response = S3Error::no_such_key("data/model.pt").into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("application/xml")
        );
        let bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(body.contains("<Code>NoSuchKey</Code>"));
        assert!(
            body.contains("<Message>The specified key does not exist: data/model.pt</Message>")
        );
        assert!(body.contains("</Error>"));
    }

    /// A stand-in for `shardline_server::error::ServerError` that mirrors the
    /// classification the server crate applies; exercising the full
    /// `ServerError` → `S3Error` mapping matrix through the generic `From`.
    #[derive(Debug, Clone, Copy)]
    enum TestServerError {
        RangeNotSatisfiable,
        NotFound,
        AccessDenied,
        Internal,
    }

    impl S3ErrorClassify for TestServerError {
        fn s3_class(&self) -> S3ErrorClass {
            match self {
                Self::RangeNotSatisfiable => S3ErrorClass::RangeNotSatisfiable,
                Self::NotFound => S3ErrorClass::NotFound,
                Self::AccessDenied => S3ErrorClass::AccessDenied,
                Self::Internal => S3ErrorClass::Internal,
            }
        }
    }

    #[test]
    fn server_error_mapping_matrix_range_not_satisfiable() {
        let error = S3Error::from(TestServerError::RangeNotSatisfiable);
        assert_eq!(error.code, "InvalidRange");
        assert_eq!(error.status, StatusCode::RANGE_NOT_SATISFIABLE);
    }

    #[test]
    fn server_error_mapping_matrix_not_found() {
        let error = S3Error::from(TestServerError::NotFound);
        assert_eq!(error.code, "NoSuchKey");
        assert_eq!(error.status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn server_error_mapping_matrix_access_denied() {
        let error = S3Error::from(TestServerError::AccessDenied);
        assert_eq!(error.code, "AccessDenied");
        assert_eq!(error.status, StatusCode::FORBIDDEN);
    }

    #[test]
    fn server_error_mapping_matrix_internal() {
        let error = S3Error::from(TestServerError::Internal);
        assert_eq!(error.code, "InternalError");
        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
    }
}
