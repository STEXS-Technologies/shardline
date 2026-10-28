//! Stream authenticated form payloads; successful EOF is the publication gate.
use std::sync::{Arc, Mutex};

use axum::body::{Body, Bytes};
use futures_util::stream;
use shardline_s3_adapter::{S3Error, VerifiedSigV4PostPolicy};

/// Preserve the S3 error envelope across the generic HTTP body error boundary.
#[derive(Clone, Default)]
pub(in crate::app::protocol_routes::s3) struct PostValidation(Arc<Mutex<Option<S3Error>>>);

impl PostValidation {
    pub(in crate::app::protocol_routes::s3) fn take_error(&self) -> Option<S3Error> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    }

    fn fail(&self, error: S3Error) -> std::io::Error {
        *self.0.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(error);
        std::io::Error::other("S3 form payload validation failed")
    }
}

struct Payload {
    field: multer::Field<'static>,
    multipart: multer::Multipart<'static>,
    policy: VerifiedSigV4PostPolicy,
    fields: Vec<(String, String)>,
    bucket: String,
    max_file_bytes: u64,
    policy_max_file_bytes: Option<u64>,
    length: u64,
    validation: PostValidation,
}

impl Payload {
    async fn next(&mut self) -> Result<Option<Bytes>, S3Error> {
        if let Some(bytes) = self
            .field
            .chunk()
            .await
            .map_err(|_error| S3Error::invalid_argument("Invalid uploaded file part"))?
        {
            self.length = self
                .length
                .checked_add(u64::try_from(bytes.len()).map_err(|_error| S3Error::internal())?)
                .ok_or_else(S3Error::internal)?;
            if self.length > self.max_file_bytes {
                return Err(super::entity_too_large());
            }
            if self
                .policy_max_file_bytes
                .is_some_and(|maximum| self.length > maximum)
            {
                return Err(S3Error::access_denied());
            }
            return Ok(Some(bytes));
        }
        self.policy
            .validate_form(&self.fields, &self.bucket, self.length)?;
        // Drop the field before polling its parent multipart parser.
        // multer enforces exclusive field ownership, including at EOF.
        Ok(None)
    }
}

pub(super) fn validated_body(
    field: multer::Field<'static>,
    multipart: multer::Multipart<'static>,
    policy: VerifiedSigV4PostPolicy,
    fields: Vec<(String, String)>,
    bucket: String,
    max_file_bytes: u64,
    validation: PostValidation,
) -> Result<Body, S3Error> {
    let policy_max_file_bytes = policy.maximum_file_size()?;
    let payload = Payload {
        field,
        multipart,
        policy,
        fields,
        bucket,
        max_file_bytes,
        policy_max_file_bytes,
        length: 0,
        validation,
    };
    Ok(Body::from_stream(stream::try_unfold(
        payload,
        |mut payload| async move {
            match payload.next().await {
                Ok(Some(bytes)) => Ok(Some((bytes, payload))),
                Ok(None) => {
                    let Payload {
                        field: completed_field,
                        multipart: mut remaining_form,
                        validation: verdict,
                        ..
                    } = payload;
                    drop(completed_field);
                    let trailing = remaining_form.next_field().await.map_err(|_error| {
                        verdict.fail(S3Error::invalid_argument("Invalid trailing multipart form"))
                    })?;
                    if trailing.is_some() {
                        return Err(verdict
                            .fail(S3Error::invalid_argument("The S3 file field must be last")));
                    }
                    Ok(None)
                }
                Err(error) => Err(payload.validation.fail(error)),
            }
        },
    )))
}
