use object_store::aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut, S3CopyIfNotExists};

use super::{S3ObjectStoreConfig, S3ObjectStoreError};

/// Builds an S3-compatible [`AmazonS3`] client from the given configuration.
///
/// This is called by [`S3ObjectStore::new`] and is extracted here to keep
/// client construction separate from the store's higher-level impl.
pub(crate) fn build_amazon_s3_client(
    config: &S3ObjectStoreConfig,
) -> Result<AmazonS3, S3ObjectStoreError> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(config.bucket.clone())
        .with_region(config.region.clone())
        .with_allow_http(config.allow_http)
        .with_virtual_hosted_style_request(config.virtual_hosted_style_request)
        .with_copy_if_not_exists(S3CopyIfNotExists::Multipart)
        .with_conditional_put(S3ConditionalPut::ETagMatch);

    if let Some(ref endpoint) = config.endpoint {
        builder = builder.with_endpoint(endpoint.clone());
    }
    match (&config.access_key_id, &config.secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => {
            builder = builder
                .with_access_key_id(access_key_id.expose_secret())
                .with_secret_access_key(secret_access_key.expose_secret());
        }
        (None, None) => {}
        (Some(_), None) | (None, Some(_)) => {
            return Err(S3ObjectStoreError::IncompleteCredentials);
        }
    }
    if let Some(ref session_token) = config.session_token {
        builder = builder.with_token(session_token.expose_secret());
    }

    builder.build().map_err(S3ObjectStoreError::External)
}
