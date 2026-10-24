use object_store::ClientOptions;
use object_store::aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut, S3CopyIfNotExists};

use super::{S3ObjectStoreConfig, S3ObjectStoreError};

/// Multipart completion needs different default headers, but read-only stores
/// should not pay for another HTTP pool and another load of the TLS trust roots.
/// Share successful initialization across store clones; failures remain retryable.
pub(super) struct ConditionalCompletionClient {
    config: S3ObjectStoreConfig,
    inner: tokio::sync::OnceCell<AmazonS3>,
}

impl ConditionalCompletionClient {
    pub(super) fn new(config: S3ObjectStoreConfig) -> Self {
        Self {
            config,
            inner: tokio::sync::OnceCell::new(),
        }
    }

    pub(super) async fn get(&self) -> Result<&AmazonS3, S3ObjectStoreError> {
        self.inner
            .get_or_try_init(|| async { build_conditional_completion_client(&self.config) })
            .await
    }

    #[cfg(test)]
    pub(super) fn is_initialized(&self) -> bool {
        self.inner.initialized()
    }
}

/// Builds an S3-compatible [`AmazonS3`] client from the given configuration.
///
/// This is called by [`S3ObjectStore::new`] and is extracted here to keep
/// client construction separate from the store's higher-level impl.
pub(crate) fn build_amazon_s3_client(
    config: &S3ObjectStoreConfig,
) -> Result<AmazonS3, S3ObjectStoreError> {
    build_client(
        config,
        object_store::RetryConfig::default(),
        false,
        ClientOptions::default(),
    )
}

/// This client is used only for multipart completion. Applying the condition to
/// CreateMultipartUpload or UploadPart would be invalid on some S3 providers.
pub(crate) fn build_conditional_completion_client(
    config: &S3ObjectStoreConfig,
) -> Result<AmazonS3, S3ObjectStoreError> {
    let headers = http::HeaderMap::from_iter([(
        http::header::IF_NONE_MATCH,
        http::HeaderValue::from_static("*"),
    )]);
    build_client(
        config,
        object_store::RetryConfig::default(),
        false,
        ClientOptions::default().with_default_headers(headers),
    )
}

pub(crate) fn build_single_attempt_client(
    config: &S3ObjectStoreConfig,
) -> Result<AmazonS3, S3ObjectStoreError> {
    build_client(
        config,
        object_store::RetryConfig {
            max_retries: 0,
            ..object_store::RetryConfig::default()
        },
        true,
        ClientOptions::default(),
    )
}

fn build_client(
    config: &S3ObjectStoreConfig,
    retry: object_store::RetryConfig,
    single_delete: bool,
    client_options: ClientOptions,
) -> Result<AmazonS3, S3ObjectStoreError> {
    let mut builder = AmazonS3Builder::new()
        .with_client_options(client_options)
        .with_retry(retry)
        .with_disable_bulk_delete(single_delete)
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

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_protocol::SecretString;

    #[tokio::test]
    async fn failed_completion_client_initialization_does_not_cache_a_failure() {
        let config = S3ObjectStoreConfig::new("assets".to_owned(), "us-east-1".to_owned())
            .with_credentials(
                Some(SecretString::from_secret("fixture-access")),
                None,
                None,
            );
        let client = ConditionalCompletionClient::new(config);
        for _ in 0..2 {
            assert!(matches!(
                client.get().await,
                Err(S3ObjectStoreError::IncompleteCredentials)
            ));
            assert!(!client.is_initialized());
        }
    }
}
