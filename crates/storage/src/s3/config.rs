use std::fmt;

use shardline_protocol::SecretString;

use super::normalize_prefix;

/// S3-compatible object store configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct S3ObjectStoreConfig {
    pub(crate) bucket: String,
    pub(crate) region: String,
    pub(crate) endpoint: Option<String>,
    pub(crate) access_key_id: Option<SecretString>,
    pub(crate) secret_access_key: Option<SecretString>,
    pub(crate) session_token: Option<SecretString>,
    pub(crate) key_prefix: Option<String>,
    pub(crate) allow_http: bool,
    pub(crate) virtual_hosted_style_request: bool,
}

impl S3ObjectStoreConfig {
    /// Creates S3-compatible object storage configuration.
    #[must_use]
    pub const fn new(bucket: String, region: String) -> Self {
        Self {
            bucket,
            region,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            key_prefix: None,
            allow_http: false,
            virtual_hosted_style_request: false,
        }
    }

    /// Adds a custom S3-compatible endpoint URL.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: Option<String>) -> Self {
        self.endpoint = endpoint;
        self
    }

    /// Adds static access-key credentials.
    #[must_use]
    pub fn with_credentials(
        mut self,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
    ) -> Self {
        self.access_key_id = access_key_id.map(SecretString::new);
        self.secret_access_key = secret_access_key.map(SecretString::new);
        self.session_token = session_token.map(SecretString::new);
        self
    }

    /// Adds an object-key prefix under the bucket.
    #[must_use]
    pub fn with_key_prefix(mut self, key_prefix: Option<&str>) -> Self {
        self.key_prefix = key_prefix.and_then(normalize_prefix);
        self
    }

    /// Allows HTTP endpoints for local S3-compatible deployments.
    #[must_use]
    pub const fn with_allow_http(mut self, allow_http: bool) -> Self {
        self.allow_http = allow_http;
        self
    }

    /// Enables virtual-hosted-style requests.
    #[must_use]
    pub const fn with_virtual_hosted_style_request(
        mut self,
        virtual_hosted_style_request: bool,
    ) -> Self {
        self.virtual_hosted_style_request = virtual_hosted_style_request;
        self
    }

    /// Returns the configured bucket.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the configured key prefix.
    #[must_use]
    pub fn key_prefix(&self) -> Option<&str> {
        self.key_prefix.as_deref()
    }
}

impl fmt::Debug for S3ObjectStoreConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3ObjectStoreConfig")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field(
                "access_key_id",
                &self.access_key_id.as_ref().map(|_value| "***"),
            )
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_value| "***"),
            )
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_value| "***"),
            )
            .field("key_prefix", &self.key_prefix)
            .field("allow_http", &self.allow_http)
            .field(
                "virtual_hosted_style_request",
                &self.virtual_hosted_style_request,
            )
            .finish()
    }
}
