use super::{S3ObjectStoreConfig, S3ObjectStoreError};
use crate::ObjectPrefix;

/// Validates that the S3 object store configuration is complete and consistent.
///
/// Checks that bucket and region are non-empty and that the key prefix,
/// if present, is a valid [`ObjectPrefix`].
pub(crate) fn validate_s3_config(config: &S3ObjectStoreConfig) -> Result<(), S3ObjectStoreError> {
    if config.bucket.trim().is_empty() {
        return Err(S3ObjectStoreError::EmptyBucket);
    }
    if config.region.trim().is_empty() {
        return Err(S3ObjectStoreError::EmptyRegion);
    }
    if let Some(ref prefix) = config.key_prefix {
        ObjectPrefix::parse(prefix).map_err(S3ObjectStoreError::InvalidKeyPrefix)?;
    }
    Ok(())
}
