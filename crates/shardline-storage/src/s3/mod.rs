pub mod client;
pub mod config;
pub mod credentials;
pub mod error;
pub mod multipart;
pub mod operations;
pub mod types;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use operations::{
    chunk_hash, existing_copy_outcome, existing_object_outcome_from_file,
    stream_payload_for_range, temporary_upload_location, validated_external_range,
    verify_file_length, verify_integrity,
};

pub use config::S3ObjectStoreConfig;
pub use error::S3ObjectStoreError;
pub use operations::S3ObjectStore;
pub use types::{BeginMultipartUploadResult, S3ByteStream, S3MultipartUploadWriter};

pub(crate) use types::{
    LARGE_COPY_CHUNK_BYTES, MAX_SINGLE_COPY_BYTES, STREAM_COMPARE_CHUNK_BYTES,
    STREAM_UPLOAD_CHUNK_BYTES, TEMP_UPLOAD_COUNTER, is_temp_upload_key, normalize_prefix,
    temp_key_for,
};
