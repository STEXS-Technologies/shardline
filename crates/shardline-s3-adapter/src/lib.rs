#![deny(unsafe_code)]

//! S3-compatible protocol adapter for the Shardline server ecosystem.
//!
//! This crate provides the S3 frontend's protocol primitives:
//!
//! - bucket (`{owner}.{name}`) encoding and decoding and the storage
//!   object-key layout (`protocols/s3/{scope_namespace}/{key}`) — [`key`];
//! - the S3 XML wire models and error envelope — [`types`] and [`error`];
//! - the SigV4 → bearer-token auth bridge and bucket-scope binding — [`auth`];
//! - the query-param sub-resource dispatch set, ETag formatting, and byte-range
//!   parsing — [`protocol_support`].
//!
//! # Quick start
//!
//! ```
//! use shardline_s3_adapter::{decode_bucket, encode_bucket, s3_object_key};
//!
//! // A bucket is the single dotted segment `{owner}.{name}` of the token scope.
//! let bucket = encode_bucket("acme", "models");
//! assert_eq!(
//!     decode_bucket(&bucket)?,
//!     ("acme".to_owned(), "models".to_owned())
//! );
//!
//! // Object keys live under the `protocols/s3/{scope_namespace}/` prefix.
//! let key = s3_object_key("global", "data/model.pt")?;
//! assert_eq!(key.as_str(), "protocols/s3/global/data/model.pt");
//!
//! // Errors serialize to the S3 XML envelope with the correct status.
//! use axum::response::IntoResponse;
//! let response = shardline_s3_adapter::S3Error::no_such_key("data/model.pt")
//!     .into_response();
//! assert_eq!(response.status(), 404);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod auth;
mod error;
mod key;
mod protocol_support;
mod types;

pub use auth::{extract_access_key, extract_access_key_from_query, require_s3_bucket_binding};
pub use error::{S3Error, S3ErrorClass, S3ErrorClassify};
pub use key::{BucketDecodeError, S3KeyError, decode_bucket, encode_bucket, s3_object_key};
pub use protocol_support::{QueryMap, S3SubResource, classify, etag_header, parse_s3_range};
pub use types::{
    CompleteMultipartUploadResult, Contents, HeadObjectHeaders, ListBucketResult,
    PutObjectResponseHeaders, S3ErrorBody,
};
