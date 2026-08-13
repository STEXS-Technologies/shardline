#![deny(unsafe_code)]

//! S3-compatible protocol adapter for the Shardline server ecosystem.
//!
//! This crate provides the S3 frontend's protocol primitives: bucket
//! (`{owner}.{name}`) encoding and decoding, and the storage object-key
//! layout (`protocols/s3/{scope_namespace}/{key}`).
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
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod key;

pub use key::{BucketDecodeError, S3KeyError, decode_bucket, encode_bucket, s3_object_key};
