//! S3 object-data routes (`PUT`/`GET`/`HEAD`/`DELETE /{bucket}/{*key}`) and
//! bucket stubs (`/{bucket}`) for the S3 frontend (Lane 3).
//!
//! Routing is `/{bucket}/{*key}` where `{bucket}` is the single dotted segment
//! `{owner}.{name}` of the bearer token's `RepositoryScope` and `{*key}` is the
//! arbitrary-depth S3 object key. Every handler carries the [`S3Repository`]
//! axum extractor, which authenticates with the SigV4→bearer bridge
//! ([`authorize_s3`]), binds the bucket to the token claims, and mints the
//! typed [`AuthorizedRepository`] capability that storage entry points require;
//! handlers then dispatch on the query sub-resources.

pub(super) mod aws_chunked;
pub(super) mod bucket;
pub(super) mod listing;
pub(super) mod multipart;
pub(super) mod object;
mod repository;

pub(crate) use bucket::{
    s3_create_bucket, s3_delete_bucket, s3_get_bucket, s3_head_bucket, s3_list_buckets,
    s3_post_bucket,
};
pub(crate) use object::{
    s3_delete_object, s3_get_object, s3_head_object, s3_post_object, s3_put_object,
};
pub(super) use repository::*;

#[cfg(test)]
mod poc_audit;

#[cfg(test)]
mod tests;
