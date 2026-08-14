//! `ListObjectsV2` handler for the S3 frontend.
//!
//! Listing is served entirely from the `shardline_s3_objects` index
//! (`scan_s3_objects`, keyset on the raw client-facing key) — zero
//! object-store reads, because record-backed objects are not materialized at
//! the protocol key. The page walk fetches `max_keys + 1` rows (the extra row
//! detects truncation), groups them into `Contents` + `CommonPrefixes` with
//! the S3 delimiter paging behavior, and serializes the `ListBucketResult`
//! XML envelope.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use shardline_s3_adapter::{
    ListBucketResult, ListBucketResultV1, S3Error, encode_continuation_token, group_page,
    parse_list_objects_v1_params, parse_list_objects_v2_params,
};

use super::{S3Repository, parse_s3_query, s3_xml_content_type};
use crate::{app::AppState, protocol_support::scope_namespace};

/// `GET /{bucket}?list-type=2` — `ListObjectsV2`.
///
/// Reads the listing index page (`prefix`/`delimiter`/`max-keys`/
/// `continuation-token`/`start-after`), groups the rows, and emits the
/// `ListBucketResult` XML: `<Contents>` rows (Key / Size / quoted ETag /
/// ISO-8601 LastModified), `<CommonPrefixes><Prefix>` rollups,
/// `<IsTruncated>`, and `<NextContinuationToken>` when truncated.
#[tracing::instrument(skip(auth, state, _headers), fields(bucket))]
pub(crate) async fn s3_list_objects_v2(
    auth: S3Repository,
    State(state): State<Arc<AppState>>,
    Path(_bucket): Path<String>,
    uri: Uri,
    _headers: HeaderMap,
) -> Result<Response, S3Error> {
    let query = parse_s3_query(&uri)?;
    let params = parse_list_objects_v2_params(&query)?;
    let scope_namespace = scope_namespace(auth.capability().namespace());

    // Fetch one extra row to detect truncation.
    let fetch_limit = params
        .max_keys
        .checked_add(1)
        .ok_or_else(S3Error::internal)?;
    let entries = state
        .backend
        .scan_s3_objects(
            &scope_namespace,
            &params.prefix,
            params.cursor(),
            fetch_limit,
        )
        .await?;
    let page = group_page(
        entries,
        &params.prefix,
        params.delimiter.map(shardline_s3_adapter::Delimiter::get),
        params.max_keys,
    );

    let next_continuation_token = if page.is_truncated {
        page.next_cursor.as_deref().map(encode_continuation_token)
    } else {
        None
    };
    let result = ListBucketResult {
        contents: page.contents,
        common_prefixes: page.common_prefixes,
        is_truncated: page.is_truncated,
        next_continuation_token,
    };
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
        result.to_xml(),
    )
        .into_response())
}

/// `GET /{bucket}` — `ListObjects` (v1), the S3 default for a bare bucket GET
/// that `s3cmd` and other legacy clients send for `ls`.
///
/// Identical index-backed paging to [`s3_list_objects_v2`] but driven by the
/// v1 `marker` cursor and serialized into the v1 `ListBucketResult` envelope
/// (`Name`/`Prefix`/`Marker`/`MaxKeys`/`Delimiter`/`IsTruncated`/`NextMarker`).
#[tracing::instrument(skip(auth, state, _headers), fields(bucket))]
pub(crate) async fn s3_list_objects_v1(
    auth: S3Repository,
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    uri: Uri,
    _headers: HeaderMap,
) -> Result<Response, S3Error> {
    let query = parse_s3_query(&uri)?;
    let params = parse_list_objects_v1_params(&query)?;
    let scope_namespace = scope_namespace(auth.capability().namespace());

    // Fetch one extra row to detect truncation.
    let fetch_limit = params
        .max_keys
        .checked_add(1)
        .ok_or_else(S3Error::internal)?;
    let entries = state
        .backend
        .scan_s3_objects(
            &scope_namespace,
            &params.prefix,
            params.marker.as_deref(),
            fetch_limit,
        )
        .await?;
    let page = group_page(
        entries,
        &params.prefix,
        params.delimiter.map(shardline_s3_adapter::Delimiter::get),
        params.max_keys,
    );

    let result = ListBucketResultV1 {
        contents: page.contents,
        common_prefixes: page.common_prefixes,
        name: bucket,
        prefix: params.prefix,
        marker: params.marker.unwrap_or_default(),
        max_keys: params.max_keys,
        delimiter: params
            .delimiter
            .map(|delimiter| delimiter.get().to_string()),
        is_truncated: page.is_truncated,
        next_marker: if page.is_truncated {
            page.next_cursor
        } else {
            None
        },
    };
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
        result.to_xml(),
    )
        .into_response())
}
