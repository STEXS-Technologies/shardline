//! Native S3 listing reads evidence-verified metadata pages, never object storage.
//! Prefix/cursor bounds are indexed. Delimiter walks seek past whole groups and
//! collect max_keys + 1 distinct logical entries for truncation, decoding at most
//! 64 raw rows per scan. Dense directory pages can require additional keyset scans.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use shardline_s3_adapter::{
    ListBucketResult, ListBucketResultV1, ListPage, S3Error, encode_continuation_token, group_page,
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

    let page = logical_listing_page(
        &state,
        &scope_namespace,
        &params.prefix,
        params.delimiter.map(shardline_s3_adapter::Delimiter::get),
        params.cursor(),
        params.max_keys,
    )
    .await?;

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

    let page = logical_listing_page(
        &state,
        &scope_namespace,
        &params.prefix,
        params.delimiter.map(shardline_s3_adapter::Delimiter::get),
        params.marker.as_deref(),
        params.max_keys,
    )
    .await?;

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

/// Builds one page over Contents and CommonPrefixes rather than over child keys.
async fn logical_listing_page(
    state: &AppState,
    scope: &str,
    prefix: &str,
    delimiter: Option<char>,
    cursor: Option<&str>,
    max_keys: usize,
) -> Result<ListPage, S3Error> {
    use shardline_index::{S3ObjectScanStart, s3_prefix_successor};
    // A zero result budget returns a terminal empty page before querying
    // metadata; a lookahead would imply truncation without making progress.
    if max_keys == 0 {
        return Ok(group_page(Vec::new(), prefix, delimiter, 0));
    }
    let fetch_limit = max_keys.checked_add(1).ok_or_else(S3Error::internal)?;
    let Some(delimiter) = delimiter else {
        let entries = state
            .backend
            .scan_s3_objects(scope, prefix, cursor, fetch_limit)
            .await?;
        return Ok(group_page(entries, prefix, None, max_keys));
    };
    let mut start = cursor.map(|key| (key.to_owned(), false));
    if let Some(key) = cursor
        && let Some(relative) = key.strip_prefix(prefix)
        && let Some((head, _)) = relative.split_once(delimiter)
    {
        let group = format!("{prefix}{head}{delimiter}");
        let Some(successor) = s3_prefix_successor(&group) else {
            return Ok(group_page(Vec::new(), prefix, Some(delimiter), max_keys));
        };
        start = Some((successor, true));
    }
    let mut entries = Vec::new();
    let mut next_cursor = None;
    loop {
        let remaining = fetch_limit.saturating_sub(entries.len());
        if remaining == 0 {
            break;
        }
        let scan_start = start.as_ref().map(|(key, inclusive)| {
            if *inclusive {
                S3ObjectScanStart::Inclusive(key)
            } else {
                S3ObjectScanStart::Exclusive(key)
            }
        });
        let batch = state
            .backend
            .scan_s3_objects_from(scope, prefix, scan_start, remaining.min(64))
            .await?;
        if batch.is_empty() {
            break;
        }
        let mut exhausted = false;
        for entry in batch {
            let key = &entry.object_key;
            if start.as_ref().is_some_and(|(bound, inclusive)| {
                if *inclusive {
                    key < bound
                } else {
                    key <= bound
                }
            }) {
                continue;
            }
            let group = key
                .strip_prefix(prefix)
                .and_then(|relative| relative.split_once(delimiter))
                .map(|(head, _)| format!("{prefix}{head}{delimiter}"));
            let logical_key = group.as_deref().unwrap_or(key);
            if cursor.is_none_or(|cursor| logical_key > cursor) {
                if entries.len() < max_keys {
                    next_cursor = Some(logical_key.to_owned());
                }
                entries.push(entry.clone());
            }
            if let Some(group) = group {
                if let Some(successor) = s3_prefix_successor(&group) {
                    start = Some((successor, true));
                } else {
                    exhausted = true;
                }
            } else {
                start = Some((entry.object_key, false));
            }
            if exhausted || entries.len() >= fetch_limit {
                break;
            }
        }
        if exhausted {
            break;
        }
    }
    let mut page = group_page(entries, prefix, Some(delimiter), max_keys);
    page.next_cursor = next_cursor;
    Ok(page)
}
