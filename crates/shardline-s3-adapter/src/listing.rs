//! Pure `ListObjectsV2` listing logic for the S3 frontend.
//!
//! Listing is served from the Lane-0 `shardline_s3_objects` index (a
//! `(scope_namespace, object_key)` snapshot keyed on the raw client-facing key)
//! — record-backed objects are not materialized at the protocol key, so the
//! index is the only enumeration source and no object-store reads happen.
//!
//! The page walk is a keyset scan: fetch `max_keys + 1` rows ordered by raw
//! `object_key` (the extra row detects truncation), then group them into
//! `Contents` rows and `CommonPrefixes` rollups with S3 paging behavior — a
//! delimiter group consumes every key under the common prefix and the page
//! cursor advances past the whole group.

use crate::{S3Error, types::Contents};

/// The S3 `ListObjectsV2` maximum page size.
pub const MAX_LIST_KEYS: usize = 1000;

/// A typed `ListObjectsV2` query-parameter name.
///
/// [`ListParam::parse`] owns the literal name table; the params parser matches
/// the enum only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListParam {
    /// `prefix`.
    Prefix,
    /// `delimiter`.
    Delimiter,
    /// `max-keys`.
    MaxKeys,
    /// `continuation-token`.
    ContinuationToken,
    /// `start-after`.
    StartAfter,
    /// `marker` (ListObjects v1).
    Marker,
}

impl ListParam {
    /// Parses a raw query-parameter name into the typed set. Unknown names
    /// (for example `fetch-owner` or `encoding-type`, which real S3 clients
    /// send) return `None` and are ignored.
    fn parse(name: &str) -> Option<Self> {
        match name {
            "prefix" => Some(Self::Prefix),
            "delimiter" => Some(Self::Delimiter),
            "max-keys" => Some(Self::MaxKeys),
            "continuation-token" => Some(Self::ContinuationToken),
            "start-after" => Some(Self::StartAfter),
            "marker" => Some(Self::Marker),
            _ => None,
        }
    }
}

/// A validated `ListObjectsV2` grouping delimiter (exactly one character).
///
/// [`Delimiter::parse`] rejects empty (treated as "no delimiter") and
/// multi-character values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delimiter(char);

impl Delimiter {
    /// Parses a delimiter value.
    ///
    /// # Errors
    ///
    /// Returns [`S3Error::invalid_argument`] when the value is more than one
    /// character.
    pub fn parse(value: &str) -> Result<Option<Self>, S3Error> {
        let mut chars = value.chars();
        let Some(first) = chars.next() else {
            return Ok(None);
        };
        if chars.next().is_some() {
            return Err(S3Error::invalid_argument(
                "delimiter must be a single character",
            ));
        }
        Ok(Some(Self(first)))
    }

    /// Returns the delimiter character.
    #[must_use]
    pub const fn get(self) -> char {
        self.0
    }
}

/// Parsed `ListObjectsV2` request parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectsV2Params {
    /// The raw-key prefix filter (empty lists every key).
    pub prefix: String,
    /// The grouping delimiter (for example `/`); `None` disables grouping.
    pub delimiter: Option<Delimiter>,
    /// The page row budget (`Contents` rows + `CommonPrefixes`), capped at
    /// [`MAX_LIST_KEYS`].
    pub max_keys: usize,
    /// The decoded raw key from `continuation-token` (the last key seen on the
    /// previous page).
    pub continuation_token: Option<String>,
    /// The raw key to start listing strictly after (`start-after`).
    pub start_after: Option<String>,
}

impl ListObjectsV2Params {
    /// The effective resume cursor for the keyset scan: `start-after` wins over
    /// the continuation token.
    #[must_use]
    pub fn cursor(&self) -> Option<&str> {
        self.start_after
            .as_deref()
            .or(self.continuation_token.as_deref())
    }
}

/// Parsed `ListObjects` (v1) request parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObjectsV1Params {
    /// The raw-key prefix filter (empty lists every key).
    pub prefix: String,
    /// The grouping delimiter (for example `/`); `None` disables grouping.
    pub delimiter: Option<Delimiter>,
    /// The page row budget (`Contents` rows + `CommonPrefixes`), capped at
    /// [`MAX_LIST_KEYS`].
    pub max_keys: usize,
    /// The raw key to start listing strictly after (v1 `marker`).
    pub marker: Option<String>,
}

/// Parses the `ListObjects` (v1) query parameters from a decoded query map.
///
/// v1 lists — which `s3cmd` and other legacy clients send for `ls` — carry
/// `marker` instead of `continuation-token`/`start-after` and no
/// `list-type=2`.
///
/// # Errors
///
/// Returns [`S3Error::invalid_argument`] when `max-keys` is not a valid
/// positive integer or the `delimiter` is more than one character.
pub fn parse_list_objects_v1_params(
    query: &[(String, String)],
) -> Result<ListObjectsV1Params, S3Error> {
    let mut params = ListObjectsV1Params {
        prefix: String::new(),
        delimiter: None,
        max_keys: MAX_LIST_KEYS,
        marker: None,
    };
    for (name, value) in query {
        match ListParam::parse(name) {
            Some(ListParam::Prefix) => params.prefix = value.clone(),
            Some(ListParam::Delimiter) => params.delimiter = Delimiter::parse(value)?,
            Some(ListParam::MaxKeys) => {
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_error| S3Error::invalid_argument("invalid max-keys"))?;
                if parsed == 0 {
                    return Err(S3Error::invalid_argument(
                        "max-keys must be greater than zero",
                    ));
                }
                params.max_keys = parsed.min(MAX_LIST_KEYS);
            }
            Some(ListParam::Marker) => params.marker = Some(value.clone()),
            Some(ListParam::ContinuationToken) => {} // v2-only; ignored by v1
            Some(ListParam::StartAfter) => {}        // v2-only; ignored by v1
            None => {}
        }
    }
    Ok(params)
}

/// One grouped page of a `ListObjectsV2` listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPage {
    /// The `Contents` rows (objects), in raw-key order.
    pub contents: Vec<Contents>,
    /// The `CommonPrefixes` rollups, in raw-key order.
    pub common_prefixes: Vec<String>,
    /// The raw key the page stopped after; the next page resumes strictly past
    /// it. `None` when the page examined no keys.
    pub next_cursor: Option<String>,
    /// Whether more keys exist beyond this page.
    pub is_truncated: bool,
}

/// Parses the `ListObjectsV2` query parameters from a decoded query map.
///
/// Only the typed listing parameters (`prefix`, `delimiter`, `max-keys`,
/// `continuation-token`, `start-after`) are read; everything else (including
/// the `list-type=2` sub-resource and client extras such as `fetch-owner`) is
/// ignored. `max-keys` must be a positive integer and is capped at
/// [`MAX_LIST_KEYS`].
///
/// # Errors
///
/// Returns [`S3Error::invalid_argument`] when `max-keys` is missing/zero/non-
/// numeric, the delimiter is more than one character, or
/// `continuation-token` is not a valid base64 cursor.
pub fn parse_list_objects_v2_params(
    query: &[(String, String)],
) -> Result<ListObjectsV2Params, S3Error> {
    let mut params = ListObjectsV2Params {
        prefix: String::new(),
        delimiter: None,
        max_keys: MAX_LIST_KEYS,
        continuation_token: None,
        start_after: None,
    };
    for (name, value) in query {
        match ListParam::parse(name) {
            Some(ListParam::Prefix) => params.prefix = value.clone(),
            Some(ListParam::Delimiter) => params.delimiter = Delimiter::parse(value)?,
            Some(ListParam::MaxKeys) => {
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_error| S3Error::invalid_argument("invalid max-keys"))?;
                if parsed == 0 {
                    return Err(S3Error::invalid_argument(
                        "max-keys must be greater than zero",
                    ));
                }
                params.max_keys = parsed.min(MAX_LIST_KEYS);
            }
            Some(ListParam::ContinuationToken) => {
                params.continuation_token = Some(decode_continuation_token(value)?);
            }
            Some(ListParam::StartAfter) => params.start_after = Some(value.clone()),
            Some(ListParam::Marker) => {} // v1-only; ignored by ListObjectsV2
            None => {}
        }
    }
    Ok(params)
}

/// Groups a sorted, prefix-filtered batch of index rows into one page.
///
/// The `entries` batch is ordered by raw `object_key` and every key starts
/// with `prefix` (guaranteed by the index scan). With a `delimiter`, the first
/// delimiter occurrence in the remainder after the prefix splits the key into
/// a `CommonPrefixes` rollup, and every key under that rollup is consumed and
/// skipped by the page cursor (S3 paging behavior). A key exactly equal to the
/// prefix is a `Contents` row (it is an object, not a directory); a delimiter
/// appearing exactly at the prefix boundary groups the remainder (`prefix` +
/// `dir/file` with delimiter `/` → `CommonPrefixes` `dir/`).
///
/// At most `max_keys` rows are produced. The caller fetches `max_keys + 1`
/// rows, so the page is reported truncated exactly when the raw batch exceeded
/// the row budget — the extra row detects that more keys exist. When a
/// delimiter group spans a page boundary the rollup may be re-emitted on the
/// following page (benign; clients merge).
#[must_use]
pub fn group_page(
    entries: Vec<shardline_index::S3ObjectEntry>,
    prefix: &str,
    delimiter: Option<char>,
    max_keys: usize,
) -> ListPage {
    let fetched_len = entries.len();
    let mut contents = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut next_cursor = None;
    let mut rows = 0_usize;
    let mut remaining = entries.into_iter().peekable();
    while let Some(entry) = remaining.next() {
        if rows >= max_keys {
            break;
        }
        let key = entry.object_key.clone();
        // The index guarantees every key starts with the prefix; keys that do
        // not (defensive) are surfaced as ordinary Contents rows.
        let Some(remainder) = key.strip_prefix(prefix) else {
            contents.push(contents_row(&entry, &key));
            next_cursor = Some(key);
            rows = rows.saturating_add(1);
            continue;
        };
        if let Some(delimiter) = delimiter
            && let Some((head, _tail)) = remainder.split_once(delimiter)
        {
            // CommonPrefixes rollup: `prefix` + the leading segment up to and
            // including the delimiter. Consume every key under it.
            let group_prefix = format!("{prefix}{head}{delimiter}");
            let mut group_last = key;
            while let Some(peeked) = remaining.peek() {
                if peeked.object_key.starts_with(&group_prefix) {
                    group_last = peeked.object_key.clone();
                    remaining.next();
                } else {
                    break;
                }
            }
            common_prefixes.push(group_prefix);
            next_cursor = Some(group_last);
            rows = rows.saturating_add(1);
        } else {
            contents.push(contents_row(&entry, &key));
            next_cursor = Some(key);
            rows = rows.saturating_add(1);
        }
    }
    ListPage {
        contents,
        common_prefixes,
        next_cursor,
        is_truncated: fetched_len > max_keys,
    }
}

fn contents_row(entry: &shardline_index::S3ObjectEntry, key: &str) -> Contents {
    Contents {
        key: key.to_owned(),
        size_bytes: entry.size_bytes,
        etag: entry.etag.clone(),
        last_modified_iso8601: format_iso8601(entry.updated_at_unix_seconds),
    }
}

/// Encodes a raw cursor key as an opaque base64 continuation token.
#[must_use]
pub fn encode_continuation_token(cursor: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(cursor.as_bytes())
}

/// Decodes an opaque base64 continuation token back to the raw cursor key.
///
/// # Errors
///
/// Returns [`S3Error::invalid_argument`] when the token is not valid base64 or
/// does not decode to a UTF-8 string.
pub fn decode_continuation_token(token: &str) -> Result<String, S3Error> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(token)
        .map_err(|_error| S3Error::invalid_argument("invalid continuation-token"))?;
    String::from_utf8(bytes)
        .map_err(|_error| S3Error::invalid_argument("invalid continuation-token"))
}

/// Formats unix seconds as an S3 `LastModified` ISO-8601 UTC string
/// (`2026-08-13T09:51:00Z`).
///
/// Falls back to the Unix epoch when the timestamp is out of the representable
/// `chrono` range.
#[must_use]
pub fn format_iso8601(unix_seconds: i64) -> String {
    chrono::DateTime::from_timestamp(unix_seconds, 0)
        .map(|date_time| date_time.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_owned())
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::arithmetic_side_effects,
        clippy::option_if_let_else,
        clippy::unreachable,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use
    )]

    use shardline_index::S3ObjectEntry;

    use super::*;

    fn entry(key: &str) -> S3ObjectEntry {
        S3ObjectEntry {
            scope_namespace: "global".to_owned(),
            object_key: key.to_owned(),
            file_id: format!("f:{key}"),
            size_bytes: key.len() as u64,
            content_hash: format!("hash:{key}"),
            etag: format!("etag:{key}"),
            user_metadata: Vec::new(),
            updated_at_unix_seconds: 1_785_110_400,
        }
    }

    fn keys(page: &ListPage) -> (Vec<String>, Vec<String>) {
        let contents: Vec<String> = page.contents.iter().map(|c| c.key.clone()).collect();
        (contents, page.common_prefixes.clone())
    }

    fn query(entries: &[(&str, &str)]) -> Vec<(String, String)> {
        entries
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    // ── parse_list_objects_v2_params ────────────────────────────────────────

    #[test]
    fn parse_params_defaults() {
        let params = parse_list_objects_v2_params(&query(&[])).unwrap();
        assert_eq!(params.prefix, "");
        assert_eq!(params.delimiter, None);
        assert_eq!(params.max_keys, MAX_LIST_KEYS);
        assert_eq!(params.continuation_token, None);
        assert_eq!(params.start_after, None);
        assert_eq!(params.cursor(), None);
    }

    #[test]
    fn parse_params_extracts_and_caps() {
        let params = parse_list_objects_v2_params(&query(&[
            ("prefix", "data/"),
            ("delimiter", "/"),
            ("max-keys", "5000"),
            ("start-after", "data/z.pt"),
        ]))
        .unwrap();
        assert_eq!(params.prefix, "data/");
        assert_eq!(params.delimiter, Some(Delimiter('/')));
        assert_eq!(params.max_keys, MAX_LIST_KEYS, "max-keys is capped at 1000");
        assert_eq!(params.start_after.as_deref(), Some("data/z.pt"));
        assert_eq!(params.cursor(), Some("data/z.pt"));
    }

    #[test]
    fn parse_params_max_keys_below_cap_is_kept() {
        let params = parse_list_objects_v2_params(&query(&[("max-keys", "7")])).unwrap();
        assert_eq!(params.max_keys, 7);
    }

    #[test]
    fn parse_params_rejects_bad_max_keys() {
        assert!(parse_list_objects_v2_params(&query(&[("max-keys", "many")])).is_err());
        assert!(parse_list_objects_v2_params(&query(&[("max-keys", "-1")])).is_err());
    }

    #[test]
    fn parse_params_decodes_continuation_token() {
        let token = encode_continuation_token("data/2.txt");
        let params =
            parse_list_objects_v2_params(&query(&[("continuation-token", &token)])).unwrap();
        assert_eq!(params.continuation_token.as_deref(), Some("data/2.txt"));
        assert_eq!(params.cursor(), Some("data/2.txt"));
        // start-after wins over the continuation token.
        let params = parse_list_objects_v2_params(&query(&[
            ("continuation-token", &token),
            ("start-after", "zz.txt"),
        ]))
        .unwrap();
        assert_eq!(params.cursor(), Some("zz.txt"));
    }

    #[test]
    fn parse_params_rejects_malformed_continuation_token() {
        assert!(
            parse_list_objects_v2_params(&query(&[("continuation-token", "not-base64!!")]))
                .is_err()
        );
    }

    #[test]
    fn parse_params_empty_delimiter_is_none() {
        let params = parse_list_objects_v2_params(&query(&[("delimiter", "")])).unwrap();
        assert_eq!(params.delimiter, None);
    }

    // ── token roundtrip ─────────────────────────────────────────────────────

    #[test]
    fn continuation_token_roundtrip() {
        for cursor in ["", "a", "data/2.txt", "spaces and ünïcode/keys"] {
            let token = encode_continuation_token(cursor);
            assert_eq!(decode_continuation_token(&token).unwrap(), cursor);
        }
    }

    #[test]
    fn decode_continuation_token_rejects_garbage() {
        assert!(decode_continuation_token("!!!").is_err());
        assert!(
            decode_continuation_token("").is_ok(),
            "empty decodes to empty"
        );
        assert_eq!(decode_continuation_token("").unwrap(), "");
    }

    // ── group_page ──────────────────────────────────────────────────────────

    #[test]
    fn group_page_empty_batch() {
        let page = group_page(Vec::new(), "", None, MAX_LIST_KEYS);
        assert!(page.contents.is_empty());
        assert!(page.common_prefixes.is_empty());
        assert_eq!(page.next_cursor, None);
        assert!(!page.is_truncated);
    }

    #[test]
    fn group_page_single_key() {
        let page = group_page(vec![entry("a.txt")], "", None, MAX_LIST_KEYS);
        let (contents, prefixes) = keys(&page);
        assert_eq!(contents, vec!["a.txt"]);
        assert!(prefixes.is_empty());
        assert_eq!(page.next_cursor.as_deref(), Some("a.txt"));
        assert!(!page.is_truncated);
        let row = &page.contents[0];
        assert_eq!(row.size_bytes, "a.txt".len() as u64);
        assert_eq!(row.etag, "etag:a.txt");
        assert_eq!(row.last_modified_iso8601, "2026-07-27T00:00:00Z");
    }

    #[test]
    fn group_page_prefix_filter() {
        // The index scan only returns keys starting with the prefix; S3 reports
        // the FULL key in Contents rows.
        let entries = vec![entry("data/1.txt"), entry("data/sub/2.txt")];
        let page = group_page(entries, "data", None, MAX_LIST_KEYS);
        let (contents, _prefixes) = keys(&page);
        assert_eq!(contents, vec!["data/1.txt", "data/sub/2.txt"]);
    }

    #[test]
    fn group_page_delimiter_groups_nested_keys() {
        let entries = vec![
            entry("a/b/c"),
            entry("a/b/d"),
            entry("a/x"),
            entry("b"),
            entry("c.txt"),
        ];
        let page = group_page(entries, "", Some('/'), MAX_LIST_KEYS);
        let (contents, prefixes) = keys(&page);
        assert_eq!(contents, vec!["b", "c.txt"]);
        assert_eq!(prefixes, vec!["a/"]);
        assert_eq!(page.next_cursor.as_deref(), Some("c.txt"));
        assert!(!page.is_truncated);
    }

    #[test]
    fn group_page_cursor_advances_past_group() {
        // A group consumes every key under the rollup and the cursor lands past
        // the last examined member, so the next page never re-sees the group.
        let mut entries: Vec<S3ObjectEntry> =
            (1..=50).map(|n| entry(&format!("dir/file{n}"))).collect();
        entries.push(entry("zz.txt"));
        let page = group_page(entries, "", Some('/'), 2);
        let (contents, prefixes) = keys(&page);
        assert_eq!(contents, vec!["zz.txt"]);
        assert_eq!(prefixes, vec!["dir/"]);
        assert_eq!(page.next_cursor.as_deref(), Some("zz.txt"));
        assert!(
            page.is_truncated,
            "the extra fetched entry beyond the 50-key group signals more keys"
        );
    }

    #[test]
    fn group_page_max_keys_truncation() {
        let entries = vec![entry("a.txt"), entry("b.txt"), entry("c.txt")];
        let page = group_page(entries, "", None, 2);
        let (contents, _prefixes) = keys(&page);
        assert_eq!(contents, vec!["a.txt", "b.txt"]);
        assert_eq!(page.next_cursor.as_deref(), Some("b.txt"));
        assert!(page.is_truncated);
    }

    #[test]
    fn group_page_not_truncated_when_batch_fits() {
        let entries = vec![entry("a.txt"), entry("b.txt")];
        let page = group_page(entries, "", None, 2);
        let (contents, _prefixes) = keys(&page);
        assert_eq!(contents, vec!["a.txt", "b.txt"]);
        assert!(!page.is_truncated);
    }

    #[test]
    fn group_page_key_exactly_equal_to_prefix_is_contents() {
        // A key exactly equal to the prefix is an object, not a directory.
        // The sibling "dir/" collapses into the "dir/" CommonPrefixes rollup.
        let entries = vec![entry("dir"), entry("dir/x")];
        let page = group_page(entries, "dir", Some('/'), MAX_LIST_KEYS);
        let (contents, prefixes) = keys(&page);
        assert_eq!(contents, vec!["dir"]);
        assert_eq!(prefixes, vec!["dir/"]);
    }

    #[test]
    fn group_page_delimiter_at_prefix_boundary() {
        // prefix "dir" + key "dir/file" → the delimiter sits at the boundary;
        // the remainder groups into CommonPrefixes "dir/".
        let entries = vec![entry("dir/file"), entry("dir/sub/x"), entry("dirx/y")];
        let page = group_page(entries, "dir", Some('/'), MAX_LIST_KEYS);
        let (contents, prefixes) = keys(&page);
        assert!(contents.is_empty());
        assert_eq!(prefixes, vec!["dir/", "dirx/"]);
    }

    #[test]
    fn group_page_key_exactly_the_prefix_with_delimiter() {
        // prefix "dir/" + a key that IS the prefix → Contents row; a sibling
        // "dir/a.txt" has no delimiter in its remainder after the prefix →
        // also a Contents row (the prefix consumed the directory part).
        let entries = vec![entry("dir/"), entry("dir/a.txt")];
        let page = group_page(entries, "dir/", Some('/'), MAX_LIST_KEYS);
        let (contents, prefixes) = keys(&page);
        assert_eq!(contents, vec!["dir/", "dir/a.txt"]);
        assert!(prefixes.is_empty());
    }

    #[test]
    fn group_page_nested_set() {
        let entries = vec![
            entry("a.txt"),
            entry("data/2026/01.csv"),
            entry("data/2026/02.csv"),
            entry("data/2027/01.csv"),
            entry("data/manifest.json"),
        ];
        let page = group_page(entries, "", Some('/'), MAX_LIST_KEYS);
        let (contents, prefixes) = keys(&page);
        assert_eq!(contents, vec!["a.txt"]);
        assert_eq!(prefixes, vec!["data/"]);
    }

    #[test]
    fn group_page_flat_set_no_delimiter() {
        let entries = vec![entry("a.txt"), entry("b.txt"), entry("c.txt")];
        let page = group_page(entries, "", None, MAX_LIST_KEYS);
        let (contents, prefixes) = keys(&page);
        assert_eq!(contents, vec!["a.txt", "b.txt", "c.txt"]);
        assert!(prefixes.is_empty());
        assert_eq!(page.next_cursor.as_deref(), Some("c.txt"));
    }

    #[test]
    fn format_iso8601_known_timestamp() {
        assert_eq!(format_iso8601(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_iso8601(1_785_110_400), "2026-07-27T00:00:00Z");
    }

    #[test]
    fn group_page_delimiter_inside_prefix_groups_remainder() {
        // When the prefix itself ends with a delimiter, grouping applies to
        // the remainder only; nested segments still collapse.
        let entries = vec![
            entry("a/b/c/d.txt"),
            entry("a/b/c/e.txt"),
            entry("a/b/f.txt"),
        ];
        let page = group_page(entries, "a/b/", Some('/'), MAX_LIST_KEYS);
        let (contents, prefixes) = keys(&page);
        // Deeper nesting collapses; a key with no delimiter in its remainder
        // is a top-level Contents row.
        assert_eq!(contents, vec!["a/b/f.txt"]);
        assert_eq!(prefixes, vec!["a/b/c/"]);
        assert_eq!(page.next_cursor.as_deref(), Some("a/b/f.txt"));
    }

    #[test]
    fn group_page_prefix_matching_nothing_is_empty() {
        // A prefix longer than any key (a superset) matches no rows.
        let page = group_page(Vec::new(), "dir/very/long", Some('/'), MAX_LIST_KEYS);
        assert!(page.contents.is_empty());
        assert!(page.common_prefixes.is_empty());
        assert_eq!(page.next_cursor, None);
        assert!(!page.is_truncated);
    }

    #[test]
    fn parse_params_rejects_zero_max_keys() {
        let result = parse_list_objects_v2_params(&query(&[("max-keys", "0")]));
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, "InvalidArgument");
        assert_eq!(error.status, 400);
    }

    #[test]
    fn parse_params_rejects_multi_char_delimiter() {
        for delimiter in ["//", "ab", "%%"] {
            let result = parse_list_objects_v2_params(&query(&[("delimiter", delimiter)]));
            assert!(result.is_err(), "delimiter {delimiter:?} must be rejected");
            let error = result.unwrap_err();
            assert_eq!(error.code, "InvalidArgument");
        }
    }

    #[test]
    fn parse_params_unknown_keys_are_ignored() {
        // Real clients send fetch-owner / encoding-type / list-type; those must
        // not break the listing params parse (list-type=2 is a sub-resource).
        let params = parse_list_objects_v2_params(&query(&[
            ("list-type", "2"),
            ("prefix", "dir/"),
            ("fetch-owner", "true"),
            ("encoding-type", "url"),
        ]))
        .unwrap();
        assert_eq!(params.prefix, "dir/");
        assert_eq!(params.max_keys, MAX_LIST_KEYS);
    }
}
