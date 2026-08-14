//! Property tests for the S3 adapter's pure logic: `group_page` paging,
//! bucket encode/decode, and range parsing.
//!
//! These exercise invariants over arbitrarily generated inputs (keys, prefixes,
//! delimiters, buckets, ranges) rather than a fixed set of hand-picked cases.

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
    clippy::let_underscore_must_use,
    clippy::string_add
)]

use proptest::prelude::*;
use shardline_index::S3ObjectEntry;
use shardline_s3_adapter::{decode_bucket, encode_bucket, group_page, parse_s3_range};

/// Builds a deterministic index row for a key.
fn entry(key: &str) -> S3ObjectEntry {
    S3ObjectEntry {
        scope_namespace: "global".to_owned(),
        object_key: key.to_owned(),
        file_id: format!("f:{key}"),
        size_bytes: u64::try_from(key.len()).unwrap(),
        content_hash: format!("hash:{key}"),
        etag: format!("etag:{key}"),
        user_metadata: Vec::new(),
        updated_at_unix_seconds: 1_785_110_400,
    }
}

/// A generated key: 1..=5 path segments joined by `/`.
fn arbitrary_key() -> impl Strategy<Value = String> {
    prop::collection::vec("[a-z0-9/]{1,12}", 1..6).prop_map(|parts| parts.join("/"))
}

proptest! {
    /// `group_page` walks are lossless and cursor-monotonic: every input key is
    /// either emitted as a `Contents` row or collapsed under exactly one
    /// `CommonPrefixes` rollup, contents keys are never duplicated or
    /// invented, and each page's cursor strictly advances.
    #[test]
    fn group_page_is_lossless_across_pages(
        keys in prop::collection::vec(arbitrary_key(), 1..40),
        prefix in prop::collection::vec("[a-z0-9/]{1,12}", 0..4).prop_map(|parts| parts.join("/")),
        delimiter in proptest::option::of(proptest::char::range('/', '/')),
        max_keys in 1_usize..8,
    ) {
        // Mirror the index contract: keys are sorted, deduplicated, and every
        // key starts with the requested prefix.
        let mut keys: Vec<String> = keys
            .into_iter()
            .filter(|key| key.starts_with(&prefix))
            .collect();
        keys.sort_unstable();
        keys.dedup();
        let full: Vec<String> = keys.clone();
        let all: Vec<S3ObjectEntry> = keys.into_iter().map(|key| entry(&key)).collect();

        // Walk the full set with cursor-based pages (exactly what the handler
        // does against the index).
        let mut contents: Vec<String> = Vec::new();
        let mut prefixes: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        let mut previous_cursor: Option<String> = None;
        let mut page_count = 0_u32;
        loop {
            let batch_start = match &cursor {
                Some(current) => all
                    .iter()
                    .position(|entry| entry.object_key > *current)
                    .unwrap_or(all.len()),
                None => 0,
            };
            let batch_end = batch_start
                .saturating_add(max_keys)
                .saturating_add(1)
                .min(all.len());
            let batch = all[batch_start..batch_end].to_vec();
            if batch.is_empty() {
                break;
            }
            let page = group_page(batch, &prefix, delimiter, max_keys);
            contents.extend(page.contents.iter().map(|row| row.key.clone()));
            prefixes.extend(page.common_prefixes.iter().cloned());
            match &page.next_cursor {
                Some(next) => {
                    // The cursor strictly advances across pages (monotonic).
                    prop_assert!(previous_cursor.as_ref().is_none_or(|prev| next > prev));
                    previous_cursor = Some(next.clone());
                    cursor = Some(next.clone());
                }
                None => break,
            }
            page_count = page_count.saturating_add(1);
            prop_assert!(page_count < 64, "paging must terminate");
        }

        // Every contents key came from the input and is emitted exactly once.
        let mut seen = std::collections::BTreeSet::new();
        for key in &contents {
            prop_assert!(full.contains(key), "contents key {key:?} not in input");
            prop_assert!(
                seen.insert(key.clone()),
                "duplicate contents key {key:?}"
            );
        }

        // Every input key is either a Contents row or under a CommonPrefix.
        for key in &full {
            let in_contents = seen.contains(key);
            let under_prefix = prefixes.iter().any(|rollup| key.starts_with(rollup));
            prop_assert!(
                in_contents || under_prefix,
                "key {key:?} is neither a contents row nor under a common prefix"
            );
        }

        // Every emitted common prefix is delimiter-terminated and has at least
        // one member in the input.
        for rollup in &prefixes {
            prop_assert!(
                rollup.ends_with('/'),
                "common prefix {rollup:?} must end with the delimiter"
            );
            prop_assert!(
                full.iter().any(|key| key.starts_with(rollup)),
                "common prefix {rollup:?} has no member in the input"
            );
        }
    }

    /// `encode_bucket`/`decode_bucket` roundtrip for arbitrary lowercase
    /// owners without dots (the string form `{owner}.{name}` decodes back to
    /// the exact pair).
    #[test]
    fn encode_decode_bucket_roundtrip(
        owner in "[a-z0-9]{1,10}",
        name in "[a-z0-9.-]{1,10}",
    ) {
        let encoded = encode_bucket(&owner, &name);
        let (decoded_owner, decoded_name) = decode_bucket(&encoded).unwrap();
        prop_assert_eq!(decoded_owner, owner);
        prop_assert_eq!(decoded_name, name);
    }

    /// A dotted owner is never silently round-tripped as a pair: when the
    /// embedded owner contains a `.`, decoding splits on the first dot so the
    /// decoded pair cannot equal the input (the string form still roundtrips,
    /// but the owner/name split is by construction different).
    #[test]
    fn dotted_owner_never_roundtrips_as_a_pair(
        owner in "[a-z0-9.]+",
        name in "[a-z0-9]{1,10}",
    ) {
        prop_assume!(owner.contains('.'));
        let encoded = encode_bucket(&owner, &name);
        match decode_bucket(&encoded) {
            Ok((decoded_owner, decoded_name)) => {
                // First-dot split: the decoded owner is the first segment.
                prop_assert!(decoded_owner != owner);
                prop_assert!(!decoded_owner.contains('.'));
                // Re-encoding the decoded pair reproduces the bucket string
                // (split-on-first-dot is lossless on the string).
                prop_assert_eq!(encode_bucket(&decoded_owner, &decoded_name), encoded);
            }
            Err(_error) => {
                // Rejected outright (oversized, empty part, or uppercase).
            }
        }
    }

    /// `parse_s3_range` never panics: a well-formed header either yields a
    /// satisfiable range within the resource or an `InvalidRange` error.
    #[test]
    fn parse_s3_range_structured_headers(
        total in 0_u64..1_000_000_u64,
        start in 0_u64..2_000_000_u64,
        end in 0_u64..2_000_000_u64,
    ) {
        let header = format!("bytes={start}-{end}");
        match parse_s3_range(Some(&header), total) {
            Ok(range) => {
                prop_assert!(range.start() <= range.end_inclusive());
                prop_assert!(range.end_inclusive() < total);
                prop_assert_eq!(range.start(), start);
            }
            Err(error) => {
                prop_assert_eq!(error.code, "InvalidRange");
                prop_assert_eq!(error.status.as_u16(), 416);
            }
        }
    }

    /// Arbitrary garbage headers never panic and parse deterministically.
    #[test]
    fn parse_s3_range_garbage_is_deterministic(
        header in "[a-zA-Z0-9=,.-]{0,24}",
        total in 0_u64..10_000_u64,
    ) {
        let first = parse_s3_range(Some(&header), total);
        let second = parse_s3_range(Some(&header), total);
        match (&first, &second) {
            (Ok(left), Ok(right)) => prop_assert_eq!(left, right),
            (Err(left), Err(right)) => prop_assert_eq!(
                (left.code, left.status),
                (right.code, right.status)
            ),
            _ => prop_assert!(false, "nondeterministic range parse"),
        }
        if let Ok(range) = first {
            prop_assert!(range.end_inclusive() < total);
        }
    }
}
