use std::{hint::black_box, process::abort};

use shardline_storage::{LocalObjectStore, ObjectKey, ObjectMetadata, ObjectPrefix, ObjectStore};

pub(crate) fn old_full_scan_page(
    store: &LocalObjectStore,
    prefix: &ObjectPrefix,
    page_size: usize,
    last: Option<&str>,
) -> Vec<String> {
    let mut tags = std::collections::BTreeSet::new();
    let mut has_more = false;
    if let Err(error) = store.visit_prefix(prefix, |object: ObjectMetadata| {
        let Some(tag) = object.key().as_str().rsplit('/').next() else {
            return Ok::<(), shardline_storage::LocalObjectStoreError>(());
        };
        if last.is_some_and(|last| tag <= last) {
            return Ok(());
        }
        let _inserted = tags.insert(tag.to_owned());
        if tags.len() > page_size {
            let _removed = tags.pop_last();
            has_more = true;
        }
        Ok(())
    }) {
        abort_with_error("old full-scan tag page benchmark failed", &error);
    }
    let tags = tags.into_iter().collect::<Vec<_>>();
    black_box(has_more);
    tags
}

pub(crate) fn new_flat_namespace_page(
    store: &LocalObjectStore,
    prefix: &ObjectPrefix,
    page_size: usize,
    last: Option<&str>,
) -> Vec<String> {
    let start_after = last.map(|last| {
        must(
            ObjectKey::parse(&format!("{}{}", prefix.as_str(), last)),
            "benchmark start-after key should parse",
        )
    });
    let page = must(
        store.list_flat_namespace_page(prefix, start_after.as_ref(), page_size.saturating_add(1)),
        "new flat-namespace tag page benchmark failed",
    );
    let mut tags = page
        .into_iter()
        .filter_map(|object| {
            object
                .key()
                .as_str()
                .rsplit('/')
                .next()
                .map(ToOwned::to_owned)
        })
        .collect::<Vec<_>>();
    let has_more = tags.len() > page_size;
    tags.truncate(page_size);
    black_box(has_more);
    tags
}

pub(crate) fn must<T, E: std::fmt::Display>(result: Result<T, E>, context: &str) -> T {
    match result {
        Ok(value) => value,
        Err(error) => abort_with_error(context, &error),
    }
}

fn abort_with_error(context: &str, error: &dyn std::fmt::Display) -> ! {
    eprintln!("{context}: {error}");
    abort();
}
