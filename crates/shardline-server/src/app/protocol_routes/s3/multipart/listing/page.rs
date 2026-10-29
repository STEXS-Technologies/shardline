//! Select a bounded ordered page from arbitrarily ordered verified metadata.
use std::collections::BTreeSet;

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct UploadEntry {
    pub(super) key: String,
    pub(super) initiated: Option<u64>,
    pub(super) id: String,
    pub(super) group: bool,
}

pub(super) struct UploadPage<'query> {
    prefix: &'query str,
    delimiter: &'query str,
    marker: &'query str,
    upload_marker: &'query str,
    marker_time: Option<Option<u64>>,
    limit: usize,
    entries: BTreeSet<UploadEntry>,
}

impl<'query> UploadPage<'query> {
    pub(super) const fn new(
        prefix: &'query str,
        delimiter: &'query str,
        marker: &'query str,
        upload_marker: &'query str,
        marker_time: Option<Option<u64>>,
        limit: usize,
    ) -> Self {
        Self {
            prefix,
            delimiter,
            marker,
            upload_marker,
            marker_time,
            limit,
            entries: BTreeSet::new(),
        }
    }

    pub(super) fn push(&mut self, key: String, id: String, initiated: Option<u64>) {
        if !key.starts_with(self.prefix) || key.as_str() < self.marker {
            return;
        }
        if key == self.marker
            && (self.upload_marker.is_empty()
                || self.marker_time.map_or_else(
                    || id.as_str() <= self.upload_marker,
                    |time| (initiated, id.as_str()) <= (time, self.upload_marker),
                ))
        {
            return;
        }
        let group = (!self.delimiter.is_empty())
            .then(|| {
                key.strip_prefix(self.prefix)
                    .and_then(|suffix| suffix.split_once(self.delimiter))
                    .map(|(head, _tail)| format!("{}{head}{}", self.prefix, self.delimiter))
            })
            .flatten();
        let entry = if let Some(group) = group {
            if group.as_str() <= self.marker {
                return;
            }
            UploadEntry {
                key: group,
                initiated: None,
                id: String::new(),
                group: true,
            }
        } else {
            UploadEntry {
                key,
                initiated,
                id,
                group: false,
            }
        };
        self.entries.insert(entry);
        if self.entries.len() > self.limit.saturating_add(1) {
            self.entries.pop_last();
        }
    }

    pub(super) fn finish(self) -> (bool, Vec<UploadEntry>) {
        let truncated = self.entries.len() > self.limit;
        let entries = self.entries.into_iter().take(self.limit).collect();
        (truncated, entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reverse_inventory_retains_only_one_page_and_lookahead() {
        let mut page = UploadPage::new("", "", "", "", None, 3);
        for index in (0..10000).rev() {
            page.push(format!("{index:05}"), format!("id-{index}"), Some(index));
            assert!(page.entries.len() <= 4);
        }
        let (truncated, entries) = page.finish();
        assert!(truncated);
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            ["00000", "00001", "00002"]
        );
    }

    #[test]
    fn repeated_groups_share_one_slot_and_known_marker_uses_initiation_order() {
        let mut groups = UploadPage::new("", "/", "", "", None, 2);
        for index in 0..10000 {
            groups.push(format!("folder/{index}"), index.to_string(), Some(index));
        }
        groups.push("z".to_owned(), "z-id".to_owned(), Some(1));
        let (truncated, entries) = groups.finish();
        assert!(!truncated);
        assert_eq!(entries.len(), 2);
        let mut marked = UploadPage::new("", "", "key", "ff", Some(Some(10)), 3);
        marked.push("key".to_owned(), "ff".to_owned(), Some(10));
        marked.push("key".to_owned(), "00".to_owned(), Some(11));
        assert_eq!(marked.finish().1.len(), 1);
    }
}
