//! Bounded request-order hints; every hit still requires remote verification.
use std::{
    collections::{HashSet, VecDeque},
    sync::Mutex,
};

use crate::ObjectKey;

// A one-GiB native upload commonly publishes about 16K compatibility chunks.
// The old 512-entry window forgot almost all of them before a second version
// arrived, forcing failed conditional PUTs before most verification GETs.
// Keep a bounded window large enough for that workload. A hit still performs
// the same remote length/hash verification; eviction only changes request order.
const MAX_HINTS: usize = 32 * 1024;

#[derive(Default)]
pub(super) struct ExistenceHints {
    entries: Mutex<Entries>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gigabyte_chunk_window_survives_until_bounded_eviction() {
        let hints = ExistenceHints::default();
        let first = ObjectKey::parse("chunks/first").unwrap();
        hints.remember(&first);
        for index in 1..16 * 1024 {
            hints.remember(&ObjectKey::parse(&format!("chunks/{index}")).unwrap());
        }
        assert!(hints.contains(&first));
        for index in 16 * 1024..MAX_HINTS {
            hints.remember(&ObjectKey::parse(&format!("chunks/{index}")).unwrap());
        }
        assert!(hints.contains(&first));
        // Duplicate observations must not consume capacity or evict another key.
        hints.remember(&first);
        assert_eq!(hints.entries.lock().unwrap().known.len(), MAX_HINTS);
        let next = ObjectKey::parse("chunks/next").unwrap();
        hints.remember(&next);
        assert!(!hints.contains(&first));
        assert!(hints.contains(&next));
        let entries = hints.entries.lock().unwrap();
        assert_eq!(entries.known.len(), MAX_HINTS);
        assert_eq!(entries.order.len(), MAX_HINTS);
    }
}

#[derive(Default)]
struct Entries {
    known: HashSet<[u8; 32]>,
    order: VecDeque<[u8; 32]>,
}

impl ExistenceHints {
    pub(super) fn contains(&self, key: &ObjectKey) -> bool {
        let digest = *blake3::hash(key.as_str().as_bytes()).as_bytes();
        self.entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .known
            .contains(&digest)
    }

    pub(super) fn remember(&self, key: &ObjectKey) {
        let digest = *blake3::hash(key.as_str().as_bytes()).as_bytes();
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if entries.known.contains(&digest) {
            return;
        }
        if entries.known.len() == MAX_HINTS
            && let Some(oldest) = entries.order.pop_front()
        {
            entries.known.remove(&oldest);
        }
        entries.known.insert(digest);
        entries.order.push_back(digest);
    }
}
