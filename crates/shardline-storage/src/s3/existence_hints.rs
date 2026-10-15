//! Bounded request-order hints; every hit still requires remote verification.
use std::{
    collections::{HashSet, VecDeque},
    sync::Mutex,
};

use crate::ObjectKey;

const MAX_HINTS: usize = 512;

#[derive(Default)]
pub(super) struct ExistenceHints {
    entries: Mutex<Entries>,
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
