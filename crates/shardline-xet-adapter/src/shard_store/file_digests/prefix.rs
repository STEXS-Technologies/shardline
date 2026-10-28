//! Disposable SHA-256 prefix states, never a substitute for source authentication.
use std::{
    collections::{HashMap, VecDeque},
    sync::{Mutex, OnceLock},
};

use sha2::Sha256;
use shardline_protocol::RepositoryScope;
use shardline_xet_core::metadata_shard::file_structs::FileDataSequenceEntry;

const CAPACITY: usize = 1024;
const MAX_PENDING: usize = 16;
type Key = [u8; 32];

#[derive(Default)]
struct Cache {
    states: HashMap<Key, Sha256>,
    order: VecDeque<Key>,
}

impl Cache {
    fn insert(&mut self, key: Key, state: Sha256) {
        if self.states.contains_key(&key) {
            return;
        }
        if self.states.len() == CAPACITY
            && let Some(oldest) = self.order.pop_front()
        {
            self.states.remove(&oldest);
        }
        self.order.push_back(key);
        self.states.insert(key, state);
    }
}

static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();

pub(super) struct Prefixes {
    key: blake3::Hasher,
    pending: Vec<(Key, Sha256)>,
}

impl Prefixes {
    pub(super) fn new(scope: &RepositoryScope) -> Self {
        let mut key = blake3::Hasher::new();
        key.update(b"shardline:verified-sha256-prefix:v2");
        for field in [scope.provider().as_str(), scope.owner(), scope.name()] {
            key.update(&field.len().to_le_bytes());
            key.update(field.as_bytes());
        }
        key.update(&[u8::from(scope.revision().is_some())]);
        if let Some(revision) = scope.revision() {
            key.update(revision.as_bytes());
        }
        Self {
            key,
            pending: Vec::new(),
        }
    }

    /// Native roots bind member content and order. The caller must still
    /// authenticate every source and validate every claimed range and length.
    /// Adjacent terms in one source describe the same bytes regardless of their
    /// split points. Canonicalize only the cache key, never the terms validated
    /// by the caller; gaps, overlap and backwards ranges remain distinct.
    pub(super) fn begin(&mut self, terms: &[FileDataSequenceEntry]) -> Option<Sha256> {
        let mut remaining = terms;
        while let Some(first) = remaining.first() {
            let adjacent = super::contiguous_source_terms(remaining).ok()?;
            let last = adjacent.last()?;
            let length = adjacent.iter().try_fold(0_u64, |total, term| {
                total.checked_add(term.unpacked_segment_bytes)
            })?;
            self.key.update(first.xorb_hash.hex().as_bytes());
            self.key.update(&first.chunk_index_start.to_le_bytes());
            self.key.update(&last.chunk_index_end.to_le_bytes());
            self.key.update(&length.to_le_bytes());
            remaining = remaining.get(adjacent.len()..)?;
        }
        CACHE
            .get_or_init(Mutex::default)
            .lock()
            .ok()?
            .states
            .get(self.key.finalize().as_bytes())
            .cloned()
    }

    pub(super) fn retain(&mut self, state: &Sha256) {
        if self.pending.len() < MAX_PENDING {
            self.pending
                .push((*self.key.finalize().as_bytes(), state.clone()));
        }
    }

    /// Called only after the complete file's identity has been verified.
    pub(super) fn publish(self) {
        if let Ok(mut cache) = CACHE.get_or_init(Mutex::default).lock() {
            for (key, state) in self.pending {
                cache.insert(key, state);
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use sha2::Digest;

    #[test]
    fn unfinished_file_keeps_only_bounded_pending_states() {
        let scope = RepositoryScope::new(
            shardline_protocol::RepositoryProvider::GitHub,
            "prefix-tests",
            "pending-bound",
            None,
        )
        .unwrap();
        let mut prefixes = Prefixes::new(&scope);
        for _ in 0..CAPACITY {
            prefixes.retain(&Sha256::new());
        }
        assert_eq!(prefixes.pending.len(), MAX_PENDING);
    }

    #[test]
    fn eviction_bounds_states_and_order_without_growing_duplicate_entries() {
        let mut cache = Cache::default();
        for number in 0..=CAPACITY {
            let mut key = [0; 32];
            key[..8].copy_from_slice(&(number as u64).to_le_bytes());
            cache.insert(key, Sha256::new());
            cache.insert(key, Sha256::new());
        }
        assert_eq!(cache.states.len(), CAPACITY);
        assert_eq!(cache.order.len(), CAPACITY);
        assert!(!cache.states.contains_key(&[0; 32]));
    }
}
