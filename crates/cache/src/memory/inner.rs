use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use tokio::sync::Notify;
use tokio::time::Instant;

use crate::ReconstructionCacheKey;

#[derive(Debug, Clone)]
pub(super) struct MemoryEntry {
    pub(super) payload: Arc<Vec<u8>>,
    pub(super) expires_at: Instant,
    pub(super) inserted_at: Instant,
    pub(super) seq: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct EvictionKey(pub(super) Instant, pub(super) u64);

impl Ord for EvictionKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0).then_with(|| self.1.cmp(&other.1))
    }
}

impl PartialOrd for EvictionKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
pub(super) struct CacheInner {
    pub(super) entries: HashMap<ReconstructionCacheKey, MemoryEntry>,
    pub(super) eviction_order: BTreeMap<EvictionKey, ReconstructionCacheKey>,
    pub(super) next_seq: u64,
    pub(super) loading: HashMap<ReconstructionCacheKey, Arc<Notify>>,
}

impl CacheInner {
    pub(super) fn new() -> Self {
        Self {
            entries: HashMap::new(),
            eviction_order: BTreeMap::new(),
            next_seq: 0,
            loading: HashMap::new(),
        }
    }

    pub(super) fn insert(&mut self, key: &ReconstructionCacheKey, entry: MemoryEntry) {
        let inserted_at = entry.inserted_at;
        let seq = entry.seq;
        if let Some(old) = self.entries.insert(key.clone(), entry) {
            self.eviction_order
                .remove(&EvictionKey(old.inserted_at, old.seq));
        }
        self.eviction_order
            .insert(EvictionKey(inserted_at, seq), key.clone());
    }

    pub(super) fn remove(&mut self, key: &ReconstructionCacheKey) -> Option<MemoryEntry> {
        if let Some(entry) = self.entries.remove(key) {
            self.eviction_order
                .remove(&EvictionKey(entry.inserted_at, entry.seq));
            Some(entry)
        } else {
            None
        }
    }

    pub(super) fn evict_oldest(&mut self) {
        while let Some((_eviction_key, key)) = self.eviction_order.pop_first() {
            if self.entries.contains_key(&key) {
                self.entries.remove(&key);
                return;
            }
        }
    }
}
