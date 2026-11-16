use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tokio::time::Instant;

use crate::ReconstructionCacheKey;

#[derive(Debug)]
pub(super) struct MemoryEntry {
    pub(super) payload: Vec<u8>,
    pub(super) inserted_at: Instant,
    pub(super) seq: u64,
}

impl MemoryEntry {
    pub(super) fn is_live(&self, now: Instant, ttl: std::time::Duration) -> bool {
        // Compare elapsed age instead of adding TTL to an Instant: every
        // accepted duration remains valid even if its deadline is unrepresentable.
        now.saturating_duration_since(self.inserted_at) < ttl
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct EvictionKey(pub(super) Instant, pub(super) u64);

/// A registered in-flight loading latch for one key.
#[derive(Debug)]
pub(super) struct LoadingEntry {
    pub(super) notify: Arc<Notify>,
    /// Aliveness stamp refreshed by the loader while it is still running.
    /// A waiter at the orphan bound re-checks this stamp to tell a
    /// slow-but-alive loader (fresh stamp) from a genuinely dead one
    /// (stale stamp) before releasing the latch.
    pub(super) last_seen_alive: Arc<Mutex<Instant>>,
}

impl LoadingEntry {
    pub(super) fn new(notify: Arc<Notify>) -> Self {
        Self {
            notify,
            last_seen_alive: Arc::new(Mutex::new(Instant::now())),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct CacheInner {
    pub(super) entries: HashMap<ReconstructionCacheKey, MemoryEntry>,
    pub(super) eviction_order: BTreeMap<EvictionKey, ReconstructionCacheKey>,
    pub(super) next_seq: u64,
    pub(super) total_bytes: usize,
}

impl CacheInner {
    pub(super) fn live_payload(
        &self,
        key: &ReconstructionCacheKey,
        now: Instant,
        ttl: std::time::Duration,
    ) -> Option<Vec<u8>> {
        self.entries
            .get(key)
            .filter(|entry| entry.is_live(now, ttl))
            .map(|entry| entry.payload.clone())
    }

    pub(super) fn store(
        &mut self,
        key: &ReconstructionCacheKey,
        payload: &[u8],
        now: Instant,
        max_entries: usize,
        max_bytes: usize,
    ) {
        // Replacement admission counts only retained payloads. Also invalidate
        // the previous value when the replacement cannot fit in the cache.
        self.remove(key);
        if payload.len() > max_bytes {
            return;
        }
        while self.entries.len() >= max_entries
            || self.total_bytes.saturating_add(payload.len()) > max_bytes
        {
            if self.entries.is_empty() {
                break;
            }
            self.evict_oldest();
        }
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        self.entries.insert(
            key.clone(),
            MemoryEntry {
                payload: payload.to_vec(),
                inserted_at: now,
                seq,
            },
        );
        self.eviction_order
            .insert(EvictionKey(now, seq), key.clone());
        self.total_bytes = self.total_bytes.saturating_add(payload.len());
    }

    pub(super) fn remove(&mut self, key: &ReconstructionCacheKey) -> Option<MemoryEntry> {
        let entry = self.entries.remove(key)?;
        self.total_bytes = self.total_bytes.saturating_sub(entry.payload.len());
        self.eviction_order
            .remove(&EvictionKey(entry.inserted_at, entry.seq));
        Some(entry)
    }

    pub(super) fn evict_oldest(&mut self) {
        while let Some((_eviction_key, key)) = self.eviction_order.pop_first() {
            if let Some(entry) = self.entries.remove(&key) {
                self.total_bytes = self.total_bytes.saturating_sub(entry.payload.len());
                return;
            }
        }
    }
}
