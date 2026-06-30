use std::{
    collections::{BTreeMap, HashMap},
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

use tokio::time::Instant;

use tokio::sync::{Notify, RwLock};

use crate::{AsyncReconstructionCache, ReconstructionCacheFuture, ReconstructionCacheKey};

#[derive(Debug, Clone)]
struct MemoryEntry {
    payload: Vec<u8>,
    expires_at: Instant,
    inserted_at: Instant,
    seq: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct EvictionKey(Instant, u64);

impl Ord for EvictionKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .cmp(&other.0)
            .then_with(|| self.1.cmp(&other.1))
    }
}

impl PartialOrd for EvictionKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct CacheInner {
    entries: HashMap<ReconstructionCacheKey, MemoryEntry>,
    eviction_order: BTreeMap<EvictionKey, ReconstructionCacheKey>,
    next_seq: u64,
    loading: HashMap<ReconstructionCacheKey, Arc<Notify>>,
}

impl CacheInner {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            eviction_order: BTreeMap::new(),
            next_seq: 0,
            loading: HashMap::new(),
        }
    }

    fn insert(&mut self, key: ReconstructionCacheKey, entry: MemoryEntry) {
        let inserted_at = entry.inserted_at;
        if let Some(old) = self.entries.insert(key.clone(), entry) {
            self.eviction_order
                .remove(&EvictionKey(old.inserted_at, old.seq));
        }
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        self.eviction_order
            .insert(EvictionKey(inserted_at, seq), key);
    }

    fn remove(&mut self, key: &ReconstructionCacheKey) -> Option<MemoryEntry> {
        if let Some(entry) = self.entries.remove(key) {
            self.eviction_order
                .remove(&EvictionKey(entry.inserted_at, entry.seq));
            Some(entry)
        } else {
            None
        }
    }

    fn evict_oldest(&mut self) {
        while let Some((_eviction_key, key)) = self.eviction_order.pop_first() {
            if self.entries.contains_key(&key) {
                self.entries.remove(&key);
                return;
            }
        }
    }

    fn prune_expired(&mut self, now: Instant) {
        let expired: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.expires_at <= now)
            .map(|(k, v)| (k.clone(), EvictionKey(v.inserted_at, v.seq)))
            .collect();
        for (key, eviction_key) in expired {
            self.entries.remove(&key);
            self.eviction_order.remove(&eviction_key);
        }
    }
}

/// Bounded in-memory reconstruction cache adapter.
#[derive(Debug, Clone)]
pub struct MemoryReconstructionCache {
    ttl: Duration,
    max_entries: NonZeroUsize,
    inner: Arc<RwLock<CacheInner>>,
}

impl MemoryReconstructionCache {
    /// Creates a bounded in-memory reconstruction cache.
    #[must_use]
    pub fn new(ttl_seconds: NonZeroU64, max_entries: NonZeroUsize) -> Self {
        Self {
            ttl: Duration::from_secs(ttl_seconds.get()),
            max_entries,
            inner: Arc::new(RwLock::new(CacheInner::new())),
        }
    }
}

impl AsyncReconstructionCache for MemoryReconstructionCache {
    fn ready(&self) -> ReconstructionCacheFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn get<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, Option<Vec<u8>>> {
        Box::pin(async move {
            let now = Instant::now();
            {
                let inner = self.inner.read().await;
                if let Some(entry) = inner.entries.get(key) {
                    if entry.expires_at > now {
                        return Ok(Some(entry.payload.clone()));
                    }
                } else if !inner.loading.contains_key(key) {
                    return Ok(None);
                }
            }

            let mut inner = self.inner.write().await;

            if let Some(entry) = inner.entries.get(key) {
                if entry.expires_at > now {
                    return Ok(Some(entry.payload.clone()));
                }
            }

            if let Some(notify) = inner.loading.get(key) {
                let notify = Arc::clone(notify);
                drop(inner);
                notify.notified().await;

                let inner = self.inner.read().await;
                if let Some(entry) = inner.entries.get(key) {
                    if entry.expires_at > Instant::now() {
                        return Ok(Some(entry.payload.clone()));
                    }
                }
                return Ok(None);
            }

            let notify = Arc::new(Notify::new());
            inner.loading.insert(key.clone(), Arc::clone(&notify));

            let should_remove = inner
                .entries
                .get(key)
                .is_some_and(|entry| entry.expires_at <= now);
            if should_remove {
                inner.remove(key);
            }
            Ok(None)
        })
    }

    fn put<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
        payload: &'operation [u8],
    ) -> ReconstructionCacheFuture<'operation, ()> {
        Box::pin(async move {
            let now = Instant::now();
            let expires_at = now.checked_add(self.ttl).map_or(now, |value| value);
            let mut inner = self.inner.write().await;
            inner.prune_expired(now);
            if !inner.entries.contains_key(key) && inner.entries.len() >= self.max_entries.get() {
                inner.evict_oldest();
            }
            inner.insert(
                key.clone(),
                MemoryEntry {
                    payload: payload.to_vec(),
                    expires_at,
                    inserted_at: now,
                    seq: 0,
                },
            );
            if let Some(notify) = inner.loading.remove(key) {
                notify.notify_waiters();
            }
            Ok(())
        })
    }

    fn delete<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, bool> {
        Box::pin(async move {
            let mut inner = self.inner.write().await;
            Ok(inner.remove(key).is_some())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU64, NonZeroUsize},
        time::Duration,
    };

    use super::MemoryReconstructionCache;
    use crate::{AsyncReconstructionCache, ReconstructionCacheKey};

    #[tokio::test]
    async fn memory_cache_roundtrips_one_payload() {
        let cache = MemoryReconstructionCache::new(NonZeroU64::MIN, NonZeroUsize::MIN);
        let key = ReconstructionCacheKey::latest("asset.bin", None);
        let put = cache.put(&key, b"payload").await;
        assert!(put.is_ok());

        let value = cache.get(&key).await;

        assert!(value.is_ok());
        assert_eq!(value.ok(), Some(Some(b"payload".to_vec())));
    }

    #[tokio::test]
    async fn memory_cache_evicts_oldest_entry_when_capacity_is_full() {
        let max_entries = NonZeroUsize::new(2).map_or(NonZeroUsize::MIN, |value| value);
        let ttl_seconds = NonZeroU64::new(60).map_or(NonZeroU64::MIN, |value| value);
        let cache = MemoryReconstructionCache::new(ttl_seconds, max_entries);
        let first = ReconstructionCacheKey::latest("asset-1.bin", None);
        let second = ReconstructionCacheKey::latest("asset-2.bin", None);
        let third = ReconstructionCacheKey::latest("asset-3.bin", None);

        assert!(cache.put(&first, b"first").await.is_ok());
        assert!(cache.put(&second, b"second").await.is_ok());
        assert!(cache.put(&third, b"third").await.is_ok());

        let first_value = cache.get(&first).await;
        let second_value = cache.get(&second).await;
        let third_value = cache.get(&third).await;

        assert!(first_value.is_ok());
        assert!(second_value.is_ok());
        assert!(third_value.is_ok());
        assert_eq!(first_value.ok(), Some(None));
        assert_eq!(second_value.ok(), Some(Some(b"second".to_vec())));
        assert_eq!(third_value.ok(), Some(Some(b"third".to_vec())));
    }

    #[tokio::test(start_paused = true)]
    async fn memory_cache_expires_entries_after_ttl() {
        let ttl_seconds = NonZeroU64::new(1).map_or(NonZeroU64::MIN, |value| value);
        let cache = MemoryReconstructionCache::new(ttl_seconds, NonZeroUsize::MIN);
        let key = ReconstructionCacheKey::latest("asset.bin", None);
        assert!(cache.put(&key, b"payload").await.is_ok());

        tokio::time::advance(Duration::from_secs(1)).await;
        let value = cache.get(&key).await;

        assert!(value.is_ok());
        assert_eq!(value.ok(), Some(None));
    }
}
