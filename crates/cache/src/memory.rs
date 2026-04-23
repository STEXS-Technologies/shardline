use std::{
    collections::HashMap,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

use crate::{AsyncReconstructionCache, ReconstructionCacheFuture, ReconstructionCacheKey};

#[derive(Debug, Clone)]
struct MemoryEntry {
    payload: Vec<u8>,
    expires_at: Instant,
    inserted_at: Instant,
}

/// Bounded in-memory reconstruction cache adapter.
#[derive(Debug, Clone)]
pub struct MemoryReconstructionCache {
    ttl: Duration,
    max_entries: NonZeroUsize,
    entries: Arc<RwLock<HashMap<ReconstructionCacheKey, MemoryEntry>>>,
}

impl MemoryReconstructionCache {
    /// Creates a bounded in-memory reconstruction cache.
    #[must_use]
    pub fn new(ttl_seconds: NonZeroU64, max_entries: NonZeroUsize) -> Self {
        Self {
            ttl: Duration::from_secs(ttl_seconds.get()),
            max_entries,
            entries: Arc::new(RwLock::new(HashMap::new())),
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
                let entries = self.entries.read().await;
                if let Some(entry) = entries.get(key) {
                    if entry.expires_at > now {
                        return Ok(Some(entry.payload.clone()));
                    }
                } else {
                    return Ok(None);
                }
            }

            let mut entries = self.entries.write().await;
            let should_remove = entries
                .get(key)
                .is_some_and(|entry| entry.expires_at <= now);
            if should_remove {
                let _removed = entries.remove(key);
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
            let mut entries = self.entries.write().await;
            prune_expired_entries(&mut entries, now);
            if !entries.contains_key(key) && entries.len() >= self.max_entries.get() {
                evict_oldest_entry(&mut entries);
            }
            entries.insert(
                key.clone(),
                MemoryEntry {
                    payload: payload.to_vec(),
                    expires_at,
                    inserted_at: now,
                },
            );
            Ok(())
        })
    }

    fn delete<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, bool> {
        Box::pin(async move {
            let mut entries = self.entries.write().await;
            Ok(entries.remove(key).is_some())
        })
    }
}

fn prune_expired_entries(entries: &mut HashMap<ReconstructionCacheKey, MemoryEntry>, now: Instant) {
    entries.retain(|_key, entry| entry.expires_at > now);
}

fn evict_oldest_entry(entries: &mut HashMap<ReconstructionCacheKey, MemoryEntry>) {
    let oldest_key = entries
        .iter()
        .min_by_key(|(_key, entry)| entry.inserted_at)
        .map(|(key, _entry)| key.clone());
    if let Some(oldest_key) = oldest_key {
        let _removed = entries.remove(&oldest_key);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU64, NonZeroUsize},
        time::Duration,
    };

    use tokio::time::sleep;

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

    #[tokio::test]
    async fn memory_cache_expires_entries_after_ttl() {
        let ttl_seconds = NonZeroU64::new(1).map_or(NonZeroU64::MIN, |value| value);
        let cache = MemoryReconstructionCache::new(ttl_seconds, NonZeroUsize::MIN);
        let key = ReconstructionCacheKey::latest("asset.bin", None);
        assert!(cache.put(&key, b"payload").await.is_ok());

        sleep(Duration::from_millis(1100)).await;
        let value = cache.get(&key).await;

        assert!(value.is_ok());
        assert_eq!(value.ok(), Some(None));
    }
}
