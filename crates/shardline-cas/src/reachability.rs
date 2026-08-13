//! Object reachability contract for GC, fsck, repair, and deletion.
//!
//! All lifecycle tools must use this trait to determine whether an object is
//! reachable instead of querying storage or index internals directly.

#[cfg(test)]
use shardline_index::MemoryIndexStore;
use shardline_index::{AsyncIndexStore, StoredObjectId};

use crate::CasError;

/// Formal object reachability check.
///
/// An object is reachable if it is registered in the index as part of a
/// committed file reconstruction or pending upload intent. This is the single
/// authority for reachability across GC, fsck, repair, and deletion — lifecycle
/// tools must never decide reachability by querying storage internals directly.
///
/// # Examples
///
/// The trait is blanket-implemented for every [`AsyncIndexStore`], so a
/// [`shardline_index::MemoryIndexStore`] can answer reachability queries out
/// of the box:
///
/// ```
/// use shardline_cas::ObjectReachability;
/// use shardline_index::{MemoryIndexStore, StoredObjectId};
/// use shardline_protocol::ShardlineHash;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let store = MemoryIndexStore::new();
///     let object_id = StoredObjectId::new(ShardlineHash::from_bytes([9; 32]));
///
///     assert!(!ObjectReachability::is_object_reachable(&store, &object_id).await?);
///     store.insert_object(&object_id)?;
///     assert!(ObjectReachability::is_object_reachable(&store, &object_id).await?);
///     Ok(())
/// }
/// ```
#[async_trait::async_trait]
pub trait ObjectReachability {
    /// Returns whether an object is reachable.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying index lookup fails.
    async fn is_object_reachable(&self, object_id: &StoredObjectId) -> Result<bool, CasError>;
}

#[async_trait::async_trait]
impl<T> ObjectReachability for T
where
    T: AsyncIndexStore + Sync,
    T::Error: std::error::Error,
{
    async fn is_object_reachable(&self, object_id: &StoredObjectId) -> Result<bool, CasError> {
        let object_id = *object_id;
        AsyncIndexStore::contains_object(self, &object_id)
            .await
            .map_err(CasError::from_index)
    }
}

#[cfg(test)]
mod tests {
    use super::{MemoryIndexStore, StoredObjectId};
    use crate::ObjectReachability;
    use shardline_protocol::ShardlineHash;

    #[tokio::test]
    async fn reachability_returns_false_for_unknown() {
        let store = MemoryIndexStore::new();
        let id = StoredObjectId::new(ShardlineHash::from_bytes([1; 32]));
        let reachable = ObjectReachability::is_object_reachable(&store, &id).await;
        assert!(matches!(reachable, Ok(false)));
    }

    #[tokio::test]
    async fn reachability_returns_true_for_registered() {
        let store = MemoryIndexStore::new();
        let hash = ShardlineHash::from_bytes([2; 32]);
        let id = StoredObjectId::new(hash);
        store.insert_object(&id).unwrap();
        let reachable = ObjectReachability::is_object_reachable(&store, &id).await;
        assert!(matches!(reachable, Ok(true)));
    }
}
